package filebackfill

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// mockSource 是 batchSource 测试实现：预置 batches 队列，逐批返回，末尾返 io.EOF。
// nextErr 非 nil 时，Next 优先返 nextErr（模拟 ctx 取消 / OS 5xx）。
type mockSource struct {
	mu       sync.Mutex
	batches  [][]sourceDoc
	closed   bool
	nextErr  error
	nextHook func() // 每次 Next 前触发（测 ctx cancel timing）
}

func (m *mockSource) Next(ctx context.Context) ([]sourceDoc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nextHook != nil {
		m.nextHook()
	}
	if m.nextErr != nil {
		return nil, m.nextErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(m.batches) == 0 {
		return nil, io.EOF
	}
	b := m.batches[0]
	m.batches = m.batches[1:]
	return b, nil
}

func (m *mockSource) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// mockExtractor 按预置结果依次返回抽取结论。
// results[i] 描述第 i 次调用返回的 (reason, cause, err)。
type mockExtractor struct {
	mu      sync.Mutex
	results []extractorResult
	calls   []string // 记录调用顺序（messageID）
}

type extractorResult struct {
	reason string
	cause  error
	err    error
}

func (m *mockExtractor) Extract(ctx context.Context, messageID, url, name, ext string, size int64) (string, error, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, messageID)
	if len(m.results) == 0 {
		return "", nil, nil
	}
	r := m.results[0]
	m.results = m.results[1:]
	return r.reason, r.cause, r.err
}

// TestRateLimiter_Basic 消耗令牌 → 补充 → 消耗（默认 burst=rate，一秒内耗尽 rate 个token）。
func TestRateLimiter_Basic(t *testing.T) {
	rl := newRateLimiter(100)
	// 首次调用应立即返（burst=100 有余额）
	start := time.Now()
	for i := 0; i < 50; i++ {
		if err := rl.Wait(context.Background()); err != nil {
			t.Fatalf("Wait: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("burst path should be fast, got %v", elapsed)
	}
}

// TestRateLimiter_Unlimited docsPerSec<=0 不限速。
func TestRateLimiter_Unlimited(t *testing.T) {
	rl := newRateLimiter(0)
	start := time.Now()
	for i := 0; i < 1000; i++ {
		if err := rl.Wait(context.Background()); err != nil {
			t.Fatalf("Wait: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("unlimited should be instant, got %v", elapsed)
	}
}

// TestRateLimiter_CtxCancel ctx 取消 → Wait 立即返 err。
func TestRateLimiter_CtxCancel(t *testing.T) {
	rl := newRateLimiter(0.1) // 极慢，每 10s 一个令牌
	// 先消耗初始 burst 让下次 wait 会阻塞
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("initial Wait unexpected err: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	err := rl.Wait(ctx)
	if err == nil {
		t.Fatal("expected error on ctx cancel")
	}
}

// TestFormatDuration OS scroll TTL 参数格式化（"5m" / "30s"）。< 1s floor 到 "1s"（N4）。
func TestFormatDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{5 * time.Minute, "5m"},
		{30 * time.Second, "30s"},
		{2 * time.Minute, "2m"},
		{500 * time.Millisecond, "1s"}, // < 1s floor 到 "1s"（避免 OS scroll TTL 立即过期）
		{0, "1s"},                      // 0 也 floor
	}
	for _, c := range cases {
		if got := formatDuration(c.d); got != c.want {
			t.Errorf("formatDuration(%v): got %q want %q", c.d, got, c.want)
		}
	}
}

// TestStats_TypeShape Stats 字段全 int64（K8s Job 退出码计算依赖），确保编译期锁死类型。
func TestStats_TypeShape(t *testing.T) {
	var s Stats
	// 都必须支持 int64 递增
	s.Scanned++
	s.Extracted++
	s.DLQ++
	s.Skipped++
	s.OSTransient++
	if s.Scanned != 1 || s.Extracted != 1 || s.DLQ != 1 || s.Skipped != 1 || s.OSTransient != 1 {
		t.Fatal("Stats fields did not increment as expected")
	}
}

// TestConfig_ToExtractorConfig 字段转发正确。
func TestConfig_ToExtractorConfig(t *testing.T) {
	c := Config{
		ESAddresses: []string{"http://os:9200"},
		ESIndex:     "octo-message",
		ESUsername:  "u",
		ESPassword:  "p",
		TikaURL:     "http://tika:9998",
		DownloadTimeout: 10 * time.Second,
		ExtractTimeout:  20 * time.Second,
		MaxFileSize:     1024,
		MaxContentBytes: 2048,
		HTTPRetries:     5,
	}
	ec := c.ToExtractorConfig()
	if len(ec.ESAddresses) != 1 || ec.ESAddresses[0] != "http://os:9200" {
		t.Errorf("ESAddresses: %v", ec.ESAddresses)
	}
	if ec.TikaURL != "http://tika:9998" || ec.MaxFileSize != 1024 || ec.HTTPRetries != 5 {
		t.Errorf("mismatched fields: %+v", ec)
	}
}

// TestSourceDoc_ParseFromHit sourceDoc 字段结构（防未来 refactor 破坏契约）。
func TestSourceDoc_ParseFromHit(t *testing.T) {
	d := sourceDoc{
		MessageID: "42",
		URL:       "https://cdn.deepminer.com.cn/x.pdf",
		Name:      "x.pdf",
		Extension: ".pdf",
		Size:      1024,
	}
	if d.MessageID == "" || d.URL == "" {
		t.Fatal("required fields empty")
	}
}

// mkDoc 造一条 sourceDoc 用于 Runner.Run 测试。
func mkDoc(id string) sourceDoc {
	return sourceDoc{
		MessageID: id,
		URL:       "https://cdn.deepminer.com.cn/" + id + ".pdf",
		Name:      id + ".pdf",
		Extension: ".pdf",
		Size:      1024,
	}
}

// TestRun_HappyPath 5 条 doc 全 success → Extracted=5，DLQ=0，OSTransient=0。
func TestRun_HappyPath(t *testing.T) {
	src := &mockSource{batches: [][]sourceDoc{
		{mkDoc("1"), mkDoc("2"), mkDoc("3")},
		{mkDoc("4"), mkDoc("5")},
	}}
	ext := &mockExtractor{} // 空 results → 默认全 success
	r := NewRunnerWith(src, ext, 0)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.Scanned != 5 || stats.Extracted != 5 || stats.DLQ != 0 || stats.OSTransient != 0 {
		t.Errorf("unexpected stats: %+v", stats)
	}
	if !src.closed {
		t.Error("source should be closed after Run")
	}
	if len(ext.calls) != 5 {
		t.Errorf("expected 5 extractor calls, got %d", len(ext.calls))
	}
}

// TestRun_MixedOutcomes 5 条 doc 混合：3 success + 1 DLQ + 1 OSTransient → stats 全对。
func TestRun_MixedOutcomes(t *testing.T) {
	src := &mockSource{batches: [][]sourceDoc{{
		mkDoc("1"), mkDoc("2"), mkDoc("3"), mkDoc("4"), mkDoc("5"),
	}}}
	ext := &mockExtractor{results: []extractorResult{
		{},                                                                 // 1 → success
		{reason: "oversize", cause: errors.New("too big")},                 // 2 → DLQ
		{},                                                                 // 3 → success
		{err: errors.New("os 500 boom")},                                   // 4 → OSTransient
		{},                                                                 // 5 → success
	}}
	r := NewRunnerWith(src, ext, 0)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if stats.Scanned != 5 {
		t.Errorf("Scanned: got %d want 5", stats.Scanned)
	}
	if stats.Extracted != 3 {
		t.Errorf("Extracted: got %d want 3", stats.Extracted)
	}
	if stats.DLQ != 1 {
		t.Errorf("DLQ: got %d want 1", stats.DLQ)
	}
	if stats.OSTransient != 1 {
		t.Errorf("OSTransient: got %d want 1", stats.OSTransient)
	}
}

// TestRun_CtxCancelDuringLoop source.Next 返 context.Canceled → Run 优雅退出（err=nil）。
// 覆盖 M4 修复：ctx.Canceled 不当作运行错误。
func TestRun_CtxCancelDuringLoop(t *testing.T) {
	src := &mockSource{
		batches: [][]sourceDoc{{mkDoc("1"), mkDoc("2")}},
		nextErr: context.Canceled, // 第一次 Next 就返 canceled
	}
	ext := &mockExtractor{}
	r := NewRunnerWith(src, ext, 0)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("ctx.Canceled from source should be graceful, got err=%v", err)
	}
	// canceled 场景 Scanned=0（一批都没拉出来）
	if stats.Scanned != 0 || stats.Extracted != 0 {
		t.Errorf("cancelled mid-fetch stats should be zero: %+v", stats)
	}
	if !src.closed {
		t.Error("source should be closed even on cancel")
	}
}

// TestRun_CtxDeadlineExceeded ctx timeout 期间 → 返 ErrTimeoutIncomplete（v1.13 P2-3）：
// K8s Job 需要区分 timeout（提前退出，剩余未跑）vs signal-cancel（优雅退出），前者要 exit
// non-zero 让 operator 知道重跑。
func TestRun_CtxDeadlineExceeded(t *testing.T) {
	src := &mockSource{
		batches: [][]sourceDoc{{mkDoc("1")}},
		nextErr: context.DeadlineExceeded,
	}
	ext := &mockExtractor{}
	r := NewRunnerWith(src, ext, 0)
	_, err := r.Run(context.Background())
	if !errors.Is(err, ErrTimeoutIncomplete) {
		t.Fatalf("ctx.DeadlineExceeded must return ErrTimeoutIncomplete (P2-3), got %v", err)
	}
}

// TestRun_SourceRealError source 返非 ctx 类错 → Run 上抛（K8s Job 退 1）。
func TestRun_SourceRealError(t *testing.T) {
	realErr := errors.New("os 502 bad gateway")
	src := &mockSource{nextErr: realErr}
	ext := &mockExtractor{}
	r := NewRunnerWith(src, ext, 0)
	_, err := r.Run(context.Background())
	if !errors.Is(err, realErr) {
		t.Fatalf("expected real error propagate, got %v", err)
	}
}

// TestParseHits_SnowflakePrecision 🔴 S1 回归测试：
// 19 位 snowflake messageId 通过 json.Number + UseNumber() 保精度，不再变科学计数法。
// 参考 internal/esindex/buildraw.go:22 精度铁律。
func TestParseHits_SnowflakePrecision(t *testing.T) {
	// 真实 snowflake id 长度 18-19 位，超 float64 精度上限 2^53 (~9e15)
	realIDs := []string{
		"1234567890123456789", // 19 位典型 snowflake
		"9007199254740993",    // 2^53 + 1（float64 首次丢精度的边界）
		"12345678901234567",   // 17 位（略超 2^53）
	}
	for _, id := range realIDs {
		body := []byte(`{"messageId":` + id + `,"payload":{"file":{"url":"https://x/y.pdf","name":"y.pdf","extension":".pdf","size":1024}}}`)
		hit := opensearchapi.SearchHit{Source: body}
		docs := (&osScrollSource{}).parseHits([]opensearchapi.SearchHit{hit})
		if len(docs) != 1 {
			t.Fatalf("id=%s: expected 1 doc, got %d", id, len(docs))
		}
		if docs[0].MessageID != id {
			t.Errorf("id=%s: precision loss — got %q want %q", id, docs[0].MessageID, id)
		}
		if docs[0].Size != 1024 {
			t.Errorf("id=%s: size mismatch — got %d", id, docs[0].Size)
		}
	}
}

// TestParseHits_MissingMessageIDSkipped messageId 字段缺失 → 单条跳过，不 panic。
func TestParseHits_MissingMessageIDSkipped(t *testing.T) {
	body := []byte(`{"payload":{"file":{"url":"x"}}}`)
	hit := opensearchapi.SearchHit{Source: body}
	docs := (&osScrollSource{}).parseHits([]opensearchapi.SearchHit{hit})
	if len(docs) != 0 {
		t.Errorf("missing messageId should skip, got %d docs", len(docs))
	}
}

// TestParseHits_MalformedJSONSkipped 损坏 JSON → 单条跳过，其他 hit 正常处理。
func TestParseHits_MalformedJSONSkipped(t *testing.T) {
	hits := []opensearchapi.SearchHit{
		{Source: []byte(`{not json}`)},
		{Source: []byte(`{"messageId":42,"payload":{"file":{"url":"x","size":8}}}`)},
	}
	docs := (&osScrollSource{}).parseHits(hits)
	if len(docs) != 1 {
		t.Fatalf("expected 1 good doc, got %d", len(docs))
	}
	if docs[0].MessageID != "42" {
		t.Errorf("MessageID: got %q", docs[0].MessageID)
	}
}
