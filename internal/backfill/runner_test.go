package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// ── fakes ─────────────────────────────────────────────────────────────────────

// fakeSource 按 table 提供预置行，支持 keyset 分页（id>after），并记录每次 ReadBatch 的入参。
type fakeSource struct {
	rows     map[string][]*srcMessageRow
	readErr  error
	readCall int
}

func (s *fakeSource) ReadBatch(_ context.Context, table string, after int64, limit int) ([]*srcMessageRow, error) {
	s.readCall++
	if s.readErr != nil {
		return nil, s.readErr
	}
	var out []*srcMessageRow
	for _, r := range s.rows[table] {
		if r.ID > after {
			out = append(out, r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

// fakeWriter 记录每条被 bulk 的 message_id 出现次数（验证幂等：去重 doc 数），并可按
// message_id 注入每条结果（OK / 429 transient（自愈次数）/ permanent 4xx）。
type fakeWriter struct {
	mu        sync.Mutex
	indexed   map[string]int      // message_id -> bulk 次数（不去重，含重试）
	transient map[string]int      // message_id -> 还需失败几次（429）后转 OK
	permanent map[string]struct{} // message_id -> 永久 4xx
	bulkErr   error               // 批级错误（设置则第一次返回，之后清空模拟自愈）
	bulkCalls int
	ensureErr error
}

func newFakeWriter() *fakeWriter {
	return &fakeWriter{
		indexed:   map[string]int{},
		transient: map[string]int{},
		permanent: map[string]struct{}{},
	}
}

func (w *fakeWriter) EnsureIndex(context.Context) error { return w.ensureErr }
func (w *fakeWriter) Close() error                      { return nil }

func (w *fakeWriter) Bulk(_ context.Context, msgs []searchmsg.Message) ([]esindex.BulkItemResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.bulkCalls++
	if w.bulkErr != nil {
		err := w.bulkErr
		w.bulkErr = nil // 自愈：下一轮成功
		out := make([]esindex.BulkItemResult, len(msgs))
		for i := range msgs {
			out[i] = esindex.BulkItemResult{MessageID: msgs[i].MessageID, OK: false, Status: 0, Err: err}
		}
		return out, err
	}
	out := make([]esindex.BulkItemResult, len(msgs))
	for i := range msgs {
		id := msgs[i].MessageID
		switch {
		case w.transient[id] > 0:
			w.transient[id]--
			out[i] = esindex.BulkItemResult{MessageID: id, OK: false, Status: 429}
		case func() bool { _, ok := w.permanent[id]; return ok }():
			out[i] = esindex.BulkItemResult{MessageID: id, OK: false, Status: 400, Err: errors.New("mapping conflict")}
		default:
			w.indexed[id]++ // 记录成功写入（同 _id 多次=非幂等的信号；此处计 bulk 次数）
			out[i] = esindex.BulkItemResult{MessageID: id, OK: true, Status: 200}
		}
	}
	return out, nil
}

// uniqueIndexed 返回成功写入的去重 message_id 数（ES `_id` 幂等下 = doc 数）。
func (w *fakeWriter) uniqueIndexed() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.indexed)
}

func textRow(id int64, mid, content string, createdUnix int64) *srcMessageRow {
	b, err := json.Marshal(map[string]interface{}{"type": contentTypeText, "content": content})
	if err != nil {
		panic(err)
	}
	return &srcMessageRow{ID: id, MessageID: mid, ChannelType: 2, Payload: b, CreatedUnix: createdUnix}
}

func newTestRunner(t *testing.T, cfg Config, src SourceReader, w esindex.Writer) (*Runner, *DLQSpill, *CheckpointStore) {
	t.Helper()
	dir := t.TempDir()
	dlq, err := OpenDLQSpill(filepath.Join(dir, "dlq"))
	if err != nil {
		t.Fatalf("dlq: %v", err)
	}
	cp, err := OpenCheckpoint(filepath.Join(dir, "cp.json"))
	if err != nil {
		t.Fatalf("cp: %v", err)
	}
	r := NewRunner(cfg, src, w, cp, dlq)
	r.sleep = func(context.Context, time.Duration) error { return nil } // 即时退避
	return r, dlq, cp
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestRunner_HappyPath 全文本行 → 全部写 ES，无 DLQ，checkpoint 推进到批末。
func TestRunner_HappyPath(t *testing.T) {
	src := &fakeSource{rows: map[string][]*srcMessageRow{
		"message": {textRow(1, "a", "x", 100), textRow(2, "b", "y", 100), textRow(3, "c", "z", 100)},
	}}
	w := newFakeWriter()
	r, dlq, cp := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 2, DocsPerSec: 0}, src, w)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Read != 3 || stats.Indexed != 3 || stats.DLQ != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if dlq.Count() != 0 {
		t.Fatalf("dlq count=%d", dlq.Count())
	}
	if cp.Get("message") != 3 {
		t.Fatalf("checkpoint not advanced to 3: %d", cp.Get("message"))
	}
}

// TestRunner_Idempotent 同一 message_id 重复行 → ES 去重，doc 数不增（`_id` 幂等）。
func TestRunner_Idempotent(t *testing.T) {
	src := &fakeSource{rows: map[string][]*srcMessageRow{
		"message": {textRow(1, "dup", "v1", 100), textRow(2, "dup", "v2", 100), textRow(3, "uniq", "w", 100)},
	}}
	w := newFakeWriter()
	r, _, _ := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 10}, src, w)
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := w.uniqueIndexed(); got != 2 {
		t.Fatalf("duplicate message_id must dedupe to 2 docs, got %d", got)
	}
}

// TestRunner_RawExcludedStillIndexed Signal / 非文本 → raw_excluded，仍写 ES 占 doc，不进 DLQ。
func TestRunner_RawExcludedStillIndexed(t *testing.T) {
	src := &fakeSource{rows: map[string][]*srcMessageRow{
		"message": {
			{ID: 1, MessageID: "sig", Signal: 1, Payload: []byte("ENC"), CreatedUnix: 100},
			textRow(2, "txt", "hi", 100),
		},
	}}
	w := newFakeWriter()
	r, dlq, _ := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 10}, src, w)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Indexed != 2 || stats.RawExcluded != 1 || stats.DLQ != 0 {
		t.Fatalf("raw_excluded must still index (occupy doc), no DLQ: %+v", stats)
	}
	if dlq.Count() != 0 {
		t.Fatalf("raw_excluded must NOT go to DLQ")
	}
}

// TestRunner_RealAnomalyToDLQ payload 真异常 → 不写 ES，落本地 DLQ spill 并计数。
func TestRunner_RealAnomalyToDLQ(t *testing.T) {
	src := &fakeSource{rows: map[string][]*srcMessageRow{
		"message": {
			textRow(1, "ok", "hi", 100),
			{ID: 2, MessageID: "bad", Payload: []byte("{not json"), CreatedUnix: 100},
		},
	}}
	w := newFakeWriter()
	r, dlq, cp := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 10}, src, w)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Indexed != 1 || stats.DLQ != 1 || stats.DLQPayload != 1 {
		t.Fatalf("real anomaly must be 1 indexed + 1 DLQ(payload): %+v", stats)
	}
	if dlq.Count() != 1 {
		t.Fatalf("dlq spill count=%d", dlq.Count())
	}
	if cp.Get("message") != 2 {
		t.Fatalf("checkpoint must advance past DLQ'd row too: %d", cp.Get("message"))
	}
	assertDLQContains(t, dlq.Path(), "bad")
}

// TestRunner_PermanentESRejectToDLQ ES 永久拒绝(4xx) → 落 DLQ spill，不卡批。
func TestRunner_PermanentESRejectToDLQ(t *testing.T) {
	src := &fakeSource{rows: map[string][]*srcMessageRow{
		"message": {textRow(1, "ok", "hi", 100), textRow(2, "poison", "boom", 100)},
	}}
	w := newFakeWriter()
	w.permanent["poison"] = struct{}{}
	r, dlq, _ := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 10}, src, w)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Indexed != 1 || stats.DLQ != 1 || stats.DLQPermanent != 1 {
		t.Fatalf("permanent ES reject must go to DLQ: %+v", stats)
	}
	if dlq.Count() != 1 {
		t.Fatalf("dlq count=%d", dlq.Count())
	}
}

// TestRunner_PermanentRejectRecordKeepsSourcePKAndPayload 🔴 P2-1：永久拒绝的 DLQ 记录须保留
// 源 PK（table/id）+ 原始 payload，便于排查 / 回灌。
func TestRunner_PermanentRejectRecordKeepsSourcePKAndPayload(t *testing.T) {
	poison := textRow(42, "poison", "boom-body", 100)
	src := &fakeSource{rows: map[string][]*srcMessageRow{"message": {poison}}}
	w := newFakeWriter()
	w.permanent["poison"] = struct{}{}
	r, dlq, _ := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 10}, src, w)
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	recs := readDLQRecords(t, dlq.Path())
	if len(recs) != 1 {
		t.Fatalf("want 1 dlq record, got %d", len(recs))
	}
	rec := recs[0]
	if rec.Table != "message" || rec.ID != 42 || rec.MessageID != "poison" {
		t.Fatalf("permanent-reject record must keep source PK: %+v", rec)
	}
	if string(rec.Payload) != string(poison.Payload) {
		t.Fatalf("permanent-reject record must keep original payload: got %q want %q", rec.Payload, poison.Payload)
	}
	if rec.CreatedAt != 100 {
		t.Fatalf("permanent-reject record must keep created_at, got %d", rec.CreatedAt)
	}
}

// TestRunner_EmptyMessageIDNoDedupCollapse 🔴 P2-1：空 message_id 的多条 DLQ 行用 table:id 兜底
// 去重键，不被静默合并 / 计数偏低。
func TestRunner_EmptyMessageIDNoDedupCollapse(t *testing.T) {
	// 两条空 message_id 的真异常（不同 id），不得塌缩成一条。
	src := &fakeSource{rows: map[string][]*srcMessageRow{
		"message": {
			{ID: 1, MessageID: "", Payload: []byte("{bad-1"), CreatedUnix: 100},
			{ID: 2, MessageID: "", Payload: []byte("{bad-2"), CreatedUnix: 100},
		},
	}}
	w := newFakeWriter()
	r, dlq, _ := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 10}, src, w)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.DLQ != 2 || dlq.Count() != 2 {
		t.Fatalf("empty message_id rows must NOT collapse: stats.DLQ=%d count=%d want 2", stats.DLQ, dlq.Count())
	}
}
func TestRunner_TransientRetriesInPlace(t *testing.T) {
	src := &fakeSource{rows: map[string][]*srcMessageRow{
		"message": {textRow(1, "a", "x", 100), textRow(2, "slow", "y", 100), textRow(3, "c", "z", 100)},
	}}
	w := newFakeWriter()
	w.transient["slow"] = 2 // 头两次 429，第三次 OK
	r, dlq, _ := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 10, TransientBackoff: time.Millisecond}, src, w)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Indexed != 3 || stats.DLQ != 0 {
		t.Fatalf("transient must eventually fully index: %+v", stats)
	}
	if w.uniqueIndexed() != 3 {
		t.Fatalf("no duplicate docs after retry: %d", w.uniqueIndexed())
	}
	if dlq.Count() != 0 {
		t.Fatalf("transient must NOT go to DLQ")
	}
}

// TestRunner_BatchLevelTransientRetried 批级错误（网络）→ 整批退避重试（幂等去重），最终成功。
func TestRunner_BatchLevelTransientRetried(t *testing.T) {
	src := &fakeSource{rows: map[string][]*srcMessageRow{
		"message": {textRow(1, "a", "x", 100), textRow(2, "b", "y", 100)},
	}}
	w := newFakeWriter()
	w.bulkErr = errors.New("connection refused") // 第一次批级失败，自愈
	r, _, _ := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 10, TransientBackoff: time.Millisecond}, src, w)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Indexed != 2 {
		t.Fatalf("batch-level transient must retry to success: %+v", stats)
	}
}

// TestRunner_Resume checkpoint 续传：第二次运行从高水位起，不重读已处理行。
func TestRunner_Resume(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp.json")
	rows := []*srcMessageRow{textRow(1, "a", "x", 100), textRow(2, "b", "y", 100), textRow(3, "c", "z", 100)}
	src := &fakeSource{rows: map[string][]*srcMessageRow{"message": rows}}

	// 第一次：只跑前 2 行（用预置 checkpoint 模拟「上次跑到 id=2」）。
	cp1, err := OpenCheckpoint(cpPath)
	if err != nil {
		t.Fatalf("open cp1: %v", err)
	}
	if err := cp1.Advance("message", 2); err != nil {
		t.Fatalf("seed cp: %v", err)
	}

	// 第二次：新 runner 载入 checkpoint，应只读 id>2（即 c）。
	dlq, err := OpenDLQSpill(filepath.Join(dir, "dlq"))
	if err != nil {
		t.Fatalf("open dlq: %v", err)
	}
	cp2, err := OpenCheckpoint(cpPath)
	if err != nil {
		t.Fatalf("reopen cp: %v", err)
	}
	if cp2.Get("message") != 2 {
		t.Fatalf("checkpoint not persisted: %d", cp2.Get("message"))
	}
	w := newFakeWriter()
	r := NewRunner(Config{Tables: []string{"message"}, BatchSize: 10}, src, w, cp2, dlq)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Read != 1 || stats.Indexed != 1 {
		t.Fatalf("resume must process only the 1 remaining row: %+v", stats)
	}
	if w.uniqueIndexed() != 1 {
		t.Fatalf("resume re-read already-done rows: %d", w.uniqueIndexed())
	}
}

// TestRunner_MultiTable 多分表各自 keyset 推进，汇总统计正确。
func TestRunner_MultiTable(t *testing.T) {
	src := &fakeSource{rows: map[string][]*srcMessageRow{
		"message":  {textRow(1, "a", "x", 100)},
		"message1": {textRow(1, "b", "y", 100), textRow(2, "c", "z", 100)},
	}}
	w := newFakeWriter()
	r, _, cp := newTestRunner(t, Config{Tables: []string{"message", "message1"}, BatchSize: 10}, src, w)
	stats, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if stats.Read != 3 || stats.Indexed != 3 {
		t.Fatalf("multi-table stats=%+v", stats)
	}
	if cp.Get("message") != 1 || cp.Get("message1") != 2 {
		t.Fatalf("per-table checkpoints wrong: %d %d", cp.Get("message"), cp.Get("message1"))
	}
}

// TestRunner_ReadErrorStops 源读错误 → 立即 STOP 返回错误。
func TestRunner_ReadErrorStops(t *testing.T) {
	src := &fakeSource{readErr: errors.New("db gone")}
	w := newFakeWriter()
	r, _, _ := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 10}, src, w)
	if _, err := r.Run(context.Background()); err == nil {
		t.Fatalf("read error must propagate as STOP")
	}
}

// TestRunner_EnsureIndexErrorStops EnsureIndex 失败 → 不读不写，直接 STOP。
func TestRunner_EnsureIndexErrorStops(t *testing.T) {
	src := &fakeSource{rows: map[string][]*srcMessageRow{"message": {textRow(1, "a", "x", 100)}}}
	w := newFakeWriter()
	w.ensureErr = errors.New("es down")
	r, _, _ := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 10}, src, w)
	if _, err := r.Run(context.Background()); err == nil {
		t.Fatalf("ensure-index error must STOP before reading")
	}
	if src.readCall != 0 {
		t.Fatalf("must not read source when ensure-index failed")
	}
}

// TestRunner_CtxCancelStops ctx 取消（SIGTERM）→ 尽快返回 ctx.Err()。
func TestRunner_CtxCancelStops(t *testing.T) {
	rows := make([]*srcMessageRow, 0, 100)
	for i := int64(1); i <= 100; i++ {
		rows = append(rows, textRow(i, fmt.Sprintf("m%d", i), "x", 100))
	}
	src := &fakeSource{rows: map[string][]*srcMessageRow{"message": rows}}
	w := newFakeWriter()
	r, _, _ := newTestRunner(t, Config{Tables: []string{"message"}, BatchSize: 1, DocsPerSec: 0}, src, w)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消
	if _, err := r.Run(ctx); err == nil {
		t.Fatalf("cancelled ctx must stop with error")
	}
}

// readDLQRecords 读出 spill 文件全部记录（测试断言用）。
func readDLQRecords(t *testing.T, path string) []dlqRecord {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // 测试路径
	if err != nil {
		t.Fatalf("read spill: %v", err)
	}
	var recs []dlqRecord
	for _, line := range splitLines(string(data)) {
		if line == "" {
			continue
		}
		var rec dlqRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("bad spill line %q: %v", line, err)
		}
		recs = append(recs, rec)
	}
	return recs
}

// assertDLQContains 断言 spill 文件含某 message_id。
func assertDLQContains(t *testing.T, path, mid string) {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // 测试路径
	if err != nil {
		t.Fatalf("read spill: %v", err)
	}
	var found bool
	for _, line := range splitLines(string(data)) {
		if line == "" {
			continue
		}
		var rec dlqRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("bad spill line %q: %v", line, err)
		}
		if rec.MessageID == mid {
			found = true
		}
	}
	if !found {
		t.Fatalf("spill file must contain message_id %q", mid)
	}
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// TestRunner_ResumeAfterDLQNoDoubleCount 模拟 codex P2-idempotency：第一次 run 把一条真异常
// 落 DLQ 但批未推进 checkpoint（崩在 Advance 前）；第二次 run 用同一 spill 目录 + 空 checkpoint
// 重读同一行 → DLQ 计数不得翻倍（去重 + replay 兜住）。
func TestRunner_ResumeAfterDLQNoDoubleCount(t *testing.T) {
	dir := t.TempDir()
	spillDir := filepath.Join(dir, "dlq")
	rows := []*srcMessageRow{
		textRow(1, "ok", "hi", 100),
		{ID: 2, MessageID: "bad", Payload: []byte("{not json"), CreatedUnix: 100},
	}
	src := &fakeSource{rows: map[string][]*srcMessageRow{"message": rows}}

	// Run #1：用独立 checkpoint（cp1），处理后 DLQ 有 1 条。
	dlq1, err := OpenDLQSpill(spillDir)
	if err != nil {
		t.Fatalf("open dlq1: %v", err)
	}
	cp1, err := OpenCheckpoint(filepath.Join(dir, "cp1.json"))
	if err != nil {
		t.Fatalf("open cp1: %v", err)
	}
	w1 := newFakeWriter()
	r1 := NewRunner(Config{Tables: []string{"message"}, BatchSize: 10}, src, w1, cp1, dlq1)
	r1.sleep = func(context.Context, time.Duration) error { return nil }
	if _, err := r1.Run(context.Background()); err != nil {
		t.Fatalf("run1: %v", err)
	}
	if dlq1.Count() != 1 {
		t.Fatalf("run1 dlq=%d want 1", dlq1.Count())
	}
	if cerr := dlq1.Close(); cerr != nil {
		t.Fatalf("close dlq1: %v", cerr)
	}

	// Run #2：模拟「崩在 Advance 前」→ 用**空** checkpoint(cp2) 重读全部行，但**同一** spill 目录。
	dlq2, err := OpenDLQSpill(spillDir) // replay 既有记录
	if err != nil {
		t.Fatalf("open dlq2: %v", err)
	}
	defer mustCloseT(t, dlq2)
	if dlq2.Count() != 1 {
		t.Fatalf("reopen must replay to count=1, got %d", dlq2.Count())
	}
	cp2, err := OpenCheckpoint(filepath.Join(dir, "cp2.json"))
	if err != nil {
		t.Fatalf("open cp2: %v", err)
	}
	w2 := newFakeWriter()
	r2 := NewRunner(Config{Tables: []string{"message"}, BatchSize: 10}, src, w2, cp2, dlq2)
	r2.sleep = func(context.Context, time.Duration) error { return nil }
	if _, err := r2.Run(context.Background()); err != nil {
		t.Fatalf("run2: %v", err)
	}
	// 关键断言：重读同一坏行后 DLQ 仍是 1（幂等去重），不翻倍。
	if dlq2.Count() != 1 {
		t.Fatalf("re-reading the same bad row must NOT inflate DLQ; got %d want 1", dlq2.Count())
	}
}
