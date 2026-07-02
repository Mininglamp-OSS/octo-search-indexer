package fileextract

// retry_test.go — Blocker #2 修复回归 test（v1.13）。
// 目标：验证 in-place bounded retry state machine 正确性：
//   1. transient (errDocNotYet / errOSTransient) 触发重试直到 OK 或达上限 DLQ
//   2. offset 只在"该消息达终态"后按连续前缀 commit（不跨越 transient）
//   3. 429 (P2-1) 归 transient，不误判 permanent
//   4. errOSPermanent (P2-2) 立即 DLQ ReasonOSPermanent，不重试
//   5. 多分区 offset prefix 按分区独立推进（fix-plan §8 #1）
//   6. 退避可 ctx-cancel（SIGTERM 立即返）

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// mockExtractor 是 extractorService 的测试假实现。
// 按 messageID 匹配预置的 outcomes 序列，逐次消费；用尽则返回默认。
type mockExtractor struct {
	mu sync.Mutex
	// outcomes[msgID] = 一序列每次返回值（(reason, cause, err)）
	outcomes    map[string][]extractResult
	calls       map[string]int // 每 msgID 被调多少次
	defaultRes  extractResult
}

type extractResult struct {
	reason string
	cause  error
	err    error
}

func newMockExtractor() *mockExtractor {
	return &mockExtractor{
		outcomes:   make(map[string][]extractResult),
		calls:      make(map[string]int),
		defaultRes: extractResult{}, // 默认 OK
	}
}

func (m *mockExtractor) queue(msgID string, results ...extractResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outcomes[msgID] = append(m.outcomes[msgID], results...)
}

func (m *mockExtractor) ExtractAndWrite(ctx context.Context, messageID string, fp *filePayload) (string, error, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls[messageID]++
	seq := m.outcomes[messageID]
	if len(seq) == 0 {
		return m.defaultRes.reason, m.defaultRes.cause, m.defaultRes.err
	}
	r := seq[0]
	m.outcomes[messageID] = seq[1:]
	return r.reason, r.cause, r.err
}

// mkFileMessage 造一条 file 消息（type=8），用给定 msgID + partition + offset。
func mkFileMessage(t *testing.T, msgID string, partition int, offset int64) fetchedMessage {
	t.Helper()
	rawPayload := map[string]any{
		"type":      8,
		"url":       "https://cdn.deepminer.com.cn/test.pdf",
		"name":      "test.pdf",
		"extension": ".pdf",
		"size":      2048,
	}
	rawBytes, err := json.Marshal(rawPayload)
	if err != nil {
		t.Fatalf("marshal rawPayload: %v", err)
	}
	msg := searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     msgID,
		RawPayload:    rawBytes,
	}
	value, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal msg: %v", err)
	}
	return fetchedMessage{
		Topic:     "test-topic",
		Partition: partition,
		Offset:    offset,
		Key:       []byte(msgID),
		Value:     value,
	}
}

// newRetryTestProcessor 构造 test-only Processor（sleep 换成 no-op 加速）。
func newRetryTestProcessor(t *testing.T, src messageSource, dlq dlqSink, ext extractorService, maxRetries int) *Processor {
	t.Helper()
	cfg := ServiceConfig{
		MaxRetriesPerMessage: maxRetries,
		TransientBackoffBase: time.Nanosecond, // 让退避几乎 0，加速 test
		TransientBackoffMax:  time.Microsecond,
	}
	p := NewProcessor(src, dlq, ext, cfg)
	// 覆盖 sleep 为 no-op（避免真实 sleep 拖慢 test）
	p.sleep = func(ctx context.Context, d time.Duration) error {
		return ctx.Err()
	}
	return p
}

// TestProcessBatch_TransientRetryThenSuccess 前 2 次 attemptOne 返 transient，第 3 次 OK →
// commit 1 次（成功那次的 offset），attempts=3。
func TestProcessBatch_TransientRetryThenSuccess(t *testing.T) {
	src := &mockSource{}
	dlq := &mockDLQSink{}
	ext := newMockExtractor()
	ext.queue("42",
		extractResult{err: errDocNotYet}, // attempt 1
		extractResult{err: errOSTransient}, // attempt 2
		extractResult{},                    // attempt 3 → OK
	)
	p := newRetryTestProcessor(t, src, dlq, ext, 10)

	batch := []fetchedMessage{mkFileMessage(t, "42", 0, 100)}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if ext.calls["42"] != 3 {
		t.Fatalf("expected 3 extractor calls, got %d", ext.calls["42"])
	}
	if len(dlq.records) != 0 {
		t.Fatalf("expected 0 DLQ records, got %d: %+v", len(dlq.records), dlq.records)
	}
	if len(src.commits) != 1 || src.commits[0].Offset != 100 {
		t.Fatalf("expected exactly 1 commit at offset 100, got %+v", src.commits)
	}
}

// TestProcessBatch_RetryExhaustedToDLQ 10 次全 errDocNotYet → 第 11 次不再调 extractor，
// 强制 DLQ ReasonRetryExhausted，然后 commit offset。
func TestProcessBatch_RetryExhaustedToDLQ(t *testing.T) {
	src := &mockSource{}
	dlq := &mockDLQSink{}
	ext := newMockExtractor()
	// 每次都返 errDocNotYet
	ext.defaultRes = extractResult{err: errDocNotYet}
	p := newRetryTestProcessor(t, src, dlq, ext, 5) // 限 5 次加速

	batch := []fetchedMessage{mkFileMessage(t, "99", 0, 200)}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if ext.calls["99"] != 5 {
		t.Fatalf("expected 5 extractor calls (bounded), got %d", ext.calls["99"])
	}
	if len(dlq.records) != 1 || dlq.records[0].Reason != ReasonRetryExhausted {
		t.Fatalf("expected 1 DLQ retry_exhausted, got %+v", dlq.records)
	}
	if p.metrics.retryExhausted.Load() != 1 {
		t.Fatalf("retryExhausted counter should be 1, got %d", p.metrics.retryExhausted.Load())
	}
	if len(src.commits) != 1 || src.commits[0].Offset != 200 {
		t.Fatalf("offset must be committed after DLQ retry_exhausted, got %+v", src.commits)
	}
}

// TestProcessBatch_429IsTransient P2-1：OS 返 429 → errOSTransient → 走 retry 不是 DLQ。
// 用 wrap 一个 429-status err（模拟 classifyOSErr 输出）。
func TestProcessBatch_429IsTransient(t *testing.T) {
	src := &mockSource{}
	dlq := &mockDLQSink{}
	ext := newMockExtractor()
	// 前 1 次 wrap errOSTransient（模拟 classifyOSErr 判 429 后的错），第 2 次 OK
	err429 := fmt.Errorf("%w: status 429: too many requests", errOSTransient)
	ext.queue("t429",
		extractResult{err: err429},
		extractResult{},
	)
	p := newRetryTestProcessor(t, src, dlq, ext, 10)

	batch := []fetchedMessage{mkFileMessage(t, "t429", 0, 300)}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if ext.calls["t429"] != 2 {
		t.Fatalf("429 must retry once then OK; got %d calls", ext.calls["t429"])
	}
	if len(dlq.records) != 0 {
		t.Fatalf("429 must NOT DLQ (transient), got %+v", dlq.records)
	}
	if len(src.commits) != 1 || src.commits[0].Offset != 300 {
		t.Fatalf("expected commit at 300 after transient resolves, got %+v", src.commits)
	}
}

// TestProcessBatch_OSPermanentToDLQ P2-2：OS 返 400（非 404/409/429）→ errOSPermanent →
// 立即 DLQ ReasonOSPermanent，不 retry。
func TestProcessBatch_OSPermanentToDLQ(t *testing.T) {
	src := &mockSource{}
	dlq := &mockDLQSink{}
	ext := newMockExtractor()
	err400 := fmt.Errorf("%w: status 400: bad request", errOSPermanent)
	ext.queue("t400", extractResult{err: err400})
	p := newRetryTestProcessor(t, src, dlq, ext, 10)

	batch := []fetchedMessage{mkFileMessage(t, "t400", 0, 400)}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if ext.calls["t400"] != 1 {
		t.Fatalf("os_permanent must NOT retry; got %d calls (want 1)", ext.calls["t400"])
	}
	if len(dlq.records) != 1 || dlq.records[0].Reason != ReasonOSPermanent {
		t.Fatalf("expected 1 DLQ os_permanent, got %+v", dlq.records)
	}
	if p.metrics.osPermanent.Load() != 1 {
		t.Fatalf("osPermanent counter should be 1, got %d", p.metrics.osPermanent.Load())
	}
}

// TestProcessBatch_OffsetPrefixCommit_101FailsFirstAttempt 关键 Jerry-Xin scenario：
// offset 100/101/102，101 first attempt fails；102 must NOT be committed until 101 reaches terminal.
// 用只有 3 条一起的 batch 复现。
func TestProcessBatch_OffsetPrefixCommit_101FailsFirstAttempt(t *testing.T) {
	src := &mockSource{}
	dlq := &mockDLQSink{}
	ext := newMockExtractor()
	// 100 立即 OK
	ext.queue("100", extractResult{})
	// 101 前 2 次 transient，第 3 次 OK
	ext.queue("101",
		extractResult{err: errDocNotYet},
		extractResult{err: errDocNotYet},
		extractResult{},
	)
	// 102 立即 OK
	ext.queue("102", extractResult{})
	p := newRetryTestProcessor(t, src, dlq, ext, 10)

	batch := []fetchedMessage{
		mkFileMessage(t, "100", 0, 100),
		mkFileMessage(t, "101", 0, 101),
		mkFileMessage(t, "102", 0, 102),
	}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	// 验证最终 commit：应达到 offset 102（全部终态后按前缀推进）
	if len(src.commits) == 0 {
		t.Fatalf("expected at least 1 commit")
	}
	// 关键：**最后一次** commit 应到 102；且过程中不应出现"越过 101 直接提交 102"
	for i, c := range src.commits {
		// 每个提交点都必须 <= 该 partition 当前已到达 terminal 的最高连续 offset
		if c.Offset > 102 {
			t.Fatalf("commit[%d] offset=%d exceeds max input 102", i, c.Offset)
		}
	}
	// 找中间过程 commit：第一轮 100 OK + 102 OK，101 transient → 前缀应只到 100
	// 最终轮 101 OK → 前缀到 102
	finalCommit := src.commits[len(src.commits)-1]
	if finalCommit.Offset != 102 {
		t.Fatalf("final commit must be 102 (full prefix), got %d", finalCommit.Offset)
	}
	// 中间不应有 "> 100 但 < 102" 的 commit（101 未达终态时不能越过）
	for i, c := range src.commits[:len(src.commits)-1] {
		if c.Offset >= 101 && c.Offset < 102 {
			t.Fatalf("commit[%d]=%d must not jump past pending 101 (was: %+v)", i, c.Offset, src.commits)
		}
	}
}

// TestProcessBatch_MultiPartitionPrefixIndependent 多分区 offset 应独立推进（fix-plan §8 #1）。
// 分区 0：offset 100 (transient forever)、101 (OK)；
// 分区 1：offset 200 (OK)、201 (OK)。
// 期望：分区 1 前缀推进到 201；分区 0 停在无提交（100 未终态直到达 retry 上限）。
func TestProcessBatch_MultiPartitionPrefixIndependent(t *testing.T) {
	src := &mockSource{}
	dlq := &mockDLQSink{}
	ext := newMockExtractor()
	// p0/100 一直 transient（模拟 doc-not-yet 永久缺）；上限触发 DLQ
	ext.defaultRes = extractResult{err: errDocNotYet}
	// 但为 p0/101, p1/200, p1/201 预置 OK 覆盖 default
	ext.queue("m101", extractResult{})
	ext.queue("m200", extractResult{})
	ext.queue("m201", extractResult{})
	p := newRetryTestProcessor(t, src, dlq, ext, 3) // p0/m100 只 retry 3 次触发 DLQ

	batch := []fetchedMessage{
		mkFileMessage(t, "m100", 0, 100),
		mkFileMessage(t, "m101", 0, 101),
		mkFileMessage(t, "m200", 1, 200),
		mkFileMessage(t, "m201", 1, 201),
	}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	// 分区 1 应推进到 201（两条都 OK）
	// 分区 0 触发 retry_exhausted DLQ 后 100 也标 dispDLQResolved，然后前缀推进到 101
	// 最终两个分区都完全推进
	if len(dlq.records) != 1 || dlq.records[0].Reason != ReasonRetryExhausted {
		t.Fatalf("expected 1 DLQ retry_exhausted for m100, got %+v", dlq.records)
	}
	// 收集每分区最后 commit offset
	lastByPart := make(map[int]int64)
	for _, c := range src.commits {
		if o, ok := lastByPart[c.Partition]; !ok || c.Offset > o {
			lastByPart[c.Partition] = c.Offset
		}
	}
	if lastByPart[0] != 101 {
		t.Fatalf("partition 0 final commit should be 101 (after m100 DLQ), got %d", lastByPart[0])
	}
	if lastByPart[1] != 201 {
		t.Fatalf("partition 1 final commit should be 201, got %d", lastByPart[1])
	}
	// 关键：分区 1 的 commit 出现应**早于**分区 0 的 m100 达终态（独立推进）
	// 检验方法：src.commits 里第一次出现分区 1 offset 201 的时机应在分区 0 的 101 commit 之前
	// 因为 m101/m200/m201 都是立刻 OK，第一轮就应该 commit p1=201；p0/100 要 3 轮 retry 才终态
	firstP1_201 := -1
	firstP0_101 := -1
	for i, c := range src.commits {
		if c.Partition == 1 && c.Offset == 201 && firstP1_201 == -1 {
			firstP1_201 = i
		}
		if c.Partition == 0 && c.Offset == 101 && firstP0_101 == -1 {
			firstP0_101 = i
		}
	}
	if firstP1_201 < 0 {
		t.Fatalf("partition 1 never committed to 201: %+v", src.commits)
	}
	if firstP0_101 >= 0 && firstP1_201 > firstP0_101 {
		t.Fatalf("partition 1 should commit earlier than partition 0 (independent prefix); p1_201_at=%d p0_101_at=%d", firstP1_201, firstP0_101)
	}
}

// TestProcessBatch_DLQWriteFailsFatal DLQ 写自身失败 → 硬停返 err（Run 停 worker，保 offset 未推进）。
func TestProcessBatch_DLQWriteFailsFatal(t *testing.T) {
	src := &mockSource{}
	dlq := &mockDLQSink{writeErr: errors.New("kafka DLQ down")}
	ext := newMockExtractor()
	// parse_error 触发 DLQ，DLQ 写失败 → outcomeFatal
	p := newRetryTestProcessor(t, src, dlq, ext, 10)

	batch := []fetchedMessage{{
		Topic: "t", Partition: 0, Offset: 500,
		Key: []byte("bad"), Value: []byte("not json"),
	}}
	err := p.processBatch(context.Background(), batch)
	if err == nil {
		t.Fatalf("expected fatal error when DLQ write fails")
	}
	if len(src.commits) != 0 {
		t.Fatalf("no commit on DLQ hard-stop, got %+v", src.commits)
	}
}

// TestProcessBatch_CtxCancelDuringBackoff 退避时 ctx cancel → 立即返 nil，无泄漏。
func TestProcessBatch_CtxCancelDuringBackoff(t *testing.T) {
	src := &mockSource{}
	dlq := &mockDLQSink{}
	ext := newMockExtractor()
	ext.defaultRes = extractResult{err: errDocNotYet} // 一直 transient 迫使 retry
	p := newRetryTestProcessor(t, src, dlq, ext, 100)
	// 覆盖 sleep 为"直接返 ctx.Err()"模拟 SIGTERM
	p.sleep = func(ctx context.Context, d time.Duration) error {
		return context.Canceled
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消，让 sleep 首次调用就返
	batch := []fetchedMessage{mkFileMessage(t, "x", 0, 600)}
	err := p.processBatch(ctx, batch)
	if !errors.Is(err, context.Canceled) {
		// 首次 attempt 是 attempts[i]=0 → 不走 sleep 而是直接 attemptOne → err.
		// attempts[i]++ 后第 2 轮进入 sleep → 返 ctx.Canceled → processBatch return
		t.Fatalf("expected ctx.Canceled after backoff sleep cancel, got %v", err)
	}
}

// TestExpJitterBackoff_MonotoneAndCap 单元测试退避函数：值在 [0, cap] 内且指数递增。
func TestExpJitterBackoff_MonotoneAndCap(t *testing.T) {
	base := 10 * time.Millisecond
	maxD := 100 * time.Millisecond
	for attempt := 1; attempt <= 20; attempt++ {
		d := expJitterBackoff(base, maxD, attempt)
		if d < 0 || d > maxD {
			t.Fatalf("attempt=%d: backoff %v out of [0, %v]", attempt, d, maxD)
		}
	}
	// base<=0 → 0
	if d := expJitterBackoff(0, maxD, 5); d != 0 {
		t.Fatalf("base=0 must return 0, got %v", d)
	}
}

// TestSleepCtx_CtxCancelEarly ctx 取消时 sleepCtx 立即返 ctx.Err()（不等满 d）。
func TestSleepCtx_CtxCancelEarly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := sleepCtx(ctx, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("sleepCtx must return canceled err, got %v", err)
	}
}
