package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// --- fakes ---

// fakeSource 按预置序列吐消息；Commit 记录已提交的最大 offset。
type fakeSource struct {
	msgs        []fetchedMessage
	idx         int
	committed   int64
	commitCalls int
	hasCommit   bool
}

func (s *fakeSource) Fetch(ctx context.Context) (fetchedMessage, error) {
	if s.idx >= len(s.msgs) {
		// 没有更多消息：阻塞到 ctx 取消（模拟 kafka 空闲）。
		<-ctx.Done()
		return fetchedMessage{}, ctx.Err()
	}
	m := s.msgs[s.idx]
	s.idx++
	return m, nil
}

func (s *fakeSource) Commit(ctx context.Context, msg fetchedMessage) error {
	s.commitCalls++
	s.committed = msg.Offset
	s.hasCommit = true
	return nil
}

func (s *fakeSource) Close() error { return nil }

// fakeWriter 按 message_id → status 预置 bulk 结果。
type fakeWriter struct {
	statusByID map[string]int // message_id → HTTP status（默认 201）
	batchErr   error          // 非 nil 时整批 transient 失败
	// healAfter：对 message_id，从第 healAfter 次 bulk 调用起改判为成功（模拟 transient 自愈，
	// 让原地重试循环能收敛）。0 表示不自愈（持续按 statusByID）。
	healAfter map[string]int
	bulkCalls int
	lastBulk  []searchmsg.Message // 最近一次 Bulk 调用的入参（断言 visibility 回填用）
	mu        sync.Mutex
}

func (w *fakeWriter) Bulk(ctx context.Context, msgs []searchmsg.Message) ([]esindex.BulkItemResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.bulkCalls++
	w.lastBulk = msgs
	out := make([]esindex.BulkItemResult, len(msgs))
	for i := range msgs {
		if w.batchErr != nil {
			out[i] = esindex.BulkItemResult{MessageID: msgs[i].MessageID, OK: false, Status: 0, Err: w.batchErr}
			continue
		}
		st := 201
		if w.statusByID != nil {
			if s, ok := w.statusByID[msgs[i].MessageID]; ok {
				st = s
			}
		}
		if w.healAfter != nil {
			if h, ok := w.healAfter[msgs[i].MessageID]; ok && w.bulkCalls >= h {
				st = 201 // 自愈：transient 在第 h 次调用后转成功
			}
		}
		res := esindex.BulkItemResult{MessageID: msgs[i].MessageID, Status: st}
		if st >= 200 && st < 300 {
			res.OK = true
		} else {
			res.Err = errors.New("bulk item failed")
		}
		out[i] = res
	}
	if w.batchErr != nil {
		return out, w.batchErr
	}
	return out, nil
}

func (w *fakeWriter) Close() error { return nil }

func (w *fakeWriter) EnsureIndex(ctx context.Context) error { return nil }

func (w *fakeWriter) AssertLiveMappingCompatible(ctx context.Context) error { return nil }

// BulkDocs 不被 consumer 路径调用（consumer 走 Bulk(msgs)）；提供空实现满足接口。
func (w *fakeWriter) BulkDocs(ctx context.Context, docs []esindex.Doc) ([]esindex.BulkItemResult, error) {
	return nil, nil
}

// fakeDLQSink 记录 DLQ 投递；可设定前 N 次失败。
type fakeDLQSink struct {
	writes     int
	failFor    int // 前 failFor 次返回错误（模拟 DLQ broker transient 失败）
	alwaysFail bool
	records    [][]byte
	mu         sync.Mutex
}

func (d *fakeDLQSink) WriteDLQ(ctx context.Context, key, value []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writes++
	if d.alwaysFail || d.writes <= d.failFor {
		return errors.New("dlq broker unavailable")
	}
	d.records = append(d.records, value)
	return nil
}

// recordAlerter 记录告警事件。
type recordAlerter struct {
	mu     sync.Mutex
	events []string
}

func (a *recordAlerter) Alert(event, detail string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, event)
}

func (a *recordAlerter) has(event string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, e := range a.events {
		if e == event {
			return true
		}
	}
	return false
}

// --- helpers ---

// mustJSON marshals or fails the test (keeps errcheck happy in helpers).
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func validMsg(id string) []byte {
	c := "正文-" + id
	return mustJSON(searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     id,
		ChannelID:     "g_1",
		ChannelType:   2,
		FromUID:       "u_1",
		Content:       &c,
		ContentType:   1,
		Source:        searchmsg.SourceETLMessageTable,
	})
}

func badSchemaMsg(id string, ver int) []byte {
	return mustJSON(searchmsg.Message{SchemaVersion: ver, MessageID: id})
}

func fm(offset int64, value []byte) fetchedMessage {
	return fetchedMessage{Topic: "octo.message.v1", Partition: 0, Offset: offset, Key: []byte("k"), Value: value}
}

func newProc(t *testing.T, src messageSource, w esindex.Writer, sink dlqSink, alert alerter, spillDir string) *Processor {
	t.Helper()
	dcfg := dlqConfig{MaxRetries: 2, RetryBackoff: time.Millisecond, SpillDir: spillDir}
	dlq := newDLQHandler(sink, alert, dcfg)
	dlq.sleep = func(context.Context, time.Duration) error { return nil }
	p := NewProcessor(src, w, dlq, alert, Config{BatchSize: 10, TransientBackoff: time.Millisecond})
	p.sleep = func(context.Context, time.Duration) error { return nil }
	return p
}

// --- tests ---

// TestProcessBatch_AllSuccessCommitsAll 全成功 → commit 到最后一条。
func TestProcessBatch_AllSuccessCommitsAll(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	batch := []fetchedMessage{fm(0, validMsg("a")), fm(1, validMsg("b")), fm(2, validMsg("c"))}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if !src.hasCommit || src.committed != 2 {
		t.Fatalf("expected commit at offset 2, got committed=%d hasCommit=%v", src.committed, src.hasCommit)
	}
	if sink.writes != 0 {
		t.Fatalf("no DLQ writes expected, got %d", sink.writes)
	}
}

// TestProcessBatch_UnknownSchemaToDLQ 未知 schema_version → 进 DLQ，不写 ES，offset 越过。
func TestProcessBatch_UnknownSchemaToDLQ(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	batch := []fetchedMessage{fm(0, validMsg("a")), fm(1, badSchemaMsg("b", 999)), fm(2, validMsg("c"))}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	// b 进 DLQ；a、c 写 ES；全连续 → commit 到 2。
	if src.committed != 2 {
		t.Fatalf("expected commit at 2, got %d", src.committed)
	}
	if sink.writes != 1 {
		t.Fatalf("expected exactly 1 DLQ write (unknown schema), got %d", sink.writes)
	}
	// 校验 ES 只写了合法的 a、c（不含 b）。
	if w.bulkCalls != 1 {
		t.Fatalf("expected 1 bulk call, got %d", w.bulkCalls)
	}
}

// TestProcessBatch_Permanent4xxToDLQOffsetCrosses 中间某条 4xx → 进 DLQ → offset 越过 → 后续继续。
func TestProcessBatch_Permanent4xxToDLQOffsetCrosses(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{statusByID: map[string]int{"b": 400}}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	batch := []fetchedMessage{fm(0, validMsg("a")), fm(1, validMsg("b")), fm(2, validMsg("c"))}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if src.committed != 2 {
		t.Fatalf("expected commit at 2 (4xx DLQ'd, crossed), got %d", src.committed)
	}
	if sink.writes != 1 {
		t.Fatalf("expected 1 DLQ write, got %d", sink.writes)
	}
}

// TestProcessBatch_TransientRetriesInPlaceThenCommits 🔴 C4：transient(503) 原地重试同一批，
// 不拉新 offset；自愈后整批成功 → commit 到末。期间绝不越过未确认的 transient。
func TestProcessBatch_TransientRetriesInPlaceThenCommits(t *testing.T) {
	src := &fakeSource{}
	// b 在第 2 次 bulk 调用后自愈（首轮 503，重试转 201）。
	w := &fakeWriter{statusByID: map[string]int{"b": 503}, healAfter: map[string]int{"b": 2}}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	batch := []fetchedMessage{fm(0, validMsg("a")), fm(1, validMsg("b")), fm(2, validMsg("c"))}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	// 自愈后全部成功 → commit 到 2。
	if src.committed != 2 {
		t.Fatalf("expected commit at 2 after transient heals, got committed=%d", src.committed)
	}
	if w.bulkCalls < 2 {
		t.Fatalf("expected in-place retry (>=2 bulk calls), got %d", w.bulkCalls)
	}
	if sink.writes != 0 {
		t.Fatalf("transient must NOT go to DLQ, got %d writes", sink.writes)
	}
}

// TestProcessBatch_PersistentTransientNoCommitNoCross 🔴 C4：持续 transient(503) 不自愈 →
// processBatch 一直原地重试、永不 commit 越过未确认条（直到 ctx 取消）。验证不丢消息。
func TestProcessBatch_PersistentTransientNoCommitNoCross(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{statusByID: map[string]int{"b": 503}} // 不自愈
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	batch := []fetchedMessage{fm(0, validMsg("a")), fm(1, validMsg("b")), fm(2, validMsg("c"))}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	err := p.processBatch(ctx, batch)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ctx error after persistent transient, got %v", err)
	}
	// a 成功 → 可 commit 到 0；但 b transient 截断前缀 → 绝不 commit 到 ≥1。
	if src.committed != 0 {
		t.Fatalf("must commit only confirmed prefix (offset 0), never cross transient b; got committed=%d", src.committed)
	}
	if sink.writes != 0 {
		t.Fatalf("transient must NOT go to DLQ, got %d", sink.writes)
	}
}

// TestProcessBatch_429IsTransient 429 限流视为 transient（不进 DLQ，不越过；持续则原地重试到 ctx 取消）。
func TestProcessBatch_429IsTransient(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{statusByID: map[string]int{"a": 429}} // 队首即 429，不自愈
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	batch := []fetchedMessage{fm(0, validMsg("a")), fm(1, validMsg("b"))}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	err := p.processBatch(ctx, batch)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ctx error, got %v", err)
	}
	if src.hasCommit {
		t.Fatalf("429 at head → no commit, got committed=%d", src.committed)
	}
	if sink.writes != 0 {
		t.Fatalf("429 must not go to DLQ")
	}
}

// TestProcessBatch_BatchFailureNoCommit bulk 批级失败（网络）→ 全 transient，原地重试，无 commit/无 DLQ。
func TestProcessBatch_BatchFailureNoCommit(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{batchErr: errors.New("network down")}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	batch := []fetchedMessage{fm(0, validMsg("a")), fm(1, validMsg("b"))}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	err := p.processBatch(ctx, batch)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ctx error, got %v", err)
	}
	if src.hasCommit {
		t.Fatalf("batch failure → no commit")
	}
	if sink.writes != 0 {
		t.Fatalf("batch failure → no DLQ")
	}
}

// TestProcessBatch_DLQRetryThenSucceed DLQ 写前 N 次 transient 失败 → 有界重试后成功 → offset 越过。
func TestProcessBatch_DLQRetryThenSucceed(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{statusByID: map[string]int{"a": 400}}
	sink := &fakeDLQSink{failFor: 2} // 前 2 次失败，第 3 次成功（MaxRetries=2 → attempts 0,1,2）
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	batch := []fetchedMessage{fm(0, validMsg("a")), fm(1, validMsg("b"))}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if src.committed != 1 {
		t.Fatalf("expected commit at 1 after DLQ retry succeeds, got %d", src.committed)
	}
	if sink.writes != 3 {
		t.Fatalf("expected 3 DLQ attempts (2 fail + 1 ok), got %d", sink.writes)
	}
}

// TestProcessBatch_DLQEscapeSpill 🔴 C4 终态逃逸：DLQ 持续失败 + 配 spill → 落地 + 告警 + offset 越过（不死锁）。
func TestProcessBatch_DLQEscapeSpill(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{statusByID: map[string]int{"a": 400}}
	sink := &fakeDLQSink{alwaysFail: true}
	alert := &recordAlerter{}
	spillDir := t.TempDir()
	p := newProc(t, src, w, sink, alert, spillDir)
	batch := []fetchedMessage{fm(0, validMsg("a")), fm(1, validMsg("b"))}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	// spill 逃逸 → a 视为已处理，b 成功 → commit 到 1。
	if src.committed != 1 {
		t.Fatalf("expected commit at 1 after spill escape, got committed=%d hasCommit=%v", src.committed, src.hasCommit)
	}
	if !alert.has("dlq_spilled_to_disk") {
		t.Fatalf("expected dlq_spilled_to_disk alert")
	}
}

// TestProcessBatch_DLQHardStopNoSpill 🔴 C4：DLQ 持续失败 + 未配 spill → 硬停返回 fatal error
// （offset 不越过毒丸，停 worker + page），绝不静默越过或死锁。
func TestProcessBatch_DLQHardStopNoSpill(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{statusByID: map[string]int{"a": 400}}
	sink := &fakeDLQSink{alwaysFail: true}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "") // 无 spill → 硬停
	batch := []fetchedMessage{fm(0, validMsg("a")), fm(1, validMsg("b"))}
	err := p.processBatch(context.Background(), batch)
	if !errors.Is(err, errDLQHardStop) {
		t.Fatalf("hard stop must return fatal errDLQHardStop, got %v", err)
	}
	// 毒丸在队首 → 无连续前缀可 commit；不静默丢，等人工。
	if src.hasCommit {
		t.Fatalf("hard stop → must not commit past poison pill, got committed=%d", src.committed)
	}
	if !alert.has("dlq_hard_stop") {
		t.Fatalf("expected dlq_hard_stop alert")
	}
}

// TestRun_CtxCancelStops Run 在 ctx 取消时干净返回。
func TestRun_CtxCancelStops(t *testing.T) {
	src := &fakeSource{msgs: []fetchedMessage{fm(0, validMsg("a"))}}
	w := &fakeWriter{}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := p.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ctx error, got %v", err)
	}
	// 处理完唯一消息后应已 commit 到 0。
	if src.committed != 0 || !src.hasCommit {
		t.Fatalf("expected commit at 0 before idle, got committed=%d hasCommit=%v", src.committed, src.hasCommit)
	}
}
