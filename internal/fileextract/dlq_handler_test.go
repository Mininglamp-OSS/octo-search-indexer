package fileextract

// dlq_handler_test.go — DLQ handler 有界重试 + spill 逃逸回归 test（v1.13 P2-1，
// yujiawei review 发现的 sibling consumer/dlq.go pattern 未 port 问题）。
//
// 覆盖场景（对齐 consumer/dlq_test.go）：
//   1. 首次成功
//   2. 前 N 次失败第 N+1 次成功（有界重试）
//   3. 全部失败 + 配 SpillDir → 落盘 + 告警 + 返回 nil（越过 offset）
//   4. 全部失败 + 未配 SpillDir → errDLQHardStop（硬停）
//   5. ctx 取消（SIGTERM）立即返 ctx.Err，不再重试

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeDLQSink 是 dlqSink 的 test 假实现。可配「首 N 次失败」/「一直失败」。
type fakeDLQSink struct {
	writes    int
	failFor   int  // 前 failFor 次返错，之后成功
	alwaysErr bool // true → 每次都返错
	err       error
}

func (f *fakeDLQSink) WriteDLQ(_ context.Context, _ []byte, _ []byte) error {
	f.writes++
	if f.alwaysErr {
		if f.err == nil {
			return errors.New("fake dlq sink always fail")
		}
		return f.err
	}
	if f.writes <= f.failFor {
		return errors.New("fake dlq sink transient fail")
	}
	return nil
}

func (f *fakeDLQSink) Close() error { return nil }

// recordAlerter 记录 Alert 事件序列供断言。
type recordAlerter struct {
	events []string // 每个元素形如 "event|detail"
}

func (r *recordAlerter) Alert(event, detail string) {
	r.events = append(r.events, event+"|"+detail)
}

// has 返回 event 是否在事件序列里出现过（前缀匹配 event 名）。
func (r *recordAlerter) has(event string) bool {
	for _, e := range r.events {
		if len(e) >= len(event) && e[:len(event)] == event {
			return true
		}
	}
	return false
}

// newTestDLQHandler 组装一个 test-only handler（sleep no-op，nowUnix 固定）。
func newTestDLQHandler(sink dlqSink, alert alerter, spillDir string) *dlqHandler {
	h := newDLQHandler(sink, alert, dlqHandlerConfig{
		MaxRetries:   2,
		RetryBackoff: time.Millisecond,
		SpillDir:     spillDir,
	})
	h.sleep = func(context.Context, time.Duration) error { return nil }
	h.nowUnix = func() int64 { return 1_700_000_000 }
	return h
}

func sampleDLQRec() dlqRecord {
	return dlqRecord{
		Reason:    ReasonOSPermanent,
		Topic:     "octo.message.v1.file-extract.dlq",
		Partition: 0,
		Offset:    42,
		Key:       []byte("msg-42"),
		Value:     []byte(`{"messageId":"42"}`),
		MessageID: "42",
	}
}

// TestDLQHandler_SuccessFirstTry 首次投递成功 → writes=1，无 alerts。
func TestDLQHandler_SuccessFirstTry(t *testing.T) {
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	h := newTestDLQHandler(sink, alert, "")
	if err := h.Send(context.Background(), sampleDLQRec()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if sink.writes != 1 {
		t.Errorf("expected 1 write, got %d", sink.writes)
	}
	if len(alert.events) != 0 {
		t.Errorf("no alerts expected on happy path, got %v", alert.events)
	}
}

// TestDLQHandler_RetryThenSucceed 前 2 次失败第 3 次成功 → writes=3。
func TestDLQHandler_RetryThenSucceed(t *testing.T) {
	sink := &fakeDLQSink{failFor: 2}
	alert := &recordAlerter{}
	h := newTestDLQHandler(sink, alert, "")
	if err := h.Send(context.Background(), sampleDLQRec()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if sink.writes != 3 {
		t.Errorf("expected 3 attempts (2 fail + 1 succeed), got %d", sink.writes)
	}
}

// TestDLQHandler_EscapeToSpill 全部失败 + 配 SpillDir → 落盘 + 告警 + 返回 nil。
// v1.13 P2-1 核心 pattern：DLQ topic 挂时保 partition 不永久卡死。
func TestDLQHandler_EscapeToSpill(t *testing.T) {
	sink := &fakeDLQSink{alwaysErr: true}
	alert := &recordAlerter{}
	dir := t.TempDir()
	h := newTestDLQHandler(sink, alert, dir)

	err := h.Send(context.Background(), sampleDLQRec())
	if err != nil {
		t.Fatalf("escape to spill must return nil (allow offset cross), got %v", err)
	}
	if !alert.has("dlq_write_exhausted") {
		t.Errorf("expected dlq_write_exhausted alert, got %v", alert.events)
	}
	if !alert.has("dlq_spilled_to_disk") {
		t.Errorf("expected dlq_spilled_to_disk alert, got %v", alert.events)
	}
	// spill dir 里应有一个文件 dlq-spill-<partition>-<offset>-<epoch>.json
	entries, rerr := os.ReadDir(dir)
	if rerr != nil {
		t.Fatalf("read spill dir: %v", rerr)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 spill file, got %d", len(entries))
	}
	wantName := "dlq-spill-0-42-1700000000.json"
	if entries[0].Name() != wantName {
		t.Errorf("spill file name: got %q want %q", entries[0].Name(), wantName)
	}
	// 文件内容应含 SpilledAt 字段
	data, rerr := os.ReadFile(filepath.Join(dir, wantName))
	if rerr != nil {
		t.Fatalf("read spill file: %v", rerr)
	}
	if !contains(data, `"spilledAt":1700000000`) {
		t.Errorf("spill file must carry SpilledAt=1700000000, got %s", data)
	}
}

// TestDLQHandler_HardStopNoSpill 全部失败 + 未配 SpillDir → errDLQHardStop（硬停分区）。
func TestDLQHandler_HardStopNoSpill(t *testing.T) {
	sink := &fakeDLQSink{alwaysErr: true}
	alert := &recordAlerter{}
	h := newTestDLQHandler(sink, alert, "")

	err := h.Send(context.Background(), sampleDLQRec())
	if !errors.Is(err, errDLQHardStop) {
		t.Fatalf("no SpillDir + exhausted must return errDLQHardStop, got %v", err)
	}
	if !alert.has("dlq_write_exhausted") {
		t.Errorf("expected dlq_write_exhausted alert even on hard stop, got %v", alert.events)
	}
	// 未配 SpillDir 时不应有 spilled_to_disk 事件
	if alert.has("dlq_spilled_to_disk") {
		t.Errorf("must NOT spill when SpillDir unset, got %v", alert.events)
	}
}

// TestDLQHandler_CtxCancelDuringRetry ctx 取消（SIGTERM）→ 立即返 ctx.Err，不再重试。
func TestDLQHandler_CtxCancelDuringRetry(t *testing.T) {
	sink := &fakeDLQSink{alwaysErr: true}
	alert := &recordAlerter{}
	h := newDLQHandler(sink, alert, dlqHandlerConfig{
		MaxRetries:   10,
		RetryBackoff: time.Hour, // 大 backoff 保证 sleep 里被 ctx cancel 中断
		SpillDir:     "",
	})
	// 用 real sleepCtx 而非 no-op，才能被 ctx cancel 打断
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	err := h.Send(ctx, sampleDLQRec())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ctx cancel must surface as context.Canceled, got %v", err)
	}
	// 第一次 write 会尝试（ctx.Err() 在 loop 首 check 时可能 nil；实际 sink 调用后 attempt++）
	// 关键断言：不会一直重试到耗尽
	if sink.writes > 3 {
		t.Errorf("cancelled ctx should stop early, got %d writes", sink.writes)
	}
}

// TestDLQHandler_DefaultsFilledFromZero MaxRetries=0 / RetryBackoff=0 → 走 default（防 divide-by-zero / 立即耗尽）。
func TestDLQHandler_DefaultsFilledFromZero(t *testing.T) {
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	h := newDLQHandler(sink, alert, dlqHandlerConfig{})
	if h.cfg.MaxRetries != defaultDLQHandlerConfig().MaxRetries {
		t.Errorf("MaxRetries=0 must fill default 5, got %d", h.cfg.MaxRetries)
	}
	if h.cfg.RetryBackoff != defaultDLQHandlerConfig().RetryBackoff {
		t.Errorf("RetryBackoff=0 must fill default 200ms, got %v", h.cfg.RetryBackoff)
	}
	// 空 SpillDir 保持空（生产默认，配了才启用）
	if h.cfg.SpillDir != "" {
		t.Errorf("SpillDir default must be empty, got %q", h.cfg.SpillDir)
	}
}

// contains 是最小 substring check（避免 import strings 只为一处）。
func contains(haystack []byte, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(needle) > len(haystack) {
		return false
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == needle {
			return true
		}
	}
	return false
}

// TestTombstoneStatus_HardcodedContract 契约锁：fileextract 侧 tombstoneStatus 必须硬编码
// 为 "unextractable"（与 filebackfill/source.go tombstoneStatusValue 同字面值）。两包独立
// 断言同一字符串，改任一处不改另一处 → CI 挂。避免跨包 import 膨胀（filebackfill 不 import
// fileextract 是有意为之，见 filebackfill/source.go tombstoneStatusValue 注释）。
func TestTombstoneStatus_HardcodedContract(t *testing.T) {
	if tombstoneStatus != "unextractable" {
		t.Fatalf("fileextract.tombstoneStatus must == \"unextractable\" (filebackfill-side scroll query value), got %q", tombstoneStatus)
	}
}
