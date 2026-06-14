package consumer

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func newTestDLQ(sink dlqSink, alert alerter, spillDir string) *dlqHandler {
	h := newDLQHandler(sink, alert, dlqConfig{MaxRetries: 2, RetryBackoff: time.Millisecond, SpillDir: spillDir})
	h.sleep = func(context.Context, time.Duration) error { return nil }
	h.nowUnix = func() int64 { return 1700000000 }
	return h
}

func sampleRec() dlqRecord {
	return dlqRecord{Reason: "permanent_4xx", Topic: "t", Partition: 0, Offset: 7, Key: []byte("k"), Value: []byte(`{"x":1}`), Status: 400}
}

func TestDLQ_SuccessFirstTry(t *testing.T) {
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	h := newTestDLQ(sink, alert, "")
	if err := h.Send(context.Background(), sampleRec()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if sink.writes != 1 {
		t.Fatalf("expected 1 write, got %d", sink.writes)
	}
}

func TestDLQ_RetryThenSucceed(t *testing.T) {
	sink := &fakeDLQSink{failFor: 2}
	alert := &recordAlerter{}
	h := newTestDLQ(sink, alert, "")
	if err := h.Send(context.Background(), sampleRec()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if sink.writes != 3 {
		t.Fatalf("expected 3 attempts, got %d", sink.writes)
	}
}

// 🔴 C4：耗尽重试 + 配 spill → 落地 + 告警 + 返回 nil（允许越过，不死锁）。
func TestDLQ_EscapeToSpill(t *testing.T) {
	sink := &fakeDLQSink{alwaysFail: true}
	alert := &recordAlerter{}
	dir := t.TempDir()
	h := newTestDLQ(sink, alert, dir)
	if err := h.Send(context.Background(), sampleRec()); err != nil {
		t.Fatalf("escape must return nil (crossed), got %v", err)
	}
	if !alert.has("dlq_write_exhausted") || !alert.has("dlq_spilled_to_disk") {
		t.Fatalf("expected exhausted + spilled alerts, got %v", alert.events)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read spill dir: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 spill file, got %d", len(files))
	}
}

// 🔴 C4：耗尽重试 + 未配 spill → 硬停（返回 errDLQHardStop），不静默丢。
func TestDLQ_HardStopNoSpill(t *testing.T) {
	sink := &fakeDLQSink{alwaysFail: true}
	alert := &recordAlerter{}
	h := newTestDLQ(sink, alert, "")
	err := h.Send(context.Background(), sampleRec())
	if !errors.Is(err, errDLQHardStop) {
		t.Fatalf("expected errDLQHardStop, got %v", err)
	}
}

func TestDLQ_CtxCancel(t *testing.T) {
	sink := &fakeDLQSink{alwaysFail: true}
	alert := &recordAlerter{}
	h := newTestDLQ(sink, alert, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.Send(ctx, sampleRec()); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ctx canceled, got %v", err)
	}
}
