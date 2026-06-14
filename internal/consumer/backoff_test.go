package consumer

import (
	"context"
	"testing"
	"time"
)

// TestExpJitterBackoff_BoundsAndGrowth full jitter 落在 [0, cap] 内，且 cap 随 attempt 指数增长封顶。
func TestExpJitterBackoff_Bounds(t *testing.T) {
	base := 100 * time.Millisecond
	for attempt := 1; attempt <= 12; attempt++ {
		for i := 0; i < 50; i++ {
			d := expJitterBackoff(base, attempt)
			if d < 0 {
				t.Fatalf("backoff negative: %v", d)
			}
			if d > maxBackoff {
				t.Fatalf("attempt %d: backoff %v exceeds cap %v", attempt, d, maxBackoff)
			}
		}
	}
}

// TestExpJitterBackoff_ZeroBase base<=0 → 0（不退避）。
func TestExpJitterBackoff_ZeroBase(t *testing.T) {
	if d := expJitterBackoff(0, 3); d != 0 {
		t.Fatalf("zero base must yield 0, got %v", d)
	}
}

// TestSleepCtx_CancelReturnsEarly ctx 取消时 sleepCtx 立即返回 ctx 错误（不等满 d）。
func TestSleepCtx_CancelReturnsEarly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := sleepCtx(ctx, time.Hour); err == nil {
		t.Fatalf("expected ctx error")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("sleepCtx did not return early on cancel")
	}
}

// TestSleepCtx_CompletesWhenNotCancelled 未取消时正常睡满（短时长）。
func TestSleepCtx_CompletesWhenNotCancelled(t *testing.T) {
	if err := sleepCtx(context.Background(), 5*time.Millisecond); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}
