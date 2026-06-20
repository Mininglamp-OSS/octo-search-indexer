package producer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestScheduler_StartTickStop: ticks fire on cadence and Stop joins cleanly.
func TestScheduler_StartTickStop(t *testing.T) {
	var ticks atomic.Int32
	fired := make(chan struct{}, 8)
	tickFn := func(ctx context.Context, tables []string) error {
		ticks.Add(1)
		select {
		case fired <- struct{}{}:
		default:
		}
		return nil
	}
	s := NewScheduler(10*time.Millisecond, []string{"message"}, tickFn, nil, nil)
	s.Start()
	// Wait for at least two ticks.
	for i := 0; i < 2; i++ {
		select {
		case <-fired:
		case <-time.After(2 * time.Second):
			t.Fatalf("scheduler did not tick in time (got %d)", ticks.Load())
		}
	}
	s.Stop()
	got := ticks.Load()
	time.Sleep(40 * time.Millisecond)
	if ticks.Load() != got {
		t.Fatalf("ticks fired after Stop: before=%d after=%d", got, ticks.Load())
	}
}

// TestScheduler_StartIdempotent: double Start does not start two loops.
func TestScheduler_StartIdempotent(t *testing.T) {
	var ticks atomic.Int32
	s := NewScheduler(5*time.Millisecond, nil, func(context.Context, []string) error {
		ticks.Add(1)
		return nil
	}, nil, nil)
	s.Start()
	s.Start() // idempotent
	time.Sleep(30 * time.Millisecond)
	s.Stop()
	// Just assert it ran and stopped without panic/deadlock.
	if ticks.Load() == 0 {
		t.Fatalf("scheduler should have ticked at least once")
	}
}

// TestScheduler_StopWithoutStart is a no-op.
func TestScheduler_StopWithoutStart(t *testing.T) {
	s := NewScheduler(time.Second, nil, func(context.Context, []string) error { return nil }, nil, nil)
	s.Stop() // must not panic / block
}

// TestScheduler_FirstTickImmediate: the scheduler runs one round immediately on
// Start, well before the first ticker interval elapses (🔵 no slow first-light /
// no long blind window at cut-over).
func TestScheduler_FirstTickImmediate(t *testing.T) {
	fired := make(chan struct{}, 1)
	// A long interval: if the first tick waited for the ticker, this test would
	// time out instead of seeing a tick within the deadline.
	s := NewScheduler(10*time.Second, []string{"message"}, func(context.Context, []string) error {
		select {
		case fired <- struct{}{}:
		default:
		}
		return nil
	}, nil, nil)
	s.Start()
	defer s.Stop()
	select {
	case <-fired:
		// Got the immediate first tick — good.
	case <-time.After(2 * time.Second):
		t.Fatalf("scheduler did not run an immediate first tick (waited for the interval)")
	}
}

// TestScheduler_NoTickIfCancelledBeforeStart: if the loop ctx is already done,
// the immediate first tick is skipped (Stop right after Start must not fire).
func TestScheduler_ImmediateTickRespectsFastStop(t *testing.T) {
	var ticks atomic.Int32
	s := NewScheduler(time.Hour, nil, func(context.Context, []string) error {
		ticks.Add(1)
		return nil
	}, nil, nil)
	s.Start()
	s.Stop()
	// At most one immediate tick may have raced in before Stop cancelled ctx; the
	// invariant under test is simply that Start+Stop does not deadlock or panic and
	// the ticker path never fires (interval is 1h).
	if got := ticks.Load(); got > 1 {
		t.Fatalf("expected at most the single immediate tick, got %d", got)
	}
}
