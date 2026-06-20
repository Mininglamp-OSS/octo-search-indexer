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
