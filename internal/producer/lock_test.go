package producer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// muxLock simulates a shared cross-replica lock: only one holder at a time.
type muxLock struct {
	mu     sync.Mutex
	holder string
}

func (m *muxLock) Acquire(token string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.holder != "" {
		return false, nil
	}
	m.holder = token
	return true, nil
}
func (m *muxLock) Renew(token string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.holder == token, nil
}
func (m *muxLock) Release(token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.holder == token {
		m.holder = ""
	}
	return nil
}

// TestRunLocked_MutualExclusion: with a shared lock, two concurrent runLocked
// calls never overlap inside run; the loser skips.
func TestRunLocked_MutualExclusion(t *testing.T) {
	lock := &muxLock{}
	var active atomic.Int32
	var maxActive atomic.Int32
	var ran atomic.Int32

	work := func(ctx context.Context) error {
		n := active.Add(1)
		if n > maxActive.Load() {
			maxActive.Store(n)
		}
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
		ran.Add(1)
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runLocked(context.Background(), lock, time.Hour, func(string, ...any) {}, nil, work); err != nil {
				t.Errorf("runLocked: %v", err)
			}
		}()
	}
	wg.Wait()

	if maxActive.Load() > 1 {
		t.Fatalf("run bodies overlapped: maxActive=%d (lock failed to serialize)", maxActive.Load())
	}
	// At least one ran; the other may have skipped (lock held) — both outcomes valid.
	if ran.Load() < 1 {
		t.Fatalf("expected at least one run to execute, got %d", ran.Load())
	}
}

// TestRunLocked_RenewFailureAborts: a lock that loses ownership cancels lockCtx,
// aborting the in-flight run.
func TestRunLocked_RenewFailureAborts(t *testing.T) {
	lock := &losingLock{}
	started := make(chan struct{})
	work := func(ctx context.Context) error {
		close(started)
		<-ctx.Done() // must be cancelled by renew failure
		return ctx.Err()
	}
	err := runLocked(context.Background(), lock, 10*time.Millisecond, func(string, ...any) {}, nil, work)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled after renew failure, got %v", err)
	}
	<-started
}

// losingLock acquires once, then reports ownership lost on renew.
type losingLock struct{ renewed atomic.Bool }

func (l *losingLock) Acquire(string) (bool, error) { return true, nil }
func (l *losingLock) Renew(string) (bool, error)   { l.renewed.Store(true); return false, nil }
func (l *losingLock) Release(string) error         { return nil }

// TestRunLocked_AcquireErrorReturns: Redis failure on acquire bubbles up (no bare run).
func TestRunLocked_AcquireErrorReturns(t *testing.T) {
	lock := &errLock{}
	ran := false
	err := runLocked(context.Background(), lock, time.Hour, func(string, ...any) {}, nil, func(context.Context) error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatalf("expected acquire error to bubble up")
	}
	if ran {
		t.Fatalf("run must not execute when acquire errors (no bare run)")
	}
}

type errLock struct{}

func (errLock) Acquire(string) (bool, error) { return false, errors.New("redis down") }
func (errLock) Renew(string) (bool, error)   { return false, nil }
func (errLock) Release(string) error         { return nil }
