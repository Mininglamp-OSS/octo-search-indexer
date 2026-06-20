package producer

import (
	"context"
	"sync"
	"time"
)

// Scheduler drives the slow-cursor incremental extraction on a fixed tick.
//
// This is the slim mirror of the source module's scheduler: a minute-grade
// time.Ticker, each tick calling RunIncremental once. It runs one round
// immediately on Start (no wait for the first interval), then ticks on cadence.
// Slow cursor only (lag held at the conservative default). Cross-replica mutual
// exclusion / lock-loss abort is handled inside RunIncremental's Redis run-lock;
// the scheduler only owns "fire on cadence + lifecycle".
type Scheduler struct {
	interval time.Duration
	tables   []string
	tickFn   func(ctx context.Context, tables []string) error
	logf     func(string, ...any)
	metrics  *Metrics

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewScheduler constructs a Scheduler. tickFn defaults to etl.RunIncremental.
func NewScheduler(interval time.Duration, tables []string, tickFn func(context.Context, []string) error, logf func(string, ...any), metrics *Metrics) *Scheduler {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Scheduler{
		interval: interval,
		tables:   tables,
		tickFn:   tickFn,
		logf:     logf,
		metrics:  metrics,
	}
}

// Start launches the background tick goroutine (idempotent). It does NOT block.
func (s *Scheduler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	s.started = true
	go s.loop(ctx)
	s.logf("producer: slow-cursor scheduler started tick=%s tables=%v", s.interval, s.tables)
}

// loop is the slow-cursor tick main loop: fire every interval until ctx cancels.
func (s *Scheduler) loop(ctx context.Context) {
	defer close(s.done)
	t := time.NewTicker(s.interval)
	defer t.Stop()
	// 🔵 Run one round immediately on start, before waiting out the first ticker
	// interval. Otherwise the producer idles for a full tick (default 60s) after
	// boot before it streams anything — slow first-light on deploy and, more
	// importantly, a long blind window during the runtime cut-over when the
	// independent producer is expected to pick up the cursor "within seconds"
	// (gate A2). The run-lock + cursor CAS + idempotent sink make an extra early
	// round safe; respect ctx cancellation so a fast SIGTERM right after Start
	// does not still fire a round.
	if ctx.Err() == nil {
		s.tick(ctx)
	}
	for {
		select {
		case <-ctx.Done():
			s.logf("producer: scheduler loop stopped")
			return
		case <-t.C:
			s.tick(ctx)
		}
	}
}

// tick fires one slow-cursor round. Errors are logged, not fatal (next tick
// retries; cross-replica / lock-loss safety is RunIncremental's responsibility).
func (s *Scheduler) tick(ctx context.Context) {
	if s.metrics != nil {
		s.metrics.MarkTick()
	}
	if err := s.tickFn(ctx, s.tables); err != nil {
		s.logf("producer: scheduled incremental failed: %v", err)
		if s.metrics != nil {
			s.metrics.MarkTickError()
		}
	}
}

// stopJoinTimeout caps how long Stop waits for the loop to exit — a tick may be
// blocked in a non-ctx-aware DB call, and an unbounded join would stall shutdown.
//
// 🔴 Lock-release tradeoff (inherited from the source searchetl scheduler): in the
// normal path a tick observes ctx cancellation, returns, and runLocked's defer
// CAS-releases the Redis run-lock before this join completes — so the lock is
// released proactively on shutdown. In the pathological case where a tick is wedged
// in a non-ctx-aware DB call past this timeout, Stop abandons the join and main
// unwinds; the run-lock is then NOT proactively released and a standby replica must
// wait for the lease TTL (runLockTTL, 30m) to expire before taking over. Go cannot
// force-kill a goroutine stuck in a blocking syscall, so the TTL is the failover
// backstop by design. The TTL bounds the worst case; it is the reason the lock has a
// lease at all. Shortening runLockTTL trades faster failover for more frequent renew
// traffic — left at the source module's 30m default here.
const stopJoinTimeout = 30 * time.Second

// Stop stops the scheduler (idempotent): cancel the loop ctx and join, with a cap.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return
	}
	s.started = false
	cancel, done := s.cancel, s.done
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
			s.logf("producer: scheduler stopped")
		case <-time.After(stopJoinTimeout):
			s.logf("producer: scheduler stop: tick did not exit within %s, abandoning join", stopJoinTimeout)
		}
	}
}
