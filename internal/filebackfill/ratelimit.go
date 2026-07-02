package filebackfill

// ratelimit.go — 简化令牌桶限速（backfill 场景够用；复制 internal/backfill/ratelimit.go 思路，
// 不 import 避免耦合到 message-shard 场景的 randInt63n 依赖）。

import (
	"context"
	"sync"
	"time"
)

// rateLimiter 令牌桶：按 docsPerSec 匀速补充，burst = 1 秒的量。
// docsPerSec <= 0 表示不限速。
//
// 并发安全：Wait 用 sync.Mutex 保护内部 mutable 状态 (tokens / last)，
// Runner 主循环当前是单 goroutine 调用，但未来若加 worker 池不会 silent 破坏限速。
type rateLimiter struct {
	docsPerSec float64
	burst      float64
	mu         sync.Mutex
	tokens     float64
	last       time.Time
}

func newRateLimiter(docsPerSec float64) *rateLimiter {
	burst := docsPerSec
	if burst <= 0 {
		burst = 1
	}
	// 关键：burst 最少 1，否则令牌永远补不到 1，Wait 死循环（例如 docsPerSec=0.1
	// 时 burst=0.1，tokens += elapsed*0.1 但上限 0.1 → 永远 < 1 → 死锁）。
	if burst < 1 {
		burst = 1
	}
	return &rateLimiter{
		docsPerSec: docsPerSec,
		burst:      burst,
		tokens:     burst,
		last:       time.Now(),
	}
}

// Wait 消耗 1 个令牌；不足则等到有为止（可被 ctx 取消）。线程安全。
func (r *rateLimiter) Wait(ctx context.Context) error {
	if r.docsPerSec <= 0 {
		return nil
	}
	for {
		r.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(r.last).Seconds()
		r.tokens += elapsed * r.docsPerSec
		if r.tokens > r.burst {
			r.tokens = r.burst
		}
		r.last = now
		if r.tokens >= 1 {
			r.tokens -= 1
			r.mu.Unlock()
			return nil
		}
		wait := time.Duration((1-r.tokens)/r.docsPerSec*1000) * time.Millisecond
		r.mu.Unlock()
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
