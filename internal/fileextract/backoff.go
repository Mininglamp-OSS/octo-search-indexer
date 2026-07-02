package fileextract

// backoff.go — in-place bounded retry 用的退避与 sleep helper。
// 复制自 internal/consumer/backoff.go 但参数化 Max（fileextract 允许 cfg 调整上限）。
// 不 import consumer 包避免循环依赖 + 保持 fileextract 独立可测。

import (
	"context"
	"math/rand"
	"time"
)

// defaultBackoffMax 是 in-place retry 单次退避的默认上限（cfg 不设时用）。
const defaultBackoffMax = 60 * time.Second

// expJitterBackoff 计算第 attempt 次（1-based）的指数退避 + 满抖动（full jitter）时长：
// base*2^(attempt-1) 截到 max，再在 [0, that] 区间随机取值打散并发副本的重试节拍
// （缓解 thundering herd）。attempt<=0 视为 1；base<=0 返 0；max<=0 用 defaultBackoffMax。
func expJitterBackoff(base, max time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	if max <= 0 {
		max = defaultBackoffMax
	}
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	// full jitter：[0, d] 上均匀取样。
	return time.Duration(rand.Int63n(int64(d) + 1)) //nolint:gosec // 抖动非安全用途
}

// sleepCtx 在 ctx 取消时尽早返回（SIGTERM/timeout 立即关停，不被 sleep 拖住）。
// 返回 ctx.Err()（被取消则非 nil）。d<=0 时仅检查一次 ctx。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
