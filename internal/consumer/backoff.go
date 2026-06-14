package consumer

import (
	"context"
	"math/rand"
	"time"
)

// maxBackoff 限制单次退避上限，避免指数增长到不可接受的停顿。
const maxBackoff = 30 * time.Second

// expJitterBackoff 计算第 attempt 次（1-based）的指数退避 + 满抖动（full jitter）时长：
// base*2^(attempt-1) 截到 maxBackoff，再在 [0, that] 区间随机取值打散并发副本的重试节拍
// （缓解 thundering herd，P2-1）。attempt<=0 视为 1。
func expJitterBackoff(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt && d < maxBackoff; i++ {
		d *= 2
	}
	if d > maxBackoff {
		d = maxBackoff
	}
	// full jitter：[0, d) 上均匀取样。
	return time.Duration(rand.Int63n(int64(d) + 1)) //nolint:gosec // 抖动非安全用途
}

// sleepCtx 在 ctx 取消时尽早返回（P2-2：退避期间收到 SIGTERM 立即关停，不被 sleep 拖住）。
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
