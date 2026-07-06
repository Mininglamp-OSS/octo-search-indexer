package filebackfill

// runner.go — 串起 scroll source → 限速 → 复用 fileextract.Extractor → OS partial update → 进度日志。

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/fileextract"
)

// ErrTimeoutIncomplete 是 Run 因 top-level context.DeadlineExceeded 提前退出的 sentinel
// （v1.13 P2-3）。K8s Job 用它区分"正常 EOF"（return nil）与"timeout 剩余未跑"（return err，
// exit non-zero → operator 得知需要重跑）。SIGTERM 触发的 context.Canceled 仍返 nil（优雅退出）。
var ErrTimeoutIncomplete = errors.New("filebackfill: run timed out before scroll EOF (incomplete)")

// scrollRetryConfig 是 P2-8：scroll source.Next 出 transient 错时的 in-place backoff retry 参数。
const (
	scrollMaxRetries = 3
	scrollBackoffMin = 500 * time.Millisecond
)

// batchSource 是 Runner 依赖的 scroll source 抽象（便于测试注入 mock）。
type batchSource interface {
	Next(ctx context.Context) ([]sourceDoc, error)
	Close(ctx context.Context) error
}

// docExtractor 抽象「抽一条 doc → OS partial update」的动作，便于 Runner.Run 单测注入 mock。
// 生产实现是 realExtractor（包 *fileextract.Extractor），测试实现 mock 返回预置结果。
//
// 返回签名对齐 fileextract.ExtractAndWriteForBackfill：
//   - (reason="",  cause=nil, err=nil)  → 成功
//   - (reason!="", cause=err, err=nil)  → 抽取失败，DLQ
//   - (reason="",  cause=nil, err!=nil) → OS transient（含 errDocNotYet）
type docExtractor interface {
	Extract(ctx context.Context, messageID, url, name, ext string, size int64) (reason string, cause error, err error)
}

// realExtractor 是 docExtractor 的生产实现，包 fileextract.Extractor。
type realExtractor struct{ e *fileextract.Extractor }

func (r *realExtractor) Extract(ctx context.Context, messageID, url, name, ext string, size int64) (string, error, error) {
	return fileextract.ExtractAndWriteForBackfill(ctx, r.e, messageID, url, name, ext, size)
}

// Runner 是一次性 Job 的主控。
type Runner struct {
	source    batchSource
	extractor docExtractor
	limiter   *rateLimiter
	progress  time.Duration // 每隔多久 log 一次进度（默认 30s）
}

// NewRunner 装配 Runner（生产用；测试走 NewRunnerWith 注入 mock）。
func NewRunner(cfg Config) (*Runner, error) {
	src, err := newOSScrollSource(cfg)
	if err != nil {
		return nil, err
	}
	ext, err := fileextract.NewExtractor(cfg.ToExtractorConfig())
	if err != nil {
		return nil, err
	}
	rate := cfg.Rate
	if rate == 0 {
		rate = 50 // v2 §9 默认 50 RPS
	}
	return &Runner{
		source:    src,
		extractor: &realExtractor{e: ext},
		limiter:   newRateLimiter(rate),
		progress:  30 * time.Second,
	}, nil
}

// NewRunnerWith 用注入的 source/extractor 建 Runner（测试用）。
func NewRunnerWith(src batchSource, ext docExtractor, rate float64) *Runner {
	return &Runner{
		source:    src,
		extractor: ext,
		limiter:   newRateLimiter(rate),
		progress:  10 * time.Millisecond,
	}
}

// Run 主循环：拉一批 → 逐条限速抽取 → 累计 stats → 直到 EOF / ctx 取消 / timeout。
// 返回汇总 stats（K8s Job 用 stats.OSTransient/DLQ 判退出码）。
//
// v1.13 P2-3：区分 ctx.Canceled（signal 优雅退出，nil）与 ctx.DeadlineExceeded（timeout
// 提前退出，ErrTimeoutIncomplete）。老代码把两者都当 graceful → K8s Job exit 0 但实际未跑完，
// 掩盖需要重跑的信号。
// v1.13 P2-8：source.Next 出 transient 错时 bounded backoff retry，避免长跑 job 因单次 blip 中断。
func (r *Runner) Run(ctx context.Context) (Stats, error) {
	var stats Stats
	lastLog := time.Now()
	defer func() {
		if err := r.source.Close(context.Background()); err != nil {
			log.Printf("filebackfill: close source: %v", err)
		}
		log.Printf("filebackfill DONE: scanned=%d extracted=%d dlq=%d skipped=%d os_transient=%d",
			stats.Scanned, stats.Extracted, stats.DLQ, stats.Skipped, stats.OSTransient)
	}()
	for {
		// P2-3：判 ctx 类型，把 DeadlineExceeded 与 Canceled 分开返
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return stats, ErrTimeoutIncomplete
			}
			return stats, nil // Canceled = SIGTERM 优雅退出
		}
		batch, err := r.nextWithRetry(ctx)
		if errors.Is(err, io.EOF) {
			return stats, nil // 正常 EOF
		}
		if errors.Is(err, context.Canceled) {
			return stats, nil
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return stats, ErrTimeoutIncomplete
		}
		if err != nil {
			return stats, err
		}
		for _, doc := range batch {
			stats.Scanned++
			if err := r.limiter.Wait(ctx); err != nil {
				stats.Skipped++
				// P2-3 一致：limiter Wait 因 ctx 取消退出，判类型
				if errors.Is(err, context.DeadlineExceeded) {
					return stats, ErrTimeoutIncomplete
				}
				return stats, nil
			}
			r.processOne(ctx, doc, &stats)
			if time.Since(lastLog) > r.progress {
				log.Printf("filebackfill progress: scanned=%d extracted=%d dlq=%d os_transient=%d",
					stats.Scanned, stats.Extracted, stats.DLQ, stats.OSTransient)
				lastLog = time.Now()
			}
		}
	}
}

// nextWithRetry 包装 source.Next 加 bounded backoff retry（P2-8）。
//   - EOF / ctx err → 直接返（上层处理）
//   - transient err（非 EOF / 非 ctx）→ retry with backoff，共 scrollMaxRetries 次
//   - 重试耗尽 → 返最后一次 err（上层视为 fatal）
//
// scroll 查询本身是幂等的（same query 每次返 same page），故 retry 安全。
func (r *Runner) nextWithRetry(ctx context.Context) ([]sourceDoc, error) {
	var lastErr error
	for attempt := 0; attempt <= scrollMaxRetries; attempt++ {
		if attempt > 0 {
			wait := scrollBackoffMin * time.Duration(1<<(attempt-1)) // 500ms/1s/2s/4s
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		batch, err := r.source.Next(ctx)
		if err == nil {
			return batch, nil
		}
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		lastErr = err
		log.Printf("filebackfill: source.Next transient err (attempt %d/%d): %v", attempt+1, scrollMaxRetries+1, err)
	}
	return nil, lastErr
}

// processOne 抽取一条 → 更新 stats（不 return err，让 Job 继续跑）。
// 若 OS transient (errDocNotYet 极少见——backfill 场景主 doc 一定存在)，记 OSTransient 计数继续。
func (r *Runner) processOne(ctx context.Context, doc sourceDoc, stats *Stats) {
	reason, cause, err := r.extractor.Extract(ctx, doc.MessageID, doc.URL, doc.Name, doc.Extension, doc.Size)
	if err != nil {
		stats.OSTransient++
		log.Printf("filebackfill: os transient for messageId=%s: %v", doc.MessageID, err)
		return
	}
	if reason != "" {
		stats.DLQ++
		log.Printf("filebackfill: dlq messageId=%s reason=%s cause=%v url=%s", doc.MessageID, reason, cause, doc.URL)
		return
	}
	stats.Extracted++
}
