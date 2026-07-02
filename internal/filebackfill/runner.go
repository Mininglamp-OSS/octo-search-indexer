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
// ctx.Canceled / DeadlineExceeded 视为优雅退出（返 nil err），不当运行错误 —— K8s SIGTERM
// 场景不应触发退出码 1 + 错误日志误报。真正的 source 错误（如 OS 5xx）仍然上抛。
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
		if err := ctx.Err(); err != nil {
			return stats, nil
		}
		batch, err := r.source.Next(ctx)
		if errors.Is(err, io.EOF) {
			return stats, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return stats, nil
		}
		if err != nil {
			return stats, err
		}
		for _, doc := range batch {
			stats.Scanned++
			if err := r.limiter.Wait(ctx); err != nil {
				stats.Skipped++
				return stats, nil // ctx 取消，优雅退出
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
