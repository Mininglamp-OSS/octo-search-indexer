// Package filebackfill 是 cmd/file-content-backfill 一次性 Job 的核心包（v1.12）。
//
// 目的：把 IDX-4 file-extractor 上线**之前**已经存在的历史 type=8 文件消息回填 payload.file.content
// （prod 实测约 123K 条，v2 §11）。file-extractor 只处理 IDX-4 上线之后的增量。
//
// 与 file-extractor 的关系：
//   - 复用 fileextract.Extractor（download + tika + os partial update 三链条同套）
//   - 独立入口 cmd/file-content-backfill，走 K8s Job 一次性跑（主人决策 backfill 走 K8s Job 一次性）
//   - 源不同：file-extractor 从 Kafka 拉；本 Job 从 OS scroll 反查 `payload.type=8 AND must_not exists content`
//     （Kafka 历史消息大多超保留期；OS scroll query 天然幂等，重跑跳过已抽取的 doc）
//   - 限速 50 RPS 避免压垮 Tika 或 CDN（v2 §9 回灌耗时估算 40min）
//   - 无 checkpoint（简化版：重跑靠 OS scroll query 幂等；123K × 200ms 一小时内跑完，中断重跑成本可接受）
//
// Runner 用法：runner.Run(ctx) 阻塞跑到 EOF / ctx 取消 / 达 timeout；返回汇总 stats
// （processed / dlq / errors）供 K8s Job 退出码判 (stats.errs > 0 → exit 1 让 K8s 报错)。
package filebackfill

import (
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/fileextract"
)

// Config 是 backfill Runner 的运行配置。
type Config struct {
	ESAddresses []string
	ESIndex     string // e.g. "octo-message" (alias 或 backing index)
	ESUsername  string
	ESPassword  string

	// TikaURL / DownloadTimeout / ExtractTimeout / MaxFileSize / MaxContentBytes / HTTPRetries
	// 转发给 fileextract.NewExtractor 复用同一套抽取核心。
	TikaURL         string
	DownloadTimeout time.Duration
	ExtractTimeout  time.Duration
	MaxFileSize     int64
	MaxContentBytes int
	HTTPRetries     int

	// Rate 是限速（docs/s），默认 50。0/负数 = 不限速。
	Rate float64
	// ScrollSize 是每批 scroll 拉取的 doc 数（默认 500）。
	ScrollSize int
	// ScrollTTL 是 scroll 上下文的 keep-alive 时间（默认 5min，每次 continue 会重置）。
	ScrollTTL time.Duration
	// Timeout 是整个 Job 上限（默认 2h）。
	Timeout time.Duration
}

// ToExtractorConfig 转换成 fileextract.ServiceConfig（extractor 只用其中一部分字段）。
func (c Config) ToExtractorConfig() fileextract.ServiceConfig {
	return fileextract.ServiceConfig{
		ESAddresses:     c.ESAddresses,
		ESIndex:         c.ESIndex,
		ESUsername:      c.ESUsername,
		ESPassword:      c.ESPassword,
		TikaURL:         c.TikaURL,
		DownloadTimeout: c.DownloadTimeout,
		ExtractTimeout:  c.ExtractTimeout,
		MaxFileSize:     c.MaxFileSize,
		MaxContentBytes: c.MaxContentBytes,
		HTTPRetries:     c.HTTPRetries,
	}
}

// Stats 是 Runner.Run 的汇总统计。K8s Job 用于退出码判定。
type Stats struct {
	Scanned     int64 // OS scroll 拉出的总 doc 数
	Extracted   int64 // 成功抽取 + OS update 的 doc 数
	DLQ         int64 // 抽取失败进 DLQ 计数（backfill 只 log 不真写 kafka，spill 到日志）
	Skipped     int64 // ctx 取消 / 其他原因跳过
	OSTransient int64 // OS errDocNotYet / transient 触发次数（backfill 场景理论上不该出现 errDocNotYet）
}
