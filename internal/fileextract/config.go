// Package fileextract 是 cmd/file-extractor 独立服务的核心包（v1.12 file content indexing）。
//
// 定位：跟 es-indexer 同一个 Kafka topic `octo.message.v1{,.prod}`，独立 consumer group
// `file-extractor`（不抢 es-indexer 位点）。命中 payload.type=8 (File) 的消息 → 下载 CDN 文件
// → 调 Tika HTTP 抽取正文 → OS partial `_update` 只写 payload.file.content + contentMeta。
// 命中非 file 类型 → commit 位点跳过。
//
// 与 internal/consumer 的关系：
//   - 结构镜像 internal/consumer/{service.go, consumer.go, kafka.go, dlq.go}（消费组
//     协调、CommitInterval=0 手动提交、DLQ Kafka producer、per-batch 处理循环），保持团队
//     心智模型一致；不 import consumer 包避免循环依赖 + 保持本包独立可测。
//   - 与 esindex.Writer 语义不同：Writer 走 _bulk index (upsert 主 doc)，fileextract.osWriter
//     走 _update (partial merge，只覆盖 content/contentMeta，doc_as_upsert=false 避免造孤儿子文档）。
//     故不复用 esindex.Writer，本包新增 osWriter。
package fileextract

import "time"

// ServiceConfig 是 file-extractor 服务的运行配置（由 cmd 从环境装配）。
//
// Kafka + OS 部分复用 es-indexer 的配置形态（同 topic 不同 groupID + 同 alias 不同写入模式）。
type ServiceConfig struct {
	// Kafka
	Brokers   []string
	Topic     string
	DLQTopic  string
	GroupID   string
	BatchSize int

	// OpenSearch（partial _update 目标）
	ESAddresses []string
	ESIndex     string
	ESUsername  string
	ESPassword  string

	// Tika / Download / Extract
	TikaURL         string        // http://localhost:9998（sidecar 部署方案 α）
	DownloadTimeout time.Duration // 单次 CDN GET 超时（默认 30s）
	ExtractTimeout  time.Duration // Tika PUT /tika 超时（默认 30s）
	MaxFileSize     int64         // 单文件抽取 size cutoff（默认 20MB）
	MaxContentBytes int           // 抽出文本截断（默认 256KB）
	HTTPRetries     int           // CDN GET 重试次数（默认 3，指数退避 1s/2s/4s/8s）

	// 时序竞态防护（v2 §7 #1）：启动时 sleep 缓解首启动瞬间 file-extractor 比
	// es-indexer 抢先跑到 OS _update 返 404 的窄窗口。稳态竞态由 errDocNotYet + rebalance
	// 自然重试兜底；Phase 2 备选独立 Kafka retry topic。
	ExtractStartupDelay time.Duration
}
