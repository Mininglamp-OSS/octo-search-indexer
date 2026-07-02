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
	// es-indexer 抢先跑到 OS _update 返 404 的窄窗口。稳态竞态由 errDocNotYet + in-place
	// bounded retry 兜底（v1.13 Blocker #2 fix）。
	ExtractStartupDelay time.Duration

	// v1.13 Blocker #2 fix — in-place bounded retry 参数。
	// 单条消息 in-place retry 最大次数（默认 10）。达上限 → 强制 DLQ ReasonRetryExhausted
	// + commit offset，避免 partition 永久阻塞。生产按 OS SLA 与业务延迟容忍度调。
	MaxRetriesPerMessage int
	// TransientBackoffBase 是 in-place retry 指数退避基（默认 1s）。第 N 次重试 sleep =
	// min(Base × 2^(N-1), Max) + 满抖动。SIGTERM 立即返（ctx 感知）。
	TransientBackoffBase time.Duration
	// TransientBackoffMax 是单次退避上限（默认 60s），避免指数增长到不可接受延迟。
	TransientBackoffMax time.Duration

	// v1.13 Blocker #1 fix — SSRF 防护参数。
	// AllowedDownloadHosts 是 CDN 下载 URL 的 host 白名单（默认 ["cdn.deepminer.com.cn"]）。
	// URL host 不在此列则 pre-check 拒绝（不下载）。future 切内网 COS 通过 env 扩展。
	AllowedDownloadHosts []string
	// AllowedDownloadSchemes 是 URL scheme 白名单（默认 ["https"]）。
	AllowedDownloadSchemes []string
	// SSRFAllowLoopback：**仅测试用**，允许 dial 解析到 127.0.0.1 / ::1 loopback IP。
	// 生产**必须 false**（默认）：loopback 是 SSRF 目标之一（内网服务）。test 场景
	// httptest.NewServer 走 127.0.0.1 才需打开。
	SSRFAllowLoopback bool

	// v1.13 P2-1 fix (yujiawei review) — DLQ 投递有界重试 + 本地 spill 逃逸。
	// 老代码 writeDLQ 直接调 dlqSink.WriteDLQ 失败即 outcomeFatal 全 worker 停 —— 与 consumer
	// 侧成熟的「有界重试 → spill 落盘 → 越过 offset」pattern 不对称。补齐同 pattern。
	// DLQMaxRetries：DLQ 投递自身 transient 失败的有界重试次数（默认 5）。
	DLQMaxRetries int
	// DLQRetryBackoff：DLQ 重试退避基（指数 + 满抖动，默认 200ms）。
	DLQRetryBackoff time.Duration
	// DLQSpillDir：DLQ 写耗尽时本地落地目录。为空 → 硬停返 errDLQHardStop（K8s 重启保 offset）；
	// 非空 → 落盘 + 告警 + 越过 offset（绝不永久卡 partition，回灌工具从此读文件重投）。
	DLQSpillDir string
}
