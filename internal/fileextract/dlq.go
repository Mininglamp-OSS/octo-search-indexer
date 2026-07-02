package fileextract

// DLQReason 是 file-extractor DLQ 投递原因枚举（v1.13）。全部触发点在 IDX-4 接上；
// IDX-3 骨架期只定义常量 + ReasonParseError（Kafka 反序列化失败）触发。
//
// v1.13 Blocker #2 fix 新增 2 项 (retry_exhausted / os_permanent)：
//
//	| reason              | 触发条件                                      | 是否重试 |
//	|---------------------|---------------------------------------------|----------|
//	| parse_error         | Kafka 消息反序列化 searchmsg 失败              | ❌      |
//	| oversize            | 文件 > MaxFileSize                            | ❌      |
//	| blacklist_ext       | 扩展名在黑名单（不该走这里，是设计正常跳过） | ❌      |
//	| download_failed     | HTTP GET CDN 5xx 或连接超时（重试耗尽）        | ✅ 3 次 |
//	| extract_timeout     | Tika 抽取 > ExtractTimeout                    | ❌      |
//	| encrypted           | Tika 抛 EncryptedDocumentException             | ❌      |
//	| empty_extract       | Tika 返回空串或空白                           | ❌      |
//	| extract_error       | 其他 Tika parse 异常                          | ✅ 1 次 |
//	| retry_exhausted     | in-place bounded retry N 次未成功（Blocker #2） | ✅ N 次 |
//	| os_permanent        | OS 返 4xx (非 404/409/429) permanent (P2-2)   | ❌      |
const (
	ReasonParseError     = "parse_error"
	ReasonOversize       = "oversize"
	ReasonBlacklistExt   = "blacklist_ext"
	ReasonDownloadFailed = "download_failed"
	ReasonExtractTimeout = "extract_timeout"
	ReasonEncrypted      = "encrypted"
	ReasonEmptyExtract   = "empty_extract"
	ReasonExtractError   = "extract_error"

	// ReasonRetryExhausted：单条消息 in-place bounded retry N 次仍未成功
	// (errDocNotYet / errOSTransient 长期未收敛) → 强制 DLQ 并 commit offset，避免
	// partition 无限阻塞。回灌工具按 messageId 从源 MySQL 重取重试。
	ReasonRetryExhausted = "retry_exhausted"

	// ReasonOSPermanent：OS 写返 4xx (非 404/409/429) permanent error，通常是请求
	// body 编程 bug 或 mapping 冲突。立即 DLQ 不重试（重试无意义 + 会阻塞 partition）。
	ReasonOSPermanent = "os_permanent"
)

// dlqRecord 是 file-extractor 落 DLQ topic 的记录载荷。
// 与 consumer/dlq.go dlqRecord 语义相同但字段更聚焦：只带 file-extractor 排障关心的信息
// （消息元数据 + 抽取上下文），不带 bulk per-item status。回灌工具按 Reason + MessageID 反查。
type dlqRecord struct {
	Reason    string `json:"reason"`    // 8 种枚举之一
	Topic     string `json:"topic"`     // 源 topic
	Partition int    `json:"partition"` // 源分区
	Offset    int64  `json:"offset"`    // 源 offset
	Key       []byte `json:"key"`       // 原始 Kafka key（= message_id）
	Value     []byte `json:"value"`     // 原始 Kafka 消息 bytes（截断规则见 maxDLQRawValueBytes）
	// 抽取上下文（IDX-4 触发点填入；IDX-3 骨架期只有 parse_error 填 Detail）
	MessageID string `json:"messageId,omitempty"` // 契约 message_id 字符串
	FileURL   string `json:"fileURL,omitempty"`   // 文件 CDN URL（download_failed / extract_* 时填）
	FileExt   string `json:"fileExt,omitempty"`   // 扩展名（oversize / blacklist_ext 时填）
	FileSize  int64  `json:"fileSize,omitempty"`  // 文件字节数（oversize 时填）
	Detail    string `json:"detail,omitempty"`    // 错误详情字符串
	SpilledAt int64  `json:"spilledAt,omitempty"` // 本地 spill 文件写入时刻（epoch second）
	// PayloadTruncated 标记 Value 因超限被截断（同 consumer/dlq.go 语义），回灌工具
	// 读到 true 时须按 MessageID 从源 MySQL 重取，不依赖 DLQ 内字节。
	PayloadTruncated bool `json:"payloadTruncated,omitempty"`
}

// maxDLQRawValueBytes 是 DLQ 信封里原始 Value 字节的截断阈值（与 consumer/dlq.go 700_000 同口径）。
// 按 base64 膨胀 (~1.33x) + 信封字段开销预留：700KB × 1.33 ≈ 931KB + 信封 < 1MiB broker 硬限。
const maxDLQRawValueBytes = 700_000

// truncateValueIfNeeded 按 maxDLQRawValueBytes 决定是否截断 Value + 标记 PayloadTruncated。
// 返回 (可能被截断的 Value, 是否被截断)。
func truncateValueIfNeeded(v []byte) ([]byte, bool) {
	if len(v) <= maxDLQRawValueBytes {
		return v, false
	}
	return v[:maxDLQRawValueBytes], true
}
