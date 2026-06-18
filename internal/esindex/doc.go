package esindex

import (
	"fmt"
	"strconv"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// Doc 是写入 OpenSearch 的文档形态 —— **逐字段对齐 octo-server reader**
// (`modules/messages_search/source.go::Doc`，PR#385)。这是分叉 B 收敛的核心：
// 索引契约的单一真源是 reader 读的形态，indexer 写成它能读的样子（reader 不动）。
//
// 与旧 flat snake_case 契约（message_id/content/...）的差异（全量收敛）：
//   - 字段名 camelCase + payload 嵌套（messageId/channelId/payload.text.content）。
//   - messageId 用 **long**（数值，全精度）——snowflake id 不可被 float64 截断，
//     reader 从 typed _source 读 int64 做 cursor tiebreaker（dsl.go::lastHitMessageID）。
//   - 新增 reader 必读的 **spaceId**(keyword) / **visibles**(keyword[]) / **messageSeq**(long)：
//     · spaceId：p2p (DM) space 召回过滤，缺则同 space 也 0 命中（reader fail-closed）。
//     · visibles：群系统消息「仅管理员可见」白名单 gate，缺则 reader fail-OPEN（V3b）。
//     · messageSeq：reader channel_offset「清空会话」gate（visibility.go），缺则保守隐藏。
//   - 排序字段 timestamp + messageId 与 reader cursor sort 口径一致。
//
// 字段标签与 reader Doc 完全一致（含 omitempty），保证 reader 反序列化无歧义。
type Doc struct {
	SchemaVersion int      `json:"schemaVersion"`
	MessageID     int64    `json:"messageId"`
	MessageSeq    uint64   `json:"messageSeq,omitempty"`
	From          string   `json:"from,omitempty"`
	To            string   `json:"to,omitempty"`
	ChannelID     string   `json:"channelId"`
	ChannelType   uint32   `json:"channelType"`
	SpaceID       string   `json:"spaceId,omitempty"`
	Visibles      []string `json:"visibles,omitempty"`
	Timestamp     int64    `json:"timestamp"`
	CreatedAt     int64    `json:"createdAt,omitempty"`
	RawExcluded   bool     `json:"rawExcluded,omitempty"`
	Source        string   `json:"source,omitempty"`
	Payload       *Payload `json:"payload,omitempty"`
}

// Payload 是结构化正文投影（镜像 reader Doc.Payload）。当前 indexer 只抽取文本正文
// （路线甲 + 与 producer/backfill payload 抽取口径一致：非文本 raw_excluded）；媒体子对象
// mapping 已就位但本期不填，待后续阶段细化（CONSTRAINTS v1.10 follow-up）。
type Payload struct {
	Type *int         `json:"type,omitempty"`
	Text *TextPayload `json:"text,omitempty"`
}

// TextPayload 镜像 reader Doc.Payload.Text（IK 分词字段 payload.text.content）。
type TextPayload struct {
	Content string `json:"content,omitempty"`
}

// idString 返回 ES `_id`（= 规范化 message_id）。messageId 为数值 snowflake，
// FormatInt(ParseInt(s)) 与原串一致，保证 backfill 与实时 consumer 写同一 _id（幂等）。
func (d Doc) idString() string {
	return strconv.FormatInt(d.MessageID, 10)
}

// parseMessageID 把契约 message_id（VARCHAR(20) 数值串）解析为全精度 int64。
// 解析失败属数据异常（reader 读 int64，非数值无法对齐）——上抛由调用方按毒丸处置，
// 绝不静默落 0（reader 对 messageId==0 直接丢，会掩盖问题）。
func parseMessageID(messageID string) (int64, error) {
	id, err := strconv.ParseInt(messageID, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("esindex: non-numeric message_id %q (reader reads messageId as long): %w", messageID, err)
	}
	return id, nil
}

// DocFromMessage 把 Kafka 契约消息（searchmsg.Message）转成 reader 可读 Doc（实时 consumer 路径）。
//
// ✅ 契约已升到 v2（SchemaVersion=2）：searchmsg.Message 携带 reader 必读的安全/正确性字段
// SpaceID / Visibles / MessageSeq（octo-server searchetl producer 富化后填全）。DocFromMessage
// 把这三字段从契约逐字段 copy 进 Doc —— 这正是 TestV2Gate_DocFromMessageWiresSafetyFields 钉死
// 的「v2 bump 必须同步接线」：若解了实时安全闸（LiveContractCarriesSafetyFields）却不接线，
// 实时路径会写出空 visibles 的 doc → reader fail-OPEN（普通成员搜出群管才可见的系统消息）。
//
// 注意：producer 是否对每条消息填非空 visibles 是 producer 侧 fail-closed（空 visibles → DLQ，
// 不进 Kafka）的职责（票2）；indexer 侧只负责忠实搬运契约已带的字段。存量数据的这三字段由
// backfill 路径（读原始 MySQL payload + searchmsg.ExtractVisibility）自源填全。
func DocFromMessage(msg searchmsg.Message) (Doc, error) {
	id, err := parseMessageID(msg.MessageID)
	if err != nil {
		return Doc{}, err
	}
	d := Doc{
		SchemaVersion: msg.SchemaVersion,
		MessageID:     id,
		MessageSeq:    msg.MessageSeq,
		From:          msg.FromUID,
		ChannelID:     msg.ChannelID,
		ChannelType:   uint32(msg.ChannelType),
		SpaceID:       msg.SpaceID,
		Visibles:      msg.Visibles,
		Timestamp:     msg.MsgTimestamp,
		CreatedAt:     msg.CreatedAt,
		RawExcluded:   msg.RawExcluded,
		Source:        msg.Source,
	}
	d.Payload = buildPayload(msg.ContentType, msg.Content)
	return d, nil
}

// buildPayload 构造 reader 可读的 payload 投影。payload.type 始终带（reader _search_all
// 据此分流）；仅文本且 content 非空时填 payload.text.content（IK 分词）。
func buildPayload(contentType int, content *string) *Payload {
	ct := contentType
	p := &Payload{Type: &ct}
	if contentType == payloadTypeText && content != nil && *content != "" {
		p.Text = &TextPayload{Content: *content}
	}
	return p
}

// payloadTypeText 对应 octo-lib common.Text（文本消息）。与 reader source.go
// payloadTypeText / producer extract 口径一致。
const payloadTypeText = 1

// SafetyFieldsSchemaVersion 是 Kafka 契约（searchmsg.Message）开始携带 reader 必读的
// 安全/正确性字段（SpaceID + Visibles + MessageSeq）的最小 SchemaVersion。
//
// 当前契约 searchmsg.SchemaVersion==1 **不带**这三字段（producer 只抽 content），故实时
// consumer 路径写出的 doc 这三字段为空——reader 对空 visibles 是 **fail-OPEN**（群系统消息
// 普通成员能搜出群管才可见消息），对空 spaceId(p2p) fail-closed，对 messageSeq=0 保守隐藏。
// 把这三字段灌进 Kafka 契约需 octo-lib 升 SchemaVersion 1→2 + octo-server producer 富化
// （阶段 9 上线前置）。届时 indexer 重 pin 到带这些字段的 octo-lib，SchemaVersion 升到本值，
// LiveContractCarriesSafetyFields() 自动转 true，实时路径解除写入封锁。
const SafetyFieldsSchemaVersion = 2

// LiveContractCarriesSafetyFields 报告当前编译进二进制的 Kafka 契约是否已携带 reader 必读的
// 安全字段（SpaceID/Visibles/MessageSeq）。consumer 据此决定是否允许实时写入（防 V3b fail-OPEN）。
func LiveContractCarriesSafetyFields() bool {
	return searchmsg.SchemaVersion >= SafetyFieldsSchemaVersion
}
