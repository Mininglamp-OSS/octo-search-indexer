package esindex

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// Doc 是写入 OpenSearch 的文档形态 —— **逐字段对齐 octo-server reader**
// (`modules/messages_search/source.go::Doc`，PR#385/PR#425)。这是分叉 B 收敛的核心：
// 索引契约的单一真源是 reader 读的形态，indexer 写成它能读的样子（reader 不动）。
//
// 与旧 flat snake_case 契约（message_id/content/...）的差异（全量收敛）：
//   - 字段名 camelCase + payload 嵌套（messageId/channelId/payload.text.content）。
//   - messageId 用 **long**（数值，全精度）——snowflake id 不可被 float64 截断，
//     reader 从 typed _source 读 int64 做 cursor tiebreaker（dsl.go::lastHitMessageID）。
//   - reader 必读的 **spaceId**(keyword) / **visibles**(keyword[]) / **messageSeq**(long)：
//     · spaceId：p2p (DM) space 召回过滤，缺则同 space 也 0 命中（reader fail-closed）。
//     · visibles：群系统消息「仅管理员可见」白名单 gate，缺则 reader fail-OPEN（V3b）。
//     · messageSeq：reader channel_offset「清空会话」gate（visibility.go），缺则保守隐藏。
//   - 排序字段 timestamp + messageId 与 reader cursor sort 口径一致。
//
// 方案 B（CDC 式写入）新增：
//   - **payloadRaw**：原始 payload 整包（json.RawMessage 原样存进 _source，mapping
//     enabled:false → 不索引、不建倒排）。以后 reader 加任何字段本地 reindex 即可，不回源 backfill。
//   - **payload.<type>.<field>** 全类型投影（text/image/gif/voice/video/file/mergeForward/richText），
//     照搬 CDC 蓝本 ToDoc，但 **visibility 不走 raw 直读**（见 buildPayloadFromRaw 与 §3 落点）。
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
	// PayloadRaw 是原始 payload 整包，原样留底（mapping payloadRaw enabled:false → 存 _source
	// 不索引）。仅当 payload 顶层是 JSON 对象时写（非对象在 visibility 预检即 fail-closed 落 DLQ，
	// 不会走到这里）。用 json.RawMessage 内联存原始字节（不二次转义）。
	PayloadRaw json.RawMessage `json:"payloadRaw,omitempty"`

	// 富文本(type=14)虚拟子文档字段（v1.10）。普通消息这三者为零值（omitempty 不输出）。
	// 子文档 payload.type∈{2,5,8}，结构与普通图片/视频/文件消息一致，让 reader 媒体/文件端点
	// 白名单（payload.type∈{2,5,8}）直接命中。撤回/编辑联动本期不做（路线甲：reader 用
	// parentMessageId 回 MySQL join 判可见性）。
	ParentMessageID   int64 `json:"parentMessageId,omitempty"`   // = 父 messageId（reader 可见性判定）
	ParentPayloadType int   `json:"parentPayloadType,omitempty"` // 父原 payload.type（=14）
	Virtual           bool  `json:"virtual,omitempty"`           // 标记本 doc 由富文本派生
	// SubSeq 是 reader 排序第三键（tiebreaker），保证 (messageId, subSeq) 全局唯一 →
	// (timestamp, messageId, subSeq) 唯一，search_after 不再丢同 tuple 的兄弟。
	// 普通 doc / 富文本父 doc = 0；富文本虚拟子文档从 1 递增（block 序号 i+1，父独占 0）。
	// **不用 omitempty**：普通 doc 要显式落盘 subSeq=0（reader 不赌「缺失=0」）。
	SubSeq int `json:"subSeq"`

	// Derivatives 是本父 doc 派生的虚拟子文档（type=14 富文本内嵌 image block → type=2）。
	// 本期**只派生 image→type=2**（上游契约 richText block 仅 text/image，file/video 全链路未打开）；
	// payload.type∈{5,8}（video/file）为前瞻预留，待上游契约打开后再扩展。
	// 仅顶层父 doc 持有；不序列化进 _source（json:"-"）。writer 编码 bulk body 时把每个父 doc
	// 与它的 Derivatives 编进**同一** _bulk 请求（父子相邻、同批原子），子文档幂等靠复合 _id。
	Derivatives []Doc `json:"-"`
	// idOverride 非空时作为 ES _id（子文档用复合键 "<父messageId>-rt<i>"）。普通 doc 留空，
	// idString() 回退到数值 messageId。json:"-" 不进 _source。
	idOverride string `json:"-"`
}

// Payload 是结构化正文投影（镜像 reader Doc.Payload + RichText 前瞻）。方案 B 后照搬 CDC
// 蓝本 ToDoc 投全 8 类 type（text=1/image=2/gif=3/voice=4/video=5/file=8/mergeForward=11/
// richText=14）；payloadRaw 兜底未投字段。
type Payload struct {
	Type         *int                 `json:"type,omitempty"`
	Text         *TextPayload         `json:"text,omitempty"`         // type=1
	Image        *ImagePayload        `json:"image,omitempty"`        // type=2
	Gif          *GifPayload          `json:"gif,omitempty"`          // type=3
	Voice        *VoicePayload        `json:"voice,omitempty"`        // type=4
	Video        *VideoPayload        `json:"video,omitempty"`        // type=5
	File         *FilePayload         `json:"file,omitempty"`         // type=8
	MergeForward *MergeForwardPayload `json:"mergeForward,omitempty"` // type=11
	RichText     *RichTextPayload     `json:"richText,omitempty"`     // type=14（前瞻，reader 暂未查）
}

// TextPayload 镜像 reader Doc.Payload.Text（IK 分词字段 payload.text.content）。
type TextPayload struct {
	Content string `json:"content,omitempty"`
}

// ImagePayload 镜像 reader Doc.Payload.Image（payload.image.{caption,name} 可搜 + url/宽高投影）。
type ImagePayload struct {
	URL     string `json:"url,omitempty"`
	Caption string `json:"caption,omitempty"`
	Name    string `json:"name,omitempty"`
	Width   int    `json:"width,omitempty"`
	Height  int    `json:"height,omitempty"`
}

// GifPayload 镜像 reader Doc.Payload.Gif（留底 url；reader 暂未查）。
type GifPayload struct {
	URL string `json:"url,omitempty"`
}

// VoicePayload 镜像 reader Doc.Payload.Voice（留底 url；reader 暂未查）。
type VoicePayload struct {
	URL string `json:"url,omitempty"`
}

// VideoPayload 镜像 reader Doc.Payload.Video（search_media 按 type=5 过滤）。
type VideoPayload struct {
	URL    string `json:"url,omitempty"`
	Cover  string `json:"cover,omitempty"`
	Width  int    `json:"width,omitempty"`
	Height int    `json:"height,omitempty"`
	Second int    `json:"second,omitempty"`
}

// FilePayload 镜像 reader Doc.Payload.File（payload.file.{caption,name,extension} 可搜 + url/size）。
//
// v1.12：新增 Content + ContentMeta 两字段，配套 cmd/file-extractor 独立服务抽取的文件正文写入
// （path: internal/fileextract → OS partial _update 只覆盖 payload.file.content + contentMeta，
// 不动其他字段）。Content 走 mapping IK 分词器 + `_source.excludes` 排除 —— 搜命中走倒排，
// _source 不膨胀（详见 internal/esindex/mapping/README.md v1.12）。
type FilePayload struct {
	URL         string           `json:"url,omitempty"`
	Name        string           `json:"name,omitempty"`
	Caption     string           `json:"caption,omitempty"`
	Size        int64            `json:"size,omitempty"`
	Extension   string           `json:"extension,omitempty"`
	Content     string           `json:"content,omitempty"`     // v1.12
	ContentMeta *FileContentMeta `json:"contentMeta,omitempty"` // v1.12
}

// FileContentMeta 是 file-extractor 抽取过程元信息（v1.12）。指针类型 + omitempty 保证未抽取
// 的 file doc 该子对象缺席，与 mapping "contentMeta" object 默认无子字段兼容（IDX-2）。
//
// 字段语义：
//   - ExtractedAt: 抽取时刻（epoch second），对应 mapping date+epoch_second
//   - Extractor:   抽取器标识字符串（例 "tika/3.3.0"），mapping keyword
//   - Truncated:   Content 是否被截到 file-extractor 配置的 MaxContentBytes（256KB 默认）
//   - ExtractMs:   Tika 抽取耗时（毫秒），mapping long
type FileContentMeta struct {
	ExtractedAt int64  `json:"extractedAt,omitempty"`
	Extractor   string `json:"extractor,omitempty"`
	Truncated   bool   `json:"truncated,omitempty"`
	ExtractMs   int64  `json:"extractMs,omitempty"`
}

// MergeForwardPayload 镜像 reader Doc.Payload.MergeForward（合并转发，type=11）。
type MergeForwardPayload struct {
	ChildCount int               `json:"childCount,omitempty"`
	Msgs       []MergeForwardMsg `json:"msgs,omitempty"`
}

// MergeForwardMsg 镜像 reader Doc.Payload.MergeForward.Msgs（PR#425）。
//
// 🔴 命名校准（§5.2）：发送人字段锚 reader 契约的 **From `json:"from"`**（PR#425），
// **不是** CDC 蓝本的 `fromUid`。timestamp 与 reader 一致（秒级 epoch）。
type MergeForwardMsg struct {
	MessageID  int64  `json:"messageId,omitempty"`
	Type       int    `json:"type,omitempty"`
	SearchText string `json:"searchText,omitempty"`
	From       string `json:"from,omitempty"`      // 发送人 UID（锚 reader PR#425 `from`，非 CDC fromUid）
	Timestamp  int64  `json:"timestamp,omitempty"` // 发送时间（秒级 epoch，与顶层 timestamp 同单位）
}

// RichTextPayload 镜像 reader 前瞻字段（type=14 图文混排，content 数组收敛为单 searchText）。
// reader main 与 PR#425 的 Payload struct **暂无** RichText，DSL 也未查——本期前瞻投影 + mapping
// 就位，待 reader 加查询时无需再动 indexer。CI 防漂移门当前不覆盖本字段（§8.4）。
type RichTextPayload struct {
	SearchText string `json:"searchText,omitempty"`
}

// idString 返回 ES `_id`（= 规范化 message_id）。messageId 为数值 snowflake，
// FormatInt(ParseInt(s)) 与原串一致，保证 backfill 与实时 consumer 写同一 _id（幂等）。
func (d Doc) idString() string {
	if d.idOverride != "" {
		return d.idOverride
	}
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

// DocFromMessage 把 Kafka 契约消息（searchmsg.Message）转成 reader 可读 Doc。
//
// 方案 B（CDC 式写入）三分支分流（§3.2，按 RawPayload 优先判，自上而下短路）：
//
//	分支 A  len(msg.RawPayload) > 0（非加密新形态——无论正文能否投影）
//	   · visibility：**不在此解析**。由调用方（实时路径 = consumer.processBatch 预检；
//	     backfill = docFromRow）先调 searchmsg.ExtractVisibility fail-closed 并把结果回填进
//	     msg.SpaceID/msg.Visibles，DocFromMessage **只消费**回填值（唯一权威解析落点见 §3.4/§3.5）。
//	   · 正文：buildPayloadFromRaw(RawPayload)（纯投影，不碰 visibility）。能投则投；
//	     投不出任何 typed 子对象的未知 type → doc.RawExcluded=true（§5.5），但 visibility 已设、非 fail-OPEN。
//	   · payloadRaw 整包留底。
//	分支 B  len(msg.RawPayload) == 0 且 msg.RawExcluded == true（加密 DM：RawPayload=nil + RawExcluded=true）
//	   · 跳过解析（绝不把密文喂 parser）；spaceId/visibles 留空（加密 DM reader fail-closed 安全方向）；不投影。
//	分支 C  else（在飞老 v2：无 RawPayload、非加密、RawExcluded=false）
//	   · 过渡期信契约 msg.SpaceID/msg.Visibles（producer 富化仍在线，§3.3）；正文走旧 buildPayload。
//	   · 迁移窗 backfill 会用 ExtractVisibility 重解析覆盖重写本类 doc，故过渡期信契约是有限过渡、非永久信任洞。
//
// 🔴 DocFromMessage 函数体内**绝不出现** ExtractVisibility（唯一权威解析落点在调用方预检，§3.5）。
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
		// spaceId/visibles：分支 A 消费调用方预检回填值；分支 C 信契约；分支 B 留空。
		SpaceID:     msg.SpaceID,
		Visibles:    msg.Visibles,
		Timestamp:   msg.MsgTimestamp,
		CreatedAt:   msg.CreatedAt,
		RawExcluded: msg.RawExcluded,
		Source:      msg.Source,
	}

	switch {
	case len(msg.RawPayload) > 0:
		// 分支 A：非加密新形态。纯正文投影 + payloadRaw 留底。
		payload, rawForStore, projected := buildPayloadFromRaw(msg.RawPayload)
		d.Payload = payload
		d.PayloadRaw = rawForStore
		// To 投影沿用蓝本 reversePeer 模式，入参取**契约** ChannelID/From（不从 payloadRaw 读，
		// 防恶意 payloadRaw 篡改 To，§5.2 U-2）；buildPayloadFromRaw 不产出 To。
		if d.ChannelType == channelTypePerson {
			d.To = reversePeer(d.ChannelID, d.From)
		}
		// RawExcluded 重定义（§5.5）：产出了 typed 可搜子对象 → false；未知 type 投不出 → true。
		d.RawExcluded = !projected
		// 富文本(type=14)虚拟子文档（v1.10）：按内嵌 image block 派生独立子 doc（本期只 image→type=2），
		// 与父同批原子写入。非富文本返回 nil（函数内部判 Payload.Type==14）。
		d.Derivatives = richTextDerivatives(d)
	case msg.RawExcluded:
		// 分支 B：加密 DM（RawPayload=nil + RawExcluded=true）。跳过解析、不投影、visibles 留空。
		d.Payload = nil
		d.RawExcluded = true
	default:
		// 分支 C：在飞老 v2（无 RawPayload、非加密）。信契约 visibles + 旧正文投影。
		d.Payload = buildPayload(msg.ContentType, msg.Content)
	}
	return d, nil
}

// buildPayload 构造 reader 可读的 payload 投影（**旧路径**：在飞老 v2 分支 C 用）。payload.type
// 始终带（reader _search_all 据此分流）；仅文本且 content 非空时填 payload.text.content（IK 分词）。
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

// SafetyFieldsSchemaVersion 是 Kafka 契约（searchmsg.Message）携带 reader 必读的安全/正确性
// 字段（SpaceID + Visibles + MessageSeq）的最小 SchemaVersion。当前契约 searchmsg.SchemaVersion
// **已 ==2**，故本 gate 恒 true、实时写入解封。
//
// 🔴 方案 B 语义重定义（§3.6）：本常量 + 下方 gate 仅保证「Kafka 契约 ≥ v2（带 RawPayload 投影
// 能力的最低契约版本）」。**方案 B 后，实时写入的 visibility fail-closed 安全保证来自消费侧
// consumer.processBatch 预检调 searchmsg.ExtractVisibility（§3.4），不再来自『契约是否带 visibles』。**
// 故此 gate 不再是 visibility 安全的充分条件，仅是契约版本下限闸；安全充分性由 §3.4 预检保证。
// 不 bump SchemaVersion（consumer 严格相等校验，bump=在飞 v2 消息全进 DLQ 风暴）。改 SchemaVersion 前必读本注释。
const SafetyFieldsSchemaVersion = 2

// LiveContractCarriesSafetyFields 报告当前编译进二进制的 Kafka 契约版本是否 ≥ v2。
//
// 🔴 方案 B 语义重定义（§3.6）：本 gate 仅是**契约版本下限闸**（≥ v2 即带 RawPayload 投影能力）。
// 实时写入的 visibility fail-closed 安全**不再**由本 gate 保证，而由消费侧 processBatch 预检调
// ExtractVisibility（§3.4）保证。consumer 据本 gate 决定是否允许实时写入（契约版本前置），但安全
// 充分性由预检负责，二者不可混淆。
func LiveContractCarriesSafetyFields() bool {
	return searchmsg.SchemaVersion >= SafetyFieldsSchemaVersion
}
