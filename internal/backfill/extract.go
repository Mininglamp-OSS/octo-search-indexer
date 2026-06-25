// Package backfill 是 YUJ-4534 阶段 6 的历史消息回灌（backfill）作业：读 MySQL message
// 5 分表 → 复用 internal/esindex 写入器直接幂等 bulk 写 OpenSearch，**绕开 Kafka**。
//
// 在 9 阶段管线中的位置（与实时增量并行、互不冲突）：
//
//	实时增量： message 分表 → searchetl(producer) → Kafka → es-indexer consumer → ES
//	历史回灌： message 分表 → 【backfill 本作业：直接 esindex.Writer bulk】 → ES
//
// 设计纪律（阶段 6 plan + 阶段 6 backfill 设计细化 comment）：
//   - **复用** internal/esindex 写入器（同一套 bulk 逻辑 / mapping / `_id=message_id`），
//     严禁重新实现写入器。
//   - **幂等**：ES doc `_id=message_id`，同一条消息即使既被 backfill 又被实时增量写也是覆盖
//     同一 doc，无双写 / 无 clobber。backfill 与 live-ingest 可并行 / 重叠安全运行。
//   - **绕开 Kafka**：历史量级（315 万行 / ~2.14GB）无需经 Kafka，直灌 ES 更快且无
//     retention 删数风险。撤回 / 删除态不进 ES（路线甲，读时回 MySQL join 过滤），故 backfill
//     无需感知历史撤回 / 删除。
//   - **DLQ 账目**：在 bypass-Kafka 路径下，真异常（本应可解析却失败的 payload）没有 Kafka DLQ
//     topic 可投——backfill 把这类行落到本地 DLQ spill 文件并**精确计数**，该计数作为对账门
//     权威输入（"ES 去重 + DLQ + 已知排除 == 源行数"），使合法路由进 DLQ 的消息**不被误判为
//     ES 缺失**。
package backfill

import (
	"encoding/json"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// ── payload 抽取常量（与 octo-server/modules/searchetl/payload.go 严格对齐）─────────────
//
// 🔴 单一真源在 octo-lib：contentTypeText = common.Text = 1（common/msg.go），
// signalSettingBit = config.Setting.Signal 位（setting >> 5 & 1，config/msg.go SettingFromUint8）。
// 这里**就地复刻**这两个常量而非 import octo-lib common/config：那两个包会把 zap/redis/grpc/
// aws-sdk 等 200+ 模块拖进本「只消费 Kafka / 写 ES」的精简镜像，得不偿失。复刻的代价是
// 必须与 producer 口径保持一致——extract_test.go 用与 producer payload_test.go **完全相同**的
// 用例向量锁住这一致性（任一侧改口径，测试即红）。
const (
	// contentTypeText 对应 octo-lib common.Text（= 1，文本消息）。只有 Text 且 content 为
	// string 才抽取正文；其余类型保守 raw_excluded。
	contentTypeText = 1

	// signalSettingMask 是 setting 字节里 Signal 加密位的掩码（bit 5），与 config.Setting.ToUint8
	// 的 `encodeBool(s.Signal)<<5` 一致。setting & mask != 0 即 Signal 加密。
	signalSettingMask = 1 << 5
)

// extractOutcome 是 payload 抽取的三态结果（P1-d 规则，与 producer extractOutcome 对齐）：
//   - outcomeOK：正常解析出可检索正文 → 进 ES 正文流。
//   - outcomeRawExcluded：已知不可索引类（Signal 加密 DM / 非文本结构化内容）——content=nil、
//     raw_excluded=true，**仍写入 ES** 占一个 doc（不算丢消息、不进 DLQ）。
//   - outcomeDLQ：本应可解析却解析失败的真异常——不写 ES，落本地 DLQ spill 并计数。
type extractOutcome int

const (
	outcomeOK extractOutcome = iota
	outcomeRawExcluded
	outcomeDLQ
)

// srcMessageRow 是 message 分表读出的单条消息（与 producer srcMessageRow 同构）。
type srcMessageRow struct {
	ID          int64
	MessageID   string
	MessageSeq  int64 // 消息序列号（严格递增）— reader channel_offset「清空会话」gate 用
	FromUID     string
	ChannelID   string
	ChannelType uint8
	Setting     uint8  // 消息 setting 位（含 Signal 加密位，bit 5）
	Signal      int    // 专用 signal 加密列（webhook 落库时与 setting 位一并写入）
	Timestamp   int64  // 发送时间（纪元秒）
	CreatedUnix int64  // 落库时间（纪元秒, = UNIX_TIMESTAMP(created_at)）
	Payload     []byte // 消息 payload（!Signal 时为明文 JSON）
}

// extractMessage 把一行源消息抽取为检索契约（searchmsg.Message）+ 三态 outcome。
//
// 口径与 octo-server/modules/searchetl/payload.go 的 extractMessage **逐分支对齐**：
//   - Signal 加密（setting 位 或 signal 列为真）→ raw_excluded（不尝试解析密文，避免误判 DLQ）。
//   - 非 Signal → payload 应为明文 JSON；解析失败 / 空 map（本应可解析却失败的真异常）→ DLQ。
//   - 解析成功后按 type（兼容 json 反序列化的 float64 / 内部 int / json.Number）：
//     · type=Text 且 content 为 string → 取该 string 作正文（outcomeOK）。
//     · 非 Text 或 content 非 string（媒体 / 富文本 / 结构化对象）→ 保守 raw_excluded。
func extractMessage(row *srcMessageRow) (searchmsg.Message, extractOutcome) {
	// MessageSeq 必须在此处填入：DocFromMessage 在内部派生富文本虚拟子文档时按值快照父
	// （deriveChild: child := parent），若在 DocFromMessage 之后才富化父的 messageSeq，
	// 子文档会停留在 messageSeq=0，违反「子继承父时序字段」契约（见 #26）。
	// 负数（脏数据）兜 0，与 docFromRow 旧富化口径一致。
	msgSeq := uint64(0)
	if row.MessageSeq > 0 {
		msgSeq = uint64(row.MessageSeq)
	}
	msg := searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     row.MessageID,
		MessageSeq:    msgSeq,
		ChannelID:     row.ChannelID,
		ChannelType:   int(row.ChannelType),
		FromUID:       row.FromUID,
		MsgTimestamp:  row.Timestamp,
		CreatedAt:     row.CreatedUnix,
		Source:        searchmsg.SourceETLMessageTable,
	}

	if isSignalEncrypted(row) {
		// Signal 加密 DM：payload 是密文，解不出明文是预期行为，非异常。
		msg.RawExcluded = true
		msg.Content = nil
		return msg, outcomeRawExcluded
	}

	var m map[string]interface{}
	if err := json.Unmarshal(row.Payload, &m); err != nil || len(m) == 0 {
		// 非加密消息本应是明文 JSON，解析失败 / 空 map 属真异常 → DLQ。
		return msg, outcomeDLQ
	}

	contentType, isText := payloadType(m)
	msg.ContentType = contentType

	if !isText {
		// 非文本（媒体 / 系统 / 富文本等结构化内容）：本期不索引，raw_excluded。
		msg.RawExcluded = true
		msg.Content = nil
		return msg, outcomeRawExcluded
	}

	c, ok := m["content"].(string)
	if !ok {
		// type=Text 但 content 非 string（如 bot 误塞 object）：保守 raw_excluded，不强转。
		msg.RawExcluded = true
		msg.Content = nil
		return msg, outcomeRawExcluded
	}

	content := c
	msg.Content = &content
	return msg, outcomeOK
}

// isSignalEncrypted 判定消息是否 Signal 加密：setting 的 Signal 位（bit 5）或专用 signal 列。
// 两个来源都查是因为历史落库既写 setting 位也写独立 signal 列，任一为真即视为加密
// （与 producer isSignalEncrypted 一致）。
func isSignalEncrypted(row *srcMessageRow) bool {
	if row.Signal != 0 {
		return true
	}
	return row.Setting&signalSettingMask != 0
}

// payloadType 从 payload map 解出消息类型（兼容 float64 / int / json.Number 三种反序列化结果，
// 与 producer payloadType / message.CoerceTextPayloadContent 口径一致），并返回是否为 Text。
// 无法识别类型时 contentType 返回 0、isText=false。
func payloadType(m map[string]interface{}) (contentType int, isText bool) {
	switch v := m["type"].(type) {
	case float64:
		contentType = int(v)
	case int:
		contentType = v
	case json.Number:
		if i, err := v.Int64(); err == nil {
			contentType = int(i)
		}
	}
	return contentType, contentType == contentTypeText
}
