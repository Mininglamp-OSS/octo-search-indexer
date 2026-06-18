package backfill

import (
	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// docFromRow 把一行源消息抽取为 reader 可读的 esindex.Doc（backfill 富化路径）。
//
// 这是分叉 B 在 backfill 侧的核心：backfill 读**原始 MySQL payload**，因此能自源填全
// reader 必读但 Kafka 契约缺失的三字段：
//   - spaceId：从 payload.space_id 抽（octo-server 发送时由 enrichPayloadWithSpaceID 写入，
//     p2p/group/topic 均经服务端权威注入）。reader 对 p2p 缺 spaceId 走 fail-closed。
//   - visibles：从 payload.visibles 抽（群系统消息「仅管理员可见」白名单）。reader 缺则 fail-OPEN。
//   - messageSeq：从 message 表 message_seq 列（reader channel_offset「清空会话」gate）。
//
// 正文 outcome（OK/RawExcluded/DLQ）完全复用 extractMessage 的口径（与实时 producer 一致），
// 故 backfill 与实时增量对同一条消息产生**同形态 doc**（同 _id 幂等覆盖无害）。
func docFromRow(row *srcMessageRow) (esindex.Doc, extractOutcome, error) {
	msg, outcome := extractMessage(row)
	if outcome == outcomeDLQ {
		return esindex.Doc{}, outcome, nil
	}

	// 复用 esindex 的契约→Doc 转换（messageId 全精度 long / payload 嵌套 / 字段名对齐 reader）。
	doc, err := esindex.DocFromMessage(msg)
	if err != nil {
		// message_id 非数值：reader 读 long messageId 无法对齐 → 当真异常落 DLQ（不静默落 0）。
		return esindex.Doc{}, outcomeDLQ, err
	}

	// 富化 reader 必读的安全/正确性字段（仅 backfill 可做：见上）。
	doc.MessageSeq = uint64(row.MessageSeq)
	if row.MessageSeq < 0 {
		doc.MessageSeq = 0
	}
	// 加密消息 payload 是密文，不解析（与 extract 一致），spaceId/visibles 留空（reader fail-closed）。
	if !isSignalEncrypted(row) {
		// 🔴 可见性解析口径的**单一真源**是 octo-lib searchmsg.ExtractVisibility（票2 落地的共享
		// fail-closed parser）。backfill 必须 import 它、而非自实现——producer 与 backfill 跑同一
		// parser + 同一组 searchmsg.FailClosedVisibilityVectors()，锁口径防 #1124 在两仓分叉
		// （验收门 ii）。共享 parser 比旧自实现更严：对 valid-but-empty visibles（键在但空数组 /
		// null / 全非字符串）也 fail-closed，而旧自实现把这些当广播 fail-OPEN 放行。
		spaceID, visibles, verr := searchmsg.ExtractVisibility(row.Payload)
		if verr != nil {
			// 🔴 fail-CLOSED：可见性是 access-control ACL。一旦它无法可信解析（payload 非 JSON 对象、
			// visibles 非数组 / valid-but-empty 等结构性损坏），**绝不**写空 visibles —— 否则 reader
			// 把空 visibles 当 fail-OPEN（群系统消息「仅管理员可见」白名单门被移除，普通成员能搜到
			// 管理员可见消息）。这类行一律落 DLQ（永久跳过写入），把 ACL 解析失败显式暴露给对账门而非
			// 静默塌掉。
			//
			// 注意：space_id 的 JSON 类型（数字/对象/上游漂移）**不**触发此路径——ExtractVisibility 把
			// space_id 解成容忍类型，非字符串 space_id 退化为空 spaceId（reader p2p fail-closed，安全
			// 方向），绝不因一个字段类型怪异连累合法的 visibles 一并清空（V3b fail-OPEN 的根因）。
			return esindex.Doc{}, outcomeDLQ, verr
		}
		doc.SpaceID = spaceID
		doc.Visibles = visibles
	}
	return doc, outcome, nil
}
