package backfill

import (
	"encoding/json"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// docFromRow 把一行源消息抽取为 reader 可读的 esindex.Doc（backfill 富化路径）。
//
// 方案 B（CDC 式写入）：backfill 是**存量路径的 visibility 权威解析落点**（与实时路径的
// consumer.processBatch 预检对称）。它读**原始 MySQL payload**，故能：
//   - 把 RawPayload 整包带进契约 → esindex.DocFromMessage 走分支 A：buildPayloadFromRaw 全类型
//     投影 + payloadRaw 留底（与实时增量同形态 doc，同 _id 幂等覆盖无害）。
//   - 用 octo-lib searchmsg.ExtractVisibility（共享 fail-closed parser 单一真源）自源解析
//     spaceId/visibles，**先回填进契约** msg.SpaceID/msg.Visibles，再交 DocFromMessage 消费。
//     DocFromMessage 分支 A **不二次解析**（唯一权威落点在这里）。
//   - messageSeq 从 message 表 message_seq 列。
//
// 加密消息（密文）走分支 B：RawPayload=nil + RawExcluded=true，跳过解析、visibles 留空
// （reader fail-closed 安全方向）。
func docFromRow(row *srcMessageRow) (esindex.Doc, extractOutcome, error) {
	msg, outcome := extractMessage(row)
	if outcome == outcomeDLQ {
		return esindex.Doc{}, outcome, nil
	}

	if !isSignalEncrypted(row) {
		// 🔴 可见性解析口径的**单一真源**是 octo-lib searchmsg.ExtractVisibility（共享 fail-closed
		// parser）。backfill 与实时跑同一 parser + 同一组 FailClosedVisibilityVectors()，锁口径防
		// #1124 分叉。fail-CLOSED：可见性无法可信解析（payload 非 JSON 对象 / visibles 非数组 /
		// valid-but-empty）→ **绝不**写空 visibles（reader fail-OPEN），该行落 DLQ。
		// 注：space_id 的 JSON 类型怪异不触发此路径（ExtractVisibility 容忍隔离），不连累合法 visibles。
		spaceID, visibles, verr := searchmsg.ExtractVisibility(row.Payload)
		if verr != nil {
			return esindex.Doc{}, outcomeDLQ, verr
		}
		// 先回填进契约（DocFromMessage 分支 A 直接消费，不二次解析）。
		msg.SpaceID = spaceID
		msg.Visibles = visibles
		// 带 RawPayload 整包 → DocFromMessage 走分支 A 全类型投影 + payloadRaw 留底。
		msg.RawPayload = json.RawMessage(row.Payload)
	}

	// 复用 esindex 的契约→Doc 转换（messageId 全精度 long / 全类型投影 / payloadRaw / 字段对齐 reader）。
	doc, err := esindex.DocFromMessage(msg)
	if err != nil {
		// message_id 非数值：reader 读 long messageId 无法对齐 → 当真异常落 DLQ（不静默落 0）。
		return esindex.Doc{}, outcomeDLQ, err
	}

	// 富化 messageSeq（仅 backfill 可从 message 表自源填）。
	doc.MessageSeq = uint64(row.MessageSeq)
	if row.MessageSeq < 0 {
		doc.MessageSeq = 0
	}
	return doc, outcome, nil
}
