package producer

import (
	"encoding/json"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// maxKafkaMessageBytes 是单条 Kafka 消息体的体积上限（留头部余量低于 broker
// message.max.bytes 默认 1MiB=1048576）。方案 B 让 producer 发原始 payload 整包，单条体积可能
// 逼近硬限；segmentio kafka-go 写侧超限 WriteMessages 失败 → cursor 不前进 → 死循环卡 partition
// （注：consumer fetch 侧已 10MB，本风险是写侧/broker 侧）。故组装时按本阈值降级。
const maxKafkaMessageBytes = 1_000_000

// chunkPlan is the delivery plan produced from one batch of read source rows
// after the stability gate + payload extraction (a pure-function product, no IO).
// The caller produces main + dlq (all confirmed) THEN advances the cursor to maxID.
type chunkPlan struct {
	// main goes to the body topic (outcomeOK bodies + outcomeRawExcluded
	// content=null messages) as searchmsg.Message contracts.
	main []searchmsg.Message
	// dlq goes to the DLQ topic (outcomeDLQ: genuine anomalies) as forensic
	// DLQEnvelope records (reason + raw payload + source locator), NOT bare body
	// contracts — the DLQ stream is terminal and optimized for triage/replay.
	dlq []DLQEnvelope
	// stableCount is the number of rows in the stable prefix (= len(main)+len(dlq);
	// used to decide whether the unstable tail was reached).
	stableCount int
	// maxID is the message-table auto id of the last stable-prefix row; the cursor
	// may advance only to here (never to the batch end, C1). 0 when none.
	maxID int64
	// advanced reports the stable prefix is non-empty and yields an advanceable id.
	advanced bool
}

// planChunk applies, over an id-ascending batch: ① the stability gate truncation
// (C1, cutoff=DB_NOW-lag) → ② per-row extraction three-state routing (OK/Raw →
// main as searchmsg.Message, DLQ → dlq as a forensic DLQEnvelope tagged with the
// dead-letter reason + source locator).
//
// Pure function: rows + cutoff in, plan out, touches no DB/Kafka/clock. The
// cursor watermark is the last stable-prefix row id regardless of outcome —
// raw_excluded / DLQ rows are consumed too, their ids must count toward the
// watermark. The DLQ envelope's produce timestamp is stamped later by the sink so
// this stays deterministic.
func planChunk(table string, rows []*srcMessageRow, cutoff int64) chunkPlan {
	stable := stablePrefix(rows, cutoff)
	plan := chunkPlan{stableCount: len(stable)}
	if len(stable) == 0 {
		return plan
	}
	plan.main = make([]searchmsg.Message, 0, len(stable))
	for _, row := range stable {
		msg, outcome, reason := extractMessage(row)
		switch outcome {
		case outcomeDLQ:
			plan.dlq = append(plan.dlq, newDLQEnvelope(table, row, reason))
		default: // outcomeOK / outcomeRawExcluded both go to the body stream
			// 🔴 Plan B oversize guard (§4.4): the raw payload整包 can push a single
			// contract message past the Kafka write-side hard limit. Degrade (drop
			// RawPayload, keep text-only) rather than discard; if still oversized
			// (rare: plaintext itself >1MB) dead-letter with a TRUNCATED envelope so
			// the DLQ write cannot itself blow the limit and wedge the partition.
			if degraded, oversized := degradeIfOversized(msg); oversized {
				plan.dlq = append(plan.dlq, newOversizeDLQEnvelope(table, row))
			} else {
				plan.main = append(plan.main, degraded)
			}
		}
	}
	plan.maxID = stable[len(stable)-1].ID
	plan.advanced = true
	return plan
}

// stablePrefix takes the no-gap prefix of an id-ascending batch whose rows have
// been persisted longer than lag (hard condition C1).
//
// message.id is assigned on INSERT but only visible on COMMIT; commit order !=
// id order. id and created_at are both assigned at insert time (near-sequential),
// so the first unstable row (CreatedUnix > cutoff) and everything after it (higher
// id) is treated as unstable — truncating there gives a no-gap stable prefix.
//
// The cursor may advance only to the prefix end, never the batch end — otherwise
// it would skip a "low id, late-committed, not yet lag-stable" row permanently
// (message_id idempotency cannot recover it; those messages were never produced).
func stablePrefix(rows []*srcMessageRow, cutoff int64) []*srcMessageRow {
	for i, r := range rows {
		if r.CreatedUnix > cutoff {
			return rows[:i]
		}
	}
	return rows
}

// degradeIfOversized applies the Plan B oversize guard (§4.4) to one body message
// before it joins the produce batch:
//   - fits under maxKafkaMessageBytes → return unchanged (oversized=false).
//   - else DEGRADE not discard: drop RawPayload and re-check — keeps the text-only
//     body searchable (avoids the v0 "discard → plaintext also unsearchable"
//     regression). Now fits → return the degraded message (oversized=false).
//   - STILL overflows (rare: plaintext content itself >1MB) → oversized=true; the
//     caller dead-letters it with a truncated envelope so the DLQ write itself
//     cannot blow the limit and wedge the partition.
//
// Pure function (json.Marshal only, no IO/clock) so planChunk stays deterministic.
func degradeIfOversized(msg searchmsg.Message) (searchmsg.Message, bool) {
	if marshaledSize(msg) <= maxKafkaMessageBytes {
		return msg, false
	}
	// Degrade: drop the raw payload整包, keep the text-only body. RawExcluded stays
	// as extractMessage set it.
	msg.RawPayload = nil
	if marshaledSize(msg) <= maxKafkaMessageBytes {
		return msg, false
	}
	return msg, true
}

// marshaledSize returns the JSON wire size of a contract message (the exact bytes
// produced for Kafka). On a marshal error (encoding bug, should not happen) it
// returns over-the-limit so the guard degrades/dead-letters conservatively.
func marshaledSize(msg searchmsg.Message) int {
	b, err := json.Marshal(msg)
	if err != nil {
		return maxKafkaMessageBytes + 1
	}
	return len(b)
}

// firstNonAscendingByID returns the index of the first row not strictly ascending
// by id (rows[i].ID <= rows[i-1].ID), or -1 if all ascending.
//
// stablePrefix's no-gap guarantee assumes the batch is strictly id-ascending
// (the ORDER BY id ASC). If an index/DB change silently breaks return order,
// stablePrefix would truncate at the wrong place → silent missed read. This makes
// that assumption an observable tripwire (the caller warns, does not change the
// correctness path).
func firstNonAscendingByID(rows []*srcMessageRow) int {
	for i := 1; i < len(rows); i++ {
		if rows[i].ID <= rows[i-1].ID {
			return i
		}
	}
	return -1
}
