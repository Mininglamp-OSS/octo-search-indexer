package producer

import "github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"

// chunkPlan is the delivery plan produced from one batch of read source rows
// after the stability gate + payload extraction (a pure-function product, no IO).
// The caller produces main + dlq (all confirmed) THEN advances the cursor to maxID.
type chunkPlan struct {
	// main goes to the body topic (outcomeOK bodies + outcomeRawExcluded
	// content=null messages).
	main []searchmsg.Message
	// dlq goes to the DLQ topic (outcomeDLQ: genuine anomalies).
	dlq []searchmsg.Message
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
// main, DLQ → dlq).
//
// Pure function: rows + cutoff in, plan out, touches no DB/Kafka. The cursor
// watermark is the last stable-prefix row id regardless of outcome — raw_excluded
// / DLQ rows are consumed too, their ids must count toward the watermark.
func planChunk(rows []*srcMessageRow, cutoff int64) chunkPlan {
	stable := stablePrefix(rows, cutoff)
	plan := chunkPlan{stableCount: len(stable)}
	if len(stable) == 0 {
		return plan
	}
	plan.main = make([]searchmsg.Message, 0, len(stable))
	for _, row := range stable {
		msg, outcome := extractMessage(row)
		switch outcome {
		case outcomeDLQ:
			plan.dlq = append(plan.dlq, msg)
		default: // outcomeOK / outcomeRawExcluded both go to the body stream
			plan.main = append(plan.main, msg)
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
