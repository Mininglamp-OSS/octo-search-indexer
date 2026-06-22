package producer

// DLQEnvelope is the producer-specific dead-letter record.
//
// The body topic carries the octo-lib searchmsg.Message contract (a clean,
// indexable shape the es-indexer consumer reads). A DLQ record has a different
// job: forensic triage + replay. Serializing dead-lettered rows as a bare
// searchmsg.Message (the pre-envelope behavior) dropped the context an operator
// needs to understand and replay a poison row — WHY it failed, the raw source
// payload, and which shard row it came from. This envelope captures that.
//
// Shape rationale (kept consistent with the other DLQ schemas in this repo so a
// triage tool can reason across all three): the realtime consumer's dlqRecord
// (internal/consumer) and the backfill dlqRecord (internal/backfill) both key on
// {reason, source locator, raw bytes, timestamps}. This producer envelope mirrors
// that vocabulary — reason / shard_table / source_id / message_id / raw_payload /
// created_at — rather than reusing the body contract.
//
// It is intentionally NOT a searchmsg.Message: the DLQ stream is terminal (the
// es-indexer consumer never reads it), so the envelope is free to diverge from
// the body contract and optimize for replay instead of indexing.
type DLQEnvelope struct {
	// SchemaVersion identifies the DLQ envelope shape. Independent of the body
	// contract's searchmsg.SchemaVersion so the two can evolve separately.
	SchemaVersion int `json:"schema_version"`

	// Reason is the machine-readable dead-letter cause (see dlqReason* below),
	// the primary triage key.
	Reason string `json:"reason"`

	// ── source locator (replay: re-read this exact row) ──────────────────────
	// ShardTable is the source message shard the row was read from.
	ShardTable string `json:"shard_table"`
	// SourceID is the source row auto-increment id (message.id) within ShardTable.
	SourceID int64 `json:"source_id"`
	// MessageID is the business message id (= the would-be ES _id / Kafka key).
	// May be empty for a malformed row; SourceID+ShardTable still locate it.
	MessageID string `json:"message_id"`
	// ChannelID / ChannelType carry routing context (omitted when empty).
	ChannelID   string `json:"channel_id,omitempty"`
	ChannelType int    `json:"channel_type,omitempty"`

	// ── forensic payload (triage: see what could not be parsed) ──────────────
	// RawPayload is the original source payload bytes, preserved verbatim so a
	// replay/triage tool can re-attempt extraction or inspect the anomaly.
	//
	// 🔴 Plan B (§4.4): for the oversize-truncated reason this is intentionally
	// omitted (nil) so the DLQ record itself cannot blow the Kafka write limit and
	// wedge the partition. Detail then carries payloadTruncatedNote and replay must
	// re-fetch the row from the source table by {ShardTable, SourceID}.
	RawPayload []byte `json:"raw_payload,omitempty"`

	// Detail is a human-readable note (e.g. the oversize-truncation marker). Empty
	// for ordinary parse/visibility dead-letters whose RawPayload is preserved.
	Detail string `json:"detail,omitempty"`

	// ── timestamps ───────────────────────────────────────────────────────────
	// CreatedAt is the source row created_at (epoch seconds) — windowed triage.
	CreatedAt int64 `json:"created_at"`
	// MsgTimestamp is the source send time (epoch seconds), when present.
	MsgTimestamp int64 `json:"msg_timestamp,omitempty"`
	// ProducedAt is when this DLQ record was produced (epoch seconds). Stamped at
	// produce time, not at extraction time, so planChunk stays a pure function.
	ProducedAt int64 `json:"produced_at"`
}

// dlqSchemaVersion is the current DLQEnvelope schema version.
const dlqSchemaVersion = 1

// Dead-letter reasons (machine-readable triage keys). The producer namespaces
// them so a shared triage tool can tell producer DLQ rows apart from the
// consumer / backfill ones.
const (
	// dlqReasonPayloadUnparseable: a non-encrypted message whose payload is not
	// valid JSON / is an empty object (a genuine anomaly — encrypted messages are
	// raw_excluded on the body topic, never dead-lettered).
	dlqReasonPayloadUnparseable = "producer_payload_unparseable"
	// dlqReasonVisibilityUntrusted: the visibility ACL could not be trusted
	// (fail-closed #1124 guard) — the row is dead-lettered rather than written
	// with empty visibles (which the reader would treat as fail-OPEN).
	dlqReasonVisibilityUntrusted = "producer_visibility_untrusted"
	// dlqReasonOversize: even after dropping RawPayload the body is still over the
	// Kafka write-side limit (rare: plaintext content itself >1MB). Dead-lettered
	// with a TRUNCATED envelope (RawPayload omitted) so the DLQ write cannot itself
	// blow the limit and wedge the partition (§4.4). Replay must re-fetch the row
	// from the source message table by {ShardTable, SourceID}.
	dlqReasonOversize = "producer_oversize_truncated"
)

// payloadTruncatedNote marks an oversize DLQ envelope whose RawPayload was dropped
// to keep the DLQ write under the broker limit. Stamped into Detail so a triage /
// replay tool knows it must re-read the source row instead of trusting RawPayload.
const payloadTruncatedNote = "payload_truncated:true (oversize; replay must re-fetch from source by shard_table+source_id)"

// maxDLQRawPayloadBytes is the truncation threshold for the raw payload retained
// in a producer DLQ envelope (§4.4). It MUST account for json.Marshal base64-
// expanding the []byte RawPayload (~1.33x) plus envelope overhead: if a poison
// row's payload is large, the ordinary DLQ envelope (visibility_untrusted /
// payload_unparseable, not just the oversize case) base64-expands past the Kafka
// 1MiB write limit → ProduceDLQ fails → cursor wedges. 700KB × 1.33 ≈ 931KB +
// envelope < 1MiB, with head-room. Same口径 as the consumer leg
// (internal/consumer/dlq.go maxDLQRawValueBytes).
const maxDLQRawPayloadBytes = 700_000

// newDLQEnvelope builds a dead-letter envelope from a source row + reason. It is
// a pure function (no clock): ProducedAt is stamped later, at produce time, by
// the Kafka sink — keeping planChunk deterministic and unit-testable.
//
// 🔴 Oversize truncation (§4.4): when the source payload itself is large, even a
// non-oversize dead-letter (visibility/parse failure) would embed it verbatim and
// the base64-expanded envelope could exceed the Kafka limit → DLQ write wedge. So
// any payload over maxDLQRawPayloadBytes is dropped here too (Detail marks it;
// replay re-fetches from the source table by {ShardTable, SourceID}).
func newDLQEnvelope(table string, row *srcMessageRow, reason string) DLQEnvelope {
	env := DLQEnvelope{
		SchemaVersion: dlqSchemaVersion,
		Reason:        reason,
		ShardTable:    table,
		SourceID:      row.ID,
		MessageID:     row.MessageID,
		ChannelID:     row.ChannelID,
		ChannelType:   int(row.ChannelType),
		RawPayload:    row.Payload,
		CreatedAt:     row.CreatedUnix,
		MsgTimestamp:  row.Timestamp,
	}
	if len(row.Payload) > maxDLQRawPayloadBytes {
		env.RawPayload = nil
		env.Detail = payloadTruncatedNote
	}
	return env
}

// newOversizeDLQEnvelope builds a TRUNCATED dead-letter envelope (§4.4): RawPayload
// is intentionally omitted so the DLQ record stays small and the DLQ write cannot
// itself exceed the Kafka write limit. Replay re-fetches the row from the source
// table by {ShardTable, SourceID}; Detail carries the truncation marker.
func newOversizeDLQEnvelope(table string, row *srcMessageRow) DLQEnvelope {
	return DLQEnvelope{
		SchemaVersion: dlqSchemaVersion,
		Reason:        dlqReasonOversize,
		ShardTable:    table,
		SourceID:      row.ID,
		MessageID:     row.MessageID,
		ChannelID:     row.ChannelID,
		ChannelType:   int(row.ChannelType),
		RawPayload:    nil, // truncated — do not embed the oversized payload
		Detail:        payloadTruncatedNote,
		CreatedAt:     row.CreatedUnix,
		MsgTimestamp:  row.Timestamp,
	}
}
