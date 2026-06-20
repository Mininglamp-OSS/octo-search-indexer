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
	RawPayload []byte `json:"raw_payload"`

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
)

// newDLQEnvelope builds a dead-letter envelope from a source row + reason. It is
// a pure function (no clock): ProducedAt is stamped later, at produce time, by
// the Kafka sink — keeping planChunk deterministic and unit-testable.
func newDLQEnvelope(table string, row *srcMessageRow, reason string) DLQEnvelope {
	return DLQEnvelope{
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
}
