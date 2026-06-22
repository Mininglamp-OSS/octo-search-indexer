package producer

import (
	"encoding/json"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// ── payload extraction constants (strictly aligned with the source module's
// payload.go and this repo's internal/backfill extract.go)──────────────────────
//
// 🔴 single source of truth lives in octo-lib: contentTypeText = common.Text = 1
// (common/msg.go); signalSettingMask = config.Setting.Signal bit (setting>>5 & 1,
// config/msg.go SettingFromUint8). We replicate these two constants in-place
// instead of importing octo-lib common/config: those packages would drag zap /
// redis / grpc / aws-sdk (200+ modules) into this slim "read MySQL -> enrich ->
// Kafka" service. The cost of replication is that we must stay in lockstep with
// the producer/backfill ports — extract_test.go locks both constants and reuses
// the SAME case vectors as internal/backfill, so any drift turns the tests red.
const (
	// contentTypeText corresponds to octo-lib common.Text (= 1, text message).
	contentTypeText = 1

	// signalSettingMask is the Signal-encryption bit (bit 5) in the setting byte,
	// matching config.Setting.ToUint8's encodeBool(s.Signal)<<5.
	signalSettingMask = 1 << 5
)

// extractOutcome is the three-state result of payload extraction (P1-d rule,
// aligned with the source producer + backfill extractOutcome):
//   - outcomeOK: parsed a searchable body → produce to the body topic.
//   - outcomeRawExcluded: known-unindexable (Signal-encrypted DM / non-text
//     structured content) — content=nil, raw_excluded=true, still produced to the
//     body topic (not a lost message, not DLQ).
//   - outcomeDLQ: a genuine anomaly that should have parsed but didn't — produce
//     to the DLQ topic; the cursor still advances over it.
type extractOutcome int

const (
	outcomeOK extractOutcome = iota
	outcomeRawExcluded
	outcomeDLQ
)

// srcMessageRow is one message-shard row (same shape as the source producer and
// this repo's backfill srcMessageRow). MessageSeq is uint64 (full precision) to
// match the message.message_seq BIGINT column and the v2 contract field.
type srcMessageRow struct {
	ID          int64
	MessageID   string
	MessageSeq  uint64
	FromUID     string
	ChannelID   string
	ChannelType uint8
	Setting     uint8 // setting bits (incl. the Signal encryption bit, bit 5)
	Signal      int   // dedicated signal column (written alongside the setting bit)
	Timestamp   int64 // send time (epoch seconds)
	CreatedUnix int64 // persist time (epoch seconds, = UNIX_TIMESTAMP(created_at))
	Payload     []byte
}

// extractMessage enriches one source row into the Kafka body contract
// (searchmsg.Message) + a three-state outcome + a dead-letter reason.
//
// The third return value is the machine-readable DLQ reason (one of the
// dlqReason* constants); it is non-empty ONLY when outcome == outcomeDLQ and is
// "" otherwise. The caller (planChunk) uses it to build a forensic DLQEnvelope —
// keeping the WHY of a dead-letter next to the row instead of re-deriving it.
//
// Branch-for-branch aligned with octo-server/modules/searchetl/payload.go:
//   - Signal-encrypted (setting bit OR signal column) → raw_excluded (do not try
//     to parse the ciphertext, avoid misclassifying it as broken JSON → DLQ).
//     spaceId/visibles stay empty (reader fail-closed, safe — body is excluded).
//   - non-Signal → payload should be plaintext JSON; parse failure / empty map
//     (a genuine anomaly) → DLQ (reason: payload unparseable).
//   - on parse success, classify by type (tolerating float64/int/json.Number):
//     · type=Text with string content → take it as the body (outcomeOK).
//     · non-Text or non-string content → conservative raw_excluded.
//
// 🔴 v2 enrichment + fail-closed visibility (prevents the #1124 leak): for
// non-encrypted messages we additionally extract SpaceID/Visibles via the SHARED
// octo-lib searchmsg.ExtractVisibility. If visibility cannot be trusted (payload
// not a JSON object, visibles present-but-unparseable or valid-but-empty) → the
// whole row goes to DLQ (reason: visibility untrusted); we NEVER write empty
// Visibles (the reader treats empty visibles as fail-OPEN). MessageSeq comes
// from the message.message_seq column.
func extractMessage(row *srcMessageRow) (searchmsg.Message, extractOutcome, string) {
	msg := searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     row.MessageID,
		ChannelID:     row.ChannelID,
		ChannelType:   int(row.ChannelType),
		FromUID:       row.FromUID,
		MsgTimestamp:  row.Timestamp,
		CreatedAt:     row.CreatedUnix,
		MessageSeq:    row.MessageSeq,
		Source:        searchmsg.SourceETLMessageTable,
	}

	if isSignalEncrypted(row) {
		// Signal-encrypted DM: payload is ciphertext, not parseable — expected,
		// not an anomaly. spaceId/visibles stay empty (reader fail-closed, safe).
		msg.RawExcluded = true
		msg.Content = nil
		return msg, outcomeRawExcluded, ""
	}

	var m map[string]interface{}
	if err := json.Unmarshal(row.Payload, &m); err != nil || len(m) == 0 {
		// Non-encrypted message should be plaintext JSON; parse failure / empty
		// map is a genuine anomaly → DLQ (cursor still advances).
		return msg, outcomeDLQ, dlqReasonPayloadUnparseable
	}

	// 🔴 Plan B (CDC-style writes): ship the raw payload整包 so the es-indexer
	// consumer does the full projection + visibility fail-closed parsing itself.
	// json.RawMessage inlines the original bytes (zero base64 bloat). Only set for
	// non-encrypted messages (encrypted DMs keep RawPayload=nil — ciphertext never
	// leaves; see the isSignalEncrypted branch above).
	msg.RawPayload = json.RawMessage(row.Payload)

	// 🔴 Fail-closed visibility (access-control ACL): if it cannot be trusted →
	// the whole row goes to DLQ, never write empty Visibles. A weird space_id JSON
	// type does not drag down valid visibles (ExtractVisibility isolates that).
	//
	// Plan B note: the es-indexer consumer is now the AUTHORITATIVE fail-closed
	// landing point (it re-parses RawPayload in processBatch pre-check). The
	// producer keeps enriching during the transition (belt-and-suspenders, §3.3):
	// both legs run the SAME octo-lib parser + FailClosedVisibilityVectors, so the
	// kou-jing (口径 / contract) never drifts; a row dead-lettered here never reaches
	// the body topic, so there is no double-DLQ. Producer enrichment is retired in a
	// separate ticket after the consumer is verified stable.
	spaceID, visibles, verr := searchmsg.ExtractVisibility(row.Payload)
	if verr != nil {
		return msg, outcomeDLQ, dlqReasonVisibilityUntrusted
	}
	msg.SpaceID = spaceID
	msg.Visibles = visibles

	contentType, isText := payloadType(m)
	msg.ContentType = contentType

	if !isText {
		// Non-text (media / system / rich structured content): the es-indexer
		// consumer projects it from RawPayload (Plan B). content stays nil here
		// (the body text field is text-only); RawExcluded is set conservatively for
		// the transition contract — the consumer recomputes RawExcluded from the
		// actual projection (§5.5), so a media row with RawPayload still gets a
		// searchable doc downstream.
		msg.RawExcluded = true
		msg.Content = nil
		return msg, outcomeRawExcluded, ""
	}

	c, ok := m["content"].(string)
	if !ok {
		// type=Text but content is not a string (e.g. a bot stuffing an object):
		// conservative raw_excluded, do not coerce.
		msg.RawExcluded = true
		msg.Content = nil
		return msg, outcomeRawExcluded, ""
	}

	content := c
	msg.Content = &content
	return msg, outcomeOK, ""
}

// isSignalEncrypted reports whether a message is Signal-encrypted: the setting
// Signal bit (bit 5) OR the dedicated signal column. Both are checked because
// historical writes set both the setting bit and the standalone signal column;
// either being true means encrypted (aligned with the source isSignalEncrypted).
func isSignalEncrypted(row *srcMessageRow) bool {
	if row.Signal != 0 {
		return true
	}
	return row.Setting&signalSettingMask != 0
}

// payloadType resolves the message type from the payload map (tolerating the
// float64 / int / json.Number deserializations, matching the source producer's
// payloadType / message.CoerceTextPayloadContent), and reports whether it is
// Text. Unknown type → contentType 0, isText=false.
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
