package producer

import (
	"encoding/json"
	"testing"
)

// TestDLQEnvelope_ReasonMapping pins the dead-letter reason returned by
// extractMessage for each genuine-anomaly path.
func TestDLQEnvelope_ReasonMapping(t *testing.T) {
	// bad JSON / empty map → payload unparseable.
	badJSON := &srcMessageRow{MessageID: "m-bad", Payload: []byte("{not json")}
	if _, outcome, reason := extractMessage(badJSON); outcome != outcomeDLQ || reason != dlqReasonPayloadUnparseable {
		t.Fatalf("bad json: outcome=%v reason=%q, want DLQ/%s", outcome, reason, dlqReasonPayloadUnparseable)
	}

	// non-DLQ paths must carry an empty reason.
	okRow := &srcMessageRow{MessageID: "m-ok", ChannelType: 2, Payload: []byte(`{"type":1,"content":"hi"}`)}
	if _, outcome, reason := extractMessage(okRow); outcome != outcomeOK || reason != "" {
		t.Fatalf("ok row: outcome=%v reason=%q, want OK/empty", outcome, reason)
	}
}

// TestDLQEnvelope_VisibilityUntrusted runs the shared fail-closed vectors and
// asserts the ones that DLQ carry the visibility-untrusted reason — so triage can
// tell a fail-closed ACL drop apart from a malformed payload.
func TestDLQEnvelope_VisibilityUntrusted(t *testing.T) {
	// A row with a present-but-empty visibles list is the canonical fail-closed
	// case (reader would treat empty visibles as fail-OPEN, so it is dead-lettered).
	row := &srcMessageRow{MessageID: "m-vis", ChannelType: 2, Payload: []byte(`{"type":99,"content":"x","visibles":[]}`)}
	_, outcome, reason := extractMessage(row)
	if outcome != outcomeDLQ {
		t.Fatalf("empty-visibles must DLQ, got %v", outcome)
	}
	if reason != dlqReasonVisibilityUntrusted {
		t.Fatalf("empty-visibles reason = %q, want %q", reason, dlqReasonVisibilityUntrusted)
	}
}

// TestDLQEnvelope_BuildAndMarshal locks the envelope build + JSON shape (the
// forensic fields the source contract dropped: reason / raw payload / source
// locator), and confirms ProducedAt is left for the sink (planChunk stays pure).
func TestDLQEnvelope_BuildAndMarshal(t *testing.T) {
	row := &srcMessageRow{
		ID: 42, MessageID: "msg-42", ChannelID: "c1", ChannelType: 2,
		Timestamp: 111, CreatedUnix: 222, Payload: []byte(`{"broken`),
	}
	env := newDLQEnvelope("message3", row, dlqReasonPayloadUnparseable)
	if env.SchemaVersion != dlqSchemaVersion {
		t.Fatalf("schema_version = %d, want %d", env.SchemaVersion, dlqSchemaVersion)
	}
	if env.ShardTable != "message3" || env.SourceID != 42 || env.MessageID != "msg-42" {
		t.Fatalf("source locator wrong: %+v", env)
	}
	if env.ChannelID != "c1" || env.ChannelType != 2 {
		t.Fatalf("routing context wrong: %+v", env)
	}
	if string(env.RawPayload) != `{"broken` {
		t.Fatalf("raw payload not preserved: %q", env.RawPayload)
	}
	if env.CreatedAt != 222 || env.MsgTimestamp != 111 {
		t.Fatalf("timestamps wrong: %+v", env)
	}
	if env.ProducedAt != 0 {
		t.Fatalf("ProducedAt must be stamped by the sink, not at build time, got %d", env.ProducedAt)
	}

	// JSON shape: forensic fields present + snake_case keys.
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{"schema_version", "reason", "shard_table", "source_id", "message_id", "raw_payload", "created_at", "produced_at"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("dlq envelope JSON missing key %q: %s", k, b)
		}
	}
}
