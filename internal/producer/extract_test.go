package producer

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// 🔴 These vectors are aligned with octo-server/modules/searchetl/payload_test.go
// and this repo's internal/backfill/extract_test.go: the producer's payload
// extraction must match the realtime producer + backfill (else the same message
// yields different docs, breaking _id=message_id idempotency). Any drift turns
// these tests red.

func mustJSON(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func textPayload(t *testing.T, content string) []byte {
	t.Helper()
	return mustJSON(t, map[string]interface{}{"type": contentTypeText, "content": content})
}

func signalSettingByte() uint8 { return 1 << 5 }

// TestExtract_ConstantsMatchOctoLib pins the two octo-lib-aligned constants.
func TestExtract_ConstantsMatchOctoLib(t *testing.T) {
	if contentTypeText != 1 {
		t.Fatalf("contentTypeText must equal octo-lib common.Text(=1), got %d", contentTypeText)
	}
	if signalSettingMask != 32 {
		t.Fatalf("signalSettingMask must equal config Signal bit (1<<5=32), got %d", signalSettingMask)
	}
}

// TestExtract_Text normal text → outcomeOK, content taken, not raw_excluded.
func TestExtract_Text(t *testing.T) {
	row := &srcMessageRow{MessageID: "m1", ChannelType: 2, Payload: textPayload(t, "hello 世界")}
	msg, outcome := extractMessage(row)
	if outcome != outcomeOK {
		t.Fatalf("want outcomeOK, got %v", outcome)
	}
	if msg.RawExcluded {
		t.Fatalf("text msg must not be raw_excluded")
	}
	if msg.Content == nil || *msg.Content != "hello 世界" {
		t.Fatalf("content mismatch: %+v", msg.Content)
	}
	if msg.SchemaVersion != searchmsg.SchemaVersion || msg.Source != searchmsg.SourceETLMessageTable {
		t.Fatalf("contract metadata not set: %+v", msg)
	}
}

// TestExtract_SignalViaSetting Signal (setting bit) → raw_excluded, content nil.
func TestExtract_SignalViaSetting(t *testing.T) {
	row := &srcMessageRow{MessageID: "m2", Setting: signalSettingByte(), Payload: []byte("ENCRYPTED-NOT-JSON")}
	msg, outcome := extractMessage(row)
	if outcome != outcomeRawExcluded || !msg.RawExcluded || msg.Content != nil {
		t.Fatalf("signal msg must be raw_excluded with nil content: outcome=%v msg=%+v", outcome, msg)
	}
}

// TestExtract_SignalViaColumn Signal (signal column) → raw_excluded even if JSON.
func TestExtract_SignalViaColumn(t *testing.T) {
	row := &srcMessageRow{MessageID: "m3", Signal: 1, Payload: textPayload(t, "ignored")}
	_, outcome := extractMessage(row)
	if outcome != outcomeRawExcluded {
		t.Fatalf("signal-column msg must be raw_excluded, got %v", outcome)
	}
}

// TestExtract_NonTextRawExcluded media (type=2) → raw_excluded.
func TestExtract_NonTextRawExcluded(t *testing.T) {
	b := mustJSON(t, map[string]interface{}{"type": 2, "url": "http://x/y.png"})
	row := &srcMessageRow{MessageID: "m4", Payload: b}
	_, outcome := extractMessage(row)
	if outcome != outcomeRawExcluded {
		t.Fatalf("want outcomeRawExcluded for non-text, got %v", outcome)
	}
}

// TestExtract_TextContentObjectRawExcluded type=Text but content object → raw_excluded.
func TestExtract_TextContentObjectRawExcluded(t *testing.T) {
	b := mustJSON(t, map[string]interface{}{"type": contentTypeText, "content": map[string]interface{}{"k": "v"}})
	row := &srcMessageRow{MessageID: "m5", Payload: b}
	msg, outcome := extractMessage(row)
	if outcome != outcomeRawExcluded || msg.Content != nil {
		t.Fatalf("want raw_excluded nil content for text/object content, got outcome=%v content=%v", outcome, msg.Content)
	}
}

// TestExtract_NonSignalBadJSON non-encrypted invalid JSON → outcomeDLQ.
func TestExtract_NonSignalBadJSON(t *testing.T) {
	row := &srcMessageRow{MessageID: "m6", Payload: []byte("{not valid json")}
	msg, outcome := extractMessage(row)
	if outcome != outcomeDLQ || msg.MessageID != "m6" {
		t.Fatalf("want outcomeDLQ keeping message_id, got outcome=%v id=%q", outcome, msg.MessageID)
	}
}

// TestExtract_EmptyMapDLQ empty JSON object → DLQ.
func TestExtract_EmptyMapDLQ(t *testing.T) {
	row := &srcMessageRow{MessageID: "m6b", Payload: []byte("{}")}
	if _, outcome := extractMessage(row); outcome != outcomeDLQ {
		t.Fatalf("empty map payload must be DLQ, got %v", outcome)
	}
}

// TestExtract_TypeAsFloat tolerate type as float64 (json default).
func TestExtract_TypeAsFloat(t *testing.T) {
	row := &srcMessageRow{MessageID: "m7", Payload: []byte(`{"type":1,"content":"x"}`)}
	msg, outcome := extractMessage(row)
	if outcome != outcomeOK || msg.Content == nil || *msg.Content != "x" {
		t.Fatalf("float64 type Text not handled: outcome=%v content=%v", outcome, msg.Content)
	}
}

// TestExtract_MessageSeqEnriched messageSeq is taken from the column into the contract.
func TestExtract_MessageSeqEnriched(t *testing.T) {
	row := &srcMessageRow{MessageID: "seq", MessageSeq: 4242, ChannelType: 2, Payload: []byte(`{"type":1,"content":"x"}`)}
	msg, outcome := extractMessage(row)
	if outcome != outcomeOK {
		t.Fatalf("want outcomeOK, got %v", outcome)
	}
	if msg.MessageSeq != 4242 {
		t.Fatalf("message_seq must be enriched into contract, got %d", msg.MessageSeq)
	}
}

// TestExtract_ValidVisiblesEnriched valid targeted system msg → main + visibles in contract.
func TestExtract_ValidVisiblesEnriched(t *testing.T) {
	row := &srcMessageRow{MessageID: "vis-ok", ChannelType: 2, Payload: []byte(`{"type":99,"content":"removed","visibles":["u_alice","u_bob"]}`)}
	msg, outcome := extractMessage(row)
	if outcome == outcomeDLQ {
		t.Fatalf("valid targeted system msg must not DLQ, got %v", outcome)
	}
	if !reflect.DeepEqual(msg.Visibles, []string{"u_alice", "u_bob"}) {
		t.Fatalf("visibles must be enriched, got %v", msg.Visibles)
	}
}

// TestExtract_NormalGroupChatBroadcast no visibles key → main broadcast, empty visibles.
func TestExtract_NormalGroupChatBroadcast(t *testing.T) {
	row := &srcMessageRow{MessageID: "chat", ChannelType: 2, Payload: []byte(`{"type":1,"content":"hi all"}`)}
	msg, outcome := extractMessage(row)
	if outcome != outcomeOK {
		t.Fatalf("normal group chat must be outcomeOK (broadcast), got %v", outcome)
	}
	if len(msg.Visibles) != 0 {
		t.Fatalf("broadcast msg must have empty visibles, got %v", msg.Visibles)
	}
}

// TestExtract_SharedFailClosedVectors runs octo-lib's shared fail-closed
// visibility vectors — producer + backfill lock the SAME vectors (prevents #1124
// from diverging across repos).
func TestExtract_SharedFailClosedVectors(t *testing.T) {
	for _, v := range searchmsg.FailClosedVisibilityVectors() {
		v := v
		t.Run(v.Name, func(t *testing.T) {
			row := &srcMessageRow{MessageID: "m_" + v.Name, ChannelType: 2, Payload: v.Payload}
			msg, outcome := extractMessage(row)
			if v.WantErr {
				if outcome != outcomeDLQ {
					t.Fatalf("%s: fail-closed must route to DLQ, got outcome=%v visibles=%v", v.Name, outcome, msg.Visibles)
				}
				if len(msg.Visibles) != 0 {
					t.Fatalf("%s: DLQ msg must not carry visibles, got %v", v.Name, msg.Visibles)
				}
				return
			}
			if outcome == outcomeDLQ {
				t.Fatalf("%s: must not DLQ, got outcome=%v", v.Name, outcome)
			}
			if msg.SpaceID != v.WantSpaceID {
				t.Fatalf("%s: spaceID=%q want %q", v.Name, msg.SpaceID, v.WantSpaceID)
			}
			if !reflect.DeepEqual(msg.Visibles, v.WantVisibles) {
				t.Fatalf("%s: visibles=%v want %v", v.Name, msg.Visibles, v.WantVisibles)
			}
		})
	}
}
