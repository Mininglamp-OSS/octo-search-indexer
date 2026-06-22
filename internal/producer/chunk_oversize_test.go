package producer

import (
	"strings"
	"testing"
)

// bigPayload 构造一个总体 >maxKafkaMessageBytes 的合法 JSON payload，content 字段可控大小。
func bigPayload(contentType int, contentSize int) []byte {
	filler := strings.Repeat("x", contentSize)
	if contentType == contentTypeText {
		return []byte(`{"type":1,"content":"` + filler + `"}`)
	}
	// 非文本（媒体）：把巨量放在一个非可搜字段，content 为空（degrade 后正文很小）。
	return []byte(`{"type":2,"name":"a.png","blob":"` + filler + `"}`)
}

// TestOversize_DegradeMediaToTextOnly §4.4：大 RawPayload + 小/无正文（媒体）→ degrade：清空
// RawPayload 发 text-only，进 main、不进 DLQ（cursor 仍前进）。
func TestOversize_DegradeMediaKeepsBody(t *testing.T) {
	// 媒体消息，RawPayload 巨大但 Content 为 nil（media raw_excluded）。
	row := &srcMessageRow{
		ID: 1, MessageID: "1", ChannelType: 2, CreatedUnix: 100,
		Payload: bigPayload(2, maxKafkaMessageBytes+50_000),
	}
	plan := planChunk("message", []*srcMessageRow{row}, 1_000_000_000)
	if len(plan.dlq) != 0 {
		t.Fatalf("media oversize must DEGRADE not DLQ, got %d dlq", len(plan.dlq))
	}
	if len(plan.main) != 1 {
		t.Fatalf("media oversize must still produce a body message, got %d", len(plan.main))
	}
	if len(plan.main[0].RawPayload) != 0 {
		t.Fatalf("degraded message must have RawPayload dropped, got %d bytes", len(plan.main[0].RawPayload))
	}
	if marshaledSize(plan.main[0]) > maxKafkaMessageBytes {
		t.Fatalf("degraded message must be under the Kafka limit, got %d", marshaledSize(plan.main[0]))
	}
}

// TestOversize_TextOnlyStillOversizeToDLQ §4.4：纯文本本身 >1MB（degrade 后仍超限）→ 落 DLQ，
// 且 DLQ 信封截断（RawPayload 抛弃、Detail 带截断标记），保证 DLQ 写入不超限。
func TestOversize_HugePlaintextToTruncatedDLQ(t *testing.T) {
	row := &srcMessageRow{
		ID: 1, MessageID: "1", ChannelType: 2, CreatedUnix: 100,
		Payload: bigPayload(contentTypeText, maxKafkaMessageBytes+50_000),
	}
	plan := planChunk("message", []*srcMessageRow{row}, 1_000_000_000)
	if len(plan.main) != 0 {
		t.Fatalf("huge plaintext must NOT go to body topic, got %d main", len(plan.main))
	}
	if len(plan.dlq) != 1 {
		t.Fatalf("huge plaintext must DLQ, got %d", len(plan.dlq))
	}
	env := plan.dlq[0]
	if env.Reason != dlqReasonOversize {
		t.Fatalf("oversize DLQ reason = %q, want %q", env.Reason, dlqReasonOversize)
	}
	if len(env.RawPayload) != 0 {
		t.Fatalf("oversize DLQ envelope must TRUNCATE RawPayload (else DLQ write blows the limit), got %d bytes", len(env.RawPayload))
	}
	if !strings.Contains(env.Detail, "payload_truncated") {
		t.Fatalf("oversize DLQ must carry truncation marker in Detail: %q", env.Detail)
	}
	// 源定位齐备（回灌从源表重取）。
	if env.ShardTable != "message" || env.SourceID != 1 || env.MessageID != "1" {
		t.Fatalf("oversize DLQ must keep source locator for replay: %+v", env)
	}
}

// TestOversize_NormalMessageUnchanged 正常小消息不受影响：RawPayload 保留、进 main。
func TestOversize_NormalKeepsRawPayload(t *testing.T) {
	row := &srcMessageRow{
		ID: 1, MessageID: "1", ChannelType: 2, CreatedUnix: 100,
		Payload: []byte(`{"type":1,"content":"小消息"}`),
	}
	plan := planChunk("message", []*srcMessageRow{row}, 1_000_000_000)
	if len(plan.main) != 1 || len(plan.dlq) != 0 {
		t.Fatalf("normal message must go to body unchanged: main=%d dlq=%d", len(plan.main), len(plan.dlq))
	}
	if len(plan.main[0].RawPayload) == 0 {
		t.Fatalf("normal message must retain RawPayload (Plan B)")
	}
}

// TestNewDLQEnvelope_OrdinaryOversizeTruncates §4.4（codex 补）：普通毒丸（visibility/parse 失败）
// 若源 payload 很大，普通 DLQ 信封也必须截断 RawPayload（否则 base64 膨胀过 1MiB → DLQ 写 wedge）。
func TestNewDLQEnvelope_OrdinaryOversizeTruncates(t *testing.T) {
	big := make([]byte, maxDLQRawPayloadBytes+1)
	for i := range big {
		big[i] = 'x'
	}
	row := &srcMessageRow{ID: 7, MessageID: "7", ChannelType: 2, Payload: big}
	env := newDLQEnvelope("message", row, dlqReasonVisibilityUntrusted)
	if len(env.RawPayload) != 0 {
		t.Fatalf("ordinary DLQ envelope with oversized payload must truncate RawPayload, got %d bytes", len(env.RawPayload))
	}
	if !strings.Contains(env.Detail, "payload_truncated") {
		t.Fatalf("truncation marker must be in Detail: %q", env.Detail)
	}
	// 源定位仍齐备（回灌从源表重取）。
	if env.ShardTable != "message" || env.SourceID != 7 {
		t.Fatalf("truncated DLQ must keep source locator: %+v", env)
	}

	// 对照：小 payload 不截断。
	small := &srcMessageRow{ID: 1, MessageID: "1", ChannelType: 2, Payload: []byte(`{"type":1}`)}
	env2 := newDLQEnvelope("message", small, dlqReasonPayloadUnparseable)
	if len(env2.RawPayload) == 0 {
		t.Fatalf("small payload DLQ must retain RawPayload verbatim")
	}
}
