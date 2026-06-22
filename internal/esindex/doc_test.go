package esindex

import (
	"encoding/json"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// readerDoc 镜像 octo-server reader 的 source.go::Doc 关键字段（驱动 cross-repo 契约断言）。
// 用 reader 的 json tag 反序列化 indexer 写出的 doc，确保字段名/类型/嵌套逐字段对齐。
type readerDoc struct {
	MessageID   int64    `json:"messageId"`
	MessageSeq  uint64   `json:"messageSeq"`
	ChannelID   string   `json:"channelId"`
	ChannelType uint32   `json:"channelType"`
	SpaceID     string   `json:"spaceId"`
	Timestamp   int64    `json:"timestamp"`
	From        string   `json:"from"`
	Visibles    []string `json:"visibles"`
	Payload     *struct {
		Type *int `json:"type"`
		Text *struct {
			Content string `json:"content"`
		} `json:"text"`
	} `json:"payload"`
}

// branchAMsg 构造一条方案 B 分支 A 消息（非加密新形态，带 RawPayload 整包）。visibility 由
// 上游预检回填，这里直接给定（投影测试只关心正文投影；visibility 三分支另有专测）。
func branchAMsg(id, rawPayload string) searchmsg.Message {
	return searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     id,
		ChannelID:     "g_1",
		ChannelType:   2,
		FromUID:       "u_1",
		MsgTimestamp:  1700000000,
		CreatedAt:     1700000001,
		Source:        searchmsg.SourceETLMessageTable,
		RawPayload:    json.RawMessage(rawPayload),
	}
}

func textMsg(id string) searchmsg.Message {
	c := "你好 world"
	return searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     id,
		ChannelID:     "g_1",
		ChannelType:   2,
		FromUID:       "u_1",
		Content:       &c,
		ContentType:   payloadTypeText,
		MsgTimestamp:  1700000000,
		CreatedAt:     1700000001,
		Source:        searchmsg.SourceETLMessageTable,
	}
}

// TestDocFromMessage_ReaderShape 实时 consumer 路径写出的 doc 能被 reader 契约逐字段读出：
// camelCase + 嵌套 payload.text.content + messageId 全精度 long。
func TestDocFromMessage_ReaderShape(t *testing.T) {
	in := textMsg("305419896765432111") // 大于 2^53，验证 long 全精度不被 float64 截断
	d, err := DocFromMessage(in)
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var rd readerDoc
	if err := json.Unmarshal(b, &rd); err != nil {
		t.Fatalf("reader unmarshal: %v", err)
	}
	if rd.MessageID != 305419896765432111 {
		t.Fatalf("messageId precision lost: got %d", rd.MessageID)
	}
	if rd.ChannelID != "g_1" || rd.ChannelType != 2 || rd.From != "u_1" {
		t.Fatalf("routing/authz fields mismatch: %+v", rd)
	}
	if rd.Timestamp != 1700000000 {
		t.Fatalf("timestamp mismatch: %d", rd.Timestamp)
	}
	if rd.Payload == nil || rd.Payload.Text == nil || rd.Payload.Text.Content != "你好 world" {
		t.Fatalf("payload.text.content not aligned to reader: %+v", rd.Payload)
	}
	if rd.Payload.Type == nil || *rd.Payload.Type != payloadTypeText {
		t.Fatalf("payload.type missing")
	}
}

// TestDocFromMessage_PrecisionRoundTrip snowflake id 经 JSON 数值往返不丢精度
// （reader 从 typed _source 读 int64 做 cursor tiebreaker，2^53 以上必须全精度）。
func TestDocFromMessage_PrecisionRoundTrip(t *testing.T) {
	const big = "9223372036854775807" // int64 max
	d, err := DocFromMessage(textMsg(big))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	// 文档行必须以数值（非字符串）形式写 messageId，且不带引号。
	if got := string(b); !containsSub(got, `"messageId":9223372036854775807`) {
		t.Fatalf("messageId must serialize as full-precision numeric long: %s", got)
	}
	if d.idString() != big {
		t.Fatalf("idString round-trip mismatch: %q", d.idString())
	}
}

// TestDocFromMessage_NonNumericIsError 非数值 message_id → 错误（reader 读 long 无法对齐），
// 由调用方按毒丸处置，绝不静默落 0。
func TestDocFromMessage_NonNumericIsError(t *testing.T) {
	if _, err := DocFromMessage(textMsg("not-a-number")); err == nil {
		t.Fatalf("non-numeric message_id must error")
	}
}

// TestDocFromMessage_RawExcludedNoText raw_excluded（无 RawPayload，加密/老形态）→ 走分支 B：
// 不投影正文（Payload nil），但仍占一个 doc 且 rawExcluded 标志保留。
func TestDocFromMessage_RawExcludedNoText(t *testing.T) {
	m := textMsg("42")
	m.RawExcluded = true
	m.Content = nil
	m.ContentType = 2 // image
	// 无 RawPayload → 分支 B（加密 DM 形态）：不解析、不投影。
	d, err := DocFromMessage(m)
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	if d.Payload != nil {
		t.Fatalf("raw_excluded without RawPayload (branch B) must not carry a payload projection: %+v", d.Payload)
	}
	if !d.RawExcluded {
		t.Fatalf("rawExcluded flag must round-trip")
	}
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
