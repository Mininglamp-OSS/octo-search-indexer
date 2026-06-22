// Command gaptable seeds the plan v2.1 gap-table verification vectors into the
// harness Kafka topic, exercising the Plan B (CDC-style) RawPayload projection +
// consumer-side visibility fail-closed path end to end.
//
// Each message is a searchmsg.Message carrying RawPayload (the raw payload整包),
// so the es-indexer consumer does: ExtractVisibility pre-check (fail-closed) +
// buildPayloadFromRaw projection + payloadRaw retention. This is the live path
// the gap table must be checked against.
//
// ISOLATION: connects only to the local harness Kafka (KAFKA_BROKERS, default
// localhost:19092). Never points at a shared environment.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/segmentio/kafka-go"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	brokers := flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:19092"), "kafka brokers (csv)")
	topic := flag.String("topic", envOr("KAFKA_TOPIC", "octo.message.v1"), "body topic")
	flag.Parse()

	w := &kafka.Writer{
		Addr:                   kafka.TCP(splitCSV(*brokers)...),
		Topic:                  *topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: true,
	}
	defer func() { _ = w.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	base := time.Now().Unix()
	msgs := vectors(base)
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		log.Fatalf("produce: %v", err)
	}
	log.Printf("gaptable: seeded %d vectors to %s", len(msgs), *topic)
}

// rawMsg builds a non-encrypted Plan B contract message: RawPayload carries the
// raw payload整包; the consumer projects + extracts visibility from it.
func rawMsg(id, channelID string, channelType int, from string, seq uint64, ts int64, rawPayload string) kafka.Message {
	m := searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     id,
		ChannelID:     channelID,
		ChannelType:   channelType,
		FromUID:       from,
		MessageSeq:    seq,
		MsgTimestamp:  ts,
		CreatedAt:     ts,
		RawPayload:    json.RawMessage(rawPayload),
		Source:        searchmsg.SourceETLMessageTable,
	}
	b, err := json.Marshal(m)
	if err != nil {
		log.Fatalf("marshal %s: %v", id, err)
	}
	return kafka.Message{Key: []byte(id), Value: b}
}

func vectors(base int64) []kafka.Message {
	var out []kafka.Message
	// ---- Gap-table positive recall vectors (each via RawPayload projection) ----
	// G1 image: payload.image.{caption,name}
	out = append(out, rawMsg("3000000000000000001", "g_gap", 2, "u_img", 11, base,
		`{"type":2,"name":"季度合同.png","caption":"图片说明文字甲","url":"https://x/a.png","width":800,"height":600}`))
	// G2 file: payload.file.{caption,name,extension}
	out = append(out, rawMsg("3000000000000000002", "g_gap", 2, "u_file", 12, base,
		`{"type":8,"name":"年度报告.pdf","caption":"文件说明文字乙","url":"https://x/r.pdf","size":123456,"extension":"pdf"}`))
	// G3 mergeForward: payload.mergeForward.msgs.{searchText,from,timestamp}
	out = append(out, rawMsg("3000000000000000003", "g_gap", 2, "u_mf", 13, base,
		`{"type":11,"msgs":[{"message_id":"9007199254740993","from_uid":"u_inner_丙","timestamp":1700000123,"payload":{"type":1,"content":"转发卡内文字丙"}},{"message_id":"9007199254740994","from_uid":"u_inner_丁","timestamp":1700000456,"payload":{"type":8,"name":"内层文件丁.doc"}}]}`))
	// G4 voice (projection present)
	out = append(out, rawMsg("3000000000000000004", "g_gap", 2, "u_voice", 14, base,
		`{"type":4,"url":"https://x/v.mp3"}`))
	// G5 video (projection present)
	out = append(out, rawMsg("3000000000000000005", "g_gap", 2, "u_video", 15, base,
		`{"type":5,"url":"https://x/v.mp4","cover":"https://x/c.jpg","width":1920,"height":1080,"second":42}`))
	// G6 richtext: payload.richText.searchText
	out = append(out, rawMsg("3000000000000000006", "g_gap", 2, "u_rich", 16, base,
		`{"type":14,"content":[{"type":"text","text":"富文本正文戊"},{"type":"image","name":"富文本图片己.png"}]}`))
	// G7 text top-level fields anchor (messageId/messageSeq/channelId/channelType/from/timestamp/spaceId)
	out = append(out, rawMsg("3000000000000000007", "g_gap", 2, "u_top", 17, base,
		`{"type":1,"content":"顶层字段锚定庚","space_id":"space_top"}`))

	// ---- P0 visibility fail-closed vectors (group, non-encrypted) ----
	// V1 valid visibles → indexed with visibles populated
	out = append(out, rawMsg("3000000000000000010", "g_acl", 2, "u_adm", 20, base,
		`{"type":99,"content":"群管可见系统消息","visibles":["u_admin1","u_admin2"]}`))
	// V2 empty-array visibles → fail-closed → DLQ (NOT indexed, NOT fail-OPEN)
	out = append(out, rawMsg("3000000000000000011", "g_acl", 2, "u_adm", 21, base,
		`{"type":99,"content":"valid-but-empty 必须进DLQ","visibles":[]}`))
	// V3 null visibles → fail-closed → DLQ
	out = append(out, rawMsg("3000000000000000012", "g_acl", 2, "u_adm", 22, base,
		`{"type":99,"content":"null visibles 必须进DLQ","visibles":null}`))
	// V4 visibles all non-string → fail-closed → DLQ
	out = append(out, rawMsg("3000000000000000013", "g_acl", 2, "u_adm", 23, base,
		`{"type":99,"content":"全非字符串 visibles 必须进DLQ","visibles":[123,456]}`))
	// V5 broadcast (no visibles key) → allowed, visibles empty (normal group chat)
	out = append(out, rawMsg("3000000000000000014", "g_acl", 2, "u_member", 24, base,
		`{"type":1,"content":"普通群聊广播放行辛"}`))

	// ---- Encrypted DM branch: RawPayload nil + RawExcluded=true → indexed, visibles empty (parity) ----
	encM := searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     "3000000000000000020",
		ChannelID:     "u_x@u_y", ChannelType: 1, FromUID: "u_x",
		MessageSeq: 30, MsgTimestamp: base, CreatedAt: base,
		RawExcluded: true, Content: nil,
		Source: searchmsg.SourceETLMessageTable,
	}
	eb, _ := json.Marshal(encM)
	out = append(out, kafka.Message{Key: []byte(encM.MessageID), Value: eb})

	return out
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
