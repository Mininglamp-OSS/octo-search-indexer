// Command seed 向 Kafka topic 直灌符合 searchmsg 契约的受控测试消息，用于本地端到端 harness
// 验证（Kafka → es-indexer → OpenSearch）。覆盖：正常文本 / 中文 / raw_excluded(Signal/非文本) /
// 重复 message_id(幂等) / 未知 schema_version(→DLQ) 等用例。
//
// 隔离纪律：仅连本地 harness 的 Kafka（KAFKA_BROKERS，默认 localhost:9092）。绝不指向共享环境。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/segmentio/kafka-go"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	var (
		brokers = flag.String("brokers", envOr("KAFKA_BROKERS", "localhost:9092"), "kafka brokers (csv)")
		topic   = flag.String("topic", envOr("KAFKA_TOPIC", "octo.message.v1"), "body topic")
		mode    = flag.String("mode", "suite", "suite|bulk")
		n       = flag.Int("n", 1000, "bulk mode: number of valid messages to produce")
		base    = flag.Int64("base-ts", time.Now().Unix(), "base created_at epoch seconds")
	)
	flag.Parse()

	w := &kafka.Writer{
		Addr:                   kafka.TCP(splitCSV(*brokers)...),
		Topic:                  *topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: true,
	}
	defer func() {
		if cerr := w.Close(); cerr != nil {
			log.Printf("warn: close writer: %v", cerr)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var msgs []kafka.Message
	switch *mode {
	case "suite":
		msgs = suite(*base)
	case "bulk":
		msgs = bulk(*n, *base)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}

	if err := w.WriteMessages(ctx, msgs...); err != nil {
		log.Fatalf("produce: %v", err)
	}
	log.Printf("seeded %d messages to %s (mode=%s)", len(msgs), *topic, *mode)
}

// suite 产出覆盖各用例的固定样本集（供不变式验证）。
func suite(base int64) []kafka.Message {
	var out []kafka.Message
	text := func(id, content string) kafka.Message {
		c := content
		return contractMsg(searchmsg.Message{
			SchemaVersion: searchmsg.SchemaVersion, MessageID: id, ChannelID: "g_1", ChannelType: 2,
			FromUID: "u_1", Content: &c, ContentType: 1, MsgTimestamp: base, CreatedAt: base,
			Source: searchmsg.SourceETLMessageTable,
		})
	}
	// 正常英文 + 中文（验证 IK 分词召回）。
	out = append(out, text("m-en-1", "hello world search pipeline"))
	out = append(out, text("m-zh-1", "今天天气很好我们去公园散步吧"))
	out = append(out, text("m-zh-2", "搜索引擎中文分词测试：北京欢迎你"))
	// 重复 message_id（幂等：写两次，ES 只应有一条 doc）。
	out = append(out, text("m-dup", "first copy"))
	out = append(out, text("m-dup", "second copy overwrites same _id"))
	// raw_excluded（Signal 加密 / 非文本）：content=null，仍写入 ES 占一个 doc。
	out = append(out, contractMsg(searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion, MessageID: "m-signal", ChannelID: "u1@u2", ChannelType: 1,
		FromUID: "u1", Content: nil, RawExcluded: true, MsgTimestamp: base, CreatedAt: base,
		Source: searchmsg.SourceETLMessageTable,
	}))
	// 未知 schema_version → consumer 应判毒丸进 DLQ，不写 ES。
	out = append(out, rawMsg("m-badschema", fmt.Sprintf(
		`{"schema_version":999,"message_id":"m-badschema","channel_id":"g_1","channel_type":2,"created_at":%d}`, base)))
	// 非法 JSON → 真异常进 DLQ。
	out = append(out, kafka.Message{Key: []byte("m-badjson"), Value: []byte("{not valid json")})
	return out
}

// bulk 产出 n 条合法消息（吞吐调参用），message_id 唯一、含中文正文。
func bulk(n int, base int64) []kafka.Message {
	out := make([]kafka.Message, 0, n)
	for i := 0; i < n; i++ {
		c := fmt.Sprintf("消息正文 message body number %d 测试中文分词与吞吐", i)
		out = append(out, contractMsg(searchmsg.Message{
			SchemaVersion: searchmsg.SchemaVersion,
			MessageID:     fmt.Sprintf("bulk-%08d", i),
			ChannelID:     "g_bulk", ChannelType: 2, FromUID: "u_bulk",
			Content: &c, ContentType: 1, MsgTimestamp: base, CreatedAt: base + int64(i%3600),
			Source: searchmsg.SourceETLMessageTable,
		}))
	}
	return out
}

func contractMsg(m searchmsg.Message) kafka.Message {
	b, err := json.Marshal(m)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	return kafka.Message{Key: []byte(m.MessageID), Value: b}
}

func rawMsg(id, payload string) kafka.Message {
	return kafka.Message{Key: []byte(id), Value: []byte(payload)}
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
