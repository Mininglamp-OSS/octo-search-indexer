package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// kafkaSource 用 kafka-go Reader 实现 messageSource。
//
// 🔴 C4：Reader 必须 CommitInterval=0（同步手动提交），且只用 FetchMessage（不用 ReadMessage，
// 后者会按 CommitInterval 自动提交，违反「连续成功前缀才 commit」）。GroupID 启用消费组协调。
type kafkaSource struct {
	reader  *kafka.Reader
	metrics *Metrics
}

// KafkaSourceConfig 配置消费侧 Kafka Reader。
type KafkaSourceConfig struct {
	Brokers []string
	Topic   string
	GroupID string
	// MinBytes/MaxBytes 控制单次 fetch 大小。
	MinBytes int
	MaxBytes int
}

// newKafkaSource 建一个手动提交的消费组 Reader（C4 提交语义）。
func newKafkaSource(cfg KafkaSourceConfig, metrics *Metrics) (*kafkaSource, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("consumer: kafka brokers required")
	}
	if cfg.Topic == "" || cfg.GroupID == "" {
		return nil, fmt.Errorf("consumer: kafka topic and groupID required")
	}
	minBytes := cfg.MinBytes
	if minBytes <= 0 {
		minBytes = 1
	}
	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 10 << 20 // 10MB
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.GroupID,
		// 🔴 CommitInterval=0 → 同步手动提交（禁自动提交）。配合只用 FetchMessage。
		CommitInterval: 0,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
	})
	return &kafkaSource{reader: r, metrics: metrics}, nil
}

func (k *kafkaSource) Fetch(ctx context.Context) (fetchedMessage, error) {
	start := time.Now()
	m, err := k.reader.FetchMessage(ctx)
	if k.metrics != nil {
		k.metrics.ObserveIO("kafka_fetch", time.Since(start))
	}
	if err != nil {
		// ctx 取消是正常停机信号，不计 IO 错误；其余才计。
		if k.metrics != nil && ctx.Err() == nil {
			k.metrics.MarkIOError("kafka_fetch")
		}
		return fetchedMessage{}, err
	}
	return fetchedMessage{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
	}, nil
}

func (k *kafkaSource) Commit(ctx context.Context, msg fetchedMessage) error {
	return k.reader.CommitMessages(ctx, kafka.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	})
}

func (k *kafkaSource) Close() error {
	return k.reader.Close()
}

// kafkaDLQSink 用 kafka-go Writer 把毒丸消息写入 DLQ topic（实现 dlqSink）。
type kafkaDLQSink struct {
	writer  *kafka.Writer
	metrics *Metrics
}

// newKafkaDLQSink 建 DLQ producer（RequireAll，保证 DLQ 持久化；失败由 dlqHandler 重试/逃逸）。
// AllowAutoTopicCreation=false：DLQ topic 须由部署侧预建——避免 topic 名拼错时静默建出错误 topic
// 把毒丸写进黑洞（P2-3）。
func newKafkaDLQSink(brokers []string, dlqTopic string, metrics *Metrics) (*kafkaDLQSink, error) {
	if len(brokers) == 0 || dlqTopic == "" {
		return nil, fmt.Errorf("consumer: DLQ brokers and topic required")
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  dlqTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
	}
	return &kafkaDLQSink{writer: w, metrics: metrics}, nil
}

func (s *kafkaDLQSink) WriteDLQ(ctx context.Context, key []byte, value []byte) error {
	start := time.Now()
	err := s.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
	if s.metrics != nil {
		s.metrics.ObserveIO("dlq_send", time.Since(start))
		if err != nil {
			s.metrics.MarkIOError("dlq_send")
		}
	}
	return err
}

func (s *kafkaDLQSink) Close() error {
	return s.writer.Close()
}
