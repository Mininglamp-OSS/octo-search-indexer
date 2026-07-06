package fileextract

// kafka.go — file-extractor 的 Kafka 消费侧封装。
// 与 internal/consumer/kafka.go 结构一致（CommitInterval=0 手动提交 + 只用 FetchMessage），
// 不 import consumer 包避免循环依赖 + 保持本包独立可测。DLQ producer 同理。

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// fetchedMessage 是从 Kafka 拉到的原始消息（含 topic/partition/offset，供 CommitBatch 用）。
type fetchedMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}

// messageSource 抽象「从 Kafka 拉一条消息 + 提交位点」。测试注入 mock。
type messageSource interface {
	Fetch(ctx context.Context) (fetchedMessage, error)
	Commit(ctx context.Context, msg fetchedMessage) error
	Close() error
}

// kafkaSource 用 kafka-go Reader 实现 messageSource（同 consumer/kafka.go C4 语义：
// CommitInterval=0 同步手动提交 + 只用 FetchMessage，禁自动提交）。
type kafkaSource struct {
	reader *kafka.Reader
}

// KafkaSourceConfig 配置消费侧 Kafka Reader。
type KafkaSourceConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int
	MaxBytes int
}

func newKafkaSource(cfg KafkaSourceConfig) (*kafkaSource, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("fileextract: kafka brokers required")
	}
	if cfg.Topic == "" || cfg.GroupID == "" {
		return nil, fmt.Errorf("fileextract: kafka topic and groupID required")
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
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		CommitInterval: 0,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
	})
	return &kafkaSource{reader: r}, nil
}

func (k *kafkaSource) Fetch(ctx context.Context) (fetchedMessage, error) {
	m, err := k.reader.FetchMessage(ctx)
	if err != nil {
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

// dlqSink 抽象「把一条 DLQ 记录投到 DLQ topic」。测试注入 mock。
type dlqSink interface {
	WriteDLQ(ctx context.Context, key []byte, value []byte) error
	Close() error
}

// kafkaDLQSink 用 kafka-go Writer 把 DLQ 记录写入 DLQ topic。
// AllowAutoTopicCreation=false：DLQ topic 须部署侧预建，避免 topic 名拼错静默建错。
type kafkaDLQSink struct {
	writer *kafka.Writer
}

func newKafkaDLQSink(brokers []string, dlqTopic string) (*kafkaDLQSink, error) {
	if len(brokers) == 0 || dlqTopic == "" {
		return nil, fmt.Errorf("fileextract: DLQ brokers and topic required")
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  dlqTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
	}
	return &kafkaDLQSink{writer: w}, nil
}

func (s *kafkaDLQSink) WriteDLQ(ctx context.Context, key []byte, value []byte) error {
	return s.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
}

func (s *kafkaDLQSink) Close() error {
	return s.writer.Close()
}
