package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/segmentio/kafka-go"
)

// Sink abstracts batch delivery to Kafka (implemented by *KafkaProducer), so the
// chunk pipeline can be unit-tested with a fake sink: the whole batch is produced
// outside the DB read transaction and any failure leaves the cursor un-advanced.
type Sink interface {
	ProduceBatch(ctx context.Context, msgs []searchmsg.Message) error
	ProduceDLQ(ctx context.Context, msgs []searchmsg.Message) error
	Close() error
}

// KafkaProducer produces enriched contracts to the body topic octo.message.v1
// and the DLQ topic octo.message.v1.dlq.
//
// Discipline:
//   - Kafka key = message_id → same message always lands in the same partition,
//     pairs with ES _id=message_id upsert for effectively-once at the sink (this
//     is at-least-once + idempotent ES sink, NOT a same-transaction accumulator).
//   - the transaction boundary is the caller's responsibility: the producer is
//     only ever called OUTSIDE the DB read transaction.
//   - RequireAll (acks=-1): wait for all ISR before counting a delivery as
//     successful, avoiding loss on leader switch.
type KafkaProducer struct {
	writer    *kafka.Writer
	dlqWriter *kafka.Writer
	topic     string
	dlqTopic  string
}

// NewKafkaProducer builds the Kafka writers. Only called when the producer is
// enabled (idle path never connects to Kafka).
func NewKafkaProducer(cfg Config) *KafkaProducer {
	mk := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{}, // hash by key(message_id) → same msg, same partition
			// RequireAll: wait for all ISR ack (strongest durability, at-least-once).
			RequiredAcks: kafka.RequireAll,
			// Synchronous: Write returns only after broker confirm (or failure);
			// the ETL decides cursor advance based on that.
			Async:        false,
			BatchTimeout: 50 * time.Millisecond,
			WriteTimeout: 10 * time.Second,
			// Allow first-write topic creation (deployments pre-provision with
			// retention; this is a local/preprod fallback).
			AllowAutoTopicCreation: true,
		}
	}
	return &KafkaProducer{
		writer:    mk(cfg.Topic),
		dlqWriter: mk(cfg.DLQTopic),
		topic:     cfg.Topic,
		dlqTopic:  cfg.DLQTopic,
	}
}

// ProduceBatch produces a body batch atomically: any single failure returns an
// error so the caller leaves the cursor un-advanced and re-produces next tick
// (deduplicated by message_id idempotency, C2).
func (p *KafkaProducer) ProduceBatch(ctx context.Context, msgs []searchmsg.Message) error {
	return produce(ctx, p.writer, p.topic, msgs)
}

// ProduceDLQ produces a DLQ batch. A DLQ write failure also returns an error so
// the whole chunk does not advance — never "DLQ write failed → silently drop".
func (p *KafkaProducer) ProduceDLQ(ctx context.Context, msgs []searchmsg.Message) error {
	return produce(ctx, p.dlqWriter, p.dlqTopic, msgs)
}

func produce(ctx context.Context, w *kafka.Writer, topic string, msgs []searchmsg.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	kmsgs := make([]kafka.Message, 0, len(msgs))
	for i := range msgs {
		b, err := json.Marshal(msgs[i])
		if err != nil {
			// Contract marshal failure is an encoding bug (not a data problem);
			// fail the whole batch so we never silently drop.
			return fmt.Errorf("producer: marshal message %s: %w", msgs[i].MessageID, err)
		}
		kmsgs = append(kmsgs, kafka.Message{Key: []byte(msgs[i].MessageID), Value: b})
	}
	if err := w.WriteMessages(ctx, kmsgs...); err != nil {
		return fmt.Errorf("producer: produce batch to %s: %w", topic, err)
	}
	return nil
}

// Close closes the underlying writer connections.
func (p *KafkaProducer) Close() error {
	var firstErr error
	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			firstErr = err
		}
	}
	if p.dlqWriter != nil {
		if err := p.dlqWriter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
