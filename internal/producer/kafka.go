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
//
// The body batch is searchmsg.Message contracts; the DLQ batch is forensic
// DLQEnvelope records (reason + raw payload + source locator), a different shape
// optimized for triage/replay rather than indexing.
type Sink interface {
	ProduceBatch(ctx context.Context, msgs []searchmsg.Message) error
	ProduceDLQ(ctx context.Context, envelopes []DLQEnvelope) error
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
	// metrics is an optional observer; produce_errors_total is incremented on
	// every WriteMessages failure (main + dlq).
	metrics *Metrics
}

// NewKafkaProducer builds the Kafka writers. Only called when the producer is
// enabled (idle path never connects to Kafka).
func NewKafkaProducer(cfg Config) *KafkaProducer {
	return NewKafkaProducerWithMetrics(cfg, nil)
}

// NewKafkaProducerWithMetrics builds the Kafka writers with an optional metrics
// observer for produce-error counting.
func NewKafkaProducerWithMetrics(cfg Config, metrics *Metrics) *KafkaProducer {
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
		metrics:   metrics,
	}
}

// ProduceBatch produces a body batch atomically: any single failure returns an
// error so the caller leaves the cursor un-advanced and re-produces next tick
// (deduplicated by message_id idempotency, C2).
func (p *KafkaProducer) ProduceBatch(ctx context.Context, msgs []searchmsg.Message) error {
	if err := produce(ctx, p.writer, p.topic, msgs); err != nil {
		if p.metrics != nil {
			p.metrics.MarkProduceError()
		}
		return err
	}
	return nil
}

// ProduceDLQ produces a DLQ batch of forensic envelopes. A DLQ write failure also
// returns an error so the whole chunk does not advance — never "DLQ write failed →
// silently drop". The Kafka key stays message_id (so DLQ records for the same row
// land in the same partition and replay tooling can co-locate by key); ProducedAt
// is stamped here, at produce time, keeping planChunk a pure function.
func (p *KafkaProducer) ProduceDLQ(ctx context.Context, envelopes []DLQEnvelope) error {
	if len(envelopes) == 0 {
		return nil
	}
	now := time.Now().Unix()
	kmsgs := make([]kafka.Message, 0, len(envelopes))
	for i := range envelopes {
		envelopes[i].ProducedAt = now
		b, err := json.Marshal(envelopes[i])
		if err != nil {
			return fmt.Errorf("producer: marshal dlq envelope (table=%s id=%d msg=%s): %w",
				envelopes[i].ShardTable, envelopes[i].SourceID, envelopes[i].MessageID, err)
		}
		kmsgs = append(kmsgs, kafka.Message{Key: []byte(envelopes[i].MessageID), Value: b})
	}
	if err := p.dlqWriter.WriteMessages(ctx, kmsgs...); err != nil {
		if p.metrics != nil {
			p.metrics.MarkProduceError()
		}
		return fmt.Errorf("producer: produce dlq batch to %s: %w", p.dlqTopic, err)
	}
	return nil
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
