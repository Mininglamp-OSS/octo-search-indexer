package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// kafkaVerifier inspects Kafka state to prove (a) the consumer group has drained
// the body topic (committed offset == log-end offset on every partition) and
// (b) the DLQ topic positively contains the expected poison-pill keys.
type kafkaVerifier struct {
	brokers []string
	client  *kafka.Client
}

func newKafkaVerifier(brokers []string) *kafkaVerifier {
	return &kafkaVerifier{
		brokers: brokers,
		client:  &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second},
	}
}

// expectGroupDrained asserts that, for every partition of `topic`, the consumer
// `group`'s committed offset has caught up to the partition log-end offset.
//
// This is the authoritative drain proof (replaces "ES doc count is stable"):
// it cannot be fooled by a consumer that stalled BEFORE the poison pills, nor by
// the DLQ-spill path — both leave a committed offset short of the log end.
//
// Polls until drained or timeout (the indexer commits asynchronously after each
// processed batch, so a short settle is expected).
func (k *kafkaVerifier) expectGroupDrained(ctx context.Context, topic, group string, timeout time.Duration) error {
	parts, err := lookupPartitions(ctx, k.brokers, topic)
	if err != nil {
		return fmt.Errorf("lookup partitions: %w", err)
	}
	if len(parts) == 0 {
		return fmt.Errorf("topic %q has no partitions", topic)
	}

	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		logEnd, err := k.logEndOffsets(ctx, topic, parts)
		if err != nil {
			lastErr = err
		} else {
			committed, cerr := k.committedOffsets(ctx, topic, group, parts)
			if cerr != nil {
				lastErr = cerr
			} else if drained, why := allCaughtUp(logEnd, committed); drained {
				return nil
			} else {
				lastErr = fmt.Errorf("not drained: %s", why)
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("group %q not drained on %q within %s: %v", group, topic, timeout, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}

// allCaughtUp reports whether every partition's committed offset >= its log-end
// offset. committed offset == log-end means "all messages up to the end have been
// committed" (kafka committed offset is the NEXT offset to read).
func allCaughtUp(logEnd, committed map[int]int64) (bool, string) {
	for p, end := range logEnd {
		c, ok := committed[p]
		if !ok {
			return false, fmt.Sprintf("partition %d has no committed offset yet", p)
		}
		if c < end {
			return false, fmt.Sprintf("partition %d committed=%d < logEnd=%d", p, c, end)
		}
	}
	return true, ""
}

// logEndOffsets returns the last offset (== count of produced messages) per partition.
func (k *kafkaVerifier) logEndOffsets(ctx context.Context, topic string, parts []int) (map[int]int64, error) {
	reqTopics := make([]kafka.OffsetRequest, 0, len(parts))
	for _, p := range parts {
		reqTopics = append(reqTopics, kafka.LastOffsetOf(p))
	}
	resp, err := k.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Addr:   kafka.TCP(k.brokers...),
		Topics: map[string][]kafka.OffsetRequest{topic: reqTopics},
	})
	if err != nil {
		return nil, err
	}
	out := make(map[int]int64)
	for _, po := range resp.Topics[topic] {
		if po.Error != nil {
			return nil, fmt.Errorf("partition %d list-offset: %w", po.Partition, po.Error)
		}
		out[po.Partition] = po.LastOffset
	}
	return out, nil
}

// committedOffsets returns the consumer group's committed offset per partition.
func (k *kafkaVerifier) committedOffsets(ctx context.Context, topic, group string, parts []int) (map[int]int64, error) {
	resp, err := k.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		Addr:    kafka.TCP(k.brokers...),
		GroupID: group,
		Topics:  map[string][]int{topic: parts},
	})
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}
	out := make(map[int]int64)
	for _, p := range resp.Topics[topic] {
		if p.Error != nil {
			return nil, fmt.Errorf("partition %d offset-fetch: %w", p.Partition, p.Error)
		}
		// CommittedOffset == -1 means "no commit yet"; leave it absent so allCaughtUp fails.
		if p.CommittedOffset >= 0 {
			out[p.Partition] = p.CommittedOffset
		}
	}
	return out, nil
}

// expectDLQKeys reads the DLQ topic and asserts each expected key is present
// among records produced by THIS run (positive verification of C4 DLQ routing).
//
// startOffset fences out stale records from a previous (kept-up / failed) harness
// run: only records at offset >= startOffset are considered. Pass the DLQ log-end
// offset captured BEFORE seeding. Reads partition-by-partition up to each
// partition's current log-end, so it terminates deterministically.
func (k *kafkaVerifier) expectDLQKeys(ctx context.Context, topic string, wantKeys []string, startOffset int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		found, err := k.collectKeys(ctx, topic, startOffset)
		if err == nil {
			missing := missingKeys(wantKeys, found)
			if len(missing) == 0 {
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("DLQ %q missing expected keys %v at/after offset %d (found %v)", topic, missing, startOffset, keysOf(found))
			}
		} else if time.Now().After(deadline) {
			return fmt.Errorf("DLQ %q read failed: %w", topic, err)
		}
		time.Sleep(2 * time.Second)
	}
}

// collectKeys reads records in [startOffset, log-end) across all partitions and
// returns the set of message keys seen. startOffset is a per-partition floor —
// in the single-partition harness it is the pre-seed log-end; on multi-partition
// topics it is applied as a conservative floor to every partition.
func (k *kafkaVerifier) collectKeys(ctx context.Context, topic string, startOffset int64) (map[string]struct{}, error) {
	parts, err := lookupPartitions(ctx, k.brokers, topic)
	if err != nil {
		return nil, err
	}
	logEnd, err := k.logEndOffsets(ctx, topic, parts)
	if err != nil {
		return nil, err
	}
	found := make(map[string]struct{})
	for _, p := range parts {
		end := logEnd[p]
		if end <= startOffset {
			continue // nothing new in this partition since startOffset
		}
		if err := k.readPartitionKeys(ctx, topic, p, startOffset, end, found); err != nil {
			return nil, err
		}
	}
	return found, nil
}

// readPartitionKeys reads partition p from startOffset until reaching `end`,
// recording the keys of records at offset >= startOffset.
func (k *kafkaVerifier) readPartitionKeys(ctx context.Context, topic string, p int, startOffset, end int64, found map[string]struct{}) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   k.brokers,
		Topic:     topic,
		Partition: p,
		MinBytes:  1,
		MaxBytes:  10 << 20,
	})
	defer func() {
		if cerr := r.Close(); cerr != nil {
			// best-effort; not fatal to verification
			_ = cerr
		}
	}()
	begin := startOffset
	if begin < 0 {
		begin = kafka.FirstOffset
	}
	if err := r.SetOffset(begin); err != nil {
		return err
	}
	readCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		m, err := r.ReadMessage(readCtx)
		if err != nil {
			return err
		}
		if m.Offset >= startOffset {
			found[string(m.Key)] = struct{}{}
		}
		if m.Offset >= end-1 {
			return nil // reached log-end of this partition
		}
	}
}

func missingKeys(want []string, found map[string]struct{}) []string {
	var missing []string
	for _, k := range want {
		if _, ok := found[k]; !ok {
			missing = append(missing, k)
		}
	}
	return missing
}

func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// lookupPartitions returns the partition IDs of a topic.
func lookupPartitions(ctx context.Context, brokers []string, topic string) ([]int, error) {
	parts, err := kafka.LookupPartitions(ctx, "tcp", brokers[0], topic)
	if err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(parts))
	for _, p := range parts {
		ids = append(ids, p.ID)
	}
	return ids, nil
}

// topicEndOffset returns the sum of log-end offsets across all partitions of a
// topic (== total records ever produced). Used to detect a non-empty DLQ when
// resolving the fence. Only a genuinely-missing topic resolves to 0 (empty);
// any other error (transient metadata/broker failure) propagates so the caller
// fail-closes instead of silently treating the DLQ as empty.
func (k *kafkaVerifier) topicEndOffset(ctx context.Context, topic string) (int64, error) {
	parts, err := lookupPartitions(ctx, k.brokers, topic)
	if err != nil {
		if errors.Is(err, kafka.UnknownTopicOrPartition) {
			return 0, nil // topic not yet created → no stale records to fence
		}
		return 0, err
	}
	if len(parts) == 0 {
		return 0, nil
	}
	logEnd, err := k.logEndOffsets(ctx, topic, parts)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, end := range logEnd {
		total += end
	}
	return total, nil
}
