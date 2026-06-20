// Command harness/producer drives the searchetl-producer against a throwaway
// MySQL + Kafka + Redis stack and asserts the end-to-end invariants:
//
//	① poll → enrich → Kafka: stable rows land on the body topic, DLQ rows on the
//	   DLQ topic, the cursor advances over the confirmed-delivered stable prefix.
//	② Redis lock mutual exclusion: two concurrent RunIncremental calls sharing the
//	   same lock key never both produce — exactly one runs, the other skips.
//	③ cursor monotonicity: a second tick over the same already-consumed rows
//	   produces nothing new and does not move the cursor backward.
//
// Throwaway local verification ONLY — never wired into a shared environment.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/producer"
	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	if err := run(); err != nil {
		log.Printf("FAIL: %v", err)
		os.Exit(1)
	}
	log.Printf("PASS: searchetl-producer e2e invariants hold")
}

func run() error {
	cfg := producer.Config{
		MySQLDSN:          envOr("PH_MYSQL_DSN", "root:rootpw@tcp(localhost:13307)/octo_im_test?parseTime=true"),
		Tables:            []string{"message"},
		DBMaxOpenConns:    8,
		DBMaxIdleConns:    4,
		DBConnMaxLifetime: 30 * time.Minute,
		Brokers:           []string{envOr("PH_KAFKA", "localhost:29092")},
		Topic:             "octo.message.v1",
		DLQTopic:          "octo.message.v1.dlq",
		RedisAddr:         envOr("PH_REDIS", "localhost:16379"),
		Batch:             5000,
		LagSeconds:        1, // fixture rows are dated in the past, so all are stable
	}

	db, err := producer.OpenMySQL(cfg.MySQLDSN, cfg.DBMaxOpenConns, cfg.DBMaxIdleConns, cfg.DBConnMaxLifetime)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			log.Printf("close mysql: %v", cerr)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping mysql: %w", err)
	}

	store := producer.NewMySQLStore(db)
	if err := store.EnsureSchema(ctx); err != nil {
		return err
	}

	// ── Invariant ②: Redis lock mutual exclusion ──────────────────────────────
	// Two ETLs sharing the same Redis lock key, run concurrently. Use a sink that
	// blocks briefly so the two ticks genuinely overlap in time; exactly one must
	// acquire + produce, the other must skip.
	mutexMetrics := producer.NewMetrics()
	var produced1, produced2 atomic.Int64
	makeETL := func(counter *atomic.Int64) (*producer.ETL, error) {
		lock, lerr := producer.NewRedisLock(cfg)
		if lerr != nil {
			return nil, lerr
		}
		return producer.NewETL(producer.ETLDeps{
			Store: store,
			NewSink: func() producer.Sink {
				return &countingSink{counter: counter, delay: 800 * time.Millisecond}
			},
			Lock:    lock,
			Batch:   cfg.Batch,
			Lag:     cfg.LagSeconds,
			Metrics: mutexMetrics,
			Logf:    log.Printf,
		}), nil
	}
	etlA, err := makeETL(&produced1)
	if err != nil {
		return err
	}
	etlB, err := makeETL(&produced2)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if e := etlA.RunIncremental(ctx, cfg.Tables); e != nil {
			log.Printf("replica A run: %v", e)
		}
	}()
	go func() {
		defer wg.Done()
		if e := etlB.RunIncremental(ctx, cfg.Tables); e != nil {
			log.Printf("replica B run: %v", e)
		}
	}()
	wg.Wait()

	total := produced1.Load() + produced2.Load()
	winners := 0
	if produced1.Load() > 0 {
		winners++
	}
	if produced2.Load() > 0 {
		winners++
	}
	if winners != 1 {
		return fmt.Errorf("lock mutual exclusion FAILED: both replicas produced (a=%d b=%d); exactly one must win",
			produced1.Load(), produced2.Load())
	}
	log.Printf("invariant ② OK: redis lock mutual exclusion — exactly one replica produced (%d rows)", total)

	// The mutex round already produced the stable prefix to Kafka via the real
	// sink path (countingSink only counts; it does not write Kafka). Re-run once
	// with the REAL Kafka sink to populate the topics for invariant ①, against a
	// fresh cursor table.
	if _, err := db.ExecContext(ctx, "DELETE FROM octo_etl_es_cursor"); err != nil {
		return fmt.Errorf("reset cursor: %w", err)
	}
	realLock, err := producer.NewRedisLock(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := realLock.Close(); cerr != nil {
			log.Printf("close redis: %v", cerr)
		}
	}()
	realMetrics := producer.NewMetrics()
	realETL := producer.NewETL(producer.ETLDeps{
		Store:   store,
		NewSink: func() producer.Sink { return producer.NewKafkaProducer(cfg) },
		Lock:    realLock,
		Batch:   cfg.Batch,
		Lag:     cfg.LagSeconds,
		Metrics: realMetrics,
		Logf:    log.Printf,
	})
	if err := realETL.RunIncremental(ctx, cfg.Tables); err != nil {
		return fmt.Errorf("real-sink tick: %w", err)
	}

	// ── Invariant ③: cursor monotonicity ──────────────────────────────────────
	cur1, err := readCursor(ctx, db, "message")
	if err != nil {
		return err
	}
	// Second tick over the same rows must produce nothing new and not move cursor.
	if err := realETL.RunIncremental(ctx, cfg.Tables); err != nil {
		return fmt.Errorf("second tick: %w", err)
	}
	cur2, err := readCursor(ctx, db, "message")
	if err != nil {
		return err
	}
	if cur2 != cur1 {
		return fmt.Errorf("cursor monotonicity FAILED: moved from %d to %d on a no-op re-tick", cur1, cur2)
	}
	if cur1 <= 0 {
		return fmt.Errorf("cursor must have advanced past 0, got %d", cur1)
	}
	log.Printf("invariant ③ OK: cursor monotonic (stable at id=%d across re-tick)", cur1)

	// ── Invariant ①: Kafka body + DLQ contents ────────────────────────────────
	// Fixture: 6 rows. main = text + visibles + signal(raw) + media(raw) = 4.
	// dlq = bad-json + empty-visibles = 2.
	mainMsgs, err := drainTopic(ctx, cfg.Brokers, cfg.Topic, 4)
	if err != nil {
		return fmt.Errorf("drain main topic: %w", err)
	}
	dlqEnvelopes, err := drainDLQ(ctx, cfg.Brokers, cfg.DLQTopic, 2)
	if err != nil {
		return fmt.Errorf("drain dlq topic: %w", err)
	}
	if len(mainMsgs) != 4 {
		return fmt.Errorf("main topic: want 4 messages, got %d", len(mainMsgs))
	}
	if len(dlqEnvelopes) != 2 {
		return fmt.Errorf("dlq topic: want 2 messages, got %d", len(dlqEnvelopes))
	}

	// Assert enrichment correctness on the main stream.
	byID := map[string]searchmsg.Message{}
	for _, m := range mainMsgs {
		byID[m.MessageID] = m
		if m.SchemaVersion != searchmsg.SchemaVersion {
			return fmt.Errorf("msg %s wrong schema_version %d", m.MessageID, m.SchemaVersion)
		}
	}
	// text message has body content + correct content_type.
	if txt := byID["1000000000000000001"]; txt.Content == nil || *txt.Content == "" {
		return fmt.Errorf("text message must carry content, got %+v", txt)
	}
	// visibles message enriched with the whitelist.
	vis := byID["1000000000000000002"]
	if len(vis.Visibles) != 2 {
		return fmt.Errorf("visibles message must carry 2 visibles, got %v", vis.Visibles)
	}
	// signal-encrypted DM is raw_excluded with nil content.
	sig := byID["1000000000000000003"]
	if !sig.RawExcluded || sig.Content != nil {
		return fmt.Errorf("signal message must be raw_excluded with nil content, got %+v", sig)
	}
	// 🔴 the empty-visibles row must be in DLQ, NOT main (fail-closed #1124 guard).
	if _, leaked := byID["1000000000000000006"]; leaked {
		return fmt.Errorf("SECURITY: empty-visibles row leaked into main topic (fail-OPEN)")
	}
	// 🟡 DLQ records are forensic envelopes (reason + raw payload + source locator),
	// not bare body contracts — assert the triage context survived.
	dlqByID := map[string]producer.DLQEnvelope{}
	for _, e := range dlqEnvelopes {
		dlqByID[e.MessageID] = e
		if e.SchemaVersion != 1 {
			return fmt.Errorf("dlq envelope %s wrong schema_version %d", e.MessageID, e.SchemaVersion)
		}
		if e.Reason == "" || len(e.RawPayload) == 0 || e.ShardTable == "" || e.SourceID == 0 {
			return fmt.Errorf("dlq envelope %s missing forensic context: %+v", e.MessageID, e)
		}
		if e.ProducedAt == 0 {
			return fmt.Errorf("dlq envelope %s must be stamped with produced_at", e.MessageID)
		}
	}
	if _, ok := dlqByID["1000000000000000006"]; !ok {
		return fmt.Errorf("empty-visibles row must be routed to DLQ (fail-closed), dlq ids=%v", dlqKeys(dlqByID))
	}
	if _, ok := dlqByID["1000000000000000005"]; !ok {
		return fmt.Errorf("bad-json row must be routed to DLQ, dlq ids=%v", dlqKeys(dlqByID))
	}
	log.Printf("invariant ① OK: 4 main (text/visibles/raw×2) + 2 dlq envelopes (bad-json + empty-visibles fail-closed, reasons=%q/%q)",
		dlqByID["1000000000000000005"].Reason, dlqByID["1000000000000000006"].Reason)
	return nil
}

// dlqKeys returns the message ids present in a DLQ-envelope map (error messages).
func dlqKeys(m map[string]producer.DLQEnvelope) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// countingSink counts produced rows and sleeps to force temporal overlap; it does
// not touch Kafka (used only for the mutex invariant).
type countingSink struct {
	counter *atomic.Int64
	delay   time.Duration
}

func (s *countingSink) ProduceBatch(_ context.Context, msgs []searchmsg.Message) error {
	if len(msgs) > 0 {
		s.counter.Add(int64(len(msgs)))
	}
	time.Sleep(s.delay)
	return nil
}
func (s *countingSink) ProduceDLQ(_ context.Context, envelopes []producer.DLQEnvelope) error {
	if len(envelopes) > 0 {
		s.counter.Add(int64(len(envelopes)))
	}
	return nil
}
func (s *countingSink) Close() error { return nil }

func readCursor(ctx context.Context, db *sql.DB, table string) (int64, error) {
	var id int64
	err := db.QueryRowContext(ctx,
		"SELECT last_id FROM octo_etl_es_cursor WHERE shard_table=?", table).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("read cursor %s: %w", table, err)
	}
	return id, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// drainTopic reads up to want messages from a topic (from the beginning), with a
// short idle timeout, and decodes them as searchmsg.Message.
func drainTopic(ctx context.Context, brokers []string, topic string, want int) ([]searchmsg.Message, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 << 20,
	})
	defer func() {
		if cerr := r.Close(); cerr != nil {
			log.Printf("close kafka reader for %s: %v", topic, cerr)
		}
	}()

	var out []searchmsg.Message
	for len(out) < want {
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		m, err := r.ReadMessage(rctx)
		cancel()
		if err != nil {
			break // idle timeout / no more messages
		}
		var msg searchmsg.Message
		if uerr := json.Unmarshal(m.Value, &msg); uerr != nil {
			return nil, fmt.Errorf("decode %s offset %d: %w", topic, m.Offset, uerr)
		}
		out = append(out, msg)
	}
	return out, nil
}

// drainDLQ reads up to want DLQ records from the DLQ topic and decodes them as
// producer.DLQEnvelope (the producer-specific forensic shape, NOT the body
// contract).
func drainDLQ(ctx context.Context, brokers []string, topic string, want int) ([]producer.DLQEnvelope, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 << 20,
	})
	defer func() {
		if cerr := r.Close(); cerr != nil {
			log.Printf("close kafka reader for %s: %v", topic, cerr)
		}
	}()

	var out []producer.DLQEnvelope
	for len(out) < want {
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		m, err := r.ReadMessage(rctx)
		cancel()
		if err != nil {
			break // idle timeout / no more messages
		}
		var env producer.DLQEnvelope
		if uerr := json.Unmarshal(m.Value, &env); uerr != nil {
			return nil, fmt.Errorf("decode dlq %s offset %d: %w", topic, m.Offset, uerr)
		}
		out = append(out, env)
	}
	return out, nil
}
