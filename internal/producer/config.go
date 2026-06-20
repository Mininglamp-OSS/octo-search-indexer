// Package producer is the standalone polling ETL producer for the octo message
// search pipeline. It reads the MySQL message shard tables on a slow-cursor
// tick, enriches each row into the octo-lib searchmsg.Message contract, and
// produces it to Kafka — where the existing es-indexer consumer picks it up and
// writes OpenSearch.
//
// Pipeline position (the producer is the new write side of the realtime path):
//
//	message shards -> [searchetl-producer: poll -> enrich -> Kafka] octo.message.v1
//	  -> es-indexer consumer (this repo) -> OpenSearch
//	  -> reader (query-side join for revoke/delete + fail-closed authz)
//
// Design discipline (deliberately a slim mirror of the source module — no dbr /
// no opentracing global side effects pulled in):
//   - payload extraction is ported as pure functions (aligned with this repo's
//     internal/backfill extract), not lifted as a whole module.
//   - the fail-closed visibility parser is the SHARED octo-lib
//     searchmsg.ExtractVisibility (single source of truth across producer +
//     backfill, prevents the #1124 leak from diverging between repos).
//   - the Redis distributed lock is self-contained (go-redis + octo-lib
//     BuildTLSConfig), not a dependency on octo-server/pkg/redis.
//   - connection pools are explicitly configured (no bare driver defaults).
//
// 🔴 Zero production risk by construction: this binary is opt-in. With
// PRODUCER_ENABLED unset/false it idles to signal and connects to nothing.
package producer

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full runtime configuration assembled from the environment.
type Config struct {
	// Source MySQL.
	MySQLDSN string
	Tables   []string

	// Connection pool (explicit — no bare driver defaults).
	DBMaxOpenConns    int
	DBMaxIdleConns    int
	DBConnMaxLifetime time.Duration

	// Kafka.
	Brokers  []string
	Topic    string
	DLQTopic string

	// Redis distributed lock (anti double-run across replicas).
	RedisAddr                  string
	RedisPassword              string
	RedisTLS                   bool
	RedisTLSInsecureSkipVerify bool
	RedisTLSCAFile             string

	// Polling cadence / safety.
	Batch       int
	LagSeconds  int64
	TickSeconds int

	// Observability HTTP server (healthz/readyz/metrics). Empty = disabled.
	ObsAddr string
}

// env knob names + clamp bounds (mirrors the source module's env semantics).
const (
	envEnabled = "PRODUCER_ENABLED"

	envMySQLDSN = "PRODUCER_MYSQL_DSN"
	envTables   = "PRODUCER_TABLES"

	envDBMaxOpen     = "PRODUCER_DB_MAX_OPEN_CONNS"
	envDBMaxIdle     = "PRODUCER_DB_MAX_IDLE_CONNS"
	envDBConnMaxLife = "PRODUCER_DB_CONN_MAX_LIFETIME"

	envBrokers  = "PRODUCER_KAFKA_BROKERS"
	envTopic    = "PRODUCER_KAFKA_TOPIC"
	envDLQTopic = "PRODUCER_KAFKA_DLQ_TOPIC"

	envRedisAddr     = "PRODUCER_REDIS_ADDR"
	envRedisPass     = "PRODUCER_REDIS_PASSWORD"
	envRedisTLS      = "PRODUCER_REDIS_TLS"
	envRedisTLSSkip  = "PRODUCER_REDIS_TLS_INSECURE_SKIP_VERIFY"
	envRedisTLSCA    = "PRODUCER_REDIS_TLS_CA_FILE"
	envRedisTLSCAAlt = "PRODUCER_REDIS_CA_FILE"

	// envBatch is the keyset page size per shard.
	envBatch     = "PRODUCER_BATCH"
	defaultBatch = 5000
	minBatch     = 100
	maxBatch     = 50000

	// envLagSeconds is the stability lag window (C1 anti missed-read condition).
	// message.id is assigned on INSERT but only visible on COMMIT; commit order
	// != id order, so a low id may commit after a higher one. We only advance the
	// cursor over the stable prefix older than DB_NOW-lag. lag MUST be > 0 in
	// production (lag=0 risks silent missed reads).
	//
	// 🔴 Operating assumption (inherited from the source searchetl module): lag
	// must exceed the maximum single-message INSERT transaction duration. The gate
	// keys off created_at (assigned at INSERT time), so a row whose transaction
	// stays OPEN longer than lag can be skipped — its id is < a higher id that
	// committed first and now looks stable, and when it finally commits it is
	// already <= last_id and never produced. The default 600s covers normal
	// message writes (sub-second commits) with wide margin; raise it if the source
	// DB can hold message-insert transactions open for minutes. This is a deploy-
	// time guarantee, not something the producer can detect at runtime.
	envLagSeconds     = "PRODUCER_LAG_SECONDS"
	defaultLagSeconds = 600
	maxLagSeconds     = 86400

	// envTickSeconds is the slow-cursor tick period.
	envTickSeconds     = "PRODUCER_TICK_SECONDS"
	defaultTickSeconds = 60
	minTickSeconds     = 5
	maxTickSeconds     = 3600

	envObsAddr     = "PRODUCER_OBS_ADDR"
	defaultObsAddr = ":9090"
)

// defaultTables mirrors the message 5-shard set used across the pipeline.
var defaultTables = []string{"message", "message1", "message2", "message3", "message4"}

// LoadConfig builds Config from the environment and reports whether the producer
// was REQUESTED via the master switch (PRODUCER_ENABLED=true).
//
// 🔴 The returned bool is the master switch ALONE — it deliberately does NOT
// fold in config completeness. The caller distinguishes two states so a cut-over
// can never silently no-op:
//   - requested=false → the producer is intentionally off; idle (zero runtime
//     behavior, no backend connection).
//   - requested=true → the operator means for it to run; the caller MUST then
//     Validate() and fail-fast (loud crashloop) if DSN/brokers/Redis/lag are
//     missing, rather than idling "ready" while producing nothing.
func LoadConfig() (Config, bool) {
	cfg := Config{
		MySQLDSN: os.Getenv(envMySQLDSN),
		Tables:   splitCSVOr(os.Getenv(envTables), defaultTables),

		DBMaxOpenConns:    clampInt(envDBMaxOpen, 8, 1, 256),
		DBMaxIdleConns:    clampInt(envDBMaxIdle, 4, 1, 256),
		DBConnMaxLifetime: envDuration(envDBConnMaxLife, 30*time.Minute),

		Brokers:  splitCSV(os.Getenv(envBrokers)),
		Topic:    envOr(envTopic, "octo.message.v1"),
		DLQTopic: envOr(envDLQTopic, "octo.message.v1.dlq"),

		RedisAddr:                  os.Getenv(envRedisAddr),
		RedisPassword:              os.Getenv(envRedisPass),
		RedisTLS:                   envBool(envRedisTLS),
		RedisTLSInsecureSkipVerify: envBool(envRedisTLSSkip),
		RedisTLSCAFile:             envOr(envRedisTLSCA, os.Getenv(envRedisTLSCAAlt)),

		Batch:       batchSize(),
		LagSeconds:  lagSeconds(),
		TickSeconds: clampInt(envTickSeconds, defaultTickSeconds, minTickSeconds, maxTickSeconds),

		ObsAddr: envOr(envObsAddr, defaultObsAddr),
	}
	// Keep idle pool <= open pool to avoid a nonsensical pool config.
	if cfg.DBMaxIdleConns > cfg.DBMaxOpenConns {
		cfg.DBMaxIdleConns = cfg.DBMaxOpenConns
	}

	requested := strings.EqualFold(os.Getenv(envEnabled), "true")
	return cfg, requested
}

// Validate checks an enabled config is internally coherent (fail-fast on boot).
func (c Config) Validate() error {
	if c.MySQLDSN == "" {
		return fmt.Errorf("producer: MySQL DSN required (%s)", envMySQLDSN)
	}
	if len(c.Tables) == 0 {
		return fmt.Errorf("producer: at least one source table required (%s)", envTables)
	}
	if len(c.Brokers) == 0 {
		return fmt.Errorf("producer: kafka brokers required (%s)", envBrokers)
	}
	if c.Topic == "" || c.DLQTopic == "" {
		return fmt.Errorf("producer: kafka topic and dlq topic required")
	}
	if c.RedisAddr == "" {
		return fmt.Errorf("producer: redis addr required for the distributed lock (%s)", envRedisAddr)
	}
	// 🔴 C1: lag must be > 0 in production (lag=0 = silent missed reads).
	if c.LagSeconds <= 0 {
		return fmt.Errorf("producer: lag must be > 0 (got %d); lag=0 risks silent missed reads", c.LagSeconds)
	}
	return nil
}

// TickInterval returns the slow-cursor tick period.
func (c Config) TickInterval() time.Duration {
	return time.Duration(c.TickSeconds) * time.Second
}

func batchSize() int { return clampInt(envBatch, defaultBatch, minBatch, maxBatch) }

func lagSeconds() int64 {
	v := os.Getenv(envLagSeconds)
	if v == "" {
		return defaultLagSeconds
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return defaultLagSeconds
	}
	if n > maxLagSeconds {
		return maxLagSeconds
	}
	return n
}

func clampInt(env string, def, lo, hi int) int {
	v := os.Getenv(env)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	if n < lo {
		return lo
	}
	if n > hi {
		return hi
	}
	return n
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string) bool {
	return strings.EqualFold(os.Getenv(key), "true")
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func splitCSVOr(s string, def []string) []string {
	if out := splitCSV(s); len(out) > 0 {
		return out
	}
	return def
}
