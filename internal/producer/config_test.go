package producer

import (
	"strings"
	"testing"
)

// TestLoadConfig_DisabledByDefault: no env → not requested, idles, no backend connect.
func TestLoadConfig_DisabledByDefault(t *testing.T) {
	t.Setenv(envEnabled, "")
	_, requested := LoadConfig()
	if requested {
		t.Fatalf("producer must be off by default (zero production risk)")
	}
}

// TestLoadConfig_RequestedIsMasterSwitchOnly: PRODUCER_ENABLED alone flips
// "requested" true — config completeness is the caller's Validate() concern, NOT
// folded into the master switch (so an enabled-but-misconfigured producer
// fail-fasts instead of silently idling "ready").
func TestLoadConfig_RequestedIsMasterSwitchOnly(t *testing.T) {
	// Enabled but NO backends set: still requested=true (caller must Validate).
	t.Setenv(envEnabled, "true")
	t.Setenv(envMySQLDSN, "")
	t.Setenv(envBrokers, "")
	t.Setenv(envRedisAddr, "")
	cfg, requested := LoadConfig()
	if !requested {
		t.Fatalf("PRODUCER_ENABLED=true must report requested=true regardless of backend config")
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("a requested-but-misconfigured producer must fail Validate (not silently idle)")
	}

	// All backends set: requested + valid.
	t.Setenv(envMySQLDSN, "user:pass@tcp(h:3306)/db")
	t.Setenv(envBrokers, "localhost:9092")
	t.Setenv(envRedisAddr, "localhost:6379")
	cfg, requested = LoadConfig()
	if !requested {
		t.Fatalf("must be requested when enabled")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Topic != "octo.message.v1" || cfg.DLQTopic != "octo.message.v1.dlq" {
		t.Fatalf("default topics wrong: %s / %s", cfg.Topic, cfg.DLQTopic)
	}
}

// TestConfig_ClampsAndDefaults: clamps + defaults applied; idle<=open enforced.
func TestConfig_ClampsAndDefaults(t *testing.T) {
	t.Setenv(envEnabled, "true")
	t.Setenv(envMySQLDSN, "dsn")
	t.Setenv(envBrokers, "b1,b2")
	t.Setenv(envRedisAddr, "r:6379")
	t.Setenv(envBatch, "1")       // below min → clamp to minBatch
	t.Setenv(envTickSeconds, "1") // below min → clamp to minTickSeconds
	t.Setenv(envDBMaxOpen, "4")
	t.Setenv(envDBMaxIdle, "999") // above open → clamped to open
	cfg, _ := LoadConfig()
	if cfg.Batch != minBatch {
		t.Fatalf("batch clamp: got %d want %d", cfg.Batch, minBatch)
	}
	if cfg.TickSeconds != minTickSeconds {
		t.Fatalf("tick clamp: got %d want %d", cfg.TickSeconds, minTickSeconds)
	}
	if cfg.DBMaxIdleConns > cfg.DBMaxOpenConns {
		t.Fatalf("idle pool must not exceed open pool: idle=%d open=%d", cfg.DBMaxIdleConns, cfg.DBMaxOpenConns)
	}
	if len(cfg.Brokers) != 2 {
		t.Fatalf("brokers csv split: %v", cfg.Brokers)
	}
}

// TestConfig_ValidateLagZeroRejected: lag<=0 fails validation (C1 silent missed read).
func TestConfig_ValidateLagZeroRejected(t *testing.T) {
	cfg := Config{
		MySQLDSN: "dsn", Tables: []string{"message"}, Brokers: []string{"b"},
		Topic: "t", DLQTopic: "d", RedisAddr: "r", LagSeconds: 0,
	}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "lag") {
		t.Fatalf("lag=0 must be rejected, got %v", err)
	}
}

// TestMetrics_Render emits the expected series.
func TestMetrics_Render(t *testing.T) {
	m := NewMetrics()
	m.AddProduced(5, 1)
	m.SetCursor("message", 42)
	m.MarkTick("ok")
	out := dumpMetrics(t, m)
	for _, want := range []string{
		`searchetl_producer_produced_total{stream="main"} 5`,
		`searchetl_producer_produced_total{stream="dlq"} 1`,
		`searchetl_producer_cursor_position{shard="message"} 42`,
		`searchetl_producer_ticks_total{result="ok"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, out)
		}
	}
}
