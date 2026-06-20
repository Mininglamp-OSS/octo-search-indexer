// Command searchetl-producer is the standalone polling ETL producer for the octo
// message search pipeline. It reads the MySQL message shard tables on a
// slow-cursor tick, enriches each row into the octo-lib searchmsg.Message
// contract (fail-closed visibility), and produces it to Kafka — where the
// es-indexer consumer (this repo) picks it up and writes OpenSearch.
//
// Pipeline position (this binary is the write side of the realtime path; it sits
// alongside es-indexer, not inside it):
//
//	message 5 shards → [searchetl-producer: poll → enrich → Kafka] octo.message.v1
//	  → es-indexer consumer → OpenSearch
//	  → reader (query-side join for revoke/delete + fail-closed authz)
//
// Boundary vs es-indexer: es-indexer is the long-running Kafka→OpenSearch
// CONSUMER; this is the MySQL→Kafka PRODUCER. They share only the octo-lib
// searchmsg contract and the Kafka topics. Backfill (cmd/backfill) is the
// one-shot historical loader that bypasses Kafka entirely; this producer is the
// realtime/incremental path.
//
// 🔴 Zero production risk by construction: the producer is opt-in. With
// PRODUCER_ENABLED unset/false (or brokers/DSN/Redis unset) it idles to signal
// and connects to nothing — it must be explicitly configured to do anything.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/producer"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil && ctx.Err() == nil {
		log.Printf("searchetl-producer exited with error: %v", err)
		os.Exit(1)
	}
	log.Printf("searchetl-producer stopped")
}

func run(ctx context.Context) error {
	cfg, requested := producer.LoadConfig()
	if !requested {
		log.Printf("searchetl-producer: PRODUCER_ENABLED not true; idling (no backend connection)")
		// Serve health endpoints even while idling so an orchestrator's
		// liveness/readiness probes do not crashloop a deliberately-disabled
		// pod (the opt-in "deployed but off" posture). Both /healthz and /readyz
		// report 200: per the plan, "idling normally" is a ready state — it lets
		// the opt-in deploy roll out cleanly without receiving any traffic (the
		// producer exposes no Service). It connects to no backend.
		serveIdleObs(ctx, cfg.ObsAddr)
		<-ctx.Done()
		return nil
	}
	// 🔴 PRODUCER_ENABLED=true means the operator intends this to run. Missing
	// DSN/brokers/Redis/lag is then a hard error (fail-fast crashloop), NOT a
	// silent idle — otherwise a cut-over would no-op while reporting healthy.
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("searchetl-producer: PRODUCER_ENABLED=true but config invalid (refusing to idle on a requested producer): %w", err)
	}

	// Source DB with an explicit connection pool (no bare driver defaults).
	db, err := producer.OpenMySQL(cfg.MySQLDSN, cfg.DBMaxOpenConns, cfg.DBMaxIdleConns, cfg.DBConnMaxLifetime)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			log.Printf("searchetl-producer: close mysql: %v", cerr)
		}
	}()
	if perr := db.PingContext(ctx); perr != nil {
		return perr
	}

	store := producer.NewMySQLStore(db)
	if serr := store.EnsureSchema(ctx); serr != nil {
		return serr
	}

	lock, err := producer.NewRedisLock(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := lock.Close(); cerr != nil {
			log.Printf("searchetl-producer: close redis: %v", cerr)
		}
	}()

	metrics := producer.NewMetrics()

	etl := producer.NewETL(producer.ETLDeps{
		Store:   store,
		NewSink: func() producer.Sink { return producer.NewKafkaProducer(cfg) },
		Lock:    lock,
		Batch:   cfg.Batch,
		Lag:     cfg.LagSeconds,
		Logf:    log.Printf,
		Metrics: metrics,
	})

	sched := producer.NewScheduler(cfg.TickInterval(), cfg.Tables, etl.RunIncremental, log.Printf, metrics)

	// Observability server (healthz/readyz/metrics). readyz pings the DB + Redis.
	var obs *producer.ObsServer
	if cfg.ObsAddr != "" {
		obs = producer.NewObsServer(cfg.ObsAddr, metrics, func(rctx context.Context) error {
			if perr := db.PingContext(rctx); perr != nil {
				return perr
			}
			return lock.Ping()
		})
		obs.Start(log.Printf)
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if serr := obs.Shutdown(sctx); serr != nil {
				log.Printf("searchetl-producer: obs shutdown: %v", serr)
			}
		}()
	}

	log.Printf("searchetl-producer running: tables=%v topic=%s dlq=%s tick=%s lag=%ds batch=%d obs=%s",
		cfg.Tables, cfg.Topic, cfg.DLQTopic, cfg.TickInterval(), cfg.LagSeconds, cfg.Batch, cfg.ObsAddr)

	sched.Start()
	if obs != nil {
		obs.SetReady(true)
	}

	<-ctx.Done()
	// 🔴 Graceful shutdown: stop the scheduler first (drains the in-flight tick;
	// the run-lock is released by runLocked's defer once the tick returns), so we
	// proactively release the Redis lock instead of waiting for TTL expiry.
	log.Printf("searchetl-producer: shutting down, stopping scheduler (releases run-lock)...")
	if obs != nil {
		obs.SetReady(false)
	}
	sched.Stop()
	return nil
}

// serveIdleObs starts the observability HTTP server for the idle (disabled)
// posture: /healthz and /readyz both return 200. Per the plan, "idling
// normally" is a ready state — this keeps a deliberately-disabled pod
// (PRODUCER_ENABLED=false, the opt-in "deployed but off" default) from
// crashlooping under an orchestrator's HTTP probes and lets the opt-in deploy
// roll out cleanly. It connects to no backend (ready check is nil; the producer
// exposes no Service so a ready idle pod receives no traffic). A graceful
// shutdown stops it on ctx cancel.
func serveIdleObs(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	obs := producer.NewObsServer(addr, nil, nil)
	obs.SetReady(true) // idling normally is a ready state (no backend dialed)
	obs.Start(log.Printf)
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if serr := obs.Shutdown(sctx); serr != nil {
			log.Printf("searchetl-producer: idle obs shutdown: %v", serr)
		}
	}()
	log.Printf("searchetl-producer: idle observability server on %s (/healthz + /readyz 200, idle)", addr)
}
