// Command filename-backfill fills S14 literal filename fields in an already
// migrated index. Credentials are read from the environment, never CLI flags.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/filenamebackfill"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	es := flag.String("es", os.Getenv("BACKFILL_ES"), "comma-separated OpenSearch URLs")
	index := flag.String("index", "", "migrated target index (required)")
	size := flag.Int("page-size", 200, "scroll page size, 1..500")
	rate := flag.Int("rate", 100, "updates per second, 1..1000")
	timeout := flag.Duration("timeout", 12*time.Hour, "overall timeout")
	dryRun := flag.Bool("dry-run", false, "read-only preflight and repair count; no updates or refresh")
	flag.Parse()
	if *es == "" || *index == "" || *timeout <= 0 {
		return fmt.Errorf("-es, -index and positive timeout required")
	}
	client, err := opensearchapi.NewClient(opensearchapi.Config{Client: opensearch.Config{
		Addresses: strings.Split(*es, ","), Username: os.Getenv("BACKFILL_ES_USER"), Password: os.Getenv("BACKFILL_ES_PASS"),
	}})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	started := time.Now()
	stats, err := filenamebackfill.RunWithOptions(ctx, client, *index, filenamebackfill.Options{PageSize: *size, Rate: *rate, DryRun: *dryRun})
	log.Printf("filename backfill: dry_run=%t scanned=%d needs_update=%d updated=%d noops=%d deleted=%d batches=%d verified=%d invalid=%d elapsed=%s", *dryRun, stats.Scanned, stats.NeedsUpdate, stats.Updated, stats.Changed, stats.Deleted, stats.Batches, stats.Verified, stats.Invalid, time.Since(started).Round(time.Millisecond))
	if *dryRun && *rate > 0 {
		lowerBound := time.Duration((stats.NeedsUpdate+*rate-1) / *rate) * time.Second
		log.Printf("update rate lower bound=%s (excludes scan, verification, reindex and server work); configured timeout=%s", lowerBound, *timeout)
		if err == nil && lowerBound >= *timeout {
			return fmt.Errorf("timeout cannot cover even the rate lower bound; increase timeout or choose a measured rate")
		}
	}
	return err
}
