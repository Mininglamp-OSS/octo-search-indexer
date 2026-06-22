// Command backfill-harness seeds a controlled set of message rows into a local MySQL
// (mimicking the octo-im message shard schema) for the phase-6 backfill e2e, then
// (optionally) verifies the result in OpenSearch after cmd/backfill has run.
//
// Isolation: connects only to the local throwaway MySQL + OpenSearch. Never a shared env.
//
// Modes:
//
//	-mode seed    create shard tables + insert a controlled suite, print the source row count
//	-mode verify  assert ES doc count / raw_excluded / DLQ-bypassed match the seeded suite
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// Seeded suite shape (kept tiny + explicit so the gate arithmetic is obvious):
//   - 3 normal text rows (incl. 中文)            → indexed, content present
//   - 1 Signal-encrypted row (signal column)     → raw_excluded, still 1 ES doc
//   - 1 non-text (image) row                     → PROJECTED (payload.image.url),
//     NOT raw_excluded (Plan B / CDC-style: docFromRow now projects every typed
//     payload from the raw payload整包 and recomputes RawExcluded from whether a
//     typed sub-object was produced; an image row yields payload.image so it is a
//     normal projected doc, not a raw-excluded one).
//   - 1 bad-JSON non-encrypted row (real anomaly)→ DLQ spill, NOT in ES
//
// So: source_rows=6, expected ES docs=5 (6 - 1 DLQ), raw_excluded=1 (only the
// Signal-encrypted row; the image row is projected post-Plan-B).
const (
	wantSource      = 6
	wantESDocs      = 5
	wantRawExcluded = 1
	wantDLQ         = 1
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	var (
		mode   = flag.String("mode", "seed", "seed|verify")
		dsn    = flag.String("mysql-dsn", envOr("BF_MYSQL_DSN", "root:root@tcp(localhost:23307)/im_test"), "MySQL DSN")
		esURL  = flag.String("es", envOr("BF_ES", "http://localhost:29200"), "OpenSearch URL")
		index  = flag.String("es-index", envOr("BF_ES_INDEX", "octo-message"), "index")
		tables = flag.String("tables", envOr("BF_TABLES", "message,message1"), "shard tables")
		base   = flag.Int64("base-ts", time.Now().Unix(), "base created_at epoch seconds")
	)
	flag.Parse()
	if err := run(*mode, *dsn, *esURL, *index, splitCSV(*tables), *base); err != nil {
		log.Fatalf("backfill-harness %s: %v", *mode, err)
	}
}

func run(mode, dsn, esURL, index string, tables []string, base int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	switch mode {
	case "seed":
		return seed(ctx, dsn, tables, base)
	case "verify":
		return verify(ctx, esURL, index, base)
	default:
		return fmt.Errorf("unknown mode %q", mode)
	}
}

func seed(ctx context.Context, dsn string, tables []string, base int64) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer mustClose(db)
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("need at least one table")
	}
	for _, t := range tables {
		if err := createTable(ctx, db, t); err != nil {
			return err
		}
	}
	// Put the 6-row suite across the shard tables (round-robin-ish; deterministic).
	t0 := tables[0]
	t1 := tables[len(tables)-1]
	rows := []struct {
		table       string
		mid         string
		messageSeq  int64
		fromUID     string
		channelID   string
		channelType uint8
		setting     uint8
		signal      int
		payload     string
	}{
		// 含 space_id（p2p）→ 验证 backfill 富化 reader 必读的 spaceId（V1b）。
		{t0, "3000000000000000001", 11, "u1", "g1", 2, 0, 0, `{"type":1,"content":"hello world pipeline","space_id":"space-A"}`},
		{t0, "3000000000000000002", 12, "u1", "g1", 2, 0, 0, `{"type":1,"content":"今天天气很好我们去公园散步吧"}`},
		// 含 visibles（群系统消息白名单）→ 验证 backfill 富化 reader 必读的 visibles（V3b）。
		{t1, "3000000000000000003", 13, "u2", "g1", 2, 0, 0, `{"type":1,"content":"搜索引擎中文分词测试北京欢迎你","visibles":["admin1"]}`},
		{t1, "3000000000000000004", 14, "u3", "u3@u4", 1, 0, 1, `ENCRYPTED-NOT-JSON`},
		{t0, "3000000000000000005", 15, "u1", "g1", 2, 0, 0, `{"type":2,"url":"http://x/y.png"}`},
		{t1, "3000000000000000006", 16, "u2", "g1", 2, 0, 0, `{not valid json`},
	}
	for i, r := range rows {
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			"INSERT INTO `%s` (message_id, message_seq, from_uid, channel_id, channel_type, setting, `signal`, `timestamp`, created_at, payload) "+
				"VALUES (?,?,?,?,?,?,?,?,FROM_UNIXTIME(?),?)", r.table),
			r.mid, r.messageSeq, r.fromUID, r.channelID, r.channelType, r.setting, r.signal, base, base+int64(i), r.payload,
		); err != nil {
			return fmt.Errorf("insert %s: %w", r.mid, err)
		}
	}
	log.Printf("seeded %d source rows across %v (expected ES docs=%d raw_excluded=%d DLQ=%d)",
		wantSource, tables, wantESDocs, wantRawExcluded, wantDLQ)
	return nil
}

func createTable(ctx context.Context, db *sql.DB, table string) error {
	// 表名来自 -tables flag（本地 harness 控制），但 DROP/CREATE 仍白名单校验防注入。
	if !safeTableName(table) {
		return fmt.Errorf("unsafe table name %q", table)
	}
	// Minimal shape mirroring the columns cmd/backfill reads.
	_, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE `%s` ("+
			"id BIGINT AUTO_INCREMENT PRIMARY KEY, "+
			"message_id VARCHAR(20) NOT NULL, "+
			"message_seq BIGINT NOT NULL DEFAULT 0, "+
			"from_uid VARCHAR(40) NOT NULL DEFAULT '', "+
			"channel_id VARCHAR(100) NOT NULL DEFAULT '', "+
			"channel_type TINYINT UNSIGNED NOT NULL DEFAULT 0, "+
			"setting TINYINT UNSIGNED NOT NULL DEFAULT 0, "+
			"`signal` INT NOT NULL DEFAULT 0, "+
			"`timestamp` BIGINT NOT NULL DEFAULT 0, "+
			"created_at DATETIME NOT NULL, "+
			"payload BLOB"+
			")", table))
	return err
}

func verify(ctx context.Context, esURL, index string, base int64) error {
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{esURL}},
	})
	if err != nil {
		return err
	}
	// Refresh so counts are visible.
	if _, rerr := client.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{Indices: []string{index}}); rerr != nil {
		return fmt.Errorf("refresh: %w", rerr)
	}
	from := base - 10
	to := base + 100
	total, err := count(ctx, client, index, rangeQuery(from, to))
	if err != nil {
		return err
	}
	rawEx, err := count(ctx, client, index, rawExcludedQuery(from, to))
	if err != nil {
		return err
	}
	var fail bool
	if total != wantESDocs {
		log.Printf("FAIL: ES doc count=%d want %d", total, wantESDocs)
		fail = true
	} else {
		log.Printf("PASS: ES doc count=%d (== source %d - DLQ %d)", total, wantSource, wantDLQ)
	}
	if rawEx != wantRawExcluded {
		log.Printf("FAIL: raw_excluded=%d want %d", rawEx, wantRawExcluded)
		fail = true
	} else {
		log.Printf("PASS: raw_excluded=%d (only the Signal-encrypted row; image row is projected post-Plan-B)", rawEx)
	}
	// Positively assert the bad-JSON row is NOT in ES (it went to DLQ spill).
	badPresent, err := count(ctx, client, index, idsQuery("3000000000000000006"))
	if err != nil {
		return err
	}
	if badPresent != 0 {
		log.Printf("FAIL: real-anomaly row 3000000000000000006 must NOT be in ES, found %d", badPresent)
		fail = true
	} else {
		log.Printf("PASS: real-anomaly row 3000000000000000006 absent from ES (routed to DLQ spill)")
	}
	if fail {
		return fmt.Errorf("backfill e2e verification FAILED")
	}
	log.Printf("backfill e2e verification PASSED")
	return nil
}

func count(ctx context.Context, c *opensearchapi.Client, index string, q map[string]any) (int64, error) {
	b, err := json.Marshal(q)
	if err != nil {
		return 0, err
	}
	resp, err := c.Indices.Count(ctx, &opensearchapi.IndicesCountReq{
		Indices: []string{index},
		Body:    strings.NewReader(string(b)),
	})
	if err != nil {
		return 0, err
	}
	if resp.Shards.Failed != 0 || resp.Shards.Successful != resp.Shards.Total {
		return 0, fmt.Errorf("incomplete shards (total=%d ok=%d failed=%d)",
			resp.Shards.Total, resp.Shards.Successful, resp.Shards.Failed)
	}
	return int64(resp.Count), nil
}

func rangeQuery(from, to int64) map[string]any {
	return map[string]any{"query": map[string]any{"bool": map[string]any{"filter": []any{rangeFilter(from, to)}}}}
}

func rawExcludedQuery(from, to int64) map[string]any {
	return map[string]any{"query": map[string]any{"bool": map[string]any{"filter": []any{
		rangeFilter(from, to),
		map[string]any{"term": map[string]any{"rawExcluded": true}},
	}}}}
}

func idsQuery(id string) map[string]any {
	return map[string]any{"query": map[string]any{"ids": map[string]any{"values": []string{id}}}}
}

func rangeFilter(from, to int64) map[string]any {
	return map[string]any{"range": map[string]any{"createdAt": map[string]any{"gte": from, "lte": to}}}
}

func mustClose(db *sql.DB) {
	if err := db.Close(); err != nil {
		log.Printf("warn: close db: %v", err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// safeTableName 仅允许 [A-Za-z0-9_]（DROP/CREATE 防注入；表名来自本地 harness flag）。
func safeTableName(t string) bool {
	if t == "" {
		return false
	}
	for _, r := range t {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}
