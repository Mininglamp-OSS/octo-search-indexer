// Command backfill 是 octo-im 消息检索管线的历史回灌作业（YUJ-4534 阶段 6）。
//
// 它读 MySQL message 5 分表 → 复用 internal/esindex 写入器（同一套 bulk / mapping /
// `_id=message_id`）直接幂等 bulk 写 OpenSearch，**绕开 Kafka**。与实时增量（Kafka →
// es-indexer）并行 / 重叠安全：`_id=message_id` 幂等覆盖，同一条消息被两条路写入即覆盖同一 doc。
//
//	message 5 分表 → 【backfill 本作业：keyset 扫描 → esindex.Writer bulk】 → OpenSearch
//
// 设计纪律：
//   - 复用 internal/esindex（不重新实现写入器）；keyset 分页（按主键 id 有序推进，不用 OFFSET）。
//   - 限速 ≤5k docs/s（默认）；checkpoint 续传（已灌 message_id 高水位，与实时游标物理隔离）。
//   - 真异常 / ES 永久拒绝落本地 DLQ spill 并精确计数，作为对账门权威输入。
//   - 配置全走 flag / env。运行结束后建议跑 cmd/reconcile（或 -reconcile 内联对账门）核验。
//     -reconcile 内联门含 count 对账 **+ 字段级抽样比对**（-recon-sample，默认 200）：
//     count 只证条数、抽样证 reader 契约关键字段（spaceId/visibles/messageSeq 等）内容一致。
//
// 🔴 隔离纪律：本作业对真实环境执行需 Yu / 运维显式 sign-off 后低峰触发，绝不自动跑生产。
//
// 用法：
//
//	backfill -mysql-dsn 'user:pass@tcp(host:3306)/im_prod' \
//	  -tables message,message1,message2,message3,message4 \
//	  -es http://localhost:9200 -es-index octo-message \
//	  -spill-dir /var/lib/octo-backfill/dlq -checkpoint /var/lib/octo-backfill/checkpoint.json \
//	  -rate 5000 -batch 1000 [-reconcile -from <epoch> -to <epoch>]
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/backfill"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/recon"
	_ "github.com/go-sql-driver/mysql"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	if err := run(); err != nil {
		log.Printf("backfill error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		mysqlDSN   = flag.String("mysql-dsn", os.Getenv("BACKFILL_MYSQL_DSN"), "MySQL DSN (or BACKFILL_MYSQL_DSN)")
		tablesS    = flag.String("tables", envOr("BACKFILL_TABLES", "message,message1,message2,message3,message4"), "comma-separated message shard tables")
		esAddrs    = flag.String("es", envOr("BACKFILL_ES", "http://localhost:9200"), "comma-separated OpenSearch addresses")
		esIndex    = flag.String("es-index", envOr("BACKFILL_ES_INDEX", "octo-message"), "OpenSearch index")
		esUser     = flag.String("es-user", os.Getenv("BACKFILL_ES_USER"), "OpenSearch username")
		esPass     = flag.String("es-pass", os.Getenv("BACKFILL_ES_PASS"), "OpenSearch password")
		spillDir   = flag.String("spill-dir", envOr("BACKFILL_SPILL_DIR", ""), "REQUIRED: local DLQ spill dir for real anomalies / permanent ES rejects")
		checkpoint = flag.String("checkpoint", envOr("BACKFILL_CHECKPOINT", ""), "checkpoint file path for resumable cursor (empty = in-memory, not resumable)")
		batch      = flag.Int("batch", envInt("BACKFILL_BATCH", 1000), "keyset batch size")
		rate       = flag.Float64("rate", envFloat("BACKFILL_RATE", 5000), "ingest rate limit (docs/s; <=0 = unlimited)")
		backoffMS  = flag.Int("transient-backoff-ms", envInt("BACKFILL_TRANSIENT_BACKOFF_MS", 1000), "ES transient retry backoff base (ms)")
		timeout    = flag.Duration("timeout", envDuration("BACKFILL_TIMEOUT", 6*time.Hour), "overall timeout")
		doRecon    = flag.Bool("reconcile", false, "after backfill, run the reconcile gate (requires -from/-to)")
		fromUnix   = flag.Int64("from", 0, "reconcile window start (epoch seconds, inclusive)")
		toUnix     = flag.Int64("to", 0, "reconcile window end (epoch seconds, inclusive; 0 = now)")
		sampleN    = flag.Int("recon-sample", envInt("BACKFILL_RECON_SAMPLE", 200), "reconcile field-level sample size (0 disables field sampling, count gate only)")
		maxDet     = flag.Int("recon-sample-max-details", envInt("BACKFILL_RECON_SAMPLE_MAX_DETAILS", 50), "cap on sample-mismatch detail entries in the report")
	)
	flag.Parse()

	if *mysqlDSN == "" {
		return fmt.Errorf("mysql DSN required (-mysql-dsn or BACKFILL_MYSQL_DSN)")
	}
	tables := splitCSV(*tablesS)
	if len(tables) == 0 {
		return fmt.Errorf("at least one source table required (-tables)")
	}

	// SIGTERM / SIGINT 优雅停止：当前批结束、checkpoint 已持久化后退出，可续传。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	db, err := sql.Open("mysql", *mysqlDSN)
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			log.Printf("warn: close mysql: %v", cerr)
		}
	}()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping mysql: %w", err)
	}

	writer, err := esindex.NewWriter(esindex.Config{
		Addresses: splitCSV(*esAddrs),
		Index:     *esIndex,
		Username:  *esUser,
		Password:  *esPass,
	})
	if err != nil {
		return fmt.Errorf("build ES writer: %w", err)
	}
	defer func() {
		if cerr := writer.Close(); cerr != nil {
			log.Printf("warn: close ES writer: %v", cerr)
		}
	}()

	cp, err := backfill.OpenCheckpoint(*checkpoint)
	if err != nil {
		return err
	}
	dlq, err := backfill.OpenDLQSpill(*spillDir)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := dlq.Close(); cerr != nil {
			log.Printf("warn: close DLQ spill: %v", cerr)
		}
	}()

	runner := backfill.NewRunner(backfill.Config{
		Tables:           tables,
		BatchSize:        *batch,
		DocsPerSec:       *rate,
		TransientBackoff: time.Duration(*backoffMS) * time.Millisecond,
	}, backfill.NewMySQLSource(db), writer, cp, dlq)

	log.Printf("backfill start: tables=%v batch=%d rate=%.0f docs/s spill=%s checkpoint=%s index=%s",
		tables, *batch, *rate, dlq.Path(), *checkpoint, *esIndex)

	stats, runErr := runner.Run(ctx)
	log.Printf("backfill stats: read=%d indexed=%d (raw_excluded=%d) dlq=%d (payload=%d permanent=%d)",
		stats.Read, stats.Indexed, stats.RawExcluded, stats.DLQ, stats.DLQPayload, stats.DLQPermanent)
	if runErr != nil {
		return fmt.Errorf("backfill run: %w", runErr)
	}

	if *doRecon {
		return reconcile(ctx, db, splitCSV(*esAddrs), *esUser, *esPass, *esIndex, tables, *fromUnix, *toUnix, *sampleN, *maxDet, dlq)
	}
	log.Printf("backfill complete. Run cmd/reconcile (or re-run with -reconcile -from/-to) to gate correctness. Total DLQ count: %d", dlq.Count())
	return nil
}

// reconcile 内联对账门：用 backfill 自己精确统计的 DLQ 计数作为权威输入，避免人工传错 -dlq。
// 🔴 用**按窗** DLQ 计数（CountInWindow）：reconcile 窗可能不覆盖本次 run 处理的全部行
// （如分批跑、或只校验某个时间段），用全量 dlqCount 会把窗外的 DLQ 行也减掉 → false
// mismatch/false OK（codex review P2-window）。
//
// 🔴 字段级抽样门（YUJ-4701 落地 Jerry-Xin non-blocking）：count 门只证「条数」一致，不证
// 「内容」一致。复用 recon.CompareSamples 逐字段核对 reader 契约关键字段（messageId/channelId/
// channelType/spaceId/visibles/messageSeq），检出「条数对得上但字段错位」的静默 drift——backfill
// 正是富化 spaceId/visibles/messageSeq 的唯一路径，这些字段错位最该在 backfill 后立即拦住。
// sampleN<=0 时退回纯 count 门（与 cmd/reconcile -sample 0 同口径）。
// 对平退出码 0；不对平返回错误（main 退出码 1）作为 STOP 信号。
func reconcile(ctx context.Context, db *sql.DB, esAddrs []string, user, pass, index string, tables []string, fromUnix, toUnix int64, sampleN, maxDet int, dlq *backfill.DLQSpill) error {
	to := toUnix
	if to == 0 {
		to = time.Now().Unix()
	}
	if fromUnix > to {
		return fmt.Errorf("invalid reconcile window: from(%d) > to(%d)", fromUnix, to)
	}
	dlqCount := dlq.CountInWindow(fromUnix, to)
	osClient, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: esAddrs, Username: user, Password: pass},
	})
	if err != nil {
		return fmt.Errorf("opensearch client: %w", err)
	}
	// 强制 refresh 让刚 bulk 的 doc 对 _count 可见——否则 refresh_interval（默认 1s / backfill
	// 期建议 -1）未到时 _count 读到旧值，会把对账门误判成漏灌（false MISMATCH）。
	if _, rerr := osClient.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{Indices: []string{index}}); rerr != nil {
		return fmt.Errorf("refresh index before reconcile: %w", rerr)
	}
	srcCounter := recon.NewMySQLSourceCounter(db, tables)
	esCounter := recon.NewOSCounter(osClient, index)

	sourceRows, err := srcCounter.CountRows(ctx, fromUnix, to)
	if err != nil {
		return err
	}
	esDocs, err := esCounter.CountDocs(ctx, fromUnix, to)
	if err != nil {
		return err
	}
	rawExcluded, err := esCounter.CountRawExcluded(ctx, fromUnix, to)
	if err != nil {
		return err
	}
	report, err := recon.ReconcileChecked(recon.Counts{
		SourceRows:  sourceRows,
		ESDocs:      esDocs,
		RawExcluded: rawExcluded,
		DLQ:         dlqCount,
	})
	if err != nil {
		return fmt.Errorf("reconcile inputs not self-consistent: %w", err)
	}

	full := recon.FullReport{Count: report, RanAtUnixSeconds: time.Now().Unix()}
	full.Window.FromUnix = fromUnix
	full.Window.ToUnix = to

	// 字段级抽样门（count 门不证内容一致）。
	if sampleN > 0 {
		sampleReader := recon.NewMySQLSampleReader(db, tables)
		docFetcher := recon.NewOSDocFetcher(osClient, index)
		// 排除本批已落 DLQ spill 的真异常 / 永久拒绝行：它们本就不该在 ES 正文索引里
		// （count 门已用同一 DLQ 计数抵消），抽样命中时不能再算成 missing（口径冲突 → false MISMATCH）。
		excluded := dlq.MessageIDsInWindow(fromUnix, to)
		sample, serr := recon.CompareSamplesExcluding(ctx, sampleReader, docFetcher, fromUnix, to, sampleN, maxDet, excluded)
		if serr != nil {
			return fmt.Errorf("sample compare: %w", serr)
		}
		full.Sample = sample
	}

	log.Printf("reconcile gate: %s", full.String())
	if !full.Healthy() {
		return fmt.Errorf("reconcile MISMATCH (count diff=%d, sample mismatch=%d missing=%d): "+
			"backfill correctness gate FAILED — STOP, do not proceed",
			report.Diff, full.Sample.Mismatch, full.Sample.Missing)
	}
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		log.Printf("backfill: invalid %s=%q, using default %d", k, v, def)
		return def
	}
	return n
}

func envFloat(k string, def float64) float64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	var f float64
	if _, err := fmt.Sscanf(v, "%g", &f); err != nil {
		log.Printf("backfill: invalid %s=%q, using default %g", k, v, def)
		return def
	}
	return f
}

func envDuration(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("backfill: invalid %s=%q, using default %s", k, v, def)
		return def
	}
	return d
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
