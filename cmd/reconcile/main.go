// Command reconcile 对账 MySQL message 表行数 vs OpenSearch doc 数（给定时间窗），扣除已知排除
// 后输出差异报告。对平退出 0，不对平退出 2（可作为阶段 6 backfill 的正确性 gate / CI 检查）。
//
// 用法（env 或 flag 配置）：
//
//	reconcile -from 1709078400 -to 1718323200 \
//	  -mysql-dsn 'user:pass@tcp(host:3306)/im_prod' -tables message,message1,message2,message3,message4 \
//	  -es http://localhost:9200 -es-index octo-message [-dlq N]
//
// 与 indexer 写入器解耦（沿用 internal/esindex 解耦纪律）：本命令只读 MySQL + 查 ES，不依赖
// consumer。阶段 6 backfill job 可直接 import internal/recon 复用对账算术。
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/recon"
	_ "github.com/go-sql-driver/mysql"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	if err := run(); err != nil {
		log.Printf("reconcile error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		fromUnix = flag.Int64("from", 0, "window start (epoch seconds, inclusive)")
		toUnix   = flag.Int64("to", 0, "window end (epoch seconds, inclusive); 0 = now")
		mysqlDSN = flag.String("mysql-dsn", os.Getenv("RECON_MYSQL_DSN"), "MySQL DSN (or RECON_MYSQL_DSN)")
		tablesS  = flag.String("tables", envOr("RECON_TABLES", "message,message1,message2,message3,message4"), "comma-separated message shard tables")
		esAddrs  = flag.String("es", envOr("RECON_ES", "http://localhost:9200"), "comma-separated OpenSearch addresses")
		esIndex  = flag.String("es-index", envOr("RECON_ES_INDEX", "octo-message"), "OpenSearch index")
		esUser   = flag.String("es-user", os.Getenv("RECON_ES_USER"), "OpenSearch username")
		esPass   = flag.String("es-pass", os.Getenv("RECON_ES_PASS"), "OpenSearch password")
		dlq      = flag.Int64("dlq", 0, "known DLQ count in window (rows that never reached ES body index)")
		sampleN  = flag.Int("sample", envInt("RECON_SAMPLE", 200), "field-level sample size (0 disables sampling)")
		maxDet   = flag.Int("sample-max-details", envInt("RECON_SAMPLE_MAX_DETAILS", 50), "cap on mismatch detail entries in the report")
		jsonOut  = flag.Bool("json", false, "emit the structured FullReport as JSON")
		pushURL  = flag.String("push-url", os.Getenv("RECON_PUSH_URL"), "optional octo-server ingestion URL to POST the search_recon gauge payload")
		pushTok  = flag.String("push-token", os.Getenv("RECON_PUSH_TOKEN"), "optional bearer token for -push-url")
		timeout  = flag.Duration("timeout", 60*time.Second, "overall timeout")
	)
	flag.Parse()

	if *mysqlDSN == "" {
		return fmt.Errorf("mysql DSN required (-mysql-dsn or RECON_MYSQL_DSN)")
	}
	to := *toUnix
	if to == 0 {
		to = time.Now().Unix()
	}
	if *fromUnix > to {
		return fmt.Errorf("invalid window: from(%d) > to(%d)", *fromUnix, to)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
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

	osClient, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: splitCSV(*esAddrs),
			Username:  *esUser,
			Password:  *esPass,
		},
	})
	if err != nil {
		return fmt.Errorf("opensearch client: %w", err)
	}

	srcCounter := recon.NewMySQLSourceCounter(db, splitCSV(*tablesS))
	esCounter := recon.NewOSCounter(osClient, *esIndex)

	sourceRows, err := srcCounter.CountRows(ctx, *fromUnix, to)
	if err != nil {
		return err
	}
	esDocs, err := esCounter.CountDocs(ctx, *fromUnix, to)
	if err != nil {
		return err
	}
	rawExcluded, err := esCounter.CountRawExcluded(ctx, *fromUnix, to)
	if err != nil {
		return err
	}

	report, err := recon.ReconcileChecked(recon.Counts{
		SourceRows:  sourceRows,
		ESDocs:      esDocs,
		RawExcluded: rawExcluded,
		DLQ:         *dlq,
	})
	if err != nil {
		// 输入不自洽（DLQ accounting 加固，阶段 6 (f)）：拒绝在可疑计数上判 OK。
		return err
	}

	full := recon.FullReport{Count: report, RanAtUnixSeconds: time.Now().Unix()}
	full.Window.FromUnix = *fromUnix
	full.Window.ToUnix = to

	// 抽样字段比对（步骤 5 核心：count 对账不证内容一致）。
	if *sampleN > 0 {
		sampleReader := recon.NewMySQLSampleReader(db, splitCSV(*tablesS))
		docFetcher := recon.NewOSDocFetcher(osClient, *esIndex)
		sample, serr := recon.CompareSamples(ctx, sampleReader, docFetcher, *fromUnix, to, *sampleN, *maxDet)
		if serr != nil {
			return serr
		}
		full.Sample = sample
	}

	// 回填 octo-server 只读 search_recon_* gauge（可选）。push 失败不改变对账结论，但要显式告警。
	if *pushURL != "" {
		if perr := pushReport(ctx, *pushURL, *pushTok, full.PushPayload()); perr != nil {
			log.Printf("warn: push recon payload to %s failed: %v", *pushURL, perr)
		}
	}

	if *jsonOut {
		b, jerr := json.MarshalIndent(full, "", "  ")
		if jerr != nil {
			return fmt.Errorf("marshal report: %w", jerr)
		}
		fmt.Println(string(b))
	} else {
		fmt.Println(full.String())
	}

	if !full.Healthy() {
		// 不对平（count 或抽样 drift）：退出码 2，作为 backfill/CI/cron gate 的失败信号。
		os.Exit(2)
	}
	return nil
}

// pushReport POST 结构化 gauge 载荷到 octo-server 只读 ingestion 端（载荷逐字段对齐
// recon_metrics.go::ReconReport）。仅在 -push-url 配置时调用。
func pushReport(ctx context.Context, url, token string, payload recon.PushPayload) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal push payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			log.Printf("reconcile: close push response body: %v", cerr)
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("push returned status %d", resp.StatusCode)
	}
	return nil
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
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
