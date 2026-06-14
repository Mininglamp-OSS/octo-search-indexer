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
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
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

	report := recon.Reconcile(recon.Counts{
		SourceRows:  sourceRows,
		ESDocs:      esDocs,
		RawExcluded: rawExcluded,
		DLQ:         *dlq,
	})
	fmt.Println(report.String())
	if !report.OK {
		// 不对平：退出码 2，作为 backfill/CI gate 的失败信号。
		os.Exit(2)
	}
	return nil
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
