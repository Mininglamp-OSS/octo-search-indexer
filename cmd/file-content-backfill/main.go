// Command file-content-backfill 一次性 K8s Job：把 IDX-4 file-extractor 上线**之前**的历史
// type=8 文件消息回填 payload.file.content（v1.12 file content indexing）。
//
// 数据链路：
//
//	OpenSearch → scroll query [payload.type=8 且 must_not exists payload.file.content]
//	           → 逐条限速抽取（复用 internal/fileextract.Extractor）
//	           → OS partial `_update` 只写 payload.file.content + contentMeta
//
// 幂等：query 的 `must_not exists content` 天然跳过已抽取 doc，中断重跑无副作用。
//
// 用法（K8s Job）：
//
//	file-content-backfill \
//	  -es https://10.10.148.6:9200,https://10.10.148.15:9200,https://10.10.148.12:9200 \
//	  -es-user octosearch -es-pass "$OS_PASSWORD" \
//	  -es-index octo-message \
//	  -tika-url http://tika-service:9998 \
//	  -rate 50 -timeout 2h
//
// 退出码：Stats.OSTransient > 0 或 DLQ 占比 > 10% → exit 1 让 K8s 报错；正常 exit 0。
//
// 🔴 隔离纪律：真实环境执行须 Yu / 运维 sign-off，主人决策 backfill 走 K8s Job 一次性跑
// （不常驻），跑完手动 delete Job 释放资源。
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

	fbf "github.com/Mininglamp-OSS/octo-search-indexer/internal/filebackfill"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	if err := run(); err != nil {
		log.Printf("file-content-backfill error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		esAddrs     = flag.String("es", envOr("BACKFILL_ES", ""), "comma-separated OpenSearch addresses (required)")
		esIndex     = flag.String("es-index", envOr("BACKFILL_ES_INDEX", "octo-message"), "OpenSearch index / alias")
		esUser      = flag.String("es-user", os.Getenv("BACKFILL_ES_USER"), "OS username (optional)")
		esPass      = flag.String("es-pass", os.Getenv("BACKFILL_ES_PASS"), "OS password (optional)")
		esTLSSkip   = flag.Bool("es-tls-insecure", envBool("BACKFILL_ES_TLS_INSECURE_SKIP_VERIFY"), "skip TLS cert verification for HTTPS OpenSearch (self-signed); default off")
		tikaURL     = flag.String("tika-url", envOr("BACKFILL_TIKA_URL", "http://tika-service:9998"), "Tika Server URL")
		rate        = flag.Float64("rate", envFloat("BACKFILL_RATE", 50), "extraction rate limit (docs/s; <=0 = unlimited)")
		scrollSize  = flag.Int("scroll-size", envInt("BACKFILL_SCROLL_SIZE", 500), "OS scroll batch size")
		scrollTTL   = flag.Duration("scroll-ttl", envDuration("BACKFILL_SCROLL_TTL", 5*time.Minute), "scroll keep-alive TTL")
		timeout     = flag.Duration("timeout", envDuration("BACKFILL_TIMEOUT", 2*time.Hour), "overall Job timeout")
		downloadMS  = flag.Int("download-timeout-ms", envInt("BACKFILL_DOWNLOAD_TIMEOUT_MS", 30000), "CDN download timeout (ms)")
		extractMS   = flag.Int("extract-timeout-ms", envInt("BACKFILL_EXTRACT_TIMEOUT_MS", 30000), "Tika extract timeout (ms)")
		maxSize     = flag.Int64("max-file-size", int64(envInt("BACKFILL_MAX_FILE_SIZE_BYTES", 20*1024*1024)), "max file size (bytes)")
		maxContent  = flag.Int("max-content-bytes", envInt("BACKFILL_MAX_CONTENT_BYTES", 256*1024), "max extracted content bytes")
		httpRetries = flag.Int("http-retries", envInt("BACKFILL_HTTP_RETRIES", 3), "CDN GET retry count")
		dlqRatioMax = flag.Float64("max-dlq-ratio", 0.10, "max allowed DLQ/scanned ratio; over threshold → exit 1")
	)
	flag.Parse()

	if *esAddrs == "" {
		return fmt.Errorf("es addresses required (-es or BACKFILL_ES)")
	}

	cfg := fbf.Config{
		ESAddresses:             splitCSV(*esAddrs),
		ESIndex:                 *esIndex,
		ESUsername:              *esUser,
		ESPassword:              *esPass,
		ESTLSInsecureSkipVerify: *esTLSSkip,
		TikaURL:                 *tikaURL,
		DownloadTimeout:         time.Duration(*downloadMS) * time.Millisecond,
		ExtractTimeout:          time.Duration(*extractMS) * time.Millisecond,
		MaxFileSize:             *maxSize,
		MaxContentBytes:         *maxContent,
		HTTPRetries:             *httpRetries,
		Rate:                    *rate,
		ScrollSize:              *scrollSize,
		ScrollTTL:               *scrollTTL,
		Timeout:                 *timeout,
	}

	runner, err := fbf.NewRunner(cfg)
	if err != nil {
		return fmt.Errorf("build runner: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("file-content-backfill starting: es_index=%s tika=%s rate=%.1f scroll_size=%d timeout=%v",
		*esIndex, *tikaURL, *rate, *scrollSize, *timeout)

	stats, err := runner.Run(ctx)
	if err != nil {
		return fmt.Errorf("runner.Run: %w (stats: %+v)", err, stats)
	}

	// 退出码判定：OSTransient>0 或 DLQ 占比过高 → 让 K8s Job 报错
	if stats.OSTransient > 0 {
		return fmt.Errorf("os transient errors during backfill: %d (stats: %+v)", stats.OSTransient, stats)
	}
	if stats.Scanned > 0 {
		ratio := float64(stats.DLQ) / float64(stats.Scanned)
		if ratio > *dlqRatioMax {
			return fmt.Errorf("dlq ratio %.3f > threshold %.3f (dlq=%d scanned=%d)", ratio, *dlqRatioMax, stats.DLQ, stats.Scanned)
		}
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envBool 解析布尔型 env（fail-closed：仅 "true"/"TRUE" 等 → true，其余一律 false）。
// 与 cmd/backfill / cmd/reconcile 同实现，方便运维在 K8s Job env 里开 self-signed TLS 跳过。
func envBool(k string) bool {
	return strings.EqualFold(os.Getenv(k), "true")
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		log.Printf("file-content-backfill: invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}

func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var f float64
	if _, err := fmt.Sscanf(v, "%f", &f); err != nil {
		log.Printf("file-content-backfill: invalid %s=%q, using default %f", key, v, def)
		return def
	}
	return f
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("file-content-backfill: invalid %s=%q, using default %v", key, v, def)
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
