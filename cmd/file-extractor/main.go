// Command file-extractor 是 octo-im 消息检索管线的文件正文抽取服务（v1.12）。
// 消费与 es-indexer 同一个 Kafka topic `octo.message.v1{,.prod}`，独立 consumer group
// `file-extractor`（不抢 es-indexer 位点）。命中 payload.type=8 (File) 时下载 CDN 文件 → 调
// Tika HTTP 抽取正文 → OS partial `_update` 只写 payload.file.content + payload.file.contentMeta。
// 命中非 file 类型 → commit 位点跳过。
//
// 与 es-indexer 的分工：
//
//	Kafka topic octo.message.v1
//	  ├── consumer group `octo-search-indexer` → es-indexer   → 写主 doc (_bulk index)
//	  └── consumer group `file-extractor`      → file-extractor → 更新 payload.file.content (_update)
//	                                                              ↑ 本服务
//	OS partial `_update` doc_as_upsert=false：主 doc 未落时报 404（errDocNotYet），
//	由本服务重试兜底（v2 §7 #1 时序竞态设计）。
//
// 配置全部走环境变量（一仓一镜像、独立部署，同 es-indexer 惯例）。未开通 (FILE_EXTRACTOR_ENABLED
// != true 或 brokers/ES 未配置) 时空转到收到终止信号——保持「未开通即零运行期行为」，须显式注入
// 配置才工作。
//
// IDX-3 是骨架 commit：Kafka 消费 + type=8 filter + DLQ 8 种 reason 常量就位，但抽取核心
// (download / Tika / OS update) 是 stub，命中 file 时只 log 占位。IDX-4 补齐抽取。
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/fileextract"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil && ctx.Err() == nil {
		log.Printf("file-extractor exited with error: %v", err)
		os.Exit(1)
	}
	log.Printf("file-extractor stopped")
}

func run(ctx context.Context) error {
	cfg, enabled := loadConfig()
	if !enabled {
		log.Printf("file-extractor: FILE_EXTRACTOR_ENABLED not true (or brokers/ES unset); idling (no backend connection)")
		<-ctx.Done()
		return nil
	}
	svc, err := fileextract.NewService(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := svc.Close(); cerr != nil {
			log.Printf("file-extractor: close error: %v", cerr)
		}
	}()
	log.Printf("file-extractor running: topic=%s group=%s dlq=%s es_index=%s tika=%s",
		cfg.Topic, cfg.GroupID, cfg.DLQTopic, cfg.ESIndex, cfg.TikaURL)
	return svc.Run(ctx)
}

// loadConfig 从环境读配置。返回 enabled=false 时服务空转（未开通）。
// 开通条件：FILE_EXTRACTOR_ENABLED (可解析为 true) 且 brokers / ES 地址均已配置。
func loadConfig() (fileextract.ServiceConfig, bool) {
	cfg := fileextract.ServiceConfig{
		Brokers:             splitCSV(os.Getenv("KAFKA_BROKERS")),
		Topic:               envOr("KAFKA_TOPIC", "octo.message.v1"),
		DLQTopic:            envOr("KAFKA_DLQ_TOPIC", "octo.message.v1.file-extract.dlq"),
		GroupID:             envOr("KAFKA_GROUP_ID", "file-extractor"),
		BatchSize:           envInt("EXTRACTOR_BATCH_SIZE", 50),
		ESAddresses:         splitCSV(os.Getenv("ES_ADDRESSES")),
		ESIndex:             envOr("ES_INDEX", "octo-message"),
		ESUsername:          os.Getenv("ES_USERNAME"),
		ESPassword:          os.Getenv("ES_PASSWORD"),
		TikaURL:             envOr("TIKA_URL", "http://localhost:9998"),
		DownloadTimeout:     time.Duration(envInt("EXTRACTOR_DOWNLOAD_TIMEOUT_MS", 30000)) * time.Millisecond,
		ExtractTimeout:      time.Duration(envInt("EXTRACTOR_EXTRACT_TIMEOUT_MS", 30000)) * time.Millisecond,
		MaxFileSize:         int64(envInt("EXTRACTOR_MAX_FILE_SIZE_BYTES", 20*1024*1024)),
		MaxContentBytes:     envInt("EXTRACTOR_MAX_CONTENT_BYTES", 256*1024),
		HTTPRetries:         envInt("EXTRACTOR_HTTP_RETRIES", 3),
		ExtractStartupDelay: time.Duration(envInt("EXTRACT_STARTUP_DELAY_SECONDS", 5)) * time.Second,
	}
	enabledFlag, _ := strconv.ParseBool(os.Getenv("FILE_EXTRACTOR_ENABLED"))
	enabled := enabledFlag && len(cfg.Brokers) > 0 && len(cfg.ESAddresses) > 0
	return cfg, enabled
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("file-extractor: invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
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
