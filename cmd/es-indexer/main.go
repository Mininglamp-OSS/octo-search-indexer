// Command es-indexer 是 octo-im 消息检索管线的独立索引器服务（YUJ-4530 v4 / YUJ-4534
// 阶段 4）。它消费 Kafka topic `octo.message.v1`（契约 octo-lib contract/searchmsg），
// 经可复用写入器 internal/esindex 幂等 bulk 写入 OpenSearch（doc _id = message_id）。
//
// 在 9 阶段管线中的位置：
//
//	message 5 分表 → searchetl(producer, octo-server) → Kafka octo.message.v1
//	  → 【es-indexer 本服务: consumer + bulk + 中文分词】 → OpenSearch
//	  → 读路径(octo-server 查询侧 join 撤回/删除过滤 + 鉴权 fail-CLOSED)
//
// 设计纪律：
//   - consumer（offset 提交/DLQ 路由）与写入器（internal/esindex.Writer）解耦，
//     以便阶段 6 backfill job 复用同一写入器。
//   - schema_version 校验：收到未知契约版本进 DLQ，不静默吃。
//   - offset 仅推进到「连续成功前缀」；transient(429/5xx) 退避重试，permanent(4xx)
//     进 DLQ；DLQ 写自身 transient 失败有终态逃逸（C4）。
//
// 配置全部走环境变量（一仓一镜像、独立部署）。未配置 Kafka brokers / ES 地址时，服务
// 不连任何后端、空转到收到终止信号——保持「未开通即零运行期行为」，须显式注入配置才工作。
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

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/consumer"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil && ctx.Err() == nil {
		log.Printf("es-indexer exited with error: %v", err)
		os.Exit(1)
	}
	log.Printf("es-indexer stopped")
}

// run 装配并运行索引器服务。未配置后端时空转到信号（零运行期行为）。
func run(ctx context.Context) error {
	cfg, enabled := loadConfig()
	obsAddr := envOr("INDEXER_OBS_ADDR", ":9090")
	if !enabled {
		log.Printf("es-indexer: ES_INDEXER_ENABLED not true (or brokers/ES unset); idling (no backend connection)")
		// 对齐 producer：空转态也起 obs server（/healthz + /readyz 200），让被刻意停用的 pod
		// 不在编排器 HTTP 探针下 crashloop。不连任何后端（metrics=nil）。
		serveIdleObs(ctx, obsAddr)
		<-ctx.Done()
		return nil
	}

	svc, err := consumer.NewService(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := svc.Close(); cerr != nil {
			log.Printf("es-indexer: close error: %v", cerr)
		}
	}()

	// Observability server（healthz/readyz/metrics）。readyz 在运行循环启动后置 ready。
	var obs *consumer.ObsServer
	if obsAddr != "" {
		obs = consumer.NewObsServer(obsAddr, svc.Metrics(), nil)
		obs.Start(log.Printf)
		obs.SetReady(true)
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if serr := obs.Shutdown(sctx); serr != nil {
				log.Printf("es-indexer: obs shutdown: %v", serr)
			}
		}()
	}

	log.Printf("es-indexer running: topic=%s group=%s es_index=%s obs=%s", cfg.Topic, cfg.GroupID, cfg.ESIndex, obsAddr)
	return svc.Run(ctx)
}

// serveIdleObs 在空转（停用）态起 obs HTTP server：/healthz 与 /readyz 均返回 200，
// /metrics 空。对齐 producer 的 idle obs，保证被刻意停用的 pod 在编排器探针下不 crashloop。
// 不连任何后端（metrics=nil），ctx 取消时优雅停机。
func serveIdleObs(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	obs := consumer.NewObsServer(addr, nil, nil)
	obs.SetReady(true) // 空转正常即 ready（未拨任何后端）
	obs.Start(log.Printf)
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if serr := obs.Shutdown(sctx); serr != nil {
			log.Printf("es-indexer: idle obs shutdown: %v", serr)
		}
	}()
	log.Printf("es-indexer: idle observability server on %s (/healthz + /readyz 200, idle)", addr)
}

// loadConfig 从环境读取配置。返回 enabled=false 时服务空转（未开通）。
// 开通条件：ES_INDEXER_ENABLED=true 且 brokers / ES 地址均已配置。
func loadConfig() (consumer.ServiceConfig, bool) {
	cfg := consumer.ServiceConfig{
		Brokers:                 splitCSV(os.Getenv("KAFKA_BROKERS")),
		Topic:                   envOr("KAFKA_TOPIC", "octo.message.v1"),
		DLQTopic:                envOr("KAFKA_DLQ_TOPIC", "octo.message.v1.dlq"),
		GroupID:                 envOr("KAFKA_GROUP_ID", "octo-search-indexer"),
		BatchSize:               envInt("INDEXER_BATCH_SIZE", 500),
		ESAddresses:             splitCSV(os.Getenv("ES_ADDRESSES")),
		ESIndex:                 envOr("ES_INDEX", "octo-message"),
		ESUsername:              os.Getenv("ES_USERNAME"),
		ESPassword:              os.Getenv("ES_PASSWORD"),
		ESTLSInsecureSkipVerify: strings.EqualFold(os.Getenv("ES_TLS_INSECURE_SKIP_VERIFY"), "true"),
		TransientBackoff:        time.Duration(envInt("INDEXER_TRANSIENT_BACKOFF_MS", 1000)) * time.Millisecond,
		DLQMaxRetries:           envInt("INDEXER_DLQ_MAX_RETRIES", 5),
		DLQRetryBackoff:         time.Duration(envInt("INDEXER_DLQ_RETRY_BACKOFF_MS", 200)) * time.Millisecond,
		DLQSpillDir:             os.Getenv("INDEXER_DLQ_SPILL_DIR"),
	}
	enabled := strings.EqualFold(os.Getenv("ES_INDEXER_ENABLED"), "true") &&
		len(cfg.Brokers) > 0 && len(cfg.ESAddresses) > 0
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
		log.Printf("es-indexer: invalid %s=%q, using default %d", key, v, def)
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
