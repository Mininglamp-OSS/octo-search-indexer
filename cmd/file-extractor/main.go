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

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
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
	obsAddr := envOr("FILE_EXTRACTOR_OBS_ADDR", ":9090")
	if !enabled {
		log.Printf("file-extractor: FILE_EXTRACTOR_ENABLED not true (or brokers/ES unset); idling (no backend connection)")
		// 对齐 es-indexer：空转态也起 obs server（/healthz + /readyz 200，/metrics 空），
		// 让被刻意停用的 pod 不在编排器 HTTP 探针下 crashloop。不连任何后端（metrics=nil）。
		serveIdleObs(ctx, obsAddr)
		<-ctx.Done()
		return nil
	}
	// v1.13 P2-10：startup mapping-compat fail-closed 断言。es-indexer 已用 esindex.Writer
	// 的 AssertLiveMappingCompatible 校验 mapping 齐备；file-extractor 也需同验证，避免部署
	// 顺序错（mapping 未 PUT 就上 file-extractor）导致 permanent 400 与 Blocker #2 修复的
	// in-place retry 循环叠加浪费时间才发现问题。缺字段 → loud crash 让 K8s 重启并告警。
	if err := assertLiveMapping(ctx, cfg); err != nil {
		return err
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

	// Observability server（healthz/readyz/metrics）。运行循环起后置 ready。
	if obsAddr != "" {
		obs := fileextract.NewObsServer(obsAddr, svc.Metrics(), nil)
		obs.Start(log.Printf)
		obs.SetReady(true)
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if serr := obs.Shutdown(sctx); serr != nil {
				log.Printf("file-extractor: obs shutdown: %v", serr)
			}
		}()
	}
	log.Printf("file-extractor running: topic=%s group=%s dlq=%s es_index=%s tika=%s obs=%s",
		cfg.Topic, cfg.GroupID, cfg.DLQTopic, cfg.ESIndex, cfg.TikaURL, obsAddr)
	return svc.Run(ctx)
}

// serveIdleObs 在空转（停用）态起 obs HTTP server：/healthz 与 /readyz 均返回 200，
// /metrics 空。对齐 es-indexer 的 idle obs，保证被停用的 pod 在探针下不 crashloop。
// 不连任何后端（metrics=nil），ctx 取消时优雅停机。
func serveIdleObs(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	obs := fileextract.NewObsServer(addr, nil, nil)
	obs.SetReady(true) // 空转正常即 ready（未拨任何后端）
	obs.Start(log.Printf)
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if serr := obs.Shutdown(sctx); serr != nil {
			log.Printf("file-extractor: idle obs shutdown: %v", serr)
		}
	}()
	log.Printf("file-extractor: idle observability server on %s (/healthz + /readyz 200, idle)", addr)
}

// assertLiveMapping 用 esindex.Writer 复用 mapping-compat 校验（P2-10）。
// 写完立即 Close 释放 client（本调用只做健康检查，主流程另建自己的 osWriter）。
func assertLiveMapping(ctx context.Context, cfg fileextract.ServiceConfig) error {
	w, err := esindex.NewWriter(esindex.Config{
		Addresses: cfg.ESAddresses,
		Index:     cfg.ESIndex,
		Username:  cfg.ESUsername,
		Password:  cfg.ESPassword,
	})
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }() //nolint:errcheck // close-on-check: nothing to do with close err
	if err := w.AssertLiveMappingCompatible(ctx); err != nil {
		return err
	}
	log.Printf("file-extractor: live mapping compat OK for index=%s", cfg.ESIndex)
	return nil
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

		// v1.13 Blocker #2 fix — in-place bounded retry 参数。生产运维按 OS SLA 与业务
		// 延迟容忍度调整；未设 env 时走 fileextract 包内 default（10 / 1s / 60s）。
		MaxRetriesPerMessage: envInt("EXTRACTOR_MAX_RETRIES_PER_MESSAGE", 0), // 0 → fileextract 用 defaultMaxRetriesPerMessage=10
		TransientBackoffBase: time.Duration(envInt("EXTRACTOR_TRANSIENT_BACKOFF_BASE_MS", 0)) * time.Millisecond,
		TransientBackoffMax:  time.Duration(envInt("EXTRACTOR_TRANSIENT_BACKOFF_MAX_MS", 0)) * time.Millisecond,

		// v1.13 Blocker #1 fix — SSRF 防护参数。允许运维在不改代码前提下扩展 host 白名单
		// （future 切内网 COS 时用），未设 env 时走 fileextract 包内 default
		// (["cdn.deepminer.com.cn"] + ["https"])。
		AllowedDownloadHosts:   splitCSV(os.Getenv("ALLOWED_DOWNLOAD_HOSTS")),
		AllowedDownloadSchemes: splitCSV(os.Getenv("ALLOWED_DOWNLOAD_SCHEMES")),
		// 🔴 SSRFAllowLoopback 显式不从 env 读取：生产必须 false（loopback 是 SSRF 目标之一）。
		// 测试专用；httptest.NewServer 走 127.0.0.1 需 test cfg 直接设 true，无 env 通道防止
		// 生产误开或运维在紧急情况下 hot-toggle 打开攻击面。
		SSRFAllowLoopback: false,

		// v1.13 P2-1 fix (yujiawei review) — DLQ 有界重试 + 本地 spill 逃逸。SpillDir 生产建议
		// 挂 K8s emptyDir 或 PVC（示例 /var/lib/file-extractor/dlq-spill），未设时走硬停 pattern
		// （DLQ 写耗尽 → errDLQHardStop → K8s 重启保 offset 不推进 + 告警 page 运维）。
		DLQMaxRetries:   envInt("DLQ_MAX_RETRIES", 0),
		DLQRetryBackoff: time.Duration(envInt("DLQ_RETRY_BACKOFF_MS", 0)) * time.Millisecond,
		DLQSpillDir:     os.Getenv("DLQ_SPILL_DIR"),
	}
	enabledFlag, err := strconv.ParseBool(os.Getenv("FILE_EXTRACTOR_ENABLED"))
	if err != nil {
		enabledFlag = false // 无效值当未启用（保持"未开通即零运行"语义）
	}
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
