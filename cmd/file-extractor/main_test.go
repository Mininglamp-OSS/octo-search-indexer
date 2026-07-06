package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

// resetEnv 清空 EXTRACTOR/KAFKA/ES/FILE_EXTRACTOR/TIKA 前缀所有环境变量，保证 test 无污染。
func resetEnv(t *testing.T) {
	t.Helper()
	for _, prefix := range []string{"FILE_EXTRACTOR_", "KAFKA_", "ES_", "EXTRACTOR_", "TIKA_", "EXTRACT_"} {
		for _, kv := range os.Environ() {
			if idx := strings.IndexByte(kv, '='); idx > 0 {
				key := kv[:idx]
				if strings.HasPrefix(key, prefix) {
					t.Setenv(key, "")
				}
			}
		}
	}
}

// TestLoadConfig_DisabledByDefault 未设 FILE_EXTRACTOR_ENABLED → enabled=false。
func TestLoadConfig_DisabledByDefault(t *testing.T) {
	resetEnv(t)
	_, enabled := loadConfig()
	if enabled {
		t.Fatal("expected enabled=false when FILE_EXTRACTOR_ENABLED is unset")
	}
}

// TestLoadConfig_MissingBrokers Enabled=true 但缺 KAFKA_BROKERS → enabled=false。
func TestLoadConfig_MissingBrokers(t *testing.T) {
	resetEnv(t)
	t.Setenv("FILE_EXTRACTOR_ENABLED", "true")
	t.Setenv("ES_ADDRESSES", "http://os:9200")
	_, enabled := loadConfig()
	if enabled {
		t.Fatal("expected enabled=false without KAFKA_BROKERS")
	}
}

// TestLoadConfig_MissingES Enabled=true + brokers 但缺 ES_ADDRESSES → enabled=false。
func TestLoadConfig_MissingES(t *testing.T) {
	resetEnv(t)
	t.Setenv("FILE_EXTRACTOR_ENABLED", "true")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	_, enabled := loadConfig()
	if enabled {
		t.Fatal("expected enabled=false without ES_ADDRESSES")
	}
}

// TestLoadConfig_HappyPath_Defaults 完整启用，覆盖默认值（DLQ topic / group / batch size 等）。
func TestLoadConfig_HappyPath_Defaults(t *testing.T) {
	resetEnv(t)
	t.Setenv("FILE_EXTRACTOR_ENABLED", "true")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("ES_ADDRESSES", "http://os:9200")
	cfg, enabled := loadConfig()
	if !enabled {
		t.Fatal("expected enabled=true")
	}
	if cfg.Topic != "octo.message.v1" {
		t.Errorf("Topic default: got %q want octo.message.v1", cfg.Topic)
	}
	if cfg.DLQTopic != "octo.message.v1.file-extract.dlq" {
		t.Errorf("DLQTopic default: got %q", cfg.DLQTopic)
	}
	if cfg.GroupID != "file-extractor" {
		t.Errorf("GroupID default: got %q", cfg.GroupID)
	}
	if cfg.BatchSize != 50 {
		t.Errorf("BatchSize default: got %d", cfg.BatchSize)
	}
	if cfg.TikaURL != "http://localhost:9998" {
		t.Errorf("TikaURL default: got %q", cfg.TikaURL)
	}
	if cfg.MaxFileSize != 20*1024*1024 {
		t.Errorf("MaxFileSize default: got %d want %d", cfg.MaxFileSize, 20*1024*1024)
	}
	if cfg.MaxContentBytes != 256*1024 {
		t.Errorf("MaxContentBytes default: got %d", cfg.MaxContentBytes)
	}
	if cfg.HTTPRetries != 3 {
		t.Errorf("HTTPRetries default: got %d", cfg.HTTPRetries)
	}
	if cfg.ExtractStartupDelay.Seconds() != 5 {
		t.Errorf("ExtractStartupDelay default: got %v", cfg.ExtractStartupDelay)
	}
}

// TestLoadConfig_V113ConfigDefaults v1.13 新增 6 字段未设 env 时留零值，由 fileextract 包内
// default 在 NewProcessor / newDownloadClient 兜底（10 / 1s / 60s / cdn.deepminer.com.cn / https / false）。
func TestLoadConfig_V113ConfigDefaults(t *testing.T) {
	resetEnv(t)
	t.Setenv("FILE_EXTRACTOR_ENABLED", "true")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("ES_ADDRESSES", "http://os:9200")
	cfg, _ := loadConfig()
	if cfg.MaxRetriesPerMessage != 0 {
		t.Errorf("MaxRetriesPerMessage default: got %d want 0 (fileextract pkg default kicks in)", cfg.MaxRetriesPerMessage)
	}
	if cfg.TransientBackoffBase != 0 {
		t.Errorf("TransientBackoffBase default: got %v want 0", cfg.TransientBackoffBase)
	}
	if cfg.TransientBackoffMax != 0 {
		t.Errorf("TransientBackoffMax default: got %v want 0", cfg.TransientBackoffMax)
	}
	if len(cfg.AllowedDownloadHosts) != 0 {
		t.Errorf("AllowedDownloadHosts default: got %v want empty (fileextract pkg default kicks in)", cfg.AllowedDownloadHosts)
	}
	if len(cfg.AllowedDownloadSchemes) != 0 {
		t.Errorf("AllowedDownloadSchemes default: got %v want empty", cfg.AllowedDownloadSchemes)
	}
	if cfg.SSRFAllowLoopback {
		t.Fatal("SSRFAllowLoopback must be false in prod (no env channel)")
	}
}

// TestLoadConfig_V113ConfigOverride v1.13 新 env 被读取到 cfg。SSRFAllowLoopback 无论 env
// 设什么都必须保持 false（生产安全 gate；测试专用只能通过 test cfg 注入）。
func TestLoadConfig_V113ConfigOverride(t *testing.T) {
	resetEnv(t)
	t.Setenv("FILE_EXTRACTOR_ENABLED", "true")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("ES_ADDRESSES", "http://os:9200")
	t.Setenv("EXTRACTOR_MAX_RETRIES_PER_MESSAGE", "5")
	t.Setenv("EXTRACTOR_TRANSIENT_BACKOFF_BASE_MS", "500")
	t.Setenv("EXTRACTOR_TRANSIENT_BACKOFF_MAX_MS", "30000")
	t.Setenv("ALLOWED_DOWNLOAD_HOSTS", "cdn.deepminer.com.cn,internal-cos.example")
	t.Setenv("ALLOWED_DOWNLOAD_SCHEMES", "https,http")
	// SSRFAllowLoopback 无 env 通道；设任何 env 都无效
	t.Setenv("SSRF_ALLOW_LOOPBACK", "true")
	cfg, _ := loadConfig()
	if cfg.MaxRetriesPerMessage != 5 {
		t.Errorf("MaxRetriesPerMessage: got %d want 5", cfg.MaxRetriesPerMessage)
	}
	if cfg.TransientBackoffBase != 500*time.Millisecond {
		t.Errorf("TransientBackoffBase: got %v want 500ms", cfg.TransientBackoffBase)
	}
	if cfg.TransientBackoffMax != 30*time.Second {
		t.Errorf("TransientBackoffMax: got %v want 30s", cfg.TransientBackoffMax)
	}
	if len(cfg.AllowedDownloadHosts) != 2 || cfg.AllowedDownloadHosts[0] != "cdn.deepminer.com.cn" || cfg.AllowedDownloadHosts[1] != "internal-cos.example" {
		t.Errorf("AllowedDownloadHosts: got %v", cfg.AllowedDownloadHosts)
	}
	if len(cfg.AllowedDownloadSchemes) != 2 || cfg.AllowedDownloadSchemes[0] != "https" || cfg.AllowedDownloadSchemes[1] != "http" {
		t.Errorf("AllowedDownloadSchemes: got %v", cfg.AllowedDownloadSchemes)
	}
	if cfg.SSRFAllowLoopback {
		t.Fatal("SSRFAllowLoopback must stay false even when SSRF_ALLOW_LOOPBACK=true env is set (no env channel by design)")
	}
}

// TestLoadConfig_DLQEnvOverride v1.13 P2-1 (yujiawei review) — DLQ 有界重试/spill 三个 env 挂载。
// SpillDir 默认空 → 硬停 pattern；生产设 emptyDir/PVC 路径转成 spill 逃逸。
func TestLoadConfig_DLQEnvOverride(t *testing.T) {
	resetEnv(t)
	t.Setenv("FILE_EXTRACTOR_ENABLED", "true")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("ES_ADDRESSES", "http://os:9200")

	// 默认 unset → cfg 都是 zero，由 dlqHandler 内部 default 兜底
	cfg, _ := loadConfig()
	if cfg.DLQMaxRetries != 0 {
		t.Errorf("DLQMaxRetries default: got %d want 0 (handler kicks default 5)", cfg.DLQMaxRetries)
	}
	if cfg.DLQRetryBackoff != 0 {
		t.Errorf("DLQRetryBackoff default: got %v want 0 (handler kicks default 200ms)", cfg.DLQRetryBackoff)
	}
	if cfg.DLQSpillDir != "" {
		t.Errorf("DLQSpillDir default: got %q want empty (hard-stop pattern)", cfg.DLQSpillDir)
	}

	// 设 env → cfg 载入
	t.Setenv("DLQ_MAX_RETRIES", "3")
	t.Setenv("DLQ_RETRY_BACKOFF_MS", "150")
	t.Setenv("DLQ_SPILL_DIR", "/var/lib/file-extractor/dlq-spill")
	cfg2, _ := loadConfig()
	if cfg2.DLQMaxRetries != 3 {
		t.Errorf("DLQMaxRetries: got %d want 3", cfg2.DLQMaxRetries)
	}
	if cfg2.DLQRetryBackoff != 150*time.Millisecond {
		t.Errorf("DLQRetryBackoff: got %v want 150ms", cfg2.DLQRetryBackoff)
	}
	if cfg2.DLQSpillDir != "/var/lib/file-extractor/dlq-spill" {
		t.Errorf("DLQSpillDir: got %q", cfg2.DLQSpillDir)
	}
}

// TestLoadConfig_ProdTopicOverride 覆盖 topic/DLQ topic 到 .prod 后缀（部署时环境变量注入）。
func TestLoadConfig_ProdTopicOverride(t *testing.T) {
	resetEnv(t)
	t.Setenv("FILE_EXTRACTOR_ENABLED", "true")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("ES_ADDRESSES", "http://os:9200")
	t.Setenv("KAFKA_TOPIC", "octo.message.v1.prod")
	t.Setenv("KAFKA_DLQ_TOPIC", "octo.message.v1.file-extract.dlq.prod")
	cfg, _ := loadConfig()
	if cfg.Topic != "octo.message.v1.prod" {
		t.Errorf("Topic override: got %q", cfg.Topic)
	}
	if cfg.DLQTopic != "octo.message.v1.file-extract.dlq.prod" {
		t.Errorf("DLQTopic override: got %q", cfg.DLQTopic)
	}
}
