package main

import (
	"os"
	"strings"
	"testing"
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
