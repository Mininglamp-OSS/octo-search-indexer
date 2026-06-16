# octo-search-indexer — build / test / recon entrypoints (YUJ-4534 / YUJ-4682)
.DEFAULT_GOAL := help

GO ?= go

.PHONY: help build test vet run-recon run-recon-json migrate-forward

help: ## 列出可用目标
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## 编译全部二进制
	$(GO) build ./...

vet: ## go vet
	$(GO) vet ./...

test: ## 跑全部单测
	$(GO) test ./... | tail -40

## ── 对账作业（步骤 5）─────────────────────────────────────────────────────────
## ES doc-count vs MySQL 行数 + 抽样字段比对，检出多 doc/少 doc/字段 drift。
## 对平退出 0；不对平退出 2（CI / cron gate 失败信号）。
## 指标/阈值钉死在 internal/recon（doc_drift!=0 / sample_mismatch!=0 / sample_missing!=0 即不健康）。
##
## 必需配置（env 或 flag）：
##   RECON_MYSQL_DSN   MySQL DSN，如 'user:pass@tcp(host:3306)/im_prod'
##   RECON_ES          OpenSearch 地址（默认 http://localhost:9200）
##   RECON_ES_INDEX    目标索引/别名（默认 octo-message；切换后对 wukongim-messages-read）
##   RECON_TABLES      message 分表（默认 message,message1,message2,message3,message4）
## 可选：RECON_SAMPLE（抽样数，默认 200）、RECON_FROM/RECON_TO（窗，纪元秒）、
##       RECON_DLQ（窗内已知 DLQ 数）、RECON_DLQ_SPILL_DIR（backfill 落下的 DLQ spill 目录；
##       其 message_id 从字段级抽样门排除，避免合法 DLQ 行被误判 sample_missing）、
##       RECON_PUSH_URL/RECON_PUSH_TOKEN（回填 octo-server gauge）。
RECON_FROM ?= 0
RECON_TO   ?= 0
RECON_DLQ  ?= 0
RECON_DLQ_SPILL_DIR ?=

run-recon: ## 跑对账（人类可读摘要 + 退出码 gate）
	$(GO) run ./cmd/reconcile -from $(RECON_FROM) -to $(RECON_TO) -dlq $(RECON_DLQ) -dlq-spill-dir '$(RECON_DLQ_SPILL_DIR)'

run-recon-json: ## 跑对账（结构化 JSON 报告，供机检 / 入库）
	$(GO) run ./cmd/reconcile -from $(RECON_FROM) -to $(RECON_TO) -dlq $(RECON_DLQ) -dlq-spill-dir '$(RECON_DLQ_SPILL_DIR)' -json

## ── 前向迁移（步骤 6 三步）─────────────────────────────────────────────────────
## 写新契约索引(v1.9) → 存量 reindex → alias 原子切换。详见 docs/forward-migration-v1.9.md。
migrate-forward: ## 执行前向迁移（写新索引 + reindex + alias 切，禁半新半旧同 alias）
	./scripts/forward-migrate.sh
