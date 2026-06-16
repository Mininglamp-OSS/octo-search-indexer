# 索引契约收敛 + 前向迁移 (v1.8 → v1.9)

> YUJ-4534 阶段 8 · 分叉 B（两索引摄入链收敛）· 由 octo-search-indexer 侧实施。
> 权威 plan：YUJ-4662 PLAN-FULL v1 + YUJ-4534「Yu 裁决」(2026-06-16)。

## 为什么要收敛

octo-server 的读路径（`modules/messages_search`，PR#361/#374/#385）直查 OpenSearch，读的是
**reader 契约** `source.go::Doc`（camelCase 嵌套）。但本仓 indexer 历史上写的是另一套
**flat snake_case** doc（`message_id/content/...`）到索引 `octo-message`。两套字段名/形态不一致
——若 reader 读不到 indexer 写的字段，阶段 1-7 建的整条摄入链白做。

本次把 indexer 写成 **reader 能读的 doc 契约**（reader 不动）。逐字段对齐见下。

## 契约对齐表（v1.9，逐字段对照 reader `Doc`）

| reader Doc 字段 | 类型 | indexer 写入来源 | 备注 |
|---|---|---|---|
| `messageId` | long | message_id（VARCHAR(20) 解析为 int64） | **全精度**，snowflake id 不被 float64 截断；= ES `_id` |
| `messageSeq` | long | message.message_seq 列（backfill）| reader channel_offset「清空会话」gate |
| `from` | keyword | from_uid | sender 过滤 / 鉴权 |
| `channelId` | keyword | channel_id | 频道鉴权过滤 |
| `channelType` | integer | channel_type | 频道类型分流 |
| `spaceId` | keyword | payload.space_id（backfill）| **p2p space 召回过滤**，缺则 reader fail-closed（V1b 红） |
| `visibles` | keyword[] | payload.visibles（backfill）| **群系统消息白名单 gate**，缺则 reader fail-OPEN（V3b 红，安全项） |
| `timestamp` | date(epoch_second) | message.timestamp | sort 字段（+ messageId） |
| `payload.type` | integer | content_type | reader `_search_all` 分流 |
| `payload.text.content` | text(IK) | content（仅文本） | ik_max_word 建索引 / ik_smart 查询 |
| `createdAt` | long | UNIX_TIMESTAMP(created_at) | 对账窗口字段 |
| `rawExcluded` | boolean | raw_excluded | Signal/非文本占位 doc 标记 |
| `source` | keyword | source | 来源（ETL/CDC） |

- **排序**：reader cursor sort = `timestamp` + `messageId`（dsl.go::applySort）。两字段都已就位。
- **alias**：reader 读 `wukongim-messages-read`（pattern `wukongim-messages-*`）。新 mapping 内嵌该 alias。

## 实时 consumer 路径 vs backfill 路径（关键差异）

| 路径 | 数据来源 | 能否填 spaceId/visibles/messageSeq |
|---|---|---|
| backfill（`internal/backfill`） | **原始 MySQL payload + message_seq 列** | ✅ 能自源填全（V1b/V3b 解红） |
| 实时 consumer（`internal/consumer`） | Kafka 契约 `searchmsg.Message` | ❌ 契约不带这三字段 → 写空（reader 安全方向：fail-closed/无 gate/保守隐藏） |

**实时路径填全这三字段 = 阶段 9 上线前置（跨仓 sibling）**：需扩 `octo-lib/contract/searchmsg.Message`
（+SpaceID/+Visibles/+MessageSeq，SchemaVersion 1→2）+ octo-server `modules/searchetl` producer 富化，
随阶段 9 开 Kafka.On 一并上线。本期 Kafka.On=OFF（plan §6），无实时流量，存量由 backfill 填全即可。

> 🔒 **实时写入安全闸（防 V3b fail-OPEN）**：`consumer.Service.Run` 在 Kafka 契约
> `searchmsg.SchemaVersion < SafetyFieldsSchemaVersion(=2)` 时**拒启动**实时写入——否则会灌出
> 空 `visibles` 的 doc，让 reader 的群系统消息白名单 gate fail-OPEN（普通成员搜出群管才可见消息）。
> octo-lib bump 到带安全字段的契约 + producer 富化后，重 pin 自动解封。存量始终由 backfill 富化。

## 前向迁移三步（钉死，禁半新半旧 doc 同 alias 服务）

脚本：`scripts/forward-migrate.sh`（`make migrate-forward`）。

1. **写新契约索引**（mapping v1.9）：`STEP=1`。建索引时剥离 aliases 段，alias 第③步单独原子挂，
   保证 reindex 完成前 read alias 不指向半空新索引。
2. **存量 reindex** 旧索引 → 新契约索引：`STEP=2`。painless 把 flat snake_case 映射成 camelCase 嵌套。
   - ⚠️ reindex 只能搬旧索引**已有**字段；spaceId/visibles/messageSeq 旧链没写 → 新 doc 这三字段空。
   - **存量富化**：要让存量 doc 带 spaceId/visibles/messageSeq，跑 backfill 重灌（`cmd/backfill`，
     读原始 MySQL payload）到新索引（`BACKFILL_ES_INDEX=<新索引>`）。backfill 与 reindex 幂等同 _id，
     重叠安全；重灌覆盖 reindex 的空字段。生产推荐：直接 backfill 重灌新索引，跳过 reindex 的空壳。
3. **alias 原子切换** read alias → 新索引：`STEP=3`。**前置门**：先 `make run-recon RECON_ES_INDEX=<新索引>`
   抽样对账通过（doc_drift==0 且 sample_mismatch==0 且 sample_missing==0）再切。

### 回滚
- read alias 秒切回旧索引（脚本末尾 ROLLBACK 段）。
- 旧索引保留 ≥7 天再删。
- 切 alias 前的任意步骤失败：旧索引仍在 alias 上服务，零影响。

## 对账门（步骤 5）

`cmd/reconcile`（`make run-recon` / `make run-recon-json`）：
- **count 对账**：ES doc-count vs MySQL 行数（扣 DLQ；raw_excluded 仍占 doc 不扣）。`doc_drift = ESDocs − MySQL行数`。
- **抽样字段比对**：取 N 条样本，按 message_id 拉 ES doc，逐字段核对
  `messageId/channelId/channelType/spaceId/visibles/messageSeq`，检出「条数对得上但字段错位」的静默 drift。
- **阈值（机检，钉死在 internal/recon）**：`doc_drift!=0` 或 `sample_mismatch!=0` 或 `sample_missing!=0` → 不健康，退出码 2。
- **回填 octo-server**：`-push-url` POST `PushPayload`（逐字段对齐 `recon_metrics.go::ReconReport`）到只读
  ingestion 端 → `search_recon_doc_drift` / `search_recon_sample_mismatch` / `search_recon_last_run_timestamp_seconds` gauge。

## 上线纪律

本票只到「写新契约 + reindex/backfill + alias 切」的**代码与本地/预发验证**层面。
真实生产开 Kafka.On + 灰度放量归阶段 9，不在本票上线生产。
