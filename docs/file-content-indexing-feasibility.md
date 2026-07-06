# File Content Indexing — Feasibility Study

> Author: cc-octo · Date: 2026-06-30 · **Version: v2**
> Scope: 评估「让 octo 搜索能搜到 type=8 文件消息的文件正文内容」可行性、推荐方案、风险与未决问题。
> 输入命题: 用户发了一个 PDF，搜「季度营收」 → 列出这个文件消息（+ 高亮命中片段）。
> Companion: 抽取工具深入对比见 [`file-extractor-tool-comparison.md`](./file-extractor-tool-comparison.md)

---

## Changelog

**v2 (2026-06-30, 集成 Max review + 主人拍板 + prod 实测数据)**：

| # | 变更点 | 位置 |
|---|---|---|
| 1 | §4.1 双 hit 论证收语气 → "边缘 case 下双 hit" | §4.1 |
| 2 | §5 补充 docling/MinerU 阶段 2 OCR 考量一句话 | §5 尾 |
| 3 | MVP 三步 → 两步：mapping + indexer + file-extractor + Tika 一锅端上 test，然后 reader + prod + backfill | §13 |
| 4 | §12.1 数据安全从"⚠️ 真风险"降为"不是 blocker，PRD 走个流程"；§12.2 未决问题 #3 撤回 | §12 |
| 5 | 🆕 §16「文件获取链路」— 推荐 (d) 公网 CDN URL 直连（Max 实测通过） | §16 |
| 6 | §6 DLQ reason 表细化 `EncryptedDocumentException` catch 路径 | §6 |
| 7 | §11 资源估算全部替换为 prod 实测数据（type=8=123,628，OS 可用 726GB，增量 3-5GB） | §11 |
| 8 | 🆕 §16.3 file-extractor 部署形态两方案并列（单 Pod 双容器 vs 独立 Deploy） | §16.3 |
| 9 | §5 引用 tool-comparison 时新增"Go 原生 unidoc/unipdf AGPL 传染风险，硬阻断" | §5 |
| 10 | §0 总判断：可行性结论保持，容量/风险描述按实测收敛 | §0 |

**v1 (2026-06-30, 初版)**：三仓扫源 → 9 维度方案对比 + MVP 三步走 + 未决问题清单。

---

## 0. 总体可行性判断（一句话）

**可行 — 推荐分两阶段交付**：阶段 1 直接扩 `payload.file.content` 字段 + 起独立 `file-extractor` 服务异步抽取后回写（覆盖 90% 业务场景，~2 周）。阶段 2 视扫描版 PDF 实际占比决定要不要引入 MinerU 兜底 OCR。

**prod 实测容量绰绰有余**：type=8 = **123,628 条**（占 17M 总 docs 的 0.7%），OS 可用磁盘 726GB，content 字段增量 **3-5GB**，不到 1%。

**不推荐** Max 提的方案 4-C（type=8 派生 virtual sub-doc）— 理由见 §4：边缘 case 双 doc 命中冲突 + 复杂度回流。

---

## 1. 推荐方案总图

```
┌──────────────┐  MySQL binlog/scan   ┌──────────────────────┐  octo.message.v1   ┌─────────────┐
│   MySQL      │ ───────────────────► │ searchetl-producer   │ ─────────────────► │ Kafka topic │
│ message_*    │                      │ (现存，不改)         │                    │             │
└──────────────┘                      └──────────────────────┘                    └──────┬──────┘
                                                                                          │
                                            ┌─────────────────────────────────────────────┴───────┐
                                            │                                                     │
                                            ▼ (consumer group: octo-search-indexer)               ▼ (consumer group: file-extractor) [新增]
                                  ┌──────────────────┐                                ┌────────────────────────┐
                                  │  es-indexer      │                                │  file-extractor        │
                                  │  (现存)          │                                │  (新独立 Deploy)       │
                                  │ 写主 doc         │                                │ • 过滤 type=8          │
                                  │ payload.file = { │                                │ • GET CDN url (公网)  │
                                  │   url,name,      │                                │ • 调 Tika HTTP        │
                                  │   caption,size,  │                                │ • OS partial-update   │
                                  │   ext            │                                │   payload.file.content│
                                  │ }                │                                │ • 失败 → DLQ          │
                                  └────────┬─────────┘                                └────────────┬───────────┘
                                           │                                                       │
                                           ▼                                                       ▼
                                  ┌────────────────────────────────────────────────────────────────┐
                                  │  OpenSearch backing index ...-2026.06-000001                   │
                                  │  payload.file.content (新 mapping field, IK 分词)              │
                                  └────────────────────────────────────────────────────────────────┘
                                                              ▲
                                                              │  (reader 端搜 payload.file.name/caption/content)
                                                              │
                                  ┌────────────────────────────┴───────────────────────────────────┐
                                  │  octo-server /v1/messages/_search_files                        │
                                  │  + payload.file.content^0.5 加入 multi_match                   │
                                  │  + highlight payload.file.content                              │
                                  └────────────────────────────────────────────────────────────────┘
```

**核心选择四连**：

| 维度 | 选择 |
|---|---|
| 抽取位置 | 独立 file-extractor Deploy（新增，见 §3） |
| 存储位置 | 扩 `FilePayload.Content` 进**同 doc**（不另起索引、不派生子 doc，见 §7） |
| 抽取工具 | Apache Tika sidecar（HTTP 模式，详见 [tool-comparison](./file-extractor-tool-comparison.md)） |
| **文件获取** | **公网 CDN URL 直连**（Max 实测通过，详见 §16） |

---

## 2. 当前现状定锚（避免重复扫）

### 2.1 文件链路 (octo-server)
- 上传上限 `MaxFileSize = 100MB`（`modules/file/const.go:128`）
- 白名单 60 种扩展名（图/文档/音/视/压缩/其他），见 `allowedExtensions`
- magic-number 校验，过滤伪装可执行文件
- 存储后端：腾讯云 COS，CDN 域名 `cdn.deepminer.com.cn`（Max 实测 prod URL 形态 `https://cdn.deepminer.com.cn/im-test-xming/chat/...`）

### 2.2 Kafka 契约 (octo-lib `searchmsg.Message`)
- `RawPayload` 整包送入；indexer 端 `buildraw.go` 按 `payload.type` 分流投影
- type=8 当前投影 `{url, name, caption, size, extension}`，**无 content 字段**

### 2.3 OS Mapping (`internal/esindex/mapping/octo-message.json`)
- `payload.file` 已有 `name`（IK）+ `caption`（IK）+ `url`(keyword) + `size`(long) + `extension`(keyword)
- `dynamic: "strict"` — 新增字段必须改 mapping
- 已有 v1.10/v1.11 virtual sub-doc 机制 (`parentMessageId/parentPayloadType/virtual/subSeq`)，本期只派生 richText→image (type=14→type=2)

### 2.4 Reader (octo-server `modules/messages_search/search_files.go`)
- `_search_files` filter `payload.type=8` + multi_match `payload.file.name^2 + payload.file.caption`
- ⚠️ search_files **未** apply `applyExcludeVirtual` — 这是后面方案 C 的关键反对理由
- `_search_all` keyword 路径 whitelist = `[1,8,11,14]`，已含 file (`search_all.go:142`)
- reader join MySQL 过滤撤回（`applyChannelAndRevoked`）— 撤回联动天然 handled

### 2.5 prod 实测容量数据（Max 拉自跳板）
- type=8 文件消息 count = **123,628**（占 17M 总 docs 的 **0.7%**）
- OS 集群：3 nodes × 267GB = **801GB 总容量**，已用 ~85GB，**可用 726GB**
- 当前 indices 总大小 ~12.4GB

---

## 3. 抽取位置（9 维度逐一）

| 选项 | 推荐? | trade-off |
|---|---|---|
| **A. producer 同步抽** | ❌ | producer 现在 5s tick 拉 MySQL 极轻量 (~30min 追平 3.4M)，加抽取拖慢主链路 + 镜像变重 + 文件 URL 抽取依赖外部 CDN 可达性 |
| **B. 独立 file-extractor Deploy** | ✅ **推荐** | 解耦 — 失败/退避不影响 es-indexer；可独立扩容；可独立金丝雀；和 es-indexer 共享 image 模式继续可用（同镜像两 Deploy + `EXTRACTOR_ENABLED=true` 切角色，复用 producer/indexer 现有套路） |
| C. es-indexer 内抽 | ❌ | 抽取 RT 长（PDF 10MB 抽取秒级），会拉低 indexer 吞吐 → Kafka lag |
| D. octo-server 上传同步抽 | ❌ | 阻塞用户上传 UX；上传成功 != 抽取成功，无重试机制 |

**B 方案细节**：
- 复用 `octo-lib/contract/searchmsg` 消息契约，消费 `octo.message.v1{,.prod}` topic
- consumer group 独立 `file-extractor`（不抢 es-indexer 的位点）
- 命中 `payload.type != 8` 直接 commit 跳过
- 命中 type=8 → HTTP GET `payload.file.url`（公网 CDN，详见 §16）→ 调 Tika → 走 OS partial update API 仅写 `payload.file.content`（不动主 doc 其他字段）
- DLQ 独立 topic `octo.message.v1.file-extract.dlq{,.prod}`

---

## 4. 为什么不选方案 4-C（虚拟子文档）

主人在派单里说「方案 C 最自然，请你重点评估可行性」— 评估完结论：**技术上可行，但反而比 4-A 复杂、收益更低**。

### 4.1 边缘 case 双 hit 冲突
4-C 方案下主 doc 的 content 字段本就是空的（内容只在 virtual sub-doc），日常搜索命中只会返回 sub-doc；**但当关键词同时出现在 name/caption 里 + content 里时**，主 doc + virtual sub-doc 都命中，返回结果里同一物理文件出现 2 条 hit（父 + 子，messageId 相同）。频率不高但**必然存在**（文件名往往含内容关键词，如"2026Q2财报_季度营收.pdf"）。

修复需要 reader 在 `_search_files` 加 dedupe by messageId 或改 `applyExcludeVirtual` 逻辑 + 把搜索 fields 同时配在父和子上 — 转一圈**复杂度回到了 4-A**。

### 4.2 v1.11 设计文档明示 sub-doc 语义
`internal/esindex/mapping/README.md` v1.11 子文档目的：「让富文本里内嵌的 image 能被 reader `_search_media` 搜到」。**核心是「父 doc 的 payload.type 跟子 doc 不一样」**（父 type=14, 子 type=2）。type=8 文件本身已经是独立消息、独立 doc、独立 type=8，**没有"派生不同 type"的语义**，硬塞 sub-doc 机制是为模式而用模式。

### 4.3 撤回联动
v1.10 路线甲设计「reader 用 parentMessageId 回 MySQL join 判可见性/撤回」是为「子 doc 没有独立 MySQL 行」准备的。文件消息**本身有 MySQL 行**，撤回信号现成走主 doc 路径，sub-doc 模式徒增 join 复杂度。

### 4.4 结论
方案 4-A（扩 `payload.file.content`）：mapping 改 1 行 + indexer FilePayload 加 1 字段 + reader multi_match 加 1 field + highlight 加 1 行。完。

---

## 5. 抽取工具

**详细五方案对比 + 决策矩阵见 [file-extractor-tool-comparison.md](./file-extractor-tool-comparison.md)**。此处只列本文档需要的结论：

| 候选 | 结论 | 一句话 |
|---|---|---|
| **Apache Tika Server (HTTP sidecar)** | ✅ **阶段 1 主引擎** | 覆盖 19 种白名单格式 + 无 GPU 无 Python 依赖 + JVM 成本可控 |
| MinerU | 阶段 2 兜底候选 | 中文扫描版王者，但 GPU 硬依赖 + 输出结构过度投入 |
| Docling | 备选 | Chinese "not yet enterprise-validated"（IBM 官方原话） |
| unstructured.io | 不用 | 商用 API 云依赖 + 已有 Kafka pipeline 不需要再套 ETL |
| Go 原生组合 | ❌ **硬阻断** | `unidoc/unipdf` AGPL 传染风险与公司 OSS 政策冲突；纯 Go PDF 库中文支持不成熟 |
| LibreOffice headless | 不用 | subprocess 冷启动 2-5s，稳定性坑多 |

**Tika 使用形态**：`apache/tika:3.3.0.0` minimal 镜像（~500MB，Max 待实拉验证），`--server` 模式监听 9998，`PUT /tika` header `Accept: text/plain`。

**OCR 决策**：
- 阶段 1 **不做** OCR（minimal 镜像不预装 Tesseract）
- 扫描版 PDF 抽出空串 → DLQ reason=`empty_extract`
- 阶段 2 分支决策：`empty_extract` 占比 < 10% 不做 OCR；10-30% 切 Tika full 镜像开 Tesseract；> 30% 且业务确认多为中文扫描版 → 引入 MinerU 作为兜底 pipeline（Tika 抽空 → 转发 MinerU 补抽）。**docling 不作为 OCR 候选**（中文实验性 + 输出结构复杂度不匹配需求）

---

## 6. 大文件 / 抽取约束策略

| 策略点 | 建议值 | 理由 |
|---|---|---|
| 单文件抽取 size cutoff | **≤ 20MB** | 100MB 上限的 20%；20MB PDF 已属罕见超大；超过直接跳过 (DLQ reason=`oversize`) |
| 抽取超时 | **30s** | Tika 默认 120s 过长；30s 覆盖 95%+ 文档；超时跳过 + DLQ |
| 抽取出文本截断 | **≤ 256KB** UTF-8 | IK 分词器对超长文本效率断崖；超过截断 + 标记 `truncated=true`（前 256KB 已够搜索命中） |
| 抽取失败重试 | **3 次指数退避**（1s/4s/16s） | 文件 URL 临时 5xx 常见 |
| 重试耗尽 | 进 DLQ | 不影响主链路 commit |
| 黑名单扩展名（不抽取） | `.mp4 .mov .avi .mkv .webm .flv .wmv .m4v .mp3 .wav .aac .flac .ogg .wma .m4a .amr .zip .rar .7z .tar .gz .bz2 .xz .dmg .pkg .deb .rpm .appimage .jpg .jpeg .png .gif .bmp .webp .ico` | 二进制媒体/压缩包/图片，抽取无意义；白名单 60 种里约 26 种纯文本可抽 |

**抽取目标扩展名**（白名单）：`.pdf .doc .docx .xls .xlsx .ppt .pptx .txt .csv .rtf .odt .ods .md .html .htm .json .xml .yaml .yml`（19 种，覆盖业务搜索需求）

### 6.1 DLQ reason 完整清单（含 Tika 异常映射）

| reason | 触发条件 | 是否重试 | 备注 |
|---|---|---|---|
| `oversize` | 文件 > 20MB | ❌ | 上传时已过 100MB 门；此处二次筛 |
| `blacklist_ext` | 扩展名在黑名单 | ❌ | 二进制/媒体，不抽 |
| `download_failed` | HTTP GET CDN 5xx 或连接超时 | ✅ 3 次 | 3 次仍失败入 DLQ |
| `extract_timeout` | Tika 抽取 > 30s | ❌ | 超时不重试（重试仍会超） |
| `encrypted` | Tika 抛 `org.apache.tika.exception.EncryptedDocumentException` | ❌ | Java 侧异常 → HTTP 500 body 含类名 → Go 侧 catch reason 字符串 |
| `empty_extract` | Tika 返回空串或空白 | ❌ | 通常是扫描版 PDF / 加密后的破损文件 |
| `parse_error` | Tika 其他 parse 异常（`TikaException` etc.） | ✅ 1 次 | 有时 Tika 内部 GC 抖动导致伪失败 |
| `truncated` | 抽取成功但内容 > 256KB | N/A | 不进 DLQ，写 `contentMeta.truncated=true` |

---

## 7. ES 存储设计

| 方案 | trade-off |
|---|---|
| **A. 扩 `FilePayload.Content` 进主 doc** ✅ **推荐** | mapping 改动小；reader 只改 fields list；但 _source 膨胀（用 `_source.excludes: ["payload.file.content"]` 缓解，content 仍可被搜被 highlight） |
| B. 独立 index `octo-file-content-{env}` | mapping 干净 + 可独立 retention；但 reader 要 multi-index 搜索 + 父子 doc dedupe，复杂 |
| C. 富文本 virtual sub-doc | **不推荐** — §4 已论证 |

**推荐 mapping 增量**（最小改动）：

```json
"payload": {
  "properties": {
    "file": {
      "properties": {
        "url":       { "type": "keyword", "ignore_above": 1024 },
        "name":      { "type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart" },
        "caption":   { "type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart" },
        "size":      { "type": "long" },
        "extension": { "type": "keyword", "ignore_above": 32 },
        "content":   { "type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart" },   // ← 新增
        "contentMeta": {                                                                                // ← 新增（可选）
          "type": "object",
          "properties": {
            "extractedAt": { "type": "date", "format": "epoch_second" },
            "extractor":   { "type": "keyword", "ignore_above": 32 },
            "truncated":   { "type": "boolean" },
            "extractMs":   { "type": "long" }
          }
        }
      }
    }
  }
}
```

**_source 控制**：
- 加 `"_source": { "excludes": ["payload.file.content"] }` 到 mapping settings
- 搜索时 highlight 仍能拿到片段（highlight 走倒排不走 _source）
- reader 端用 `requireFieldMatch=true` + `payload.file.content` field 高亮

---

## 8. 搜索端 API 改动 (octo-server)

### 8.1 `_search_files` (主战场)
```go
// modules/messages_search/search_files.go::buildSearchFilesDSL
clause, err := buildKeywordClauseGated(ctx, analyzer, stopwordStripEnabled, req.Keyword,
    "payload.file.name^2",      // 文件名最高权重（短，命中度高）
    "payload.file.caption",     // caption 次之
    "payload.file.content^0.5", // ← 新增 content，权重 < name/caption（内容长易噪声）
)
b.Must(clause)
```
- highlight: 在 `_search_files` 加 highlight 配置（当前没有；之前 search_messages 才有），新增 field `payload.file.content` + `FragmentSize(120)` + `NumOfFragments(1)`

### 8.2 `_search_all` (大全搜)
```go
// modules/messages_search/search_all.go:171-172
"payload.file.name^2",
"payload.file.caption",
"payload.file.content^0.5",  // ← 新增
```
- pickSnippet field 优先级表 (`dsl.go:202`) 在 `payload.file.name` 后追加 `payload.file.content`

### 8.3 `_search` (text 类) 不动
- 当前 type whitelist `[1,11,14]` 不含 file，content 不参与 text 搜
- 不变更口径，避免文本搜结果被文件内容污染

---

## 9. 历史回灌

| 问题 | 答案 |
|---|---|
| prod 现有 type=8 数量 | **123,628 条**（Max 实测，占 17M 总 docs 的 0.7%） |
| 历史是否回灌？ | **建议回灌**（否则历史文件全文搜不到，体验断层） |
| 怎么做 | 起 one-off Job `cmd/file-content-backfill`（仿 `cmd/backfill` 模式），从 OS 反查 type=8 docs → 触发 file-extractor 抽取 → partial update |
| 切流策略 | (1) test 环境验证 file-extractor + Tika；(2) prod 部署 + 接增量；(3) 等增量稳定 24h；(4) 跑历史 backfill Job（限速 50 RPS 避免压垮 Tika 或 CDN）；(5) 抽完核对 OS count(content) ≈ count(type=8 in whitelist) |
| 回灌耗时估算 | 123K 文件 × 50 RPS ≈ **40 分钟**（假设 Tika 抽取 avg 200ms/file）— 半小时到 1 小时窗口即可完成 |

---

## 10. 撤回 / 删除联动

**结论：不用单独处理**。

- file 消息撤回路径：MySQL `message_*` 行 `revoked=1` → reader join MySQL 时 `applyChannelAndRevoked` 直接过滤
- 选方案 7-A（扩主 doc）后，`payload.file.content` 跟父 doc 同生同死，OS doc 不删，reader 端过滤
- 与现有撤回机制零差异

---

## 11. 资源 / 成本预估（prod 实测口径 v2）

| 项 | 实测/预估 | 备注 |
|---|---|---|
| **prod type=8 count** | **123,628** | 占 17M 总 docs 的 **0.7%**（远低于 v1 估算的 525K） |
| **OS 集群总容量** | 3 nodes × 267GB = **801GB** | |
| **OS 已用磁盘** | ~85GB | 当前 indices 12.4GB |
| **OS 可用磁盘** | **726GB** | 90% 空闲 |
| **content 字段增量估算** | **3-5GB** | 123K × 30KB 平均 ≈ 3.7GB；含倒排 + 不入 _source |
| **容量占比** | **< 1%** | 相对可用 726GB 忽略不计 |
| file-extractor Deploy | 1 副本，0.5 CPU / 512MB 起步 | prod 看 lag 扩到 3 副本 |
| Tika sidecar | 1 副本，1 CPU / 1.5GB | JVM 吃内存 |
| Kafka 容量 | 0 | 复用现有 topic |
| 抽取吞吐 | 单 Tika 实例 ~5-10 docs/s（PDF 主导） | 3 副本足够 100 docs/s 峰值 |
| 上线阶段 1 工时 | ~2 周 | mapping + indexer 改字段 + file-extractor 服务 + 部署 + e2e 测试 |

**结论**：容量层零风险；机器资源需求最小（file-extractor 1 副本 + Tika sidecar 1 副本 起步）。

---

## 12. 风险与未决问题清单

### 12.1 风险（标 → 缓解）

| 风险 | 缓解 |
|---|---|
| 加密 PDF / 带密码文档 | Tika 抛 `EncryptedDocumentException` → HTTP 500 body 含类名 → Go 侧 catch reason 字符串 → DLQ reason=`encrypted`；前端搜不到但不影响功能（见 §6.1） |
| 二进制畸形文件 | Tika 兜底返空串 → DLQ reason=`empty_extract` |
| 中文分词覆盖 | 复用现有 IK 配置，零风险（payload.text.content 已验证） |
| 数据安全边界 | ✅ **不是 blocker，PRD 走个流程即可**。搜索鉴权已是 channel 维度（`checkChannelAccess`），文件在会话里所有成员都能下载阅读，"搜 content" = "搜可见内容"，**不是新增权限边界**。上线前 PRD 走个 review 走个流程即可 |
| 抽取服务故障导致 lag | file-extractor 跟 es-indexer 独立 consumer group，互不影响；监控 file-extractor lag > 1000 告警 |
| Tika OOM | Tika sidecar 加 memory limit 1.5GB + JVM `-Xmx1g`；2.x+ 走 pipes-async，`--spawnChild` 默认开，OOM 只 kill child 不带崩 server |
| OS 容量超 | ✅ **风险几乎为零**（v2 实测：726GB 可用，content 增量 3-5GB） |
| 撤回时间窗 | 文件撤回但 content 已索引到 OS — reader join MySQL 过滤掉，不显示，但 OS 内部仍有数据。如有合规要求"撤回即物理删除"需另议（不在本期范围） |
| CDN 公网访问失败 | HTTP GET 失败 → 3 次退避重试 → DLQ reason=`download_failed`；备胎方案 (a) 内部 COS SDK 直连保留说明"如公网访问出问题时切"（详见 §16） |
| CDN 出口带宽成本 | 走公网 CDN 而非内网直连 COS，出口费未测；123K 文件 × avg 5MB ≈ **600GB** 一次性回灌流量，日常增量文件流量小；建议阶段 1 上线后观察 CDN 账单 |

### 12.2 未决问题（要主人 / Max 拍板）

1. ✅ **prod 现有 type=8 数量** — 已解，Max 实测 = 123,628
2. **是否需要 OCR**（扫描版 PDF/图片型 PDF 抽出为空的占比未知）— 阶段 1 先不做，阶段 2 看实际占比。**需要业务侧 Max 抽 100 个真实业务 PDF 肉眼判"文本原生 vs 扫描"占比**，决定阶段 2 是否上 Tesseract 或 MinerU
3. ~~数据安全审查~~ ✅ **已撤回**（v2 §12.1 已论证）
4. **历史是否回灌** — 默认建议回灌；否决方需说明业务可接受度
5. ✅ **OS 集群当前 disk 余量** — 已解，Max 实测 726GB 可用
6. 🆕 **file-extractor 部署形态**（单 Pod 双容器 vs 独立 Deploy + Service）— 见 §16.3 两方案并列，MVP 先按单 Pod 起，可后续拆分

---

## 13. 最小 MVP 路线 (两步走 v2)

按交付顺序：

### MVP-1 (week 1-1.5): 一锅端上 test
一次性做完：
- 改 `internal/esindex/mapping/octo-message.json` 加 `payload.file.content` + `contentMeta`
- 改 `internal/esindex/doc.go::FilePayload` 加 `Content string` + `ContentMeta`
- 改 `internal/esindex/mapping_compat.go::requiredMappingFieldPaths` 加新字段
- 起 `cmd/file-extractor` 单文件 main，消费 Kafka 同 topic、独立 consumer group `file-extractor`
- 加 Tika sidecar 到 file-extractor Deploy（`apache/tika:3.3.0.0` minimal）
- 实现：consume → filter type=8 → HTTP GET `payload.file.url`（CDN 直连） → 调 Tika → OS partial update `payload.file.content`
- DLQ 实现（§6.1 完整 reason 表）
- **验证**：test 环境 e2e — 发一个 PDF → 1 分钟内 OS doc.payload.file.content 有内容 → 手工 `_msearch` 用 content 关键词命中该 doc
- **金丝雀**：只在 test 跑 1 周观察 DLQ 比例 < 5%

### MVP-2 (week 2): reader 改 + 上 prod + 历史 backfill
- 改 `search_files.go` + `search_all.go` 加 content field 权重 + highlight
- 跑 octo-server CI 全套（dsl_test / search_files_test）
- prod 切流顺序：
  1. reader 先 ready（允许 content field 不存在，向后兼容）
  2. file-extractor 上线 prod，接增量
  3. 稳定 24h 后跑历史 backfill Job（限速 50 RPS）
  4. 抽完核对 OS `count(payload.file.content exists) / count(type=8 in whitelist) > 90%`
- **验证**：prod 选 10 个已知 PDF 文件，调 `_search_files` 用 PDF 内容关键词搜，能命中

---

## 14. Out of scope（本期不做）

- ❌ 图片 OCR (image OCR 是另一条线，需 GPU + 模型)
- ❌ 音视频转录 (Whisper 类，更大工程)
- ❌ 文件智能问答（文件向量化 + RAG）
- ❌ 跨文件去重 / 文件相似度
- ❌ 文件内容审核 / 合规扫描
- ❌ 富文本(type=14) 内嵌文件抽取（octo-lib `ValidateRichTextBlocks` 当前还不放行 file block，先等契约打开再说，参见 `richtext_derive.go:13`）

---

## 15. 给 Max 的回执清单

- (a) **文档路径**：`~/Project/Mininglamp-OSS/octo-search-indexer/docs/file-content-indexing-feasibility.md`
- (b) **可行性总判断**：可行，推荐 §1 总图四连（B 独立 extractor + A 扩主 doc + Tika sidecar + CDN URL 直连），不选 4-C virtual sub-doc
- (c) **v2 剩余未决问题（要主人拍板）**：
  1. **扫描版 PDF 占比** — 需 Max 抽 100 个真实业务 PDF 肉眼判断，决定阶段 2 是否上 OCR / MinerU
  2. **是否回灌历史 123K 文件** — 默认建议回灌，回灌窗口 ~40 分钟
  3. **file-extractor 部署形态** — 单 Pod 双容器 vs 独立 Deploy，MVP 先按单 Pod 起，可后续拆分（§16.3）

---

## 16. 文件获取链路（🆕 v2 新增）

file-extractor 拿到 Kafka 消息里的 `payload.file.url` 后，怎么把文件字节读出来给 Tika？四种方案：

### 16.1 方案对比

| 方案 | 描述 | 推荐? |
|---|---|---|
| (a) 内部 COS/S3 SDK 直连 | file-extractor 复用 octo-server 的 `cfg.FileService` 配置，直接初始化同一个 storage backend client，绕过 HTTP 层 | 备胎 |
| (b) 特权 service token | 给 file-extractor 一个 service token bypass octo-server auth | ❌ 反 pattern |
| (c) 新增 internal-only endpoint | 在 octo-server 加 `/internal/file/raw/*path` 不走用户 auth | 备胎 |
| **(d) 公网 CDN URL 直连** | HTTP GET `payload.file.url`（腾讯云 COS CDN 直挂域名），无 auth 无签名 | ✅ **推荐** |

### 16.2 方案 (d) 推荐理由

**Max prod 实测事实**（2026-06-30）：
- `payload.file.url` 形态：`https://cdn.deepminer.com.cn/im-test-xming/chat/...`（腾讯云 COS CDN 直挂域名）
- 公网可达，无 auth 无签名
- 三种真实格式 curl 测试：

| 格式 | 大小 | Content-Type | 状态 |
|---|---|---|---|
| PDF | 128KB | `application/pdf` | ✅ 200，magic `%PDF-1.7` 校验通过 |
| DOCX | 16KB | `application/vnd.openxmlformats-officedocument.wordprocessingml.document` | ✅ 200 |
| PPTX | 5MB | `application/vnd.openxmlformats-officedocument.presentationml.presentation` | ✅ 200 |

**方案 (d) 优点**：
- **零耦合**：不绑 COS SDK / 不改 octo-server / 不需要 auth 逻辑
- **依赖最少**：file-extractor 只依赖 Kafka + Tika + OS
- **未来兼容**：将来存储后端换掉（S3 / MinIO / 自建），只要 URL 还能 HTTP GET 就零改动
- **实现最简**：`http.Get(url)` 一行

**方案 (d) 潜在风险**（不阻塞 MVP，标为观察项）：
- 走公网 CDN 而非内网直连 COS，出口带宽成本（回灌 123K × avg 5MB ≈ 600GB 一次性流量，账单待观察）
- CDN miss 回源速度未测（正常业务上传后热数据应命中 CDN 缓存）
- 如 CDN 或域名遇故障 → file-extractor 全线抽取失败 → DLQ 暴增（同 octo-web 用户下载失败情形，风险共担）

**降级路径**：若 CDN 访问出现系统性问题，切换到方案 (a) 内部 COS SDK 直连；改动限于 file-extractor 内部 download client 实现，接口不变。

### 16.3 file-extractor 部署形态（方案分歧，两方案并列）

MVP 阶段先按 **方案 α** 起，观察 1-2 周后再决定是否拆到 β。

| 方案 | 描述 | 优点 | 缺点 |
|---|---|---|---|
| **α. 单 Pod 双容器 (sidecar)** ✅ **MVP 用** | file-extractor 主容器 + Tika 副容器同 Pod，共享 emptyDir 临时盘 | 部署单元少（1 个 Deploy）；延迟低（localhost HTTP）；配置简单；符合 kubernetes sidecar pattern | Tika 和 extractor 生命周期绑定，rolling update 时 Tika 也重启浪费；扩缩策略无法独立（Tika 内存重、extractor IO 重） |
| β. Tika 独立 Deploy + Service | file-extractor Deploy 通过 K8s Service 调 Tika Deploy | Tika 可独立扩缩 / 独立 rolling；将来 chatbot 文件问答 / 审核可复用同一个 Tika 服务；解耦干净 | 多一个 Deploy 要维护；网络多一跳（K8s ClusterIP）；MVP 阶段无第二个消费方，收益兑现不了 |

**决策**：MVP 用 α（单 Pod）先跑；观察运维摩擦（是否 rolling update 频繁重启 Tika 造成实际浪费？是否出现第二个 Tika 消费方？）后再决定是否拆到 β。**拆分改动小**（只是把 sidecar 容器搬到独立 Deploy + 改 file-extractor 的 Tika endpoint 从 `localhost:9998` 到 `tika-service:9998`），不是不可逆决策，MVP 不阻塞。

---

## 附录 A: v1 → v2 diff 摘要

- **总判断保留**：可行，两阶段交付；不选 4-C
- **实测数据替换**：type=8 count 525K→**123,628**；OS 容量增量 15-25GB→**3-5GB**；OS 可用 **726GB**（新披露）
- **文件获取链路澄清**：v1 §12.2 遗漏项（Max review 指出），v2 §16 补齐并推荐 CDN 直连（Max 实测通过）
- **数据安全降级**：v1 列为"⚠️ 真风险"，v2 §12.1 降为"不是 blocker"（Max 判断 channel 鉴权已覆盖）
- **MVP 三步→两步**：v1 MVP-1 只改 mapping 无数据价值低，合并到 MVP-2
- **DLQ reason 表完善**：v2 §6.1 8 种 reason 完整清单，含 EncryptedDocumentException catch 路径
- **抽取工具决策抽离**：v1 §5 表格短，v2 引用 [tool-comparison](./file-extractor-tool-comparison.md) 深入版；补 Go 原生 AGPL 硬阻断
- **部署形态两方案并列**：v2 §16.3 新增；MVP 用单 Pod，可后续拆分
