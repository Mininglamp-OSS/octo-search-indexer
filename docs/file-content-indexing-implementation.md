# File Content Indexing — Implementation Task Book (octo-search-indexer, single PR)

> Author: cc-octo · Date: 2026-07-01 · Status: **v2 sig-off, 开工中**
>
> **v2 changelog (2026-07-01, 集成 Max review 4 条 + 主人授权)**：
> 1. §7 未决问题 #1（file-extractor vs es-indexer 竞态）：MVP 走 5s delay + Kafka rebalance 自然重试；独立 retry topic 作为 phase 2 备选
> 2. §6.2 test 环境提前起 Tika sidecar 改为**硬要求**（不是 optional）
> 3. §5 mapping 不可逆警告嵌入 IDX-1 commit message + PR description 顶部
> 4. §8 估时 IDX-4 5h→**8-10h**，总估 11h→**13-15h ≈ 2 工作日**；timebox IDX-4 开发超 6h / 测试超 4h 回来对齐
> Repo scope: **`Mininglamp-OSS/octo-search-indexer`** 单仓单 PR，5 commit 串行
> 依赖文档: [feasibility v2](./file-content-indexing-feasibility.md) + [tool-comparison](./file-extractor-tool-comparison.md)
> **不在本任务书范围**：octo-server 的 `search_files.go / search_all.go` 改动（独立 PR，独立仓库）；deploy YAML（在 codex `dmwork/octo-search-indexer` manifests/ 下，独立 OPS-1/OPS-2 任务）

---

## §1 分支 + PR 元信息

### 1.1 分支
- **Base branch**: `Mininglamp-OSS/octo-search-indexer` 远程最新 `main`
- **分支名**: `feat/file-content-indexing`
- **Checkout 命令**：
  ```bash
  cd ~/Project/Mininglamp-OSS/octo-search-indexer
  git fetch origin main
  git checkout -b feat/file-content-indexing origin/main
  ```

### 1.2 Commit 消息模板
每个 commit 用 conventional-commits 前缀（本仓 git log 已是这个风格），格式：
```
<type>(<scope>): <subject>

<body — 一段说清「改了什么、为什么、验证方式」>

Refs: docs/file-content-indexing-feasibility.md, docs/file-content-indexing-implementation.md
```

**⚠️ IDX-1 commit message 必须嵌以下警告段**（源自 Max review v2 §3）：
> ⚠️ 此 PR 引入的 mapping 变更（`payload.file.content` + `contentMeta`）PUT 到 live index 后**不可逆**——OS 3.x mapping 字段只能通过 reindex + 切换 alias 才能移除。合并前 sig-off 意味着接受该单向决策。

### 1.3 PR title 模板
```
feat: file content indexing (payload.file.content + file-extractor + backfill)
```

### 1.4 PR description 模板
```markdown
## Summary
新增文件正文全文检索能力：抽取 type=8 消息里的 PDF/Office/纯文本内容 → 写 `payload.file.content` → reader 搜关键词命中文件正文（reader 侧改动在独立 PR：octo-server#XXX）。

背景 + 方案参见 `docs/file-content-indexing-feasibility.md`（v2）；工具选型见 `docs/file-extractor-tool-comparison.md`；本 PR 实施拆分与设计决策见 `docs/file-content-indexing-implementation.md`。

## Commits（5 步串行，建议按顺序 review）
1. **IDX-1** `feat(mapping)`: OS mapping v1.12 加 payload.file.content + contentMeta
2. **IDX-2** `feat(esindex)`: FilePayload 加 Content + ContentMeta 字段
3. **IDX-3** `feat(file-extractor)`: 骨架 (Kafka consumer + type=8 filter + DLQ skeleton)
4. **IDX-4** `feat(file-extractor)`: CDN download + Tika HTTP client + OS partial update
5. **IDX-5** `feat(backfill)`: cmd/file-content-backfill one-shot job

## Review 建议
- 先看 IDX-1 mapping.json diff（可视化最小改动）
- IDX-4 是最重的 commit，关注错误分类与 DLQ reason 触发点
- IDX-5 backfill 关注限速与 checkpoint 语义

## 回滚方案
详见 `docs/file-content-indexing-implementation.md §5`。**mapping PUT 不可逆**（新字段只能靠 reindex 消除），故先 PR 合并但不执行 mapping PUT，等 test 部署时人工上（本 PR 不涉及部署）。

## 测试
- 单元测试全绿：`go test ./...`
- 手工端到端联调：在本地 docker 起 kafka + opensearch + tika，跑 file-extractor 抽取一份 PDF，验证 `payload.file.content` 落 OS

## Non-goals（不在本 PR）
- octo-server reader 改动（独立仓独立 PR）
- codex 部署 manifests 改动（OPS-1/OPS-2 独立任务）
- 现役 OS index mapping PUT（等 test 部署前 admin 人工触发）
- octo-web 前端改动（观察期）
```

---

## §2 每个 commit 的详细 diff 计划

### IDX-1 · `feat(mapping): add payload.file.content + contentMeta to OS mapping (v1.12)`

**只改 JSON + Markdown，不改代码，不加测试**（mapping.json 由启动断言 mapping_compat.go 隐式测试，IDX-2 补测）

#### 改动文件

**修改**：`internal/esindex/mapping/octo-message.json`
- `payload.properties.file.properties` 下新增两个字段：
  ```json
  "content": {
    "type": "text",
    "analyzer": "ik_max_word",
    "search_analyzer": "ik_smart"
  },
  "contentMeta": {
    "type": "object",
    "properties": {
      "extractedAt": { "type": "date", "format": "epoch_second" },
      "extractor":   { "type": "keyword", "ignore_above": 32 },
      "truncated":   { "type": "boolean" },
      "extractMs":   { "type": "long" }
    }
  }
  ```
- `settings.index` 同层新增：
  ```json
  "_source": { "excludes": ["payload.file.content"] }
  ```
  ⚠️ 注意 OS `_source` config 位置是 `mappings._source`（不是 settings 下），需要放到 `"mappings": { "_source": {...}, "dynamic": "strict", "properties": {...} }` 位置。IDX-1 里对照 OS 官方 mapping schema 确认落点。

**修改**：`internal/esindex/mapping/README.md`
- 顶部加 v1.12 changelog：
  ```markdown
  ## v1.12 文件正文全文检索（file content indexing）
  
  v1.12 在 `payload.file` 下新增两个字段，配套 file-extractor 抽取的文件正文写入：
  
  - `payload.file.content` (text, IK 分词) — Tika 抽出的文件正文纯文本。用 IK 双分析器
    保持与 `payload.text.content` 同口径。`_source.excludes` 排除该字段，节省 _source
    体积；搜索仍可命中并 highlight（走倒排不走 _source）。
  - `payload.file.contentMeta.{extractedAt, extractor, truncated, extractMs}` (object) —
    抽取元信息，便于运维观察抽取延迟/覆盖率/截断率。
  
  写入契约：由独立 `cmd/file-extractor` 服务通过 OS `_update` partial update 只更新
  这两个字段，不动主 doc 其他字段（父 doc 由 es-indexer 主流程写入）。
  
  详见 `docs/file-content-indexing-feasibility.md` v2 §7。
  ```

#### 单测覆盖点
无（本 commit 无代码）。IDX-2 里 `mapping_compat_test.go` 会通过 fail-closed 断言隐式验证新字段。

#### 可能的坑
- **`_source.excludes` 位置**：OS mapping schema 里 `_source` 是 `mappings` 的直接子字段，不在 `settings.index` 下。旧代码里没有 `_source` 段，本次是首次引入 — 需查 OpenSearch 3.x mapping 官方文档确认精确 JSON 层级
- **IK 分析器可用性**：`ik_max_word / ik_smart` 依赖 analysis-ik 插件已装（README v1.11 已说明由 octo-deployment 协调）— 现役 OS 已装（`payload.text.content` 早在用），无新增依赖
- **`dynamic: strict` 与新字段冲突风险**：mapping.json 加了新字段后，代码写入含 `content` 的 doc 前 live mapping 必须已 PUT — 主人决策 #2 明确"改 mapping.json 但不 PUT 到现役 index"，本 PR 合并后到 test 部署前的窗口，如果误把新代码上到 test 但没 PUT mapping，会 4xx 塌 → **IDX-1 commit message 里加显式 warning**：merge 后先 PUT mapping 再上 file-extractor

**开发+测试估时**: **20 分钟**（改 JSON + Markdown + 目视 diff）

---

### IDX-2 · `feat(esindex): add Content/ContentMeta fields to FilePayload`

#### 改动文件

**修改**：`internal/esindex/doc.go`
- `FilePayload` struct 加两个字段（第 125-131 行）：
  ```go
  type FilePayload struct {
      URL         string           `json:"url,omitempty"`
      Name        string           `json:"name,omitempty"`
      Caption     string           `json:"caption,omitempty"`
      Size        int64            `json:"size,omitempty"`
      Extension   string           `json:"extension,omitempty"`
      Content     string           `json:"content,omitempty"`      // v1.12 新增
      ContentMeta *FileContentMeta `json:"contentMeta,omitempty"`  // v1.12 新增
  }
  ```
- 新增 struct `FileContentMeta`：
  ```go
  // FileContentMeta 是 file-extractor 抽取元信息（v1.12）。指针类型 + omitempty 保证
  // 未抽取的 file doc 该子对象缺席，与 mapping "contentMeta" object 默认无子字段兼容。
  type FileContentMeta struct {
      ExtractedAt int64  `json:"extractedAt,omitempty"` // 抽取时刻 (epoch seconds)
      Extractor   string `json:"extractor,omitempty"`   // "tika/3.3.0" 之类
      Truncated   bool   `json:"truncated,omitempty"`   // content 是否被截到 256KB
      ExtractMs   int64  `json:"extractMs,omitempty"`   // Tika 抽取耗时 (毫秒)
  }
  ```

**修改**：`internal/esindex/mapping_compat.go`
- `requiredMappingFieldPaths` 加两条新字段路径（第 29-40 行）：
  ```go
  var requiredMappingFieldPaths = []string{
      // ... 现有字段保留
      // v1.12：文件正文全文检索
      "payload.file.content",
      "payload.file.contentMeta.extractedAt",  // 采样一个子字段代表整个 contentMeta object
  }
  ```
- 加注释说明 v1.12 增量含义（承接 v1.10/v1.11 注释风格）

#### 新增文件
无。

#### 单测覆盖点

**新增/扩展**：`internal/esindex/doc_test.go`
- `TestFilePayload_ContentSerialization` — 构造 FilePayload{Content: "hello 你好", ContentMeta: {...}} → json.Marshal → 验证 JSON 字段名/嵌套/omitempty 语义
- `TestFilePayload_EmptyContentOmitted` — Content="" + ContentMeta=nil → JSON 无 content/contentMeta 键（omitempty 生效）
- `TestFileContentMeta_ExtractedAtSerialization` — 单独覆盖 ContentMeta 各字段

**新增/扩展**：`internal/esindex/mapping_compat_test.go`
- `TestRequiredMappingFieldPaths_IncludesV112Fields` — 验证 requiredMappingFieldPaths 包含 `payload.file.content` + `payload.file.contentMeta.extractedAt`
- `TestAssertLiveMappingCompatible_FailsWithoutContent` — 构造缺 content 字段的假 mapping → AssertLiveMappingCompatible 应返 error（fail-closed 语义验证）

#### 可能的坑
- **buildraw.go 不用改**：现有 buildraw.go 从 RawPayload 投 FilePayload 不会填 Content（Kafka 消息里没有），Content 只由 file-extractor 后写。omitempty 保证不影响现有 doc 结构
- **backfill 路径不用改**：backfill 走 buildraw 同路径，同样不填 Content
- **BulkDocs 不用改**：Doc.Payload.File.Content 有值时自动序列化进 _bulk body，无需修改 encodeBulkBody
- **mapping_compat_test 假 mapping 构造**：需构造 nested map[string]any 模拟 GET _mapping 响应，测试文件里可能已有 helper 复用

**开发+测试估时**: **40 分钟**（struct 加字段 20min + 单测 20min）

---

### IDX-3 · `feat(file-extractor): scaffold cmd/file-extractor with Kafka consumer + type=8 filter + DLQ skeleton`

#### 新增文件

**`cmd/file-extractor/main.go`** (~130 行，仿 `cmd/es-indexer/main.go` 骨架)
```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "github.com/Mininglamp-OSS/octo-search-indexer/internal/fileextract"
)

func main() { /* log 初始化 + 信号处理 + run(ctx) */ }

func run(ctx context.Context) error {
    cfg, enabled := loadConfig()
    if !enabled {
        log.Printf("file-extractor: FILE_EXTRACTOR_ENABLED not true (or config missing); idling")
        <-ctx.Done()
        return nil
    }
    svc, err := fileextract.NewService(cfg)
    if err != nil { return err }
    defer svc.Close()
    return svc.Run(ctx)
}

func loadConfig() (fileextract.ServiceConfig, bool) {
    cfg := fileextract.ServiceConfig{
        Brokers:       splitCSV(os.Getenv("KAFKA_BROKERS")),
        Topic:         envOr("KAFKA_TOPIC", "octo.message.v1"),
        DLQTopic:      envOr("KAFKA_DLQ_TOPIC", "octo.message.v1.file-extract.dlq"),
        GroupID:       envOr("KAFKA_GROUP_ID", "file-extractor"),
        BatchSize:     envInt("EXTRACTOR_BATCH_SIZE", 50),
        // Tika + OS + Download 相关配置 IDX-4 补，本 commit 只装骨架
        ESAddresses:   splitCSV(os.Getenv("ES_ADDRESSES")),
        ESIndex:       envOr("ES_INDEX", "octo-message"),
        ESUsername:    os.Getenv("ES_USERNAME"),
        ESPassword:    os.Getenv("ES_PASSWORD"),
    }
    enabled := strings.EqualFold(os.Getenv("FILE_EXTRACTOR_ENABLED"), "true") &&
        len(cfg.Brokers) > 0 && len(cfg.ESAddresses) > 0
    return cfg, enabled
}

// envOr / envInt / splitCSV: 复用 es-indexer/main.go 的实现（本 commit 里复制，
// 阶段 4 加公用 pkg 时再抽）
```

**`internal/fileextract/config.go`** (~50 行)
```go
package fileextract

import "time"

type ServiceConfig struct {
    Brokers       []string
    Topic         string
    DLQTopic      string
    GroupID       string
    BatchSize     int

    // IDX-4 补充：TikaURL / DownloadTimeout / ExtractTimeout / MaxFileSize / MaxContentBytes ...
    TikaURL         string  // http://localhost:9998 (sidecar)
    DownloadTimeout time.Duration
    ExtractTimeout  time.Duration
    MaxFileSize     int64   // 20MB cutoff
    MaxContentBytes int     // 256KB 截断
    HTTPRetries     int     // 3

    ESAddresses   []string
    ESIndex       string
    ESUsername    string
    ESPassword    string
}
```

**`internal/fileextract/service.go`** (~100 行，仿 `internal/consumer/service.go`)
```go
package fileextract

import (
    "context"
    "fmt"
    "log"
)

type Service struct {
    proc    *Processor
    source  *kafkaSource
    dlqSink *kafkaDLQSink
    // writer 在 IDX-4 引入
}

func NewService(cfg ServiceConfig) (*Service, error) {
    source, err := newKafkaSource(...)
    // ...
    dlqSink, err := newKafkaDLQSink(...)
    // ...
    proc := NewProcessor(source, dlqSink, cfg)
    return &Service{...}, nil
}

func (s *Service) Run(ctx context.Context) error { return s.proc.Run(ctx) }
func (s *Service) Close() error { /* 关闭 source + dlqSink */ }
```

**`internal/fileextract/consumer.go`** (~150 行，仿 `internal/consumer/consumer.go` 简化版)
```go
package fileextract

// Processor：拉批 → filter type=8 → 抽取 (IDX-4 stub) → OS update (IDX-4 stub) → DLQ 路由 → commit
type Processor struct {
    source  messageSource
    dlqSink dlqSink
    cfg     ServiceConfig
}

func (p *Processor) Run(ctx context.Context) error { /* 循环 fetchBatch + processBatch */ }

func (p *Processor) processBatch(ctx context.Context, batch []fetchedMessage) error {
    for _, m := range batch {
        msg, err := searchmsg.UnmarshalMessage(m.value)
        if err != nil {
            // 契约解析失败 → DLQ reason=parse_error
            p.dlqSink.WriteDLQ(ctx, m.key, buildDLQEnvelope(m, "parse_error", err.Error()))
            continue
        }
        contentType, ok := extractContentType(msg.RawPayload)  // 复用 buildraw.go decodeObjectUseNumber
        if !ok || contentType != payloadTypeFile {  // != 8
            continue  // 非文件消息，跳过（IDX-3 只到这一步，IDX-4 接抽取）
        }
        // IDX-3: 打日志占位，不做真正抽取
        log.Printf("file-extractor: skip type=8 (extractor stub, waiting IDX-4): messageId=%s", msg.MessageID)
    }
    return p.source.CommitBatch(ctx, batch)
}
```

**`internal/fileextract/kafka.go`** (~120 行)
- 复制 `internal/consumer/kafka.go` 的 `kafkaSource` + `kafkaDLQSink` 实现（同 C4 提交语义：CommitInterval=0 + 只用 FetchMessage）
- **不 import consumer 包**（避免循环依赖 + 保持 fileextract 独立性）

**`internal/fileextract/dlq.go`** (~180 行，简化版 `internal/consumer/dlq.go`)
- `dlqRecord` struct，含字段：`Reason / MessageID / Topic / Partition / Offset / URL / DetailErr / SpilledAt`
- `DLQReason` 枚举常量（本 commit 定义齐 8 种，触发点在 IDX-4 补）：
  ```go
  const (
      ReasonParseError    = "parse_error"       // Kafka 消息解不出 searchmsg
      ReasonOversize      = "oversize"          // 文件 > MaxFileSize
      ReasonBlacklistExt  = "blacklist_ext"     // 扩展名黑名单
      ReasonDownloadFail  = "download_failed"   // HTTP GET 失败（重试耗尽）
      ReasonExtractTimeout = "extract_timeout"  // Tika 抽取超时
      ReasonEncrypted     = "encrypted"         // Tika EncryptedDocumentException
      ReasonEmptyExtract  = "empty_extract"     // 抽出空串
      ReasonExtractError  = "extract_error"     // 其他 Tika 异常
  )
  ```
- 带 base64-膨胀 aware 的 value 截断阈值（复用 consumer/dlq.go 常量 700_000）

**`internal/fileextract/metrics.go`** (~40 行)
- 骨架 counter/timer 定义（阶段暂用 stdlib log 打，接 Prometheus 在阶段 7 独立任务）：
  - `processedTotal / skippedNonFile / dlqTotal[reason] / extractDurationMs`

#### 单测覆盖点

**新增**：`cmd/file-extractor/main_test.go`
- `TestLoadConfig_Disabled` — 未设 FILE_EXTRACTOR_ENABLED → enabled=false
- `TestLoadConfig_MissingBrokers` — Enabled=true 但缺 KAFKA_BROKERS → enabled=false
- `TestLoadConfig_Defaults` — 默认 topic/group/batch 等值

**新增**：`internal/fileextract/consumer_test.go`
- `TestProcessBatch_SkipNonFileMessages` — 构造 type=1/2/5 消息 → 应跳过不写 DLQ
- `TestProcessBatch_UnmarshalError_ToDLQ` — 塞坏 JSON → 触发 parse_error DLQ
- `TestProcessBatch_TypeFileGoesToExtractor` — type=8 消息（IDX-3 stub 版本）应命中日志占位分支

**新增**：`internal/fileextract/dlq_test.go`
- `TestDLQRecord_SerializationRoundtrip` — 构造 → marshal → unmarshal → 字段对齐
- `TestDLQReason_Constants` — 8 种 reason 常量存在
- `TestDLQValueTruncation` — 超 700KB Value 应被截断 + PayloadTruncated=true

#### 可能的坑
- **kafka-go rebalance 处理**：多副本 file-extractor 部署时，同 groupID 的实例会分 partition。file-extractor 拉了消息但抽取中的 rebalance 会导致该消息被另一 pod 重取 — **靠 OS partial update 幂等兜底**（同 doc 覆盖同字段，无副作用）。IDX-3 里不用特殊处理，IDX-4 补充说明
- **type filter 早剪**：processBatch 里 non-file 消息立即 skip 是关键性能优化 — 非 type=8 消息占 99.3%，早剪能让 file-extractor 吞吐≈es-indexer 吞吐
- **contentType 提取**：复用 buildraw.go 的 `decodeObjectUseNumber` + `extractType` — IDX-3 可以 import esindex 包直接调（消费者读 payload，是合理依赖）
- **kafka 分组冲突**：`file-extractor` 是新 groupID，不抢 es-indexer 位点，两个消费者独立推进

**开发+测试估时**: **2 小时**（4 个新 .go 文件骨架 + 3 个 test 文件；跟 consumer 包相似结构可 copy-adapt，主要工时在契约对齐 + test setup）

---

### IDX-4 · `feat(file-extractor): implement CDN download + Tika HTTP client + OS partial update`

#### 新增文件

**`internal/fileextract/download.go`** (~100 行)
```go
package fileextract

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "time"
)

// downloadClient 从 CDN URL 拉文件 bytes。带超时 + 指数退避重试 + size cutoff。
type downloadClient struct {
    hc          *http.Client
    maxSize     int64
    retries     int
    retryBackoff time.Duration
}

func newDownloadClient(cfg ServiceConfig) *downloadClient { /* ... */ }

// Fetch: GET url → 读 body → 返 (bytes, contentType, error)
// 错误分类：
//   - HTTP 5xx / net.OpError / DNS 失败 → transient，触发重试
//   - HTTP 4xx（非 429）→ permanent（DLQ reason=download_failed）
//   - Content-Length > maxSize → permanent（DLQ reason=oversize）
//   - 3 次重试耗尽 → permanent
func (d *downloadClient) Fetch(ctx context.Context, url string) ([]byte, string, error) {
    for attempt := 0; attempt <= d.retries; attempt++ {
        body, ct, err := d.tryFetch(ctx, url)
        if err == nil { return body, ct, nil }
        if !isTransient(err) { return nil, "", err }
        wait := d.retryBackoff * (1 << attempt) // 1s / 4s / 16s
        select { case <-time.After(wait): case <-ctx.Done(): return nil, "", ctx.Err() }
    }
    return nil, "", fmt.Errorf("download exhausted retries: %s", url)
}
```

**`internal/fileextract/tika.go`** (~120 行)
```go
package fileextract

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "net/http"
    "strings"
    "time"
)

// tikaClient 调 Tika Server 的 PUT /tika 端点，得纯文本。
type tikaClient struct {
    hc      *http.Client
    baseURL string  // http://localhost:9998
    timeout time.Duration
    maxContentBytes int  // 256KB 截断
}

func newTikaClient(cfg ServiceConfig) *tikaClient { /* http.Client{ Timeout: cfg.ExtractTimeout } */ }

// Extract 上传 file bytes，返回抽出的纯文本 + truncated 标记 + error
// 错误分类（关键设计）：
//   - HTTP 500 body 含 "EncryptedDocumentException" → 特殊错误 errEncrypted
//   - HTTP 500 其他 → errExtractGeneric
//   - HTTP 422 (unsupported media type) → errExtractGeneric（Tika 明确不支持）
//   - context.DeadlineExceeded → errExtractTimeout
//   - 抽出空串 → 上层触发 empty_extract（这里返 ("", false, nil) 不算错）
//   - 抽出 > maxContentBytes → 截断 + truncated=true
func (t *tikaClient) Extract(ctx context.Context, fileBytes []byte, filename string) (content string, truncated bool, err error)
```

**`internal/fileextract/oswriter.go`** (~150 行)
```go
package fileextract

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "github.com/opensearch-project/opensearch-go/v3"
    "github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// osWriter 只做一件事：给指定 messageId 的 doc 做 partial update，
// 仅写 payload.file.content + payload.file.contentMeta。
type osWriter struct {
    client *opensearch.Client
    index  string
}

func newOSWriter(cfg ServiceConfig) (*osWriter, error) { /* opensearch.NewClient(...) */ }

// UpdateContent 用 _update API（partial merge doc），只更新 file.content + contentMeta。
// 参数 retryOnConflict=3 缓解与主 doc 首次写入的乐观锁冲突。
func (w *osWriter) UpdateContent(ctx context.Context, messageID string, content string, meta esindex.FileContentMeta) error {
    body := map[string]any{
        "doc": map[string]any{
            "payload": map[string]any{
                "file": map[string]any{
                    "content":     content,
                    "contentMeta": meta,
                },
            },
        },
        "doc_as_upsert": false,  // 关键：doc 不存在时不 upsert（避免造孤儿子文档；等主 doc 先落）
    }
    buf, _ := json.Marshal(body)
    req := opensearchapi.UpdateReq{
        Index:      w.index,
        DocumentID: messageID,
        Body:       bytes.NewReader(buf),
        Params: opensearchapi.UpdateParams{RetryOnConflict: ptrInt(3)},
    }
    resp, err := w.client.Update(ctx, req)
    if err != nil { return err }
    if resp.StatusCode == 404 {
        // 主 doc 还没落到 OS（es-indexer 还没消费到这条）— 让上层等下一轮 rebalance/重试
        return errDocNotYet
    }
    // 其他 status 语义分类：2xx=OK，4xx=permanent，5xx=transient
    return classifyOSStatus(resp.StatusCode)
}
```

**修改**：`internal/fileextract/service.go` — 加 downloadClient / tikaClient / osWriter 装配

**修改**：`internal/fileextract/consumer.go::processBatch` — 把 IDX-3 stub 换成真实抽取流程：
```go
if contentType == payloadTypeFile {
    payload := extractFilePayload(msg.RawPayload)  // 从 raw 取 url/name/ext/size
    if !isExtractable(payload.Extension) {
        // 黑名单扩展名，跳过（不写 DLQ，这是设计正常路径）
        continue
    }
    if payload.Size > cfg.MaxFileSize {
        p.writeDLQ(m, ReasonOversize, ...)
        continue
    }
    // 1. download
    bytes, _, err := p.download.Fetch(ctx, payload.URL)
    if err != nil { p.writeDLQ(m, ReasonDownloadFail, err); continue }
    // 2. extract
    start := time.Now()
    content, truncated, err := p.tika.Extract(ctx, bytes, payload.Name)
    extractMs := time.Since(start).Milliseconds()
    if err != nil {
        switch {
        case errors.Is(err, errEncrypted): p.writeDLQ(m, ReasonEncrypted, err)
        case errors.Is(err, errExtractTimeout): p.writeDLQ(m, ReasonExtractTimeout, err)
        default: p.writeDLQ(m, ReasonExtractError, err)
        }
        continue
    }
    if content == "" {
        p.writeDLQ(m, ReasonEmptyExtract, nil)
        continue
    }
    // 3. os partial update
    meta := esindex.FileContentMeta{
        ExtractedAt: time.Now().Unix(),
        Extractor:   "tika/3.3.0",
        Truncated:   truncated,
        ExtractMs:   extractMs,
    }
    if err := p.oswriter.UpdateContent(ctx, msg.MessageID, content, meta); err != nil {
        if errors.Is(err, errDocNotYet) {
            // 主 doc 未落 → 本 message 暂不 commit 位点，让 kafka 下轮再取
            // 简化：这里直接 return err（外层 backoff 后重跑整批）
            return err
        }
        // 其他 OS 错误 → transient 重试或 DLQ
    }
}
```

#### 单测覆盖点

**新增**：`internal/fileextract/download_test.go`
- `TestFetch_Success` — httptest.Server 返 200 → bytes 正确
- `TestFetch_5xxRetryThenSuccess` — 前两次 500，第三次 200 → 抽取成功
- `TestFetch_5xxRetryExhausted` — 三次都 500 → DLQ 触发 err
- `TestFetch_ContentLengthOversize` — Content-Length > MaxFileSize → 立即 oversize
- `TestFetch_ContextCancelled` — ctx 取消 → 立即返 ctx.Err

**新增**：`internal/fileextract/tika_test.go`
- `TestExtract_Success` — httptest 模拟 Tika 返纯文本 → content OK truncated=false
- `TestExtract_ContentTruncatedAt256KB` — 返 300KB → 截到 256KB + truncated=true
- `TestExtract_EncryptedDocument500Body` — httptest 返 500 body 含 "EncryptedDocumentException" → 返 errEncrypted
- `TestExtract_TimeoutExceeded` — httptest sleep 40s，client timeout 30s → 返 errExtractTimeout
- `TestExtract_UnsupportedMedia422` — 返 422 → 返 errExtractGeneric
- `TestExtract_EmptyBody` — 返 200 空 body → content="" err=nil (让上层判)

**新增**：`internal/fileextract/oswriter_test.go`
- `TestUpdateContent_Success` — httptest OS 返 200 → nil err
- `TestUpdateContent_404_DocNotYet` — OS 返 404 → 返 errDocNotYet
- `TestUpdateContent_409_VersionConflict` — OS 返 409 → 走 retry_on_conflict 由 OS 内部处理，本地不重试；返 nil
- `TestUpdateContent_RequestBodyShape` — 拦截请求 body 断言 `doc.payload.file.{content,contentMeta}` 字段正确嵌套 + `doc_as_upsert=false`

**扩展**：`internal/fileextract/consumer_test.go`
- `TestProcessBatch_FileMessage_EndToEnd` — 用 mock download/tika/oswriter 组一遍 e2e
- `TestProcessBatch_Encrypted_ToDLQ` — 模拟 Tika 抛 encrypted → DLQ reason=encrypted
- `TestProcessBatch_Oversize_SkipDownload` — 大于 MaxFileSize → 不下载直接 DLQ oversize
- `TestProcessBatch_BlacklistExt_SkipCleanly` — .mp4 消息 → 跳过不写 DLQ 不 log 错

#### 可能的坑
- **Tika HTTP 500 body 判 EncryptedDocumentException**：Tika Server 返错时 body 是 Java stack trace 纯文本；判断靠字符串 contains "EncryptedDocumentException"。**风险**：Tika 版本升级 body 格式变化导致判断失效 — 加 unit test 用真实 Tika body 采样字符串锁死这个契约，升 Tika 主版本时同步 review
- **OS partial update 时序**：file-extractor 消费同一 topic 但独立 group，es-indexer 可能还没消费到主 doc → OS `_update` 返 404。**降级策略**：`errDocNotYet` 触发本批重试（Kafka rebalance 会重新取），最终追平 es-indexer 后成功。**风险**：如果 file-extractor 消费速度长期快于 es-indexer，大量重试；缓解：file-extractor 开工前 sleep 5s 等 es-indexer 抢先跑（但更好方案是加个"预检查" query 或直接接受重试代价）
- **retry_on_conflict=3**：OS 内部处理 version 冲突，通常够。如果 backfill Job 也在同时写同一 doc，可能超过 3 次；本 PR 里 file-extractor 和 backfill 时序错开（backfill 在 file-extractor 稳定 24h 后跑），不会撞车
- **HTTP client 复用**：单例 `http.Client{ Transport: ..., Timeout: ... }`（stdlib 推荐做法）；不用 `google/go-tika` 库（多一层依赖 + wrapper 薄）
- **byte 拷贝内存放大**：Tika HTTP PUT 需把整份文件放请求 body，20MB × 并发 10 = 200MB 峰值内存 — Deploy memory limit 至少 512MB 起
- **黑名单/白名单扩展名判断**：filename 用 `path.Ext(name)` 取；同时校对 payload.file.extension 字段（Kafka 消息里有）— 优先用 extension 字段，兜底 filename 后缀
- **contentType 不识别的 msg**：processBatch 里 `extractContentType` 返 (0, false) 视作跳过，不进 DLQ（非文件消息本身合法）

**开发+测试估时**: **5-6 小时**（3 个新 pkg 文件 + 3 个 test 文件 + consumer.go 改造 + service.go 装配；这是最重 commit）

---

### IDX-5 · `feat(backfill): add cmd/file-content-backfill one-shot job`

#### 新增文件

**`cmd/file-content-backfill/main.go`** (~200 行，仿 `cmd/backfill/main.go` 骨架但**源改成 OS scroll**)
```go
package main

import (
    "context"
    "flag"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
    "github.com/Mininglamp-OSS/octo-search-indexer/internal/fileextract"
    fbf "github.com/Mininglamp-OSS/octo-search-indexer/internal/filebackfill"
)

func main() { /* flag 解析 + 信号处理 + run() */ }

func run() error {
    var (
        esAddrs   = flag.String("es", envOr("BACKFILL_ES", "http://localhost:9200"), "OS addresses")
        esIndex   = flag.String("es-index", envOr("BACKFILL_ES_INDEX", "octo-message"), "OS index (alias)")
        // ... esUser / esPass / tikaURL 
        rate      = flag.Float64("rate", 50, "extraction rate limit (docs/s)")
        scrollSize = flag.Int("scroll-size", 500, "OS scroll batch size")
        checkpoint = flag.String("checkpoint", "", "checkpoint file (empty=in-memory)")
        timeout   = flag.Duration("timeout", 2*time.Hour, "overall timeout")
    )
    flag.Parse()

    // 装配：downloadClient + tikaClient + osWriter 复用 internal/fileextract/*
    // 加 osReader（新增，只做 scroll 查询）+ rateLimiter（复用 internal/backfill/ratelimit.go 或复制）
    runner := fbf.NewRunner(fbf.Config{
        Source: osReader,       // scroll query type=8 且 content 缺失
        Extract: extractClient, // 复用 fileextract.Extractor
        Rate: rate,
    })
    return runner.Run(ctx)
}
```

**`internal/filebackfill/source.go`** (~120 行)
```go
package filebackfill

// osScrollSource 用 OS scroll API 遍历 type=8 且 payload.file.content 不存在的 doc
type osScrollSource struct {
    client *opensearch.Client
    index  string
    size   int
    scrollTTL time.Duration
}

// Next 返回下一批 {messageID, filePayload}，直到没有为止返 io.EOF
// Query DSL:
//   {
//     "query": {
//       "bool": {
//         "filter": [{"term":{"payload.type":8}}],
//         "must_not": [{"exists":{"field":"payload.file.content"}}]
//       }
//     },
//     "_source": ["messageId","payload.file"]
//   }
func (s *osScrollSource) Next(ctx context.Context) ([]sourceDoc, error) { /* ... */ }
```

**`internal/filebackfill/runner.go`** (~150 行)
```go
package filebackfill

// Runner 串起：scroll source → 限速 → 复用 fileextract.processOne → OS partial update → 进度日志
type Runner struct {
    source    *osScrollSource
    extractor *fileextract.Extractor   // 抽出 IDX-4 processBatch 逻辑成可复用 Extractor
    limiter   *rateLimiter
    // ...
}

func (r *Runner) Run(ctx context.Context) error { /* 循环 Next + 逐条 extract + 打进度 */ }
```

**`internal/filebackfill/ratelimit.go`** — 复用 internal/backfill/ratelimit.go 实现（直接 copy 或 refactor 抽公用 pkg，本 PR 里 **直接 copy** 保持修改边界最小）

**`internal/filebackfill/checkpoint.go`** — 复用 internal/backfill/checkpoint.go 模式（scroll_id 持久化到文件，恢复时从上次 scroll_id 续跑；简化：MVP 阶段可以先不做 checkpoint，123K 文件 40 分钟能跑完，即便中断重跑也只多消耗一次抽取时间；本 commit 加简化 in-memory 版，checkpoint 文件版留待后续 PR 增强）

#### 单测覆盖点

**新增**：`internal/filebackfill/source_test.go`
- `TestOSScrollSource_QueryShape` — 拦截 OS 请求 body 断言 filter/must_not/exists 语义
- `TestOSScrollSource_Pagination` — mock OS 返 3 页 → 全部消费
- `TestOSScrollSource_EmptyResult_ReturnsEOF` — 空结果 → io.EOF

**新增**：`internal/filebackfill/runner_test.go`
- `TestRunner_HappyPath` — 3 条 doc → 全部抽取成功
- `TestRunner_RateLimitHonored` — 限 10 docs/s，100 条 → 应耗 ~10s（tick 计时验证）
- `TestRunner_PartialFailure` — 3 条里 1 条 encrypted → 2 成功 1 DLQ，Job 不整体失败
- `TestRunner_ContextCancelled` — ctx.Cancel → 立即优雅退出

**新增**：`cmd/file-content-backfill/main_test.go`
- `TestRun_FlagsRequired` — 缺 esAddrs → 返 error
- `TestRun_Defaults` — 默认 rate/scrollSize/timeout 值

#### 可能的坑
- **共用 fileextract.processOne 需要 refactor**：IDX-4 里 `processBatch` 是 consumer 内嵌逻辑，为了 backfill 复用需要抽成独立可测函数（比如 `fileextract.Extractor.Extract(ctx, msgID, filePayload) error`）。IDX-4 里就应该考虑复用性，IDX-5 里如果发现耦合太紧要小改 IDX-4（review 时可能反馈）
- **OS scroll 长时间开着**：默认 scroll TTL 5min；123K 文件 × 200ms/抽取 = 40min，需要在每批之间刷新 scroll TTL 或改用 search_after（更现代）。**推荐用 search_after + PIT (point-in-time)** 而非老 scroll API（OpenSearch 3.x 建议）
- **限速与并发的关系**：限速 50 RPS 是单实例；一次性 Job 单副本跑，不用担心多副本互相冲突
- **backfill 时机**：主 doc 一定已存在（backfill 是回填历史，不是新数据），所以 UpdateContent 不会 404
- **失败重跑**：如果 Job 中途挂了，重跑时 scroll query `must_not exists payload.file.content` 会自动跳过已抽取的 doc（幂等自然）— 不需要复杂 checkpoint 逻辑
- **一次性 K8s Job 部署**：本 commit 只出 Go 代码，YAML 在 codex 部署仓 OPS-2 独立任务

**开发+测试估时**: **3-4 小时**（IDX-4 已建好复用基础的话工时集中在 scroll source + runner；如果 IDX-4 没抽 Extractor 复用点，这里需要 refactor 加 1 小时）

---

## §3 关键设计决策

### 3.1 Tika HTTP client 用哪个 Go 库？
**决策**：**stdlib `net/http` 直调**。

**理由**：
- Tika HTTP API 极简（PUT /tika + header + body），wrap 一层没意义
- `google/go-tika` 已 3+ 年无更新（本地未验证但社区活跃度低），依赖风险高于收益
- stdlib http.Client 支持 timeout / context / connection reuse 齐全，够用

**代码位置**：`internal/fileextract/tika.go`

### 3.2 OS partial update 用 `_update` 还是 `_update_by_query`？
**决策**：**用 `_update`（by docID）**。

**理由**：
- 每条 Kafka 消息对应一个明确的 messageID → `_update/<id>` 精准命中
- `_update_by_query` 是批量语义（一次改 N 条匹配 query 的 doc），不适合单条 update
- `retry_on_conflict=3` 参数处理与主 doc 首次写入的乐观锁冲突（OS 内部 retry，本地无需重试逻辑）
- `doc_as_upsert=false` 避免造孤儿子文档（主 doc 未落时报 404 而不是 upsert 出无父的 content-only doc）

**代码位置**：`internal/fileextract/oswriter.go::UpdateContent`

### 3.3 Kafka 消息处理并发模型
**决策**：**每 partition 单 goroutine（kafka-go consumer group 默认）+ 批内串行处理**。

**理由**：
- 现有 `internal/consumer` 就是这套模型，保持一致低风险
- Tika 抽取是 IO/CPU 混合，批内并行的收益要跟 Tika sidecar 并发能力匹配 — Tika 默认 thread pool = CPU × 2；单 pod 里 file-extractor 单 goroutine 串行 = Tika 一次一个请求，简单且 Tika 侧无争抢
- 未来如果吞吐不够，通过**扩 file-extractor Deploy 副本数**扩容（Kafka 自动 rebalance），而不是加批内并发
- 简化排障：一条消息处理一次抽取一次 update，日志一目了然

**替代方案**（不采用）：批内 worker pool 并行抽取 N 条。**否决理由**：加复杂度但对小规模消息（几百 QPS）无收益

### 3.4 Backfill Job 通过 OS scroll 还是 Kafka 重放？
**决策**：**OS scroll (推荐 search_after + PIT) 遍历，不走 Kafka**。

**理由**：
- Kafka 消息保留期有限（默认 7 天），历史 123K 消息大多已过期
- OS scroll 直接查 `type=8 且 content 缺失` 精准命中回填目标，无需过滤
- 重跑幂等自动（`must_not exists` 自动跳过已抽取的 doc）
- 走 Kafka 会污染 file-extractor consumer group 位点（一次性 Job 混进日常 consumer 里不干净）

**代码位置**：`internal/filebackfill/source.go`

### 3.5 file-extractor 与 backfill 复用抽取逻辑
**决策**：IDX-4 里把抽取核心逻辑抽成 `fileextract.Extractor` 独立 struct（`Extract(ctx, msgID, filePayload) error`），consumer.go 和 filebackfill/runner.go 都调它。

**理由**：DRY + 单测更集中；避免 IDX-5 里重复 IDX-4 的 download/tika/oswriter 装配代码

---

## §4 单测覆盖清单（汇总）

| Commit | 测试文件 | 关键 case 数 |
|---|---|---|
| IDX-1 | 无（改 JSON/Markdown） | 0 |
| IDX-2 | `doc_test.go`（扩展）+ `mapping_compat_test.go`（扩展） | 5 |
| IDX-3 | `main_test.go`（新）+ `consumer_test.go`（新）+ `dlq_test.go`（新） | 8 |
| IDX-4 | `download_test.go`（新）+ `tika_test.go`（新）+ `oswriter_test.go`（新）+ `consumer_test.go`（扩） | 20 |
| IDX-5 | `source_test.go`（新）+ `runner_test.go`（新）+ `main_test.go`（新） | 10 |
| **合计** | 11 个测试文件 | **~43 case** |

**跑测命令**：
```bash
cd ~/Project/Mininglamp-OSS/octo-search-indexer
go test ./...
go test -race ./internal/fileextract/... ./internal/filebackfill/...  # 竞态
go test -cover ./internal/fileextract/... ./internal/filebackfill/... # 覆盖率
```

**覆盖率目标**：新增包 line coverage ≥ 80%（现有 internal/consumer 是 ~85%，对齐）

---

## §5 回滚方案

### 5.1 单个 commit revert
- **IDX-5 revert**：删除 `cmd/file-content-backfill` + `internal/filebackfill/` → 只损失 backfill 能力，不影响增量抽取
- **IDX-4 revert**：file-extractor 退回 stub 状态（跑但不实际抽取，全 skip）→ 增量抽取停摆，OS 里已抽取的 content 不受影响
- **IDX-3 revert**：删除 `cmd/file-extractor` + `internal/fileextract/` → file-extractor Deploy 缺 binary，需要同时 revert 部署 YAML（在 codex 仓，不在本 PR）
- **IDX-2 revert**：FilePayload 回退到 5 字段 → 已写 content 的 doc _source 里字段依然存在（OS 不 care）+ mapping 里 content 字段依然存在（无用不占多少空间）— **无害保留**
- **IDX-1 revert**：mapping.json 回退 → **mapping.json 与 live mapping 不一致**（live 里已有 content 字段，embed 里没了）→ 启动断言 mapping_compat 会通过（fail-closed 检的是 embed→live，不是反向）— **实际上 revert IDX-1 无副作用但也无价值**（live mapping 不可逆）

### 5.2 整个 PR revert
```bash
git revert -m 1 <merge-commit-sha>  # 如果走 merge commit
# 或
git checkout main; git reset --hard <pre-branch-sha>  # 危险，慎用
```

Revert 后：
- 代码回到 pre-branch 状态
- **已 PUT 到 OS 的 mapping 保留**（新字段不消失，OS mapping add-only）
- **已抽取的 content 数据保留**（OS 里 doc 的 payload.file.content 字段依然存在）
- 未来若要重新上线，只需要重新 apply 本 PR 即可（幂等）

### 5.3 已 PUT mapping 的**不可逆性**（关键提示）
OpenSearch 3.x mapping 新增字段是**不可逆操作**：
- 一旦 `PUT _mapping` 添加了 `payload.file.content` + `payload.file.contentMeta`，无法通过 API 删除
- 只能通过 **reindex** 到新 backing index + 切换 alias 才能"消除"字段
- 但字段存在但不写入是零开销的（IK 分词器不会自动填内容）

**结论**：mapping 变更是**一次性单向决策**。IDX-1 里 commit message + PR description 显式警告这一点，主人拍板前确认。

---

## §6 Max 需要在 PR review 前做的事

### 6.1 IDX-1 前
- ✅ **已完成**：主人决策 #2 已拍板"改 mapping.json 但不 PUT"
- **待做**：跟 OS admin 提前打招呼，说明 test 环境 mapping PUT 计划（IDX-1 PR merge 后择时 apply）— 走 tech-deploy.md 里改 OS mapping 的红线路径

### 6.2 IDX-4 前
- **🔴 硬要求（Max review v2 §2）**：test 环境**必须**预先起 Tika sidecar Deploy（`apache/tika:3.3.0.0` minimal 镜像 165MB 压缩），不是 optional。理由：dmwork/local-dev-stack 缺 tika，本地 compose 30-60min 不划算；file-extractor 是新服务部到 test 环境抽真实文件更贴近生产。IDX-4 dev cycle = 本地跑单测 + push test 分支镜像跑 e2e。**由 Max 负责在 IDX-4 开发前起好 Tika Deploy**。

### 6.3 IDX-5 前
- **待做**：跑一次 OS aggregation 拿 type=8 avg file size：
  ```json
  GET prod-octo-message-read/_search
  {
    "size": 0,
    "query": {"term": {"payload.type": 8}},
    "aggs": {
      "avg_size": {"avg": {"field": "payload.file.size"}},
      "sum_size": {"sum": {"field": "payload.file.size"}}
    }
  }
  ```
  验证 v2 §11 假设 avg 5MB 是否准确；如果实际 avg 20MB → 回灌 CDN 流量估算翻 4 倍到 ~2.4TB，需重估账单

### 6.4 Push PR 前的最后 checklist
- [ ] 5 个 commit 消息符合 conventional-commits 格式
- [ ] `go test ./...` 全绿
- [ ] `go build ./...` 全绿
- [ ] 分支已 rebase 到最新 main
- [ ] docs/ 下 feasibility + tool-comparison + implementation 三份文档都在本 PR 里（或已 merge）
- [ ] PR description 用 §1.4 模板填齐

---

## §7 (b) 设计时发现的技术风险 / 未决问题（比 v2 更实施层）

1. **file-extractor 与 es-indexer 时序竞态**（IDX-4 §3.3 已讨论）— 新消息进 Kafka 后，file-extractor 可能比 es-indexer 先抽完 → OS `_update` 返 404。**MVP 策略（v2 收敛）**：`errDocNotYet` 触发本批 Kafka rebalance 自然重试 + IDX-4 加 `EXTRACT_STARTUP_DELAY_SECONDS=5` config 缓解启动瞬间竞态。**局限**：只治启动瞬间，稳态下 es-indexer 重启 30s 依然会撞 404。**Phase 2 备选方案**（观察 prod DLQ `errDocNotYet` 触发率超阈值后启用）：塞独立 Kafka retry topic `octo.message.v1.file-extract.retry{,.prod}`，5s 后重放 — 需要多一个 topic + 消费逻辑，MVP 不做。

2. **Tika HTTP 500 body 解析脆弱性**（IDX-4 已讨论）— 用字符串 contains "EncryptedDocumentException" 区分错误类型。**风险**：Tika 版本升级/patch 改 body 格式 → 判断失效 → 所有 encrypted 文件被误判为 extract_error → DLQ reason 统计失真。**缓解**：IDX-4 单测里用真实 Tika 3.3.0 采样 body 字符串锁死；升 Tika 版本时同步 review

3. **OS `_update` 语义 vs bulk 语义不一致**（IDX-4）— 主流程用 `_bulk` 批量，file-extractor 用 `_update` 单条。**风险**：`_update` 单条 RTT 高，123K 回灌 40min 假设成立需 avg 200ms/请求；如果 OS 慢查询 > 500ms/请求 → 回灌变 2 小时。**缓解**：实测阶段 1 上线后如果发现慢，切 `_bulk update actions`（`_bulk` 支持 update action，语义相同批量提交）

4. **黑名单/白名单扩展名 corner case**（IDX-4）— 文件名可能没扩展名 or 扩展名与内容不符（用户改名）。**决策**：优先信任 `payload.file.extension` 字段（Kafka 消息里有，是 octo-server 上传时磁数存的），fallback `path.Ext(name)`；两个都空 → 默认走白名单（尝试抽取）而不是跳过（宁抽多不漏）

5. **backfill 与 file-extractor 同时跑的冲突**（IDX-5）— 主人决策"backfill 在 file-extractor 稳定 24h 后跑"避免了这个问题。**风险**：如果紧急情况提前跑 backfill，两者同时 update 同一 doc 会触发 version 冲突 → retry_on_conflict=3 通常够用。**缓解**：backfill Job 里加 flag `-skip-if-content-exists`（scroll query 已经带 `must_not exists content` 天然满足，无需额外 flag）

6. **单 PR 5 commit 的 review 疲劳**（元层面）— 单 PR 累计 ~1500-2000 行新增 + 修改（估算）— review 需要 1-2 小时集中精力。**建议**：Max review 时按 §1.4 模板"先看 IDX-1 mapping.json"的顺序进入，避免直接看 IDX-4 迷失在 DLQ 分支里

---

## §8 (c) 每个 commit 的开发+测试估时（v2 修订）

| Commit | 开发估时 | 测试估时 | 合计 |
|---|---|---|---|
| IDX-1 | 15min | 5min（无代码） | **20min** |
| IDX-2 | 20min | 20min | **40min** |
| IDX-3 | 1h | 1h | **2h** |
| IDX-4 | **5-6h**（v2 上调，Max review） | **3-4h**（v2 上调） | **8-10h** ⚠️ |
| IDX-5 | 2h | 1.5h | **3.5h** |
| **合计** | ~9-10h | ~6-7h | **~13-15h ≈ 2 工作日** |

**timebox（v2 修订）**：IDX-4 开发超 **6h** 或测试超 **4h** 回来对齐范围（v1 是 4h/3h）。

---

## §9 (d) 动工前需要 Max/主人补充的信息

**必须**（阻塞开工）：
- 无 — 主人 3 个决策已全部拍板（reader 并行 / backfill 一次性 K8s Job / file-extractor 独立 Deploy）；本任务书假设已确认

**建议**（不阻塞但影响准确度）：
1. **prod type=8 avg file size** —— 影响 §11 CDN 流量估算准确度（§6.3 已列步骤）
2. **是否有 monorepo dev 环境** —— 本地 e2e 联调需要 kafka + opensearch + tika 三件套，如果 dmwork 有现成 docker-compose 快 30 分钟；如果没有，IDX-4 开发时需要自己 compose 起
3. **Tika 镜像选型** —— 默认按 §5 feasibility 决策用 `apache/tika:3.3.0.0` minimal (~165MB 压缩)；full 版 (~333MB) 需要吗（本期不做 OCR，minimal 就够）— 主人如无异议默认 minimal

---

## §10 完成后交付清单

- (a) 任务书路径：`~/Project/Mininglamp-OSS/octo-search-indexer/docs/file-content-indexing-implementation.md`（本文件）
- (b) 实施层未决问题：§7 6 条
- (c) 时间估算：§8 合计 ~11h ≈ 1.5 工作日
- (d) 动工前信息缺口：§9 无阻塞项，3 条建议项

**待 Max review 通过后**，按 IDX-1→IDX-5 顺序开工，每 commit 单测通过再进下一步。**全部 5 commit 完成 + 本地 e2e 跑通后再 push GitHub**（主人已拍板）。
