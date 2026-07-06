# file content indexing — Code Review

**Branch**: `feat/file-content-indexing` (5 commits ahead of `origin/main`)
**Reviewer**: cc-octo (以 fresh-context independent reviewer 立场，非原作者视角)
**Review date**: 2026-07-02
**Scope**: 5 commits (IDX-1 → IDX-5) 共 33 files / 5042+ / 7-；重点 pkg `internal/fileextract`、`internal/filebackfill`、两个 `cmd/*`、`internal/esindex` FilePayload/mapping/mapping_compat 改动，以及 3 份 docs 与代码对齐度。

---

## §0 总判定

**BLOCK** — push GitHub PR 前必须修 §1 里的 1 条严重问题（backfill snowflake messageId float64 精度丢失，直接导致 K8s Job OS `_update` 打不到目标 doc）。修完后建议一并处理 §2 里的可维护性 / 单测覆盖问题，再走 push → PR → 主人 sig-off 流程。

**理由摘要**：

- 严重 1 条：backfill 从 OS `_source.messageId`（mapping 声明 `long`，snowflake 19 位数字）反解到 `MessageID any`，Go 默认 JSON unmarshal 把 JSON number 解成 `float64` → `fmt.Sprintf("%v", ...)` 输出科学计数法 `1.234e+18` → 作为 OS `_id` 送 partial `_update` → 100% 打不到真 doc（要么 404 计入 OSTransient 撤销 Job，要么写错到 alias 里精度相同的假 doc）。这与本 repo 早已确立的 snowflake 精度铁律（`internal/esindex/buildraw.go:22` 明确注释 + `TestProject_SnowflakePrecision` 现有测试）冲突。
- 应修 9 条：`defer cancel()` 在 for 循环内累积、JSON body 用 `fmt.Sprintf` 拼接、Content-Disposition 头未 sanitize（filename 用户可控存在 CRLF 注入面）、runner ctx 取消被当作错误、`strings.Contains` 被手写复刻、tika ctx 取消只覆盖 DeadlineExceeded、`Runner.Run` 零测试覆盖、DLQ retry/spill 字段声明但未实现、ratelimit 无锁但无 doc 警示。
- Nit 若干：陈旧的 IDX-3/4/5 stub 注释、`FilePayload` 拆参 bridge 冗余、backoff 注释在 config vs download 不一致、`formatDuration(500ms)` 返回 `"0s"` 是 OS scroll 立即过期陷阱、`DLQMaxRetries` 等未消费字段。

---

## §1 严重问题（BLOCK — 必须修才能 push）

### S1. `internal/filebackfill/source.go:159` — snowflake messageId 被 float64 截断

**问题**：`parseHits` 里 struct 定义把 messageId 声明为 `any`：

```go
var src struct {
    MessageID any `json:"messageId"` // long 反序列成 float64；用 fmt %v 转字符串
    ...
```

Go 标准 `json.Unmarshal` 遇到 JSON number 且目标是 `interface{}` 时，一律解为 `float64`。IEEE 754 double 只能无损表示 2^53 以内的整数，snowflake messageId 通常 18-19 位（远超 2^53 ≈ 9e15）。`fmt.Sprintf("%v", float64(1234567890123456789))` 输出 `"1.2345678901234568e+18"`（后 3-4 位精度丢失 + 科学计数法字符串）。

**下游影响链**：
1. `sourceDoc.MessageID = "1.234e+18"` （错误字符串）
2. `runner.processOne` → `fileextract.ExtractAndWriteForBackfill(..., doc.MessageID, ...)`
3. `extractor.ExtractAndWrite` → `osWriter.UpdateContent(ctx, messageID, ...)`
4. `opensearchapi.UpdateReq{DocumentID: "1.234e+18"}` → OS 找不到该 `_id`
5. 返 HTTP 404 → `errDocNotYet` → `stats.OSTransient++` → K8s Job 退出码 1
6. 更坏情况：若两条不同的 snowflake 落入同一 float64 桶（低位截断相同），可能把一条的 content 写到另一条上（数据污染，很难排查）

**建议修法**：

方案 A（首选，最小改动，与 `internal/esindex/buildraw.go:180` `decodeObjectUseNumber` 风格对齐）：

```go
// 用 json.Decoder + UseNumber() 保精度
dec := json.NewDecoder(bytes.NewReader(h.Source))
dec.UseNumber()
var src struct {
    MessageID json.Number `json:"messageId"`
    Payload   struct{...} `json:"payload"`
}
if err := dec.Decode(&src); err != nil { continue }
msgID := src.MessageID.String()  // 全精度字符串，无科学计数法
```

方案 B（更保守）：改 `MessageID string` — OS 会自动把 long 序列化成带引号的字符串？No，OS 不会，long 类型 `_source` 返 JSON number。所以必须走方案 A。

**新增回归测试**：

```go
func TestParseHits_SnowflakePrecision(t *testing.T) {
    // 19 位真实 snowflake id
    body := `{"messageId":1234567890123456789,"payload":{"file":{"url":"x"}}}`
    hit := opensearchapi.SearchHit{Source: []byte(body)}
    docs := (&osScrollSource{}).parseHits([]opensearchapi.SearchHit{hit})
    if docs[0].MessageID != "1234567890123456789" {
        t.Fatalf("precision loss: got %q", docs[0].MessageID)
    }
}
```

**严重性理由**：backfill Job 是本项目一次性 123K docs 存量回填的核心动作，主人已 sig-off 「backfill 走 K8s Job 一次性跑」路径。此 bug 使 Job **100% 无法完成 happy path**（每条 doc 都是 OS 404），Job 会被 dlqRatio 或 OSTransient 判为失败退出，测试环境 e2e 就会暴露，但也可能被误诊为「主 doc 未落」时序竞态，浪费排查时间。且该 pattern 与 repo 已有铁律冲突（同类 bug 早在 esindex 里被显式防御），是纯粹的忘记复用既有防御方案。修不修属于是否遵守项目工程铁律的问题。

---

## §2 应修问题（REQUEST CHANGES — 强烈建议 push 前处理，最低限度处理 M1/M2/M4/M7）

### M1. `internal/fileextract/consumer.go:88-92` — `defer cancel()` 在 for 循环内堆积

**问题**：

```go
for len(batch) < size {
    fetchCtx := ctx
    if len(batch) > 0 {
        var cancel context.CancelFunc
        fetchCtx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
        defer cancel()  // <-- 每次循环都注册一个 defer，直到 fetchBatch 返回才统一 fire
    }
    m, err := p.source.Fetch(fetchCtx)
    ...
}
```

每循环一次 `defer cancel()` 入栈，凑一个 50 条 batch 就堆 49 个 pending cancel（+ 关联 timer / goroutine 资源），全部等 fetchBatch 返回才释放。相对：`internal/consumer/consumer.go:103-105` 是 idiomatic 写法：

```go
fctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
m, ferr := p.src.Fetch(fctx)
cancel()  // 立即释放，不 defer
```

**建议修法**：删掉 `defer`，紧跟 `Fetch` 后 `cancel()` 显式释放（同 reference impl）：

```go
for len(batch) < size {
    fetchCtx, cancel := deriveFetchCtx(ctx, len(batch))
    m, err := p.source.Fetch(fetchCtx)
    cancel()
    if err != nil { ... }
    batch = append(batch, m)
}
```

**严重性理由**：稳态每分钟 12-60 批（Kafka poll 频率），每批堆 ~50 defer + timer，一小时数万个未释放 context/timer 资源慢慢积压。虽然 fetchBatch 返回时会全释放（不是不可恢复泄漏），但 Kafka client 内部若因 rebalance 之类拖长 fetchBatch 生命周期就出问题。且属于典型 go vet / golangci-lint `deferloop` 类可自动检出的 lint 违规，push 到 GitHub CI 大概率就红。

---

### M2. `internal/fileextract/tika.go:70` — Content-Disposition filename 未 sanitize（HTTP 头注入面）

**问题**：

```go
req.Header.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
```

`filename` 来自 `filePayload.Name`，源头是 octo-server 上传时 user-supplied 的文件名，**未经任何 sanitize**。攻击面：
- filename 含 `"` → 头被过早闭合，剩余字符可能被 Tika 视作后续 header 参数
- filename 含 `\r\n` → CRLF 注入，可以插入任意额外 header 到 Tika 请求（typical HTTP header smuggling）
- filename 含 unicode → 非 RFC-compliant，Tika 可能拒收或误解

实际用户上传中文文件名（`预算表2026.pdf`）也常见，双引号 quote 已经不 robust。

**建议修法**：

```go
// 只保留 ASCII 可打印字符 + 常见中文/日文；过滤 CR/LF/双引号
sanitized := sanitizeFilename(filename)
if sanitized != "" {
    req.Header.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, sanitized))
}

func sanitizeFilename(s string) string {
    var b strings.Builder
    for _, r := range s {
        if r == '"' || r == '\r' || r == '\n' || r < 0x20 {
            continue
        }
        b.WriteRune(r)
    }
    return b.String()
}
```

或更严格：走 RFC 6266 `filename*=UTF-8''...` 编码格式（Tika 支持）。

**回归测试**：filename = `"attack.pdf\r\nX-Injected: yes\r\n\r\n"` 应产出无 CRLF 的头值。

**严重性理由**：即使源头 octo-server 应该 sanitize，file-extractor 是新引入的外部依赖（Tika Server）调用方，防御性 sanitize 是 code hygiene 硬要求。真实攻击可能性低（要 octo 上传接口先漏），但 CRLF 注入 → Tika 拒服务 → 该 doc extract_error DLQ 是相对高概率的 non-attack 场景（用户上传了不规范文件名）。

---

### M3. `internal/filebackfill/source.go:89-99, 118-123, 192` — JSON body 用 `fmt.Sprintf` 字符串拼接

**问题**：三处 OS 请求 body 都是 raw string + `fmt.Sprintf` 拼字段，例如：

```go
body := `{
    "size": ` + fmt.Sprintf("%d", s.size) + `,
    ...
}`
```

```go
body := fmt.Sprintf(`{"scroll":"%s","scroll_id":"%s"}`, formatDuration(s.scrollTTL), s.scrollID)
```

问题：
1. **风格与 repo 不一致**：整个 `esindex` 包用 `json.Marshal` 或结构化 builder，唯独此处退化到字符串拼接。
2. **潜在注入面**：`s.scrollID` 来自 OS 返回，理论上是 base64 变体，但如果未来 OS 版本升级返回结构变化，或 `s.size` 被恶意 config 注入 `1, "malicious": true`，都会破坏 JSON。
3. **难以扩展**：加 `_source_includes` 之类过滤需再拼字符串，容易漏 comma。

**建议修法**：

```go
type scrollQuery struct {
    Size    int             `json:"size"`
    Query   json.RawMessage `json:"query"`
    Source  []string        `json:"_source"`
}
body, _ := json.Marshal(scrollQuery{Size: s.size, Query: filterQuery, Source: []string{"messageId", "payload.file"}})
```

**严重性理由**：不是运行期 bug（当前 inputs 都是内部可信值），但是明确 code smell，push 前顺手清理性价比高。也是 §6 里陈旧文档更新的一部分。

---

### M4. `internal/filebackfill/runner.go:74-83` — ctx.Canceled 被当作 error 返回

**问题**：

```go
for {
    if err := ctx.Err(); err != nil {
        return stats, nil  // ← 循环开头判 ctx 是 nil 返回
    }
    batch, err := r.source.Next(ctx)
    if errors.Is(err, io.EOF) {
        return stats, nil
    }
    if err != nil {
        return stats, err  // ← 但 source.Next 的 ctx 取消错也走这里
    }
    ...
}
```

`source.Next()` 内部调 `client.Search(ctx, ...)`，ctx 取消时返回 wrapped `context.Canceled` err（不是 nil）。此处直接 `return stats, err` 冒泡到 `main.go:103`：

```go
stats, err := runner.Run(ctx)
if err != nil {
    return fmt.Errorf("runner.Run: %w (stats: %+v)", err, stats)
}
```

结果 K8s Job 收到 SIGTERM（滚动更新 / 手动 delete pod）后退 1 + 打错误日志，实际是「正常优雅退出」。误报会让运维困惑。

**建议修法**：

```go
batch, err := r.source.Next(ctx)
if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
    return stats, nil
}
if err != nil {
    return stats, err
}
```

**严重性理由**：影响 K8s Job 退出语义 + 运维排障成本；不影响正确性但影响 signal-noise ratio。

---

### M5. `internal/fileextract/download.go:139-146` — 手写复刻 `strings.Contains`

**问题**：

```go
// contains 是 strings.Contains 的替身避免多导一个包（本文件仅两处 substring 判断）。
func contains(s, sub string) bool {
    for i := 0; i+len(sub) <= len(s); i++ {
        if s[i:i+len(sub)] == sub {
            return true
        }
    }
    return false
}
```

且 `tika.go:90` 也调这个 `contains`。理由（「避免多导一个包」）不成立：`strings` 是 stdlib 零 cost 依赖，本 repo 其它文件（`esindex/mapping_compat.go`, `consumer.go`）大量使用。手写实现还比 `strings.Contains`（走 Rabin-Karp 优化）慢。

**建议修法**：删 `contains`，全部改 `strings.Contains`。改动 3 行：`download.go:135`, `tika.go:90`, 加 `import "strings"` 到 download.go。

**严重性理由**：纯 code hygiene，独立 nit 但和 M3 组合起来读起来像作者要么在赶时间要么不熟 stdlib，任何 reviewer 会挑。

---

### M6. `internal/fileextract/tika.go:74` — ctx 取消判定只覆盖 DeadlineExceeded

**问题**：

```go
resp, err := t.hc.Do(req)
if err != nil {
    if ctx.Err() == context.DeadlineExceeded {
        return "", false, errExtractTimeout
    }
    return "", false, fmt.Errorf("%w: %v", errExtractGeneric, err)
}
```

如果 caller ctx 是 `context.WithCancel` 且手动 cancel（用户 SIGINT），此处会走 errExtractGeneric 分支，caller 判 permanent 失败进 DLQ = extract_error，浪费一条重试机会 + DLQ 污染。

**建议修法**：

```go
if err != nil {
    if ctxErr := ctx.Err(); ctxErr != nil {
        // ctx 取消/超时都视为 timeout，caller 决定是否重试/退出
        return "", false, errExtractTimeout
    }
    return "", false, fmt.Errorf("%w: %v", errExtractGeneric, err)
}
```

同样问题在 `download.go` 的重试循环里也存在（line 84-87 处理 `ctx.Done()` OK，但 `tryFetch` 里 `d.hc.Do(req)` 返错时不区分是 ctx 取消还是网络错，走 transient 分支会白白重试 3 次然后失败）。参考建议：`tryFetch` 出错后加 `if ctx.Err() != nil { return nil, "", ctx.Err() }` 快速返。

**严重性理由**：影响优雅关停语义 + DLQ 干净度。

---

### M7. `internal/filebackfill/runner_test.go` — 核心 `Runner.Run` 零直接测试覆盖

**问题**：`runner_test.go` 只测了 rateLimiter / Stats 字段类型 / Config 转换 / formatDuration / sourceDoc 字段。**`Runner.Run` 主循环没有一条直接测试**：
- ❌ mock source 塞 3 batch (10/20/EOF)，mock extractor 分别返 success / DLQ reason / errDocNotYet，验证 Stats.Extracted=25 DLQ=3 OSTransient=2
- ❌ ctx 中途 cancel，验证 Run 优雅返回 + Stats 部分数据
- ❌ progress log 分支（time.Since > progress → 打日志）
- ❌ defer Close 一定被调（`mockSource.closed` 状态验证）

**建议**：至少加 3 条测试：
```go
func TestRun_HappyPath(t *testing.T) { /* 全 success */ }
func TestRun_MixedOutcomes(t *testing.T) { /* success + DLQ + OSTransient 各若干 */ }
func TestRun_CtxCancelDuringLoop(t *testing.T) { /* 中途 cancel */ }
```

用 `mockSource` + inject 一个 mock extractor（现在 extractor 类型是 `*fileextract.Extractor` 具体类型，不易 mock；建议在 filebackfill 里定义一个 `extractorIface interface { ExtractAndWrite(...) }`，Runner 依赖 interface 而非 struct）。

**严重性理由**：backfill 是本项目一次性 K8s Job 的**唯一**执行路径，其状态机（success/DLQ/transient/ctx-cancel 分支）零测试覆盖 = 上线只能靠人工 e2e，且 S1 那个 bug 一条基本 Run 测试就能捕获（造一个 messageId=1234567890123456789 的假 hit 走一遍 processOne 即可）。缺少这些测试是导致 S1 上线才发现的直接原因。

---

### M8. `internal/fileextract/config.go:53-55` — DLQMaxRetries / DLQRetryBackoff / DLQSpillDir 声明但未消费

**问题**：三个字段在 config 里声明，在 `cmd/file-extractor/main.go` 从 env 读入，但 `internal/fileextract/kafka.go` 的 `kafkaDLQSink.WriteDLQ` 就是一次 `writer.WriteMessages` 直接返，没有 retry、没有 spill 到 disk。config.go:52 注释还写：

```
// DLQ 自身 transient 重试参数（复用 consumer/dlq.go 语义）
```

—— 但实现完全没有。误导性文档 + 死配置。

**建议修法**：二选一
- **A（首选）**：删掉这三个 field + env 读取 + 注释；DLQ 就是一次写，写不了错就 return，让 caller 决定（consumer.go 已经有 `return err` 逻辑，会暂停批推进）。
- **B**：真正实现 retry + spill，参考 `internal/consumer/dlq.go` 的 handler 结构。

给规划来看，A 更合理（file-extractor 场景 DLQ 量远小于主 consumer，简化就够）。

**严重性理由**：dead code + 文档欺骗，push 之前挑，避免运维被误导设置 spill dir 结果没落文件。

---

### M9. `internal/filebackfill/ratelimit.go` — 无锁 + 无线程安全 doc 警示

**问题**：`rateLimiter` struct 有 mutable fields (`tokens`, `last`) 但 `Wait` 无 mutex，无 doc 明说「非线程安全」。当前 `runner.go` 单 goroutine 调用 OK，但下次谁 refactor 加个并发 worker 池就会 silent 破坏限速。

**建议修法**：
- **最小改动**：加 doc `// NOT safe for concurrent use; caller must serialize.`
- **稳妥**：加 `sync.Mutex` 保护 `Wait`（对性能影响可忽略）

**严重性理由**：defensive coding，future-proof。

---

## §3 nits（optional，push 后可跟进；建议顺手改 N1/N4/N5）

### N1. 陈旧 IDX-3 stub 注释

`internal/fileextract/config.go:9-16`, `consumer.go:1-4, 26, 37, 40, 105, 141-146` 都仍在讲「IDX-3 骨架期字段声明但未使用」「本 commit 只搭骨架 stub」「IDX-4 补齐」等阶段性叙事。5 commits 合并成 branch 后阶段性说明失去意义，读起来像作者忘了清理。

**建议**：删所有 `IDX-3`/`IDX-4`/`IDX-5` 阶段词，把语义描述改成稳态视角。例：`consumer.go:1-3` 从「IDX-4 补 抽取」改成「type=8 消息走 download → Tika → OS partial update；非 type=8 跳过 commit 位点」。

### N2. `internal/fileextract/backfill_bridge.go` — 拆参 bridge 冗余

现在为了让 `filePayload` 保持 unexported，写了个 5 参 wrapper：

```go
func ExtractAndWriteForBackfill(ctx, e, messageID, url, name, ext string, size int64) (string, error, error)
```

如果 filePayload 就是纯数据 struct（当前是），直接 export 它成 `FilePayload` 会更清爽，backfill 直接构造：

```go
e.ExtractAndWrite(ctx, doc.MessageID, &fileextract.FilePayload{URL:..., Name:..., ...})
```

拆参没有隐藏什么，5 参跟 5 字段一模一样。且返回 signature `(string, error, error)` 两个 error 也不 idiomatic — dlqReason 更适合是 `Outcome` enum，cause 应该合并进 `err` 的 wrapping。

**建议**：export `FilePayload`，删 `backfill_bridge.go`。或至少把 `(reason, cause, err)` 三元组改成 `Result` struct，语义更清楚。

### N3. `internal/fileextract/config.go:45` vs `download.go:82` — backoff 注释矛盾

- `config.go:45`: `HTTPRetries int // CDN GET 重试次数（默认 3，指数退避 1s/4s/16s）`
- `download.go:82`: `wait := d.retryBackoff * time.Duration(1<<attempt) // 1s / 2s / 4s / 8s (base=1s, retries=3 → 1s/2s/4s)`

实际是 `1s / 2s / 4s / 8s`（2^n），不是 `1s/4s/16s`（4^n）。config.go 注释错，删或改。

### N4. `internal/filebackfill/source.go:203-208` — `formatDuration(500ms) → "0s"` 陷阱

`formatDuration` 对 < 1s 返 `"0s"`。OS 视 `scroll=0s` 为立即过期 → scroll context 首批就没了。如果有人误设 `-scroll-ttl 500ms` 或 env `BACKFILL_SCROLL_TTL=500ms`，backfill 立刻死。

**建议**：加 floor —
```go
func formatDuration(d time.Duration) string {
    if d < time.Second { d = time.Second }
    ...
}
```
或在 `newOSScrollSource` 里加 validation：`if ttl < time.Second { return nil, fmt.Errorf("ScrollTTL must be >= 1s") }`。

### N5. `cmd/file-extractor/main.go:96` — `strings.EqualFold` 判 bool

```go
enabled := strings.EqualFold(os.Getenv("FILE_EXTRACTOR_ENABLED"), "true") && ...
```

只识别 `"true"/"TRUE"/"True"`，不识别 `"1"/"yes"`。K8s manifest 常用 `enabled: "1"`。用 `strconv.ParseBool` 更 idiomatic + 覆盖广：

```go
b, _ := strconv.ParseBool(os.Getenv("FILE_EXTRACTOR_ENABLED"))
enabled := b && ...
```

### N6. `internal/fileextract/consumer.go:141-146` stub 分支只测试用

`extractor == nil → stub log` 分支在生产 `NewService` 里不可能触发（NewExtractor 失败会直接 return error）。只有 `TestProcessOne_TypeFileHitsStub` 走这里，测试的是「stub 兼容」而不是任何真实行为。

**建议**：删该分支 + 该测试，让 consumer_test 走真 extractor（httptest CDN + Tika + OS 已经有基础设施在 idx4_test.go 里）。测的东西反而更真实。

### N7. `internal/filebackfill/source.go:172` — `fmt.Sprintf("%v", src.MessageID)` 若为 `<nil>` 判定被 stringify 侵蚀

即使修了 S1 用 `json.Number`，`json.Number` 也有 `""` 零值情况（field 缺失）。此处 line 173 判 `msgID == "<nil>"` 只对 `any` 类型有效，改 `json.Number` 后应改成 `if src.MessageID == "" { continue }`。修 S1 时一起调整。

### N8. `internal/esindex/mapping/README.md:1` — 版本号叙事

标题写 `v1.12 — file content indexing`，但 repo 里其它文件（如 `doc.go`）叙事还是 v1.9/1.10/1.11 演进，v1.12 一致性 OK。这条 nit 撤销，仅记录我检查了。

---

## §4 已识别的正向亮点（不超过 5 条）

1. **`internal/esindex/mapping_compat.go` v1.12 断言集扩展设计干净**：`requiredMappingFieldPaths` 加两条 + 走既有 flatten/断言路径，`mapping_compat_test.go` 补 4 条测试（missing content / missing contentMeta / IncludesV112Fields / 全 mapping pass），fail-closed 语义与既有 payloadRaw / mergeForward 断言一致，这是本 PR 最干净的一部分。
2. **`internal/fileextract/tika.go:101-113` `truncateContent` UTF-8 边界回退** + `TestTika_Utf8SafeTruncate` 直接测中文截断不产生半字符，这类细节做到位显示作者对 IK 分词下游影响有考虑。
3. **`internal/fileextract/oswriter.go:9-11` errDocNotYet 分类文档 + errors.Is 分类清晰**：把 OS 404 / 409 / 5xx / 4xx 四类分成 3 个 sentinel error 让 caller 分级决策，比一个统一 error 好读。
4. **`internal/filebackfill/config.go:12-15` scroll query 幂等设计**：`must_not exists content` 天然跳过已抽取 doc，中断重跑无副作用 —— 这个决策免掉了 checkpoint 复杂度，是明智的 trade-off（作者显式在 `Package filebackfill` doc 里陈述这个决策也是加分）。
5. **`internal/fileextract/consumer.go:45-54` startupDelay 缓解时序竞态 + `errDocNotYet` 稳态兜底**：Phase 1 startup sleep + Phase 2 独立 retry topic 备选的分阶段设计，加上错误分类 → Kafka rebalance 自然重试，思路清楚，实现相对简单。

---

## §5 未验证但值得后续 e2e 关注的点

1. **Tika 3.3.0.0 minimal 镜像实际行为**：`TestTika_EncryptedDocument500Body` 用 `"org.apache.tika.exception.EncryptedDocumentException"` 字符串锁死契约（v2 §7 #2 提及升级需 review），但实际 Tika minimal 镜像启动参数、default JVM heap、是否装 IK-friendly OCR pipeline 都需要 test 环境跑真实 PDF 才能确认。特别是加密 PDF 的错误路径，`EncryptedDocumentException` 是否总是出现在 body 里、是否有 wrap 层影响 substring 匹配，需在 test 环境用 3-5 个真实加密 PDF 样本 validate。
2. **OS `_update` doc_as_upsert=false + retry_on_conflict=3 的行为**：单测覆盖 code path，但 partial update 遇到 mapping 上 dynamic:strict 时若 `contentMeta` 里出现意外字段（比如 truncated 为 true 被 omitempty 掉、extractedAt 是零值）的实际行为需 e2e 跑一遍。特别是 `TestFilePayload_ContentSerialization` 里 truncated=false 走 omitempty 不落盘，若真的 partial update 缺 truncated 字段，OS 索引里对应 doc 的 truncated 会保留旧值还是 unset 需实测。
3. **file-extractor vs es-indexer 稳态竞态 rebalance 兜底**：作者主张 errDocNotYet 触发本批 Kafka 上抛让 rebalance 重取，但 kafka-go Reader 的 offset 提交语义（`CommitInterval=0` 手动）+ rebalance 时 uncommitted messages 是否真会重取，代码逻辑合理但缺一个双 pod 交替 rebalance 的 chaos 测试 validate。
4. **backfill Job 大量 DLQ 时 dlqRatioMax=0.10 阈值合理性**：`cmd/file-content-backfill/main.go:65` 硬编码 10%，123K docs 里若 12K 走 DLQ 就 exit 1。若真实 corpus 里加密 PDF / 空白 doc / oversize 比例本来就高（feasibility.md 提「阶段 2 视 empty_extract 占比决定是否上 MinerU」），这个阈值可能一开始就误报。需 test 环境跑一批采样看真实占比再决定 threshold。
5. **CDN 直连稳定性**：`download.go` 走公网 CDN + 指数退避重试，但没有 circuit breaker（例如 CDN 短暂全挂时 file-extractor 会 3s+6s+... 阻塞每条消息 → Kafka 滞后堆积）。稳态跑 24h 观察 CDN p99 延迟 + 失败率再决定是否加熔断。

---

## §6 review 方法论

**顺序**（约 55 min）：

1. (5 min) `git log --oneline`, `git diff --stat`, 确认改动范围与我先前脑中的实现路径吻合。
2. (10 min) `internal/fileextract` 6 个非-test 文件全读：`service.go` → `consumer.go` → `extractor.go` → `download.go` → `tika.go` → `oswriter.go` → `dlq.go` → `payload.go` → `kafka.go` → `metrics.go` → `config.go` → `backfill_bridge.go`。按调用链自顶向下读。
3. (10 min) `internal/filebackfill` 3 个非-test 文件 + `cmd/file-content-backfill/main.go`：`config.go` → `runner.go` → `source.go` → `ratelimit.go` → cmd 入口。
4. (5 min) `internal/esindex/doc.go` diff (FilePayload 加字段) + `mapping/octo-message.json` diff + `mapping_compat.go` diff。
5. (10 min) 测试文件覆盖度扫描：每个 pkg 的 `*_test.go` 都过一遍，标记「测了 A、B，没测 C、D」。发现 `runner_test.go` 零 Run 覆盖 = 红旗。
6. (5 min) `go build ./...` + `go test -race ./internal/fileextract/... ./internal/filebackfill/...`，确认基础工具链干净（都过）。
7. (5 min) 对比 reference impl（`internal/consumer/consumer.go` fetchBatch pattern）+ 查 opensearch-go v3.1.0 vendored src 确认 `RetryOnConflict`, `BuildRequest`, `Update` API 语义 + err 返回契约。
8. (5 min) `docs/*.md` 三份文档快速扫，找与代码不对齐的地方（发现 config.go 里 IDX-3 stub 注释叙事全篇陈旧）。

**跳过说明**：
- 没深读 `docs/file-content-indexing-feasibility.md` v2 全文（460 行），主要看目录结构 + 与代码对齐的关键段（DLQ 8 种 reason 表格、`_source.excludes` 决策、payload.type=8 数值）。假定 v2 已在早期 review 通过。
- 没跑 e2e 或 mock OS，`go test -race` 都过即视为基础健康。§5 明确列出 e2e 才能验证的点。
- `docs/file-extractor-tool-comparison.md` 只扫标题，Tika 选型论证不在本 code review 范围（那是设计层决策，本轮只 review 实现）。

**发现分布**：
- §1 严重: 1 条（snowflake 精度）
- §2 应修: 9 条（defer cancel 循环、CRLF 注入、JSON 拼接、ctx.Canceled 误报、手写 Contains、tika ctx 分类、Runner.Run 零覆盖、DLQ 字段死代码、ratelimit 无锁 doc）
- §3 nits: 8 条

**最出乎意料的发现**：§1 的 snowflake 精度 bug —— 这个陷阱在 `internal/esindex/buildraw.go` 里 line 22 有一整段 doc 显式讲「json.Decoder + UseNumber()，否则默认 float64 会截断 >2^53 的雪花 id」，还配了 `TestProject_SnowflakePrecision` 测试。作者写 backfill 时明显没参考 esindex 里已有的 `decodeObjectUseNumber` helper（line 180-194），也没运行一次带真实 snowflake 数字的 mock hit 走通 Runner.Run。这是一个「repo 已经把答案写在墙上，但复用没发生」的失误 —— 也直接对应 M7 里 Runner.Run 零测试覆盖的问题。修 S1 时把 M7 的 Runner.Run mock 测试一起补，等于两个问题一次解决。

---

## 交付给 Max

- (a) review 文档路径：`~/Project/Mininglamp-OSS/octo-search-indexer/docs/file-content-indexing-code-review.md`
- (b) 总判定：**BLOCK**
- (c) §1 严重问题条数：**1**（snowflake messageId 精度丢失）
- (d) 最出乎意料的发现：backfill parseHits 里 `MessageID any` + `fmt.Sprintf("%v", ...)` 让 19 位 snowflake id 变成科学计数法字符串，与 repo 已有的 `decodeObjectUseNumber` + `TestProject_SnowflakePrecision` 铁律直接冲突 —— 且 Runner.Run 零测试覆盖是这个 bug 逃逸的直接原因，修 S1 时应把 Runner.Run mock 测试（M7）一起补上，一箭双雕。

---

## §7 修复轮结论（2026-07-02 追加）

主人拍板"应修尽修"（review §2 应修 = 全部必修，nit 至少改 6 条），cc-octo 在同一分支 `feat/file-content-indexing` 追加 3 个 commit 完成修复。

### 追加 commit 列表

```
9a3231d refactor(filebackfill): json.Marshal + ratelimit thread-safety + scroll TTL floor (M3+M9+N4)
b5ca29d fix(file-extractor): defer cancel loop + CRLF sanitize + ctx classification (M1+M2+M6+M8+nits)
de5b64c fix(backfill): snowflake messageId precision + Runner.Run test coverage (S1+M7+N7)
```

### §1 严重 + §2 应修落地位置

| 编号 | 落地文件:行 | 修法一句话 |
|---|---|---|
| **S1** | `internal/filebackfill/source.go:160-200` | `MessageID json.Number` + `json.Decoder.UseNumber()`，size 字段同法处理；参考 `esindex/buildraw.go:22` 铁律 |
| **M1** | `internal/fileextract/consumer.go:81-107` | 删掉 for 循环内 `defer cancel()`，改为 `cancel()` 紧跟 Fetch 后立即释放，对齐 `internal/consumer/consumer.go:103-105` |
| **M2** | `internal/fileextract/tika.go:60-77` + 新增 `sanitizeFilename` | 剔除 CR/LF/双引号/反斜杠/控制字符；新增 `TestTika_FilenameCRLFInjection` + `TestSanitizeFilename` 回归 |
| **M3** | `internal/filebackfill/source.go:89-172` | 三处 `fmt.Sprintf` 拼 JSON 全改 `json.Marshal(map[...])`；去掉冗余 `req.Header.Set("Content-Type", ...)` |
| **M4** | `internal/filebackfill/runner.go:74-107` + Commit 6 部分 | `errors.Is(err, context.Canceled/DeadlineExceeded)` 视为优雅退出返 `(stats, nil)`，不再让 SIGTERM 造 K8s Job exit 1 |
| **M5** | `internal/fileextract/download.go` + `tika.go:100` | 删手写 `contains` funcion，改 `strings.Contains` |
| **M6** | `internal/fileextract/tika.go:73-79` + `download.go:97-104,73-76` | tika 判 `ctx.Err() != nil` 全部走 `errExtractTimeout`；download `tryFetch` 出错先判 ctx 快速返；`Fetch` 循环也识别 ctx err 不再消耗 backoff |
| **M7** | `internal/filebackfill/runner.go:23-42` + `runner_test.go` | 引入 `docExtractor` interface + `realExtractor` 适配器；补 `TestRun_HappyPath` / `TestRun_MixedOutcomes` / `TestRun_CtxCancelDuringLoop` / `TestRun_CtxDeadlineExceeded` / `TestRun_SourceRealError` + `TestParseHits_SnowflakePrecision` / `TestParseHits_MissingMessageIDSkipped` / `TestParseHits_MalformedJSONSkipped` |
| **M8** | `internal/fileextract/config.go:52-55` + `cmd/file-extractor/main.go:92-94` | 删 `DLQMaxRetries` / `DLQRetryBackoff` / `DLQSpillDir` 三个未消费字段 + 对应 env 读取；避免误导运维 |
| **M9** | `internal/filebackfill/ratelimit.go` | 加 `sync.Mutex` 保护 `tokens/last`；unlock 前不阻塞 select 保证 ctx 取消响应 |

### 顺手改的 nits

- **N1** 陈旧 IDX-3/4/5 stub 叙事全清（`config.go` doc / `consumer.go` doc / processOne 分支注释）
- **N3** `config.go:45` backoff 注释从 `1s/4s/16s` 改成 `1s/2s/4s/8s`（与 download.go:82 对齐）
- **N4** `formatDuration` < 1s floor 到 `"1s"` + `newOSScrollSource` 构造期同步 normalize；`TestFormatDuration` 用例改为 `500ms → "1s"`
- **N5** `cmd/file-extractor/main.go:96` `strings.EqualFold(..., "true")` 改 `strconv.ParseBool`，兼容 `"1"/"yes"` 等 K8s manifest 常见写法
- **N6** 删 `consumer.go` 里 `extractor == nil → stub log` 分支 + `TestProcessOne_TypeFileHitsStub` 对应测试；合并 `NewProcessor` / `NewProcessorWithExtractor` 为一个构造函数
- **N7** S1 修复一并处理：`msgID` 判空条件从 `msgID == "" || msgID == "<nil>"` 简化为 `msgID == ""`（`json.Number` 零值就是 `""`）

**未改的 nits**：
- N2（`FilePayload` export 决策）—— 涉及 API 面外部化，主人指令明确「不改」
- N8（`mapping/README.md` 版本一致性）—— 本身就是 「已检查通过」的正向记录，无需改

### 修复过程中新发现的问题

1. **`TestTika_FilenameCRLFInjection` 首轮误设 assertion**：初版把 `X-Injected` 关键字也列进 `mustNot`，实际 sanitize 只剔除结构性危险字符（CRLF/双引号/控制字符），字面「X-Injected」是普通字符，剔除 CRLF 之后就没有 header 注入面了。校正 assertion 为「只检查 CR/LF 被剔除」。这是设计边界澄清，不是新 bug —— 记录以便后续 reviewer 理解 sanitize 的语义边界。
2. **`consumer_test.go` 的 test helper `mkFileMessage` 变孤儿**：N6 删了唯一使用者 `TestProcessOne_TypeFileHitsStub`，helper 也一起删。发现时 `go build` 没报错（test-only helper 不参与 build），是 `go test` 运行后 vet 才提示的 —— 顺手删掉。

### 验收数字

- `go vet ./...` ✅ 干净
- `go build ./...` ✅ 全绿
- `go test -race -timeout 120s ./...` ✅ 全 15 个包绿
- 新增测试 8 条（S1 精度回归 + Runner.Run 5 条状态机 + parseHits 2 条 malformed 分支 + M2 CRLF injection + sanitizeFilename 单元）
- 3 commit 主题清晰单一，每 commit body 明确 review 报告条目对应关系

### 与本次 review 的关联

修复轮把「repo 铁律墙上写着但复用没发生」这类问题一次性拉齐（S1 复用 `esindex/buildraw.go:180` decodeObjectUseNumber pattern、M1 复用 `consumer/consumer.go:103-105` fetchBatch pattern、M3 复用 esindex 里 `json.Marshal` request body pattern）。下次 file-extractor / filebackfill 类新增服务时，可把这 3 处作为参照对齐点。
