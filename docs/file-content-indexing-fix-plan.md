# file-content-indexing 综合修复方案 + 具体 diff 计划

> **文档定位**：本文档是 PR #46（`feat/file-content-indexing`）在收到 4 位 reviewer CHANGES_REQUESTED 后的**修复设计文档**，覆盖 3 blocker + 10 P2 + CI Lint 修复 + commit 组织 + 部署顺序。**先出方案 Max review → 主人可选看 → 才开工改代码**。
>
> **上下文**：
> - PR: https://github.com/Mininglamp-OSS/octo-search-indexer/pull/46
> - Head SHA: `9f2ffa8`
> - 主人 2026-07-02 拍板：Blocker #2 走**方案 A（in-place bounded retry）**；Blocker #3 走**方案 A（es-indexer 改 script update 保留 content）**
> - reviewer 原文：`/tmp/pr46-review/review-{0..6}-*.md` + `comment-0-yujiawei.md`
> - 4 份姊妹文档（本文档不改动）：`file-content-indexing-feasibility.md` / `-implementation.md` / `-code-review.md` / `file-extractor-tool-comparison.md`

---

## §1 摘要（一览表）

| 编号 | 类型 | 问题 | 状态 | 处置方向 |
|---|---|---|---|---|
| **Blocker #1** | SSRF | download URL 无 scheme/host/private-IP 校验 | Max 判断 | host allowlist + SSRF-restricted transport（config 可扩） |
| **Blocker #2** | 数据丢失 | Kafka `FetchMessage` 已 advance offset，err 上抛不 commit → 后续消息 commit 越过 → 永久丢 | **主人拍板方案 A** | in-place bounded retry（照抄 `internal/consumer` state-machine pattern），N 次未成功 → DLQ `reason=retry_exhausted` |
| **Blocker #3** | 数据丢失 | es-indexer `_bulk index` full-replace，redeliver 时会覆盖 file-extractor 写的 content | **主人拍板方案 A** | es-indexer 改 `scripted_upsert` + painless 保留 `payload.file.content` + `contentMeta` |
| **P2-1** | 分类 | OS 429 归 permanent | Max 判断 | 合入 Commit 12（Blocker #2）：`classifyOSErr` 加 `429 → errOSTransient` |
| **P2-2** | 分类 | `errOSPermanent` 无 DLQ 路径 | Max 判断 | 合入 Commit 12：extractor 加 `errors.Is(uerr, errOSPermanent) → ReasonExtractError` |
| **P2-3** | 语义 | backfill `DeadlineExceeded` 与 signal 混同为 success | Max 判断 | Runner.Run 判 `ctx.Err()` 类型：DeadlineExceeded → 非零退出 |
| **P2-4** | 内存 | Tika `io.ReadAll` 无 LimitReader | Max 判断 | 合入 Commit 14：加 `io.LimitReader(resp.Body, maxContentBytes+4)` |
| **P2-5** | 语义 | `Truncated bool` + `omitempty` 无法清除 stale `true` | Max 判断 | 改 `*bool`（doc.go + 所有引用点） |
| **P2-6** | 健壮性 | `isPermanentDownloadErr` 字符串匹配 | Max 判断 | 改 sentinel error + `errors.Is` |
| **P2-7** | 语义 | `empty_extract` 只判 `== ""`，whitespace-only 漏 | Max 判断 | 改 `strings.TrimSpace(content) == ""` |
| **P2-8** | 健壮性 | backfill scroll 无 transient retry | Max 判断 | `source.Next()` 外套 bounded backoff retry |
| **P2-9** | 分类 | Tika `http.Client.Timeout` 与 ctx 混淆 | Max 判断 | 用 `context.WithTimeout` 驱动 + `errors.Is(err, context.DeadlineExceeded)` |
| **P2-10** | 部署 | file-extractor 无 startup mapping-compat 检查 | Max 判断 | 复用 `esindex.AssertLiveMappingCompatible` |
| **CI Lint** | 门禁 | 16 issues（15 errcheck + 1 staticcheck） | Max 判断 | 合入 Commit 14 顺手修 |

**新发现问题**：见 §8（1 条：`retry_on_conflict` 语义在 script update 下的行为需实证）。

**总工时预估**：**5-7 h**（含新 test + 本地 e2e，不含 push 后 CI 迭代）
- Blocker #3（script update）：2-3 h（最复杂，改 core writer + 完整回归 test）
- Blocker #2（retry state machine）：1.5-2 h（架构对齐 `internal/consumer`，可复用思路）
- Blocker #1（SSRF）：0.5-1 h（加 config + 校验层）
- 10 条 P2 + CI Lint：1-1.5 h（都是小改动）
- 本地 e2e 复测（test 环境 tika-service:9998）：0.5 h

---

## §2 Blocker 修复方案

### §2.1 Blocker #1 — SSRF 防护（Max 判断）

#### 问题重述

`internal/fileextract/payload.go:48-49` 从 Kafka 消息 raw 读 `payload.file.url`，`internal/fileextract/download.go:102` `tryFetch` 用**裸 `http.Client`** 发 GET，全程无 scheme / host / private-IP 校验。

Reviewer 引用：
- `review-0-CHANGES_REQUESTED-OctoBoooot.md:11-15`（Major SSRF）
- `review-6-CHANGES_REQUESTED-Jerry-Xin.md:7-11`（byte-verified at `9f2ffa81c911`）
- `comment-0-yujiawei.md:23`（P2 defense-in-depth）

失败输入：`type=8` 消息的 `payload.file.url = http://169.254.169.254/latest/meta-data/…` → in-cluster GET 触达 cloud metadata；或 `http://internal-service:port/…` → 内网端口扫描 / API 打点。

#### 修法（方案：host allowlist + SSRF-restricted dialer）

**双闸门设计**（layered defense）：

1. **URL 前置校验**（`download.go::Fetch` 入口，无 net I/O 前完成）：
   - scheme 必须 `https`（如需兼容 `http` 可 config 化，默认只 https）
   - host 必须在 allowlist 内（默认 `["cdn.deepminer.com.cn"]`，env `ALLOWED_DOWNLOAD_HOSTS` 可扩，逗号分隔）
   - port 校验：如 host 带 port，须与 allowlist 中登记的一致（缺 port 默认 443）

2. **Dial 时校验**（`http.Transport.DialContext` hook，防 DNS rebinding）：
   - 解析后的 IP 拒绝 private / link-local / loopback / metadata（`169.254.169.254`, `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `127.0.0.0/8`, `::1`, `fc00::/7`, `fe80::/10`, `100.64.0.0/10`, `0.0.0.0/8`, `::` unspecified）
   - Redirect 时**重新触发**闸门 1 + 闸门 2（`http.Client.CheckRedirect`）

#### diff 计划

**新文件 `internal/fileextract/ssrf.go`**（~150 行）：

```go
package fileextract

// ssrf.go — URL 前置校验 + SSRF-restricted transport。
// 双闸门：(1) URL 语法/host allowlist 前置判 (2) dial 时 IP 判 private/metadata。

var (
    errSSRFScheme    = errors.New("fileextract: url scheme not allowed")
    errSSRFHost      = errors.New("fileextract: url host not in allowlist")
    errSSRFPrivateIP = errors.New("fileextract: resolved IP is private/link-local/metadata")
)

// validateURL 前置校验 URL（scheme + host allowlist）。无 net I/O。
func validateURL(rawURL string, allowedHosts []string, allowedSchemes []string) error {
    u, err := url.Parse(rawURL)
    if err != nil { return fmt.Errorf("%w: parse: %v", errSSRFScheme, err) }
    // 校验 scheme ∈ allowedSchemes（默认 ["https"]）
    // 校验 u.Host ∈ allowedHosts（含 port 归一化）
}

// newSSRFRestrictedDialer 返回 dial 时拒 private/metadata IP 的 DialContext。
func newSSRFRestrictedDialer(base *net.Dialer) func(ctx context.Context, network, addr string) (net.Conn, error) {
    return func(ctx context.Context, network, addr string) (net.Conn, error) {
        host, _, _ := net.SplitHostPort(addr)
        ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
        if err != nil { return nil, err }
        for _, ip := range ips {
            if isBlockedIP(ip.IP) { return nil, fmt.Errorf("%w: %s", errSSRFPrivateIP, ip.IP) }
        }
        return base.DialContext(ctx, network, addr)
    }
}

// isBlockedIP 判 IP 是否在 SSRF 黑名单（private / link-local / loopback / metadata / unspecified）。
func isBlockedIP(ip net.IP) bool {
    // 覆盖 IsPrivate / IsLoopback / IsLinkLocalUnicast / IsLinkLocalMulticast / IsUnspecified
    // 加 169.254.169.254 (cloud metadata) 显式判 + 100.64.0.0/10 (CGNAT) + fc00::/7 (ULA)
}
```

**改 `internal/fileextract/config.go`**：加 `AllowedDownloadHosts []string` + `AllowedDownloadSchemes []string`，env 读 `ALLOWED_DOWNLOAD_HOSTS`（默认 `["cdn.deepminer.com.cn"]`）+ `ALLOWED_DOWNLOAD_SCHEMES`（默认 `["https"]`）。

**改 `internal/fileextract/download.go`**（`newDownloadClient` + `Fetch`）：
- `newDownloadClient` 构造 `http.Transport{DialContext: newSSRFRestrictedDialer(...)}` 注入 http.Client；`CheckRedirect` 加校验 hook
- `Fetch` 入口先调 `validateURL()`，失败 → `errDownloadFailed`（走现有 DLQ reason=download_failed 路径，不新增 reason）
- 校验失败**不重试**（scheme/host 不变，重试无意义）

#### 影响面

- 单文件新增 + 3 文件小改（download.go / config.go / config_test.go）
- **无跨模块变更**（只在 fileextract 包内）
- 无 mapping / API / 契约变更
- 环境变量新增：`ALLOWED_DOWNLOAD_HOSTS` / `ALLOWED_DOWNLOAD_SCHEMES`（cm 加），未来切内网 COS 直接改 cm 即可，代码不动

#### 回归测试计划

新增 `internal/fileextract/ssrf_test.go`（~200 行）：

| Test | 场景 | 断言 |
|---|---|---|
| `TestValidateURL_HTTPSAllowedHost` | `https://cdn.deepminer.com.cn/x.pdf` | pass |
| `TestValidateURL_HTTPRejectedByScheme` | `http://cdn.deepminer.com.cn/x.pdf` | `errSSRFScheme` |
| `TestValidateURL_UnknownHostRejected` | `https://evil.example.com/x.pdf` | `errSSRFHost` |
| `TestValidateURL_MetadataHostRejected` | `https://169.254.169.254/latest/…` | `errSSRFHost`（host allowlist 层就拦下） |
| `TestValidateURL_MalformedURL` | `://bad-url` | `errSSRFScheme` (parse err) |
| `TestSSRFDialer_PrivateIP` | mock DNS → 10.0.0.1 | `errSSRFPrivateIP` |
| `TestSSRFDialer_MetadataIP` | mock DNS → 169.254.169.254 | `errSSRFPrivateIP` |
| `TestSSRFDialer_PublicIP` | mock DNS → 1.1.1.1 | pass |
| `TestFetch_RedirectToPrivate` | 200 → 302 → `http://10.0.0.1/x` | 拒 redirect（`errDownloadFailed`） |
| `TestFetch_AllowlistViaEnv` | env `ALLOWED_DOWNLOAD_HOSTS=a.com,b.com` | 两个 host 都放行 |

#### 部署顺序 / 已知坑

- 无部署顺序约束（纯 fileextract 内变更，es-indexer / mapping 不动）
- **坑**：`net.Dialer.DialContext` 在解析 host → IP 后如 IP 从 A 变 B（DNS rebinding），必须**在真正 dial 前**重解析 + 判 IP。示例已用 `net.DefaultResolver.LookupIPAddr(ctx, host)` 显式解析后传 base.DialContext 目标 IP（而不是让 Transport 内部再解析一次），避免 TOCTOU。
- **坑**：Redirect 时 `CheckRedirect` 只能拿到 `req.URL`，须**手动重跑** `validateURL()`，Transport 层 dialer 只兜底 IP。

#### 权衡

- **不用 net.IP.IsPrivate**：Go 1.17+ `IsPrivate` 只覆盖 RFC 1918 `10./172.16-31./192.168.`，不含 CGNAT `100.64/10` / metadata `169.254.169.254` / IPv6 ULA `fc00::/7`。自定义 `isBlockedIP` 更严格。
- **不新增 DLQ reason**：`download_failed` 语义已足够覆盖 SSRF 拦截（"下载被拒"）。future 如需精细分类可加 `reason=blocked_by_ssrf`，本次不做。

---

### §2.2 Blocker #2 — Consumer 数据丢失（主人拍板方案 A）

#### 问题重述

`internal/fileextract/consumer.go:65-67`（`Run`）+ `consumer.go:105-115`（`processBatch`）+ `consumer.go:124-151`（`processOne`）：

Reviewer 引用：
- `review-4-CHANGES_REQUESTED-yujiawei.md:27-41`（P0 详解）
- `review-6-CHANGES_REQUESTED-Jerry-Xin.md:13-17`
- `review-2-CHANGES_REQUESTED-mochashanyao.md:34-61`（P1，含 429 misclassification）
- `comment-0-yujiawei.md:7-9`（**机制校正**：kafka-go `FetchMessage` 在 fetch 时执行 `r.offset = m.message.Offset + 1`，err 上抛并非 infinite loop 而是 **silent skip**）

**当前行为**：
1. `processOne` 返 `errDocNotYet` / `errOSTransient` → `processBatch` `return err` 不 commit 本条
2. `Run` 只 `log` `processBatch error` 后 continue → `fetchBatch` → 下一 `FetchMessage` 返**下一 offset**（reader 已本地 advance）
3. 下一条成功 → `source.Commit()` 提交更高 offset → **失败的那条 offset 被永久越过**
4. 一旦 group 高水位越过，重启也救不回

**根因**：`file-extractor` 的 `Run` + `processBatch` 只做「fetch → process → commit」单向流，没有 retry state machine 概念。而 `internal/consumer/consumer.go` 早已有成熟的 in-place retry 模式（`resolvePass` + `dispositions` state machine + `for attempt` loop）。

#### 修法（方案 A：in-place bounded retry，照抄 `internal/consumer` 模式）

**核心思路**：
- 引入 `itemDisposition` 三态枚举 `{transient, ok, dlqResolved}`
- `processBatch` 变成 `for attempt := 0; ; attempt++` 循环，只对仍 transient 的条目重跑
- 重试次数上限 N（默认 10）→ 达上限后**强制 DLQ** `reason=retry_exhausted`，然后 commit（避免 partition 永久阻塞）
- commit 只推进"连续成功前缀"（同 `internal/consumer::commitPrefixes`）

**关键差异 vs `internal/consumer`**：
- `internal/consumer` 的 retry 无上限（依赖 4xx→DLQ 自然收敛）；file-extractor 涉及 `errDocNotYet`（主 doc 永久缺）+ OS 5xx 长期挂等场景，**必须有上限**避免 partition 阻塞
- `internal/consumer` 是 batch bulk（`writer.Bulk`）；file-extractor 是**单条**（每条独立下载 + Tika + OS update），retry loop 粒度是**单条**，state machine 简化

#### diff 计划

**改 `internal/fileextract/consumer.go`**（主体重写 processBatch + processOne 分离）：

```go
// itemDisposition 三态：transient（未终态）/ ok / dlqResolved。
type itemDisposition int

const (
    dispTransient itemDisposition = iota
    dispOK
    dispDLQResolved
)

// Config 加字段：
type ServiceConfig struct {
    // ... 原有字段
    // MaxRetriesPerMessage 单条消息最大 in-place 重试次数（默认 10）。
    // 超过 → 强制 DLQ reason=retry_exhausted + commit offset（避免 partition 阻塞）。
    MaxRetriesPerMessage int
    // TransientBackoffBase 指数退避基（默认 1s）。1s / 2s / 4s / 8s / 16s / 32s / 60s(cap) …
    TransientBackoffBase time.Duration
    // TransientBackoffMax 单次退避上限（默认 60s，避免退避到不可接受的延迟）。
    TransientBackoffMax time.Duration
}

// processBatch 改成 in-place retry state machine。
func (p *Processor) processBatch(ctx context.Context, batch []fetchedMessage) error {
    n := len(batch)
    dispositions := make([]itemDisposition, n)
    attempts := make([]int, n) // 每条独立 attempt 计数

    for {
        if err := ctx.Err(); err != nil { return err }

        changed := false
        for i, m := range batch {
            if dispositions[i] != dispTransient { continue }

            // 退避（attempts[i]>0 才 sleep）
            if attempts[i] > 0 {
                if err := sleepCtx(ctx, expBackoff(p.cfg.TransientBackoffBase, p.cfg.TransientBackoffMax, attempts[i])); err != nil {
                    return err
                }
            }

            // 达重试上限 → 强制 DLQ retry_exhausted + 落 disposition
            if attempts[i] >= p.cfg.MaxRetriesPerMessage {
                if err := p.writeDLQ(ctx, m, ReasonRetryExhausted, "", nil,
                    fmt.Errorf("in-place retry exhausted after %d attempts", attempts[i])); err != nil {
                    return err // DLQ 硬停 → 让 Run loop 退出（K8s 重启保 offset 未推进）
                }
                p.metrics.IncRetryExhausted()
                dispositions[i] = dispDLQResolved
                changed = true
                continue
            }

            // 走原 processOne 单条处理（拆出下面）
            outcome, err := p.attemptOne(ctx, m)
            attempts[i]++
            switch outcome {
            case outcomeOK:
                dispositions[i] = dispOK
                changed = true
            case outcomeDLQ:
                // processOne 内部已投 DLQ
                dispositions[i] = dispDLQResolved
                changed = true
            case outcomeTransient:
                // 保持 transient，下轮重试
            case outcomeFatal:
                // DLQ 写失败等 → 硬停
                return err
            }
        }

        // 有条目终态 → 推进"连续成功前缀"commit
        if changed {
            if err := p.commitPrefix(ctx, batch, dispositions); err != nil {
                return err
            }
        }

        // 全终态 → 本批完
        if !hasTransient(dispositions) { return nil }
    }
}

// attemptOne 单次尝试，返回三态结果。
type attemptOutcome int
const (
    outcomeOK attemptOutcome = iota
    outcomeDLQ           // 已投 DLQ（永久失败）
    outcomeTransient     // 需重试
    outcomeFatal         // DLQ 写失败等，硬停
)

func (p *Processor) attemptOne(ctx context.Context, m fetchedMessage) (attemptOutcome, error) {
    var msg searchmsg.Message
    if err := json.Unmarshal(m.Value, &msg); err != nil {
        if werr := p.writeDLQ(ctx, m, ReasonParseError, "", nil, err); werr != nil {
            return outcomeFatal, werr
        }
        return outcomeDLQ, nil
    }
    fp, isFile := extractContentTypeFile(msg.RawPayload)
    if !isFile {
        p.metrics.IncSkippedNonFile()
        return outcomeOK, nil
    }
    p.metrics.IncProcessed()
    dlqReason, cause, err := p.extractor.ExtractAndWrite(ctx, msg.MessageID, fp)
    if err != nil {
        // errDocNotYet / errOSTransient → transient
        // errOSPermanent → 走 DLQ（P2-2 修复）
        if errors.Is(err, errOSPermanent) {
            if werr := p.writeDLQ(ctx, m, ReasonOSPermanent, msg.MessageID, fp, err); werr != nil {
                return outcomeFatal, werr
            }
            return outcomeDLQ, nil
        }
        return outcomeTransient, nil
    }
    if dlqReason != "" {
        if werr := p.writeDLQ(ctx, m, dlqReason, msg.MessageID, fp, cause); werr != nil {
            return outcomeFatal, werr
        }
        return outcomeDLQ, nil
    }
    return outcomeOK, nil
}

// commitPrefix 提交"从头连续终态"的最后一条 offset。
// 与 internal/consumer 差异：file-extractor 单分区消费（GroupID + 单 topic），无跨分区聚合。
func (p *Processor) commitPrefix(ctx context.Context, batch []fetchedMessage, dispositions []itemDisposition) error {
    lastIdx := -1
    for i := range batch {
        if dispositions[i] == dispTransient { break }
        lastIdx = i
    }
    if lastIdx < 0 { return nil }
    return p.source.Commit(ctx, batch[lastIdx])
}

// hasTransient 判是否还有未终态条目。
func hasTransient(d []itemDisposition) bool {
    for _, x := range d {
        if x == dispTransient { return true }
    }
    return false
}
```

**改 `internal/fileextract/oswriter.go`**（P2-1：429 分类）：

```go
// classifyOSErr：加 429 → errOSTransient（与 download.go 一致）
case status == http.StatusTooManyRequests: // 429
    return errOSTransient
case status == http.StatusNotFound:
    return errDocNotYet
// ... 原有分支
```

**改 `internal/fileextract/dlq.go`**（新增 DLQ reason 常量）：

```go
const (
    // ... 原有 8 个 reason
    ReasonRetryExhausted = "retry_exhausted"  // in-place retry N 次未成功
    ReasonOSPermanent    = "os_permanent"     // OS 写永久失败（非 404/409/429/5xx）
)
```

**改 `internal/fileextract/metrics.go`**（加 `IncRetryExhausted` counter）。

**改 `docs/file-content-indexing-implementation.md`**（如需同步 retry 上限 + 新 DLQ reason 到已 sig-off 任务书 v2 — 这一步待 Max 判是否要动 v2 doc，本文档只列 diff 计划）。

#### 影响面

- `internal/fileextract/consumer.go` 主体重写（~180 行 → ~250 行）
- `oswriter.go` + `dlq.go` + `metrics.go` 小改
- **无跨模块变更**（file-extractor 独立 Deploy）
- **不影响 es-indexer 主流程**
- 新增 env: `MAX_RETRIES_PER_MESSAGE`（默认 10）+ `TRANSIENT_BACKOFF_BASE` / `TRANSIENT_BACKOFF_MAX`

#### 回归测试计划

新增/改 `internal/fileextract/consumer_test.go`（~300 行新 test）：

| Test | 场景 | 断言 |
|---|---|---|
| `TestProcessOne_TransientRetryThenSuccess` | 前 2 次 mock 返 errDocNotYet，第 3 次成功 | offset 只 commit 一次且是成功那次 attempts=3 |
| `TestProcessOne_RetryExhausted` | 10 次全 errDocNotYet | 第 11 次不再调 attemptOne，DLQ reason=retry_exhausted，offset commit |
| `TestProcessOne_429IsTransient` | OS 返 429 → 第 2 次成功 | 走 retry 不是 DLQ；offset 只 commit 成功那次 |
| `TestProcessOne_OSPermanentToDLQ` | OS 返 400（非 404/409/429）| 立即 DLQ reason=os_permanent，不 retry |
| `TestProcessOne_ParseErrorToDLQ` | 消息 JSON 非法 | 立即 DLQ reason=parse_error，不 retry |
| `TestProcessBatch_OffsetPrefixCommit` | offset 100/101/102 中 101 fails first attempt | 102 不 commit 直到 101 达终态（对齐 Jerry-Xin `review-6:17` 建议） |
| `TestProcessBatch_HeadStuckDoesNotCommitTail` | offset 100 forever transient（未达上限）、101/102 立即 OK | commit 停在 100 之前（不提交），直到 100 终态 |
| `TestProcessBatch_HeadDLQResolvedAdvancesTail` | 100 达上限 → DLQ retry_exhausted，101/102 OK | commit 推进到 102（100 也算终态） |
| `TestProcessBatch_MixedDLQAndOK` | 100 OK, 101 parse_error→DLQ, 102 OK | commit 推进到 102 |
| `TestExpBackoff_MonotoneAndCap` | 单元测试退避函数 | 递增且 cap=Max，SIGTERM 立即返 |
| `TestProcessBatch_CtxCancelDuringBackoff` | Sleep 时 ctx cancel | 立即返 nil，无 goroutine 泄漏 |

#### 部署顺序 / 已知坑

- 无部署顺序约束（file-extractor 独立 Deploy，单方向兼容）
- **坑**：`internal/consumer::processBatch` 有多分区 `commitPrefixes`，file-extractor 是**单 topic 单 GroupID**，理论也是多分区，但 kafka-go `FetchMessage` 返的顺序与 partition 无关。**必须按 partition group 分别推进前缀**，否则跨分区的 head-of-line blocking 会互相干扰。**当前 diff 假设简化为单一 commit prefix**（现有 file-extractor 无跨分区聚合），若未来发现跨分区问题需照抄 `internal/consumer::partitionCommitPoints`。**这是 §8 新发现问题 #1**。
- **坑**：`MaxRetriesPerMessage=10` + `TransientBackoffMax=60s` → 单条最坏阻塞 ~10 min。生产要根据实际 OS SLA 调（业务 SLO 允许延迟范围内）。
- **坑**：retry_exhausted DLQ 后 offset 推进，**回灌需要人肉从 DLQ topic 拉回**（现有 SOP 已支持，见父群 group.md「DLQ 排查 / 回灌」）。

#### 权衡

- **不实现 Phase 2 retry-topic**（feasibility 曾提）：本 PR 范围内加 in-place retry + 硬上限已足够守住数据不丢；retry-topic 是未来独立 PR
- **不动 `internal/consumer`**：本 PR 只改 file-extractor（`internal/fileextract`），主 es-indexer 消费路径不动（Blocker #3 只碰 esindex writer，不碰 consumer）

---

### §2.3 Blocker #3 — Dual-writer overwrite（主人拍板方案 A）

#### 问题重述

`internal/esindex/writer.go:130-135`（`bulkActionLine{index}`）+ `internal/esindex/buildraw.go` `payloadTypeFile` 分支：

Reviewer 引用：
- `review-4-CHANGES_REQUESTED-yujiawei.md:43-52`（P1 详解 + `_source.excludes` ≠ 保留倒排语义）
- `review-5-COMMENTED-OctoBoooot.md:7-15`（补充 verify + at-least-once 侧写）
- `review-6-CHANGES_REQUESTED-Jerry-Xin.md:21`（non-blocking 交叉验证）

**当前行为**：
- **es-indexer** 用 `_bulk index`（`writer.go:130-135`）—— full-document 替换，同 `_id` 覆盖
- **file-extractor** 用 `_update`（`oswriter.go:67`）+ `doc_as_upsert=false` —— 只 merge `payload.file.content` + `contentMeta`
- **race**：es-indexer 写 doc → file-extractor 补 content → es-indexer 重放（rebalance / restart / retry）**再次写 doc** → `index` full-replace → **content 被抹掉**

关键：`_source.excludes` 只影响 `_source` 存储字段可见性，**不阻止** `index` action 全量替换 doc 内容。倒排索引也随 doc 覆盖失效。

#### 修法（方案 A：es-indexer 改 `_bulk update` + scripted_upsert，保留 file-extractor 写的字段）

**核心思路**：
- 把 `_bulk index` action 改成 `_bulk update` + `scripted_upsert`
- Painless script 逻辑：
  - **首次写入（doc 不存在，走 upsert）**：`ctx.op == 'create'`，用 `params.doc` 覆盖 `ctx._source`（无 preserve 需求）
  - **重复写入（doc 已存在，走 script）**：先保存 `ctx._source.payload.file.content` + `contentMeta` → 用 `params.doc` 全量替换 → 恢复保留字段

#### diff 计划

**改 `internal/esindex/writer.go`**（`bulkActionLine` + `writeBulkDoc` + `encodedSingleDocSize`）：

```go
// bulkActionLine 从 index 改成 update：
type bulkUpdateAction struct {
    Update struct {
        ID              string `json:"_id"`
        RetryOnConflict int    `json:"retry_on_conflict"`
    } `json:"update"`
}

// preservePainless 保留 file-extractor 写的 payload.file.content + contentMeta。
// scripted_upsert=true：doc 不存在时也走 script，ctx.op 会是 'create'。
const preservePainless = `
if (ctx.op == 'create') {
  ctx._source = params.doc;
} else {
  def savedContent = null;
  def savedMeta = null;
  if (ctx._source.containsKey('payload') && ctx._source.payload instanceof Map) {
    def f = ctx._source.payload.get('file');
    if (f instanceof Map) {
      savedContent = f.get('content');
      savedMeta = f.get('contentMeta');
    }
  }
  ctx._source = params.doc;
  if (savedContent != null || savedMeta != null) {
    if (!ctx._source.containsKey('payload') || !(ctx._source.payload instanceof Map)) {
      ctx._source.payload = new HashMap();
    }
    if (!ctx._source.payload.containsKey('file') || !(ctx._source.payload.file instanceof Map)) {
      ctx._source.payload.file = new HashMap();
    }
    if (savedContent != null) { ctx._source.payload.file.content = savedContent; }
    if (savedMeta != null) { ctx._source.payload.file.contentMeta = savedMeta; }
  }
}
`

// bulkUpdateBody：一条 doc 的 update+scripted_upsert body。
type bulkUpdateBody struct {
    Script          scriptBlock `json:"script"`
    Upsert          Doc         `json:"upsert"`
    ScriptedUpsert  bool        `json:"scripted_upsert"`
}

type scriptBlock struct {
    Lang   string         `json:"lang"`
    Source string         `json:"source"`
    Params map[string]any `json:"params"`
}

// writeBulkDoc 改成 write update action + scripted_upsert body。
func writeBulkDoc(buf *bytes.Buffer, d Doc) error {
    id := d.idString()
    var action bulkUpdateAction
    action.Update.ID = id
    action.Update.RetryOnConflict = 3  // 与 file-extractor 一致
    actionJSON, err := json.Marshal(action)
    if err != nil {
        return fmt.Errorf("esindex: marshal bulk action for %s: %w", id, err)
    }
    body := bulkUpdateBody{
        Script:         scriptBlock{Lang: "painless", Source: preservePainless, Params: map[string]any{"doc": d}},
        Upsert:         d,
        ScriptedUpsert: true,
    }
    bodyJSON, err := json.Marshal(body)
    if err != nil {
        return fmt.Errorf("esindex: marshal update body for %s: %w", id, err)
    }
    buf.Write(actionJSON)
    buf.WriteByte('\n')
    buf.Write(bodyJSON)
    buf.WriteByte('\n')
    return nil
}

// encodedSingleDocSize 重算：update+scripted_upsert body 比 index body 大约 ~2x
// (script 常量 + params.doc + upsert.doc)，行动行也大 ~40 → ~80 bytes。
func encodedSingleDocSize(d Doc) int {
    docJSON, err := json.Marshal(d)
    if err != nil { return 0 }
    // 动作行 ~80 字节（{"update":{"_id":"...","retry_on_conflict":3}}）
    // body 行 ≈ script const (~800 bytes) + 2×doc（params.doc + upsert）+ overhead
    return 80 + len(preservePainless) + 2*len(docJSON) + 100 + 2
}

// mapBulkResults 响应解析：action key 从 "index" 改成 "update"
// 但 update+scripted_upsert 的响应也在 items[i]["update"] 下
func bulkItemResult(id string, resp *opensearchapi.BulkResp, idx int) BulkItemResult {
    // ... 原有防御逻辑
    item, ok := resp.Items[idx]["update"]  // 从 "index" 改成 "update"
    // ...
}
```

**改 `internal/esindex/writer_test.go`**（同步 mock response 用 `"update"` key）。

**改 `internal/esindex/mapping/README.md`**（可选，说明 script update 的语义 + preserve 契约，让未来 reader 或运维知道 script 保留字段的清单）。

#### 影响面

- 🔴 **影响面 = 现网 17M docs 已存在 + 所有新写入 doc**——**是当前项目最高风险变更**
- 改动只在 `internal/esindex/writer.go`（`bulkActionLine` / `writeBulkDoc` / `mapBulkResults` / `encodedSingleDocSize`）
- **无 mapping 变更**（只是写入 op 从 index 换 update）
- 影响下游模块：`cmd/es-indexer`（消费主流程调 `Writer.Bulk`）+ `cmd/rt-backfill` + `internal/filebackfill`（如有调 `esindex.Writer` 的路径）+ 阶段 6 backfill 
- **性能影响**：Painless script 比 index 慢约 **10-20%**（`params.doc` + `upsert.doc` 双 body + script 编译缓存 + `retry_on_conflict` 语义变化）。生产 17M docs / 5s 一 batch 500 条现有基线下**需实测**（Max 前置 e2e 覆盖）
- **兼容性**：老 doc 已有的所有字段应完整保留 —— script `ctx._source = params.doc` 全量替换后**只 preserve `payload.file.content` + `contentMeta`**，其他 file-extractor 未来可能新增的字段需在 script 里显式列
- **回滚方案**：改动集中在 `writer.go`，git revert 该 commit + 重部署 es-indexer 即可。回滚窗口：已用 script update 写入的 doc 结构上兼容 index op（doc 结构没变，只是写入方式变），回滚后自动切回 index op

#### 回归测试计划

新增/改 `internal/esindex/writer_test.go`（~250 行新 test）：

| Test | 场景 | 断言 |
|---|---|---|
| `TestBulk_ScriptPreservesContent` | 模拟 doc 已含 payload.file.content='xxx' + contentMeta，es-indexer 重写同 _id | 响应 200，OS 端（mock 层校验请求 body）script 保留字段；无实 OS 时用 mock 断言 painless script 内容 |
| `TestBulk_NewDocSetsAllFields` | 首次写入 doc，走 upsert 分支 | upsert 分支执行；ctx.op=='create' 走 params.doc 全量赋值 |
| `TestBulk_OtherFieldsNotAffected` | doc 已有 content=A, extension=B；es-indexer 重写把 extension 改成 C | content 保留 A（file-extractor 写的不动），extension 从 B→C（es-indexer 主流程可覆盖） |
| `TestBulk_UpdateActionEncoding` | 单条 doc encode | action 行 == `{"update":{"_id":"...","retry_on_conflict":3}}`，body 含 `scripted_upsert:true` + `script.lang=painless` |
| `TestBulk_DerivativesUsePreserveScript` | doc 有 Derivatives（虚拟子文档） | 每个 derivative 也走 update+preserve script |
| `TestBulk_ScriptPreservesContentMetaWhenContentMissing` | doc 已含 contentMeta 但 content=nil（异常 case） | contentMeta 保留，不因为 content=nil 触发 script 短路 |
| `TestBulk_LargeDocSizeEstimateUpdated` | 单 doc 编码字节数估算 | `encodedSingleDocSize` 返回值反映 update body 的膨胀，subBatchEnd 按新估算切子批 |
| `TestBulkResults_UpdateResponseKey` | mock BulkResp items[i]["update"] | `bulkItemResult` 从 "update" key 读 status |
| `TestBulk_Idempotency` | 同 doc 写 3 次 | 每次都成功，content preserve 保持一致 |
| `TestBulk_ScriptSyntaxSmoke` | 单元测试 painless script 常量非空且格式合法 | 简单字符串校验（script const 非 truncated） |
| **`TestBulk_E2EAgainstOSMock`** | **在测试环境 OS test cluster 跑一遍真实 script**（Max 后续 e2e） | doc create → file-extractor update content → es-indexer bulk update → content 保留 |

#### 部署顺序（**关键**）

🔴 **Blocker #3 的 es-indexer 改动必须先于 file-extractor 上线**：

1. **T-2h**：mapping v1.12 PUT to **test**（若未做，本 PR 已含 v1.12 mapping 已就绪）
2. **T-1h**：**es-indexer 升级 test**（含 script update）→ 观察 15 min gating（DLQ 无暴增 / OS 5xx 率 / latency）
3. **T-0h**：file-extractor 首次 apply test → gating 观察 30 min
4. **T+1h**：DLQ 采样 / OS content 字段抽验 / 搜索 API smoke → ✅ → **prod 部署同顺序**
5. **prod 顺序**：mapping 已 v1.12 (2026-06-27 已上线) → es-indexer 升 → file-extractor apply → gating

若 es-indexer 后于 file-extractor 上线：**旧 es-indexer 会覆盖 file-extractor 写的 content**，本 PR 修复失效 → 触发 Blocker #3 原 bug。

#### 已知坑 / 权衡

- **坑**：Painless script 每次 update 都会**编译**（除非 hit script cache）。OS 默认 script cache = 100 slots，本 PR 的 script 是**参数化常量**（`preservePainless` 字符串每次一样，`params.doc` 用参数注入），会 hit cache。**要确认**：script 常量 hash 稳定，不会因序列化差异 miss cache。
- **坑**：`retry_on_conflict=3` 在 update + scripted_upsert 下的语义是**版本冲突时重试整个 script**（不是重试单个 field）。file-extractor 的 update 也用 `retry_on_conflict=3`。两个 writer 并发写同 doc 时的最坏 case 是 3+3=6 次重试，OS 5xx 概率上升需实测。
- **坑**：Painless 里 `ctx._source = params.doc` **不能直接赋值**（`ctx._source` 是 Map，Painless 允许赋值给 Map 类型的字段，但需要 `params.doc` 类型也是 `Map`）。JSON 反序列化默认 `params.doc` 就是 `Map<String, Object>`，OK。**要验证**：test 环境用真实 OS 3.6 跑一遍确认 script 语法 pass（Max 后续 e2e 承担）。
- **权衡**：**不用 `_bulk update`+`doc:{}`+`doc_as_upsert:true` 模式**（更简单但无 script）—— 该模式下 update body 的 `doc:{}` 是 partial merge，file-extractor 写的 content 会被 es-indexer 的 partial 覆盖（如果 es-indexer 的 doc 也带 `payload.file` 子对象）。方案 A 用 script 完全掌控 preserve 语义，是唯一保证 content 不丢的路径。
- **权衡**：**不改 doc structure / mapping**——只改写入 op，doc 字段照旧，兼容性最好，回滚容易。
- **权衡**：**不缩小 script 到只 preserve 字段的 patch update**——考虑到 es-indexer 主流程可能新增字段的场景，全量 replace + preserve 更符合"es-indexer 拥有 doc 全量、file-extractor 只补 content 子字段"的所有权语义。

---

## §3 P2 修复清单（10 条）

| P2 | 位置 | 改法 | 归属 commit | 是否有回归 test |
|---|---|---|---|---|
| **P2-1** OS 429 → transient | `internal/fileextract/oswriter.go::classifyOSErr` line 105-115 | `case status == http.StatusTooManyRequests: return errOSTransient` 加在 404 分支之前 | Commit 12（Blocker #2） | ✅ 已列 `TestProcessOne_429IsTransient` |
| **P2-2** errOSPermanent → DLQ | `internal/fileextract/consumer.go::attemptOne` + `dlq.go` | 加 `ReasonOSPermanent` 常量；attemptOne 判 `errors.Is(err, errOSPermanent) → DLQ` | Commit 12（Blocker #2） | ✅ 已列 `TestProcessOne_OSPermanentToDLQ` |
| **P2-3** backfill DeadlineExceeded vs signal | `internal/filebackfill/runner.go::Run` line 95-104 | 拆判：`ctx.Err()==context.DeadlineExceeded` → return `stats, ErrTimeout`（新 sentinel）；`context.Canceled` → 保留 `nil` | Commit 14（P2） | ✅ 新增 `TestRunner_TimeoutDistinctFromCancel` |
| **P2-4** Tika LimitReader | `internal/fileextract/tika.go` line 96-97 | `body, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(t.maxContentBytes)+4))` | Commit 14（P2） | ✅ 新增 `TestTika_LimitReadResponse` |
| **P2-5** Truncated *bool | `internal/esindex/doc.go` line 148-153 `FileContentMeta.Truncated bool` → `*bool`；所有引用点 | 改 struct 字段类型 + `extractor.go:100-105` 构造用 `truncatedPtr := truncated; meta.Truncated = &truncatedPtr` | Commit 14（P2） | ✅ 新增 `TestFileContentMeta_TruncatedCanTransitionToFalse` |
| **P2-6** sentinel error | `internal/fileextract/download.go` line 118 (`cdn permanent status`) + line 138-145 `isPermanentDownloadErr` | 定义 `errCDNPermanent = errors.New(...)`；`tryFetch` 用 `fmt.Errorf("%w: status %d", errCDNPermanent, status)`；`isPermanentDownloadErr` 改 `errors.Is(err, errCDNPermanent)` | Commit 14（P2） | ✅ 新增 `TestIsPermanentDownloadErr_UsesSentinel` |
| **P2-7** empty_extract TrimSpace | `internal/fileextract/extractor.go` line 96 | `if strings.TrimSpace(content) == ""` | Commit 14（P2） | ✅ 新增 `TestExtractor_WhitespaceOnlyIsEmptyExtract` |
| **P2-8** backfill scroll retry | `internal/filebackfill/runner.go::Run` line 105-107 | 抽出 `sourceNextWithRetry(ctx, retries=3, backoff=exp)`，OS 5xx 类 retry；permanent 错继续 return | Commit 14（P2） | ✅ 新增 `TestRunner_ScrollTransientRetry` |
| **P2-9** Tika timeout ctx-driven | `internal/fileextract/tika.go::newTikaClient` + `Extract` | 去掉 `http.Client.Timeout`；`Extract` 内 `perReqCtx, cancel := context.WithTimeout(ctx, t.timeout); defer cancel()`；分类 `errors.Is(perReqCtx.Err(), context.DeadlineExceeded) → errExtractTimeout`；`errors.Is(err, context.Canceled) && ctx.Err() != nil → 上抛 ctx 取消` | Commit 14（P2） | ✅ 新增 `TestTika_TimeoutFiresDeadlineExceeded` + `TestTika_ParentCancelDistinctFromTimeout` |
| **P2-10** file-extractor startup mapping-compat | `cmd/file-extractor/main.go` startup 前加 `esindex.NewWriter(cfg).AssertLiveMappingCompatible(ctx)` 复用（Writer 内部包 client + Index）；失败 → loud crash | Commit 14（P2） | ✅ 新增 `TestMain_AssertsMappingBeforeStart`（e2e level，可 skip 本地跑） |

**新增 DLQ reason 汇总**（写进 `dlq.go`）：
- `ReasonRetryExhausted = "retry_exhausted"`（Blocker #2）
- `ReasonOSPermanent = "os_permanent"`（P2-2）

---

## §4 修复 commit 组织

追加 4 个 commit，保持每 commit 独立可 revert：

| # | Commit message | 变更范围 | 依赖 |
|---|---|---|---|
| 11 | `fix(esindex): scripted_upsert to preserve file-extractor written fields (Blocker #3)` | `internal/esindex/writer.go` + `writer_test.go` + mapping/README.md | 无依赖，最先出（部署要求先上） |
| 12 | `fix(file-extractor): in-place retry + DLQ 429/permanent (Blocker #2 + P2-1 + P2-2)` | `internal/fileextract/{consumer,oswriter,dlq,metrics,config}.go` + `consumer_test.go` | 无依赖 |
| 13 | `fix(file-extractor): SSRF host allowlist + private-IP block (Blocker #1)` | `internal/fileextract/{ssrf,download,config}.go` + `ssrf_test.go` | 无依赖 |
| 14 | `fix: P2 cleanup (Tika timeout/LimitReader/Truncated ptr/backfill retry/mapping-compat/sentinel err/empty_extract/CI lint)` | `internal/fileextract/{tika,download,extractor}.go` + `internal/esindex/doc.go` + `internal/filebackfill/runner.go` + `cmd/file-extractor/main.go` + tests + CI lint errcheck/staticcheck fixes | 无依赖 |

**顺序建议**（也可并行开工，但**push 前**按此顺序调整）：11 → 12 → 13 → 14（部署要求 11 先上）

**关键要求**：每 commit 独立可 revert（reviewer re-review 时能定位问题；如某 commit 有问题只 revert 该 commit 不牵一发动全身）

---

## §5 测试策略

### 新增 test case 汇总

| 归属 commit | 新增/改动 test 文件 | 新 test case 数 |
|---|---|---|
| Commit 11 (Blocker #3) | `internal/esindex/writer_test.go` | 11 条（详见 §2.3 表） |
| Commit 12 (Blocker #2) | `internal/fileextract/consumer_test.go` | 11 条（详见 §2.2 表） |
| Commit 13 (Blocker #1) | `internal/fileextract/ssrf_test.go`（新文件） | 10 条（详见 §2.1 表） |
| Commit 14 (P2) | 各文件散 test | 10 条（每 P2 一条 + 1 条 backfill timeout test） |

**总计**：~42 条新 test case，预估覆盖新增逻辑 90%+

### e2e 复测（Max 承担）

在 test 环境（dmwork-test / tika-service:9998）跑一遍：

1. **老 5 份文件**（沿用之前实测）
2. **新增 scenario**：
   - "故意让 OS 返 429" → 断言走 retry + 最终 OK（`kubectl exec` 手动降 OS 副本模拟）
   - "故意 delete 主 doc 触发 errDocNotYet" → 断言 retry N 次后 DLQ retry_exhausted
   - "故意让 es-indexer 重放" → 断言 file-extractor 写的 content 未丢（Blocker #3 修复验证）
   - "故意让 URL 指向 metadata IP" → 断言 SSRF 拦截（Blocker #1 修复验证）
3. **性能基线复测**：
   - script update 引入的 latency 变化（p50/p99）
   - OS 5xx 率变化（`retry_on_conflict` 语义变化后）

---

## §6 CI 修复

### Lint 失败详情（`gh api repos/Mininglamp-OSS/octo-search-indexer/actions/jobs/84737633961/logs`）

**16 issues：15 errcheck + 1 staticcheck**

| 位置 | 类型 | 修法 |
|---|---|---|
| `cmd/file-extractor/main.go:92` | errcheck `strconv.ParseBool` | `_` 换成实名 var，判 err |
| `internal/esindex/file_content_test.go:98` | errcheck `json.Marshal` | 加 `require.NoError(t, err)` |
| `internal/filebackfill/runner_test.go:110` | errcheck `rl.Wait` | 加 `require.NoError(t, err)` |
| `internal/filebackfill/source.go:144` | errcheck `resp.Body.Close` | `defer func() { _ = resp.Body.Close() }()` |
| `internal/filebackfill/source.go:145` | errcheck `io.ReadAll` | 显式判 err |
| `internal/filebackfill/source.go:228` | errcheck `s.client.Client.Perform` | 显式判 err |
| `internal/fileextract/consumer_test.go:79` | errcheck `json.Marshal` | `require.NoError` |
| `internal/fileextract/consumer_test.go:85` | errcheck `json.Marshal` | `require.NoError` |
| `internal/fileextract/download.go:115` | errcheck `resp.Body.Close` | `defer func() { _ = resp.Body.Close() }()` |
| `internal/fileextract/idx4_test.go:30/52/127` | errcheck `w.Write` | `_, _ = w.Write(...)` |
| `internal/fileextract/idx4_test.go:342` | errcheck `ExtractAndWrite` | 显式判 err |
| `internal/fileextract/idx4_test.go:365` | errcheck `io.ReadAll` | 显式判 err |
| `internal/fileextract/tika.go:96` | errcheck `resp.Body.Close` | `defer func() { _ = resp.Body.Close() }()` |
| `internal/fileextract/consumer.go:81` | staticcheck ST1023 | `var fetchCtx context.Context = ctx` → `fetchCtx := ctx` |

**统一放到 Commit 14 里**（P2 cleanup），全是本项目引入的（`--new-from-patch` diff-only lint 门禁）

### 其他 CI check 说明

- `check-sprint / check-sprint`：**maintainer sprint-field gate**（governance，非代码），需 maintainer 手动清除
- `code-review`：**已 review 且 requested changes**（4 位 reviewer 已 CR），re-request review 前先修完再 dismiss stale
- 其他 `Build/Test/Vet/CodeQL/osv-scan/secret-scan` 全 pass ✅

---

## §7 部署顺序（合并后）

### test 环境

```
Step 1  |  确认 mapping v1.12 已在 test（现状已上线，2026-06-27）
Step 2  |  es-indexer 升级 test（Commit 11 script update）→ gating 15 min
        |  · 检查：DLQ 无暴增 / OS 5xx 率 < 0.1% / p99 latency < 500ms
Step 3  |  file-extractor 首次 apply test（含 Commit 12/13/14）→ gating 30 min
        |  · 检查：抽取成功率 / DLQ 分布 / 无 SSRF 拦截误报
Step 4  |  DLQ 采样 + OS content 字段抽验（挑 10 条最新写入 doc） + 搜索 API smoke（关键字搜到）
Step 5  |  ✅ → prod 上线
```

### prod 环境

同 test 顺序，mapping v1.12 已 2026-06-27 上线，直接从 Step 2 开始。

**回滚触发条件**：
- OS 5xx 率 > 1%（sustained 5 min）
- p99 latency > 2s
- DLQ 异常暴增（>10x 基线）
- 搜索 API 命中率显著下降

**回滚步骤**：
1. `git revert <commit-11>` + 重部署 es-indexer（file-extractor 与 mapping 保持不动）
2. Commit 12/13/14 独立可回滚，按需 revert

---

## §8 已识别的风险

### 新发现问题（不在 4 位 reviewer 报的 3 blocker + 10 P2 里）

**#1 file-extractor 多分区 commit prefix 未按 partition group 聚合**

- **位置**：Blocker #2 修复中的 `commitPrefix` 函数
- **描述**：`internal/consumer::commitPrefixes` 有多分区聚合（`partitionCommitPoints`）；file-extractor 单 topic 单 GroupID 也是多分区消费，但 kafka-go `FetchMessage` 返消息顺序与 partition 无关。若一批中混含多个 partition 的消息，只按"从头连续前缀"提交会**误跨 partition** commit。
- **当前 file-extractor 现状**：BatchSize=50，每条独立 commit，无跨消息前缀概念，所以现有 bug 不受此影响；但 Blocker #2 修复引入 batch retry 后，如果 batch 内混多 partition，`commitPrefix` 需按 partition group 分别推进最大连续前缀。
- **建议**：Blocker #2 修复直接按 partition group 聚合（照抄 `internal/consumer::partitionCommitPoints`），一次做到位，避免二次改动
- **主人是否需 sig-off**：不需要（是 Blocker #2 方案 A 的实施细节，不改方案方向）
- **本文档 §2.2 已在"已知坑"提及，实施时按此处理**

### Painless script 兼容性

- OS 3.6.0 版本 painless 语法支持已确认（3.x 一路兼容，语法与 ES 8.x 基本一致）
- 生产 test 前用 mock unit test 覆盖 script 常量字段格式；实测在 Max 后续 e2e 阶段承担（无 real OS 单测环境）
- **风险**：`ctx._source = params.doc` 全量替换在极老 OS 版本可能不支持（本项目 3.6.0 支持）

### 性能损耗

- script update vs index：预估 10-20% latency 上升 + OS CPU 占用上升 ~5-10%
- 现网 5s/batch 500 条 → 单 batch 编码字节从 ~500KB → ~1MB（script 常量 + 双 doc），仍远低于 `maxBulkBodyBytes=50MB` 阈值
- **风险**：极端场景（一批全大 doc，含 `payloadRaw` ~1MB×500 = 500MB） → 触发 `subBatchEnd` 按字节切子批更频繁；unit test `TestBulk_LargeDocSizeEstimateUpdated` 覆盖此路径

### 老 doc 已有字段的完整保留

- Painless script `ctx._source = params.doc` 是**全量替换**，只 preserve `payload.file.content` + `contentMeta` 两字段
- 如果 es-indexer 老 doc 里有 file-extractor 未来打算写的其他 payload.file.* 字段（例如 `payload.file.previewURL`），本方案会丢
- **当前不构成风险**（file-extractor 只写 content + contentMeta），但如未来 file-extractor 扩字段需**同步扩 script preserve 清单**
- **建议**：script 里 preserve 的字段清单加注释 + 加一条 test `TestBulk_PreserveContractDocumented` 断言 script 常量含指定字段名（未来扩字段时测试失败提醒同步 script）

### `retry_on_conflict` 语义

- update + scripted_upsert 下的 `retry_on_conflict=3` 表示 OS 检测到 version conflict 时重试整个 script
- file-extractor `oswriter.go` update 也用 `retry_on_conflict=3`
- 两 writer 并发写同 doc 时最坏 case 是 3+3=6 次重试
- **风险**：如生产实测发现冲突率高（>1%），需评估是否降 `retry_on_conflict` 或引入 external optimistic locking（`_seq_no` + `if_seq_no`），本 PR 不做

### `_source.excludes` 与 script update 的交互

- `_source.excludes: ["payload.file.content"]` 语义 = 从 `_source` 存储字段剔除
- Painless script `ctx._source.payload.file.content` 是**索引前** `_source` 的镜像
- 顺序：write → script 处理 `ctx._source` → `_source.excludes` 剔除 → 索引 → **倒排索引里 content 仍在**，但 `_source` 里没
- **确认无兼容性问题**（`_source.excludes` 与 script 处理 `_source` 阶段无冲突）

### 已由 mochashanyao P2 提出但暂不修复的项

- **Kafka reader/DLQ writer 无 TLS/SASL**（`review-2:79-81`）：全仓统一缺失，非本 PR 引入，push GitHub 前**主人 sig-off 决定**是否本 PR 覆盖；建议独立 PR

---

## 完成清单交底

- **(a) 文档路径**：`~/Project/Mininglamp-OSS/octo-search-indexer/docs/file-content-indexing-fix-plan.md`
- **(b) 3 blocker + 10 P2 diff 计划复杂度评估**：
  - Blocker #1 (SSRF)：**简单**（新 1 文件 + 3 文件小改，逻辑独立）
  - Blocker #2 (retry)：**中等**（consumer.go 主体重写，需重设计 state machine + 10+ 条新 test）
  - Blocker #3 (script update)：**复杂**（涉及 es-indexer core writer + Painless script + 部署顺序 + 性能实测；改动物理小但风险最大）
  - 10 条 P2：**简单**（每条 < 30 min，其中 P2-9 Tika timeout 稍复杂 ~1h）
- **(c) 预估修复总工时**：**5-7 h**（详见 §1 分解）
- **(d) 新发现问题**：**1 条**（§8 #1 file-extractor 多分区 commit prefix），已合入 Blocker #2 diff 计划，不需主人拍板
- **(e) Painless script 熟悉度**：**中等**——语法框架清晰（`ctx._source` / `params.doc` / `ctx.op`），生产版本 OS 3.6.0 兼容性已确认；细节实测在 Max e2e 阶段兜底。**不需要 Max 帮查文档**，如实际 diff 时遇到 corner case（例如 `containsKey` 对 nested Map 的行为）再来 Max 讨论。

---

## §9 待 Max review 的关键点

1. **§2.3 Painless script 语法细节**（`preservePainless` 常量）——请 Max 判断是否需要主人再次 sig-off，还是属于"已 sig-off 方案 A 的实施细节"直接推进
2. **§2.2 in-place retry 上限 = 10 + backoff base=1s max=60s**——参数是否合理，需要根据 test 环境实际 OS latency 调
3. **§4 commit 组织顺序（11→12→13→14）**——是否符合 review 友好性预期
4. **§8 已识别但暂不修复的 TLS/SASL**（mochashanyao P2）——是否本 PR 覆盖 or 独立 PR
5. **§2.3 部署顺序 T-2h/T-1h/T-0h 是否 realistic**——test 环境 gating 时长（15/30 min）是否够

Max review 通过后，cc-octo 按 §4 commit 顺序逐 commit 开工，本地 e2e 跑通后 push GitHub 前停报主人 sig-off。
