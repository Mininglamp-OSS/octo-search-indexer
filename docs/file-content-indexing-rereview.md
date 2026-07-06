# File Content Indexing — v1.13 修复轮独立复审报告

> Reviewer: cc-octo（独立 fresh-context 视角，非修复轮作者）
> Date: 2026-07-02
> 范围: `feat/file-content-indexing` 分支相对 `origin/main` (`0970dd8`) 领先的 5 commit 中，除 first commit `9f2ffa8`（已在 PR #46 里 review 过）之外的：
> - `b327642` docs: sync implementation.md v3 with fix-round changes
> - `2f973f0` fix: P2 cleanup + CI lint
> - `0b754aa` fix(file-extractor): SSRF host allowlist + private-IP block (Blocker #1)
> - `51a7e25` fix(file-extractor): in-place bounded retry + DLQ 429/permanent (Blocker #2 + P2-1 + P2-2)
> - `2535448` fix(esindex): scripted_upsert to preserve file-extractor written fields (Blocker #3)

## §0 总判定

**APPROVE WITH NITS**

3 个 blocker 修复方向正确、代码扎实、回归 test 覆盖足够，逻辑上均经受得住反面构造。10 P2 逐条落地、CI Lint 清理。发现的问题都在 §2 应修（P2）与 §3 nits 层面，**无 §1 严重问题需修**；但 §6 e2e 未验点较多（Painless script、OS 3.6 兼容、prod DNS SSRF 路径等），push GitHub 前 Max 需确认 e2e 计划。

## §1 严重问题（BLOCK — 必须修才能 push）

**无**。

三个 blocker 修复的核心正确性（silent skip 消除 / SSRF 双闸门 / preserve script）我都做了反面构造，均未构造出破坏的输入：

- **Blocker #2**：模拟 batch=[100(OK),101(transient),102(OK)] → commit 顺序确定；forced DLQ 后 disposition 变 dispDLQResolved 触发 partitionCommitPoints 前缀推进；无 silent skip。
- **Blocker #1**：模拟 URL 走 metadata IP、URL host 走白名单外域、redirect 到 evil host、CGNAT/IPv6-ULA/link-local IP — 均被闸门 1 或闸门 2 拒绝；TOCTOU 由 dial 时 resolve + 用 resolved IP 直连闭合。
- **Blocker #3**：模拟 doc 已存在（file-extractor 已写 content）+ es-indexer redeliver 场景 → savedContent/Meta 保留 → 全量替换 → 恢复；模拟 type=1 text 消息 doc（无 file 字段）→ savedContent=null → 不进恢复分支 → 不凭空写 payload.file。

## §2 应修问题（P2 — 强烈建议但不 block push）

### §2.1 `cmd/file-extractor/main.go:101-119` `loadConfig` 未把 v1.13 新增 config field 挂到 env

**问题**：`ServiceConfig` v1.13 新增 6 个字段全部 hardcode 走 default，`loadConfig` 未从 env 读取：
- `MaxRetriesPerMessage` / `TransientBackoffBase` / `TransientBackoffMax`（Blocker #2 引入）
- `AllowedDownloadHosts` / `AllowedDownloadSchemes` / `SSRFAllowLoopback`（Blocker #1 引入）

**影响**：生产要调 SLA（比如把 MaxRetries 从 10 降到 5）或 future 切内网 COS 扩展 host 白名单时，只能改代码 + 走整个发版流程；不是 hot-config。commit 0b754aa message 明确写"future 切内网 COS 时通过 env ALLOWED_DOWNLOAD_HOSTS 扩展"，但 env 挂载没跟上。

**严重性理由**：功能上正确（default 与生产需求一致），但**违背文档承诺** + 运维/演进灵活性降低。SSRFAllowLoopback 尤其危险：future 若因某种 test/debug 需要临时打开，因 env 未挂载会诱导直接改代码 push——比 env 开关的可审计性差。

**建议修法**：`loadConfig` 追加：
```go
MaxRetriesPerMessage:  envInt("EXTRACTOR_MAX_RETRIES_PER_MESSAGE", 10),
TransientBackoffBase:  time.Duration(envInt("EXTRACTOR_TRANSIENT_BACKOFF_BASE_MS", 1000)) * time.Millisecond,
TransientBackoffMax:   time.Duration(envInt("EXTRACTOR_TRANSIENT_BACKOFF_MAX_MS", 60000)) * time.Millisecond,
AllowedDownloadHosts:  splitCSV(os.Getenv("ALLOWED_DOWNLOAD_HOSTS")),
AllowedDownloadSchemes: splitCSV(os.Getenv("ALLOWED_DOWNLOAD_SCHEMES")),
// SSRFAllowLoopback 显式**不挂 env**（生产绝不该打开），加注释说明测试专用只能通过 test cfg 注入
```

---

### §2.2 `internal/fileextract/consumer.go:193-197` `ReasonRetryExhausted` DLQ 记录缺失 `messageID` + `fp`

**问题**：`processBatch` 强制 DLQ 时调 `writeDLQ(ctx, m, ReasonRetryExhausted, "", nil, ...)` — 传入 `messageID=""` + `fp=nil`。DLQ 记录里 `MessageID / FileURL / FileExt / FileSize` 全空。

**影响**：运维查 DLQ 排障 retry_exhausted 类记录时，只能通过 Kafka `Key`（原生是 messageID 字节）反查源，或去解 `Value` bytes JSON 找 messageID —— 不如同批次 `os_permanent`（正常 attemptOne 路径填 messageID+fp）好排障。回灌工具要多写一层解析。

**严重性理由**：不是数据丢失（Key + Value 都在），但**运维体验下降 + DLQ record schema 一致性受损**。processBatch 层不解析 msg 是为了减少重复工作，可以接受，但这块**应加注释显式说明**"messageID/fp 空是有意为之，回灌用 Kafka Key 反查"。

**建议修法**（二选一，任一 OK）：
1. **(推荐)** 在 processBatch retry-exhaust 分支之前 lazy-unmarshal 一次拿 messageID/fp 传给 writeDLQ；避免每次 attemptOne 重复解析可以缓存在 batch 结构里（`batch[i].parsedMsg` 或类似）。
2. 保持现状 + 在 dlq.go 加醒目注释 + writeDLQ 里 messageID 空时从 `m.Key` fallback 填 `MessageID: string(m.Key)`（Kafka key = messageID 是本项目约定）。

---

### §2.3 `internal/fileextract/download.go:180-184` `isPermanentDownloadErr` 保留字符串 `strings.Contains` 兜底，违背 P2-6 fix 声称

**问题**：P2-6 commit message + doc v3 changelog #6 均声称"用 sentinel + errors.Is，无字符串耦合"。实际代码：
```go
if errors.Is(err, errCDNPermanent) {
    return true
}
return strings.Contains(err.Error(), "cdn permanent status")
```
sentinel 优先 + 字符串兜底。注释说"兼容旧路径"，但整个 codebase 里现在只有 `tryFetch` 一处产生这类 err，且已改 sentinel — 没有"外部构造 err 字符串"的实际来源。

**影响**：doc-code inconsistency（doc 说"重构风险已消除"，代码里字符串匹配依然是重构风险）。若未来有人重构 `errCDNPermanent.Error()` 的字符串（例如加 i18n），依赖字符串兜底的路径会静默把 4xx 归成 transient，就是 P2-6 声称已消除的 bug 复现。

**严重性理由**：目前无实际 bug（sentinel 分支已覆盖所有生产路径），但**违背 P2-6 fix 的初衷 + doc/code 不一致**。属于代码卫生问题。

**建议修法**：删掉 `strings.Contains` 兜底行，只留 sentinel 判断；本轮 test `TestIsPermanentDownloadErr_UsesSentinel` line 33-36 里"legacy string-form"这条 case 也一起删。或者，如果确有必要保留字符串兜底，请在 commit message / doc v3 changelog 里改口径为"sentinel 优先 + legacy 字符串兜底（future 可清）"。

---

### §2.4 `internal/esindex/scripted_upsert_test.go:74-98` `TestPreservePainless_ContainsAllPreservedPaths` 契约锁死方向弱、单向

**问题**：契约 test 只按 `preservedFilePaths` 里每条 path 取叶字段名（"content" / "contentMeta"），断言 `strings.Contains(preservePainless, leaf)`。有两个弱点：
- **假阳性风险**：叶字段名是通用英文词（"summary" / "content" / "meta"），可能凭空 match 到 script 里的 comment 或不相关代码，误 pass。
- **单向锁死**：只检查"数组里的字段都在 script 里"，不检查"script 里 preserve 目标必须都在数组里"（有反向断言但只查了"payload" + "file" 顶层，粒度太粗）。若有人手工在 script 里加个 preserve `summary` 但没加到 `preservedFilePaths` 数组，test 会 pass。

**影响**：contract test 的价值是"防漂移"，双向断言才是真的锁死。目前的 test 只挡"改数组不改 script"这一方向，反向不管。低概率但真会出现的 case：someone 加 script preserve field 忘记登记到数组 → 未来 review 那个字段是不是要保留时无源可查。

**严重性理由**：不是当前 bug（当前 preserve 字段和数组完全对齐），是 test 质量问题。修复成本很低。

**建议修法**：
1. 反向断言：从 script 里抽出所有 `ctx._source.payload.file.<X>` 出现的 X（可以用简单 regex `ctx\._source\.payload\.file\.(\w+)`），要求每个 X 都是 `preservedFilePaths` 里某 path 的叶字段名。
2. 或用更结构化的替代：把 `preservePainless` 参数化成 Go 模板，从 `preservedFilePaths` 生成，从根本上锁死（改动量大，权衡）。

---

### §2.5 `internal/fileextract/consumer.go:266-281` `attemptOne` 在 `ctx.Canceled` 时误分类为 `outcomeTransient`

**问题**：当 ctx cancel 发生在 attemptOne 内部（例如 download 中 `select case <-ctx.Done()`），extractor 层返回 `ctx.Err()`；attemptOne line 266 判 err 非 nil，走 line 268 `errors.Is(err, errOSPermanent)` false → line 276 `errors.Is(err, errDocNotYet)` false → 落到 line 280 `return outcomeTransient, nil`。

**影响**：
- ctx.Canceled 被视为 transient → 该条目 `dispositions[i]` 保持 transient + `attempts[i]++`。
- 下一轮 processBatch 开头 line 182 `ctx.Err() != nil` 检测到并 `return err`；Run 层 line 107 判 `ctx.Err() != nil` 返 nil 优雅退出。
- **正确性无损**（不会 silent skip，未 commit 的 offset 保持未 commit，K8s 重启后从旧 offset 重取），但 metrics 层面 `attempts[i]++` 虚增；若 ctx cancel 恰好发生在最后一条消息、且这条一直被视为 transient 直到最终真的走完 max retries，会浪费 DLQ 空间。

**严重性理由**：**不是数据丢失、不是 blocker**；是"ctx cancel 应尽早分流"的 nice-to-have。改动成本极小。

**建议修法**：attemptOne 在检测 err 时先判 ctx.Err：
```go
if err != nil {
    if ctx.Err() != nil {
        return outcomeTransient, nil // 保持 disposition，让 processBatch 外层 ctx check 优雅退出
        // 或者更明确：新增 outcomeCanceled 类型，让 processBatch 立刻 return nil
    }
    if errors.Is(err, errOSPermanent) { ... }
    ...
}
```
或添加 outcome case 显式处理 canceled。

---

### §2.6 `internal/fileextract/ssrf_test.go` 缺 IPv4-mapped IPv6 用例断言

**问题**：`TestIsBlockedIP_CoversAllTargets` 只测了纯 IPv4 (`169.254.169.254`) 和纯 IPv6 (`fe80::1` / `fc00::1`) 场景。没测 IPv4-mapped-in-IPv6 形式（`::ffff:169.254.169.254` / `::ffff:10.0.0.1`）。

**分析**：Go stdlib `net.IP.IsLinkLocalUnicast()` 内部先 `To4()`，所以 `::ffff:169.254.169.254` 会被判为 link-local → `isBlockedIP` 会返 true。**功能上是对的**。但没 test 覆盖，属于攻击面盲点：一旦 Go stdlib 行为变化（极小概率）或者未来自己重写 isBlockedIP 时不当心，会引入回归。

**严重性理由**：**当前无 bug**（Go 行为正确），test 补充成本很低。

**建议修法**：在 blocked 列表补 3-4 条 IPv4-mapped 用例（`::ffff:169.254.169.254`、`::ffff:10.0.0.1`、`::ffff:127.0.0.1`）+ 断言拦截。

## §3 Nits（可选跟进）

### §3.1 `internal/fileextract/backoff.go:37` `rand.Int63n` 无显式 seed

Go 1.20+ 全局 rand 自动 seed，行为正确。已有 `//nolint:gosec` 注释说明"抖动非安全用途"。建议进一步注释一行"依赖 Go 1.20+ 自动 seed"，避免读者以为需要 seed。

### §3.2 `internal/esindex/writer.go:336-344` `encodedSingleDocSize` 上限估算稍保守

新估算公式 `80 + preservePainless_len + 2×docJSON + 120 + 2`；实际单 doc `_bulk` body 结构 `action_line + body_line` 约为 `50 + 60 + preservePainless_len + 2×docJSON`。估算比实际略大 (~100 bytes/doc)，会让 `subBatchEnd` 切子批稍激进（每子批少几条）。**不是 bug**，效率略降但保守方向正确。若关注可精细化到 `+ 50 + 60`（省 ~90 bytes overhead）。

### §3.3 `internal/fileextract/consumer.go:298-308` `processOne` 兼容 wrapper 只供 test 用

生产路径全走 processBatch，`processOne` 是老 test 兼容层（consumer_test.go 里断言）。commit 51a7e25 增改 210 行的 test 里若已经全用 processBatch，可以考虑删掉 `processOne` + `errTransientNeedsRetry` sentinel 收敛代码面。若保留请在注释里加一行"仅测试用，生产别调"（已经有类似措辞但可以更醒目）。

### §3.4 `internal/esindex/writer.go:161-186` `preservePainless` script 常量的多行字符串反引号缩进

script 里的 `if / else / def savedContent = null;` 块用 2-space 缩进但 Painless 支持任意空白 — 视觉上略乱，可以整成一致 2-space。**无功能影响**。

## §4 v1.13 修复轮验收（3 blocker + 10 P2 逐条确认）

| 编号 | 状态 | 证据 & 备注 |
|---|---|---|
| **Blocker #1 SSRF** | ✅ Confirmed | `ssrf.go:118-148` 双闸门（validateURL + ssrfRestrictedDialer + ssrfCheckRedirect）；`isBlockedIP` 覆盖 RFC1918/loopback/link-local/metadata/CGNAT/IPv6-ULA 全部；231 行 test 覆盖 scheme/host/direct IP/metadata/redirect；反面构造未突破。P2 见 §2.6（IPv4-mapped IPv6 test 补齐）。 |
| **Blocker #2 in-place retry** | ✅ Confirmed | `consumer.go:172-243` state machine 正确；`decision.go:43-72` partitionCommitPoints 多分区独立前缀；410 行 retry_test.go 覆盖 transient→OK / retry_exhausted / 429 / os_permanent / 多分区独立 / DLQ 写失败 fatal / ctx cancel 快退。反面：offset 100/101/102 场景验证 commit 顺序单调。P2 见 §2.2（DLQ 缺 messageID）+ §2.5（ctx.Canceled 误分类）。 |
| **Blocker #3 scripted_upsert** | ✅ Confirmed | `writer.go:132-186` script + retry_on_conflict=3；`writer.go:465-490` mapBulkResults 改 "update" key；218 行 scripted_upsert_test.go 覆盖 action 编码 / preserve script 常量 / derivatives / 老 "index" key 响应触发 transient / doc size 估算膨胀。反面：type=1 text 消息 doc 不会凭空生 payload.file；已存在 doc 保留 content/contentMeta 后 params.doc 全量替换正确。P2 见 §2.4（contract test 弱）。 |
| **P2-1 429 → errOSTransient** | ✅ Confirmed | `oswriter.go:115-117`，位置在 `>= 500` 之前、`>= 400` catch-all 之前，顺序正确；`retry_test.go:173-198` 覆盖。 |
| **P2-2 errOSPermanent → DLQ** | ✅ Confirmed | `consumer.go:268-275`，`ReasonOSPermanent` 常量在 `dlq.go:37`；`retry_test.go:200-223` 覆盖不重试 + DLQ + metric。 |
| **P2-3 backfill DeadlineExceeded vs SIGTERM** | ✅ Confirmed | `filebackfill/runner.go:97-146` 三个 ctx check 点都区分 `DeadlineExceeded` → `ErrTimeoutIncomplete` vs `Canceled` → nil；`runner_test.go:278-303` 覆盖三条路径（HappyPath / Canceled / DeadlineExceeded / RealError）。 |
| **P2-4 Tika io.LimitReader** | ✅ Confirmed | `tika.go:110-112`，上限 `maxContentBytes+4`；`p2_test.go:161-186` 用 100KB body + 1KB cfg 断言 truncate。 |
| **P2-5 Truncated `*bool`** | ✅ Confirmed（部分） | `esindex/doc.go:154` 改为 `*bool`；`extractor.go:105` 传 `&truncated`；分析确认 nil 被 omitempty、非 nil (true/false) 都会 marshal 出来。**但缺一条显式回归 test**：`TestFileContentMeta_TruncatedFalseSerializes` 在 doc changelog 里提到但我在 test 文件里找不到（`p2_test.go` 里不覆盖此项 case，`file_content_test.go` 未 grep 到）；建议加一条断言 `json.Marshal(FileContentMeta{Truncated: &f})` 里 `"truncated":false` 显式出现。 |
| **P2-6 sentinel errCDNPermanent** | ⚠️ Partial | sentinel 建立 + `isPermanentDownloadErr` 优先走 `errors.Is`；**但保留了 `strings.Contains` 兜底**，违背 P2-6 fix 声称的"无字符串耦合"。见 §2.3。 |
| **P2-7 empty_extract TrimSpace** | ✅ Confirmed | `extractor.go:96-100` 用 `strings.TrimSpace(content) == ""`；`p2_test.go:48-106` 覆盖 4 case（空串/换行/空格/混合空白）+ 断言 OS 未 hit。 |
| **P2-8 backfill scroll nextWithRetry** | ✅ Confirmed | `filebackfill/runner.go:154-176` bounded backoff 3 次（500ms→4s，指数）+ ctx aware；EOF/ctx err 直接返不 retry。运行时逻辑正确。**test 覆盖弱**：runner_test.go 里未见 nextWithRetry 专用 test（只有 real error propagate 的 TestRun_SourceRealError），建议补一条断言 "transient err retry 3 次后成功"。 |
| **P2-9 Tika ctx-driven timeout** | ✅ Confirmed | `tika.go:80-82` per-request `context.WithTimeout(ctx, t.timeout)`；`tika.go:97-107` 通过 `parentCtx.Err() != nil` 区分 parent cancel vs per-request DeadlineExceeded；`p2_test.go:108-157` 覆盖两条路径（per-req timeout → errExtractTimeout；parent cancel → context.Canceled）。 |
| **P2-10 file-extractor startup mapping-compat** | ✅ Confirmed | `cmd/file-extractor/main.go:62-97` 主流程前调 `AssertLiveMappingCompatible`；`esindex/mapping_compat.go:44-45` 已列 `payload.file.content` + `payload.file.contentMeta.extractedAt` 到 `requiredMappingFieldPaths`；缺字段 → loud crash 让 K8s 重启告警。 |
| **CI Lint 16 issues** | ⏳ 未本地跑 | `defer func() { _ = resp.Body.Close() }()` 在 download.go/tika.go/source.go 里已加，符合 errcheck；其他 15 issues 未逐一 verify（工具需 `golangci-lint run --new-from-patch=<base>` 或 PR push 后看 Job）。建议 Max 侧 push 前本地跑一次 `golangci-lint run --timeout=5m ./...` 拿全量结果附 PR。 |

## §5 亮点（不虚吹，≤5 条）

1. **`decision.go` + `partitionCommitPoints`**（`consumer.go:230` 调用）：一步到位处理多分区 offset 独立推进，杜绝"分区 A 卡住时分区 B 的高 offset commit 越过 A 未终态 offset"的隐性丢消息，而不是先出简化单分区版本再迭代。fix-plan §8 #1 识别的问题在 Blocker #2 修复里直接解掉，认知负载低。
2. **`ssrf.go` 双闸门 + `isBlockedIP` 显式列 CGNAT (100.64.0.0/10) + IPv6 ULA (fc00::/7)**：Go stdlib `IsPrivate()` 只覆盖 RFC 1918，作者显式扩展这两个 SSRF 高危段，覆盖 attacker 常用的绕过点。
3. **`writer.go` `preservePainless` 用 `ctx.op == 'create'` 分流**：正确处理 `scripted_upsert` 语义（doc 不存在时 script 必须主动填 `ctx._source = params.doc`，否则会造 empty doc）；这是很容易踩坑的点。
4. **`tika.go` P2-9 `parentCtx` vs `reqCtx` 区分**：在 err 分类里保留 `parentCtx` 引用来判 parent cancel，是 Go 里 ctx-driven timeout 的经典优雅写法。老代码用 `http.Client.Timeout` 是典型坑，这个 fix 从根子上处理。
5. **`retry_test.go` `TestProcessBatch_OffsetPrefixCommit_101FailsFirstAttempt`**：显式构造 100/101/102 三条消息，101 前 2 次 transient 第 3 次 OK，断言"中间过程 commit 不越过 101、最终 commit 到 102"，直接对应 Blocker #2 的核心 fear scenario。

## §6 未验证但 e2e 层需关注

代码 review 只能确认代码路径，以下需要在 test / prod 环境跑一遍才能定论：

1. **Painless script 在 OS 3.6 (dmwork-test/prod 实际部署版本) 的语法兼容性**
   代码里的 script 用 `def / instanceof Map / .get() / new HashMap()` — 都是 Painless 官方支持语法。但**没有真跑一次 `_bulk` 验证 script cache hit、执行开销、`ctx.op` 分支正确工作**。建议 e2e：test 环境 mock 一条 message → 走 es-indexer 写入 → 再走 file-extractor partial update → 再让 es-indexer redeliver 同 _id → 断言 OS 里 payload.file.content 未被覆盖。

2. **429 rate-limit 真触发路径**
   代码里 `classifyOSErr` 加 429 分支，但 OS 什么时候返 429？需要压测（bulk 写入超过 `search.max_open_scroll_context` 或类似 threshold）观察真实响应。若从未真触发，P2-1 修的是"理论上正确的分类"，但触发条件未验。

3. **SSRF metadata IP 真拦截**
   test 里 mock resolver 返 IP + 走 dialer 层拒；生产 DNS 环境（DNS/host resolution 差异）、redirect chain 中间 CDN 302 到 metadata 域的真实 SSRF payload 未验。**建议**：test 环境部署后手工造一条 payload.file.url=`https://169.254.169.254/...` 消息进 Kafka，看 file-extractor 是否拒 + 走 download_failed DLQ。

4. **Truncated *bool 清 stale true 的真回归**
   代码分析确认 `*bool + omitempty` 的 marshal 行为正确，但缺一条 e2e：先写 truncated=true → 后重抽 truncated=false → 查 OS `_source` 确认 truncated 字段值确实变了（partial update deep merge 语义在 OS 3.6 上实际行为）。

5. **backfill scroll TTL 5 min 在实际 17M docs 上是否够**
   一批 500 doc × 50 RPS = 10s/batch，17M / 500 = 34K batch → 单机 Job 完整回填 ~95 hour；即使 scroll TTL 每次 continue 都续期，也需要长跑 Job 不 crash。建议 e2e 先跑一个小样本（10K docs）看 scroll 会不会中途过期。

6. **retry_exhausted 长期 backoff 的 partition 阻塞影响**
   MaxRetries=10 + base=1s max=60s → 单条最坏 ~10 min 阻塞该 partition 里后续 offset。若某 CDN URL 长期返 500 → 该分区 SLA 下降到 10 min/条。需要监控 metric 观察实际 retry_exhausted 触发频率是否可接受。

## §7 Review 方法论

**读取顺序**（约 75 min）：
1. (5 min) `git log --stat` / `git show --stat` 摸清 5 commit 变更范围与文件行数分布，识别核心文件。
2. (25 min) 并行读 Blocker 核心文件：`ssrf.go` + `consumer.go` + `decision.go` + `backoff.go` + `writer.go`(esindex) + `download.go`；每读完一段做反面构造思考（TOCTOU / silent skip / preserve edge case）。
3. (15 min) 并行读 P2 相关：`oswriter.go` + `tika.go` + `extractor.go` + `dlq.go` + `mapping_compat.go` + `filebackfill/{runner,source}.go` + `cmd/file-extractor/main.go` + `esindex/doc.go`（FileContentMeta 定义）+ `config.go`。
4. (15 min) 并行读关键 test：`retry_test.go` + `ssrf_test.go` + `scripted_upsert_test.go` + `p2_test.go` + `runner_test.go`；核对 test 覆盖 vs commit 声称。
5. (5 min) `docs/file-content-indexing-implementation.md` v3 changelog 段 vs 代码实际状态对齐检查（识别 doc-code inconsistency）。
6. (10 min) 综合分析 + 写本文档；反复推敲 §1 是否真"无严重"（若有 blocker 修复留 bug 不能 approve）。

**跳过 / 未深读**：
- `filebackfill/rate_limiter.go`（本轮未改）
- `internal/consumer/*`（Blocker #2 pattern 源头，只对比结构不逐行读）
- `esindex/{mapping/octo-message.json, buildraw.go}`（本轮未改）
- 未跑 `go test ./...` 本地实测（信任 commit message 声称 37 test 全绿；生产 push 前 CI 会跑一遍）。
- 未跑 `golangci-lint run`；CI Lint 16 issues 只按代码目视核对，未全量 verify。

**反面构造 / 攻击面思考**（关键路径）：
- SSRF：能否构造 URL 绕过双闸门？（scheme 大小写、URL encoding、redirect chain、DNS 双重返回、IPv4-mapped IPv6）— 全部未突破。
- Blocker #2：能否构造 batch 让 partition silent skip 或永久阻塞？（transient 消息卡首、retry_exhausted 后 offset 越过、ctx cancel 时机、DLQ 写失败）— 全部收敛于 forced DLQ 或 fatal fast-return。
- Blocker #3：能否构造 doc 让 script 抹掉不该抹的字段或凭空造字段？（type=1 无 file、type=8 首次上、type=8 redeliver）— 保留分支只在 saved 非 null 时触发，非 null 前提是原 doc 有 payload.file.content —— 逻辑闭合。

---

**最终判定**：**APPROVE WITH NITS** — 3 blocker 修复方向和执行都靠谱，无 push blocker。§2 六条 P2 建议 Max 派回 cc-octo 消化后再 push（10-30 min 工作量）；若时间紧可以 push GitHub 后在 PR review 里作为 comment 提出，本轮修复轮质量本身 sig-off。§6 e2e 未验点较多，push 后 test 环境部署 e2e 计划需 Max 拉齐。
