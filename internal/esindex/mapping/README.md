# octo-message index mapping (v1.12 — file content indexing)

`octo-message.json` is the **canonical index mapping + analyzer** the `es-indexer`
writes against, embedded into the binary (`//go:embed`). The index must be
**pre-created manually** with this mapping (plus ISM/lifecycle policy, shards/replicas
and aliases) — `esindex.EnsureIndex` only **verifies the index exists** at startup and
**refuses to start** if it is missing (auto-create intentionally disabled, see issue #29).

## v1.12 文件正文全文检索（file content indexing）

v1.12 在 `payload.file` 下新增两个字段，配套 `cmd/file-extractor` 独立服务抽取的文件正文写入：

- `payload.file.content` (text, IK 分词) — Tika 抽出的文件正文纯文本。用 IK 双分析器
  保持与 `payload.text.content` 同口径（index-time `ik_max_word` / query-time `ik_smart`）。
  `mappings._source.excludes` 排除该字段，节省 _source 体积；搜索仍可命中并 highlight
  （走倒排不走 _source）。
- `payload.file.contentMeta.{extractedAt, extractor, truncated, extractMs}` (object) —
  抽取元信息，便于运维观察抽取延迟 / 覆盖率 / 截断率。子字段类型：
  - `extractedAt` — 抽取时刻 (epoch second, date)
  - `extractor`   — 抽取器标识 (keyword, 如 "tika/3.3.0")
  - `truncated`   — content 是否被截到 MaxContentBytes (boolean)
  - `extractMs`   — Tika 抽取耗时 (long, 毫秒)

写入契约：由独立 `cmd/file-extractor` 服务通过 OS `_update` partial update 只更新
`payload.file.content` + `payload.file.contentMeta`，不动主 doc 其他字段（父 doc 由
`cmd/es-indexer` 主流程写入）。file-extractor 消费同一 Kafka topic 但独立 consumer group
`file-extractor`（不抢 es-indexer 位点）。

> `payload.file.content` + `payload.file.contentMeta.extractedAt` 已纳入
> `mapping_compat.go` 启动断言集（fail-closed），live mapping 不符直接拒启动。

**⚠️ 不可逆警告**：`PUT _mapping` 添加 `payload.file.content` + `contentMeta` 后**不可逆**
——OS 3.x mapping 字段只能通过 reindex + 切换 alias 才能移除。合并前 sig-off 意味着接受
该单向决策。详见 `docs/file-content-indexing-feasibility.md` v2 §7 / §5 回滚方案。

详见 `docs/file-content-indexing-feasibility.md`（v2 §7）+ `docs/file-content-indexing-implementation.md`（v2）+ `docs/file-extractor-tool-comparison.md`（Tika 选型论证）。

## v1.11 subSeq 排序 tiebreaker（配套 B2 虚拟子文档）

reader 排序键是 `[timestamp, messageId]`，翻页靠 `search_after`（排他）。B2 虚拟子文档的
`messageId`/`timestamp` 都 = 父值，同一父的 N 个子文档 tuple 完全相同 → 跨页边界时兄弟被静默跳过。

v1.11 加一个数值 tiebreaker `subSeq` 作为**第三排序键**：

- `subSeq` (integer) — (messageId, subSeq) 全局唯一 → (timestamp, messageId, subSeq) 唯一。

写入规则（indexer）：
- 普通消息 doc / 富文本父 doc：`subSeq = 0`（**显式落盘**，字段不用 omitempty，reader 不赌缺失=0）。
- 富文本虚拟子文档：`subSeq = block序号 i + 1`（从 1 递增，父独占 0，保父子不撞）。

游标变更由 **reader** 做（排序键改 `[timestamp, messageId, subSeq]`、把 subSeq 编进 cursor、
旧 cursor 平滑降级）；indexer **不碰 cursor**，只负责把 subSeq 写对。subSeq 随 B2 那次 reindex
一起落地，无额外 reindex。

> subSeq 已纳入 `mapping_compat.go` 启动断言集（fail-closed）。

## v1.10 富文本(type=14)内嵌媒体虚拟子文档（B2 方案）

v1.10 在顶层新增 3 个字段，为「富文本里内嵌的图片/文件派生独立可搜子文档」配套：

- `parentMessageId` (long) — 子文档指向父富文本 messageId（reader 用它回 MySQL join 判可见性/撤回）。
- `parentPayloadType` (integer) — 父原 payload.type（=14）。
- `virtual` (boolean) — 标记该 doc 由富文本派生（reader 在文本检索端点用 must_not 排除）。

子文档 `payload.type ∈ {2,5,8}`，**复用现有** `payload.image`/`payload.file` mapping，本期不新增子字段。
本期**只派生 image(→type=2)**：富文本(type=14) block 上游契约仅 text/image，file 全链路未打开
（octo-lib `ValidateRichTextBlocks` 拒非 text/image block，octo-web 发送侧不发 file），故现实中不会
产生 file/video 富文本子文档。待上游 octo-lib/octo-web 契约打开 file/video block 后，再扩展
file→type=8 / video→type=5 派生（`payload.type ∈ {5,8}` mapping 已前瞻就位）。撤回/编辑联动
**不做**（路线甲：reader 用 parentMessageId 回 MySQL join 判定）。详见
`richtext-virtual-docs-indexer-dev.md`。

> 这 3 个新字段已纳入 `mapping_compat.go` 启动断言集（`requiredMappingFieldPaths`），live mapping
> 不符直接拒启动（沿用现有 fail-closed 风格）。

## v1.9 contract convergence (YUJ-4534 阶段 8 · 分叉 B)

v1.9 收敛索引契约到 **octo-server reader** 读的形态
(`modules/messages_search/source.go::Doc`)，reader 不动。相对旧 flat snake_case 契约：

- 字段名 **camelCase** + payload **嵌套**（`messageId/channelId/payload.text.content`）。
- `messageId` 用 **long**（数值全精度）——snowflake id 不被 float64 截断（reader 从 typed _source
  读 int64 做 cursor tiebreaker）。
- 新增 reader 必读字段：`spaceId`(keyword) / `visibles`(keyword[]) / `messageSeq`(long)。
- sort 字段 `timestamp`(epoch_second) + `messageId` 与 reader cursor sort 口径一致。
- reader 读 **alias `wukongim-messages-read`**（pattern `wukongim-messages-*`）。**本 mapping 不再内嵌该
  alias**（v1.9 R2）：人工预建索引用本文件 PUT，若内嵌 alias 会让新索引一建好就被 reader
  读到（reindex/backfill 完成前的半量数据）。alias 改由迁移脚本步骤③在对账通过后单独原子挂。

完整迁移说明见 `docs/forward-migration-v1.9.md`。

## Chinese tokenization — IK (analysis-ik)

`payload.text.content`（及 image.caption/name、file.name/caption、mergeForward.msgs.searchText）
用 IK 双分析器：

- index-time `analyzer: ik_max_word` — 穷举切分，最大化召回（IM 搜索「宁可多召回不漏」）。
- query-time `search_analyzer: ik_smart` — 智能切分，保精度。

> 需 OpenSearch 集群装 `analysis-ik` 插件（由 octo-deployment 协调变更 provision）。若部署无 IK，
> 在 octo-deployment 模板覆盖 analyzer（如 cjk_bigram），该选择归部署侧，不在本文件钉死。

## Field rationale (route 甲: body + query-side authz visibility only)

| Field | Type | Why |
| --- | --- | --- |
| `messageId` | long | = ES `_id`（规范化）/ exact-match dedupe key；全精度 snowflake |
| `messageSeq` | long | reader channel_offset「清空会话」可见性 gate |
| `from` / `to` | keyword | sender 过滤 / 鉴权；p2p 参与方 |
| `channelId` | keyword | query-side 频道鉴权过滤（group/topic/DM） |
| `channelType` | integer | 按频道类型分流鉴权 |
| `spaceId` | keyword | p2p（DM）space 召回过滤；缺则 reader fail-closed |
| `visibles` | keyword[] | 群系统消息「仅管理员可见」白名单；缺则 reader fail-OPEN（安全项） |
| `timestamp` | date(epoch_second) | 发送时间；cursor sort 字段（+ messageId） |
| `payload.type` | integer | 消息类型，reader `_search_all` 分流 |
| `payload.text.content` | text + IK | 可检索正文，中文分词 |
| `payload.image/gif/voice/video/file/mergeForward.*` | keyword/text/integer/long | reader `_search_media`/`_search_files`/forward 卡片字段（mapping 就位，本期 indexer 只填文本） |
| `createdAt` | long | 落库时间（epoch s），对账窗口字段 |
| `rawExcluded` | boolean | Signal/非文本占位 doc 标记（content null） |
| `revoked` | boolean | OS-side best-effort 撤回标记（非安全边界，权威撤回靠 reader 回 MySQL join） |
| `source` | keyword | 来源（ETL vs 未来 CDC） |

No per-user `is_deleted` field — route 甲 在 reader 读时回 MySQL join 过滤。
Mapping 为 `dynamic: strict`：任何意外字段直接报错，不静默污染索引。

> mapping 须与 octo-deployment 托管模板 + octo-server reader Doc 保持同契约版本对齐（半兼容会
> 悄悄打断 search_after / around / p2p filter）。
