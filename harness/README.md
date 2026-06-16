# Local e2e verification harness

A throwaway local stack that runs the **whole** message-search pipeline for real:

```
seed → Kafka octo.message.v1 → es-indexer consumer → OpenSearch (analysis-ik)
                                      └→ octo.message.v1.dlq (poison pills)
```

> **v1.9 契约收敛（YUJ-4534 阶段 8）**：索引 doc 已收敛到 octo-server reader 形态
> （camelCase 嵌套，`messageId`(long) / `payload.text.content`(IK) / `spaceId` / `visibles` /
> `messageSeq`，alias `wukongim-messages-read`）。harness 的 message_id 因此用**数值** snowflake 串
> （reader 读 `messageId` 为 long；非数值串会被判毒丸进 DLQ）。详见 `docs/forward-migration-v1.9.md`。

> 🔒 **v1.9 实时写入安全门（YUJ-4698 / Jerry-Xin Critical）— 决定该用哪条 e2e 路径。**
> 实时 consumer（`internal/consumer.Service.Run`）在 Kafka 契约（octo-lib `searchmsg.SchemaVersion`）
> **未携带** reader 安全字段（`spaceId`/`visibles`/`messageSeq`，即 `SchemaVersion < 2`）时**拒启动**
> （fail-CLOSED）：放行会让 reader 对空 `visibles` fail-OPEN（普通成员搜出群管才可见消息）。这是
> 生产安全设计，**harness 不可绕过、也不应削弱**。
>
> 后果：当前契约 `SchemaVersion=1` 下，**`./harness/run.sh all`（Kafka→consumer→OpenSearch
> 实时链）跑不通**——indexer 在消费前就退出。`run.sh` 会先跑 `./harness/contractgate`（复用
> consumer 同一判定 `esindex.LiveContractCarriesSafetyFields()`，永不与生产门冲突）探测门状态：
> - **门关（现状，SchemaVersion=1）**：`run.sh all` 不再尝试拉栈/seed，而是打印指引并 exit 0，
>   把 v1.9 e2e 引到下面的 **backfill harness**——这是当前唯一能端到端验证 v1.9 reader 契约的路径
>   （只有 backfill 能从 MySQL payload 自源填全 `spaceId`/`visibles`/`messageSeq`）。
> - **门开（octo-lib 升 `SchemaVersion>=2` + 阶段 9 producer 富化后）**：门自动解封，`run.sh all`
>   重新驱动实时链，**无需改 harness**。
>
> 单独看门状态：`./harness/run.sh gate`（门开 exit 0 / 门关 exit≥1，并打印当前 SchemaVersion）。
> （仅在契约已升到 `>=2` 后才有意义的逃生口：`FORCE_LIVE=1 ./harness/run.sh all` 跳过预检直跑实时链；
> indexer 仍自带同一安全门自检，所以这个开关无法绕过生产安全门。）

> 🔒 **Isolation.** This is a local-only stack (`docker compose`). It is NEVER
> wired into a shared test environment, and the indexer's `Kafka.On` / real
> deployment wiring stays off until a separate, explicitly signed-off step.
> Real deployment provisioning (octo-deployment, IK dictionaries, topics) is out
> of scope here.

## Contents

| Path | What |
| --- | --- |
| `docker-compose.yml` | Kafka (KRaft, 1 broker) + OpenSearch (1 node) |
| `opensearch-ik.Dockerfile` | OpenSearch image with the `analysis-ik` plugin installed (version-matched) |
| `run.sh` | orchestrator: `up` / `down` / `gate` (print live-ingestion gate state) / `seed` / `verify` / `all` |
| `contractgate/` | harness-only probe: reports whether the compiled contract carries the reader safety fields (live gate open/closed); reuses the consumer's own predicate so it can't disagree with production |
| `seed/` | producer of a controlled message suite (normal / 中文 / raw_excluded / duplicate _id / unknown schema / bad JSON) and a `-mode bulk -n N` throughput load |
| `verify/` | asserts the invariants against OpenSearch |

## Run it

> ⚠️ At the current contract (`SchemaVersion=1`) the **live** path below is gated
> off (see the v1.9 safety-gate note above). `./harness/run.sh all` will detect
> this, print guidance, and route you to `./harness/run-backfill.sh` — the
> backfill harness is the v1.9 end-to-end verification path today. The live steps
> below become runnable again once the contract is bumped to `SchemaVersion>=2`.

```sh
# inspect the live-ingestion safety gate first (exit 0 = open, e2e live path available)
./harness/run.sh gate

# full cycle: up → indexer → seed suite → verify invariants → down
# (auto-skips with guidance while the live gate is closed)
./harness/run.sh

# or step by step, leaving the stack up (only meaningful once the gate is OPEN):
KEEP_UP=1 ./harness/run.sh up
ES_INDEXER_ENABLED=true KAFKA_BROKERS=localhost:19092 \
  ES_ADDRESSES=http://localhost:19200 ES_INDEX=octo-message \
  KAFKA_DLQ_TOPIC=octo.message.v1.dlq INDEXER_DLQ_SPILL_DIR=/tmp/spill \
  go run ./cmd/es-indexer &        # start the indexer against the harness
                                   # (exits immediately while the contract is SchemaVersion<2)

# Fence the DLQ check to THIS run: capture the DLQ end offset BEFORE seeding.
FENCE=$(docker exec octo-harness-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic octo.message.v1.dlq 2>/dev/null \
  | awk -F: '{s+=$3} END{print s+0}')
go run ./harness/seed -mode suite  # seed the controlled suite
DLQ_START_OFFSET="$FENCE" go run ./harness/verify   # assert invariants
./harness/run.sh down
```

> The verifier is **fail-closed** on the DLQ fence: if `DLQ_START_OFFSET` is not
> provided and the DLQ topic already contains records, `verify` aborts rather than
> risk matching stale poison-pill records from a previous run. On a fresh/empty DLQ
> no fence is needed. `run.sh` captures and passes the fence automatically.

Ports: Kafka `localhost:19092`, OpenSearch `http://localhost:19200`.

## Invariants asserted (`verify/`)

- **Consumer-group drained (Kafka)** — the committed offset of the consumer group
  reaches the body topic's log-end offset on every partition. This is the
  authoritative drain proof (queried from Kafka directly, not inferred from ES doc
  counts), so the negative "not indexed" assertions below are sound. It cannot be
  fooled by a consumer that stalled before the poison pills or by the DLQ-spill path.
- **Idempotency** — duplicate `message_id` → exactly one ES doc (`_id` upsert).
- **C4 schema gate — both directions:**
  - positive: the **DLQ topic actually contains** the poison-pill keys
    (`m-badschema`, `m-badjson`) — read straight from the DLQ topic, proving the
    routing happened;
  - negative: those messages are **NOT** in the ES body index.
- **raw_excluded** — Signal/non-text doc IS indexed (content null), occupies a doc.
- **IK tokenization** — Chinese terms (`公园`, `北京`) recall their docs; English
  (`pipeline`) too.

The C2/C4 producer/consumer state-machine invariants (per-partition ordered
commit, transient in-place retry, DLQ terminal escape) are unit-tested under
`-race` in `internal/consumer`; the harness exercises the same code paths against
a real broker + real OpenSearch.

## Notes

- The DLQ topic must be **pre-created** (the indexer's DLQ producer uses
  `AllowAutoTopicCreation=false` by design). `run.sh up` creates both topics; in
  production the platform provisions them.
- The harness OpenSearch disables the security plugin for plain-HTTP local
  testing only — production uses authenticated endpoints (`ES_USERNAME`/`ES_PASSWORD`).

## Phase-6 backfill harness (`run-backfill.sh` + `backfill/`)

> 🔒 **This is the v1.9 end-to-end verification path while the live-ingestion gate
> is closed (current `SchemaVersion=1`).** Only backfill can populate the reader
> safety fields (`spaceId`/`visibles`/`messageSeq`) — it reads them straight from
> the MySQL payload — so it is the one path that exercises the full v1.9 reader
> contract end to end against real infra today.

A second, self-contained e2e that exercises the historical backfill job (`cmd/backfill`)
against a real MySQL + OpenSearch(IK), bypassing Kafka:

```
MySQL message shards → cmd/backfill (keyset scan → internal/esindex.Writer bulk) → OpenSearch
```

| Path | What |
| --- | --- |
| `run-backfill.sh` | orchestrator: `up` (throwaway OpenSearch(IK) + MySQL) / `seed` / `backfill` / `verify` / `down` / `all` |
| `backfill/` | seeds a controlled 6-row suite into the `message` shard tables and verifies the ES result |

The seeded suite is deliberately tiny and explicit so the gate arithmetic is obvious:
3 text rows (incl. 中文) + 1 Signal-encrypted + 1 non-text (image) + 1 bad-JSON anomaly.
So `source_rows=6`, expected `ES docs=5` (`6 - 1 DLQ`), `raw_excluded=2`.

```sh
./harness/run-backfill.sh          # up → seed MySQL → backfill+reconcile gate → verify → down
KEEP_UP=1 ./harness/run-backfill.sh
```

What it proves (mirrors the unit tests against real infra):
- reuses `internal/esindex` writer + IK mapping (`ik_max_word`/`ik_smart`), `_id=message_id`;
- `raw_excluded` rows (Signal/non-text) still occupy an ES doc; the bad-JSON anomaly is
  routed to the local DLQ spill and is **positively asserted absent** from ES;
- the reconcile gate runs inline and is `OK | source_rows=6 es_docs=5 expected=5 diff=0`
  (it force-refreshes the index first so a refresh lag can't cause a false MISMATCH);
- re-running is a no-op via the checkpoint and idempotent (`_id` upsert) — ES count stays 5;
- IK 中文 recall works on backfilled docs (`公园` → the 公园 row, `北京` → the 北京 row).

See `docs/backfill.md` for the production runbook and isolation discipline.
