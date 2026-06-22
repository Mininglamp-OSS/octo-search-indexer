# octo-search-indexer

The message-search **write + index** layer for the OCTO pipeline. This repo
builds one image carrying **two long-running binaries** plus on-demand ops tools:

- **`searchetl-producer`** (write side) — a standalone polling ETL that reads the
  MySQL message shard tables on a slow-cursor tick, enriches each row into the
  message contract (fail-closed visibility), and produces it to Kafka.
- **`es-indexer`** (read/index side) — the Kafka consumer that idempotently
  bulk-writes documents into OpenSearch (`doc _id = message_id`), with Chinese
  tokenization handled by the index mapping/analyzer.

The two binaries are **decoupled** — they share **only** the
[`octo-lib`](https://github.com/Mininglamp-OSS/octo-lib) `contract/searchmsg`
message contract and the Kafka topics (`octo.message.v1` + its DLQ). Neither
imports the other; one image, two Deployments.

## Where it sits in the pipeline

```
message 5-shard tables (octo-server, SoR)
  → [ searchetl-producer  (THIS repo, write side) ]  # poll → enrich → Kafka
  → Kafka topic octo.message.v1                       # body contract (octo-lib/contract/searchmsg)
  → [ es-indexer          (THIS repo, index side) ]  # consumer + ES bulk writer + 中文分词
  → OpenSearch
  → read path (octo-server)                           # query-side join to filter revoked/deleted
                                                       #  + authz fail-CLOSED + paging

# one-shot historical load (bypasses Kafka, not part of the realtime path):
message 5-shard tables → [ backfill (cmd/backfill) ] → OpenSearch
```

`searchetl-producer` is the **standalone replacement** for the producer that used
to live inside `octo-server` — it now lives **in this repo**, running alongside
`es-indexer` as the realtime write side. `backfill` (`cmd/backfill`) is the
separate one-shot historical loader that bypasses Kafka entirely.

Each binary lives here because the platform's CI/CD builds one image per
repository (one-repo-one-image); the message-search write/index workers ship as
binaries distinct from `octo-server`.

## Design discipline

- **Two decoupled binaries, one contract.** `searchetl-producer` (write side) and
  `es-indexer` (index side) never import each other; they meet only at the
  `octo-lib` `searchmsg` contract and the Kafka topics. The producer is a slim
  mirror of the source ETL module — payload extraction is ported as pure
  functions and the fail-closed visibility parser is the **shared** octo-lib
  `searchmsg.ExtractVisibility` (single source of truth across producer +
  backfill, so the ACL leak guard can't diverge between repos).
- **Decoupled reusable writer.** The Kafka consumer (`cmd/es-indexer`) and the
  ES writer (`internal/esindex`) are separated so the backfill job can
  import `internal/esindex` and reuse the exact same write path
  (read `message` table → contract → `Writer.Bulk`) without copying ES code.
- **Idempotent sink.** ES `_id = message_id` (= Kafka key) gives
  effectively-once on top of an at-least-once delivery.
- **No revoked/deleted state in ES.** Route 甲: revoke/delete filtering happens
  at read time via MySQL join in `octo-server`. ES stores only body + the
  visibility fields required for query-side authz — matching the `searchmsg`
  contract.
- **Schema-version checked.** Unknown contract versions go to DLQ, never
  silently consumed.

## Layout

| Path                  | Purpose                                                        |
| --------------------- | ------------------------------------------------------------- |
| `cmd/searchetl-producer/` | Producer entrypoint (write side): MySQL→Kafka polling ETL, env config, graceful shutdown, opt-in idle posture |
| `cmd/es-indexer/`     | Consumer entrypoint (index side): env config, graceful shutdown |
| `cmd/backfill/`       | One-shot historical loader: MySQL shards → OpenSearch, bypassing Kafka |
| `internal/producer/`  | Producer internals: slow-cursor scheduler, keyset extraction, fail-closed enrich, Kafka sink + DLQ, Redis run-lock, obs server (`config.go` is the env SoT) |
| `internal/consumer/`  | Kafka consumer: FetchMessage + manual commit, ordered-prefix offset, DLQ routing + terminal escape (C4) |
| `internal/esindex/`   | Reusable ES bulk writer + index mapping bootstrap (imported by both the service and the backfill job) |
| `internal/esindex/mapping/` | Canonical `octo-message` index mapping + 中文 analyzer (single source for the octo-deployment change) |
| `harness/producer/`   | Local docker-compose e2e harness for the producer (see its own `README.md`) |

## Reliability semantics (C4)

- **Manual commit only.** The Reader uses `FetchMessage` + `CommitInterval=0`
  (no `ReadMessage` auto-commit). Offset advances **only to the contiguous
  success prefix** — the first transient failure stops the prefix, so Kafka's
  monotonic high-watermark commit can never silently confirm an unprocessed
  message.
- **Transient vs permanent.** 429 / 5xx / network / batch-level failures are
  transient → in-place backoff retry, offset not advanced. 4xx (except 429) and
  unknown `schema_version` are permanent poison pills → routed to the DLQ topic,
  then the offset crosses them.
- **DLQ terminal escape.** If the DLQ write itself keeps failing (transient),
  a bounded retry is followed by either local spill-to-disk + alarm + advance
  (when `INDEXER_DLQ_SPILL_DIR` is set), or a hard stop + page (when it is not).
  A DLQ outage can never wedge the prefix forever.
- **Idempotent sink.** `_id = message_id` → duplicate delivery upserts the same
  doc (effectively-once on top of at-least-once).

## Configuration (env)

Each binary reads its own env namespace; they share no variables.

### `es-indexer` (consumer)

| Variable | Default | Notes |
| -------- | ------- | ----- |
| `ES_INDEXER_ENABLED` | `false` | must be `true` AND brokers+ES set, else the binary idles (zero runtime effect) |
| `KAFKA_BROKERS` | — | CSV broker list |
| `KAFKA_TOPIC` | `octo.message.v1` | body topic |
| `KAFKA_DLQ_TOPIC` | `octo.message.v1.dlq` | poison-pill topic |
| `KAFKA_GROUP_ID` | `octo-search-indexer` | consumer group |
| `ES_ADDRESSES` | — | CSV OpenSearch node list |
| `ES_INDEX` | `octo-message` | target index |
| `ES_USERNAME` / `ES_PASSWORD` | — | HTTP basic auth |
| `INDEXER_BATCH_SIZE` | `500` | max docs per bulk |
| `INDEXER_TRANSIENT_BACKOFF_MS` | `1000` | retry backoff on transient |
| `INDEXER_DLQ_MAX_RETRIES` | `5` | DLQ write retries before escape |
| `INDEXER_DLQ_RETRY_BACKOFF_MS` | `200` | DLQ retry backoff base |
| `INDEXER_DLQ_SPILL_DIR` | — | set to enable spill escape; empty → hard-stop escape |

### `searchetl-producer` (producer)

Source of truth: `internal/producer/config.go`.

| Variable | Default | Notes |
| -------- | ------- | ----- |
| `PRODUCER_ENABLED` | `false` | master switch. Must be `true` to run; unset/`false` → idle (see safety note below) |
| `PRODUCER_MYSQL_DSN` | — | source MySQL DSN (message shards). Required when enabled |
| `PRODUCER_TABLES` | `message,message1,message2,message3,message4` | CSV of shard tables to poll |
| `PRODUCER_DB_MAX_OPEN_CONNS` | `8` | source pool max open conns (clamped 1–256) |
| `PRODUCER_DB_MAX_IDLE_CONNS` | `4` | source pool max idle conns (clamped 1–256; capped to max-open) |
| `PRODUCER_DB_CONN_MAX_LIFETIME` | `30m` | source conn max lifetime (Go duration) |
| `PRODUCER_KAFKA_BROKERS` | — | CSV broker list. Required when enabled |
| `PRODUCER_KAFKA_TOPIC` | `octo.message.v1` | body topic (must match the consumer's) |
| `PRODUCER_KAFKA_DLQ_TOPIC` | `octo.message.v1.dlq` | producer-side DLQ topic |
| `PRODUCER_REDIS_ADDR` | — | Redis addr for the cross-replica distributed run-lock. Required when enabled |
| `PRODUCER_REDIS_PASSWORD` | — | Redis auth password |
| `PRODUCER_REDIS_TLS` | `false` | `true` enables TLS to Redis |
| `PRODUCER_REDIS_TLS_INSECURE_SKIP_VERIFY` | `false` | `true` skips Redis TLS cert verification (test only) |
| `PRODUCER_REDIS_TLS_CA_FILE` | — | CA bundle for Redis TLS (`PRODUCER_REDIS_CA_FILE` is an accepted alias) |
| `PRODUCER_BATCH` | `5000` | keyset page size per shard (clamped 100–50000) |
| `PRODUCER_LAG_SECONDS` | `600` | stability lag window; must be > 0 in prod (lag=0 risks silent missed reads), clamped ≤ 86400 |
| `PRODUCER_TICK_SECONDS` | `60` | slow-cursor tick period (clamped 5–3600) |
| `PRODUCER_OBS_ADDR` | `:9090` | observability HTTP addr (`/healthz` `/readyz` `/metrics`) |

> **Opt-in safety contract (zero production risk by construction).** With
> `PRODUCER_ENABLED` unset/`false` the producer **idles** — it dials no backend
> and produces nothing (it only serves health probes so a "deployed but off" pod
> doesn't crashloop). When `PRODUCER_ENABLED=true` the operator means it to run,
> so the config is validated and the binary **fails fast (crashloops loudly)** if
> DSN / brokers / Redis are missing or `lag` is set to 0 — it never idles "ready"
> while silently producing nothing. The switch is intentionally separate from
> config completeness so a cut-over can never silently no-op.

## Backfill & reconciliation (must-run gate)

The backfill job (`cmd/backfill`) loads **historical / pre-existing** `message`
shards into OpenSearch, bypassing Kafka, and fills the reader's safety fields
(`spaceId`/`visibles`/`messageSeq`) from the raw MySQL payload for those rows.
Realtime rows get the same fields enriched by `searchetl-producer` (fail-closed
enrich) on the Kafka write path — so backfill is the loader for the historical
back-load slice, not the only writer of these fields.

A count match alone does not prove correctness — it proves only that the row
*counts* tie, not that each doc's authz/correctness fields are right. **Always run
the field-level reconciliation gate after a backfill:**

```sh
# inline after the backfill run (uses the job's own exact DLQ count as authority):
go run ./cmd/backfill ... -reconcile -from <epoch> -to <epoch>   # count gate + field-level sample gate

# or standalone, any time (pass the backfill DLQ spill dir so legit DLQ rows are
# excluded from the sample gate exactly like the inline path — otherwise a sampled
# DLQ row false-fails as sample_missing and the gate exits non-zero):
make run-recon RECON_FROM=<epoch> RECON_TO=<epoch> RECON_DLQ=<dlq-count> \
  RECON_DLQ_SPILL_DIR=<the backfill -spill-dir>
```

The sample gate cross-checks `messageId` (full precision), `messageSeq`,
`channelId`, `channelType`, `spaceId`, and **`visibles`** between MySQL and the ES
doc, so a silently dropped ACL (`visibles`) field surfaces as `sample_mismatch>0`
and the gate exits non-zero. Rows the backfill deliberately routed to the DLQ (bad
payload / non-numeric id / permanent ES reject — already counted in `-dlq`) are
*expected* to have no ES doc; both reconcile paths exclude them from the sample
gate (inline reads its in-memory spill, standalone reads the same spill dir via
`-dlq-spill-dir` / `RECON_DLQ_SPILL_DIR`) so they never false-fail as
`sample_missing`. `make run-recon` is **mandatory** before switching the read
alias to a freshly backfilled index (see `docs/forward-migration-v1.9.md`).

## Build

```sh
go build ./...
go test ./...
```

## License

[Apache License 2.0](./LICENSE).
