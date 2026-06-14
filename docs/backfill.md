# Historical backfill runbook (`cmd/backfill`)

Phase 6 of the octo message-search pipeline (YUJ-4534). One-shot job that loads the
historical `message` shard tables into OpenSearch, **bypassing Kafka**, reusing the
same write path as the live indexer.

```
message 5 shards → [cmd/backfill: keyset scan → internal/esindex.Writer bulk] → OpenSearch
```

## Why bypass Kafka

The historical volume (~3.15M rows / ~2.14 GB at ~700 B/row) does not need to go
through Kafka. Direct bulk-to-ES is faster and avoids any Kafka retention-deletion
risk during a large one-shot load. The live increment keeps flowing through Kafka
unchanged; the two paths are safe to run in parallel because the ES doc `_id =
message_id` makes every write an idempotent upsert — the same message written by both
paths just overwrites the same doc.

Revoked/deleted messages are **not** a concern for backfill: route 甲 keeps only the
body in ES and filters revoke/delete at read time via a MySQL join, so even if a
historically-revoked body is loaded it is filtered on read.

## What it does (correctness properties)

- **Reuses `internal/esindex`** (writer + `EnsureIndex` mapping + `_id=message_id`).
  No ES write code is re-implemented.
- **Keyset pagination** by primary-key `id` (`WHERE id>? ORDER BY id ASC LIMIT ?`),
  never `OFFSET` — O(1) deep-page seeks, no offset drift.
- **Idempotent** with the live increment (`_id=message_id` upsert; overlap is safe).
- **Resumable**: a per-shard high-watermark is persisted to a local checkpoint file
  (atomic temp-file + rename). A watermark is advanced **only after** the batch is
  terminal (indexed, or routed to the local DLQ spill). The checkpoint is physically
  isolated from the live increment cursor (`octo_etl_es_cursor`).
- **Rate-limited** (`-rate`, default ≤5000 docs/s) to avoid overrunning ES / source DB.
- **payload extraction is byte-for-byte aligned with the live producer** (P1-d three-way
  split): Signal-encrypted / non-text → `raw_excluded` (still occupies an ES doc, NOT a
  loss, NOT DLQ); a real parse anomaly → local DLQ spill (counted).
- **fail-closed**: any DLQ spill-write failure or checkpoint-persist failure STOPs the
  whole job (real anomalies must never silently vanish — that would corrupt reconcile).

## DLQ accounting (no Kafka DLQ topic on this path)

The live indexer routes poison pills to a Kafka DLQ topic; backfill bypasses Kafka, so
it lands two classes of "not in ES body index" rows into a **local DLQ spill file**
(`<spill-dir>/backfill-dlq.ndjson`) and counts them precisely:

1. `payload_unparseable` — a non-encrypted payload that should have parsed but didn't.
2. permanent ES rejects — per-item 4xx (e.g. mapping conflict) returned by `_bulk`.

That exact count is fed into the reconcile gate as the authoritative `DLQ` input, so
legitimately-DLQ'd messages are **not** misjudged as ES-missing.

**Crash recovery.** The spill is append-only; each batch fsyncs the spill and then
atomically advances a durable offset sidecar (`<spill-dir>/backfill-dlq.synced`)
*before* the checkpoint advances. On restart the spill is truncated to that synced
offset, discarding the entire un-fsynced dirty suffix (whatever its torn shape) — those
rows belong to a batch whose checkpoint never advanced, so resume re-derives them. Any
parse error *within* the synced prefix is treated as real corruption and is fatal.

## Reconciliation gate (correctness gate)

The job can run the gate inline (`-reconcile -from <epoch> -to <epoch>`) using its own
authoritative DLQ count, or you can run `cmd/reconcile` separately. Equation:

```
ES_docs(window) == source_rows(window) - DLQ(window)
```

`raw_excluded` docs still occupy an ES doc (content null) and are NOT subtracted;
revoked/deleted rows keep their ES doc (read-time join) and are NOT subtracted; only
the DLQ rows (which never reached the ES body index) are subtracted. The gate rejects
self-inconsistent inputs (e.g. `DLQ > source_rows`, `raw_excluded > es_docs`) rather
than risk a false OK, and fails (non-zero exit / error) on any mismatch → STOP.

## Suggested ES settings during backfill (octo-deployment)

Per `docs/tuning.md`: `index.refresh_interval=-1`, `index.number_of_replicas=0`
during the load, then restore and force one `_refresh`, then run the gate.

## Build & run

`cmd/backfill` is an on-demand ops tool (like `cmd/reconcile`); it is **not** baked
into the long-running `es-indexer` service image. Build it where you run it:

```sh
go build -o backfill ./cmd/backfill

./backfill \
  -mysql-dsn 'user:pass@tcp(host:3306)/im_prod' \
  -tables message,message1,message2,message3,message4 \
  -es http://opensearch:9200 -es-index octo-message \
  -spill-dir /var/lib/octo-backfill/dlq \
  -checkpoint /var/lib/octo-backfill/checkpoint.json \
  -rate 5000 -batch 1000 \
  -reconcile -from 1709078400 -to 1718323200
```

`-spill-dir` is **required** (real anomalies must be durably accounted). Env-var
equivalents: `BACKFILL_MYSQL_DSN`, `BACKFILL_TABLES`, `BACKFILL_ES`,
`BACKFILL_ES_INDEX`, `BACKFILL_ES_USER`, `BACKFILL_ES_PASS`, `BACKFILL_SPILL_DIR`,
`BACKFILL_CHECKPOINT`, `BACKFILL_BATCH`, `BACKFILL_RATE`.

SIGTERM/SIGINT stops cleanly after the current batch (checkpoint persisted) → resumable.

## 🔴 Isolation discipline

Running backfill against a **real** environment requires explicit Yu/ops sign-off and a
low-peak window. It is never run automatically against production and is decoupled from
code release — code ships through the normal test→prod path first; backfill is a
deployment-time ops action verified in the local harness here. The job only **reads**
the message tables (no schema change, no webhook write path, no binlog); it is
idempotent, resumable, and pairs with a zero-downtime index alias rebuild.
