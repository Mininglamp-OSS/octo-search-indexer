# es-indexer bulk tuning — recommended baseline

Recommendations from the local e2e harness (`harness/`), to be re-validated at
deployment scale in phase 6. The harness is a single-node throwaway stack (Kafka
KRaft single broker + OpenSearch single node with `analysis-ik`), so absolute
throughput numbers are a **floor**, not a capacity plan — the value here is the
relative trade-offs and a safe starting config.

## What was measured

- Pipeline: `seed → Kafka octo.message.v1 → es-indexer consumer → OpenSearch`.
- Workload: 5,000 contract messages (~Chinese body, `ik_max_word` indexed).
- Observed: ~5,000 docs indexed end-to-end in ~1.4 s at `INDEXER_BATCH_SIZE=500`
  (≈3.5k docs/s) on the harness box, lag returning to 0, offset committed to the
  log end, ES `_count` matching expected (5 suite + 5000 bulk = 5005, 2 poison
  pills routed to the DLQ topic, 1 `raw_excluded` doc present).
- Idempotency confirmed: duplicate `message_id` → exactly one ES doc.

## Knobs (env on the indexer)

| Env | Default | Meaning | Guidance |
| --- | --- | --- | --- |
| `INDEXER_BATCH_SIZE` | 500 | max docs per `_bulk` request | 500–2000. Larger = higher throughput + better amortised round-trips, but bigger bulk bodies and coarser retry granularity (a whole batch re-bulks its unresolved subset on transient). At ~700 B/doc (prod avg), 1000 docs ≈ 0.7 MB body — well within limits. **Start 1000.** |
| `INDEXER_TRANSIENT_BACKOFF_MS` | 1000 | base for transient in-place retry (exp + full jitter, capped 30 s) | Keep ~1 s. Jitter already de-correlates replicas; raising it slows recovery, lowering it risks hammering a degraded ES. |
| `INDEXER_DLQ_MAX_RETRIES` | 5 | DLQ-write retries before terminal escape | Keep 5. With exp backoff this is ~tens of seconds of tolerance for a DLQ blip before spill/hard-stop. |
| `INDEXER_DLQ_RETRY_BACKOFF_MS` | 200 | base for DLQ-write retry backoff | Keep. |
| `INDEXER_DLQ_SPILL_DIR` | "" | enable spill-to-disk terminal escape | **Set in production** to a durable, monitored path so a DLQ outage can't wedge the partition; leave empty only where a hard-stop+page is preferred. |

## OpenSearch-side (owned by octo-deployment, not the indexer binary)

| Setting | Recommendation | Why |
| --- | --- | --- |
| `index.refresh_interval` | `30s` (steady state); `-1` during phase-6 backfill, then restore + force `_refresh` | The indexer does not force per-batch refresh, so search visibility lags by `refresh_interval`. For bulk backfill, disabling refresh then re-enabling once is materially faster. |
| `index.number_of_replicas` | `0` during backfill, raise to `1` after | Fewer replicas = faster bulk; restore for HA once loaded. |
| `index.translog.durability` | `async` acceptable for a derived read model (SoR is MySQL) | The index is rebuildable from MySQL, so trading a tiny crash-window for throughput is safe. |
| bulk concurrency | drive parallelism via **Kafka partitions + indexer replicas**, one consumer per partition | The writer issues one `_bulk` per batch synchronously; horizontal scale comes from partitions/replicas (each replica owns partitions, C3/C4 keep per-partition ordered commit correct), not in-process bulk fan-out. Keeps the failure model simple. |

## Phase-6 backfill starting point

- `INDEXER_BATCH_SIZE=1000`, ES `refresh_interval=-1`, `number_of_replicas=0`,
  rate-limit ≤5k docs/s (matches the phase-6 plan), then restore index settings
  and force one `_refresh`, then run the reconcile gate (below).
- Backfill reuses `internal/esindex` (writer + `EnsureIndex` mapping) and
  `internal/recon` (the gate) directly — no ES code is re-implemented.

## Reconciliation gate

`cmd/reconcile` (and `internal/recon`) implement the phase-6 correctness gate:

```
ES_docs(window) == source_rows(window) - DLQ(window)
```

`raw_excluded` docs (Signal/non-text) **still occupy an ES doc** (content null),
so they are NOT subtracted. Revoked/deleted rows also keep their ES doc (route
甲 filters at read time), so they are not subtracted either. Verified against the
live harness OpenSearch: `5007 source - 2 DLQ == 5005 ES docs → OK (diff 0)`.
