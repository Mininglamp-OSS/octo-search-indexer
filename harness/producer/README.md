# searchetl-producer e2e harness

Local, throwaway end-to-end verification for the standalone polling ETL producer
(`cmd/searchetl-producer`).

```
message shards (MySQL) -> [searchetl-producer: poll -> enrich -> Kafka] octo.message.v1(.dlq)
```

## What it asserts

`run-producer.sh all` brings up a throwaway **MySQL + Kafka + Redis** stack,
seeds a controlled message fixture (`seed.sql`), and runs the Go verifier
(`harness/producer`) which checks three invariants:

1. **poll → enrich → Kafka** — stable rows land on the body topic with the right
   enrichment (text body, `visibles` whitelist, `raw_excluded` for Signal/media),
   and genuine anomalies + fail-closed visibility violations land on the DLQ
   topic. This is a **field-level** assertion (not a bare doc count): it checks
   the actual contract contents, including the #1124 fail-closed guard that the
   empty-`visibles` row must route to DLQ, never to the body topic.
2. **Redis lock mutual exclusion** — two concurrent `RunIncremental` calls
   sharing the same lock key never both produce; exactly one wins, the other
   skips.
3. **cursor monotonicity** — a second tick over the already-consumed rows
   produces nothing and does not move the cursor backward.

## Usage

```bash
./harness/producer/run-producer.sh          # up -> seed -> verify -> down
KEEP_UP=1 ./harness/producer/run-producer.sh  # leave the stack up after verify
./harness/producer/run-producer.sh down     # tear down (+volumes)
```

## Full-chain reconcile gate

This harness verifies the producer's own output (the write side). The full
realtime chain — **producer → Kafka → es-indexer consumer → OpenSearch →
reconcile gate** — is exercised by composing this with the repo's existing
`harness/run.sh` (the live-ingestion gate is OPEN now that the contract is
SchemaVersion 2): point the es-indexer harness at the same Kafka topics this
producer writes, then run `cmd/reconcile` (or backfill's inline `-reconcile`)
against MySQL + OpenSearch. The reconcile gate (count + field-level sample), not
a bare doc count, is the end-to-end correctness judge for that combined run.

ISOLATION: throwaway local stack only. It is never wired into a shared
environment, and the producer stays opt-in (`PRODUCER_ENABLED`) in real deploys.
