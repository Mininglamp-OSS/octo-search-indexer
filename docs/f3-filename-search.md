# S14 F3 attachment filename search

This change ships with the matching `octo-server` query changes. It does not
change permissions, message identity, visibility revalidation, fulltext analysis,
or frontend interfaces.

## Index contract

`payload.file.name` remains the original filename and retains its IK mapping for
T2 cross-field fulltext queries. `FilePayload.MarshalJSON` additionally writes
`payload.file.nameLiteral`: `cases.Fold(width.Fold(name))`, using x/text v0.32.0,
without trimming the stored name. Only the query's edge whitespace is trimmed.
No NFKC, transliteration, simplified/traditional conversion, or wildcard syntax
is added.

`filename_literal` uses an ngram tokenizer with min=max=1 and `token_chars: []`.
Every Unicode character, including whitespace/punctuation, has its own position.
The reader uses a zero-slop `match_phrase` over the folded query. This is an
internal exact-substring implementation, not a user-facing phrase-search feature.
It works on OpenSearch 2.11; it does not require the later `wildcard` field type.
See the [OpenSearch tokenizer contract](https://docs.opensearch.org/latest/analyzers/tokenizers/ngram/).

Search executes against postings and positions. It does not pull all filenames
into the application or execute a per-document `_source` script. Index cost is
linear in filename length; very common one-character searches can still have
large posting lists and must be measured against production data.

## Coordinated rollout

Do not deploy the new reader against an unbackfilled index: missing derived
fields would silently omit filename-only matches. The indexer's existing startup
mapping guard now requires the new field and its analyzer. Old writers cannot
maintain it. A mixed-version write rollout is unsupported.

1. Pause all writers to the old concrete index (indexer, extractor and backfill
   jobs). Keep the old reader serving the old read alias. Record both write-index
   and read-alias targets. Do not change or delete the old index.
2. Create a **new concrete index** from `internal/esindex/mapping/octo-message.json`
   (adjust shard/replica settings for the environment). Reindex old to new without
   a transformation script, retaining IDs, raw payloads, visibility, extracted
   body and metadata. Check task completion, failures and document counts. Do not
   use the legacy flat-schema `scripts/forward-migrate.sh` for this migration.
3. Run `/usr/local/bin/filename-backfill -es "$BACKFILL_ES" -index NEW_INDEX
   -dry-run -page-size 200 -rate 100` first. This is read-only: it checks the
   concrete index, mapping, analyzer settings and runtime character positions,
   scans only names/derived values, and reports the repair count plus the rate
   lower bound. It rejects aliases, incomplete source storage and incompatible
   analysis even when there are zero documents. Measure a representative isolated
   copy before choosing rate and timeout; defaults are 100 updates/s and 12 hours.
   The lower bound excludes scans, verification, initial reindex and server work.
   Then rerun without `-dry-run`, with an explicit measured `-timeout`. The image
   includes the binary; `go run ./cmd/filename-backfill` also works. Credentials
   come from `BACKFILL_ES_USER`/`BACKFILL_ES_PASS`; TLS verification stays enabled.
   Require successful final verification and `invalid=0` before cutover.
4. Point new writers to the new index, atomically move the read alias to that
   single index, then deploy the new reader. Resume ingestion and verify fresh
   filenames. Before new writes resume, rollback can restore the old alias and
   writer target. After resuming, rollback needs reconciliation of new writes;
   blindly pointing to the old index would lose searchable recent content.

The backfill scans only names and derived values in pages of 1..500. It repairs
both missing and incorrect derived values, and skips correct values on rerun.
The update transport has one in-flight bulk request, at most
`min(page-size, rate)` items and 1 MiB encoded payload per request. Rate remains a
1..1000 updates/s upper bound; time waiting for network/server work lowers actual
throughput. Oversized individual updates fail before sending. Every bulk item is
checked independently; failures stop the job with a nonzero exit, retaining
successful work for a safe rerun. `_update` still reindexes the stored document,
including its extracted body: batching removes roundtrips, not that server cost.

Updates use exact IDs (including virtual IDs), never upsert, and compare the
current raw filename and payload type. Concurrent deletion is skipped; rename
or an already-correct value becomes a no-op. No visibility/body fields are
submitted. After updating, the job refreshes the target and performs a second
bounded scan checking **every eligible derived value**, checks indexed-field
coverage, and executes literal phrase queries for up to eight actual samples.
This also detects values present in `_source` but absent from postings. Incorrect
remaining values, partial shard responses or failed search samples make the job
fail. Keep writers paused throughout this snapshot-and-cutover process; this is
not a transactional certificate under continuous writes.

An offline full filename audit on each run is intentional: an `exists`-only
selection cannot detect stale nonempty derived values. This does not fetch bodies
or add a scan to interactive search. A dry-run itself scans the filename corpus;
it is not a constant-time estimate. Scroll cleanup runs on success/failure, and
cancellation interrupts rate waits. Index creation, reindex, pause/resume and alias
switch remain explicit operator actions; the tool performs none of them.

## Local capacity evidence (2026-11-04)

OpenSearch 2.11 + IK, isolated single node, 512 MiB JVM heap; 2,000 documents per
index with 8,640-byte extracted bodies. Same 1,000 updates/s cap, serial run first:

| Path | Update requests | Elapsed | Observed documents/s |
| --- | ---: | ---: | ---: |
| Previous serial loop | 2,000 | 10.306 s | 194.1 |
| Bounded bulk plus final verification | 10 | 3.737 s | 535.1 |

This is a local ordered comparison, not a production SLA or a complete migration
benchmark. It excludes initial reindex, uses a repetitive synthetic body, and does
not measure production shard counts, concurrent search, or disk merge pressure.
At the **default** 100/s cap, one million repairs still need at least 2.8 hours.
Use the dry-run and measured copy to budget the full write pause; do not infer that
bulk makes a large migration fit a short maintenance window. Reproduce with
`F3_OPENSEARCH_URL=... F3_CAPACITY_DOCS=2000 go test ./internal/filenamebackfill
-run TestFilenameBackfillCapacity -count=1 -v` (opt-in, max 10,000 documents/index).

## Acceptance

| Filename / source | Query | Expected |
| --- | --- | --- |
| `ＯＣＴＯ方案ｖ１．２.pdf` | `octo`, `cto`, `v1.2` | Hit; original filename preserved |
| Same filename | `v12` | No filename-only hit |
| `排期与评审.pdf`, no matching body | `排期 评审` | No hit |
| `排期.pdf` with caption `评审` | `排期 评审` | Existing joint fulltext hit retained |
| `100%_完成*?.pdf` | `%_`, `*?` | Literal hits, no wildcard interpretation |
| Two different attachments with the same name | Matching fragment | Both identities retained |
| Matching filename while body pending/unsupported | Matching fragment | Hit without inventing body evidence |

Run `go test -race -shuffle=on ./...`. With an isolated OpenSearch + IK instance,
set `F3_OPENSEARCH_URL` and run `go test ./internal/filenamebackfill`: the live test
covers bounded paging/bulk, preflight rejection, repair/verification, restart/idempotence, concurrent rename/deletion/revocation,
and preservation of extracted body/metadata. The companion server's
`TestF3FilenameOpenSearch` consumes this exact mapping through
`F3_INDEXER_MAPPING` and verifies four query entry points with real IK analysis.
