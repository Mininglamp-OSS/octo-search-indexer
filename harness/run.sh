#!/usr/bin/env bash
# Local end-to-end verification harness for the octo message-search pipeline.
#
#   Kafka octo.message.v1 -> es-indexer consumer -> OpenSearch (analysis-ik)
#
# Brings up Kafka + OpenSearch(IK), runs the es-indexer against them, seeds a
# controlled message suite, and asserts the C2/C4 + idempotency + IK-tokenization
# invariants. Throwaway local stack ONLY — never wired into a shared environment.
#
# 🔒 v1.9 LIVE-INGESTION SAFETY GATE (YUJ-4698 / Jerry-Xin Critical):
#   The live consumer (internal/consumer.Service.Run) refuses to start while the
#   Kafka contract (octo-lib searchmsg.SchemaVersion) does not carry the reader
#   safety fields spaceId/visibles/messageSeq (i.e. SchemaVersion < 2). Writing
#   live docs without those fields would fail-OPEN the reader's visibles gate, so
#   the gate is fail-CLOSED by design and is NOT bypassable from this harness.
#
#   Consequence: at the current contract (SchemaVersion=1) `run.sh all` cannot
#   drive a Kafka->consumer->OpenSearch e2e — the indexer exits before consuming.
#   This script detects that up front (./harness/contractgate) and tells you to
#   use ./harness/run-backfill.sh, which IS the v1.9 e2e path: only backfill can
#   populate the safety fields (read straight from MySQL payload), so it exercises
#   the full v1.9 reader contract end to end. When octo-lib bumps SchemaVersion>=2
#   (phase 9 producer enrichment), the gate auto-unlocks and `run.sh all` drives
#   the live path again — no harness change needed.
#
# Usage:
#   ./harness/run.sh            # full run: up -> indexer -> seed -> verify -> down
#                               #   (auto-skips with guidance while the live gate is closed)
#   ./harness/run.sh up         # just bring the stack up
#   ./harness/run.sh down       # tear the stack down (+volumes)
#   ./harness/run.sh gate       # print the live-ingestion gate state and exit
#   KEEP_UP=1 ./harness/run.sh  # leave the stack running after verify
#   FORCE_LIVE=1 ./harness/run.sh  # (advanced) run live path even if gate reports closed;
#                               #   the indexer still enforces its own gate, so this only
#                               #   makes sense once the contract is bumped to SchemaVersion>=2
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
cd "$HERE"

KAFKA_EXTERNAL="${KAFKA_EXTERNAL:-localhost:19092}"
ES_URL="${ES_URL:-http://localhost:19200}"
ES_INDEX="${ES_INDEX:-octo-message}"

compose() { docker compose "$@"; }

# live_gate_open reports whether the live-ingestion safety gate is currently OPEN
# (contract carries spaceId/visibles/messageSeq, SchemaVersion>=2). It delegates to
# ./harness/contractgate, which imports the SAME predicate the production consumer
# uses (esindex.LiveContractCarriesSafetyFields) so it can never disagree with the
# indexer's own startup check.
#
# contractgate exit codes: 0 = gate OPEN, 10 = gate CLOSED (gated by design). Any
# OTHER non-zero (compile error, broken import, missing Go toolchain) is a broken
# probe, NOT a closed gate — we must fail loudly rather than treat a broken harness
# as a passing skip (would mask real breakage).
#
# NB: we `go build` the probe to a temp binary and run THAT, rather than `go run`:
# `go run` collapses any non-zero program exit to 1, which would make the gated
# exit 10 indistinguishable from a compile error. Building first preserves the
# probe's real exit code (and a build failure is itself surfaced as a probe error).
#
# Returns: 0 = open, 10 = closed-by-design, other = probe error (caller must abort).
live_gate_open() {
  local bin="/tmp/octo-contractgate.bin"
  if ! ( cd "$ROOT" && go build -o "$bin" ./harness/contractgate ) >/tmp/octo-contractgate.out 2>&1; then
    return 3 # build failed → broken probe
  fi
  "$bin" >/tmp/octo-contractgate.out 2>&1
}

gate() {
  echo "[harness] checking live-ingestion safety gate (octo-lib contract)..."
  local rc=0
  live_gate_open || rc=$?
  if [[ $rc -eq 0 ]]; then
    echo "[harness] gate=OPEN — $(cat /tmp/octo-contractgate.out)"
    echo "[harness] live Kafka->consumer->OpenSearch e2e is available."
    return 0
  fi
  if [[ $rc -ne 10 ]]; then
    echo "[harness] FATAL: contract gate probe failed (exit $rc) — this is a broken probe," >&2
    echo "[harness] not a closed gate. Refusing to skip silently. Probe output:" >&2
    sed 's/^/[harness]   /' /tmp/octo-contractgate.out >&2
    return 2
  fi
  echo "[harness] gate=CLOSED — $(cat /tmp/octo-contractgate.out)"
  cat >&2 <<'GATE'
[harness] The live consumer refuses to start at this contract version: the Kafka
[harness] contract does not yet carry the reader safety fields (spaceId/visibles/
[harness] messageSeq), so live ingestion would fail-OPEN the reader's visibles gate.
[harness] This is the v1.9 fail-CLOSED safety gate (by design), not a harness bug.
[harness]
[harness] v1.9 end-to-end verification path → use the backfill harness instead:
[harness]     ./harness/run-backfill.sh
[harness] It populates spaceId/visibles/messageSeq from MySQL payload and asserts the
[harness] full v1.9 reader contract (camelCase nested doc, IK 中文 recall, reconcile gate)
[harness] end to end against real OpenSearch.
[harness]
[harness] When octo-lib bumps SchemaVersion>=2 (phase 9 producer enrichment), this gate
[harness] auto-unlocks and `./harness/run.sh all` drives the live path again.
GATE
  return 1
}

up() {
  echo "[harness] building + starting Kafka + OpenSearch(IK)..."
  compose up -d --build
  echo "[harness] waiting for OpenSearch..."
  for i in $(seq 1 60); do
    if curl -fs "$ES_URL/_cluster/health" >/dev/null 2>&1; then break; fi
    sleep 3
  done
  curl -fs "$ES_URL/_cluster/health?wait_for_status=yellow&timeout=60s" >/dev/null
  echo "[harness] waiting for Kafka..."
  for i in $(seq 1 60); do
    if docker exec octo-harness-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then break; fi
    sleep 3
  done
  create_topics
  echo "[harness] stack up."
}

# create_topics pre-creates the body + DLQ topics. The DLQ topic MUST be
# pre-created because the indexer's DLQ producer runs with
# AllowAutoTopicCreation=false (a deployment-safety choice from phase 4 rework:
# a mistyped topic name should fail loudly, not silently create a wrong topic).
# In production these topics are provisioned by the platform; here we mirror that.
create_topics() {
  echo "[harness] pre-creating topics (body + DLQ)..."
  for t in octo.message.v1 octo.message.v1.dlq; do
    local out rc
    # Capture rc safely under `set -e`: a bare `out="$(cmd)"` would exit the
    # script on non-zero before we can inspect rc, so use an if-guard.
    if out="$(docker exec octo-harness-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic "$t" --partitions 1 --replication-factor 1 2>&1)"; then
      rc=0
    else
      rc=$?
    fi
    # --if-not-exists makes an existing topic a no-op; tolerate only that.
    # Any other non-zero (broker down, auth, bad config) must fail loudly.
    if [[ $rc -ne 0 ]] && ! grep -qiE "already exists" <<<"$out"; then
      echo "[harness] FATAL: creating topic $t failed: $out" >&2
      exit 1
    fi
  done
}

# dlq_end_offset prints the current DLQ topic log-end offset (sum across
# partitions). Used to fence the verifier so it only inspects DLQ records
# produced by THIS run (guards against stale records on a kept-up stack).
dlq_end_offset() {
  docker exec octo-harness-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic octo.message.v1.dlq 2>/dev/null \
    | awk -F: '{s+=$3} END{print s+0}'
}

down() {
  echo "[harness] tearing down..."
  compose down -v --remove-orphans || true
}

# create_index pre-creates the target index from the embedded canonical mapping.
# The indexer no longer auto-creates a missing index (it fail-fasts on 404, see
# issue #29), so the harness must provision it explicitly — mirroring how a real
# deployment pre-creates the index with mapping/ISM/shards/aliases. Idempotent:
# tolerate 200 (created) and a 400 already-exists; anything else fails loud.
create_index() {
  echo "[harness] pre-creating index $ES_INDEX from embedded mapping..."
  local code
  code="$(curl -s -o /dev/null -w "%{http_code}" -XPUT "$ES_URL/$ES_INDEX" \
    -H 'Content-Type: application/json' \
    -d @"$ROOT/internal/esindex/mapping/octo-message.json")"
  if [[ "$code" != "200" && "$code" != "400" ]]; then
    echo "[harness] FATAL: creating index $ES_INDEX failed (HTTP $code)" >&2
    exit 1
  fi
}

run_indexer() {
  create_index
  echo "[harness] starting es-indexer (background)..."
  ( cd "$ROOT" && \
    ES_INDEXER_ENABLED=true \
    KAFKA_BROKERS="$KAFKA_EXTERNAL" \
    KAFKA_TOPIC=octo.message.v1 \
    KAFKA_DLQ_TOPIC=octo.message.v1.dlq \
    KAFKA_GROUP_ID=octo-search-indexer-harness \
    ES_ADDRESSES="$ES_URL" \
    ES_INDEX="$ES_INDEX" \
    INDEXER_DLQ_SPILL_DIR=/tmp/octo-indexer-spill \
    go run ./cmd/es-indexer >/tmp/octo-indexer.log 2>&1 ) &
  INDEXER_PID=$!
  echo "$INDEXER_PID" > /tmp/octo-indexer.pid
  echo "[harness] es-indexer pid=$INDEXER_PID (logs: /tmp/octo-indexer.log)"
  sleep 5
}

stop_indexer() {
  if [[ -f /tmp/octo-indexer.pid ]]; then
    kill "$(cat /tmp/octo-indexer.pid)" 2>/dev/null || true
    rm -f /tmp/octo-indexer.pid
  fi
}

seed() {
  # Fence the DLQ verification to records produced by THIS run: capture the DLQ
  # log-end offset BEFORE seeding so the verifier ignores stale records from a
  # prior kept-up/failed run.
  DLQ_START_OFFSET="$(dlq_end_offset)"
  export DLQ_START_OFFSET
  echo "[harness] DLQ fence start offset = ${DLQ_START_OFFSET}"
  echo "[harness] seeding controlled message suite..."
  ( cd "$ROOT" && KAFKA_BROKERS="$KAFKA_EXTERNAL" go run ./harness/seed -mode suite )
}

verify() {
  echo "[harness] verifying invariants..."
  # If the fence wasn't captured (e.g. standalone `run.sh verify`), pass -1 so the
  # verifier engages its fail-closed default (abort on a non-empty DLQ) rather than
  # silently scanning from offset 0 and risking a stale-record false PASS.
  ( cd "$ROOT" && \
    ES_URL="$ES_URL" ES_INDEX="$ES_INDEX" \
    KAFKA_BROKERS="$KAFKA_EXTERNAL" \
    KAFKA_TOPIC=octo.message.v1 \
    KAFKA_DLQ_TOPIC=octo.message.v1.dlq \
    KAFKA_GROUP_ID=octo-search-indexer-harness \
    DLQ_START_OFFSET="${DLQ_START_OFFSET:--1}" \
    go run ./harness/verify )
}

case "${1:-all}" in
  up) up ;;
  down) down ;;
  gate) gate ;;
  seed) seed ;;
  verify) verify ;;
  all)
    # 🔒 v1.9 gate pre-flight: the live consumer will refuse to start while the
    # contract lacks safety fields, so seed/verify can never pass. Detect that
    # up front and point at the backfill harness instead of bringing up a stack
    # that the indexer would immediately bail out of. FORCE_LIVE=1 overrides for
    # when the contract has been bumped (the indexer still self-enforces the gate).
    #
    # Distinguish closed-by-design (gate rc=1, skip cleanly) from a BROKEN probe
    # (gate rc>=2, e.g. compile error): a broken probe must abort, not masquerade
    # as a passing skip.
    if [[ "${FORCE_LIVE:-0}" != "1" ]]; then
      grc=0
      gate || grc=$?
      if [[ $grc -ge 2 ]]; then
        echo "[harness] aborting: contract gate probe is broken (see error above)." >&2
        exit "$grc"
      fi
      if [[ $grc -ne 0 ]]; then
        echo "[harness] skipping live e2e (gate closed). See guidance above." >&2
        exit 0
      fi
    fi
    up
    run_indexer
    trap 'stop_indexer' EXIT
    seed
    verify
    stop_indexer
    if [[ "${KEEP_UP:-0}" != "1" ]]; then down; fi
    echo "[harness] e2e run complete."
    ;;
  *) echo "usage: $0 {all|up|down|gate|seed|verify}"; exit 1 ;;
esac
