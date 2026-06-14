#!/usr/bin/env bash
# Local end-to-end verification harness for the octo message-search pipeline.
#
#   Kafka octo.message.v1 -> es-indexer consumer -> OpenSearch (analysis-ik)
#
# Brings up Kafka + OpenSearch(IK), runs the es-indexer against them, seeds a
# controlled message suite, and asserts the C2/C4 + idempotency + IK-tokenization
# invariants. Throwaway local stack ONLY — never wired into a shared environment.
#
# Usage:
#   ./harness/run.sh            # full run: up -> indexer -> seed -> verify -> down
#   ./harness/run.sh up         # just bring the stack up
#   ./harness/run.sh down       # tear the stack down (+volumes)
#   KEEP_UP=1 ./harness/run.sh  # leave the stack running after verify
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
cd "$HERE"

KAFKA_EXTERNAL="${KAFKA_EXTERNAL:-localhost:19092}"
ES_URL="${ES_URL:-http://localhost:19200}"
ES_INDEX="${ES_INDEX:-octo-message}"

compose() { docker compose "$@"; }

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

run_indexer() {
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
  seed) seed ;;
  verify) verify ;;
  all)
    up
    run_indexer
    trap 'stop_indexer' EXIT
    seed
    verify
    stop_indexer
    if [[ "${KEEP_UP:-0}" != "1" ]]; then down; fi
    echo "[harness] e2e run complete."
    ;;
  *) echo "usage: $0 {all|up|down|seed|verify}"; exit 1 ;;
esac
