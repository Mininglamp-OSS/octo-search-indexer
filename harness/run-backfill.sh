#!/usr/bin/env bash
# Local end-to-end verification for the phase-6 historical backfill (cmd/backfill).
#
#   MySQL message shards --(cmd/backfill: keyset scan -> esindex.Writer bulk)--> OpenSearch
#
# Brings up a throwaway MySQL + OpenSearch(IK), seeds a controlled message suite into
# the shard tables, runs cmd/backfill (bypassing Kafka), runs the reconcile gate inline,
# and asserts ES doc count / raw_excluded / DLQ-bypass + IK 中文 recall. Throwaway local
# stack ONLY — never wired into a shared environment.
#
# Usage:
#   ./harness/run-backfill.sh        # up -> seed -> backfill -> verify -> down
#   KEEP_UP=1 ./harness/run-backfill.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
cd "$ROOT"

MYSQL_DSN="${BF_MYSQL_DSN:-root:root@tcp(localhost:23307)/im_test}"
ES_URL="${BF_ES:-http://localhost:29200}"
ES_INDEX="${BF_ES_INDEX:-octo-message}"
TABLES="${BF_TABLES:-message,message1}"
SPILL="${BF_SPILL_DIR:-/tmp/octo-backfill-spill}"
CHECKPOINT="${BF_CHECKPOINT:-/tmp/octo-backfill-cp.json}"

up() {
  echo "[backfill-harness] starting throwaway OpenSearch(IK) + MySQL..."
  docker rm -f octo-bf-os octo-bf-mysql >/dev/null 2>&1 || true
  docker run -d --name octo-bf-os \
    -e discovery.type=single-node -e bootstrap.memory_lock=true \
    -e OPENSEARCH_JAVA_OPTS="-Xms512m -Xmx512m" \
    -e DISABLE_SECURITY_PLUGIN=true -e DISABLE_INSTALL_DEMO_CONFIG=true \
    --ulimit memlock=-1:-1 -p 29200:9200 \
    "$(build_os_image)" >/dev/null
  docker run -d --name octo-bf-mysql \
    -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=im_test \
    -p 23307:3306 mysql:8.0 >/dev/null
  echo "[backfill-harness] waiting for OpenSearch..."
  for _ in $(seq 1 40); do curl -fs "$ES_URL/_cluster/health" >/dev/null 2>&1 && break; sleep 3; done
  echo "[backfill-harness] waiting for MySQL..."
  for _ in $(seq 1 40); do docker exec octo-bf-mysql mysqladmin ping -uroot -proot >/dev/null 2>&1 && break; sleep 3; done
  echo "[backfill-harness] stack up."
}

# build_os_image reuses the IK-enabled OpenSearch image from the main harness.
build_os_image() {
  docker build -q -f "$HERE/opensearch-ik.Dockerfile" --build-arg OPENSEARCH_VERSION=2.17.0 "$HERE" >&2
  docker build -q -f "$HERE/opensearch-ik.Dockerfile" --build-arg OPENSEARCH_VERSION=2.17.0 "$HERE"
}

down() {
  echo "[backfill-harness] tearing down..."
  docker rm -f octo-bf-os octo-bf-mysql >/dev/null 2>&1 || true
}

# create_index pre-creates the target index from the embedded canonical mapping.
# cmd/backfill no longer auto-creates a missing index (fail-fasts on 404, see
# issue #29), so the harness provisions it explicitly. Idempotent: tolerate 200
# (created) and 400 already-exists; anything else fails loud.
create_index() {
  echo "[backfill-harness] pre-creating index $ES_INDEX from embedded mapping..."
  local code
  code="$(curl -s -o /dev/null -w "%{http_code}" -XPUT "$ES_URL/$ES_INDEX" \
    -H 'Content-Type: application/json' \
    -d @"$ROOT/internal/esindex/mapping/octo-message.json")"
  if [[ "$code" != "200" && "$code" != "400" ]]; then
    echo "[backfill-harness] FATAL: creating index $ES_INDEX failed (HTTP $code)" >&2
    exit 1
  fi
}

seed() {
  BASE_TS="$(date +%s)"
  export BASE_TS
  go run ./harness/backfill -mode seed -mysql-dsn "$MYSQL_DSN" -tables "$TABLES" -base-ts "$BASE_TS"
}

backfill() {
  echo "[backfill-harness] running cmd/backfill (bypass Kafka) + inline reconcile gate..."
  rm -rf "$SPILL" "$CHECKPOINT"
  go run ./cmd/backfill \
    -mysql-dsn "$MYSQL_DSN" -tables "$TABLES" \
    -es "$ES_URL" -es-index "$ES_INDEX" \
    -spill-dir "$SPILL" -checkpoint "$CHECKPOINT" \
    -rate 5000 -batch 1000 \
    -reconcile -from "$((BASE_TS - 10))" -to "$((BASE_TS + 100))"
}

verify() {
  echo "[backfill-harness] verifying ES result..."
  go run ./harness/backfill -mode verify -es "$ES_URL" -es-index "$ES_INDEX" -base-ts "$BASE_TS"
}

case "${1:-all}" in
  up) up ;;
  down) down ;;
  seed) seed ;;
  backfill) backfill ;;
  verify) verify ;;
  all)
    up
    trap 'if [[ "${KEEP_UP:-0}" != "1" ]]; then down; fi' EXIT
    create_index
    seed
    backfill
    verify
    echo "[backfill-harness] backfill e2e complete."
    ;;
  *) echo "usage: $0 {all|up|down|seed|backfill|verify}"; exit 1 ;;
esac
