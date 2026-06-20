#!/usr/bin/env bash
# Local end-to-end verification harness for the searchetl-producer.
#
#   message shards (MySQL) -> [searchetl-producer: poll -> enrich -> Kafka]
#                                                            octo.message.v1(.dlq)
#
# Brings up MySQL + Kafka + Redis, seeds a controlled message fixture, then runs
# the producer verifier which asserts: poll->enrich->Kafka, Redis lock mutual
# exclusion across two replicas, and cursor monotonicity. Throwaway local stack
# ONLY — never wired into a shared environment.
#
# Usage:
#   ./harness/producer/run-producer.sh         # up -> seed -> verify -> down
#   ./harness/producer/run-producer.sh up      # bring the stack up
#   ./harness/producer/run-producer.sh down    # tear down (+volumes)
#   KEEP_UP=1 ./harness/producer/run-producer.sh   # leave the stack up after verify
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
cd "$HERE"

MYSQL_PORT="${MYSQL_PORT:-13307}"
KAFKA_EXTERNAL="${KAFKA_EXTERNAL:-localhost:29092}"
REDIS_ADDR="${REDIS_ADDR:-localhost:16379}"
MYSQL_DSN="${PH_MYSQL_DSN:-root:rootpw@tcp(localhost:${MYSQL_PORT})/octo_im_test?parseTime=true}"

compose() { docker compose "$@"; }

up() {
  echo "[ph] starting MySQL + Kafka + Redis (+OpenSearch)..."
  compose up -d --build mysql kafka redis
  echo "[ph] waiting for MySQL..."
  for i in $(seq 1 60); do
    if docker exec octo-prod-harness-mysql mysqladmin ping -h127.0.0.1 -uroot -prootpw >/dev/null 2>&1; then break; fi
    sleep 2
  done
  echo "[ph] waiting for Kafka..."
  for i in $(seq 1 60); do
    if docker exec octo-prod-harness-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then break; fi
    sleep 2
  done
  echo "[ph] waiting for Redis..."
  for i in $(seq 1 30); do
    if docker exec octo-prod-harness-redis redis-cli ping 2>/dev/null | grep -q PONG; then break; fi
    sleep 1
  done
  create_topics
  seed_db
  echo "[ph] stack up."
}

create_topics() {
  echo "[ph] pre-creating topics (body + DLQ)..."
  for t in octo.message.v1 octo.message.v1.dlq; do
    docker exec octo-prod-harness-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
  done
}

seed_db() {
  echo "[ph] seeding message fixture..."
  docker exec -i octo-prod-harness-mysql mysql -uroot -prootpw octo_im_test < seed.sql
}

down() {
  echo "[ph] tearing down..."
  compose down -v --remove-orphans || true
}

verify() {
  echo "[ph] running producer e2e verifier..."
  ( cd "$ROOT" && \
    PH_MYSQL_DSN="$MYSQL_DSN" \
    PH_KAFKA="$KAFKA_EXTERNAL" \
    PH_REDIS="$REDIS_ADDR" \
    go run ./harness/producer )
}

case "${1:-all}" in
  up) up ;;
  down) down ;;
  seed) seed_db ;;
  verify) verify ;;
  all)
    up
    trap 'if [[ "${KEEP_UP:-0}" != "1" ]]; then down; fi' EXIT
    verify
    echo "[ph] e2e run complete."
    ;;
  *) echo "usage: $0 {all|up|down|seed|verify}"; exit 1 ;;
esac
