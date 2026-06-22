#!/usr/bin/env bash
# Plan v2.1 gap-table end-to-end regression (API-only, zero browser).
#
# Drives the FULL Plan B (CDC-style writes) live path against a throwaway local
# stack (Kafka + OpenSearch-IK from harness/docker-compose.yml) and machine-checks
# every row of the plan v2.1 gap table + the P0 visibility fail-closed safety闸:
#
#   0. P0 migrate-forward fail-closed gate: a stale-mapping index makes es-indexer
#      REFUSE to start (AssertLiveMappingCompatible) → proves `make migrate-forward`
#      is a hard pre-req, not optional.
#   1. make migrate-forward: new contract index (payloadRaw enabled:false +
#      image/file/mergeForward.{from,timestamp}/voice/video/richText, dynamic:strict)
#      + read-alias switch.
#   2. es-indexer live: Kafka octo.message.v1 -> consumer (RawPayload projection +
#      ExtractVisibility fail-closed pre-check) -> OpenSearch.
#   3. Gap-table reader-DSL recall: image/file/mergeForward(from/searchText/ts)/
#      voice/video/richText + top-level messageId/messageSeq/channelId/channelType/
#      from/timestamp/spaceId + snowflake precision + payloadRaw retention.
#   4. P0 security: valid visibles indexed; empty/null/non-string visibles -> DLQ
#      (visibility_untrusted, NOT fail-OPEN); broadcast (no visibles key) allowed;
#      encrypted DM (RawExcluded) parity (no payload, empty visibles).
#
# ISOLATION: throwaway local stack ONLY (harness project). Never wired into any
# shared/real environment. The standalone backfill existing-data path lives in
# harness/run-backfill.sh.
#
# Usage:
#   ./harness/gaptable-e2e.sh           # full run: up -> migrate -> seed -> assert -> down
#   KEEP_UP=1 ./harness/gaptable-e2e.sh # leave the stack up after asserting
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
cd "$ROOT"

ES_URL="${ES_URL:-http://localhost:19200}"
KAFKA_EXTERNAL="${KAFKA_EXTERNAL:-localhost:19092}"
NEW_INDEX="${NEW_INDEX:-wukongim-messages-000001}"
ALIAS="${ALIAS:-wukongim-messages-read}"
OLD_INDEX="${OLD_INDEX:-octo-message}"
INDEXER_LOG=/tmp/octo-gaptable-indexer.log
INDEXER_PID_FILE=/tmp/octo-gaptable-indexer.pid

# BIN_DIR (optional): a directory of pre-built binaries (es-indexer, gaptable).
# When set, the script runs those instead of `go run` (handy in CI / sandboxes
# where `go run`'s child-process signal handling is awkward). When empty the
# script self-builds them once into a temp dir.
BIN_DIR="${BIN_DIR:-}"
if [ -z "$BIN_DIR" ]; then
  BIN_DIR="$(mktemp -d)"
  echo "[gaptable] building binaries into $BIN_DIR ..."
  go build -o "$BIN_DIR/es-indexer" ./cmd/es-indexer
  go build -o "$BIN_DIR/gaptable" ./harness/gaptable
fi
ES_INDEXER_BIN="$BIN_DIR/es-indexer"
GAPTABLE_BIN="$BIN_DIR/gaptable"

PASS=0; FAIL=0
ok(){ echo "PASS | $1"; PASS=$((PASS+1)); }
no(){ echo "FAIL | $1"; FAIL=$((FAIL+1)); }

jqid(){ python3 -c "import json,sys;r=json.load(sys.stdin);print(','.join(h['_id'] for h in r['hits']['hits']))"; }

recall(){ # label dsl want
  local got; got=$(curl -s "$ES_URL/$ALIAS/_search" -H 'Content-Type: application/json' -d "$2" | jqid)
  if echo ",$got," | grep -q ",$3,"; then ok "$1 -> $3"; else no "$1 -> want $3 got [$got]"; fi
}
src(){ curl -s "$ES_URL/$NEW_INDEX/_doc/$1"; } # fetch _doc json

stop_indexer(){ [ -f "$INDEXER_PID_FILE" ] && { kill "$(cat "$INDEXER_PID_FILE")" 2>/dev/null||true; rm -f "$INDEXER_PID_FILE"; }; }
down(){ echo "[gaptable] tearing down..."; stop_indexer; ( cd "$HERE" && docker compose down -v --remove-orphans ) || true; }

echo "[gaptable] === version assert ==="
git rev-parse HEAD
grep octo-lib go.mod

echo "[gaptable] === bring up throwaway Kafka + OpenSearch(IK) ==="
( cd "$HERE" && docker compose up -d --build )
for _ in $(seq 1 60); do curl -fs "$ES_URL/_cluster/health" >/dev/null 2>&1 && break; sleep 3; done
curl -fs "$ES_URL/_cluster/health?wait_for_status=yellow&timeout=60s" >/dev/null
for _ in $(seq 1 60); do docker exec octo-harness-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 && break; sleep 3; done
for t in octo.message.v1 octo.message.v1.dlq; do
  docker exec octo-harness-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
done
[ "${KEEP_UP:-0}" = "1" ] || trap down EXIT

echo "[gaptable] === STEP 0: P0 migrate-forward fail-closed gate ==="
curl -s -XDELETE "$ES_URL/$OLD_INDEX" >/dev/null 2>&1 || true
curl -s -XDELETE "$ES_URL/$NEW_INDEX" >/dev/null 2>&1 || true
STALE='{"mappings":{"dynamic":"strict","properties":{"messageId":{"type":"long"},"payload":{"type":"object","properties":{"type":{"type":"integer"},"text":{"type":"object","properties":{"content":{"type":"text"}}}}}}}}'
curl -s -XPUT "$ES_URL/$OLD_INDEX" -H 'Content-Type: application/json' -d "$STALE" >/dev/null
if env ES_INDEXER_ENABLED=true KAFKA_BROKERS="$KAFKA_EXTERNAL" KAFKA_TOPIC=octo.message.v1 KAFKA_DLQ_TOPIC=octo.message.v1.dlq KAFKA_GROUP_ID=octo-gaptable-gate ES_ADDRESSES="$ES_URL" ES_INDEX="$OLD_INDEX" INDEXER_DLQ_SPILL_DIR=/tmp/octo-e2e-gate-spill timeout 30 "$ES_INDEXER_BIN" >/tmp/octo-gate.log 2>&1; then
  no "STEP0 stale-mapping gate (indexer should have refused to start)"
else
  if grep -q 'mapping-compat FAILED' /tmp/octo-gate.log && grep -q 'make migrate-forward' /tmp/octo-gate.log; then
    ok "STEP0 stale-mapping gate: es-indexer fail-closed, demands make migrate-forward"
  else
    no "STEP0 stale-mapping gate: indexer exited but not with mapping-compat failure"; cat /tmp/octo-gate.log
  fi
fi

echo "[gaptable] === STEP 1: make migrate-forward ==="
ES="$ES_URL" ALIAS="$ALIAS" OLD_INDEX="$OLD_INDEX" NEW_INDEX="$NEW_INDEX" STEP=1 ./scripts/forward-migrate.sh >/dev/null 2>&1
ES="$ES_URL" ALIAS="$ALIAS" OLD_INDEX="$OLD_INDEX" NEW_INDEX="$NEW_INDEX" STEP=2 ./scripts/forward-migrate.sh >/dev/null 2>&1
ES="$ES_URL" ALIAS="$ALIAS" OLD_INDEX="$OLD_INDEX" NEW_INDEX="$NEW_INDEX" STEP=3 ./scripts/forward-migrate.sh >/dev/null 2>&1
MF=$(curl -s "$ES_URL/$NEW_INDEX/_mapping")
echo "$MF" | python3 -c "import json,sys;m=json.load(sys.stdin)['$NEW_INDEX']['mappings']['properties'];assert m['payloadRaw']['enabled'] is False;mf=m['payload']['properties']['mergeForward']['properties']['msgs']['properties'];assert 'from' in mf and 'timestamp' in mf;assert 'richText' in m['payload']['properties']" \
  && ok "STEP1 migrate-forward: new mapping has payloadRaw(enabled:false)+mergeForward.{from,timestamp}+richText" \
  || no "STEP1 migrate-forward: new mapping missing required fields"
A=$(curl -s "$ES_URL/_alias/$ALIAS" | python3 -c "import json,sys;print(list(json.load(sys.stdin).keys()))")
echo "$A" | grep -q "$NEW_INDEX" && ok "STEP1 read alias -> $NEW_INDEX" || no "STEP1 read alias not on $NEW_INDEX (got $A)"

echo "[gaptable] === STEP 2: start es-indexer (mapping-compat passes) ==="
nohup env ES_INDEXER_ENABLED=true KAFKA_BROKERS="$KAFKA_EXTERNAL" KAFKA_TOPIC=octo.message.v1 KAFKA_DLQ_TOPIC=octo.message.v1.dlq KAFKA_GROUP_ID=octo-gaptable-e2e ES_ADDRESSES="$ES_URL" ES_INDEX="$NEW_INDEX" INDEXER_DLQ_SPILL_DIR=/tmp/octo-e2e-spill "$ES_INDEXER_BIN" >"$INDEXER_LOG" 2>&1 &
echo $! > "$INDEXER_PID_FILE"
sleep 12
grep -q 'es-indexer running' "$INDEXER_LOG" && ! grep -q 'exited with error' "$INDEXER_LOG" \
  && ok "STEP2 es-indexer started against migrated index (startup mapping-compat + safety gate OK)" \
  || { no "STEP2 es-indexer failed to start"; cat "$INDEXER_LOG"; }

echo "[gaptable] === seed gap-table vectors ==="
# Fence the DLQ assertion to records produced by THIS run (a kept-up/reused stack
# may carry stale DLQ records from a prior run): capture the DLQ log-end offset
# BEFORE seeding and count only records at/after it.
DLQ_START="$(docker exec octo-harness-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic octo.message.v1.dlq 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
echo "[gaptable] DLQ fence start offset = ${DLQ_START}"
KAFKA_BROKERS="$KAFKA_EXTERNAL" KAFKA_TOPIC=octo.message.v1 "$GAPTABLE_BIN"
sleep 8
curl -s -XPOST "$ES_URL/$NEW_INDEX/_refresh" >/dev/null

echo "[gaptable] === STEP 3: gap-table reader-DSL recall ==="
recall "payload.image.caption"            '{"query":{"match":{"payload.image.caption":"图片说明文字甲"}}}' 3000000000000000001
recall "payload.image.name"               '{"query":{"match":{"payload.image.name":"季度合同"}}}'        3000000000000000001
recall "payload.file.caption"             '{"query":{"match":{"payload.file.caption":"文件说明文字乙"}}}' 3000000000000000002
recall "payload.file.name"                '{"query":{"match":{"payload.file.name":"年度报告"}}}'         3000000000000000002
recall "payload.file.extension"           '{"query":{"term":{"payload.file.extension":"pdf"}}}'           3000000000000000002
recall "mergeForward.msgs.searchText(text)" '{"query":{"match":{"payload.mergeForward.msgs.searchText":"转发卡内文字丙"}}}' 3000000000000000003
recall "mergeForward.msgs.searchText(file)" '{"query":{"match":{"payload.mergeForward.msgs.searchText":"内层文件丁"}}}'   3000000000000000003
recall "mergeForward.msgs.from"           '{"query":{"term":{"payload.mergeForward.msgs.from":"u_inner_丙"}}}' 3000000000000000003
recall "richText.searchText(text)"        '{"query":{"match":{"payload.richText.searchText":"富文本正文戊"}}}' 3000000000000000006
recall "richText.searchText(image-name)"  '{"query":{"match":{"payload.richText.searchText":"富文本图片己"}}}' 3000000000000000006

src 3000000000000000007 | python3 -c "import json,sys;d=json.load(sys.stdin)['_source'];import sys as s;ok=d['messageId']==3000000000000000007 and d['messageSeq']==17 and d['channelId']=='g_gap' and d['channelType']==2 and d['from']=='u_top' and d['spaceId']=='space_top' and d['timestamp']>0;sys.exit(0 if ok else 1)" \
  && ok "top-level messageId/messageSeq/channelId/channelType/from/timestamp/spaceId" || no "top-level fields"
src 3000000000000000004 | python3 -c "import json,sys;p=json.load(sys.stdin)['_source']['payload'];sys.exit(0 if p['type']==4 and p['voice']['url'] else 1)" && ok "voice projection" || no "voice projection"
src 3000000000000000005 | python3 -c "import json,sys;p=json.load(sys.stdin)['_source']['payload'];sys.exit(0 if p['type']==5 and p['video']['second']==42 else 1)" && ok "video projection" || no "video projection"
src 3000000000000000003 | python3 -c "import json,sys;mf=json.load(sys.stdin)['_source']['payload']['mergeForward'];ids=[m['messageId'] for m in mf['msgs']];sys.exit(0 if mf['childCount']==2 and 9007199254740993 in ids and mf['msgs'][0]['timestamp']==1700000123 else 1)" && ok "mergeForward childCount+timestamp+snowflake precision" || no "mergeForward childCount/snowflake"
src 3000000000000000001 | python3 -c "import json,sys;pr=json.load(sys.stdin)['_source'].get('payloadRaw');sys.exit(0 if pr and pr.get('width')==800 else 1)" && ok "payloadRaw retained" || no "payloadRaw retained"

echo "[gaptable] === STEP 4: P0 visibility fail-closed security ==="
src 3000000000000000010 | python3 -c "import json,sys;d=json.load(sys.stdin);sys.exit(0 if d['found'] and d['_source']['visibles']==['u_admin1','u_admin2'] else 1)" && ok "valid visibles indexed+populated" || no "valid visibles"
for id in 3000000000000000011 3000000000000000012 3000000000000000013; do
  f=$(src "$id" | python3 -c "import json,sys;print(json.load(sys.stdin).get('found'))")
  [ "$f" = "False" ] && ok "fail-closed $id absent from index (not fail-OPEN)" || no "fail-closed $id still indexed"
done
src 3000000000000000014 | python3 -c "import json,sys;d=json.load(sys.stdin);sys.exit(0 if d['found'] and not d['_source'].get('visibles') else 1)" && ok "broadcast (no visibles key) allowed, no gate" || no "broadcast allowed"
src 3000000000000000020 | python3 -c "import json,sys;d=json.load(sys.stdin);s=d.get('_source',{});sys.exit(0 if d['found'] and s.get('rawExcluded') and not s.get('payload') and not s.get('visibles') else 1)" && ok "encrypted DM parity (rawExcluded, no payload, empty visibles)" || no "encrypted DM parity"

DLQ_END="$(docker exec octo-harness-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic octo.message.v1.dlq 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')"
DLQ_NEW=$(( DLQ_END - DLQ_START ))
# Cross-check the reason on the new records (single-partition topic → offset slice
# from DLQ_START is exactly this run's records).
DLQ_VU=$(docker exec octo-harness-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic octo.message.v1.dlq --offset "$DLQ_START" --partition 0 --max-messages "$DLQ_NEW" --timeout-ms 8000 2>/dev/null | grep -c 'visibility_untrusted' || true)
{ [ "$DLQ_NEW" -eq 3 ] && [ "$DLQ_VU" -eq 3 ]; } && ok "DLQ has exactly 3 NEW visibility_untrusted records (fenced from offset $DLQ_START)" || no "DLQ new=$DLQ_NEW visibility_untrusted=$DLQ_VU want 3/3"

echo
echo "[gaptable] ============== RESULT: PASS=$PASS FAIL=$FAIL =============="
[ "$FAIL" -eq 0 ] || exit 1
