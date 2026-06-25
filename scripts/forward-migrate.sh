#!/usr/bin/env bash
# 前向迁移三步（YUJ-4682 步骤 6 / YUJ-4662 §4 步骤 6 + Yu 裁决 Coda 补遗）：
#   ① 写新契约索引（mapping v1.9，camelCase 嵌套 + spaceId/visibles/messageSeq + IK + alias）
#   ② 存量 doc reindex 旧索引 → 新契约索引
#   ③ read alias 原子切换到新索引
# 禁止半新半旧 doc 同 alias 服务：reindex 完成、抽样对账通过后才切 alias。
# 回滚：保留旧索引（默认 7 天），alias 可秒切回（见末尾 ROLLBACK）。
#
# 这是一个**受控运维脚本**（非生产自动执行；本票只到代码 + 本地/预发验证层面，
# 真实生产开 Kafka.On + 灰度放量归阶段 9）。所有破坏性操作前打印并要求确认。
set -euo pipefail

ES="${ES:-http://localhost:9200}"
ALIAS="${ALIAS:-wukongim-messages-read}"
OLD_INDEX="${OLD_INDEX:-octo-message}"                  # 旧契约索引（flat snake_case）
NEW_INDEX="${NEW_INDEX:-wukongim-messages-000001}"      # 新契约索引（v1.9）
MAPPING_FILE="${MAPPING_FILE:-internal/esindex/mapping/octo-message.json}"
CURL=(curl -sS -H 'Content-Type: application/json')
[ -n "${ES_USER:-}" ] && CURL+=(-u "${ES_USER}:${ES_PASS:-}")

log(){ printf '\n=== %s ===\n' "$*"; }

# mapping 文件本身**不再内嵌** read alias（v1.9 R2：裸 PUT 默认安全——EnsureIndex / 本脚本建索引
# 时不会让新索引一建好就挂上 read alias，避免 reindex 完成前就被 reader 读到半量数据）。alias 只在
# 步骤③ reindex + 抽样对账通过后单独原子挂。这里仍 del(.aliases) 兜底：若有人把 alias 加回 mapping，
# 此处仍剥离，保证「建索引 ≠ 上线」的纪律不被 mapping 改动悄悄破坏。
new_index_body(){
  # 用 jq 去掉 aliases 段（mapping 现已无该段，del 为幂等 no-op；保留以防 mapping 回退加回 alias）。
  if command -v jq >/dev/null 2>&1; then
    jq 'del(.aliases)' "$MAPPING_FILE"
  elif grep -q '"aliases"' "$MAPPING_FILE"; then
    echo "ERROR: mapping embeds an aliases section but jq is unavailable to strip it for staged migration (set MAPPING_NOALIAS_FILE)" >&2
    exit 3
  else
    # mapping 无 aliases 段（当前状态）：无 jq 也安全，裸 PUT 不会提前挂 alias。
    cat "$MAPPING_FILE"
  fi
}

step1_create_new(){
  log "① 创建新契约索引 $NEW_INDEX (mapping v1.9，无内嵌 alias / 兜底剥离 aliases 段)"
  new_index_body | "${CURL[@]}" -XPUT "$ES/$NEW_INDEX" -d @- | tee /dev/stderr | grep -q '"acknowledged":true'
}

step2_reindex(){
  log "② reindex $OLD_INDEX → $NEW_INDEX（字段名收敛 painless）"
  # painless：把旧 flat snake_case doc 映射成新 camelCase 嵌套契约。
  # ⚠️ 旧链不写 space_id/visibles/message_seq（Kafka 契约缺），故 reindex 出的新 doc 这三字段为空
  # （reader p2p fail-closed / 无 visibles gate / messageSeq=0 保守）。要填全须走 backfill 重灌
  # （读原始 MySQL payload）——见 docs/forward-migration-v1.9.md「存量富化」。
  # subSeq 显式置 0：v1.11 契约要求所有 doc 显式落盘 subSeq（reader 不赌「缺失=0」，见 #26）。
  "${CURL[@]}" -XPOST "$ES/_reindex?wait_for_completion=true" -d @- <<JSON | tee /dev/stderr | grep -q '"failures":\[\]'
{
  "source": { "index": "$OLD_INDEX" },
  "dest":   { "index": "$NEW_INDEX", "op_type": "index" },
  "script": {
    "lang": "painless",
    "source": "ctx._source.messageId = Long.parseLong(ctx._source.remove('message_id')); def cid = ctx._source.remove('channel_id'); if (cid != null) ctx._source.channelId = cid; def ct = ctx._source.remove('channel_type'); if (ct != null) ctx._source.channelType = ct; def f = ctx._source.remove('from_uid'); if (f != null) ctx._source.from = f; def ts = ctx._source.remove('msg_timestamp'); if (ts != null) ctx._source.timestamp = ts; def ca = ctx._source.remove('created_at'); if (ca != null) ctx._source.createdAt = ca; def re = ctx._source.remove('raw_excluded'); if (re != null) ctx._source.rawExcluded = re; def sv = ctx._source.remove('schema_version'); if (sv != null) ctx._source.schemaVersion = sv; def cont = ctx._source.remove('content'); def cty = ctx._source.remove('content_type'); def p = ['type': cty]; if (cont != null) p.text = ['content': cont]; ctx._source.payload = p; ctx._source.subSeq = 0;"
  }
}
JSON
}

step3_switch_alias(){
  log "③ 原子切换 read alias $ALIAS → $NEW_INDEX（从所有索引摘掉同名 alias 再挂新索引，幂等且单指向）"
  # 🔴 单指向不变式：read alias **任何时刻只能指向一个索引**（禁半新半旧同 alias 服务）。故 remove
  # 用 index="*"（摘掉 alias 当前挂着的**任意**索引，不只是 OLD_INDEX）—— 若只 remove OLD_INDEX，
  # 而 alias 当前其实挂在别的索引上（OLD_INDEX 名传错 / 历史迁移残留），remove 会 no-op，add 后 alias
  # 就同时指向那个旧索引 + NEW_INDEX（reader 读到半量数据）。配 must_exist:false：首次迁移 alias 尚
  # 不存在时 remove 不报错。整个 _aliases 调用是**原子**的（remove+add 同事务），无中间真空态。
  "${CURL[@]}" -XPOST "$ES/_aliases" -d @- <<JSON | tee /dev/stderr | grep -q '"acknowledged":true'
{
  "actions": [
    { "remove": { "index": "*", "alias": "$ALIAS", "must_exist": false } },
    { "add":    { "index": "$NEW_INDEX", "alias": "$ALIAS" } }
  ]
}
JSON
}

log "前向迁移：ES=$ES OLD=$OLD_INDEX NEW=$NEW_INDEX ALIAS=$ALIAS"
echo "STEP=${STEP:-all}（可设 STEP=1|2|3 单步执行；reindex 完成 + 抽样对账通过后才执行 STEP=3）"
case "${STEP:-all}" in
  1) step1_create_new ;;
  2) step2_reindex ;;
  3) step3_switch_alias ;;
  all)
    step1_create_new
    step2_reindex
    echo ">>> 切 alias 前请先跑：make run-recon RECON_ES_INDEX=$NEW_INDEX（抽样对账通过再 STEP=3）"
    echo ">>> 确认无误后执行：STEP=3 $0"
    ;;
  *) echo "unknown STEP=${STEP}" >&2; exit 2 ;;
esac

# ROLLBACK 指引（alias 秒切回旧索引）。用展开式 heredoc（<<ROLLBACK，非单引号）让
# $ES/$NEW_INDEX/$ALIAS/$OLD_INDEX 展开成**可直接复制执行**的命令，而不是字面量占位符。
# JSON body 单独用单引号包住（避免外层展开破坏 JSON），index/alias 名以双引号拼接展开。
cat <<ROLLBACK

ROLLBACK（alias 秒切回旧索引；下面命令已展开当前 ES/索引/alias，可直接复制执行）：
  curl -sS -XPOST "$ES/_aliases" -H 'Content-Type: application/json' -d '{
    "actions":[
      {"remove":{"index":"$NEW_INDEX","alias":"$ALIAS"}},
      {"add":{"index":"$OLD_INDEX","alias":"$ALIAS"}}
    ]}'
  旧索引保留 ≥7 天后再删：curl -sS -XDELETE "$ES/$OLD_INDEX"
ROLLBACK
