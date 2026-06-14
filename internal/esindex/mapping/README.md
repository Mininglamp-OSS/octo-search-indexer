# octo-message index mapping

`octo-message.json` is the **canonical index mapping + analyzer** the
`es-indexer` writes against, embedded into the binary (`//go:embed`) and used by
`esindex.EnsureIndex` to bootstrap the index idempotently when it does not yet
exist.

It is the single source the **octo-deployment** coordinated change references:
provisioning/managed-OpenSearch templates must stay in lockstep with this file
at the same contract version (`searchmsg.SchemaVersion`).

## Chinese tokenization — IK (analysis-ik)

`content` uses the IK plugin's dual-analyzer setup:

- index-time `analyzer: ik_max_word` — exhaustive segmentation, maximises recall
  (IM search is a "rather over-recall than miss" scenario).
- query-time `search_analyzer: ik_smart` — smart segmentation, keeps precision.

IK is chosen over the built-in `smartcn` because IK's dictionary segmentation is
markedly better on IM colloquialisms, CN/EN/digit mixes and net-slang, and it
supports hot-reloadable custom dictionaries (product names, @nicknames, in-group
jargon). The IK plugin + dictionaries are provisioned by the **octo-deployment**
change; the local verification harness (`harness/`) installs the IK plugin into
its OpenSearch container so the mapping is exercised for real.

> Requires the `analysis-ik` plugin on the OpenSearch cluster. If a deployment
> must run without IK, override `content.analyzer`/`search_analyzer` in the
> octo-deployment template (e.g. to a `cjk_bigram`-based custom analyzer) — that
> selection is owned by the deployment change, not pinned here.

## Field rationale (route 甲: body + query-side authz visibility only)

| Field            | Type                | Why                                                        |
| ---------------- | ------------------- | --------------------------------------------------------- |
| `message_id`     | `keyword`           | = ES `_id` / Kafka key; exact-match dedupe key            |
| `channel_id`     | `keyword`           | query-side authz filter (group/topic/DM resolution)       |
| `channel_type`   | `integer`           | authz routing per channel type                            |
| `from_uid`       | `keyword`           | sender filter / authz                                     |
| `content`        | `text` + IK         | the searchable body — Chinese tokenization                |
| `content_type`   | `integer`           | message type                                              |
| `raw_excluded`   | `boolean`           | Signal/non-text marker (content null)                     |
| `msg_timestamp`  | `long`              | send time (epoch s)                                       |
| `created_at`     | `long`              | landed time (epoch s) — reconciliation window field       |
| `source`         | `keyword`           | provenance (ETL vs future CDC)                            |
| `schema_version` | `integer`           | contract version stamped on every doc                     |

No `revoked` / `deleted` fields — route 甲 filters those at read time via MySQL
join. Mapping is `dynamic: strict` so any unexpected field fails loudly rather
than silently polluting the index.
