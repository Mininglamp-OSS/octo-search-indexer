# dmwork-test 隔离实时索引 k8s bundle (YUJ-5039 / Phase 0 物料)

把 `harness/docker-compose.yml` 的本地全链路验证栈翻译成可 apply 到腾讯云 EKS
`dmwork-test` 的隔离 k8s manifest。**本目录只出物料 —— 不部署 / 不 apply / 不碰
dmwork-test / 不动 octo-server。** apply 由 Coda review 后做 (见 YUJ-5034 PlanBot 方案
Phase 1–5)。

对应方案: YUJ-5034 (issue b51a8099) **路线 B** —— 自带隔离 OpenSearch (无 security),
规避现网共享 OS 的 https 自签证书校验阻塞 (PlanBot §3b / STOP#3b), 污染面=0。

## 链路

```
message 5 分表 → searchetl(producer, octo-server, 后续 Phase 开 Kafka.On)
  → Kafka octo.message.v1 → es-indexer(consumer, 本 bundle) → OpenSearch(IK, 本 bundle)
  → octo-server reader (后续 Phase 接 OS)
```

本 bundle 只含 **Kafka + OpenSearch-IK + es-indexer** 三件 (compose 的全部)。
**不含 octo-server 的 Kafka.On / reader 改动** —— 那是后续 Phase, Coda 做。

## 资源清单 (namespace=dmwork-test)

| 文件 | Kind | 名字 | compose 对应 |
|---|---|---|---|
| search-kafka-statefulset.yaml | StatefulSet | search-kafka | kafka (KRaft 单节点) |
| search-kafka-service.yaml | Service(headless) | search-kafka | — (k8s 稳定 DNS) |
| search-kafka-topic-init-job.yaml | Job | search-kafka-topic-init | AUTO_CREATE_TOPICS 替代 |
| search-opensearch-statefulset.yaml | StatefulSet | search-opensearch | opensearch (single-node IK) |
| search-opensearch-service.yaml | Service(ClusterIP) | search-opensearch | — |
| es-indexer-deployment.yaml | Deployment | es-indexer | es-indexer (consumer) |

全部走集群内 service DNS 明文互联, 不暴露外网。前缀 `search-` / `es-indexer`
避免与现有 octo-server / octo-search-batch / octo-messages-sync / octo-redis /
octo-wukongim 冲突。

## 镜像 (均 linux/amd64, 不可变 tag, 推到 dmwork-test 可达的腾讯 TCR)

registry = `tbj7-xtiao-tcr1.tencentcloudcr.com/xtiao-release/dmwork`
(= 现网 octo-server 镜像同源 registry, dmwork-test 已验证能拉, 已配 imagePullSecret)。

| 镜像 | tag | 构建自 |
|---|---|---|
| octo-search-indexer | d2c7cc3 (@digest 钉死) | 仓内 Dockerfile (commit d2c7cc3) |
| octo-search-opensearch-ik | 2.17.0-ik | harness/opensearch-ik.Dockerfile |
| apache-kafka | 3.8.0 | mirror apache/kafka:3.8.0 |

> 镜像全名 + digest 见 kustomization.yaml `images:` 段 / YUJ-5039 结果 comment。
> imagePullSecret: dmwork-test 现有 pod (octo-server) 已用同 registry, 复用其
> namespace 级 pull secret 即可; manifest 未硬编码 secret 名 (留 Coda apply 时按
> 现网约定补 imagePullSecrets, 或确认 default SA 已挂)。

## 校验 (client 端, 不连集群)

```bash
kubectl kustomize deploy/k8s | kubectl create --dry-run=client -f -
```

全部 6 资源 created (dry run) 通过。注: 用 `create --dry-run=client` 而非
`apply --dry-run=client` —— 后者会向当前 context 集群发 GET (本机当前 context 是
另一集群, 不是 dmwork-test, 不能碰)。

## ⚠️ 存疑点 (apply 前 / apply 后必读, 对应 PlanBot STOP 条件)

1. **eklet serverless 上 StatefulSet + PVC + 稳定身份 (STOP#1)**:
   dmwork-test 节点是 eklet (virtual-kubelet)。Kafka/OS 的 StatefulSet 稳定 Pod DNS +
   cbs PVC 挂载 + 重启数据/身份持久化, **未验真起**。apply 后若 PVC Pending /
   Pod CrashLoop>3 / hostname 不稳 → 命中 STOP#1, 停, 评估备选 broker。
2. **OpenSearch memlock (bootstrap.memory_lock=true)**: 已加 securityContext
   capabilities IPC_LOCK, 但 eklet 是否尊重该 cap + 放开 memlock rlimit **存疑**。
   起不来可把 env `bootstrap.memory_lock` 改 `false`。
3. **vm.max_map_count>=262144**: OS 要求的内核 sysctl。eklet 上能否预置 / 能否用
   privileged initContainer 调 **存疑**。报 "max virtual memory areas too low" 即命中。
4. **STOP#B1 (reader 侧, 本 bundle 不含)**: 隔离 OS 无 alias; es-indexer EnsureIndex
   裸 PUT 字面索引 `octo-message`。后续 Coda 开 reader 时需 `OCTO_SEARCH_OS_READ_ALIAS=
   octo-message` 且确认 reader 能把 READ_ALIAS 当索引名直读。
5. **es-indexer 无 health/metrics endpoint**: 阶段 7 才接 (Dockerfile EXPOSE 9090 占位,
   当前无监听)。故 Deployment 不配 HTTP probe。观察期靠日志 + OS doccount。

## 开通条件 (es-indexer)

`cmd/es-indexer/main.go` loadConfig: 需 `ES_INDEXER_ENABLED=true` 且
`KAFKA_BROKERS` / `ES_ADDRESSES` 均非空, 否则服务空转 (idling, 不连后端)。
本 manifest 三者齐备 = 真正消费。
