# octo-search-indexer 监控指标技术落地方案

> 配套指标方案（producer 8 / consumer 7）的代码落地拆解。
> 只动埋点 + obs 基建，不碰 ETL/消费的正确性路径（事务边界、cursor CAS、DLQ 决策、offset 提交一律不动）。
> 仓库：`github.com/Mininglamp-OSS/octo-search-indexer`（go 1.25）

## 0. 现状盘点（已读代码确认）

| 模块 | 现状 |
|---|---|
| producer 指标 | 手搓 `internal/producer/metrics.go`：`atomic` + `strings.Builder` 拼 Prometheus 文本，**无 SDK、无 Histogram** |
| producer obs | `internal/producer/obs.go` 已有 `/healthz` `/readyz` `/metrics`（纯 net/http），`cmd/searchetl-producer` 已接 `PRODUCER_OBS_ADDR`（默认 `:9090`） |
| consumer 指标 | **完全没有**。只有 `service.go` 里的占位 `logAlerter`（打 `[ALERT]` 日志） |
| consumer obs | **没有 obs server、没有 /metrics** |

结论：producer 是「重写已有埋点 + 升级为 SDK + 补两个 Histogram」；consumer 是「从零起 obs server + metrics + alerter 换计数实现」。

---

## 1. 通用基建（两腿共用约定）

1. 引入 `github.com/prometheus/client_golang`（需主人批准装依赖，见 §6）。
2. 每条腿各自 `prometheus.NewRegistry()`（不用全局默认 registry，避免两 binary 误注册彼此指标）。
3. `/metrics` 换 `promhttp.HandlerFor(reg, promhttp.HandlerOpts{})`，`/healthz` `/readyz` 保留现有实现。
4. Histogram buckets 统一 `[.005,.01,.025,.05,.1,.25,.5,1,2.5,5]`（5ms~5s）。
5. label 基数克制：`result`(ok/error)、`op`、`reason`、`disp`、`partition`、`stream`、`shard`。

---

## 2. Producer 改动（searchetl-producer）

### 2.1 `internal/producer/metrics.go`（重写）

**删**（方案要求去掉的 3 个）：
- `searchetl_producer_db_op_duration_seconds`（代码里实际没埋，无对应字段，确认无残留）
- `searchetl_producer_db_op_errors_total`（同上）
- `searchetl_producer_last_tick_timestamp_seconds`（现为 `last_tick_unixtime` gauge，删字段 `lastTickUnix` + Render 段）

**改**（ticks 两个合并用 label）：
- 删 `ticks` / `tickErrors` 两个独立 `atomic.Int64`
- 改成一个 `CounterVec`：`searchetl_producer_ticks_total{result}`，`result∈{ok,error}`
- `MarkTick()` / `MarkTickError()` 改为 `MarkTick(result string)` 单入口，或保留两方法各自 `.WithLabelValues("ok"/"error").Inc()`

**增**（两个 Histogram）：
- `searchetl_producer_tick_duration_seconds`（无 label）：单个 tick 整体耗时
- `searchetl_producer_read_batch_duration_seconds`（无 label）：`ReadStableBatchTx` 单次读耗时

**保留**（SDK 化，行为不变）：
- `produced_total{stream=main/dlq}` → `CounterVec`
- `cursor_position{shard}` → `GaugeVec`（替掉手写的 `mu+map[string]int64`）
- `produce_errors_total`、`lock_renew_failures_total`、`dlq_total{reason}` —— 方案表里有但**当前代码未埋**，本次一并补齐（见 §2.4）

新结构示意（字段全部换成 prometheus 类型）：
```go
type Metrics struct {
    reg            *prometheus.Registry
    produced       *prometheus.CounterVec   // {stream}
    cursor         *prometheus.GaugeVec     // {shard}
    ticks          *prometheus.CounterVec   // {result}
    tickDuration   prometheus.Histogram
    readBatchDur   prometheus.Histogram
    dlq            *prometheus.CounterVec   // {reason}
    produceErrors  prometheus.Counter
    lockRenewFails prometheus.Counter
}
```

### 2.2 `internal/producer/scheduler.go`（埋 tick 成败 + tick 耗时）

`tick()` 当前：
```go
func (s *Scheduler) tick(ctx context.Context) {
    if s.metrics != nil { s.metrics.MarkTick() }
    if err := s.tickFn(ctx, s.tables); err != nil {
        s.logf(...); if s.metrics != nil { s.metrics.MarkTickError() }
    }
}
```
改为（计时 + result 区分）：
```go
func (s *Scheduler) tick(ctx context.Context) {
    start := time.Now()
    err := s.tickFn(ctx, s.tables)
    if s.metrics != nil {
        s.metrics.ObserveTickDuration(time.Since(start))
        if err != nil { s.metrics.MarkTick("error") } else { s.metrics.MarkTick("ok") }
    }
    if err != nil { s.logf("producer: scheduled incremental failed: %v", err) }
}
```
> 影响面：纯埋点，tick 触发/cadence 逻辑一字不动。

### 2.3 `read_batch_duration_seconds`：runChunk 需拿到 metrics（这是上次主人问的那条）

现状：`ReadStableBatchTx` 在 `runChunk` 里调，而 `runChunk` 是**包级自由函数**，签名里没有 `metrics`。要给它加一个 `metrics *Metrics` 参数，调用处（`etl.go runTick` 里）把 `e.metrics` 传进去：

```go
// 改签名
func runChunk(ctx, store, sink, table, cutoff, batch, logf, metrics *Metrics) (chunkPlan, int, error) {
    start := time.Now()
    cursor, rows, err := store.ReadStableBatchTx(ctx, table, batch)
    if metrics != nil { metrics.ObserveReadBatch(time.Since(start)) }
    ...
}
// 调用处 etl.go:
plan, n, cerr := runChunk(ctx, e.store, sink, table, cutoff, e.batch, e.logf, e.metrics)
```

**改的是函数签名 + 调用处传参。SQL、事务边界（FOR UPDATE/Commit）、keyset 读取逻辑、cursor CAS —— 一个字不动。** 计时只包 `ReadStableBatchTx` 这一次 DB 往返，是「单批读的整体耗时」（含 FOR UPDATE 锁等待 + keyset 查询 + scan），不拆到单行。

### 2.4 顺带补齐 3 个方案有、代码缺的埋点（低风险）

| 指标 | 埋点位置 |
|---|---|
| `produce_errors_total` | `kafka.go` `ProduceBatch`/`produce` WriteMessages 返回 err 时 `Inc()`（需把 metrics 传进 KafkaProducer，或 sink 回调） |
| `lock_renew_failures_total` | `lock.go` `renewUntilDone` 里 Renew 返回 err/false 时 `Inc()` |
| `dlq_total{reason}` | `chunk.go planChunk` 产 DLQ envelope 时按 `reason` 计数（注意 planChunk 是纯函数，计数要挪到 `runChunk` produce 成功后按 `plan.dlq` 的 reason 累加，保持 planChunk 无副作用） |

> 若要严格控制本次范围，§2.4 可拆成第二个 PR；§2.1~2.3 是方案明确点名的核心。

---

## 3. Consumer 改动（es-indexer）—— 从零起

### 3.1 新增 `internal/consumer/obs.go`

直接镜像 producer 的 `obs.go`，但 `/metrics` 用 `promhttp`：起 `/healthz` `/readyz` `/metrics`，绑独立 registry。

### 3.2 新增 `internal/consumer/metrics.go`

按方案 consumer 表埋 7 个：
```go
type Metrics struct {
    reg          *prometheus.Registry
    disposition  *prometheus.CounterVec   // {disp: ok/dlq/transient}
    dlq          *prometheus.CounterVec   // {reason}
    dlqHardStop  prometheus.Counter
    bulkErrors   prometheus.Counter
    committed    *prometheus.GaugeVec     // {partition}
    ioOpDuration *prometheus.HistogramVec // {op}
    ioOpErrors   *prometheus.CounterVec   // {op}
}
```
`op∈{es_bulk, kafka_fetch, kafka_commit, dlq_send}`，`disp∈{ok,dlq,transient}`，`reason∈{unknown_schema_version,visibility_untrusted,permanent_4xx}`。

### 3.3 `logAlerter` → 计数实现

`service.go` 里 `logAlerter` 换成持有 `*Metrics` 的实现，把现有 alert event 映射到计数器：
- `bulk_batch_error` → `bulkErrors.Inc()`
- `dlq_hard_stop` / `dlq_write_exhausted` → `dlqHardStop.Inc()`
- 其余保留日志即可。
> alerter 接口签名 `Alert(event, detail string)` 不变，只换实现，注入点在 `NewService`。

### 3.4 埋点位置（consumer.go / kafka.go）

| 指标 | 位置 | 备注 |
|---|---|---|
| `disposition_total{disp}` | `consumer.go resolvePass` 每条定 disp 时（dispOK/dispDLQResolved/dispTransient）`Inc()` | 核心健康指标 |
| `dlq_total{reason}` | `routePoison` 成功落 DLQ 后按 `reason.reasonString()` `Inc()` | |
| `dlq_hard_stop_total` | `routePoison` 返回 hard-stop err 时 `Inc()` | 最高优先级告警 |
| `bulk_errors_total` | `resolvePass` `writer.Bulk` 返回批级 err 时 `Inc()` | |
| `committed_offset{partition}` | `commitPrefixes` 每次 Commit 成功后 `Set(offset)` | 判推进是否卡住 |
| `io_op_duration_seconds{op}` + `io_op_errors_total{op}` | `es_bulk`=writer.Bulk;`kafka_fetch`=kafka.go Fetch;`kafka_commit`=Commit;`dlq_send`=kafkaDLQSink.WriteDLQ 各包一层计时 | consumer 不直连 DB |

> 计时统一 `start:=time.Now(); ...; metrics.ObserveIO("es_bulk", time.Since(start))`，err 非 nil 再 `IOErr("es_bulk")`。Processor/kafkaSource 需新增 `*Metrics` 字段，构造器注入。

### 3.5 `cmd/es-indexer/main.go`：起 obs server

仿 producer main：读 `INDEXER_OBS_ADDR`（默认 `:9090`），`run()` 里 `NewObsServer` + `Start`，readyz 检查可先返回 ready（或 ping ES/Kafka，二期）。idle 路径也起一个 200 的 idle obs（和 producer 对齐）。

---

## 4. Prometheus 抓取

加两个 scrape job（producer / es-indexer 各暴露 `:9090/metrics`）。具体落 deployment 仓的 Prometheus 配置，本仓不含。

---

## 5. 落地顺序（建议拆 PR）

1. **PR1 依赖**：`go.mod` 引入 client_golang（需主人批准 `go get` + `go mod tidy`）。
2. **PR2 producer**：metrics.go 重写为 SDK + scheduler 计时 + runChunk 传 metrics（§2.1~2.3 核心）。
3. **PR3 producer 补漏**：produce_errors / lock_renew_fails / dlq_total（§2.4，可选并入 PR2）。
4. **PR4 consumer 基建**：obs.go + metrics.go + logAlerter 换计数 + 埋 7 指标 + main 起 obs（§3）。
5. 各 PR 补/改单测（metrics 注入用 fake，断言计数；现有 chunk/decision 纯函数测试不受影响）。

---

## 6. 需要主人拍板的点

- **装依赖**：`go get github.com/prometheus/client_golang` —— 按红线，装依赖要主人明确 OK。
- **范围**：§2.4 / §3 是否一次做完，还是先 producer 核心（§2.1~2.3）跑通再做 consumer。
- **远端**：开 PR / push 一律先问（红线不变）。

## 7. 风险评估（主人之前问「有影响核心逻辑改动较大的指标吗」）

**没有。** 全部是埋点 + obs 基建：
- 唯一动到函数签名的是 `runChunk` 加 `metrics` 参数（§2.3），但 SQL/事务/CAS/读取逻辑零改动。
- consumer 的 `disposition/io` 埋点都是在已有决策点旁加一行 `Inc()`/计时，不改 offset 提交、不改 DLQ 决策、不改 transient 重试收敛。
- `logAlerter` 换实现但接口不变。
- 最大「体量」在 consumer 从零起 obs（新文件），但不触碰 `processBatch`/`resolvePass` 的正确性分支。
