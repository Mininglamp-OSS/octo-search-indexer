package fileextract

// metrics.go — file-extractor Prometheus 指标（阶段 7：从 atomic stub 升级为 client_golang 私有 registry）。
//
// 设计对齐 sibling internal/consumer/metrics.go：
//   - 私有 registry（不用 global default registry，避免多 binary 交叉注册）。
//   - namespace = "fileextract"（与 indexer_* / searchetl_producer_* 对称）。
//   - 由 obs.go 的 ObsServer 通过 Registry() 暴露 /metrics。
//
// 指标（全部 fileextract_ 前缀）：
//   - processed_total          counter          抽取+回写成功数（核心吞吐）
//   - skipped_non_file_total   counter          非 type=8 跳过数
//   - dlq_total{reason}        counter          dead-letter 数 by reason
//   - doc_not_yet_total        counter          主 doc 未落时序竞态触发数
//   - retry_exhausted_total    counter          原地重试耗尽转 DLQ 数
//   - os_permanent_total       counter          OS 4xx permanent 转 DLQ 数
//
// 兼容说明：本次（P0）只把已有 6 个计数点接到 client_golang 私有 registry，方法签名尽量不动 consumer 调用点，
// 唯一变更是 IncDLQ 增加 reason 参数（调用点旁边本就有 reason 变量）。
// 更细粒度指标（io_op_duration{op} / tika / download / content_bytes / tombstone /
// dlq_write_errors）需新增埋点、涉及 download/tika/oswriter/dlq_handler 多文件，
// 留待 P1 增量补齐（见 .omc/monitoring/file-extractor-monitoring-plan.md §3）。

import (
	"github.com/prometheus/client_golang/prometheus"
)

// metricsNamespace 给所有 series 加 fileextract_ 前缀。
const metricsNamespace = "fileextract"

// counters 是 file-extractor 的可观测集，backed by prometheus client_golang，
// 绑定在一个私有 registry 上（同 consumer.Metrics 惯例）。
type counters struct {
	reg *prometheus.Registry

	processed      prometheus.Counter
	skippedNonFile prometheus.Counter
	dlqTotal       *prometheus.CounterVec
	docNotYet      prometheus.Counter
	retryExhausted prometheus.Counter
	osPermanent    prometheus.Counter
}

// newCounters 构造并注册所有指标到私有 registry。
func newCounters() *counters {
	reg := prometheus.NewRegistry()
	c := &counters{
		reg: reg,
		processed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "processed_total",
			Help:      "Files extracted and written back to OpenSearch successfully.",
		}),
		skippedNonFile: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "skipped_non_file_total",
			Help:      "Non type=8 messages skipped.",
		}),
		dlqTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "dlq_total",
			Help:      "Messages dead-lettered by reason.",
		}, []string{"reason"}),
		docNotYet: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "doc_not_yet_total",
			Help:      "Timing-race hits where the main doc was not yet indexed by es-indexer (404).",
		}),
		retryExhausted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "retry_exhausted_total",
			Help:      "In-place retries exhausted, forced to DLQ.",
		}),
		osPermanent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "os_permanent_total",
			Help:      "OpenSearch 4xx permanent errors routed to DLQ.",
		}),
	}
	reg.MustRegister(
		c.processed, c.skippedNonFile, c.dlqTotal,
		c.docNotYet, c.retryExhausted, c.osPermanent,
	)
	return c
}

// Registry 暴露私有 registry 供 obs /metrics handler 使用。
func (c *counters) Registry() *prometheus.Registry { return c.reg }

func (c *counters) IncProcessed()      { c.processed.Inc() }
func (c *counters) IncSkippedNonFile() { c.skippedNonFile.Inc() }

// IncDLQ 记一次 dead-letter，reason 为空时归到 "unknown" 避免空 label。
func (c *counters) IncDLQ(reason string) {
	if reason == "" {
		reason = "unknown"
	}
	c.dlqTotal.WithLabelValues(reason).Inc()
}

func (c *counters) IncDocNotYet()      { c.docNotYet.Inc() }
func (c *counters) IncRetryExhausted() { c.retryExhausted.Inc() }
func (c *counters) IncOSPermanent()    { c.osPermanent.Inc() }
