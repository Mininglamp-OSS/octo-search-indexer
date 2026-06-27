package consumer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metricsNamespace prefixes every consumer series (indexer_*).
const metricsNamespace = "indexer"

// latencyBuckets is the shared Histogram bucketing (5ms~5s), matching the
// producer leg.
var latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

// Metrics is the es-indexer consumer observability set, backed by the prometheus
// client_golang SDK on a private registry (no global default registry — the two
// binaries must never cross-register each other's series).
//
// Series (all prefixed indexer_):
//   - disposition_total{disp}        counter   per-item disposition (ok/dlq/transient)
//   - dlq_total{reason}              counter   dead-lettered messages by reason
//   - dlq_hard_stop_total            counter   DLQ terminal-escape hard stops
//   - bulk_errors_total              counter   batch-level bulk failures
//   - committed_offset{partition}    gauge     last committed offset per partition
//   - io_op_duration_seconds{op}     histogram IO latency by op
//   - io_op_errors_total{op}         counter   IO failures by op
type Metrics struct {
	reg *prometheus.Registry

	disposition  *prometheus.CounterVec
	dlq          *prometheus.CounterVec
	dlqHardStop  prometheus.Counter
	dlqExhausted prometheus.Counter
	bulkErrors   prometheus.Counter
	committed    *prometheus.GaugeVec
	ioOpDuration *prometheus.HistogramVec
	ioOpErrors   *prometheus.CounterVec
}

// NewMetrics constructs a Metrics bound to its own registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		disposition: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "disposition_total",
			Help:      "Per-item disposition after a resolve pass (ok/dlq/transient).",
		}, []string{"disp"}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "dlq_total",
			Help:      "Messages dead-lettered by reason.",
		}, []string{"reason"}),
		dlqHardStop: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "dlq_hard_stop_total",
			Help:      "DLQ terminal-escape hard stops (worker stopped, no spill configured).",
		}),
		dlqExhausted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "dlq_write_exhausted_total",
			Help:      "DLQ writes that exhausted bounded retries (then either hard-stopped or spilled to disk).",
		}),
		bulkErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "bulk_errors_total",
			Help:      "Batch-level bulk failures (writer.Bulk returned an error).",
		}),
		committed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "committed_offset",
			Help:      "Last committed offset per partition.",
		}, []string{"partition"}),
		ioOpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "io_op_duration_seconds",
			Help:      "IO operation latency by op (es_bulk/kafka_fetch/kafka_commit/dlq_send).",
			Buckets:   latencyBuckets,
		}, []string{"op"}),
		ioOpErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "io_op_errors_total",
			Help:      "IO operation failures by op.",
		}, []string{"op"}),
	}
	reg.MustRegister(
		m.disposition, m.dlq, m.dlqHardStop, m.dlqExhausted, m.bulkErrors,
		m.committed, m.ioOpDuration, m.ioOpErrors,
	)
	return m
}

// Registry exposes the private registry for the obs /metrics handler.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// MarkDisposition records one item disposition (disp ∈ {ok, dlq, transient}).
func (m *Metrics) MarkDisposition(disp string) {
	m.disposition.WithLabelValues(disp).Inc()
}

// MarkDLQ records one dead-lettered message by reason.
func (m *Metrics) MarkDLQ(reason string) {
	m.dlq.WithLabelValues(reason).Inc()
}

// MarkDLQHardStop records one DLQ terminal-escape hard stop.
func (m *Metrics) MarkDLQHardStop() { m.dlqHardStop.Inc() }

// MarkDLQWriteExhausted records one DLQ write that exhausted bounded retries
// (independent of whether it then hard-stopped or spilled to disk).
func (m *Metrics) MarkDLQWriteExhausted() { m.dlqExhausted.Inc() }

// MarkBulkError records one batch-level bulk failure.
func (m *Metrics) MarkBulkError() { m.bulkErrors.Inc() }

// SetCommittedOffset records the last committed offset for a partition.
func (m *Metrics) SetCommittedOffset(partition string, offset int64) {
	m.committed.WithLabelValues(partition).Set(float64(offset))
}

// ObserveIO records the latency of one IO op (op ∈ {es_bulk, kafka_fetch,
// kafka_commit, dlq_send}).
func (m *Metrics) ObserveIO(op string, d time.Duration) {
	m.ioOpDuration.WithLabelValues(op).Observe(d.Seconds())
}

// MarkIOError records one IO failure by op.
func (m *Metrics) MarkIOError(op string) {
	m.ioOpErrors.WithLabelValues(op).Inc()
}
