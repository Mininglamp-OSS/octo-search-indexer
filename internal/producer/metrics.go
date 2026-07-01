package producer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metricsNamespace prefixes every producer series (kept stable across the SDK
// migration: callers/dashboards still see searchetl_producer_*).
const metricsNamespace = "searchetl_producer"

// latencyBuckets is the shared Histogram bucketing (5ms~5s) for both the
// producer and consumer legs.
var latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

// Metrics is the producer observability set, backed by the prometheus
// client_golang SDK on a private registry (no global default registry — the two
// binaries must never cross-register each other's series).
//
// Series (all prefixed searchetl_producer_):
//   - produced_total{stream}          counter   messages produced by stream (main/dlq)
//   - cursor_position{shard}          gauge     per-shard cursor watermark (message.id)
//   - source_max_id{shard}           gauge     per-shard MySQL MAX(id) (lag = source_max_id - cursor_position)
//   - ticks_total{result}            counter   slow-cursor ticks by result (ok/error)
//   - tick_duration_seconds          histogram whole-tick latency
//   - read_batch_duration_seconds    histogram single ReadStableBatchTx latency
//   - dlq_total{reason}              counter   dead-lettered envelopes by reason
//   - produce_errors_total           counter   Kafka WriteMessages failures
//   - lock_renew_failures_total      counter   run-lock renew failures
type Metrics struct {
	reg *prometheus.Registry

	produced       *prometheus.CounterVec
	cursor         *prometheus.GaugeVec
	sourceMaxID    *prometheus.GaugeVec
	ticks          *prometheus.CounterVec
	tickDuration   prometheus.Histogram
	readBatchDur   prometheus.Histogram
	dlq            *prometheus.CounterVec
	produceErrors  prometheus.Counter
	lockRenewFails prometheus.Counter
}

// NewMetrics constructs a Metrics bound to its own registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		produced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "produced_total",
			Help:      "Messages produced to Kafka by stream.",
		}, []string{"stream"}),
		cursor: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "cursor_position",
			Help:      "Per-shard cursor watermark (message.id).",
		}, []string{"shard"}),
		sourceMaxID: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "source_max_id",
			Help:      "Per-shard MySQL MAX(id) (source watermark for lag).",
		}, []string{"shard"}),
		ticks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "ticks_total",
			Help:      "Slow-cursor ticks fired by result.",
		}, []string{"result"}),
		tickDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "tick_duration_seconds",
			Help:      "Whole slow-cursor tick latency.",
			Buckets:   latencyBuckets,
		}),
		readBatchDur: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "read_batch_duration_seconds",
			Help:      "Single ReadStableBatchTx latency (DB read round-trip).",
			Buckets:   latencyBuckets,
		}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "dlq_total",
			Help:      "Dead-lettered envelopes produced by reason.",
		}, []string{"reason"}),
		produceErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "produce_errors_total",
			Help:      "Kafka WriteMessages failures (main + dlq).",
		}),
		lockRenewFails: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "lock_renew_failures_total",
			Help:      "Run-lock renew failures (error or ownership lost).",
		}),
	}
	reg.MustRegister(
		m.produced, m.cursor, m.sourceMaxID, m.ticks, m.tickDuration, m.readBatchDur,
		m.dlq, m.produceErrors, m.lockRenewFails,
	)
	return m
}

// Registry exposes the private registry for the obs /metrics handler.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// AddProduced records produced message counts for a tick.
func (m *Metrics) AddProduced(main, dlq int64) {
	if main != 0 {
		m.produced.WithLabelValues("main").Add(float64(main))
	}
	if dlq != 0 {
		m.produced.WithLabelValues("dlq").Add(float64(dlq))
	}
}

// SetCursor records the latest cursor watermark for a shard.
func (m *Metrics) SetCursor(table string, id int64) {
	m.cursor.WithLabelValues(table).Set(float64(id))
}

// SetSourceMaxID records the latest source MAX(id) watermark for a shard.
func (m *Metrics) SetSourceMaxID(table string, id int64) {
	m.sourceMaxID.WithLabelValues(table).Set(float64(id))
}

// MarkTick records a tick outcome (result ∈ {ok, error}).
func (m *Metrics) MarkTick(result string) {
	m.ticks.WithLabelValues(result).Inc()
}

// ObserveTickDuration records whole-tick latency.
func (m *Metrics) ObserveTickDuration(d time.Duration) {
	m.tickDuration.Observe(d.Seconds())
}

// ObserveReadBatch records a single ReadStableBatchTx latency.
func (m *Metrics) ObserveReadBatch(d time.Duration) {
	m.readBatchDur.Observe(d.Seconds())
}

// MarkDLQ records one dead-lettered envelope by reason.
func (m *Metrics) MarkDLQ(reason string) {
	m.dlq.WithLabelValues(reason).Inc()
}

// MarkProduceError records one Kafka WriteMessages failure.
func (m *Metrics) MarkProduceError() { m.produceErrors.Inc() }

// MarkLockRenewFailure records one run-lock renew failure.
func (m *Metrics) MarkLockRenewFailure() { m.lockRenewFails.Inc() }
