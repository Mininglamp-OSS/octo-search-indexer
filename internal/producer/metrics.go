package producer

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics is the minimal observability set the plan moved back into scope:
// produced counts, per-shard cursor position, tick health. Rendered as a tiny
// Prometheus text exposition (no prometheus client dep in this slim image).
type Metrics struct {
	producedMain atomic.Int64
	producedDLQ  atomic.Int64
	ticks        atomic.Int64
	tickErrors   atomic.Int64
	lastTickUnix atomic.Int64

	mu      sync.Mutex
	cursors map[string]int64
}

// NewMetrics constructs a Metrics.
func NewMetrics() *Metrics {
	return &Metrics{cursors: map[string]int64{}}
}

// AddProduced records produced message counts for a tick.
func (m *Metrics) AddProduced(main, dlq int64) {
	m.producedMain.Add(main)
	m.producedDLQ.Add(dlq)
}

// SetCursor records the latest cursor watermark for a shard (cursor position log
// + metric: the plan requires cursor-position observability).
func (m *Metrics) SetCursor(table string, id int64) {
	m.mu.Lock()
	m.cursors[table] = id
	m.mu.Unlock()
}

// MarkTick records a tick fired.
func (m *Metrics) MarkTick() {
	m.ticks.Add(1)
	m.lastTickUnix.Store(time.Now().Unix())
}

// MarkTickError records a tick that returned an error.
func (m *Metrics) MarkTickError() { m.tickErrors.Add(1) }

// Render returns the Prometheus text exposition.
func (m *Metrics) Render() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP searchetl_producer_produced_total Messages produced to Kafka by stream.\n")
	fmt.Fprintf(&b, "# TYPE searchetl_producer_produced_total counter\n")
	fmt.Fprintf(&b, "searchetl_producer_produced_total{stream=\"main\"} %d\n", m.producedMain.Load())
	fmt.Fprintf(&b, "searchetl_producer_produced_total{stream=\"dlq\"} %d\n", m.producedDLQ.Load())

	fmt.Fprintf(&b, "# HELP searchetl_producer_ticks_total Slow-cursor ticks fired.\n")
	fmt.Fprintf(&b, "# TYPE searchetl_producer_ticks_total counter\n")
	fmt.Fprintf(&b, "searchetl_producer_ticks_total %d\n", m.ticks.Load())

	fmt.Fprintf(&b, "# HELP searchetl_producer_tick_errors_total Slow-cursor ticks that errored.\n")
	fmt.Fprintf(&b, "# TYPE searchetl_producer_tick_errors_total counter\n")
	fmt.Fprintf(&b, "searchetl_producer_tick_errors_total %d\n", m.tickErrors.Load())

	fmt.Fprintf(&b, "# HELP searchetl_producer_last_tick_unixtime Unix time of the last tick.\n")
	fmt.Fprintf(&b, "# TYPE searchetl_producer_last_tick_unixtime gauge\n")
	fmt.Fprintf(&b, "searchetl_producer_last_tick_unixtime %d\n", m.lastTickUnix.Load())

	m.mu.Lock()
	tables := make([]string, 0, len(m.cursors))
	for t := range m.cursors {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	fmt.Fprintf(&b, "# HELP searchetl_producer_cursor_position Per-shard cursor watermark (message.id).\n")
	fmt.Fprintf(&b, "# TYPE searchetl_producer_cursor_position gauge\n")
	for _, t := range tables {
		fmt.Fprintf(&b, "searchetl_producer_cursor_position{shard=%q} %d\n", t, m.cursors[t])
	}
	m.mu.Unlock()
	return b.String()
}
