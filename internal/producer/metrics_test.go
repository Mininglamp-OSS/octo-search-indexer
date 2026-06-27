package producer

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// dumpMetrics renders the registry to a stable text form (one line per series,
// "name{labels} value" for counters/gauges and "name_count N" for histograms),
// so assertions can match exact series without depending on expfmt formatting.
func dumpMetrics(t *testing.T, m *Metrics) string {
	t.Helper()
	families, err := m.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var lines []string
	for _, mf := range families {
		name := mf.GetName()
		for _, metric := range mf.Metric {
			labels := formatLabels(metric.Label)
			switch {
			case metric.Counter != nil:
				lines = append(lines, fmt.Sprintf("%s%s %s", name, labels, formatVal(metric.Counter.GetValue())))
			case metric.Gauge != nil:
				lines = append(lines, fmt.Sprintf("%s%s %s", name, labels, formatVal(metric.Gauge.GetValue())))
			case metric.Histogram != nil:
				lines = append(lines, fmt.Sprintf("%s_count %s", name, formatVal(float64(metric.Histogram.GetSampleCount()))))
			}
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func formatLabels(lps []*dto.LabelPair) string {
	if len(lps) == 0 {
		return ""
	}
	parts := make([]string, 0, len(lps))
	for _, lp := range lps {
		parts = append(parts, fmt.Sprintf("%s=%q", lp.GetName(), lp.GetValue()))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// formatVal renders integer-valued samples without a decimal point.
func formatVal(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}

// TestMetrics_TickAndDurations verifies tick result counters and histogram
// sample counts increment as expected.
func TestMetrics_TickAndDurations(t *testing.T) {
	m := NewMetrics()
	m.MarkTick("ok")
	m.MarkTick("ok")
	m.MarkTick("error")
	m.ObserveTickDuration(10 * time.Millisecond)
	m.ObserveReadBatch(5 * time.Millisecond)
	m.ObserveReadBatch(20 * time.Millisecond)

	out := dumpMetrics(t, m)
	for _, want := range []string{
		`searchetl_producer_ticks_total{result="ok"} 2`,
		`searchetl_producer_ticks_total{result="error"} 1`,
		`searchetl_producer_tick_duration_seconds_count 1`,
		`searchetl_producer_read_batch_duration_seconds_count 2`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, out)
		}
	}
}

// TestMetrics_DLQAndErrors verifies the补漏 counters (dlq/produce/lock).
func TestMetrics_DLQAndErrors(t *testing.T) {
	m := NewMetrics()
	m.MarkDLQ(dlqReasonVisibilityUntrusted)
	m.MarkDLQ(dlqReasonVisibilityUntrusted)
	m.MarkDLQ(dlqReasonOversize)
	m.MarkProduceError()
	m.MarkLockRenewFailure()

	out := dumpMetrics(t, m)
	for _, want := range []string{
		`searchetl_producer_dlq_total{reason="producer_visibility_untrusted"} 2`,
		`searchetl_producer_dlq_total{reason="producer_oversize_truncated"} 1`,
		`searchetl_producer_produce_errors_total 1`,
		`searchetl_producer_lock_renew_failures_total 1`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, out)
		}
	}
}
