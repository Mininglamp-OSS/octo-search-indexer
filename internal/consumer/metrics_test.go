package consumer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// dumpMetrics renders the registry to a stable text form (one line per series),
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
				lines = append(lines, fmt.Sprintf("%s_count%s %s", name, labels, formatVal(float64(metric.Histogram.GetSampleCount()))))
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

// TestMetrics_DispositionAndIO verifies disposition/dlq/io counters increment.
func TestMetrics_DispositionAndIO(t *testing.T) {
	m := NewMetrics()
	m.MarkDisposition("ok")
	m.MarkDisposition("ok")
	m.MarkDisposition("dlq")
	m.MarkDLQ("unknown_schema_version")
	m.MarkDLQHardStop()
	m.MarkBulkError()
	m.SetCommittedOffset("0", 42)
	m.ObserveIO("es_bulk", 10*time.Millisecond)
	m.MarkIOError("kafka_commit")

	out := dumpMetrics(t, m)
	for _, want := range []string{
		`indexer_disposition_total{disp="ok"} 2`,
		`indexer_disposition_total{disp="dlq"} 1`,
		`indexer_dlq_total{reason="unknown_schema_version"} 1`,
		`indexer_dlq_hard_stop_total 1`,
		`indexer_bulk_errors_total 1`,
		`indexer_committed_offset{partition="0"} 42`,
		`indexer_io_op_duration_seconds_count{op="es_bulk"} 1`,
		`indexer_io_op_errors_total{op="kafka_commit"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, out)
		}
	}
}

// TestProcessBatch_DispositionMetrics 跑一批（1 条成功 + 1 条 schema 非法毒丸）经 processBatch
// 后，断言 disposition_total{ok}=1、{dlq}=1 且 committed_offset 推进——验证埋点接进真实处置路径。
func TestProcessBatch_DispositionMetrics(t *testing.T) {
	m := NewMetrics()
	src := &fakeSource{}
	w := &fakeWriter{}
	sink := &fakeDLQSink{}
	dlq := newDLQHandler(sink, metricsAlerter{metrics: m}, dlqConfig{MaxRetries: 1, RetryBackoff: time.Millisecond})
	dlq.sleep = func(context.Context, time.Duration) error { return nil }
	p := NewProcessor(src, w, dlq, metricsAlerter{metrics: m}, Config{BatchSize: 10, TransientBackoff: time.Millisecond}, m)
	p.sleep = func(context.Context, time.Duration) error { return nil }

	batch := []fetchedMessage{
		fm(0, validMsg("ok-1")),
		fm(1, badSchemaMsg("bad-1", 999)),
	}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	out := dumpMetrics(t, m)
	for _, want := range []string{
		`indexer_disposition_total{disp="ok"} 1`,
		`indexer_disposition_total{disp="dlq"} 1`,
		`indexer_dlq_total{reason="unknown_schema_version"} 1`,
		`indexer_committed_offset{partition="0"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, out)
		}
	}
}

// TestDLQHardStopVsExhausted_MetricSemantics 锁定修复（review #1）：
// dlq_write_exhausted 不再误并入 dlq_hard_stop。
//   - 无 spill 硬停：dlq_hard_stop_total=1 且 dlq_write_exhausted_total=1（各一次，无双计）。
//   - 配 spill 成功越过（非硬停）：dlq_hard_stop_total 不出现（=0），dlq_write_exhausted_total=1。
func TestDLQHardStopVsExhausted_MetricSemantics(t *testing.T) {
	run := func(spillDir string) string {
		m := NewMetrics()
		al := metricsAlerter{metrics: m}
		src := &fakeSource{}
		w := &fakeWriter{statusByID: map[string]int{"a": 400}} // 毒丸 4xx
		sink := &fakeDLQSink{alwaysFail: true}                 // DLQ 写一直失败 → 耗尽
		dcfg := dlqConfig{MaxRetries: 1, RetryBackoff: time.Millisecond, SpillDir: spillDir}
		dlq := newDLQHandler(sink, al, dcfg)
		dlq.sleep = func(context.Context, time.Duration) error { return nil }
		p := NewProcessor(src, w, dlq, al, Config{BatchSize: 10, TransientBackoff: time.Millisecond}, m)
		p.sleep = func(context.Context, time.Duration) error { return nil }
		_ = p.processBatch(context.Background(), []fetchedMessage{fm(0, validMsg("a"))})
		return dumpMetrics(t, m)
	}

	// 无 spill：真实硬停。
	out := run("")
	if !strings.Contains(out, `indexer_dlq_hard_stop_total 1`) {
		t.Fatalf("no-spill hard stop: want dlq_hard_stop_total=1 in:\n%s", out)
	}
	if !strings.Contains(out, `indexer_dlq_write_exhausted_total 1`) {
		t.Fatalf("no-spill hard stop: want dlq_write_exhausted_total=1 in:\n%s", out)
	}

	// 配 spill 成功越过：不是硬停。
	out = run(t.TempDir())
	if strings.Contains(out, `indexer_dlq_hard_stop_total 1`) {
		t.Fatalf("spill escape must NOT count hard_stop in:\n%s", out)
	}
	if !strings.Contains(out, `indexer_dlq_write_exhausted_total 1`) {
		t.Fatalf("spill escape: want dlq_write_exhausted_total=1 in:\n%s", out)
	}
}
