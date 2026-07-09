package fileextract

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
)

// dumpMetrics 把私有 registry 渲染成稳定文本（每 series 一行，含 label），
// 供断言精确匹配 series 而不依赖 expfmt 格式（惯例对齐 sibling internal/consumer/metrics_test.go）。
func dumpMetrics(t *testing.T, c *counters) string {
	t.Helper()
	families, err := c.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var lines []string
	for _, mf := range families {
		name := mf.GetName()
		for _, metric := range mf.Metric {
			if metric.Counter == nil {
				continue
			}
			lines = append(lines, fmt.Sprintf("%s%s %s", name, formatLabels(metric.Label), formatVal(metric.Counter.GetValue())))
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

// formatVal 把整数值样本渲染成无小数点形式。
func formatVal(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}
