package main

import (
	"os"
	"strings"
	"testing"
)

// resetBackfillEnv 清空 BACKFILL_ 前缀所有 env，避免 test 污染。
func resetBackfillEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		if idx := strings.IndexByte(kv, '='); idx > 0 {
			key := kv[:idx]
			if strings.HasPrefix(key, "BACKFILL_") {
				t.Setenv(key, "")
			}
		}
	}
}

// TestSplitCSV 覆盖 CSV 解析各种输入形态。
func TestSplitCSV(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", []string{}},
		{"a", []string{"a"}},
		{"a,b,c", []string{"a", "b", "c"}},
		{"a, b , c", []string{"a", "b", "c"}}, // trim
		{",a,,b,", []string{"a", "b"}},        // 忽略空
	}
	for _, c := range cases {
		got := splitCSV(c.in)
		if len(got) != len(c.want) {
			t.Errorf("splitCSV(%q): len got=%d want=%d (%v)", c.in, len(got), len(c.want), got)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("splitCSV(%q)[%d]: got %q want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}

// TestEnvOr 默认值 / env 覆盖。
func TestEnvOr(t *testing.T) {
	resetBackfillEnv(t)
	if got := envOr("BACKFILL_NOT_SET_XYZ", "default"); got != "default" {
		t.Errorf("envOr default: got %q", got)
	}
	t.Setenv("BACKFILL_TEST_KEY", "override")
	if got := envOr("BACKFILL_TEST_KEY", "default"); got != "override" {
		t.Errorf("envOr override: got %q", got)
	}
}

// TestEnvInt 有效整数 / 无效字符串走 default / env 缺省。
func TestEnvInt(t *testing.T) {
	resetBackfillEnv(t)
	if got := envInt("BACKFILL_NOT_SET", 42); got != 42 {
		t.Errorf("envInt default: got %d", got)
	}
	t.Setenv("BACKFILL_NUM", "100")
	if got := envInt("BACKFILL_NUM", 42); got != 100 {
		t.Errorf("envInt override: got %d", got)
	}
	t.Setenv("BACKFILL_BAD", "not-a-number")
	if got := envInt("BACKFILL_BAD", 42); got != 42 {
		t.Errorf("envInt invalid falls back to default: got %d", got)
	}
}
