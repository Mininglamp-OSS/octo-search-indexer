package recon

import "testing"

// TestReconcile_AllIndexed 全部源行都进了 ES（含 raw_excluded doc），无 DLQ → 对平。
func TestReconcile_AllIndexed(t *testing.T) {
	r := Reconcile(Counts{SourceRows: 1000, ESDocs: 1000, RawExcluded: 50, DLQ: 0})
	if !r.OK || r.Diff != 0 || r.Expected != 1000 {
		t.Fatalf("expected OK diff=0 expected=1000, got %+v", r)
	}
}

// TestReconcile_DLQExcludedFromExpected DLQ 条目不写 ES → 期望 doc 数扣除 DLQ。
func TestReconcile_DLQExcludedFromExpected(t *testing.T) {
	// 1000 源行，3 条进 DLQ（未写 ES）→ 期望 997 doc。ES 实际 997 → 对平。
	r := Reconcile(Counts{SourceRows: 1000, ESDocs: 997, DLQ: 3})
	if !r.OK {
		t.Fatalf("expected OK, got %+v", r)
	}
	if r.Expected != 997 {
		t.Fatalf("expected 997, got %d", r.Expected)
	}
}

// TestReconcile_RawExcludedStillCounts raw_excluded 仍是一条 ES doc → 不从期望扣除。
func TestReconcile_RawExcludedStillCounts(t *testing.T) {
	// 全部 1000 行都写入 ES（其中 200 条 raw_excluded，content=null 但占 doc）→ ES 1000 → 对平。
	r := Reconcile(Counts{SourceRows: 1000, ESDocs: 1000, RawExcluded: 200, DLQ: 0})
	if !r.OK {
		t.Fatalf("raw_excluded must NOT be subtracted from expected; got %+v", r)
	}
}

// TestReconcile_Shortfall ES 比期望少（漏灌/丢失）→ Diff<0，不对平（STOP 信号）。
func TestReconcile_Shortfall(t *testing.T) {
	r := Reconcile(Counts{SourceRows: 1000, ESDocs: 990, DLQ: 0})
	if r.OK || r.Diff != -10 {
		t.Fatalf("expected mismatch diff=-10, got %+v", r)
	}
}

// TestReconcile_Surplus ES 比期望多（越界/串窗）→ Diff>0，不对平。
func TestReconcile_Surplus(t *testing.T) {
	r := Reconcile(Counts{SourceRows: 1000, ESDocs: 1005, DLQ: 0})
	if r.OK || r.Diff != 5 {
		t.Fatalf("expected mismatch diff=+5, got %+v", r)
	}
}

// TestReconcile_DLQAndShortfall 综合：DLQ 扣除后仍缺口。
func TestReconcile_DLQAndShortfall(t *testing.T) {
	// 源 500，DLQ 10 → 期望 490；ES 只有 485 → 缺 5。
	r := Reconcile(Counts{SourceRows: 500, ESDocs: 485, DLQ: 10})
	if r.OK || r.Diff != -5 || r.Expected != 490 {
		t.Fatalf("expected expected=490 diff=-5, got %+v", r)
	}
}

func TestReport_StringRendersStatus(t *testing.T) {
	ok := Reconcile(Counts{SourceRows: 10, ESDocs: 10}).String()
	if !contains(ok, "OK") {
		t.Fatalf("OK report should say OK: %s", ok)
	}
	bad := Reconcile(Counts{SourceRows: 10, ESDocs: 9}).String()
	if !contains(bad, "MISMATCH") {
		t.Fatalf("mismatch report should say MISMATCH: %s", bad)
	}
}

func TestSafeTableName(t *testing.T) {
	for _, ok := range []string{"message", "message1", "msg_4"} {
		if !safeTableName(ok) {
			t.Fatalf("%q should be safe", ok)
		}
	}
	for _, bad := range []string{"", "msg;drop", "msg table", "msg`x", "msg-1"} {
		if safeTableName(bad) {
			t.Fatalf("%q must be rejected", bad)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
