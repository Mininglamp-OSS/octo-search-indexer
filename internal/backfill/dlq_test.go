package backfill

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestDLQSpill_WriteAndCount 写若干条 → 计数精确等于落盘行数。
func TestDLQSpill_WriteAndCount(t *testing.T) {
	s, err := OpenDLQSpill(filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	for i := 0; i < 3; i++ {
		if err := s.Write(dlqRecord{ID: int64(i), MessageID: fmt.Sprintf("m%d", i)}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if s.Count() != 3 {
		t.Fatalf("count=%d want 3", s.Count())
	}
	data := mustRead(t, s.Path())
	if got := countLines(string(data)); got != 3 {
		t.Fatalf("file lines=%d want 3 (count must equal durable rows)", got)
	}
}

// TestDLQSpill_EmptyDirRejected dir 为空 → 拒绝构造（真异常不得静默消失）。
func TestDLQSpill_EmptyDirRejected(t *testing.T) {
	if _, err := OpenDLQSpill(""); err == nil {
		t.Fatalf("empty spill dir must be rejected (fail-closed)")
	}
}

// TestDLQSpill_AppendAcrossOpens 重开同目录 → append，不截断旧记录。
func TestDLQSpill_AppendAcrossOpens(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	s1, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := s1.Write(dlqRecord{ID: 1, MessageID: "a"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if cerr := s1.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	s2, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s2)
	if err := s2.Write(dlqRecord{ID: 2, MessageID: "b"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	data := mustRead(t, s2.Path())
	if got := countLines(string(data)); got != 2 {
		t.Fatalf("append across opens must keep both records, got %d lines", got)
	}
}

func countLines(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			n++
		}
	}
	return n
}

// TestDLQSpill_IdempotentByMessageID 同一 message_id 重复 Write → no-op，不重复 append/计数
// （codex P2-idempotency：批崩在 DLQ 写后、checkpoint 推进前，resume 重读同行不得膨胀 DLQ）。
func TestDLQSpill_IdempotentByMessageID(t *testing.T) {
	s, err := OpenDLQSpill(filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	for i := 0; i < 5; i++ {
		if err := s.Write(dlqRecord{ID: 7, MessageID: "same", CreatedAt: 100}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if s.Count() != 1 {
		t.Fatalf("duplicate message_id must dedupe to 1, got %d", s.Count())
	}
	data := mustRead(t, s.Path())
	if got := countLines(string(data)); got != 1 {
		t.Fatalf("file must hold exactly 1 record, got %d lines", got)
	}
}

// TestDLQSpill_ReplayCountOnReopen 重开既有 spill → Count 从文件重建，不归零
// （codex P1：resume 后 Count 归零会让 inline reconcile 把已 DLQ 行当 ES 缺失 → false mismatch）。
func TestDLQSpill_ReplayCountOnReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	s1, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := s1.Write(dlqRecord{ID: int64(i), MessageID: fmt.Sprintf("m%d", i), CreatedAt: 100}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if cerr := s1.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	s2, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mustCloseT(t, s2)
	if s2.Count() != 3 {
		t.Fatalf("reopen must rebuild count from spill file, got %d want 3", s2.Count())
	}
	// 重开后再写一条已存在的 → 仍幂等。
	if err := s2.Write(dlqRecord{ID: 1, MessageID: "m1", CreatedAt: 100}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if s2.Count() != 3 {
		t.Fatalf("re-writing an existing key after reopen must stay 3, got %d", s2.Count())
	}
}

// TestDLQSpill_CountInWindow 仅数 created_at ∈ 窗的记录
// （codex P2-window：窗不覆盖整个 run 时，用全量 DLQ 会减掉窗外行 → false mismatch/OK）。
func TestDLQSpill_CountInWindow(t *testing.T) {
	s, err := OpenDLQSpill(filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	writes := []dlqRecord{
		{ID: 1, MessageID: "before", CreatedAt: 50},
		{ID: 2, MessageID: "in1", CreatedAt: 150},
		{ID: 3, MessageID: "in2", CreatedAt: 200},
		{ID: 4, MessageID: "after", CreatedAt: 500},
	}
	for _, w := range writes {
		if err := s.Write(w); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if s.Count() != 4 {
		t.Fatalf("total count=%d want 4", s.Count())
	}
	if got := s.CountInWindow(100, 300); got != 2 {
		t.Fatalf("window [100,300] must count 2 (in1,in2), got %d", got)
	}
	// 边界含端点（gte/lte，与 recon range filter 一致）。
	if got := s.CountInWindow(50, 50); got != 1 {
		t.Fatalf("inclusive boundary must count 'before', got %d", got)
	}
}

// TestDLQSpill_Sync Sync 刷盘后记录可被另一个只读 reopen 看到（落盘可见性）。
func TestDLQSpill_Sync(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	s, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	if err := s.Write(dlqRecord{ID: 1, MessageID: "x", CreatedAt: 100}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	// 另一个 reopen（replay）应已能看到这条记录（已落盘）。
	s2, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mustCloseT(t, s2)
	if s2.Count() != 1 {
		t.Fatalf("synced record must be visible to reopen, got %d", s2.Count())
	}
}

// writeRawSpill 直接往 spill 文件写原始字节（模拟崩溃留下的尾部状态），不经 DLQSpill。
func writeRawSpill(t *testing.T, dir, content string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, "backfill-dlq.ndjson")
	if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
		t.Fatalf("write raw spill: %v", err)
	}
	return path
}

// validLine 返回一条合法 NDJSON 记录行（含结尾换行）。
func validLine(t *testing.T, mid string, createdAt int64) string {
	t.Helper()
	rec := dlqRecord{Reason: "backfill_payload_unparseable", MessageID: mid, CreatedAt: createdAt}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b) + "\n"
}

// TestDLQSpill_RecoverTornTrailingLine 🔴 P1(a)：尾部半写行（无结尾换行）→ 优雅截断、启动成功，
// 完整行计数正确，且文件被截到只剩完整行。
func TestDLQSpill_RecoverTornTrailingLine(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	// 两条完整行 + 一条未 fsync 的半写尾行（无换行结尾，且 JSON 被截断）。
	torn := `{"reason":"backfill_payload_unparseable","message_id":"m3","created_at":300`
	content := validLine(t, "m1", 100) + validLine(t, "m2", 200) + torn
	path := writeRawSpill(t, dir, content)

	s, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("torn trailing line must recover, got error: %v", err)
	}
	defer mustCloseT(t, s)
	if s.Count() != 2 {
		t.Fatalf("recovered count must be 2 (complete lines), got %d", s.Count())
	}
	// 文件应被截断到只剩两条完整行（torn 尾行已去除）。
	data := mustRead(t, path)
	if got := countLines(string(data)); got != 2 {
		t.Fatalf("file must be truncated to 2 complete lines, got %d", got)
	}
	// 截断后继续写一条新记录 → 仍是完整行，重开可再解析（不会和旧半行拼成损坏行）。
	if err := s.Write(dlqRecord{MessageID: "m4", CreatedAt: 400}); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	if cerr := s.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}
	s2, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("reopen after recovery+append must succeed: %v", err)
	}
	defer mustCloseT(t, s2)
	if s2.Count() != 3 {
		t.Fatalf("after recovery+append, count must be 3, got %d", s2.Count())
	}
}

// TestDLQSpill_NonTrailingCorruptionFatal 🔴 P1(b)：非末尾的完整行（以换行结尾）损坏 → 仍致命，
// 拒绝启动（那是真损坏，不是未 fsync 的半行）。
func TestDLQSpill_NonTrailingCorruptionFatal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	// 第一行损坏但以换行结尾（=完整行损坏），后面跟一条合法行。
	content := "{this is not valid json}\n" + validLine(t, "m2", 200)
	writeRawSpill(t, dir, content)
	if _, err := OpenDLQSpill(dir); err == nil {
		t.Fatalf("non-trailing corrupt (newline-terminated) line must be fatal")
	}
}

// TestDLQSpill_AllCompleteLinesNoTruncate 全是完整行（末行有换行）→ 不截断、不豁免，全部计入。
func TestDLQSpill_AllCompleteLinesNoTruncate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	content := validLine(t, "m1", 100) + validLine(t, "m2", 200)
	path := writeRawSpill(t, dir, content)
	s, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	if s.Count() != 2 {
		t.Fatalf("count=%d want 2", s.Count())
	}
	if got := countLines(string(mustRead(t, path))); got != 2 {
		t.Fatalf("complete file must be untouched, got %d lines", got)
	}
}

// TestDLQSpill_TornSingleLineOnly 只有一条半写行（无完整行）→ 截断为空、启动成功、计数 0。
func TestDLQSpill_TornSingleLineOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	writeRawSpill(t, dir, `{"message_id":"m1","created_at":1`)
	s, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("single torn line must recover: %v", err)
	}
	defer mustCloseT(t, s)
	if s.Count() != 0 {
		t.Fatalf("count must be 0 after truncating the only (torn) line, got %d", s.Count())
	}
}
