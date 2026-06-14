package backfill

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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

// writeSyncedSidecar 写 offset sidecar（模拟已 fsync 的持久长度）。
func writeSyncedSidecar(t *testing.T, dir string, off int64) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "backfill-dlq.synced"), []byte(strconv.FormatInt(off, 10)), 0o640); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
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

// TestDLQSpill_RecoverDirtySuffix 🔴 P1（根因）：sidecar 记录已 fsync 的前缀长度；崩溃留下的
// 未 fsync 脏后缀（任意条数、任意撕裂形态）在重开时整段截到 sidecar offset，干净前缀完整保留。
func TestDLQSpill_RecoverDirtySuffix(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	clean := validLine(t, "m1", 100) + validLine(t, "m2", 200) // 已 fsync 的干净前缀
	// 脏后缀：一条完整行 + 一条撕裂半行（模拟一批多条 append 中途崩溃）。
	dirty := validLine(t, "m3", 300) + `{"message_id":"m4","created_at":40`
	path := writeRawSpill(t, dir, clean+dirty)
	writeSyncedSidecar(t, dir, int64(len(clean))) // 只有 clean 段被 fsync 过

	s, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("dirty suffix must recover to sidecar offset, got: %v", err)
	}
	defer mustCloseT(t, s)
	if s.Count() != 2 {
		t.Fatalf("recovered count must be 2 (synced prefix), got %d", s.Count())
	}
	if got := countLines(string(mustRead(t, path))); got != 2 {
		t.Fatalf("file must be truncated to the 2 synced lines, got %d", got)
	}
	// 续写后重开应得 3 条（脏后缀的 m3/m4 已被丢弃，不会拼成损坏行）。
	if err := s.Write(dlqRecord{MessageID: "m5", CreatedAt: 500}); err != nil {
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

// TestDLQSpill_CorruptionWithinSyncedPrefixFatal 🔴 已 fsync 前缀（[0,offset)）内的记录损坏 = 真损坏
// （位翻转/篡改），致命拒启动——不被当作可丢弃的脏后缀。
func TestDLQSpill_CorruptionWithinSyncedPrefixFatal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	// 前缀内：一条损坏完整行 + 一条合法行，整段都在 sidecar offset 之内。
	prefix := "{this is not valid json}\n" + validLine(t, "m2", 200)
	writeRawSpill(t, dir, prefix)
	writeSyncedSidecar(t, dir, int64(len(prefix))) // 整个前缀都被「fsync 过」
	if _, err := OpenDLQSpill(dir); err == nil {
		t.Fatalf("corruption within the synced prefix must be fatal")
	}
}

// TestDLQSpill_SyncedPrefixNotEndingAtBoundaryFatal sidecar offset 落在记录中间（前缀不以换行结尾）
// = 持久状态不一致，致命。
func TestDLQSpill_SyncedPrefixNotEndingAtBoundaryFatal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	line := validLine(t, "m1", 100)
	writeRawSpill(t, dir, line)
	writeSyncedSidecar(t, dir, int64(len(line))-3) // 截在记录中间
	if _, err := OpenDLQSpill(dir); err == nil {
		t.Fatalf("synced offset not at a record boundary must be fatal")
	}
}

// TestDLQSpill_NoSidecarTruncatesAll 无 sidecar（offset=0）+ 既有内容 → 视为全未 fsync，截到 0。
// （首次崩溃在第一批 Sync 之前的退化情形；那批 checkpoint 也没推进，resume 重写。）
func TestDLQSpill_NoSidecarTruncatesAll(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	path := writeRawSpill(t, dir, validLine(t, "m1", 100)+validLine(t, "m2", 200))
	s, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("no sidecar must recover (truncate all), got: %v", err)
	}
	defer mustCloseT(t, s)
	if s.Count() != 0 {
		t.Fatalf("no sidecar => everything un-synced => count 0, got %d", s.Count())
	}
	if got := countLines(string(mustRead(t, path))); got != 0 {
		t.Fatalf("file must be truncated to empty, got %d lines", got)
	}
}

// TestDLQSpill_FileShorterThanSidecarFatal 文件比 sidecar 记录的同步长度还短 → 持久状态丢失，致命。
func TestDLQSpill_FileShorterThanSidecarFatal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	line := validLine(t, "m1", 100)
	writeRawSpill(t, dir, line)
	writeSyncedSidecar(t, dir, int64(len(line))+50) // 谎称同步长度超过文件实际大小
	if _, err := OpenDLQSpill(dir); err == nil {
		t.Fatalf("file shorter than synced offset must be fatal")
	}
}

// TestDLQSpill_AllSyncedNoTruncate 全部内容都在 sidecar offset 内（已 fsync）→ 不截断、全部计入。
func TestDLQSpill_AllSyncedNoTruncate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	content := validLine(t, "m1", 100) + validLine(t, "m2", 200)
	path := writeRawSpill(t, dir, content)
	writeSyncedSidecar(t, dir, int64(len(content))) // 整段都已 fsync
	s, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	if s.Count() != 2 {
		t.Fatalf("count=%d want 2", s.Count())
	}
	if got := countLines(string(mustRead(t, path))); got != 2 {
		t.Fatalf("fully-synced file must be untouched, got %d lines", got)
	}
}

// TestDLQSpill_TornSingleLineOnly 只有一条半写行（无 sidecar）→ 截断为空、启动成功、计数 0。
func TestDLQSpill_TornSingleLineOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	writeRawSpill(t, dir, `{"message_id":"m1","created_at":1`)
	s, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("single torn line must recover: %v", err)
	}
	defer mustCloseT(t, s)
	if s.Count() != 0 {
		t.Fatalf("count must be 0 after truncating the only (un-synced) line, got %d", s.Count())
	}
}
