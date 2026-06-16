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

// TestDLQSpill_MessageIDsInWindow 返回窗内 DLQ 行的 message_id 集合（抽样门排除集），
// 与 CountInWindow 同窗口口径；空 message_id 不入集合。
func TestDLQSpill_MessageIDsInWindow(t *testing.T) {
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
	got := s.MessageIDsInWindow(100, 300)
	if len(got) != 2 || !got["in1"] || !got["in2"] {
		t.Fatalf("window [100,300] must yield {in1,in2}, got %v", got)
	}
	if got["before"] || got["after"] {
		t.Fatalf("out-of-window ids must be excluded: %v", got)
	}
}

// TestDLQSpill_MessageIDsInWindow_SkipsEmptyID 空 message_id 行（去重键退化为 table:id）
// 不得把 table:id 当成 message_id 吐进排除集——否则会污染抽样门排除集（codex P2）。
func TestDLQSpill_MessageIDsInWindow_SkipsEmptyID(t *testing.T) {
	s, err := OpenDLQSpill(filepath.Join(t.TempDir(), "d"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer mustCloseT(t, s)
	if err := s.Write(dlqRecord{Table: "message", ID: 123, MessageID: "", CreatedAt: 150}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.Write(dlqRecord{Table: "message", ID: 124, MessageID: "real1", CreatedAt: 160}); err != nil {
		t.Fatalf("write: %v", err)
	}
	// 两行都在窗内、都计数；但排除集只含真实非空 message_id。
	if got := s.CountInWindow(100, 300); got != 2 {
		t.Fatalf("CountInWindow must count both rows, got %d", got)
	}
	got := s.MessageIDsInWindow(100, 300)
	if len(got) != 1 || !got["real1"] {
		t.Fatalf("exclusion set must contain only real non-empty message_id {real1}, got %v", got)
	}
	if got["message:123"] {
		t.Fatalf("table:id fallback key must NOT leak into exclusion set: %v", got)
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

// TestLoadDLQMessageIDsInWindow_RoundTrip 写 spill（经 in-memory DLQSpill，含 sidecar）后，
// 只读 loader 复原的 message_id 集合必须与 in-memory MessageIDsInWindow 完全一致——保证 standalone
// reconcile 与 inline backfill 两条字段级抽样门对 DLQ 行口径一致。
func TestLoadDLQMessageIDsInWindow_RoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	s, err := OpenDLQSpill(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, rec := range []dlqRecord{
		{ID: 1, MessageID: "m1", CreatedAt: 100},
		{ID: 2, MessageID: "m2", CreatedAt: 200},
		{ID: 3, MessageID: "m3", CreatedAt: 999}, // 窗外
		{Table: "message", ID: 4, MessageID: "", CreatedAt: 150}, // 空 message_id：不入集
	} {
		if err := s.Write(rec); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	want := s.MessageIDsInWindow(50, 300)
	if cerr := s.Close(); cerr != nil { // Close 推进 sidecar，落同步前缀
		t.Fatalf("close: %v", cerr)
	}

	got, err := LoadDLQMessageIDsInWindow(dir, 50, 300)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !sameStringBoolSet(got, want) {
		t.Fatalf("loader set %v != in-memory set %v (standalone/inline must agree)", got, want)
	}
	if got["m3"] {
		t.Fatalf("out-of-window id must be excluded from set")
	}
	if _, ok := got["message:4"]; ok || got[""] {
		t.Fatalf("empty message_id row must not enter exclusion set")
	}
}

// TestLoadDLQMessageIDsInWindow_EmptyDir dir=="" → 空集、无错（退化回旧 CompareSamples 行为）。
func TestLoadDLQMessageIDsInWindow_EmptyDir(t *testing.T) {
	got, err := LoadDLQMessageIDsInWindow("", 0, 1000)
	if err != nil {
		t.Fatalf("empty dir must not error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty dir must yield empty set, got %v", got)
	}
}

// TestLoadDLQMessageIDsInWindow_MissingFile spill 文件不存在 → 空集、无错（无 backfill 的环境）。
func TestLoadDLQMessageIDsInWindow_MissingFile(t *testing.T) {
	got, err := LoadDLQMessageIDsInWindow(filepath.Join(t.TempDir(), "nope"), 0, 1000)
	if err != nil {
		t.Fatalf("missing spill file must not error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("missing file must yield empty set, got %v", got)
	}
}

// TestLoadDLQMessageIDsInWindow_SyncedPrefixOnly 有 sidecar 时只解析已 fsync 的同步前缀，
// 崩溃留下的未 fsync 脏后缀（撕裂行）被丢弃，不污染排除集、不报错。
func TestLoadDLQMessageIDsInWindow_SyncedPrefixOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	clean := validLine(t, "m1", 100) + validLine(t, "m2", 200)
	dirty := `{"message_id":"m3","created_at":30` // 未 fsync 撕裂半行
	writeRawSpill(t, dir, clean+dirty)
	writeSyncedSidecar(t, dir, int64(len(clean)))

	got, err := LoadDLQMessageIDsInWindow(dir, 0, 1000)
	if err != nil {
		t.Fatalf("synced-prefix load must not error on dirty suffix: %v", err)
	}
	if !got["m1"] || !got["m2"] || len(got) != 2 {
		t.Fatalf("only synced-prefix ids expected, got %v", got)
	}
}

// TestLoadDLQMessageIDsInWindow_NoSidecarYieldsEmpty 无 sidecar（如崩溃 / 部分拷贝 / 损坏 sidecar，
// syncedLen==0）→ 无任何可信任的已 fsync 前缀，返回空排除集（与 OpenDLQSpill 把 offset 0 当「无持久
// 记录」一致）。绝不退化去信任未 fsync 的裸 .ndjson：那会让 standalone 信任不持久数据、对这些 id 跳过
// sample_missing，与 inline 门口径漂移。回退到「不排除」是安全侧（最坏把合法 DLQ 行算成 missing → 门
// 转红人工复核，绝不悄悄放过真漏灌）。
func TestLoadDLQMessageIDsInWindow_NoSidecarYieldsEmpty(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	// 即便整文件都是完整记录，无 sidecar 也不信任（不持久）。
	content := validLine(t, "m1", 100) + validLine(t, "m2", 200)
	writeRawSpill(t, dir, content)

	got, err := LoadDLQMessageIDsInWindow(dir, 0, 1000)
	if err != nil {
		t.Fatalf("no-sidecar load must not error (degrades to no-exclusion): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("no synced sidecar must yield empty exclusion set (untrusted, fail-closed safe side), got %v", got)
	}
}

// TestLoadDLQMessageIDsInWindow_SidecarSaysBytesButFileMissingFatal sidecar 记录了非零同步长度，
// 但 spill 文件不存在：持久状态丢失，致命（与 recoverAndLoad 一致，不可静默放过）。
func TestLoadDLQMessageIDsInWindow_SidecarSaysBytesButFileMissingFatal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeSyncedSidecar(t, dir, 42) // 谎称有 42 字节同步前缀，但没有 .ndjson 文件
	if _, err := LoadDLQMessageIDsInWindow(dir, 0, 1000); err == nil {
		t.Fatalf("sidecar bytes with missing spill file must be fatal")
	}
}

// TestLoadDLQMessageIDsInWindow_SyncedPrefixNotRecordBoundaryFatal 同步前缀不以记录边界（换行）
// 结尾 → 真损坏，致命（fail-closed；与 inline parseSyncedSpillPrefix 同一校验）。
func TestLoadDLQMessageIDsInWindow_SyncedPrefixNotRecordBoundaryFatal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	clean := validLine(t, "m1", 100)
	partial := `{"message_id":"m2"` // 半行，无换行
	writeRawSpill(t, dir, clean+partial)
	// sidecar 谎称同步前缀覆盖到半行中间（非换行边界）。
	writeSyncedSidecar(t, dir, int64(len(clean)+len(partial)))
	if _, err := LoadDLQMessageIDsInWindow(dir, 0, 1000); err == nil {
		t.Fatalf("synced prefix not ending on a record boundary must be fatal")
	}
}

// TestLoadDLQMessageIDsInWindow_CorruptSyncedRecordFatal 同步前缀内一条完整记录解析失败是真损坏
// → 报错（fail-closed：鉴权关键门绝不在可疑 DLQ 数据上静默放过）。
func TestLoadDLQMessageIDsInWindow_CorruptSyncedRecordFatal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	bad := "{not-json}\n"
	writeRawSpill(t, dir, bad)
	writeSyncedSidecar(t, dir, int64(len(bad)))
	if _, err := LoadDLQMessageIDsInWindow(dir, 0, 1000); err == nil {
		t.Fatalf("corrupt record in synced prefix must be fatal")
	}
}

// TestLoadDLQMessageIDsInWindow_ShorterThanSyncedFatal 文件比 sidecar 记录的同步长度还短：
// 持久层不一致 → 报错（不可静默放过）。
func TestLoadDLQMessageIDsInWindow_ShorterThanSyncedFatal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	line := validLine(t, "m1", 100)
	writeRawSpill(t, dir, line)
	writeSyncedSidecar(t, dir, int64(len(line))+50)
	if _, err := LoadDLQMessageIDsInWindow(dir, 0, 1000); err == nil {
		t.Fatalf("file shorter than synced offset must be fatal")
	}
}

func sameStringBoolSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
