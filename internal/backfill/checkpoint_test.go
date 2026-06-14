package backfill

import (
	"path/filepath"
	"testing"
)

// TestCheckpoint_PersistAndReload Advance 后重开应载入同一水位。
func TestCheckpoint_PersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	cp, err := OpenCheckpoint(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := cp.Advance("message", 10); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := cp.Advance("message1", 5); err != nil {
		t.Fatalf("advance: %v", err)
	}
	cp2, err := OpenCheckpoint(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if cp2.Get("message") != 10 || cp2.Get("message1") != 5 {
		t.Fatalf("reload mismatch: %d %d", cp2.Get("message"), cp2.Get("message1"))
	}
}

// TestCheckpoint_Monotonic 回退 / 同值 Advance 被忽略（幂等重跑安全）。
func TestCheckpoint_Monotonic(t *testing.T) {
	cp, err := OpenCheckpoint(filepath.Join(t.TempDir(), "cp.json"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := cp.Advance("message", 10); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := cp.Advance("message", 5); err != nil { // 回退被忽略
		t.Fatalf("advance: %v", err)
	}
	if cp.Get("message") != 10 {
		t.Fatalf("monotonic violated: %d", cp.Get("message"))
	}
}

// TestCheckpoint_EmptyPathInMemory path 为空 → 内存态，不报错（不可续传）。
func TestCheckpoint_EmptyPathInMemory(t *testing.T) {
	cp, err := OpenCheckpoint("")
	if err != nil {
		t.Fatalf("empty path must be ok (in-memory): %v", err)
	}
	if err := cp.Advance("message", 3); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if cp.Get("message") != 3 {
		t.Fatalf("in-memory advance failed")
	}
}

// TestCheckpoint_MissingFileStartsEmpty 文件不存在 → 空起步，不报错。
func TestCheckpoint_MissingFileStartsEmpty(t *testing.T) {
	cp, err := OpenCheckpoint(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil {
		t.Fatalf("missing file must start empty: %v", err)
	}
	if cp.Get("message") != 0 {
		t.Fatalf("missing file must yield 0 watermark")
	}
}

// TestCheckpoint_CreatesParentDir 🔴 P2-2：父目录不存在时 OpenCheckpoint 应建好目录（fail-fast），
// 之后 Advance 能直接持久化，不晚到第一次写才失败。
func TestCheckpoint_CreatesParentDir(t *testing.T) {
	// 多层尚不存在的父目录。
	path := filepath.Join(t.TempDir(), "nested", "deep", "cp.json")
	cp, err := OpenCheckpoint(path)
	if err != nil {
		t.Fatalf("OpenCheckpoint must create missing parent dirs: %v", err)
	}
	if err := cp.Advance("message", 5); err != nil {
		t.Fatalf("advance must persist after dir prepared: %v", err)
	}
	// 重开验证已落盘。
	cp2, err := OpenCheckpoint(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if cp2.Get("message") != 5 {
		t.Fatalf("checkpoint not persisted into created dir: %d", cp2.Get("message"))
	}
}
