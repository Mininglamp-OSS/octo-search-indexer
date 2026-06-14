package backfill

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Checkpoint 记录每个分表已灌的 message 表自增 id 高水位（keyset 续传游标）。
//
// 设计纪律（阶段 6 (d) 可续传）：
//   - 与实时增量游标表 octo_etl_es_cursor **物理隔离**——backfill 用独立的本地高水位记录，
//     绝不读 / 写实时游标，互不污染（设计细化 comment 明确）。
//   - 持久化为本地 JSON 文件，原子写（写临时文件 + rename），中断后从断点续跑。
//   - 高水位只在「该批已 bulk 写 ES 成功（含 raw_excluded 写入）且真异常已落 DLQ spill」后
//     才推进——即与对账门口径一致：推进的每个 id 都已「进 ES 或进 DLQ」终态处理。
type Checkpoint struct {
	// LastID[table] = 该分表已处理到的最大 message 表自增 id（含）。下批从 id>LastID 起读。
	LastID map[string]int64 `json:"last_id"`
}

// CheckpointStore 负责 Checkpoint 的加载 / 原子持久化。
type CheckpointStore struct {
	path string
	mu   sync.Mutex
	cp   Checkpoint
}

// OpenCheckpoint 从 path 载入已有 checkpoint（不存在则空起步）。path 为空表示不持久化
// （内存态，中断不可续——仅供测试 / 一次性小窗使用）。
func OpenCheckpoint(path string) (*CheckpointStore, error) {
	s := &CheckpointStore{path: path, cp: Checkpoint{LastID: map[string]int64{}}}
	if path == "" {
		return s, nil
	}
	// 提早建好父目录并 fail-fast（P2-2）：否则父目录缺失要等到第一次 Advance 写临时文件时才报错，
	// 那时已扫了一整批白工。开局就把目录准备好，让配置错误立刻暴露。
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("backfill: mkdir checkpoint dir: %w", err)
	}
	data, err := os.ReadFile(path) //nolint:gosec // path 来自运维配置，非用户输入
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil // 首次运行，空 checkpoint 起步
		}
		return nil, fmt.Errorf("backfill: read checkpoint %s: %w", path, err)
	}
	if len(data) == 0 {
		return s, nil
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("backfill: parse checkpoint %s: %w", path, err)
	}
	if cp.LastID == nil {
		cp.LastID = map[string]int64{}
	}
	s.cp = cp
	return s, nil
}

// Get 返回某分表当前高水位（不存在为 0，从头扫）。
func (s *CheckpointStore) Get(table string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cp.LastID[table]
}

// Advance 把某分表高水位推进到 newID 并原子持久化。
//
// 单调校验：newID 必须 > 当前水位才推进（防回退 / 重复推进）；newID<=当前则无声忽略
// （幂等重跑安全）。持久化失败返回错误，调用方应 STOP（避免「内存推进了但盘上没落」导致
// 重启后重扫 / 漏扫）。
func (s *CheckpointStore) Advance(table string, newID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newID <= s.cp.LastID[table] {
		return nil
	}
	prev := s.cp.LastID[table]
	s.cp.LastID[table] = newID
	if err := s.persistLocked(); err != nil {
		s.cp.LastID[table] = prev // 回滚内存态，保持与盘一致
		return err
	}
	return nil
}

// snapshot 返回 LastID 的有序拷贝（日志 / 测试用，确定性输出）。
func (s *CheckpointStore) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	tables := make([]string, 0, len(s.cp.LastID))
	for t := range s.cp.LastID {
		tables = append(tables, t)
	}
	sort.Strings(tables)
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		out = append(out, fmt.Sprintf("%s=%d", t, s.cp.LastID[t]))
	}
	return out
}

// persistLocked 原子写 checkpoint 文件（临时文件 + rename）。调用方须持锁。
func (s *CheckpointStore) persistLocked() error {
	if s.path == "" {
		return nil
	}
	data, err := json.Marshal(s.cp)
	if err != nil {
		return fmt.Errorf("backfill: marshal checkpoint: %w", err)
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".backfill-cp-*.tmp")
	if err != nil {
		return fmt.Errorf("backfill: create temp checkpoint: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		if rerr := os.Remove(tmpName); rerr != nil && !os.IsNotExist(rerr) {
			log.Printf("backfill: cleanup temp checkpoint %s: %v", tmpName, rerr)
		}
	}
	closeTmp := func() {
		if cerr := tmp.Close(); cerr != nil {
			log.Printf("backfill: close temp checkpoint %s: %v", tmpName, cerr)
		}
	}
	if _, err := tmp.Write(data); err != nil {
		closeTmp()
		cleanup()
		return fmt.Errorf("backfill: write temp checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		closeTmp()
		cleanup()
		return fmt.Errorf("backfill: sync temp checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("backfill: close temp checkpoint: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		cleanup()
		return fmt.Errorf("backfill: rename checkpoint into place: %w", err)
	}
	return nil
}
