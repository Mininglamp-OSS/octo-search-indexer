package backfill

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// dlqRecord 是 backfill 路径下「真异常 / ES 永久拒绝」消息的本地 DLQ 落地记录。
//
// 与实时 consumer 的区别：实时 indexer 有 Kafka DLQ topic 可投；backfill **绕开 Kafka**，
// 这两类「没进 ES 正文索引」的行无 topic 可投，故落本地 spill 文件并精确计数。该计数是阶段 6
// 对账门的权威 DLQ 输入（"ES 去重 + DLQ + 已知排除 == 源行数"）。
type dlqRecord struct {
	Reason    string `json:"reason"`     // backfill_payload_unparseable / permanent_es_reject
	Table     string `json:"table"`      // 源分表
	ID        int64  `json:"id"`         // 源自增 id
	MessageID string `json:"message_id"` // 源 message_id（= 本应的 ES _id；去重键）
	Payload   []byte `json:"payload"`    // 原始 payload 字节（供排查 / 回灌）
	CreatedAt int64  `json:"created_at"` // 源行 created_at（纪元秒），用于按窗对账
	SpilledAt int64  `json:"spilled_at"` // 落地时间（纪元秒）
}

// DLQSpill 把 backfill 的 DLQ 行可靠落地到本地文件并精确计数（对账门权威输入）。
//
// 设计（吸取 codex review 的 3 个 DLQ-accounting 缺陷）：
//   - **spill 文件是去重后的真相源**：以 message_id 为去重键（= ES _id，每条消息唯一）。
//     重开时**从既有文件重建去重集 + 计数**（修：resume 后 Count 归零会让 inline reconcile
//     把已 DLQ 的行当 ES 缺失，误报 mismatch）。
//   - **写入幂等**：同一 message_id 重复 Write 是 no-op（修：批在 DLQ 写之后、checkpoint
//     推进之前崩溃，resume 重读同一行会重复 append/计数，膨胀 DLQ）。这与「整条管线
//     `_id=message_id` 幂等」口径一致。
//   - **按窗计数**：CountInWindow 只数 created_at ∈ 窗的记录（修：reconcile 窗不覆盖整个 run
//     时，用全量 dlqCount 会把窗外的 DLQ 行也减掉 → false mismatch/false OK）。
//   - **fail-closed**：任一 spill 写盘失败立即返回错误，调用方须 STOP（真异常绝不静默消失）。
//
// DLQ 量级极小（真异常稀少；线上实测撤回都仅 0.21%、真不可解析的更罕见），故在内存保留全部
// 记录（去重键 → created_at）以支持按窗计数，开销可忽略。
type DLQSpill struct {
	path       string
	offsetPath string

	mu         sync.Mutex
	f          *os.File
	seen       map[string]dlqMeta // dedup key (message_id, 空则 table:id) -> 记账元数据
	pendingLen int64              // 当前文件总字节数（含尚未 fsync 的 append）
	syncedLen  int64              // 已 fsync 且已记入 offset sidecar 的持久长度（字节）
	nowUnix    func() int64
}

// dlqMeta 是单条去重 DLQ 记录的记账元数据。createdAt 供按窗对账；messageID 是源行的**真实**
// message_id（可能为空——空 message_id 行的去重键退化为 table:id，但其真实 message_id 仍记为空），
// 供抽样门排除集用：只有非空 message_id 才能与 ES doc / 抽样行对齐，故 MessageIDsInWindow 只吐非空值。
type dlqMeta struct {
	createdAt int64
	messageID string
}

// OpenDLQSpill 打开（或创建）spill 文件，并从既有内容重建去重集 + 计数（resume 安全）。
// dir 为空表示禁用 spill——此时若 backfill 遇到 DLQ 行必须硬停（见 runner），绝不允许 DLQ 行
// 静默消失破坏对账。
//
// 🔴 崩溃可续传性（P1，根因修法）：用一个持久 offset sidecar（`backfill-dlq.synced`）记录
// 「已 fsync 的 spill 字节长度」。Sync() 在每批 fsync spill 内容后，把当时长度原子写入 sidecar
// （temp+rename）。重开时把 spill **截断到 sidecar 记录的 offset**——丢弃其后**全部**未 fsync 的
// 脏后缀（可能是单条撕裂、也可能是一批多条 append 中途崩溃留下的多条脏记录）。这段脏后缀对应的
// 源 id 从未 Advance 过 checkpoint（Advance 在 Sync 之后），resume 必重读重写，丢弃绝对安全。
// 截断后只解析 [0, offset) 这段「既 fsync 又与 checkpoint 一致」的干净前缀——其中任何解析失败都是
// 真损坏（位翻转/篡改），致命。这样彻底取代了「猜哪条是撕裂尾行」的脆弱启发式。
func OpenDLQSpill(dir string) (*DLQSpill, error) {
	if dir == "" {
		return nil, fmt.Errorf("backfill: DLQ spill dir is required (DLQ rows must be durably accounted; refuse to silently drop)")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("backfill: mkdir spill dir: %w", err)
	}
	path := filepath.Join(dir, "backfill-dlq.ndjson")
	offsetPath := filepath.Join(dir, "backfill-dlq.synced")

	syncedLen, err := readSyncedOffset(offsetPath)
	if err != nil {
		return nil, err
	}
	seen, err := recoverAndLoad(path, syncedLen)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, fmt.Errorf("backfill: open spill file: %w", err)
	}
	// fsync 父目录，让新建的 spill 文件「目录项」本身可崩溃恢复——否则 host 崩溃后 checkpoint
	// 可能存活而 backfill-dlq.ndjson 整个消失，replay 漏计该文件里所有已被游标越过的 DLQ 行。
	// 文件内容的 fsync 由 Sync()/Close() 负责；这里补的是目录项的 fsync（仅创建时一次性成本）。
	if err := fsyncDir(dir); err != nil {
		if cerr := f.Close(); cerr != nil {
			return nil, fmt.Errorf("backfill: %w (and close spill: %v)", err, cerr)
		}
		return nil, err
	}
	return &DLQSpill{
		path: path, offsetPath: offsetPath, f: f, seen: seen,
		pendingLen: syncedLen, syncedLen: syncedLen,
		nowUnix: func() int64 { return time.Now().Unix() },
	}, nil
}

// readSyncedOffset 读 offset sidecar（不存在/空 → 0：表示尚无任何已 fsync 的 spill 前缀）。
// sidecar 由 temp+rename 原子写，故不会出现撕裂；解析失败即真损坏 → 致命。
func readSyncedOffset(offsetPath string) (int64, error) {
	data, err := os.ReadFile(offsetPath) //nolint:gosec // path 来自运维配置
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("backfill: read spill offset sidecar: %w", err)
	}
	s := strings.TrimSpace(string(data))
	if s == "" {
		return 0, nil
	}
	off, err := strconv.ParseInt(s, 10, 64)
	if err != nil || off < 0 {
		return 0, fmt.Errorf("backfill: corrupt spill offset sidecar %q (atomic-rename written, so this is real corruption): %v", s, err)
	}
	return off, nil
}

// recoverAndLoad 把 spill 截断到 syncedLen（丢弃未 fsync 的脏后缀），再严格解析 [0, syncedLen)
// 干净前缀，重建去重集 + 计数。
func recoverAndLoad(path string, syncedLen int64) (map[string]dlqMeta, error) {
	seen := map[string]dlqMeta{}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			if syncedLen != 0 {
				return nil, fmt.Errorf("backfill: spill offset sidecar says %d bytes but spill file is missing (durability state lost; manual inspection required)", syncedLen)
			}
			return seen, nil
		}
		return nil, fmt.Errorf("backfill: stat spill file: %w", err)
	}
	size := info.Size()
	if size < syncedLen {
		// 文件比已记录的同步长度还短：持久层不一致（被外部截断/损坏），不可静默放过。
		return nil, fmt.Errorf("backfill: spill file (%d bytes) shorter than synced offset (%d) — durability state lost; manual inspection required", size, syncedLen)
	}
	if size > syncedLen {
		// 丢弃 syncedLen 之后的未 fsync 脏后缀（崩溃恢复；这段对应的源 id 从未 Advance 过 checkpoint）。
		if err := os.Truncate(path, syncedLen); err != nil {
			return nil, fmt.Errorf("backfill: truncate un-synced dirty spill suffix (%d -> %d bytes) during crash recovery: %w", size, syncedLen, err)
		}
		fmt.Fprintf(os.Stderr, "backfill: recovered spill by truncating a %d-byte un-fsynced dirty suffix to synced offset %d\n", size-syncedLen, syncedLen)
	}
	if syncedLen == 0 {
		return seen, nil
	}

	data, err := os.ReadFile(path) //nolint:gosec // path 来自运维配置
	if err != nil {
		return nil, fmt.Errorf("backfill: read spill for replay: %w", err)
	}
	// [0, syncedLen) 是既 fsync、又与 checkpoint 一致的干净前缀：必由完整记录构成、以换行结尾。
	// 任何偏差（非换行结尾 / 解析失败）都是该持久区内的真损坏，致命。
	if data[len(data)-1] != '\n' {
		return nil, fmt.Errorf("backfill: synced spill prefix does not end at a record boundary (real corruption)")
	}
	for _, line := range bytes.Split(data[:len(data)-1], []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec dlqRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("backfill: corrupt record in synced spill prefix during replay (real corruption): %w", err)
		}
		seen[dedupKey(rec)] = dlqMeta{createdAt: rec.CreatedAt, messageID: rec.MessageID}
	}
	return seen, nil
}

// dedupKey 以 message_id 为去重键（= ES _id，每条消息唯一）；空 message_id 退化为 table:id。
func dedupKey(rec dlqRecord) string {
	if rec.MessageID != "" {
		return rec.MessageID
	}
	return fmt.Sprintf("%s:%d", rec.Table, rec.ID)
}

// fsyncDir 打开并 fsync 一个目录，使其下新建 / 重命名的目录项可崩溃恢复。
func fsyncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // dir 来自运维配置，非用户输入
	if err != nil {
		return fmt.Errorf("backfill: open spill dir for fsync: %w", err)
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return fmt.Errorf("backfill: fsync spill dir: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("backfill: close spill dir after fsync: %w", closeErr)
	}
	return nil
}

// Write 幂等落地一条 DLQ 记录：同一去重键已存在则 no-op（不重复 append/计数）；否则 append
// 并记入去重集。写盘失败返回错误（fail-closed）。
func (s *DLQSpill) Write(rec dlqRecord) error {
	key := dedupKey(rec)
	rec.Reason = reasonOrDefault(rec.Reason)
	rec.SpilledAt = s.nowUnix()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[key]; ok {
		return nil // 幂等：该源行已记账，不重复
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("backfill: marshal dlq record (id=%d msg=%s): %w", rec.ID, rec.MessageID, err)
	}
	line := append(data, '\n')
	if _, err := s.f.Write(line); err != nil {
		return fmt.Errorf("backfill: write dlq spill (id=%d msg=%s): %w", rec.ID, rec.MessageID, err)
	}
	s.pendingLen += int64(len(line))
	s.seen[key] = dlqMeta{createdAt: rec.CreatedAt, messageID: rec.MessageID}
	return nil
}

// reasonOrDefault 给未显式置 reason 的记录补默认（payload 不可解析）。
func reasonOrDefault(r string) string {
	if r == "" {
		return "backfill_payload_unparseable"
	}
	return r
}

// Count 返回已记账的去重 DLQ 记录总数（日志 / 全量对账用）。
func (s *DLQSpill) Count() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.seen))
}

// CountInWindow 返回 created_at ∈ [fromUnix, toUnix] 的去重 DLQ 记录数（按窗对账门用）。
// 与 internal/recon 的 range filter（gte/lte）口径一致。
func (s *DLQSpill) CountInWindow(fromUnix, toUnix int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, m := range s.seen {
		if m.createdAt >= fromUnix && m.createdAt <= toUnix {
			n++
		}
	}
	return n
}

// MessageIDsInWindow 返回 created_at ∈ [fromUnix, toUnix] 的去重 DLQ 记录的**真实** message_id 集合。
// 用于字段级抽样对账门（recon.CompareSamplesExcluding）：这些行**本应不在** ES 正文索引里
// （真异常 / 永久拒绝，已记账为 DLQ），故抽样命中它们时「ES 无 doc」是预期，不算 missing。
// 注意：空 message_id（源行 message_id 缺失/为空）不进集合——这类行去重键退化为 table:id，无法与
// ES doc / 抽样行（抽样器按 message_id 取行）对齐，不会被抽到，吐出 table:id 反而会污染排除集。
func (s *DLQSpill) MessageIDsInWindow(fromUnix, toUnix int64) map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]bool)
	for _, m := range s.seen {
		if m.messageID != "" && m.createdAt >= fromUnix && m.createdAt <= toUnix {
			out[m.messageID] = true
		}
	}
	return out
}

// Sync 把已 append 的 DLQ 记录刷盘（fsync），再原子推进 offset sidecar 到当前文件长度。
// **必须在推进 checkpoint 前调用**：否则主机崩溃 / 延迟写回失败可能让 checkpoint 跳过某些 id，
// 而它们的 DLQ 记录尚未落盘 → resume 后 DLQ 漏计、该行不经手动回退 checkpoint 不可恢复
// （durability ordering：先让 DLQ 落盘 + offset 记账，再推进游标）。
//
// 顺序保证崩溃可恢复：先 fsync spill 内容（[0,pendingLen) 全部持久），再原子写 offset=pendingLen。
// 若崩在「fsync 后、写 sidecar 前」，offset 仍是旧值，多出的已 fsync 后缀会在重开时被当脏后缀
// 截掉——保守但安全（那批 checkpoint 也没推进，resume 会重写）。
func (s *DLQSpill) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	if s.pendingLen == s.syncedLen {
		return nil // 无新增，免去重复 fsync / sidecar 写
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("backfill: sync dlq spill: %w", err)
	}
	if err := writeSyncedOffset(s.offsetPath, s.pendingLen); err != nil {
		return err
	}
	s.syncedLen = s.pendingLen
	return nil
}

// writeSyncedOffset 原子写 offset sidecar（temp + fsync + rename + fsync dir），使其本身崩溃可恢复。
func writeSyncedOffset(offsetPath string, off int64) error {
	dir := filepath.Dir(offsetPath)
	tmp, err := os.CreateTemp(dir, ".backfill-dlq-synced-*.tmp")
	if err != nil {
		return fmt.Errorf("backfill: create temp offset sidecar: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() {
		if rerr := os.Remove(tmpName); rerr != nil && !os.IsNotExist(rerr) {
			fmt.Fprintf(os.Stderr, "backfill: cleanup temp offset sidecar %s: %v\n", tmpName, rerr)
		}
	}
	closeTmp := func() {
		if cerr := tmp.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "backfill: close temp offset sidecar %s: %v\n", tmpName, cerr)
		}
	}
	if _, err := tmp.WriteString(strconv.FormatInt(off, 10)); err != nil {
		closeTmp()
		cleanup()
		return fmt.Errorf("backfill: write temp offset sidecar: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		closeTmp()
		cleanup()
		return fmt.Errorf("backfill: sync temp offset sidecar: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("backfill: close temp offset sidecar: %w", err)
	}
	if err := os.Rename(tmpName, offsetPath); err != nil {
		cleanup()
		return fmt.Errorf("backfill: rename offset sidecar into place: %w", err)
	}
	if err := fsyncDir(dir); err != nil {
		return err
	}
	return nil
}

// Path 返回 spill 文件路径（日志 / 运维用）。
func (s *DLQSpill) Path() string { return s.path }

// Close 把缓冲刷盘（fsync）、推进 offset sidecar 到当前长度，再关闭文件。
// Close 是**优雅停止**路径（非崩溃），故把已写入的记录全部标记为持久（推进 sidecar）——
// 与 Sync 同样的「先 fsync 内容、再原子写 sidecar」顺序，下次重开不会把这些已落盘记录当脏后缀截掉。
func (s *DLQSpill) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	var err error
	if s.pendingLen != s.syncedLen {
		if serr := s.f.Sync(); serr != nil {
			err = fmt.Errorf("backfill: sync dlq spill on close: %w", serr)
		} else if oerr := writeSyncedOffset(s.offsetPath, s.pendingLen); oerr != nil {
			err = oerr
		} else {
			s.syncedLen = s.pendingLen
		}
	}
	if cerr := s.f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	s.f = nil
	return err
}
