package backfill

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// Stats 汇总一次 backfill 运行的处置计数（用于日志 + 对账核对）。
type Stats struct {
	// Read 是从源分表读出的总行数。
	Read int64
	// Indexed 是成功写入 ES 的文档数（含 raw_excluded 的 content=null doc）。
	Indexed int64
	// RawExcluded 是其中 raw_excluded（Signal / 非文本）的 doc 数（仍计入 Indexed，观测用）。
	RawExcluded int64
	// DLQ 是落本地 DLQ spill 的总行数 = 真异常(payload 解析失败) + ES 永久拒绝(4xx 毒丸)。
	// 这两类都**没进 ES 正文索引**，对账门据此从期望 doc 数扣除。
	DLQ int64
	// DLQPayload / DLQPermanent 是 DLQ 的两类细分（观测用）。
	DLQPayload   int64
	DLQPermanent int64
}

// Config 配置 backfill 运行。
type Config struct {
	// Tables 是 message 分表名集合（如 message, message1..4）。
	Tables []string
	// BatchSize 是每次 keyset 读 + bulk 写的批大小（默认 1000，见 docs/tuning.md）。
	BatchSize int
	// DocsPerSec 是限速（docs/s，默认 5000；<=0 不限速）。
	DocsPerSec float64
	// TransientBackoff 是 ES transient(429/5xx) 退避基（指数 + 满抖动，封顶 30s）。
	TransientBackoff time.Duration
}

// Runner 编排 backfill：按分表 keyset 分页扫描 → 抽取 → 复用 esindex.Writer 幂等 bulk 写 ES，
// 真异常 / ES 永久拒绝落本地 DLQ spill 并计数，按高水位续传，限速摄入。
type Runner struct {
	cfg    Config
	src    SourceReader
	writer esindex.Writer
	cp     *CheckpointStore
	dlq    *DLQSpill
	rl     *rateLimiter

	sleep func(context.Context, time.Duration) error
	logf  func(string, ...any)
}

// NewRunner 装配 Runner。writer 须为已就绪的 esindex.Writer（复用同一套 bulk / mapping /
// `_id=message_id`）。cp / dlq 由调用方注入（便于测试与运维配置 spill / checkpoint 路径）。
func NewRunner(cfg Config, src SourceReader, writer esindex.Writer, cp *CheckpointStore, dlq *DLQSpill) *Runner {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.TransientBackoff <= 0 {
		cfg.TransientBackoff = time.Second
	}
	return &Runner{
		cfg:    cfg,
		src:    src,
		writer: writer,
		cp:     cp,
		dlq:    dlq,
		rl:     newRateLimiter(cfg.DocsPerSec, cfg.BatchSize),
		sleep:  sleepCtx,
		logf:   log.Printf,
	}
}

// Run 执行整个 backfill：先幂等确保索引存在（mapping / 中文分词），再逐分表回灌。
// 返回累计 Stats。任一 fail-closed 条件（DLQ spill 写失败 / checkpoint 持久化失败 /
// ES 永久错误无法落地）触发即返回错误并 STOP（绝不静默吞行破坏对账）。
func (r *Runner) Run(ctx context.Context) (Stats, error) {
	var total Stats
	if err := r.writer.EnsureIndex(ctx); err != nil {
		return total, fmt.Errorf("backfill: ensure index: %w", err)
	}
	for _, table := range r.cfg.Tables {
		s, err := r.runTable(ctx, table)
		total.add(s)
		if err != nil {
			return total, fmt.Errorf("backfill: table %s: %w", table, err)
		}
	}
	r.logf("backfill: done read=%d indexed=%d (raw_excluded=%d) dlq=%d (payload=%d permanent=%d) checkpoint=%v",
		total.Read, total.Indexed, total.RawExcluded, total.DLQ, total.DLQPayload, total.DLQPermanent, r.cp.snapshot())
	return total, nil
}

// runTable keyset 分页回灌单个分表，从 checkpoint 高水位续传。
func (r *Runner) runTable(ctx context.Context, table string) (Stats, error) {
	var s Stats
	after := r.cp.Get(table)
	for {
		if err := ctx.Err(); err != nil {
			return s, err
		}
		rows, err := r.src.ReadBatch(ctx, table, after, r.cfg.BatchSize)
		if err != nil {
			return s, err
		}
		if len(rows) == 0 {
			return s, nil // 该分表读尽
		}
		// 限速：按本批行数等令牌（覆盖源读 + ES 写压力），再处理本批。
		if err := r.rl.Wait(ctx, len(rows)); err != nil {
			return s, err
		}
		if err := r.processBatch(ctx, table, rows, &s); err != nil {
			return s, err
		}
		// 🔴 durability ordering：先把本批 DLQ 记录 fsync 落盘，**再**推进 checkpoint。否则崩溃 /
		// 延迟写回失败可能让游标越过某 id 而其 DLQ 记录未落盘 → resume 漏计 DLQ、行不可恢复。
		if err := r.dlq.Sync(); err != nil {
			return s, err
		}
		// 全批已终态（进 ES 或落 DLQ）→ 推进高水位到批末 id 并持久化（fail → STOP）。
		last := rows[len(rows)-1].ID
		if err := r.cp.Advance(table, last); err != nil {
			return s, err
		}
		after = last
	}
}

// mainItem 把一条进 ES 正文流的契约消息与其源行配对，使 ES 永久拒绝时仍能把源 PK（table/id）
// + 原始 payload 写进 DLQ 记录（P2-1：便于排查 / 回灌，且空 message_id 时用 table:id 兜底去重，
// 不致 dedup 塌缩 / 计数偏低）。
type mainItem struct {
	row *srcMessageRow
	msg searchmsg.Message
}

// processBatch 处理一批（同分表、按 id 升序）：抽取三态分流 → 真异常落 DLQ spill →
// main 批幂等 bulk 写 ES（transient 原地重试未解决子集，permanent 4xx 落 DLQ spill）。
//
// fail-closed：任一 DLQ spill 写失败立即返回错误 STOP（绝不静默吞真异常）。返回后调用方才
// 推进 checkpoint，保证「推进的每个 id 都已终态处理」。
func (r *Runner) processBatch(ctx context.Context, table string, rows []*srcMessageRow, s *Stats) error {
	s.Read += int64(len(rows))
	main := make([]mainItem, 0, len(rows))
	for _, row := range rows {
		msg, outcome := extractMessage(row)
		switch outcome {
		case outcomeDLQ:
			// 真异常（payload 本应可解析却失败）：落本地 DLQ spill 并计数（fail-closed）。
			if err := r.dlq.Write(dlqRecord{
				Table: table, ID: row.ID, MessageID: row.MessageID,
				Payload: row.Payload, CreatedAt: row.CreatedUnix,
			}); err != nil {
				return err
			}
			s.DLQ++
			s.DLQPayload++
		default: // outcomeOK / outcomeRawExcluded 都进 ES 正文流
			if outcome == outcomeRawExcluded {
				s.RawExcluded++
			}
			main = append(main, mainItem{row: row, msg: msg})
		}
	}
	return r.writeMain(ctx, table, main, s)
}

// writeMain 把 main 批幂等 bulk 写 ES。语义对齐实时 consumer 的 C4：
//   - 批级失败（网络 / 非 2xx 整体）→ 退避后整批重试（不推进，靠 ES `_id` 幂等去重）。
//   - per-item permanent(4xx，非 429) → ES 永久拒绝的毒丸，落本地 DLQ spill 计数（不进 ES）。
//   - per-item transient(429/5xx) → 仅对未解决子集原地退避重试，直到全部 OK 或 permanent 终态。
//
// 阻塞直到本批所有 main 文档均终态（OK 或已 spill）；ctx 取消（SIGTERM）即返回。
func (r *Runner) writeMain(ctx context.Context, table string, main []mainItem, s *Stats) error {
	remaining := main
	for attempt := 0; len(remaining) > 0; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 0 {
			if err := r.sleep(ctx, expJitterBackoff(r.cfg.TransientBackoff, attempt)); err != nil {
				return err
			}
		}
		msgs := make([]searchmsg.Message, len(remaining))
		for i := range remaining {
			msgs[i] = remaining[i].msg
		}
		results, err := r.writer.Bulk(ctx, msgs)
		if err != nil {
			// 批级 transient：保持 remaining 不变，下轮整批重试。
			r.logf("backfill: bulk batch-level transient (table=%s n=%d attempt=%d): %v",
				table, len(remaining), attempt, err)
			continue
		}
		next := remaining[:0:0] // 新底层数组，避免别名污染
		for i, res := range results {
			item := remaining[i]
			switch {
			case res.OK:
				s.Indexed++
			case res.Permanent():
				// ES 永久拒绝（4xx 毒丸，如 mapping 冲突）：未进 ES，落 DLQ spill 计数。
				// P2-1：保留源 PK（table/id）+ 原始 payload；空 message_id 用 table:id 兜底去重。
				rec := dlqRecord{
					Reason: "permanent_es_reject",
					Table:  table, ID: item.row.ID, MessageID: item.row.MessageID,
					Payload: item.row.Payload, CreatedAt: item.row.CreatedUnix,
				}
				if werr := r.dlq.Write(rec); werr != nil {
					return werr
				}
				s.DLQ++
				s.DLQPermanent++
				r.logf("backfill: permanent ES reject -> DLQ spill (table=%s id=%d msg=%q status=%d): %v",
					table, item.row.ID, item.row.MessageID, res.Status, res.Err)
			default: // transient(429/5xx)：留待下轮原地重试。
				next = append(next, item)
			}
		}
		remaining = next
	}
	return nil
}

// add 把另一组 Stats 累加进 s。
func (s *Stats) add(o Stats) {
	s.Read += o.Read
	s.Indexed += o.Indexed
	s.RawExcluded += o.RawExcluded
	s.DLQ += o.DLQ
	s.DLQPayload += o.DLQPayload
	s.DLQPermanent += o.DLQPermanent
}
