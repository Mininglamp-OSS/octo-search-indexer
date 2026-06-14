package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// fetchedMessage 是从 Kafka 拉取的一条原始消息（解耦 kafka-go 类型，便于单测）。
type fetchedMessage struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
}

// messageSource 抽象 Kafka 拉取 + 手动提交（由 kafka-go Reader 适配实现）。
//
// 🔴 C4：实现必须用 FetchMessage（手动）+ CommitInterval=0（禁 ReadMessage 自动提交），
// 使 offset 提交完全由 Commit 调用按「连续成功前缀」驱动。
type messageSource interface {
	// Fetch 阻塞拉取下一条消息（ctx 取消则返回 err）。
	Fetch(ctx context.Context) (fetchedMessage, error)
	// Commit 提交到（含）给定消息——kafka 单调高水位语义。仅传连续成功前缀的最后一条。
	Commit(ctx context.Context, msg fetchedMessage) error
	Close() error
}

// Processor 串起一轮批处理：拉批 → schema 校验 → bulk → DLQ 路由 → 连续前缀 commit。
type Processor struct {
	src       messageSource
	writer    esindex.Writer
	dlq       *dlqHandler
	alert     alerter
	batchSize int
	// transientBackoff 是整批含 transient 时、原地退避重试的间隔基（指数+抖动，offset 不前进）。
	transientBackoff time.Duration
	sleep            func(context.Context, time.Duration) error
}

// Config 配置 Processor。
type Config struct {
	BatchSize        int
	TransientBackoff time.Duration
}

func defaultConfig() Config {
	return Config{BatchSize: 500, TransientBackoff: time.Second}
}

// NewProcessor 组装 Processor。
func NewProcessor(src messageSource, writer esindex.Writer, dlq *dlqHandler, alert alerter, cfg Config) *Processor {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultConfig().BatchSize
	}
	if cfg.TransientBackoff <= 0 {
		cfg.TransientBackoff = defaultConfig().TransientBackoff
	}
	return &Processor{
		src:              src,
		writer:           writer,
		dlq:              dlq,
		alert:            alert,
		batchSize:        cfg.BatchSize,
		transientBackoff: cfg.TransientBackoff,
		sleep:            sleepCtx,
	}
}

// Run 持续消费直到 ctx 取消。每轮拉一批 → processBatch。
func (p *Processor) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := p.fetchBatch(ctx)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			continue
		}
		if err := p.processBatch(ctx, batch); err != nil {
			return err
		}
	}
}

// fetchBatch 拉取至多 batchSize 条；首条阻塞等待，其后用短超时尽量凑批但不久等。
func (p *Processor) fetchBatch(ctx context.Context) ([]fetchedMessage, error) {
	first, err := p.src.Fetch(ctx)
	if err != nil {
		return nil, err
	}
	batch := []fetchedMessage{first}
	for len(batch) < p.batchSize {
		fctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		m, ferr := p.src.Fetch(fctx)
		cancel()
		if ferr != nil {
			break // 超时/无更多消息：以当前批处理
		}
		batch = append(batch, m)
	}
	return batch, nil
}

// processBatch 处理一批（C4 核心）：
//  1. schema_version 校验：未知版本标记为毒丸（不进 bulk，直接 DLQ）。
//  2. 对**尚未终态**的条目走 esindex bulk；毒丸（schema 非法 + bulk 4xx）按原序送 DLQ。
//  3. 计算**每分区**连续可越过前缀并 commit（多分区各自推进，互不越权）。
//  4. transient 条目**原地重试同一批**（退避后重跑 bulk，绝不拉新 offset）——直到本批无 transient。
//     这保证 kafka 单调高水位 commit 永不越过未确认的 transient（杜绝丢消息）。
//  5. DLQ 终态逃逸为硬停（未配 spill）→ 返回 fatal error，调用方停该 worker + page，
//     绝不把毒丸标 transient 后继续拉新 offset（否则后续成功 commit 会越过它）。
func (p *Processor) processBatch(ctx context.Context, batch []fetchedMessage) error {
	n := len(batch)
	schemaInvalid := make([]bool, n)
	parsed := make([]searchmsg.Message, n)
	for i := range batch {
		msg, ok := decodeAndValidate(batch[i].Value)
		if !ok {
			schemaInvalid[i] = true
			continue
		}
		parsed[i] = msg
	}

	dispositions := make([]itemDisposition, n)
	for i := range dispositions {
		dispositions[i] = dispTransient // 初始全部「未终态」，下面逐轮收敛
	}

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 0 {
			// 🔴 C4 原地重试：退避后重跑**同一批**未终态条目，不拉新 offset。
			// 指数退避 + 满抖动 + ctx 感知（P2-1/P2-2）：缓解多副本 thundering herd，SIGTERM 即停。
			if serr := p.sleep(ctx, expJitterBackoff(p.transientBackoff, attempt)); serr != nil {
				return serr
			}
		}

		// 仅对仍 transient 的条目重跑（已 OK / 已进 DLQ 的不再处理）。
		changed, err := p.resolvePass(ctx, batch, parsed, schemaInvalid, dispositions)
		if err != nil {
			return err // DLQ 硬停 fatal：停 worker（不静默越过毒丸）
		}

		// 仅在有条目新终态时按分区推进已确认前缀（避免空转重复 commit 同一 offset）。
		if changed {
			if err := p.commitPrefixes(ctx, batch, dispositions); err != nil {
				return err
			}
		}

		if !hasTransient(dispositions) {
			return nil // 全部终态（OK 或已进 DLQ）→ 本批完成
		}
	}
}

// resolvePass 对仍为 dispTransient 的条目跑一轮 bulk + DLQ 路由，就地更新 dispositions。
// 返回 changed=是否有条目从 transient 转为终态（OK/DLQResolved）；error 仅当 DLQ 硬停逃逸（fatal）。
func (p *Processor) resolvePass(ctx context.Context, batch []fetchedMessage, parsed []searchmsg.Message, schemaInvalid []bool, dispositions []itemDisposition) (bool, error) {
	changed := false
	// schema 非法的毒丸：直接送 DLQ（不进 bulk）。
	for i := range batch {
		if dispositions[i] != dispTransient || !schemaInvalid[i] {
			continue
		}
		if err := p.routePoison(ctx, batch[i], esindex.BulkItemResult{Status: 0}, true); err != nil {
			return changed, err
		}
		dispositions[i] = dispDLQResolved
		changed = true
	}

	// 收集仍 transient 且 schema 合法的条目走 bulk。
	var toBulk []searchmsg.Message
	bulkIdx := make([]int, 0, len(batch))
	for i := range batch {
		if dispositions[i] == dispTransient && !schemaInvalid[i] {
			toBulk = append(toBulk, parsed[i])
			bulkIdx = append(bulkIdx, i)
		}
	}
	if len(toBulk) == 0 {
		return changed, nil
	}

	bulkRes, bulkErr := p.writer.Bulk(ctx, toBulk)
	if bulkErr != nil {
		p.alert.Alert("bulk_batch_error", bulkErr.Error())
	}
	for j, idx := range bulkIdx {
		var res esindex.BulkItemResult
		if j < len(bulkRes) {
			res = bulkRes[j]
		} else {
			res = esindex.BulkItemResult{MessageID: parsed[idx].MessageID, OK: false, Status: 0}
		}
		ok, permanent := classifyBulk(false, res)
		switch {
		case ok:
			dispositions[idx] = dispOK
			changed = true
		case permanent:
			if err := p.routePoison(ctx, batch[idx], res, false); err != nil {
				return changed, err
			}
			dispositions[idx] = dispDLQResolved
			changed = true
		default:
			dispositions[idx] = dispTransient // 仍 transient，下轮原地重试
		}
	}
	return changed, nil
}

// routePoison 把一条毒丸送 DLQ。DLQ 终态成功 → nil；硬停逃逸 → 返回 fatal error。
func (p *Processor) routePoison(ctx context.Context, m fetchedMessage, res esindex.BulkItemResult, schemaInvalid bool) error {
	rec := buildDLQRecord(schemaInvalid, m, res)
	if err := p.dlq.Send(ctx, rec); err != nil {
		p.alert.Alert("dlq_hard_stop", fmt.Sprintf("offset=%d: %v", m.Offset, err))
		return fmt.Errorf("consumer: DLQ terminal escape hard-stop at offset %d: %w", m.Offset, err)
	}
	return nil
}

// commitPrefixes 按分区提交各自连续前缀末（多分区正确，kafka 高水位语义）。
func (p *Processor) commitPrefixes(ctx context.Context, batch []fetchedMessage, dispositions []itemDisposition) error {
	for _, point := range partitionCommitPoints(batch, dispositions) {
		if err := p.src.Commit(ctx, point); err != nil {
			return fmt.Errorf("consumer: commit offset failed (partition=%d offset=%d): %w",
				point.Partition, point.Offset, err)
		}
	}
	return nil
}

// decodeAndValidate 解析消息字节并做 schema_version 校验（C4：未知版本 → 毒丸进 DLQ，不静默吃）。
// 返回 (msg, true) 表示通过；(zero, false) 表示 schema 非法。
func decodeAndValidate(value []byte) (searchmsg.Message, bool) {
	var msg searchmsg.Message
	if err := json.Unmarshal(value, &msg); err != nil {
		return searchmsg.Message{}, false
	}
	if msg.SchemaVersion != searchmsg.SchemaVersion {
		return searchmsg.Message{}, false
	}
	return msg, true
}

// buildDLQRecord 构造一条 DLQ 记录（保留原始字节供回灌）。
func buildDLQRecord(schemaInvalid bool, m fetchedMessage, res esindex.BulkItemResult) dlqRecord {
	reason := "permanent_4xx"
	detail := ""
	status := res.Status
	if schemaInvalid {
		reason = "unknown_schema_version"
		status = 0
	} else if res.Err != nil {
		detail = res.Err.Error()
	}
	return dlqRecord{
		Reason:    reason,
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Status:    status,
		Detail:    detail,
	}
}
