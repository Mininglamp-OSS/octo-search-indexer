// Package fileextract consumer 主循环：pull batch → filter type=8 → 抽取 → OS partial update
// → DLQ 路由 → 每条独立 commit（同 consumer/consumer.go 模式，但简化为单条粒度）。
package fileextract

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// Processor 串起一轮批处理。字段命名对齐 consumer/consumer.go Processor 便于阅读。
type Processor struct {
	source    messageSource
	dlqSink   dlqSink
	metrics   *counters
	extractor *Extractor
	cfg       ServiceConfig
}

// NewProcessor 组装 Processor（生产用；extractor 必须非 nil）。
func NewProcessor(src messageSource, dlq dlqSink, ext *Extractor, cfg ServiceConfig) *Processor {
	return &Processor{
		source:    src,
		dlqSink:   dlq,
		metrics:   &counters{},
		extractor: ext,
		cfg:       cfg,
	}
}

// Run 持续消费直到 ctx 取消。每轮拉一批 → processBatch → 短暂间隔（避免空转打满 CPU）。
func (p *Processor) Run(ctx context.Context) error {
	// v2 §7 #1 时序竞态缓解：启动时 sleep 5s（可配），给 es-indexer 先跑机会。
	// 稳态竞态由 errDocNotYet 触发本批重试 + Kafka rebalance 兜底。
	if p.cfg.ExtractStartupDelay > 0 {
		log.Printf("file-extractor: startup delay %v (mitigate es-indexer race)", p.cfg.ExtractStartupDelay)
		select {
		case <-time.After(p.cfg.ExtractStartupDelay):
		case <-ctx.Done():
			return nil
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		batch, err := p.fetchBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("file-extractor: fetch error: %v", err)
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return nil
			}
			continue
		}
		if err := p.processBatch(ctx, batch); err != nil {
			log.Printf("file-extractor: processBatch error: %v", err)
		}
	}
}

// fetchBatch 拉一批消息（BatchSize 上限），拿到就返回；单条 fetch 失败上抛。
// 首条阻塞等，后续 10ms 短超时凑批；每轮的短超时 ctx 立即 cancel（不 defer 到 fetchBatch 返回，
// 避免 defer 在 for 循环内累积 timer 资源，对齐 internal/consumer/consumer.go:103-105 pattern）。
func (p *Processor) fetchBatch(ctx context.Context) ([]fetchedMessage, error) {
	size := p.cfg.BatchSize
	if size <= 0 {
		size = 50
	}
	batch := make([]fetchedMessage, 0, size)
	for len(batch) < size {
		var fetchCtx context.Context = ctx
		var cancel context.CancelFunc
		if len(batch) > 0 {
			fetchCtx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
		}
		m, err := p.source.Fetch(fetchCtx)
		if cancel != nil {
			cancel()
		}
		if err != nil {
			if len(batch) > 0 && fetchCtx.Err() != nil {
				return batch, nil // 超时凑批就走
			}
			return batch, err
		}
		batch = append(batch, m)
	}
	return batch, nil
}

// processBatch 处理一批：逐条判 type=8 → 抽取 → commit。
//
// commit 语义（C4，同 consumer/consumer.go）：一条处理完立即 commit 该条 offset（简化版
// 「连续成功前缀」= 每条独立成功），因为 file-extractor 每条独立、无 batch bulk 语义。
func (p *Processor) processBatch(ctx context.Context, batch []fetchedMessage) error {
	for _, m := range batch {
		if err := p.processOne(ctx, m); err != nil {
			// processOne 内部已把毒丸投 DLQ；这里只在 DLQ 自身写失败时返 error（暂停批推进）
			return err
		}
		if err := p.source.Commit(ctx, m); err != nil {
			return fmt.Errorf("commit offset: %w", err)
		}
	}
	return nil
}

// processOne 处理一条消息。
//   - Kafka 反序列化失败 → DLQ reason=parse_error
//   - 非 type=8 → skip (increment metrics)
//   - type=8 → 真实抽取（download → Tika → OS partial update）
//     · 抽取 permanent 失败 → 投 DLQ 对应 reason
//     · OS errDocNotYet → 上抛让批级重试（Kafka rebalance 兜底）
func (p *Processor) processOne(ctx context.Context, m fetchedMessage) error {
	var msg searchmsg.Message
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		p.metrics.IncDLQ()
		return p.writeDLQ(ctx, m, ReasonParseError, "", nil, err)
	}
	fp, isFile := extractContentTypeFile(msg.RawPayload)
	if !isFile {
		p.metrics.IncSkippedNonFile()
		return nil
	}
	p.metrics.IncProcessed()
	dlqReason, cause, err := p.extractor.ExtractAndWrite(ctx, msg.MessageID, fp)
	if err != nil {
		if errors.Is(err, errDocNotYet) {
			p.metrics.IncDocNotYet()
			// 主 doc 未落 → 上抛不 commit 本条 offset，让 Kafka 下一轮重取（同 partition 自然重放）
			return err
		}
		// 其他 OS transient 错也上抛，触发重试
		return err
	}
	if dlqReason != "" {
		p.metrics.IncDLQ()
		return p.writeDLQ(ctx, m, dlqReason, msg.MessageID, fp, cause)
	}
	return nil
}

// writeDLQ 序列化 dlqRecord → 投 DLQ topic。key 用原消息 key（=messageId）保证分区一致性。
func (p *Processor) writeDLQ(ctx context.Context, m fetchedMessage, reason, messageID string, fp *filePayload, cause error) error {
	value, truncated := truncateValueIfNeeded(m.Value)
	rec := dlqRecord{
		Reason:           reason,
		Topic:            m.Topic,
		Partition:        m.Partition,
		Offset:           m.Offset,
		Key:              m.Key,
		Value:            value,
		MessageID:        messageID,
		PayloadTruncated: truncated,
	}
	if fp != nil {
		rec.FileURL = fp.URL
		rec.FileExt = fp.Extension
		rec.FileSize = fp.Size
	}
	if cause != nil {
		rec.Detail = cause.Error()
	}
	body, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal dlq record: %w", err)
	}
	return p.dlqSink.WriteDLQ(ctx, m.Key, body)
}
