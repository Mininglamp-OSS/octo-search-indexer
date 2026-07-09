// Package fileextract consumer 主循环：pull batch → filter type=8 → 抽取 → OS partial update
// → DLQ 路由 → in-place bounded retry state machine → 按分区连续成功前缀 commit。
//
// v1.13 Blocker #2 fix：从"单条独立 commit + err 上抛"改为"batch 内 in-place bounded retry
// state machine"（照抄 internal/consumer pattern）。老 pattern 触发 silent skip / data loss：
// kafka-go FetchMessage 在 fetch 时执行 r.offset = m.message.Offset + 1，err 上抛不 commit
// 后 reader 已经 advance，后续 message commit 越过前面 → 永久丢。
//
// 新语义（对齐 internal/consumer::processBatch）：
//   - 一批消息进 processBatch → dispositions state machine（transient/ok/dlqResolved）
//   - 每轮对仍 transient 条目跑 attemptOne → 更新 disposition
//   - 达 MaxRetriesPerMessage 上限的 transient → 强制 DLQ ReasonRetryExhausted + 标 dlqResolved
//     （避免 partition 永久阻塞）
//   - 每轮结束按 partitionCommitPoints 推进 commit（每分区连续可越过前缀末）
//   - 全部条目终态 → 本批完成，拉下一批
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
	dlq       *dlqHandler // v1.13 P2-1：DLQ 投递有界重试 + spill 逃逸（yujiawei review fix）
	metrics   *counters
	extractor extractorService
	cfg       ServiceConfig
	// sleep 由 backoff.go 提供；抽成字段便于测试注入（跳过真实 sleep）。
	sleep func(context.Context, time.Duration) error
}

// Metrics 暴露 Processor 的可观测计数集，供 Service/obs server 拿 registry 暴露 /metrics。
func (p *Processor) Metrics() *counters { return p.metrics }

// extractorService 抽象「抽取 + OS partial update」核心行为，供 Processor 测试注入 mock。
// 生产实现是 *Extractor（extractor.go），本 interface 只暴露 processOne/attemptOne 需要的
// 单个方法，便于最小化 test surface。
type extractorService interface {
	ExtractAndWrite(ctx context.Context, messageID string, fp *filePayload) (dlqReason string, cause error, err error)
}

// defaultMaxRetriesPerMessage 是单条消息 in-place retry 上限的兜底默认（cfg 未设时用）。
// 10 次 + backoff 1s→60s → 单条最坏阻塞 ~10 min（业务 SLO 允许）。
const defaultMaxRetriesPerMessage = 10

// NewProcessor 组装 Processor（生产用；extractor 必须非 nil）。
// ext 类型为 extractorService interface：生产传 *Extractor，测试注入 mock。
func NewProcessor(src messageSource, dlq dlqSink, ext extractorService, cfg ServiceConfig) *Processor {
	if cfg.MaxRetriesPerMessage <= 0 {
		cfg.MaxRetriesPerMessage = defaultMaxRetriesPerMessage
	}
	if cfg.TransientBackoffBase <= 0 {
		cfg.TransientBackoffBase = time.Second
	}
	if cfg.TransientBackoffMax <= 0 {
		cfg.TransientBackoffMax = defaultBackoffMax
	}
	// v1.13 P2-1：dlqHandler 有界重试 + spill 逃逸（yujiawei review fix）。缺 SpillDir 时
	// DLQ 写耗尽 → errDLQHardStop → outcomeFatal → Run 停 worker + K8s 重启（保 offset 不推进）。
	// 配 SpillDir 后转成落盘 + 告警 + offset 越过（绝不永久卡 partition）。
	handler := newDLQHandler(dlq, newLogAlerter(log.Printf), dlqHandlerConfig{
		MaxRetries:   cfg.DLQMaxRetries,
		RetryBackoff: cfg.DLQRetryBackoff,
		SpillDir:     cfg.DLQSpillDir,
	})
	return &Processor{
		source:    src,
		dlqSink:   dlq,
		dlq:       handler,
		metrics:   newCounters(),
		extractor: ext,
		cfg:       cfg,
		sleep:     sleepCtx,
	}
}

// Run 持续消费直到 ctx 取消。每轮拉一批 → processBatch → 短暂间隔（避免空转打满 CPU）。
func (p *Processor) Run(ctx context.Context) error {
	// v2 §7 #1 时序竞态缓解：启动时 sleep 5s（可配），给 es-indexer 先跑机会。
	// 稳态竞态由 in-place bounded retry 兜底（v1.13 Blocker #2 fix）。
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
		if len(batch) == 0 {
			continue
		}
		if err := p.processBatch(ctx, batch); err != nil {
			// DLQ 硬停 fatal 或 ctx 取消 → Run 退出（K8s 重启保 offset 未推进）
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("file-extractor: processBatch fatal, stopping Run: %v", err)
			return err
		}
	}
}

// fetchBatch 拉一批消息（BatchSize 上限），拿到就返回；单条 fetch 失败上抛。
// 首条阻塞等，后续 10ms 短超时凑批。
func (p *Processor) fetchBatch(ctx context.Context) ([]fetchedMessage, error) {
	size := p.cfg.BatchSize
	if size <= 0 {
		size = 50
	}
	batch := make([]fetchedMessage, 0, size)
	for len(batch) < size {
		fetchCtx := ctx
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

// attemptOutcome 是 attemptOne 单次尝试的结果（v1.13 Blocker #2）。
type attemptOutcome int

const (
	// outcomeOK 抽取 + OS 写入成功。
	outcomeOK attemptOutcome = iota
	// outcomeDLQ 已投 DLQ（永久失败：parse_error / oversize / blacklist_ext /
	// download_failed / extract_* / os_permanent）。offset 可越过。
	outcomeDLQ
	// outcomeTransient 需重试（errDocNotYet / errOSTransient / 429/5xx）。
	// caller 应下轮再调 attemptOne（或达上限强制 DLQ）。
	outcomeTransient
	// outcomeFatal 硬停（DLQ 写失败）；上抛让 Run 停 worker，保 offset 未推进。
	outcomeFatal
)

// processBatch 批内 in-place bounded retry state machine（v1.13 Blocker #2 fix 核心）。
//
// 语义（对齐 internal/consumer::processBatch）：
//  1. dispositions 初始全 transient；attempts 每条独立计数
//  2. 每轮对仍 transient 条目跑 attemptOne：
//     - attempts[i] > 0 时先退避（指数 + jitter，ctx 感知）
//     - attempts[i] >= MaxRetriesPerMessage → 强制 DLQ ReasonRetryExhausted，标 dlqResolved
//     - 否则调 attemptOne，按 outcome 更新 disposition + attempts[i]++
//  3. 有条目终态变更 → 按 partitionCommitPoints 推进 commit（多分区各自前缀末）
//  4. 全部终态 → return nil
//  5. attemptOne 返 outcomeFatal → 立即 return err（Run 停 worker）
func (p *Processor) processBatch(ctx context.Context, batch []fetchedMessage) error {
	n := len(batch)
	dispositions := make([]itemDisposition, n)
	attempts := make([]int, n)
	// 初始全部 transient（下面 attemptOne 后按 outcome 收敛）
	for i := range dispositions {
		dispositions[i] = dispTransient
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		changed := false
		for i, m := range batch {
			if dispositions[i] != dispTransient {
				continue
			}

			// 达重试上限 → 强制 DLQ ReasonRetryExhausted（避免 partition 永久阻塞）
			if attempts[i] >= p.cfg.MaxRetriesPerMessage {
				if werr := p.writeDLQ(ctx, m, ReasonRetryExhausted, "", nil,
					fmt.Errorf("in-place retry exhausted after %d attempts", attempts[i])); werr != nil {
					return werr // DLQ 硬停 fatal
				}
				p.metrics.IncRetryExhausted()
				p.metrics.IncDLQ(ReasonRetryExhausted)
				dispositions[i] = dispDLQResolved
				changed = true
				continue
			}

			// 退避（attempts[i]>0 才 sleep；1-based attempt = attempts[i]+1）
			if attempts[i] > 0 {
				if serr := p.sleep(ctx, expJitterBackoff(p.cfg.TransientBackoffBase, p.cfg.TransientBackoffMax, attempts[i])); serr != nil {
					return serr
				}
			}

			outcome, err := p.attemptOne(ctx, m)
			attempts[i]++
			switch outcome {
			case outcomeOK:
				dispositions[i] = dispOK
				changed = true
			case outcomeDLQ:
				dispositions[i] = dispDLQResolved
				changed = true
			case outcomeTransient:
				// 保持 transient，下轮继续（或触发上限）
			case outcomeFatal:
				return err
			}
		}

		// 有条目终态 → 按分区推进连续前缀 commit
		if changed {
			for _, point := range partitionCommitPoints(batch, dispositions) {
				if err := p.source.Commit(ctx, point); err != nil {
					return fmt.Errorf("commit offset (partition=%d offset=%d): %w",
						point.Partition, point.Offset, err)
				}
			}
		}

		// 全终态 → 本批完成
		if !hasTransient(dispositions) {
			return nil
		}
	}
}

// attemptOne 单次抽取尝试（v1.13 Blocker #2 fix）。
// 返回 (outcome, err)。outcome 见 attemptOutcome 常量；err 仅在 outcomeFatal 时非 nil。
//
// P2-2：errOSPermanent 走 DLQ ReasonOSPermanent（老代码只上抛 err → 无 DLQ 路径 →
// 与 Blocker #2 老 skip 语义叠加 → 4xx 永久错误也被 silent skip）。
func (p *Processor) attemptOne(ctx context.Context, m fetchedMessage) (attemptOutcome, error) {
	var msg searchmsg.Message
	if err := json.Unmarshal(m.Value, &msg); err != nil {
		p.metrics.IncDLQ(ReasonParseError)
		if werr := p.writeDLQ(ctx, m, ReasonParseError, "", nil, err); werr != nil {
			return outcomeFatal, werr
		}
		return outcomeDLQ, nil
	}
	fp, isFile := extractContentTypeFile(msg.RawPayload)
	if !isFile {
		p.metrics.IncSkippedNonFile()
		return outcomeOK, nil
	}
	p.metrics.IncProcessed()
	dlqReason, cause, err := p.extractor.ExtractAndWrite(ctx, msg.MessageID, fp)
	if err != nil {
		// P2-2：OS permanent 4xx → 走 DLQ ReasonOSPermanent（不走 transient retry）
		if errors.Is(err, errOSPermanent) {
			p.metrics.IncDLQ(ReasonOSPermanent)
			p.metrics.IncOSPermanent()
			if werr := p.writeDLQ(ctx, m, ReasonOSPermanent, msg.MessageID, fp, err); werr != nil {
				return outcomeFatal, werr
			}
			return outcomeDLQ, nil
		}
		if errors.Is(err, errDocNotYet) {
			p.metrics.IncDocNotYet()
		}
		// 其他 OS transient / errDocNotYet → 交给 processBatch 状态机重试
		return outcomeTransient, nil
	}
	if dlqReason != "" {
		p.metrics.IncDLQ(dlqReason)
		if werr := p.writeDLQ(ctx, m, dlqReason, msg.MessageID, fp, cause); werr != nil {
			return outcomeFatal, werr
		}
		return outcomeDLQ, nil
	}
	return outcomeOK, nil
}

// processOne 单次尝试的兼容 wrapper（供老 test 直接调用）。
//   - outcomeOK / outcomeDLQ → nil
//   - outcomeTransient → 上抛 errTransientNeedsRetry（暴露"需要 caller 重试"语义）
//   - outcomeFatal → 上抛具体 err（DLQ 写失败等）
//
// 生产路径不用 processOne；processBatch 走 attemptOne + state machine。
func (p *Processor) processOne(ctx context.Context, m fetchedMessage) error {
	outcome, err := p.attemptOne(ctx, m)
	switch outcome {
	case outcomeOK, outcomeDLQ:
		return nil
	case outcomeTransient:
		return errTransientNeedsRetry
	default: // outcomeFatal
		return err
	}
}

// errTransientNeedsRetry 是 processOne 老签名（单次尝试）向 caller 表达"需要 in-place retry"
// 的 sentinel。生产路径不会返 —— processBatch 状态机内部消化 transient 语义。仅供 test 断言。
var errTransientNeedsRetry = errors.New("fileextract: transient error needs in-place retry")

// writeDLQ 构造 dlqRecord → 通过 dlqHandler 有界重试 + spill 逃逸投递到 DLQ topic。
// 返回 nil 表示已终态处理（投递成功 or spill 落盘）—— caller 可放心越过 offset；返回 error 表示
// 硬停（errDLQHardStop：未配 SpillDir 且 DLQ 写耗尽）—— caller 应走 outcomeFatal 让 Run 停 worker。
// key 用原消息 key（=messageId）保证分区一致性 + spill 文件命名有意义。
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
	return p.dlq.Send(ctx, rec)
}
