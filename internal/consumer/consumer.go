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
//     1b. 🔴 visibility 预检 pass（方案 B §3.4，**visibility 解析的唯一权威落点**）：对分支 A
//     （len(RawPayload)>0，非加密新形态）逐条调 searchmsg.ExtractVisibility fail-closed；失败者
//     剔出 bulk + 落 DLQ(reason=visibility_untrusted, status=0)，绝不写空 visibles；成功者把
//     解析值回填进 parsed[i].SpaceID/Visibles（下游 DocFromMessage 直接消费、不二次解析）。
//  2. 对**尚未终态**的条目走 esindex bulk；毒丸（schema 非法 + visibility 失败 + bulk 4xx）按原序送 DLQ。
//  3. 计算**每分区**连续可越过前缀并 commit（多分区各自推进，互不越权）。
//  4. transient 条目**原地重试同一批**（退避后重跑 bulk，绝不拉新 offset）——直到本批无 transient。
//     这保证 kafka 单调高水位 commit 永不越过未确认的 transient（杜绝丢消息）。
//  5. DLQ 终态逃逸为硬停（未配 spill）→ 返回 fatal error，调用方停该 worker + page，
//     绝不把毒丸标 transient 后继续拉新 offset（否则后续成功 commit 会越过它）。
func (p *Processor) processBatch(ctx context.Context, batch []fetchedMessage) error {
	n := len(batch)
	schemaInvalid := make([]bool, n)
	visibilityInvalid := make([]bool, n)
	parsed := make([]searchmsg.Message, n)
	for i := range batch {
		msg, ok := decodeAndValidate(batch[i].Value)
		if !ok {
			schemaInvalid[i] = true
			continue
		}
		parsed[i] = msg
	}

	// 🔴 visibility 预检 pass（§3.4，唯一权威解析落点）：只对分支 A（schema 合法且
	// len(RawPayload)>0 = 非加密新形态；加密 RawPayload==nil 天然不入此集，无须看 RawExcluded）
	// 跑 ExtractVisibility。失败 → 标 visibilityInvalid 剔出 bulk + 落 DLQ；成功 → 回填解析值。
	for i := range batch {
		if schemaInvalid[i] || len(parsed[i].RawPayload) == 0 {
			continue
		}
		spaceID, visibles, verr := searchmsg.ExtractVisibility(parsed[i].RawPayload)
		if verr != nil {
			// fail-closed：可见性 ACL 不可信，绝不写空 visibles（reader fail-OPEN，#1124）。落 DLQ。
			visibilityInvalid[i] = true
			continue
		}
		parsed[i].SpaceID = spaceID
		parsed[i].Visibles = visibles
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
		changed, err := p.resolvePass(ctx, batch, parsed, schemaInvalid, visibilityInvalid, dispositions)
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
func (p *Processor) resolvePass(ctx context.Context, batch []fetchedMessage, parsed []searchmsg.Message, schemaInvalid []bool, visibilityInvalid []bool, dispositions []itemDisposition) (bool, error) {
	changed := false
	// schema 非法的毒丸：直接送 DLQ（不进 bulk）。
	for i := range batch {
		if dispositions[i] != dispTransient || !schemaInvalid[i] {
			continue
		}
		if err := p.routePoison(ctx, batch[i], esindex.BulkItemResult{Status: 0}, reasonSchemaInvalid); err != nil {
			return changed, err
		}
		dispositions[i] = dispDLQResolved
		changed = true
	}

	// 🔴 visibility 失败的毒丸（§3.4）：bulk 之前剔出 + 落 DLQ(visibility_untrusted, status=0)，
	// 绝不进 bulk、绝不写空 visibles。幂等卫与 schemaInvalid 毒丸块同构（重试每轮重跑，已终态跳过）。
	for i := range batch {
		if dispositions[i] != dispTransient || !visibilityInvalid[i] {
			continue
		}
		if err := p.routePoison(ctx, batch[i], esindex.BulkItemResult{Status: 0}, reasonVisibilityUntrusted); err != nil {
			return changed, err
		}
		dispositions[i] = dispDLQResolved
		changed = true
	}

	// 收集仍 transient 且非毒丸（schema 合法 + visibility 通过）的条目走 bulk。
	var toBulk []searchmsg.Message
	bulkIdx := make([]int, 0, len(batch))
	for i := range batch {
		if dispositions[i] == dispTransient && !schemaInvalid[i] && !visibilityInvalid[i] {
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
			if err := p.routePoison(ctx, batch[idx], res, reasonPermanent4xx); err != nil {
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
func (p *Processor) routePoison(ctx context.Context, m fetchedMessage, res esindex.BulkItemResult, reason dlqReason) error {
	rec := buildDLQRecord(reason, m, res)
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

// dlqReason 是 consumer 侧毒丸落 DLQ 的机器可读原因（§3.4 把原 schemaInvalid bool 泛化为枚举，
// 表达第三类「bulk 前 visibility 预检失败」）。
type dlqReason int

const (
	reasonSchemaInvalid dlqReason = iota
	reasonPermanent4xx
	// reasonVisibilityUntrusted：bulk 前 visibility 预检失败（payload 非对象 / visibles
	// present-but-empty / 不可信）。是**数据完整性/解析**失败，**不是** HTTP 4xx → status=0
	// （与 schema-invalid 同档）。命名用 visibility_untrusted（无 producer_ 前缀，与 producer 腿
	// 的 producer_visibility_untrusted 有意区分来源腿，非笔误）。
	reasonVisibilityUntrusted
)

// reasonString 返回 dlqReason 的线格字符串（写进 dlqRecord.Reason）。
func (r dlqReason) reasonString() string {
	switch r {
	case reasonSchemaInvalid:
		return "unknown_schema_version"
	case reasonVisibilityUntrusted:
		return "visibility_untrusted"
	default:
		return "permanent_4xx"
	}
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

// buildDLQRecord 构造一条 DLQ 记录。
//
// 🔴 信封超限对称截断（§4.4.3 B-5）：DLQ 实际写出的是 json.Marshal(dlqRecord)，Go 把
// Value []byte 编成 base64(~1.33x 膨胀) + 信封字段开销。一条带大 RawPayload 的消息落 consumer DLQ
// 时，原样保留整条字节会触发 DLQ topic 超限连锁卡死（与 producer 腿同构）。故当原始 Value 体积
// 超阈值（按预留 base64 余量的原始字节判，见 maxDLQRawValueBytes）时**截断 Value**，只留截断占位
// + 标记 payload_truncated=true，保证 DLQ 写入必成功（回灌须从源 message 表按 messageId 重取）。
func buildDLQRecord(reason dlqReason, m fetchedMessage, res esindex.BulkItemResult) dlqRecord {
	detail := ""
	status := res.Status
	switch reason {
	case reasonSchemaInvalid, reasonVisibilityUntrusted:
		status = 0
	default:
		if res.Err != nil {
			detail = res.Err.Error()
		}
	}
	rec := dlqRecord{
		Reason:    reason.reasonString(),
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Status:    status,
		Detail:    detail,
	}
	// 超限对称截断（§4.4.3）：按 marshal 后 base64 膨胀预留余量的原始字节阈值判。
	if len(m.Value) > maxDLQRawValueBytes {
		rec.Value = nil
		rec.PayloadTruncated = true
		if rec.Detail != "" {
			rec.Detail += "; "
		}
		rec.Detail += fmt.Sprintf("payload truncated (orig %d bytes > %d); replay must re-fetch from source by messageId(=key)",
			len(m.Value), maxDLQRawValueBytes)
	}
	return rec
}
