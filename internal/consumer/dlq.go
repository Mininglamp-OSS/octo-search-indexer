package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// dlqSink 抽象「把一条毒丸消息投到 DLQ topic」。由 kafka producer 实现；测试注入假实现。
type dlqSink interface {
	// WriteDLQ 投递一条 DLQ 记录。返回 error 表示投递失败（由 dlqHandler 判定是否 transient 重试）。
	WriteDLQ(ctx context.Context, key []byte, value []byte) error
}

// alerter 抽象「响铃告警」。生产接 Prometheus 计数 + 告警规则（阶段 7）；这里先回调 + 日志。
type alerter interface {
	// Alert 报告一次需人工关注的事件（如 DLQ 写持续失败触发本地 spill 逃逸）。
	Alert(event string, detail string)
}

// dlqRecord 是落 DLQ 的记录载荷：原始消息 + 进 DLQ 的原因（便于排查/回灌）。
type dlqRecord struct {
	Reason    string `json:"reason"`     // permanent_4xx / unknown_schema_version / visibility_untrusted
	Topic     string `json:"topic"`      // 源 topic
	Partition int    `json:"partition"`  // 源分区
	Offset    int64  `json:"offset"`     // 源 offset
	Key       []byte `json:"key"`        // 原始 Kafka key（= message_id）
	Value     []byte `json:"value"`      // 原始消息字节（原样保留供回灌；超限时截断置 nil，见 PayloadTruncated）
	Status    int    `json:"status"`     // bulk per-item HTTP 状态（schema/visibility 错误为 0）
	Detail    string `json:"detail"`     // 失败详情
	SpilledAt int64  `json:"spilled_at"` // 仅本地 spill 文件记录写入时间（纪元秒）
	// PayloadTruncated 标记 Value 因超限被截断（§4.4.3 B-5）：带大 RawPayload 的毒丸落 DLQ 时，
	// 原样保留整条字节经 base64 膨胀会触发 DLQ topic 超限连锁卡死，故截断 Value 保证写入必成功。
	// 回灌工具读到 true 时须从源 message 表按 messageId(=Key) 重取，而非依赖 DLQ 内字节。
	PayloadTruncated bool `json:"payload_truncated,omitempty"`
}

// maxDLQRawValueBytes 是 consumer DLQ 信封里原始消息字节（Value）的截断阈值（§4.4.3 B-5）。
//
// 🔴 必须按「marshal 后信封体积」判，不能按原始字节直接拿 1MiB 判：DLQ 实际写出的是
// json.Marshal(dlqRecord)，Go 把 Value []byte 编成 base64(~1.33x 膨胀) + 信封字段开销。若按
// len(Value)>1048576 判，则 Value 落在 ~786KB–1MiB 区间时过不了原始字节判定 → 原样保留 →
// base64 后 JSON 信封 >1MiB（broker message.max.bytes 硬限）→ DLQ 写失败 → 分区卡死。
// 故取**预留 base64 余量的原始字节阈值** 700KB（700KB×1.33≈931KB + 信封 < 1MiB，留足头部余量）。
// 与 producer 腿（internal/producer/dlq.go maxDLQRawPayloadBytes）同口径。
const maxDLQRawValueBytes = 700_000

// dlqConfig 配置 DLQ 投递的有界重试 + 终态逃逸（C4 硬条件）。
type dlqConfig struct {
	// MaxRetries 是 DLQ 投递自身 transient 失败的有界重试次数（超过则触发逃逸）。
	MaxRetries int
	// RetryBackoff 是重试间隔基。
	RetryBackoff time.Duration
	// SpillDir 是逃逸时本地落地目录（不为空则启用 spill 逃逸；为空则逃逸策略=硬停返回错误）。
	SpillDir string
}

// defaultDLQConfig 给出生产默认值。
func defaultDLQConfig() dlqConfig {
	return dlqConfig{MaxRetries: 5, RetryBackoff: 200 * time.Millisecond, SpillDir: ""}
}

// dlqHandler 负责把毒丸消息可靠送进 DLQ；DLQ 写自身 transient 失败时执行终态逃逸（C4）：
// 有界重试 → 本地 spill 落地 + 响铃告警 + 返回成功（让 offset 越过），或（未配 SpillDir）硬停返回错误。
// 绝不允许「DLQ 写失败 → 前缀永久卡死」。
type dlqHandler struct {
	sink    dlqSink
	alert   alerter
	cfg     dlqConfig
	sleep   func(context.Context, time.Duration) error // ctx-aware（测试注入即时返回）
	nowUnix func() int64
}

func newDLQHandler(sink dlqSink, alert alerter, cfg dlqConfig) *dlqHandler {
	return &dlqHandler{
		sink:    sink,
		alert:   alert,
		cfg:     cfg,
		sleep:   sleepCtx,
		nowUnix: func() int64 { return time.Now().Unix() },
	}
}

// errDLQHardStop 表示 DLQ 写持续失败且未配置 spill 逃逸——调用方应硬停分区并 page（不前进）。
var errDLQHardStop = errors.New("consumer: DLQ write exhausted retries and no spill configured (hard stop)")

// Send 把一条毒丸消息送进 DLQ。返回 nil 表示「已终态处理」（投递成功 或 已 spill 落地），
// 调用方可把该条计入连续前缀越过；返回 error 表示硬停逃逸（offset 不前进，需人工介入）。
func (h *dlqHandler) Send(ctx context.Context, rec dlqRecord) error {
	value, err := json.Marshal(rec)
	if err != nil {
		// 序列化失败属编码 bug：直接尝试 spill 原始 value，仍失败则硬停。
		h.alert.Alert("dlq_marshal_failed", fmt.Sprintf("offset=%d: %v", rec.Offset, err))
		return h.escape(rec, fmt.Sprintf("marshal failed: %v", err))
	}

	var lastErr error
	for attempt := 0; attempt <= h.cfg.MaxRetries; attempt++ {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if attempt > 0 {
			// 指数退避 + 满抖动 + ctx 感知（P2-1/P2-2）：缓解多副本 thundering herd，且 SIGTERM 即停。
			if serr := h.sleep(ctx, expJitterBackoff(h.cfg.RetryBackoff, attempt)); serr != nil {
				return serr
			}
		}
		if werr := h.sink.WriteDLQ(ctx, rec.Key, value); werr == nil {
			return nil // DLQ 投递成功，终态处理完成
		} else {
			lastErr = werr
		}
	}

	// 有界重试耗尽 → 终态逃逸（C4）：spill 落地 + 告警 + 越过；或硬停。
	h.alert.Alert("dlq_write_exhausted",
		fmt.Sprintf("offset=%d retries=%d lastErr=%v", rec.Offset, h.cfg.MaxRetries, lastErr))
	return h.escape(rec, fmt.Sprintf("dlq write exhausted: %v", lastErr))
}

// escape 执行终态逃逸：若配置了 SpillDir 则把记录落地本地文件（返回 nil 允许越过）；
// 否则返回 errDLQHardStop（硬停分区 + page，offset 不前进）。
func (h *dlqHandler) escape(rec dlqRecord, detail string) error {
	if h.cfg.SpillDir == "" {
		return errDLQHardStop
	}
	rec.SpilledAt = h.nowUnix()
	rec.Detail = detail
	data, err := json.Marshal(rec)
	if err != nil {
		// 连 spill 都序列化不了：只能硬停。
		return fmt.Errorf("%w: spill marshal failed: %v", errDLQHardStop, err)
	}
	if err := os.MkdirAll(h.cfg.SpillDir, 0o750); err != nil {
		return fmt.Errorf("%w: spill mkdir failed: %v", errDLQHardStop, err)
	}
	name := fmt.Sprintf("dlq-spill-%d-%d-%d.json", rec.Partition, rec.Offset, rec.SpilledAt)
	path := filepath.Join(h.cfg.SpillDir, name)
	if err := os.WriteFile(path, append(data, '\n'), 0o640); err != nil {
		return fmt.Errorf("%w: spill write failed: %v", errDLQHardStop, err)
	}
	h.alert.Alert("dlq_spilled_to_disk", fmt.Sprintf("path=%s offset=%d", path, rec.Offset))
	return nil // 已 spill 落地，允许 offset 越过（绝不永久卡死前缀）
}
