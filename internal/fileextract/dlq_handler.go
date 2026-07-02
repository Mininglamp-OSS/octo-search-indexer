package fileextract

// dlq_handler.go — DLQ 投递有界重试 + 本地 spill 逃逸（v1.13 P2-1 yujiawei review fix）。
//
// 端口自 internal/consumer/dlq.go 的 dlqHandler pattern。老 fileextract.Processor 直接调
// p.dlqSink.WriteDLQ，DLQ topic 挂即 outcomeFatal 全 worker 停 —— 与 consumer 侧成熟的
// 「有界重试 → spill 落盘 → 告警 → 越过 offset（绝不永久卡 partition）」pattern 不对称。
// 本 handler 补齐同 pattern；不 import consumer 包避免循环依赖 + 保持独立可测。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// alerter 抽象「响铃告警」。生产接 Prometheus counter + 告警规则；这里默认走 log。
// 测试注入假实现记录 event 序列做断言。
type alerter interface {
	Alert(event string, detail string)
}

// dlqHandlerConfig 配 DLQ 投递有界重试 + 逃逸策略（与 consumer/dlq.go dlqConfig 语义一致）。
type dlqHandlerConfig struct {
	// MaxRetries：DLQ 投递自身 transient 失败的有界重试次数。默认 5。
	MaxRetries int
	// RetryBackoff：重试退避基（指数 + 满抖动，与 backoff.go expJitterBackoff 一致）。默认 200ms。
	RetryBackoff time.Duration
	// SpillDir：逃逸时本地落地目录；为空 → 逃逸=硬停返 errDLQHardStop（分区不推进，人工介入）。
	// 生产建议挂一个 emptyDir 或 PVC 目录（K8s pod），运维回灌工具从此读文件重投 DLQ。
	SpillDir string
}

// defaultDLQHandlerConfig 生产默认（对齐 consumer/dlq.go defaultDLQConfig）。
func defaultDLQHandlerConfig() dlqHandlerConfig {
	return dlqHandlerConfig{MaxRetries: 5, RetryBackoff: 200 * time.Millisecond, SpillDir: ""}
}

// errDLQHardStop 表示 DLQ 写持续失败且未配置 spill → 调用方硬停分区（不前进 offset）。
// 与 consumer/dlq.go errDLQHardStop 语义一致；processBatch 收到此错走 outcomeFatal 停 worker，
// K8s 会重启保 offset 未推进；同时 alerter 已 page 告警提示运维手工介入。
var errDLQHardStop = errors.New("fileextract: DLQ write exhausted retries and no spill configured (hard stop)")

// dlqHandler 负责把 fileextract 的毒丸消息可靠送进 DLQ topic；DLQ 写自身 transient 失败时
// 执行终态逃逸：
//   - 有界重试（指数退避 + 满抖动 + ctx 感知，SIGTERM 即停）
//   - 全部失败 → 若配置 SpillDir，落盘 + 告警 + 返回 nil（允许 offset 越过，绝不卡分区）
//   - 未配 SpillDir → 返 errDLQHardStop 硬停分区 + page 告警（人工介入）
type dlqHandler struct {
	sink    dlqSink
	alert   alerter
	cfg     dlqHandlerConfig
	sleep   func(context.Context, time.Duration) error // ctx-aware，测试注入即时返
	nowUnix func() int64
}

// newDLQHandler 组装 handler。sleep 走本包 backoff.go 的 sleepCtx，nowUnix 走 time.Now。
func newDLQHandler(sink dlqSink, alert alerter, cfg dlqHandlerConfig) *dlqHandler {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultDLQHandlerConfig().MaxRetries
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = defaultDLQHandlerConfig().RetryBackoff
	}
	return &dlqHandler{
		sink:    sink,
		alert:   alert,
		cfg:     cfg,
		sleep:   sleepCtx,
		nowUnix: func() int64 { return time.Now().Unix() },
	}
}

// Send 把一条 DLQ record 送进 DLQ topic。返回 nil = 已终态处理（投递成功 or 已 spill 落地），
// 调用方可放心越过 offset；返回 error = 硬停逃逸（offset 不推进，需人工介入）。
// ctx 取消（SIGTERM）时立刻返 ctx.Err()。
func (h *dlqHandler) Send(ctx context.Context, rec dlqRecord) error {
	value, err := json.Marshal(rec)
	if err != nil {
		h.alert.Alert("dlq_marshal_failed", fmt.Sprintf("offset=%d: %v", rec.Offset, err))
		return h.escape(rec, fmt.Sprintf("marshal failed: %v", err))
	}

	var lastErr error
	for attempt := 0; attempt <= h.cfg.MaxRetries; attempt++ {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if attempt > 0 {
			// 指数退避 + 满抖动 + ctx 感知（复用本包 backoff.go）。
			if serr := h.sleep(ctx, expJitterBackoff(h.cfg.RetryBackoff, 0, attempt)); serr != nil {
				return serr
			}
		}
		if werr := h.sink.WriteDLQ(ctx, rec.Key, value); werr == nil {
			return nil // DLQ 投递成功
		} else {
			lastErr = werr
		}
	}

	h.alert.Alert("dlq_write_exhausted",
		fmt.Sprintf("offset=%d retries=%d lastErr=%v", rec.Offset, h.cfg.MaxRetries, lastErr))
	return h.escape(rec, fmt.Sprintf("dlq write exhausted: %v", lastErr))
}

// escape 执行终态逃逸：SpillDir 非空 → 落盘 + 告警 + 返 nil 允许越过；否则返 errDLQHardStop。
// 落盘文件名 `dlq-spill-<partition>-<offset>-<epoch>.json`（partition+offset 保证唯一，epoch 便于排序）。
func (h *dlqHandler) escape(rec dlqRecord, detail string) error {
	if h.cfg.SpillDir == "" {
		return errDLQHardStop
	}
	rec.SpilledAt = h.nowUnix()
	if rec.Detail == "" {
		rec.Detail = detail
	}
	data, err := json.Marshal(rec)
	if err != nil {
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
	return nil
}

// logAlerter 是默认 alerter 实现，走 log.Printf。生产可以在 cmd 里注入 Prometheus counter 版。
type logAlerter struct {
	logf func(format string, args ...any)
}

// newLogAlerter 组装 log-based alerter（logf 通常传 log.Printf；测试可注入自己的记录函数）。
func newLogAlerter(logf func(string, ...any)) *logAlerter {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &logAlerter{logf: logf}
}

func (l *logAlerter) Alert(event string, detail string) {
	l.logf("file-extractor ALERT [%s]: %s", event, detail)
}
