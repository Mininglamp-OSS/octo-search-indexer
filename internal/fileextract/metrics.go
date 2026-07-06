package fileextract

// metrics.go — file-extractor 计数器骨架（IDX-3 只做 log 打印，IDX-4+ 接 Prometheus 阶段 7 独立任务）。
// 名字对齐 feasibility.md v2 §11 里提到的观察指标：processed / skipped_non_file / dlq_total[reason] /
// extract_duration_ms / doc_not_yet（时序竞态触发次数）。

import (
	"log"
	"sync/atomic"
)

// counters 是一组进程内简单计数（uint64 atomic），无并发风险且零依赖。
type counters struct {
	processed      atomic.Uint64
	skippedNonFile atomic.Uint64
	dlqTotal       atomic.Uint64
	docNotYet      atomic.Uint64 // v2 §7 #1 时序竞态触发计数，观察 Phase 2 独立 retry topic 是否要上
	retryExhausted atomic.Uint64 // v1.13 Blocker #2：in-place retry N 次未成功 → DLQ 触发计数
	osPermanent    atomic.Uint64 // v1.13 P2-2：OS 4xx permanent → DLQ 触发计数
}

func (c *counters) IncProcessed()      { c.processed.Add(1) }
func (c *counters) IncSkippedNonFile() { c.skippedNonFile.Add(1) }
func (c *counters) IncDLQ()            { c.dlqTotal.Add(1) }
func (c *counters) IncDocNotYet()      { c.docNotYet.Add(1) }
func (c *counters) IncRetryExhausted() { c.retryExhausted.Add(1) }
func (c *counters) IncOSPermanent()    { c.osPermanent.Add(1) }

// LogSnapshot 打印当前累计计数（周期性调用，比如每 N 秒或 debug 时）。
// IDX-3 stub 版：只 log；阶段 7 接 Prometheus counter 替换。
func (c *counters) LogSnapshot() {
	log.Printf("file-extractor metrics: processed=%d skipped_non_file=%d dlq_total=%d doc_not_yet=%d retry_exhausted=%d os_permanent=%d",
		c.processed.Load(), c.skippedNonFile.Load(), c.dlqTotal.Load(), c.docNotYet.Load(),
		c.retryExhausted.Load(), c.osPermanent.Load())
}
