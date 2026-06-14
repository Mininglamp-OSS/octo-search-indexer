// Package recon 是消息检索管线的对账（reconciliation）工具：给定时间窗，比对 MySQL message
// 表行数 vs OpenSearch doc 数，扣除「已知排除」后输出差异报告。
//
// 设计纪律（YUJ-4534）：
//   - 对账口径与 es-indexer 写入器**解耦**（沿用 internal/esindex 的解耦纪律）：本包只描述
//     「源行数 / sink doc 数 / 已知排除」的算术，数据来源经接口注入，可被阶段 6 backfill job
//     直接复用为其正确性 gate。
//   - 不变式（阶段 6 backfill 验收门）：
//     ES(按 _id=message_id 去重 doc 数) + DLQ 条数 + 已知排除(raw_excluded: Signal/非文本)
//     == 源 message 行数(时间窗内)
//     无法归因的缺口（reconciled != 0）即 STOP。
//
// 路线甲提醒：撤回/删除态**不进** ES，但它们的正文行仍在 message 表里（撤回是 message_extra
// 原地 UPDATE，不删 message 行）。对账以 message 表行数为源真相；撤回/删除不计入「源排除」——
// 它们的正文 doc 确实应在 ES 中（读时再 join 过滤），故不从源行数里扣。
package recon

import "fmt"

// Counts 是一次对账的输入计数（均针对同一时间窗 [FromUnix, ToUnix]）。
type Counts struct {
	// SourceRows 是 MySQL message 5 分表在时间窗内的总行数（created_at ∈ 窗）。
	SourceRows int64
	// ESDocs 是 OpenSearch 索引内、同时间窗（created_at 字段）去重后的 doc 数（_id=message_id 天然去重）。
	ESDocs int64
	// RawExcluded 是「已知不可索引类」源行数（Signal 加密 / 非文本 → raw_excluded=true，不产 content
	// 但仍写入 ES 占一个 doc）。**注意**：raw_excluded 仍是一条 ES doc，不从 ESDocs 缺。见 Reconcile 注释。
	RawExcluded int64
	// DLQ 是进了死信、未写入 ES 正文索引的源行数（真异常 / 未知 schema_version）。
	DLQ int64
}

// Report 是对账结论。
type Report struct {
	Counts
	// Expected 是按口径推导的「ES 应有 doc 数」。
	Expected int64
	// Diff = ESDocs - Expected。0 = 完全对平；>0 多了（重复/越界）；<0 少了（漏灌/丢失）。
	Diff int64
	// OK 为 true 表示对平（Diff==0）。
	OK bool
}

// Reconcile 计算对账结论。
//
// 口径推导（raw_excluded 仍是一条 ES doc）：
//
//	Expected_ES_docs = SourceRows - DLQ
//
// 解释：源行数里，进了 DLQ 的那部分不写 ES 正文索引（schema 非法/真异常），故从期望 doc 数扣除。
// raw_excluded（Signal/非文本）**仍写入 ES**（content=null 的 doc，供读路径统一处理），因此**不**从
// 期望里扣——它占一个 doc。撤回/删除态不进 DLQ、也不是 raw_excluded：其正文 doc 本应在 ES（路线甲
// 读时 join 过滤），故同样不扣。
//
// 因此对平条件： ESDocs == SourceRows - DLQ。
func Reconcile(c Counts) Report {
	expected := c.SourceRows - c.DLQ
	diff := c.ESDocs - expected
	return Report{
		Counts:   c,
		Expected: expected,
		Diff:     diff,
		OK:       diff == 0,
	}
}

// String 渲染人类可读的对账报告。
func (r Report) String() string {
	status := "MISMATCH"
	if r.OK {
		status = "OK"
	}
	return fmt.Sprintf(
		"reconcile %s | source_rows=%d es_docs=%d expected=%d diff=%d (raw_excluded=%d dlq=%d)",
		status, r.SourceRows, r.ESDocs, r.Expected, r.Diff, r.RawExcluded, r.DLQ,
	)
}
