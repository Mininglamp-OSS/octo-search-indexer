package fileextract

// decision.go — 处置状态机与「每分区连续可越过前缀」提交点计算。
// 复制自 internal/consumer/decision.go（不 import consumer 避免循环依赖 + 保持独立可测）。
// v1.13 Blocker #2 fix：Blocker #2 §8 已识别问题 #1（多分区 commit prefix 未按 partition
// group 聚合）在此一步到位——不引入单分区简化版，直接照抄成熟 pattern。

import "sort"

// itemDisposition 是单条消息经处理（含 in-place retry）后的最终处置类别。
type itemDisposition int

const (
	// dispOK 抽取成功 + OS partial update 成功。
	dispOK itemDisposition = iota
	// dispDLQResolved 永久失败（parse_error / oversize / blacklist_ext / download_failed /
	// extract_* / os_permanent / retry_exhausted）且已成功落 DLQ → offset 可越过。
	dispDLQResolved
	// dispTransient 暂时失败（errDocNotYet / errOSTransient / 429/5xx）→ 原地退避重试；
	// offset 不越过此条，直到达 MaxRetriesPerMessage 上限被强制归入 dispDLQResolved。
	dispTransient
)

// hasTransient 报告 dispositions 中是否含 transient（需继续原地重试）。
func hasTransient(dispositions []itemDisposition) bool {
	for _, disp := range dispositions {
		if disp == dispTransient {
			return true
		}
	}
	return false
}

// partitionCommitPoints 计算**每分区**「连续可越过前缀」的提交点。
//
// kafka offset 是 per-partition 的；本 processor 单 Reader 可同时被分配多个分区，
// FetchMessage 在不同分区间交错返回。因此连续前缀必须**按分区**算：每个分区内按 offset
// 升序，从最低 offset 起，dispOK/dispDLQResolved 计入前缀，遇到第一个 dispTransient 立即停。
// 返回每个分区前缀末的那条消息（供 CommitMessages 提交，kafka 单调高水位语义）。
//
// 这杜绝「A 分区某 offset transient 未确认，却因 B 分区更高 offset 成功 commit 把它隐式越过」
// 造成的丢消息（Blocker #2 fix + fix-plan §8 #1）。
func partitionCommitPoints(batch []fetchedMessage, dispositions []itemDisposition) []fetchedMessage {
	type pkey struct {
		topic     string
		partition int
	}
	idxByPart := make(map[pkey][]int)
	for i := range batch {
		k := pkey{batch[i].Topic, batch[i].Partition}
		idxByPart[k] = append(idxByPart[k], i)
	}

	var points []fetchedMessage
	for _, idxs := range idxByPart {
		// 按 offset 升序（FetchMessage 单分区本就有序，排序以防御乱序入参）。
		sort.Slice(idxs, func(a, b int) bool {
			return batch[idxs[a]].Offset < batch[idxs[b]].Offset
		})
		lastPrefix := -1
		for _, i := range idxs {
			if dispositions[i] == dispTransient {
				break // 该分区前缀在首个 transient 处截断
			}
			lastPrefix = i
		}
		if lastPrefix >= 0 {
			points = append(points, batch[lastPrefix])
		}
	}
	return points
}
