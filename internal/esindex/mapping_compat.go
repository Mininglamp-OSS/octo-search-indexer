package esindex

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// 本文件实现两件互相关联的事，单一真源都是内嵌 mapping octo-message.json：
//   1. flattenMappingFields：把一棵 OS mapping properties 树拍平成「可索引叶子字段的点号
//      全路径集合」，**跳过 enabled:false 子树**（payloadRaw 不索引 → 不入集）。drift 防漂移门
//      （drift_gate_test）与启动期 mapping-compat 断言共用此拍平逻辑，口径一处不漂。
//   2. AssertLiveMappingCompatible：启动期 fail-closed 断言——GET 目标索引 live mapping，
//      校验本期所有新字段路径齐备；缺则**拒启动**（loud crash），不静默灌 dynamic:strict 4xx。

// requiredMappingFieldPaths 是方案 B 后 indexer 写出的、**必须**在 live mapping 里声明的字段
// 路径（dynamic:strict 下未声明即 bulk 4xx 静默全量塌）。从内嵌 mapping 派生 + 本期新增三类：
//   - payload.mergeForward.msgs.from / .timestamp（PR#425 锚）
//   - payload.richText.searchText（前瞻）
//   - payloadRaw（enabled:false，不在 flatten 集里，单独断言其声明存在 §6.4）
//
// 之所以钉一份显式清单而非「整张 embed mapping ⊆ live」：embed 与 live 可能因历史
// migrate-forward 有良性差异；这里只断言**本期变更引入的新字段**齐备，命中 §6.4 的部署竞态
// （漏迁新字段 → 启动即每条 bulk 4xx）。
var requiredMappingFieldPaths = []string{
	"payload.mergeForward.msgs.from",
	"payload.mergeForward.msgs.timestamp",
	"payload.richText.searchText",
	// v1.10：富文本虚拟子文档三字段（derivatives 指向父 + virtual 标记）。
	"parentMessageId",
	"parentPayloadType",
	"virtual",
	// v1.11：subSeq 排序第三键 tiebreaker（search_after 不丢同 tuple 兄弟）。
	"subSeq",
	// v1.12：文件正文全文检索（file content indexing）。
	// - payload.file.content 是 file-extractor 独立服务通过 OS _update partial 写入的字段。
	// - payload.file.contentMeta.extractedAt 采样 contentMeta object 一个子字段代表整个对象
	//   （flatten 只到 leaf field，逐 leaf 断言过重；断言存在 extractedAt 即证 contentMeta object
	//   已声明 properties）。
	"payload.file.content",
	"payload.file.contentMeta.extractedAt",
	// Round-3 Blocker B (yujiawei P1 / Jerry-Xin #2)：contentMeta 新增 status + reason 字段供
	// permanent-fail tombstone 写入（extractor 层，dlqReason 非空时写 status="unextractable"）。
	// 与 backfill source.go scroll query `must_not term status=unextractable` 配合，防 rerun
	// 重复 DLQ 已知永久失败文件。mapping 缺这两字段则 dynamic:strict 会 4xx 拒 tombstone 写入
	// → 启动期 loud crash 让运维先 PUT mapping 再滚 pod。
	"payload.file.contentMeta.status",
	"payload.file.contentMeta.reason",
}

// requiredDisabledObjectPaths 是必须以 enabled:false object 形态存在的留底字段（payloadRaw）。
// 它不在 flatten 的可索引叶子集里（enabled:false 子树被跳过），故单独按「顶层 properties 里
// 存在该键且 enabled==false」断言。
var requiredDisabledObjectPaths = []string{"payloadRaw"}

// forbiddenSourceExcludes 是 live mapping `_source.excludes` **不能包含**的字段路径。
//
// 🔴 v1.13 Blocker #3 复发教训：老 mapping 曾写 `"_source": {"excludes": ["payload.file.content"]}`
// 剔除 content 从 `_source`。但 v1.13 Blocker #3 修复引入 scripted_upsert Painless script 从
// `ctx._source.payload.file.content` 读**已存在的 content** 做 preserve —— 字段被 excludes 剔除
// 后 `ctx._source` 里永远读到 null，preserve 分支永不执行，redeliver 时 content 仍被覆盖。
// 修复方向：mapping 去掉 excludes（本 embed mapping 已删）+ 本断言 fail-closed 拦住 live index
// 尚残留的旧 excludes（deployed mapping 若未同步更新会被启动期 loud crash 挡住）。
//
// 未来 file-extractor 扩 preserve 字段时**同步扩本清单**：任何 preservedFilePaths 里的路径都必须
// 保证不被 `_source.excludes` 剔除，否则 scripted_upsert preserve 语义直接失效。
var forbiddenSourceExcludes = []string{
	"payload.file.content",
	"payload.file.contentMeta",
}

// AssertLiveMappingCompatible 启动期 fail-closed 断言：GET 目标索引 live mapping，校验本期所有
// 新字段路径齐备（可索引字段 + payloadRaw enabled:false object）。缺任一 → 返回 error，调用方
// 据此拒启动（loud crash），绝不静默向 dynamic:strict 索引灌 4xx。
//
// v1.13 Blocker #3 复发 fix（P2-2）：增加 `_source.excludes` 校验——若 live mapping 里 excludes
// 包含 forbiddenSourceExcludes 任一路径（scripted_upsert preserve 依赖字段），loud crash。
//
// 🔴 与 EnsureIndex 的存在性校验**互相独立、不打架**（eng-review §4 额外发现 + PR #29 fail-fast）：
// EnsureIndex 只判索引是否存在（存在即放行，缺失则拒启动）；本断言只校验**字段路径齐备 +
// _source.excludes 干净**，缺字段 or excludes 污染才 loud crash。即「索引存在」放行，
// 「字段缺失 / excludes 挡 preserve」才拒启动——二者方向不冲突。
func (w *osWriter) AssertLiveMappingCompatible(ctx context.Context) error {
	return assertLiveMappingCompatible(ctx, w.client, w.index)
}

func assertLiveMappingCompatible(ctx context.Context, client *opensearchapi.Client, index string) error {
	resp, err := client.Indices.Mapping.Get(ctx, &opensearchapi.MappingGetReq{Indices: []string{index}})
	if err != nil {
		return fmt.Errorf("esindex: mapping-compat get live mapping for %q failed: %w", index, err)
	}
	entry, ok := resp.Indices[index]
	if !ok {
		// alias / 单索引：GET 返回的 key 可能是底层具体索引名而非传入名。取唯一一个兜底。
		if len(resp.Indices) == 1 {
			for _, v := range resp.Indices {
				entry = v
				ok = true
				break
			}
		}
	}
	if !ok {
		return fmt.Errorf("esindex: mapping-compat live mapping for %q not found in response (got %d index entries)",
			index, len(resp.Indices))
	}

	var root mappingNode
	if err := json.Unmarshal(entry.Mappings, &root); err != nil {
		return fmt.Errorf("esindex: mapping-compat parse live mapping for %q failed: %w", index, err)
	}

	indexable := flattenMappingNode(root)
	var missing []string
	for _, p := range requiredMappingFieldPaths {
		if !indexable[p] {
			missing = append(missing, p)
		}
	}
	for _, p := range requiredDisabledObjectPaths {
		if !disabledObjectPresent(root, p) {
			missing = append(missing, p+" (enabled:false object)")
		}
	}
	// v1.13 Blocker #3 复发 fix (P2-2)：校验 `_source.excludes` **不含**保留字段路径。
	// live mapping 若残留旧 excludes（部署未同步），scripted_upsert preserve 会从 ctx._source 读到
	// null → preserve 失效 → redeliver 覆盖 file-extractor 写的字段。loud crash 拦下。
	var polluted []string
	for _, p := range forbiddenSourceExcludes {
		if sourceExcludesContains(entry.Mappings, p) {
			polluted = append(polluted, p)
		}
	}
	if len(missing) > 0 || len(polluted) > 0 {
		sort.Strings(missing)
		sort.Strings(polluted)
		msg := fmt.Sprintf("esindex: mapping-compat FAILED for index %q", index)
		if len(missing) > 0 {
			msg += fmt.Sprintf(" — missing required field paths %v", missing)
		}
		if len(polluted) > 0 {
			msg += fmt.Sprintf(" — _source.excludes contains %v which breaks scripted_upsert preserve "+
				"(Blocker #3 revive; run migrate-forward to remove these excludes)", polluted)
		}
		msg += "; refusing to ingest into a mis-configured index"
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// sourceExcludesContains 报告 live mapping 的 `_source.excludes` 数组里是否含 path。
// mappings 是 `client.Indices.Mapping.Get` 返回的 entry.Mappings 原始 JSON（顶层是 mappings 对象），
// 我们只关心 `_source.excludes` 数组是否含指定字段路径。excludes 缺失（未设 _source）时返 false。
func sourceExcludesContains(mappings json.RawMessage, path string) bool {
	var top struct {
		Source struct {
			Excludes []string `json:"excludes"`
		} `json:"_source"`
	}
	if err := json.Unmarshal(mappings, &top); err != nil {
		return false
	}
	for _, e := range top.Source.Excludes {
		if e == path {
			return true
		}
	}
	return false
}

// mappingNode 是 OS mapping 树的最小递归形态：一个节点可能有 properties（子字段）、
// enabled（object 是否索引）、type。
type mappingNode struct {
	Enabled    *bool                  `json:"enabled,omitempty"`
	Type       string                 `json:"type,omitempty"`
	Properties map[string]mappingNode `json:"properties,omitempty"`
}

// MappingIndexableFieldPaths 返回内嵌规范 mapping（octo-message.json）里所有**可索引**叶子
// 字段的点号全路径集合（跳过 enabled:false 子树，即 payloadRaw）。drift 防漂移门用它断言
// reader 字段 ⊆ 本集。
func MappingIndexableFieldPaths() (map[string]bool, error) {
	var top struct {
		Mappings mappingNode `json:"mappings"`
	}
	if err := json.Unmarshal(mappingJSON, &top); err != nil {
		return nil, fmt.Errorf("esindex: parse embedded mapping: %w", err)
	}
	return flattenMappingNode(top.Mappings), nil
}

// flattenMappingNode 递归遍历 mapping 节点，收集所有可索引叶子字段的点号全路径。
// 规则：
//   - enabled==false 的 object 子树整体跳过（不索引，payloadRaw）。
//   - 有 properties 的节点是中间对象，递归下钻（自身不作叶子，除非它同时是某叶子——OS 里
//     object 容器本身不可作 multi_match 字段，故只收叶子）。
//   - 无 properties 的节点是叶子字段 → 收其全路径。
func flattenMappingNode(root mappingNode) map[string]bool {
	out := make(map[string]bool)
	var walk func(prefix string, n mappingNode)
	walk = func(prefix string, n mappingNode) {
		if n.Enabled != nil && !*n.Enabled {
			return // enabled:false 子树跳过（不索引）
		}
		if len(n.Properties) == 0 {
			if prefix != "" {
				out[prefix] = true
			}
			return
		}
		for name, child := range n.Properties {
			next := name
			if prefix != "" {
				next = prefix + "." + name
			}
			walk(next, child)
		}
	}
	walk("", root)
	return out
}

// disabledObjectPresent 报告 root.properties 链路上 dotted path 处存在一个 enabled:false 的 object。
// 当前只用于顶层 payloadRaw（path 无点号）；保留 dotted 解析以备将来。
func disabledObjectPresent(root mappingNode, path string) bool {
	segs := strings.Split(path, ".")
	n := root
	for i, seg := range segs {
		child, ok := n.Properties[seg]
		if !ok {
			return false
		}
		if i == len(segs)-1 {
			return child.Enabled != nil && !*child.Enabled
		}
		n = child
	}
	return false
}
