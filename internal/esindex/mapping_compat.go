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
}

// requiredDisabledObjectPaths 是必须以 enabled:false object 形态存在的留底字段（payloadRaw）。
// 它不在 flatten 的可索引叶子集里（enabled:false 子树被跳过），故单独按「顶层 properties 里
// 存在该键且 enabled==false」断言。
var requiredDisabledObjectPaths = []string{"payloadRaw"}

// AssertLiveMappingCompatible 启动期 fail-closed 断言：GET 目标索引 live mapping，校验本期所有
// 新字段路径齐备（可索引字段 + payloadRaw enabled:false object）。缺任一 → 返回 error，调用方
// 据此拒启动（loud crash），绝不静默向 dynamic:strict 索引灌 4xx。
//
// 🔴 与 EnsureIndex 的存在性校验**互相独立、不打架**（eng-review §4 额外发现）：EnsureIndex 只判
// 索引是否存在（存在即放行，缺失则拒启动）；本断言只校验**字段路径齐备**，缺字段才 loud crash。
// 即「索引存在」放行，「字段缺失」才拒启动——二者方向不冲突。
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
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("esindex: mapping-compat FAILED for index %q — live mapping is missing required field paths %v; "+
			"run `make migrate-forward` to add them before starting (refusing to ingest into a dynamic:strict index that "+
			"would 4xx every bulk item)", index, missing)
	}
	return nil
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
