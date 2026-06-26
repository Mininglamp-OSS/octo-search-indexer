package esindex

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// mappingJSON 是 octo-message 索引的规范 mapping + analyzer（中文分词），编译进二进制。
// 它是 octo-deployment 协调变更引用的单一真源——托管 OpenSearch 模板须与本文件同契约版本对齐。
//
//go:embed mapping/octo-message.json
var mappingJSON []byte

// IndexMappingJSON 返回内嵌的规范 mapping 字节（供 backfill / 部署校验复用）。
func IndexMappingJSON() []byte {
	out := make([]byte, len(mappingJSON))
	copy(out, mappingJSON)
	return out
}

// EnsureIndex 校验目标索引**已存在**：存在则放行，缺失（404）则拒启动并报错。
// 不再自动裸建索引——裸建会丢失 ISM/lifecycle、shards/replicas、aliases 等部署级能力（见 issue #29），
// 索引须由人工按规范 mapping 预建。供 es-indexer 启动时与 backfill job 调用。
func (w *osWriter) EnsureIndex(ctx context.Context) error {
	return ensureIndex(ctx, w.client, w.index)
}

// ensureIndex 抽出便于测试（注入 mock transport 的 client）。纯存在性校验，不创建。
func ensureIndex(ctx context.Context, client *opensearchapi.Client, index string) error {
	resp, err := client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{Indices: []string{index}})
	// Exists 在 404 时返回 (resp, err)；据状态码判定，而非仅看 err。
	if resp != nil && resp.StatusCode == http.StatusOK {
		return nil // 已存在，放行
	}
	if resp != nil && resp.StatusCode == http.StatusNotFound {
		// 404 → 索引缺失，拒启动。不自动创建（裸建丢 ISM/shards/replicas/aliases）。
		return fmt.Errorf("esindex: index %q does not exist; refusing to start — create it manually with the required mapping, ISM/lifecycle policy, shards/replicas and aliases first (auto-create intentionally disabled, see issue #29)", index)
	}
	// 非 200 / 非 404 的异常（如 5xx / 鉴权失败）：上报，不盲目放行。
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}
	return fmt.Errorf("esindex: index exists check for %q returned status %d: %w", index, status, err)
}
