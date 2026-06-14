package esindex

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// mappingJSON 是 octo-message 索引的规范 mapping + analyzer（中文分词），编译进二进制。
// 它是 octo-deployment 协调变更引用的单一真源——托管 OpenSearch 模板须与本文件同契约版本对齐。
//
//go:embed mapping/octo-message.json
var mappingJSON []byte

// resourceAlreadyExistsType 是 OpenSearch 在「索引已存在」时返回的 error.type（HTTP 400）。
// 多副本并发启动时抢输的副本 Create 会收到它——必须幂等视为成功，不得冒泡触发 Exit。
const resourceAlreadyExistsType = "resource_already_exists_exception"

// IndexMappingJSON 返回内嵌的规范 mapping 字节（供 backfill / 部署校验复用）。
func IndexMappingJSON() []byte {
	out := make([]byte, len(mappingJSON))
	copy(out, mappingJSON)
	return out
}

// EnsureIndex 幂等创建目标索引：若不存在则用内嵌 mapping 创建；已存在则不动（不覆盖既有
// mapping，避免运行期破坏）。供 es-indexer 启动时与 backfill job 调用。
//
// 🔴 并发安全（修 PR#6 P1 TOCTOU）：Exists→Create 非原子。多副本同时启动（replicas≥2 /
// 滚动更新 / pod 重调度）时，抢输的副本 Create 会收到 400 resource_already_exists_exception。
// 该错误**幂等视为成功**（索引确已就绪），绝不冒泡到 os.Exit 触发 CrashLoopBackOff。
func (w *osWriter) EnsureIndex(ctx context.Context) error {
	return ensureIndex(ctx, w.client, w.index)
}

// ensureIndex 抽出便于测试（注入 mock transport 的 client）。
func ensureIndex(ctx context.Context, client *opensearchapi.Client, index string) error {
	resp, err := client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{Indices: []string{index}})
	// Exists 在 404 时返回 (resp, err)；据状态码判定，而非仅看 err。
	if resp != nil && resp.StatusCode == http.StatusOK {
		return nil // 已存在，不覆盖
	}
	if resp != nil && resp.StatusCode != http.StatusNotFound {
		// 非 200 / 非 404 的异常（如 5xx / 鉴权失败）：上报，不盲目建索引。
		return fmt.Errorf("esindex: index exists check for %q returned status %d: %w", index, resp.StatusCode, err)
	}

	// 404（或无 resp 但 err 提示不存在）→ 创建。
	cresp, cerr := client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: index,
		Body:  bytes.NewReader(mappingJSON),
	})
	if cerr == nil {
		return nil // 创建成功
	}
	// 🔴 幂等收口：Create 失败时若是「已存在」竞态（另一副本抢先建好）→ 视为成功。
	// 仅在 HTTP 400 + already-exists 信号时才**权威**判定为已存在（避免 5xx/代理/封装错误恰好
	// 含该 token 时把真失败误判成功）。其余一律走 existsNow 兜底再确认。
	if isAlreadyExists(cresp, cerr) {
		return nil
	}
	// 兜底：并发下错误形态可能各异（非 400 / 文本不含 token）。只有此刻索引**确已就绪**才算成功；
	// 否则（如真 5xx 且索引仍缺失）冒泡报错，不掩盖真问题。
	if existsNow(ctx, client, index) {
		return nil
	}
	return fmt.Errorf("esindex: create index %q: %w", index, cerr)
}

// isAlreadyExists 权威判定 Create 失败是否为「索引已存在」竞态——**要求实际 HTTP 响应状态为 400**：
//   - 先确认真实 HTTP 状态码（cresp.Inspect().Response.StatusCode）== 400；用响应体里的 status 字段
//     不可靠（代理可能 HTTP 500 却回 body status:400），故只信传输层状态码。
//   - 再看 already-exists 信号：opensearchapi.Error.Type / root_cause.Type == 该类型，或错误文本含该串。
//
// 非 400（5xx/代理/封装错误等）一律返回 false——交由 existsNow 兜底，仅当索引确已就绪才放行，
// 杜绝真失败被掩盖。
func isAlreadyExists(cresp *opensearchapi.IndicesCreateResp, cerr error) bool {
	// 唯一可信的状态来源：实际 HTTP 响应状态码。无响应或非 400 → 不在此判已存在。
	if cresp == nil || cresp.Inspect().Response == nil ||
		cresp.Inspect().Response.StatusCode != http.StatusBadRequest {
		return false
	}
	var apiErr opensearchapi.Error
	if errors.As(cerr, &apiErr) {
		if apiErr.Err.Type == resourceAlreadyExistsType {
			return true
		}
		for _, rc := range apiErr.Err.RootCause {
			if rc.Type == resourceAlreadyExistsType {
				return true
			}
		}
	}
	return strings.Contains(cerr.Error(), resourceAlreadyExistsType)
}

// existsNow 重新探测索引是否已就绪（幂等兜底，忽略探测自身错误——404 时 client 会返回 err）。
func existsNow(ctx context.Context, client *opensearchapi.Client, index string) bool {
	resp, err := client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{Indices: []string{index}})
	_ = err // 探测错误（如 404 携带的 err）不影响判定，仅看状态码
	return resp != nil && resp.StatusCode == http.StatusOK
}
