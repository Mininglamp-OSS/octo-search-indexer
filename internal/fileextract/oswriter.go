package fileextract

// oswriter.go — OS partial `_update` client。只写 payload.file.content + payload.file.contentMeta，
// 不动主 doc 其他字段。doc_as_upsert=false 避免主 doc 未落时造孤儿子文档（reader 会崩）。
//
// 错误分类：
//   - HTTP 200/201 → nil
//   - HTTP 404 → errDocNotYet（v2 §7 #1 时序竞态：es-indexer 还没消费到，本批重试）
//   - HTTP 409 → nil（OS 内部 retry_on_conflict=3 已处理，超过依然报 409 时视为 caller 该重试）
//   - HTTP 5xx → errOSTransient（transient，触发重试或吐 error 让 kafka 重放）
//   - HTTP 4xx (非 404/409) → errOSPermanent（应罕见；写请求 body 4xx 是编程 bug 需修）

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// OS 写入错误哨兵，上层用 errors.Is 分类。
var (
	// errDocNotYet 主 doc 还没落到 OS（es-indexer 慢一步），触发本批 Kafka 重试。
	errDocNotYet = errors.New("fileextract: doc not yet indexed by es-indexer (404)")
	// errOSTransient 5xx / 网络错，可重试。
	errOSTransient = errors.New("fileextract: opensearch transient error")
	// errOSPermanent 4xx (非 404/409) permanent，通常是请求 body 编程 bug。
	errOSPermanent = errors.New("fileextract: opensearch permanent error")
)

// osWriter 只做一件事：给指定 messageId 的 doc 做 partial update payload.file.content + contentMeta。
type osWriter struct {
	client *opensearchapi.Client
	index  string
}

// newOSWriter 构造 OpenSearch 客户端（复用 esindex.NewWriter 相同 dial 参数形态便于运维一致）。
// cfg.ESTLSInsecureSkipVerify=true 时注入 esindex.InsecureSkipVerifyTransport() 跳过自签
// 证书校验（与 es-indexer / reconcile / backfill 同一 helper，避免各自实现漂移）。
func newOSWriter(cfg ServiceConfig) (*osWriter, error) {
	if len(cfg.ESAddresses) == 0 {
		return nil, errors.New("fileextract: at least one OS address required")
	}
	if cfg.ESIndex == "" {
		return nil, errors.New("fileextract: OS index required")
	}
	var transport http.RoundTripper
	if cfg.ESTLSInsecureSkipVerify {
		transport = esindex.InsecureSkipVerifyTransport()
	}
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: cfg.ESAddresses,
			Username:  cfg.ESUsername,
			Password:  cfg.ESPassword,
			Transport: transport,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("fileextract: new opensearch client: %w", err)
	}
	return &osWriter{client: client, index: cfg.ESIndex}, nil
}

// UpdateContent partial update 只写 payload.file.content + payload.file.contentMeta。
// doc_as_upsert=false → 主 doc 未落时返 errDocNotYet（不 upsert 造孤儿）。
// retry_on_conflict=3 → OS 内部处理 optimistic-lock 冲突（file-extractor 与 backfill Job
// 同时写同一 doc 时缓解）。
func (w *osWriter) UpdateContent(ctx context.Context, messageID string, content string, meta esindex.FileContentMeta) error {
	body := map[string]any{
		"doc": map[string]any{
			"payload": map[string]any{
				"file": map[string]any{
					"content":     content,
					"contentMeta": meta,
				},
			},
		},
		"doc_as_upsert": false,
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal update body: %w", err)
	}
	retry := 3
	req := opensearchapi.UpdateReq{
		Index:      w.index,
		DocumentID: messageID,
		Body:       bytes.NewReader(buf),
		Params:     opensearchapi.UpdateParams{RetryOnConflict: &retry},
	}
	resp, err := w.client.Update(ctx, req)
	if err != nil {
		return classifyOSErr(resp, err)
	}
	if resp == nil {
		return errors.New("fileextract: nil update response")
	}
	return nil
}

// tombstoneStatus 是 permanent-fail 文件在 contentMeta.status 里写入的常量，与 backfill
// source.go scroll query `must_not term contentMeta.status=<value>` 严格对齐。改此值必须
// 同步改 source.go（有 test 契约锁）。
const tombstoneStatus = "unextractable"

// WriteTombstone 是 Round-3 Blocker B (yujiawei P1 / Jerry-Xin #2) fix：对 permanent-fail 文件
// 写 tombstone marker 到 contentMeta，让 backfill scroll query 通过 must_not term 过滤，避免
// rerun 无限重复 DLQ 已知永久失败文件。
//
// 语义：partial _update 只写 contentMeta.{status,reason,extractedAt}，**不写 content 字段**
// —— 保留 "真无 content" 语义（reader 搜索时不会命中，与不写 tombstone 时的行为一致）。
// doc_as_upsert=false → 主 doc 未落时返 errDocNotYet（不 upsert 造孤儿）；上层收到 errDocNotYet
// 视为可容忍（tombstone 只是补丁不阻塞主 DLQ 路径，backfill 兜底重跑时若主 doc 已落再补写）。
//
// 错误分类同 UpdateContent（复用 classifyOSErr）。retry_on_conflict=3 缓解与 es-indexer 并发。
func (w *osWriter) WriteTombstone(ctx context.Context, messageID string, reason string) error {
	status := tombstoneStatus
	body := map[string]any{
		"doc": map[string]any{
			"payload": map[string]any{
				"file": map[string]any{
					"contentMeta": esindex.FileContentMeta{
						ExtractedAt: time.Now().Unix(),
						Status:      &status,
						Reason:      &reason,
					},
				},
			},
		},
		"doc_as_upsert": false,
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal tombstone body: %w", err)
	}
	retry := 3
	req := opensearchapi.UpdateReq{
		Index:      w.index,
		DocumentID: messageID,
		Body:       bytes.NewReader(buf),
		Params:     opensearchapi.UpdateParams{RetryOnConflict: &retry},
	}
	resp, err := w.client.Update(ctx, req)
	if err != nil {
		return classifyOSErr(resp, err)
	}
	if resp == nil {
		return errors.New("fileextract: nil tombstone response")
	}
	return nil
}

// classifyOSErr 把 opensearch-go 的通用 error 转成本包 sentinel（errors.Is 可读）。
// opensearch-go v3 的 Update 在 HTTP 非 2xx 时会返回带 status code 的 err（含 response body）。
//
// v1.13 P2-1 fix：新增 429 (Too Many Requests) → errOSTransient 分类
// （老代码走 status >= 400 catch-all 误归 errOSPermanent，429 是 OS 限流是 transient 语义，
// 与 download.go:117 的 CDN 429 处理一致）。
func classifyOSErr(resp *opensearchapi.UpdateResp, err error) error {
	if resp != nil && resp.Inspect().Response != nil {
		status := resp.Inspect().Response.StatusCode
		switch {
		case status == http.StatusNotFound:
			return errDocNotYet
		case status == http.StatusConflict:
			// retry_on_conflict=3 已经尝试过，仍报 409 视为 transient（下轮重试）
			return errOSTransient
		case status == http.StatusTooManyRequests:
			// v1.13 P2-1：OS 限流是 transient 语义，需在 400 catch-all 之前拦下
			return errOSTransient
		case status >= 500:
			return errOSTransient
		case status >= 400:
			return fmt.Errorf("%w: status %d: %v", errOSPermanent, status, err)
		}
	}
	return fmt.Errorf("%w: %v", errOSTransient, err)
}
