// Package esindex 是 es-indexer 的可复用写入器包：把 octo-im 消息检索契约
// (octo-lib contract/searchmsg.Message) 批量写入 OpenSearch/ES。
//
// 设计纪律（YUJ-4530 v4 / YUJ-4534）：
//   - 写入器与 Kafka consumer **解耦**。consumer（cmd/es-indexer）负责拉取/offset
//     提交/DLQ 路由；本包只负责「一批契约消息 → ES bulk upsert」。这样阶段 6 的
//     存量 backfill job 可以直接 import 本包复用同一套写入逻辑（读 message 表 →
//     转契约 → Writer.Bulk），无需复制 ES 写入代码。
//   - ES doc `_id = MessageID`（= Kafka key），保证 at-least-once 上线下的
//     effectively-once：重复投递走 upsert 幂等，不产生重复文档。
//   - 撤回/删除态**绝不**写入 ES（路线甲：读时回 MySQL join 过滤）。本包只索引
//     正文 + 查询侧鉴权所需可见性字段，与 searchmsg.Message 契约一致。
package esindex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// Writer 把检索契约消息批量幂等写入 ES/OpenSearch。
//
// 实现须满足：
//   - bulk upsert（index 动作，幂等），doc _id = msg.MessageID（幂等 sink）。
//   - 逐项回报 per-item status，便于 consumer 区分 transient(429/5xx 重试) 与
//     permanent(4xx 进 DLQ)，并据「连续成功前缀」推进 offset（C4）。
//   - 中文分词由索引 mapping/analyzer 承担（mapping 配置在 octo-deployment 协调变更）。
type Writer interface {
	// EnsureIndex 幂等创建目标索引（不存在则用内嵌 mapping 创建，已存在则不动）。
	// 供服务启动与 backfill job 复用同一套 mapping bootstrap。
	EnsureIndex(ctx context.Context) error
	// AssertLiveMappingCompatible 启动期 fail-closed 断言：GET 目标索引 live mapping，校验本期
	// 所有新字段路径齐备（payload.mergeForward.msgs.{from,timestamp} / payload.richText.searchText /
	// payloadRaw enabled:false）。缺则返回 error，调用方拒启动（loud crash），不静默向
	// dynamic:strict 索引灌 4xx（§6.4 部署竞态防护）。与 EnsureIndex 存在性幂等互相独立。
	AssertLiveMappingCompatible(ctx context.Context) error
	// Bulk 幂等写入一批 Kafka 契约消息（实时 consumer 路径）。内部转 reader 可读 Doc 后
	// bulk upsert；契约 message_id 非数值（无法对齐 reader long messageId）的条目按
	// **永久错误（4xx 语义，Status=400）**回报，由 consumer 路由 DLQ，绝不静默落 0。
	// 返回与入参等长、顺序对齐的 per-item 结果切片。
	Bulk(ctx context.Context, msgs []searchmsg.Message) ([]BulkItemResult, error)
	// BulkDocs 幂等写入一批已构造好的 reader 可读 Doc（backfill 富化路径用：从原始 MySQL
	// payload 自源填 spaceId/visibles/messageSeq）。与 Bulk 共用同一套 _bulk 写入 + 结果映射。
	BulkDocs(ctx context.Context, docs []Doc) ([]BulkItemResult, error)
	// Close 释放底层 ES 客户端资源。
	Close() error
}

// docConvertBadRequest 是契约消息无法转 Doc（message_id 非数值）时回报的伪 HTTP 状态，
// 复用 400 的 permanent 语义让 consumer 路由 DLQ（与 ES mapping 冲突等永久错误同处置）。
const docConvertBadRequest = 400

// BulkItemResult 是单条文档的写入结果，供 consumer 做 offset / DLQ 决策。
type BulkItemResult struct {
	// MessageID 对应 searchmsg.Message.MessageID（= ES _id）。
	MessageID string
	// OK 为 true 表示该文档已成功 upsert。
	OK bool
	// Status 是底层 ES 返回的 HTTP 状态码（成功为 2xx）。批级失败时为 0。
	Status int
	// Err 携带写入失败的原因（OK=true 时为 nil）。
	Err error
}

// Permanent 报告该条失败是否为永久错误（4xx，除 429）——consumer 据此进 DLQ；
// 否则为 transient（429/5xx/网络），consumer 原地退避重试、不推进 offset。
func (r BulkItemResult) Permanent() bool {
	return isPermanentStatus(r.Status)
}

// isPermanentStatus 判定 HTTP 状态码是否为「永久错误」（毒丸，进 DLQ）。
// 4xx 表示请求本身有问题（如 mapping 冲突、文档格式非法），重试无意义 → permanent。
// 例外：429 Too Many Requests 是限流，属 transient（退避重试）。
// 5xx / 0（网络/批级失败）属 transient。
func isPermanentStatus(status int) bool {
	if status == http.StatusTooManyRequests { // 429
		return false
	}
	return status >= 400 && status < 500
}

// Config 是 Writer 的构造配置。
type Config struct {
	// Addresses 是 OpenSearch/ES 节点地址列表。
	Addresses []string
	// Index 是目标索引名。
	Index string
	// Username/Password 为 HTTP Basic 认证（可空）。
	Username string
	Password string
	// Transport 可选：注入自定义 http.RoundTripper（测试用 mock；生产留空走默认）。
	Transport http.RoundTripper
}

// osWriter 是基于 opensearch-go v3 的 Writer 实现。
type osWriter struct {
	client *opensearchapi.Client
	index  string
}

// NewWriter 构造一个 OpenSearch 写入器。
func NewWriter(cfg Config) (Writer, error) {
	if len(cfg.Addresses) == 0 {
		return nil, errors.New("esindex: at least one OpenSearch address is required")
	}
	if cfg.Index == "" {
		return nil, errors.New("esindex: target index is required")
	}
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: cfg.Addresses,
			Username:  cfg.Username,
			Password:  cfg.Password,
			Transport: cfg.Transport,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("esindex: new opensearch client: %w", err)
	}
	return &osWriter{client: client, index: cfg.Index}, nil
}

// bulkActionLine / bulkDocLine 构成 _bulk NDJSON 的每条两行：动作行 + 文档行。
// 用 index 动作（非 create）实现 upsert：同 _id 重复投递覆盖同一 doc，幂等去重。
type bulkActionLine struct {
	Index struct {
		ID string `json:"_id"`
	} `json:"index"`
}

// Bulk 把一批 Kafka 契约消息转成 reader 可读 Doc 后以 _bulk 幂等写入 ES。
// 返回与 msgs 等长、按 MessageID 对齐的结果。
//
// 错误语义：
//   - 单条转 Doc 失败（message_id 非数值，无法对齐 reader long messageId）→ 该条 **permanent**
//     （Status=400），不进 _bulk，由 consumer 路由 DLQ，绝不静默落 messageId=0（reader 会丢弃 0）。
//   - 批级失败（网络/非 2xx 整体响应）→ 返回 error，且每条结果 OK=false、Status=0（transient，
//     consumer 整批退避重试不推进 offset）。
//   - 批级成功但部分条目失败 → error=nil，逐条 OK/Status/Err 反映 per-item 状态，consumer 据此
//     按「连续成功前缀」推进 offset、permanent 条进 DLQ。
func (w *osWriter) Bulk(ctx context.Context, msgs []searchmsg.Message) ([]BulkItemResult, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	out := make([]BulkItemResult, len(msgs))
	docs := make([]Doc, 0, len(msgs))
	docIdx := make([]int, 0, len(msgs)) // docs[j] 对应 msgs/out 的下标
	for i := range msgs {
		d, err := DocFromMessage(msgs[i])
		if err != nil {
			// 转 Doc 失败属永久数据错误（非数值 message_id）：标 permanent(400) 让 consumer 进 DLQ。
			out[i] = BulkItemResult{MessageID: msgs[i].MessageID, OK: false, Status: docConvertBadRequest, Err: err}
			continue
		}
		docs = append(docs, d)
		docIdx = append(docIdx, i)
	}

	if len(docs) == 0 {
		return out, nil
	}

	docRes, err := w.BulkDocs(ctx, docs)
	if err != nil {
		// 批级失败：把可转条目标 transient（保留转换已永久失败的条目结果不变）。
		// BulkDocs 在编码错误时可能返回 nil 结果切片——此时按 transient 兜底，绝不索引越界。
		for j, idx := range docIdx {
			if j < len(docRes) {
				out[idx] = docRes[j]
			} else {
				out[idx] = BulkItemResult{OK: false, Status: 0, Err: err}
			}
			out[idx].MessageID = msgs[idx].MessageID
		}
		return out, err
	}
	for j, idx := range docIdx {
		out[idx] = docRes[j]
		out[idx].MessageID = msgs[idx].MessageID
	}
	return out, nil
}

// maxBulkBodyBytes 是单次 _bulk 请求体的字节上限（软阈值，留足余量低于 OpenSearch 默认
// http.max_content_length=100MB）。方案 B 后每条 branch-A doc 内嵌 payloadRaw（可达 ~1MB），
// 仅按条数批（如 500 条）会让 _bulk body 远超限 → 整批非 2xx → 被当 transient 无限重试 → 卡死
// ingestion。故按**编码字节**再切子批，每子批独立 _bulk，结果按序拼接，per-item 契约不变。
// 取 50MB（远低于 100MB 默认 + 余量），足以摊薄大 payloadRaw 又不至于子批过多。
const maxBulkBodyBytes = 50 << 20 // 50 MiB

// BulkDocs 把一批 reader 可读 Doc 以 _bulk 幂等写入 ES（backfill 富化路径直接调用）。
// doc _id = 规范化 messageId，与实时 consumer 路径同 _id → 重叠运行幂等去重。
//
// 🔴 按字节切子批（codex 补）：payloadRaw 内嵌后单批体积可能远超 http.max_content_length，
// 整批失败被当 transient 无限重试卡死。故先按 maxBulkBodyBytes 把 docs 切成多个子批，逐子批
// _bulk，结果按入参顺序拼接返回。任一子批批级失败 → 该子批条目标 transient + 返回 error（与
// 原单批语义一致：调用方整批退避重试；下一轮同 _id 幂等覆盖已成功子批，无重复 doc）。
func (w *osWriter) BulkDocs(ctx context.Context, docs []Doc) ([]BulkItemResult, error) {
	if len(docs) == 0 {
		return nil, nil
	}

	out := make([]BulkItemResult, 0, len(docs))
	var firstErr error
	for start := 0; start < len(docs); {
		end := subBatchEnd(docs, start)
		sub := docs[start:end]
		// 🔴 单条 doc 自身就超限（end==start+1 且其编码 > 阈值）：发出去会被 ES 在批级 413 拒绝，
		// 映射成 Status=0(transient) → consumer/backfill 无限重试卡死。改判为 **permanent(413)**，
		// 让调用方把这条毒丸路由 DLQ（不重试）。实时路径已被 producer 1MB guard 拦下，本兜底主要
		// 防 backfill 读到异常大的历史 payloadRaw 行。
		if len(sub) == 1 && encodedDocSize(sub[0]) > maxBulkBodyBytes {
			out = append(out, BulkItemResult{
				MessageID: sub[0].idString(),
				OK:        false,
				Status:    http.StatusRequestEntityTooLarge, // 413 → Permanent()
				Err:       fmt.Errorf("esindex: doc %s encoded size exceeds max bulk body %d bytes (poison, routed to DLQ not retried)", sub[0].idString(), maxBulkBodyBytes),
			})
			start = end
			continue
		}
		res, err := w.bulkOnce(ctx, sub)
		out = append(out, res...)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		start = end
	}
	return out, firstErr
}

// subBatchEnd 返回从 start 起、累计编码字节不超过 maxBulkBodyBytes 的子批结束下标（半开区间）。
// 至少含 1 条（即便单条 >阈值也单独成批——交给 ES 报错走 per-item，不在此死循环）。
func subBatchEnd(docs []Doc, start int) int {
	size := 0
	for i := start; i < len(docs); i++ {
		ds := encodedDocSize(docs[i])
		if i > start && size+ds > maxBulkBodyBytes {
			return i
		}
		size += ds
	}
	return len(docs)
}

// encodedDocSize 估算一条 doc 在 _bulk body 里占的字节（动作行 + 文档行 + 两个换行）。
// marshal 失败时返回 0（该条会在 bulkOnce 编码时再次失败并按 per-item 处理）。
func encodedDocSize(d Doc) int {
	docJSON, err := json.Marshal(d)
	if err != nil {
		return 0
	}
	// 动作行约 ~40 字节（{"index":{"_id":"<id>"}}）+ 文档行 + 2 换行；动作行用 id 长度近似。
	return len(docJSON) + len(d.idString()) + 24 + 2
}

// bulkOnce 执行单次 _bulk（原 BulkDocs 主体），用于 BulkDocs 的按字节子批切分。
func (w *osWriter) bulkOnce(ctx context.Context, docs []Doc) ([]BulkItemResult, error) {
	if len(docs) == 0 {
		return nil, nil
	}

	body, err := encodeBulkBody(docs)
	if err != nil {
		// 编码失败也是批级失败：必须返回与 docs 等长、位置对齐的结果切片（batchFailure），
		// 绝不返回 nil。否则 BulkDocs 的 out 会比 sub 短，上层 Bulk() 按位 docRes[j]↔docIdx[j]
		// 归属时整体错位 → 一条编码失败 doc 可能被贴上另一条的 success → 误判已索引、既不进 DLQ
		// 也搜不到（静默丢消息）。标 transient(Status=0) 让调用方整批退避重试。
		return batchFailure(docs, err), fmt.Errorf("esindex: encode bulk body failed: %w", err)
	}

	resp, err := w.client.Bulk(ctx, opensearchapi.BulkReq{
		Index: w.index,
		Body:  bytes.NewReader(body),
	})
	if err != nil {
		// 批级失败（网络/超时/非 2xx）：全部标 transient，让调用方整批退避重试。
		return batchFailure(docs, err), fmt.Errorf("esindex: bulk request failed: %w", err)
	}

	return mapBulkResults(docs, resp), nil
}

// encodeBulkBody 把 Doc 序列化为 _bulk NDJSON（每条：index 动作行 + 文档行）。
func encodeBulkBody(docs []Doc) ([]byte, error) {
	var buf bytes.Buffer
	for i := range docs {
		id := docs[i].idString()
		var action bulkActionLine
		action.Index.ID = id
		actionJSON, err := json.Marshal(action)
		if err != nil {
			return nil, fmt.Errorf("esindex: marshal bulk action for %s: %w", id, err)
		}
		docJSON, err := json.Marshal(docs[i])
		if err != nil {
			return nil, fmt.Errorf("esindex: marshal doc for %s: %w", id, err)
		}
		buf.Write(actionJSON)
		buf.WriteByte('\n')
		buf.Write(docJSON)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// batchFailure 在批级失败时把每条标为 transient（Status=0, OK=false）。
func batchFailure(docs []Doc, err error) []BulkItemResult {
	out := make([]BulkItemResult, len(docs))
	for i := range docs {
		out[i] = BulkItemResult{MessageID: docs[i].idString(), OK: false, Status: 0, Err: err}
	}
	return out
}

// mapBulkResults 把 _bulk 响应逐项映射回结果切片（顺序与入参一致）。
// _bulk 响应 Items 顺序与请求顺序一致，按下标对齐；防御性校验长度与 _id。
func mapBulkResults(docs []Doc, resp *opensearchapi.BulkResp) []BulkItemResult {
	out := make([]BulkItemResult, len(docs))
	for i := range docs {
		id := docs[i].idString()
		res := BulkItemResult{MessageID: id}
		if i >= len(resp.Items) {
			// 响应条目缺失（理论上不应发生）：标 transient 让整批重试，绝不静默当成功。
			res.OK = false
			res.Status = 0
			res.Err = fmt.Errorf("esindex: missing bulk response item for %s", id)
			out[i] = res
			continue
		}
		item, ok := resp.Items[i]["index"]
		if !ok {
			res.OK = false
			res.Status = 0
			res.Err = fmt.Errorf("esindex: bulk response item for %s has no index action", id)
			out[i] = res
			continue
		}
		res.Status = item.Status
		if item.Status >= 200 && item.Status < 300 {
			res.OK = true
		} else {
			res.OK = false
			if item.Error != nil {
				res.Err = fmt.Errorf("esindex: bulk item %s status=%d type=%s reason=%s",
					id, item.Status, item.Error.Type, item.Error.Reason)
			} else {
				res.Err = fmt.Errorf("esindex: bulk item %s status=%d", id, item.Status)
			}
		}
		out[i] = res
	}
	return out
}

// Close 释放底层 ES 客户端资源。opensearch-go 客户端无显式 Close；预留接口对齐。
func (w *osWriter) Close() error {
	return nil
}
