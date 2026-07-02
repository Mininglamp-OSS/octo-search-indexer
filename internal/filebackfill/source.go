package filebackfill

// source.go — OS scroll query 反查 type=8 且 payload.file.content 缺失的 doc（IDX-5 backfill 源）。
//
// 用 OS scroll API 而非 search_after + PIT：scroll 简单够用，一次性 Job 场景不追求实时性。
// query DSL：
//   {
//     "size": <ScrollSize>,
//     "query": {
//       "bool": {
//         "filter": [{"term":{"payload.type":8}}],
//         "must_not": [{"exists":{"field":"payload.file.content"}}]
//       }
//     },
//     "_source": ["messageId","payload.file"]
//   }
// 幂等：`must_not exists content` 自动跳过已抽取 doc，重跑无副作用。

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// sourceDoc 是 scroll 拉出的一条待回填 doc 摘要。
type sourceDoc struct {
	MessageID string
	URL       string
	Name      string
	Extension string
	Size      int64
}

// osScrollSource 用 OS scroll API 遍历待回填 doc。
type osScrollSource struct {
	client     *opensearchapi.Client
	index      string
	size       int
	scrollTTL  time.Duration
	scrollID   string // 首批后由 OS 返回
	exhausted  bool
}

func newOSScrollSource(cfg Config) (*osScrollSource, error) {
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: cfg.ESAddresses,
			Username:  cfg.ESUsername,
			Password:  cfg.ESPassword,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("filebackfill: new opensearch client: %w", err)
	}
	size := cfg.ScrollSize
	if size <= 0 {
		size = 500
	}
	ttl := cfg.ScrollTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if ttl < time.Second {
		// OS scroll TTL 精度到秒；< 1s 会被 formatDuration floor 到 "1s"，
		// 显式在构造期兜底避免运维改 config 后困惑「为啥我设 500ms 没生效」。
		ttl = time.Second
	}
	return &osScrollSource{
		client:    client,
		index:     cfg.ESIndex,
		size:      size,
		scrollTTL: ttl,
	}, nil
}

// Next 拉下一批。首批调 /_search + scroll=<TTL>；后续调 /_search/scroll 用 scroll_id。
// 返回 (docs, io.EOF) 表示遍历完（docs 可能是空）。
func (s *osScrollSource) Next(ctx context.Context) ([]sourceDoc, error) {
	if s.exhausted {
		return nil, io.EOF
	}
	if s.scrollID == "" {
		return s.firstBatch(ctx)
	}
	return s.continueScroll(ctx)
}

func (s *osScrollSource) firstBatch(ctx context.Context) ([]sourceDoc, error) {
	// 用结构体 + json.Marshal 构造 body，避免字符串拼接（对齐 esindex 里 request body 构造 pattern）。
	query := map[string]any{
		"size": s.size,
		"query": map[string]any{
			"bool": map[string]any{
				"filter":   []map[string]any{{"term": map[string]any{"payload.type": 8}}},
				"must_not": []map[string]any{{"exists": map[string]any{"field": "payload.file.content"}}},
			},
		},
		"_source": []string{"messageId", "payload.file"},
	}
	body, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("filebackfill: marshal scroll query: %w", err)
	}
	req := &opensearchapi.SearchReq{
		Indices: []string{s.index},
		Body:    bytes.NewReader(body),
		Params:  opensearchapi.SearchParams{Scroll: s.scrollTTL},
	}
	resp, err := s.client.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("filebackfill: first scroll search: %w", err)
	}
	if resp.ScrollID != nil {
		s.scrollID = *resp.ScrollID
	}
	return s.parseHits(resp.Hits.Hits), nil
}

// continueScroll 用 scroll_id 请求下一批。走裸 HTTP：
// opensearch-go v3.1.0 的 opensearchapi 未提供高层 ScrollReq，
// 但 client.Client 是底层 opensearch.Client 可发裸请求。
func (s *osScrollSource) continueScroll(ctx context.Context) ([]sourceDoc, error) {
	body, err := json.Marshal(map[string]string{
		"scroll":    formatDuration(s.scrollTTL),
		"scroll_id": s.scrollID,
	})
	if err != nil {
		return nil, fmt.Errorf("filebackfill: marshal continue scroll body: %w", err)
	}
	req, err := opensearch.BuildRequest("POST", "/_search/scroll", bytes.NewReader(body), nil, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Client.Perform(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("filebackfill: continue scroll: %w", err)
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // close-on-read: nothing to do with close err
	raw, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("filebackfill: continue scroll read body: %w", readErr)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("filebackfill: continue scroll status %d: %s", resp.StatusCode, string(raw))
	}
	var out struct {
		ScrollID *string `json:"_scroll_id"`
		Hits     struct {
			Hits []opensearchapi.SearchHit `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("filebackfill: parse scroll response: %w", err)
	}
	if out.ScrollID != nil {
		s.scrollID = *out.ScrollID
	}
	hits := s.parseHits(out.Hits.Hits)
	if len(hits) == 0 {
		s.exhausted = true
		return nil, io.EOF
	}
	return hits, nil
}

// parseHits 从 SearchHit slice 里抽 messageId + payload.file 字段。
//
// 🔴 精度铁律：messageId 是 OS mapping 声明的 long（snowflake 19 位 > 2^53），MUST 走
// json.Decoder + UseNumber() 保精度。默认 json.Unmarshal 遇到 interface{}/any 目标会把
// JSON number 解成 float64，snowflake 会被截断成 "1.234e+18"，作为 OS `_id` 打不到 doc。
// 参考 internal/esindex/buildraw.go:22 段落警告 + decodeObjectUseNumber helper。
func (s *osScrollSource) parseHits(hits []opensearchapi.SearchHit) []sourceDoc {
	docs := make([]sourceDoc, 0, len(hits))
	for _, h := range hits {
		var src struct {
			MessageID json.Number `json:"messageId"`
			Payload   struct {
				File struct {
					URL       string      `json:"url"`
					Name      string      `json:"name"`
					Extension string      `json:"extension"`
					Size      json.Number `json:"size"`
				} `json:"file"`
			} `json:"payload"`
		}
		dec := json.NewDecoder(bytes.NewReader(h.Source))
		dec.UseNumber()
		if err := dec.Decode(&src); err != nil {
			continue // 单条解析失败跳过
		}
		msgID := src.MessageID.String()
		if msgID == "" {
			continue
		}
		var size int64
		if src.Payload.File.Size != "" {
			if n, err := src.Payload.File.Size.Int64(); err == nil {
				size = n
			}
		}
		docs = append(docs, sourceDoc{
			MessageID: msgID,
			URL:       src.Payload.File.URL,
			Name:      src.Payload.File.Name,
			Extension: src.Payload.File.Extension,
			Size:      size,
		})
	}
	return docs
}

// Close 清理 scroll 上下文。best-effort，不 block Job 退出。
func (s *osScrollSource) Close(ctx context.Context) error {
	if s.scrollID == "" {
		return nil
	}
	body, err := json.Marshal(map[string][]string{"scroll_id": {s.scrollID}})
	if err != nil {
		return err
	}
	req, err := opensearch.BuildRequest("DELETE", "/_search/scroll", bytes.NewReader(body), nil, nil)
	if err != nil {
		return err
	}
	// Close scroll 是尽力操作（scroll ctx 会自动过期），错误只 log 不阻断。
	if _, perr := s.client.Client.Perform(req.WithContext(ctx)); perr != nil {
		// intentional: 忽略 close 失败（scroll 后 5 min 自动 GC）；不返 err 以免 Runner defer 报误。
		_ = perr
	}
	return nil
}

// formatDuration 把 Duration 转成 OS scroll 参数格式（"5m" / "30s" 而非 Go "5m0s"）。
// < 1s 会被 floor 到 "1s"，避免误配置让 OS 视 scroll TTL 为 0 → scroll context 立即过期。
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return "1s"
	}
	if d >= time.Minute {
		return fmt.Sprintf("%dm", int(d/time.Minute))
	}
	return fmt.Sprintf("%ds", int(d/time.Second))
}
