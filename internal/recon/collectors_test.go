package recon

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func osCounter(t *testing.T, rt http.RoundTripper) *OSCounter {
	t.Helper()
	c, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{"http://os.test:9200"}, Transport: rt},
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return NewOSCounter(c, "octo-message")
}

func resp(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}

// TestOSCounter_CleanCount 全分片成功 → 返回 count。
func TestOSCounter_CleanCount(t *testing.T) {
	c := osCounter(t, rtFunc(func(*http.Request) (*http.Response, error) {
		return resp(200, `{"count":42,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`), nil
	}))
	n, err := c.CountDocs(context.Background(), 0, 100)
	if err != nil || n != 42 {
		t.Fatalf("want 42,nil got %d,%v", n, err)
	}
}

// TestOSCounter_CountDocsExcludesVirtual CountDocs 必须排除 virtual=true 的富文本虚拟子文档：
// 生成的 _count 查询体须含 must_not {term:{virtual:true}}（同时保留 createdAt range filter）。
// 否则一条含 N 图的富文本会让 ESDocs 虚高 N → reconcile gate 误报不健康。
func TestOSCounter_CountDocsExcludesVirtual(t *testing.T) {
	var gotBody string
	c := osCounter(t, rtFunc(func(r *http.Request) (*http.Response, error) {
		b, rerr := io.ReadAll(r.Body)
		if rerr != nil {
			t.Fatalf("read body: %v", rerr)
		}
		gotBody = string(b)
		return resp(200, `{"count":5,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`), nil
	}))
	if _, err := c.CountDocs(context.Background(), 0, 100); err != nil {
		t.Fatalf("CountDocs: %v", err)
	}
	if !strings.Contains(gotBody, "must_not") || !strings.Contains(gotBody, "virtual") {
		t.Fatalf("CountDocs query must exclude virtual via must_not term virtual=true: %s", gotBody)
	}
	if !strings.Contains(gotBody, "createdAt") {
		t.Fatalf("CountDocs query must still range-filter createdAt: %s", gotBody)
	}
	// 结构断言：virtual term 必须落在 must_not 子句里（而非误放进 filter）。
	var parsed struct {
		Query struct {
			Bool struct {
				MustNot []map[string]any `json:"must_not"`
			} `json:"bool"`
		} `json:"query"`
	}
	if err := json.Unmarshal([]byte(gotBody), &parsed); err != nil {
		t.Fatalf("parse query body: %v", err)
	}
	if len(parsed.Query.Bool.MustNot) != 1 {
		t.Fatalf("want exactly 1 must_not clause (virtual term), got %d: %s", len(parsed.Query.Bool.MustNot), gotBody)
	}
	term, _ := parsed.Query.Bool.MustNot[0]["term"].(map[string]any)
	if term == nil || term["virtual"] != true {
		t.Fatalf("must_not clause must be {term:{virtual:true}}, got %v", parsed.Query.Bool.MustNot[0])
	}
}

// TestOSCounter_PartialShardFailure 🔴 gate 安全：HTTP 200 但有分片失败 → 报错，不返回可疑计数。
func TestOSCounter_PartialShardFailure(t *testing.T) {
	c := osCounter(t, rtFunc(func(*http.Request) (*http.Response, error) {
		return resp(200, `{"count":42,"_shards":{"total":5,"successful":4,"skipped":0,"failed":1}}`), nil
	}))
	if _, err := c.CountDocs(context.Background(), 0, 100); err == nil {
		t.Fatalf("partial shard failure must error (count unreliable), got nil")
	}
}

// TestOSCounter_IncompleteShards successful<total（无 failed 计数）也视为不可信。
func TestOSCounter_IncompleteShards(t *testing.T) {
	c := osCounter(t, rtFunc(func(*http.Request) (*http.Response, error) {
		return resp(200, `{"count":10,"_shards":{"total":3,"successful":2,"skipped":0,"failed":0}}`), nil
	}))
	if _, err := c.CountDocs(context.Background(), 0, 100); err == nil {
		t.Fatalf("incomplete shards (successful<total) must error")
	}
}

// TestOSCounter_RawExcludedQuery rawExcluded 计数走 term filter，正常返回。
func TestOSCounter_RawExcludedQuery(t *testing.T) {
	var gotBody string
	c := osCounter(t, rtFunc(func(r *http.Request) (*http.Response, error) {
		b, rerr := io.ReadAll(r.Body)
		if rerr != nil {
			t.Fatalf("read body: %v", rerr)
		}
		gotBody = string(b)
		return resp(200, `{"count":7,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`), nil
	}))
	n, err := c.CountRawExcluded(context.Background(), 0, 100)
	if err != nil || n != 7 {
		t.Fatalf("want 7,nil got %d,%v", n, err)
	}
	if !strings.Contains(gotBody, "rawExcluded") || !strings.Contains(gotBody, "createdAt") {
		t.Fatalf("rawExcluded query must filter on rawExcluded + createdAt: %s", gotBody)
	}
}
