package recon

import (
	"context"
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

// TestOSCounter_RawExcludedQuery raw_excluded 计数走 term filter，正常返回。
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
	if !strings.Contains(gotBody, "raw_excluded") || !strings.Contains(gotBody, "created_at") {
		t.Fatalf("raw_excluded query must filter on raw_excluded + created_at: %s", gotBody)
	}
}
