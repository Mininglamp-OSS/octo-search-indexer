package esindex

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// seqTransport 按请求方法路由到一个可变的响应序列。
type seqTransport struct {
	// existsResponses 是对 HEAD（Exists）的连续应答状态码队列；用尽后取最后一个。
	existsStatuses []int
	existsIdx      int
	// createStatus/createBody 是对 PUT（Create）的应答。
	createStatus int
	createBody   string

	headCalls int
	putCalls  int
}

func (s *seqTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	switch r.Method {
	case http.MethodHead:
		s.headCalls++
		st := s.existsStatuses[len(s.existsStatuses)-1]
		if s.existsIdx < len(s.existsStatuses) {
			st = s.existsStatuses[s.existsIdx]
			s.existsIdx++
		}
		body := ""
		if st == http.StatusNotFound {
			body = `{"error":"index_not_found"}`
		}
		return mkResp(st, body), nil
	case http.MethodPut:
		s.putCalls++
		return mkResp(s.createStatus, s.createBody), nil
	default:
		return mkResp(200, "{}"), nil
	}
}

func mkResp(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}

func ensureWriter(t *testing.T, rt http.RoundTripper) *osWriter {
	t.Helper()
	w, err := NewWriter(Config{Addresses: []string{"http://os.test:9200"}, Index: "octo-message", Transport: rt})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w.(*osWriter)
}

// TestEnsureIndex_ExistsShortCircuits Exists==200 → 放行，且绝不发 PUT（不再自动创建）。
func TestEnsureIndex_ExistsShortCircuits(t *testing.T) {
	rt := &seqTransport{existsStatuses: []int{http.StatusOK}, createStatus: 200}
	w := ensureWriter(t, rt)
	if err := w.EnsureIndex(context.Background()); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if rt.putCalls != 0 {
		t.Fatalf("must not Create when index exists, got %d PUT", rt.putCalls)
	}
}

// TestEnsureIndex_MissingFailsFast Exists==404 → 拒启动报错，绝不发 PUT 自动创建；
// 错误信息含 "does not exist" 与 "refusing to start"（issue #29 fail-fast）。
func TestEnsureIndex_MissingFailsFast(t *testing.T) {
	rt := &seqTransport{existsStatuses: []int{http.StatusNotFound}, createStatus: 200}
	w := ensureWriter(t, rt)
	err := w.EnsureIndex(context.Background())
	if err == nil {
		t.Fatalf("missing index (404) must fail fast, got nil error")
	}
	if rt.putCalls != 0 {
		t.Fatalf("must not auto-create on 404, got %d PUT", rt.putCalls)
	}
	if !strings.Contains(err.Error(), "does not exist") || !strings.Contains(err.Error(), "refusing to start") {
		t.Fatalf("error must mention the missing index and refusal, got %v", err)
	}
}

// TestEnsureIndex_5xxPropagates Exists 返回 5xx（如鉴权/集群异常）→ 报错冒泡，绝不放行也不创建。
func TestEnsureIndex_5xxPropagates(t *testing.T) {
	rt := &seqTransport{existsStatuses: []int{http.StatusInternalServerError}, createStatus: 200}
	w := ensureWriter(t, rt)
	if err := w.EnsureIndex(context.Background()); err == nil {
		t.Fatalf("a 5xx exists-check must propagate, got nil error")
	}
	if rt.putCalls != 0 {
		t.Fatalf("must not Create on 5xx, got %d PUT", rt.putCalls)
	}
}
