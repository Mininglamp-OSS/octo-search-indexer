package esindex

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// seqTransport 按请求方法路由到一个可变的响应序列，用于模拟并发 TOCTOU 竞态。
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

// alreadyExistsBody 是 OpenSearch 对「索引已存在」返回的 400 错误体。
const alreadyExistsBody = `{"error":{"root_cause":[{"type":"resource_already_exists_exception","reason":"index [octo-message] already exists","index":"octo-message"}],"type":"resource_already_exists_exception","reason":"index [octo-message] already exists","index":"octo-message"},"status":400}`

// TestEnsureIndex_TOCTOURaceIsIdempotent 🔴 P1 回归：并发竞态路径
// Exists→404 → Create→400(resource_already_exists_exception) 必须**成功**（视为已就绪），
// 不得报错/退出（杜绝抢输副本 CrashLoopBackOff）。
func TestEnsureIndex_TOCTOURaceIsIdempotent(t *testing.T) {
	rt := &seqTransport{
		existsStatuses: []int{http.StatusNotFound}, // 首次探测：不存在
		createStatus:   http.StatusBadRequest,      // 抢输：已存在 400
		createBody:     alreadyExistsBody,
	}
	w := ensureWriter(t, rt)
	if err := w.EnsureIndex(context.Background()); err != nil {
		t.Fatalf("TOCTOU race must be idempotent (no error), got %v", err)
	}
	if rt.putCalls != 1 {
		t.Fatalf("expected exactly 1 Create attempt, got %d", rt.putCalls)
	}
}

// TestEnsureIndex_RaceFallbackReCheck Create 返回非 already-exists 形态的错误，但兜底 re-check
// Exists==200（另一副本刚建好）→ 仍视为成功。
func TestEnsureIndex_RaceFallbackReCheck(t *testing.T) {
	rt := &seqTransport{
		// 首次 HEAD 404 → Create 失败（非典型错误体）→ 兜底 HEAD 返回 200。
		existsStatuses: []int{http.StatusNotFound, http.StatusOK},
		createStatus:   http.StatusBadRequest,
		createBody:     `{"error":{"type":"some_other_error","reason":"weird"},"status":400}`,
	}
	w := ensureWriter(t, rt)
	if err := w.EnsureIndex(context.Background()); err != nil {
		t.Fatalf("fallback re-check (Exists==200) must succeed, got %v", err)
	}
	if rt.headCalls < 2 {
		t.Fatalf("expected a fallback Exists re-check (>=2 HEAD), got %d", rt.headCalls)
	}
}

// TestEnsureIndex_RealCreateErrorPropagates Create 真失败（如 5xx）且索引确未就绪 → 报错冒泡
// （不能把所有 Create 失败都吞掉，否则真问题被掩盖）。
func TestEnsureIndex_RealCreateErrorPropagates(t *testing.T) {
	rt := &seqTransport{
		existsStatuses: []int{http.StatusNotFound, http.StatusNotFound}, // 始终不存在
		createStatus:   http.StatusInternalServerError,
		createBody:     `{"error":{"type":"internal","reason":"boom"},"status":500}`,
	}
	w := ensureWriter(t, rt)
	if err := w.EnsureIndex(context.Background()); err == nil {
		t.Fatalf("a genuine create failure (5xx, index still absent) must propagate")
	}
}

// TestEnsureIndex_HTTP500BodyClaims400Propagates 🔴 P1 加固：代理返回真实 HTTP 500，但响应体
// 谎称 status:400 + already-exists type，索引仍缺失 → 必须报错（只信传输层状态码，不信 body status）。
func TestEnsureIndex_HTTP500BodyClaims400Propagates(t *testing.T) {
	rt := &seqTransport{
		existsStatuses: []int{http.StatusNotFound, http.StatusNotFound},
		createStatus:   http.StatusInternalServerError, // 真实 HTTP 500
		createBody:     `{"error":{"root_cause":[{"type":"resource_already_exists_exception","reason":"x"}],"type":"resource_already_exists_exception","reason":"x"},"status":400}`,
	}
	w := ensureWriter(t, rt)
	if err := w.EnsureIndex(context.Background()); err == nil {
		t.Fatalf("HTTP 500 with body claiming status:400 + already-exists, index absent → must propagate")
	}
}
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

// TestIsAlreadyExists_Only400IsAuthoritative 仅 HTTP 400 + already-exists 信号才权威判已存在；
// 非 400（即便文本含 token）不在此判成功——避免真失败被掩盖（交 existsNow 兜底）。
func TestIsAlreadyExists_Only400IsAuthoritative(t *testing.T) {
	// 无状态/非 400 的纯文本错误：不判已存在。
	if isAlreadyExists(nil, &textError{s: "create failed: resource_already_exists_exception index [x]"}) {
		t.Fatalf("non-400 text containing token must NOT be treated as already-exists")
	}
	if isAlreadyExists(nil, &textError{s: "some unrelated error"}) {
		t.Fatalf("unrelated error must not be treated as already-exists")
	}
}

// TestEnsureIndex_5xxWithTokenStillAbsentPropagates 🔴 P1 加固回归：Create 返回 5xx 且错误文本
// 恰含 resource_already_exists_exception，但索引仍缺失（兜底 Exists 仍 404）→ 必须报错，不掩盖。
func TestEnsureIndex_5xxWithTokenStillAbsentPropagates(t *testing.T) {
	rt := &seqTransport{
		existsStatuses: []int{http.StatusNotFound, http.StatusNotFound}, // 始终不存在
		createStatus:   http.StatusInternalServerError,
		// 5xx 错误体文本恰含 token（代理/封装场景），但 status=500。
		createBody: `{"error":{"type":"internal_proxy_error","reason":"upstream said resource_already_exists_exception but failed"},"status":500}`,
	}
	w := ensureWriter(t, rt)
	if err := w.EnsureIndex(context.Background()); err == nil {
		t.Fatalf("5xx create error with index still absent must propagate, even if text contains the token")
	}
}

type textError struct{ s string }

func (e *textError) Error() string { return e.s }
