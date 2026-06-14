package esindex

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// roundTripFunc 把函数适配成 http.RoundTripper，便于 mock OpenSearch 响应。
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// readAll drains a request body or panics (test helper; keeps errcheck happy).
func readAll(rc io.Reader) string {
	b, err := io.ReadAll(rc)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// jsonResp 构造一个 mock HTTP 响应。
func jsonResp(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
	}
}

// newTestWriter 用注入的 transport 建 writer（不连真实 ES）。
func newTestWriter(t *testing.T, rt http.RoundTripper) Writer {
	t.Helper()
	w, err := NewWriter(Config{
		Addresses: []string{"http://opensearch.test:9200"},
		Index:     "octo-message",
		Transport: rt,
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w
}

func msg(id, content string) searchmsg.Message {
	c := content
	return searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     id,
		ChannelID:     "g_1",
		ChannelType:   2,
		FromUID:       "u_1",
		Content:       &c,
		ContentType:   1,
		Source:        searchmsg.SourceETLMessageTable,
	}
}

// TestNewWriter_Validation 缺地址/索引时报错。
func TestNewWriter_Validation(t *testing.T) {
	if _, err := NewWriter(Config{Index: "x"}); err == nil {
		t.Fatalf("expected error when addresses empty")
	}
	if _, err := NewWriter(Config{Addresses: []string{"http://x:9200"}}); err == nil {
		t.Fatalf("expected error when index empty")
	}
}

// TestBulk_AllSuccess 全部 201 → 每条 OK，doc _id = message_id（请求体含正确动作行）。
func TestBulk_AllSuccess(t *testing.T) {
	var gotBody string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotBody = readAll(r.Body)
		return jsonResp(200, `{"took":1,"errors":false,"items":[
			{"index":{"_id":"m1","status":201}},
			{"index":{"_id":"m2","status":200}}
		]}`), nil
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), []searchmsg.Message{msg("m1", "hi"), msg("m2", "世界")})
	if err != nil {
		t.Fatalf("Bulk: %v", err)
	}
	if len(res) != 2 || !res[0].OK || !res[1].OK {
		t.Fatalf("expected both OK, got %+v", res)
	}
	// 校验 NDJSON 动作行带 _id（upsert 幂等键 = message_id）。
	if !strings.Contains(gotBody, `"_id":"m1"`) || !strings.Contains(gotBody, `"_id":"m2"`) {
		t.Fatalf("bulk body missing _id action lines: %s", gotBody)
	}
	// 校验文档行含中文正文（可被 mapping analyzer 分词）。
	if !strings.Contains(gotBody, "世界") {
		t.Fatalf("bulk body missing chinese content: %s", gotBody)
	}
}

// TestBulk_MixedStatuses 混合状态：成功/4xx(permanent)/429(transient)/5xx(transient)。
func TestBulk_MixedStatuses(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"took":1,"errors":true,"items":[
			{"index":{"_id":"ok","status":201}},
			{"index":{"_id":"bad","status":400,"error":{"type":"mapper_parsing_exception","reason":"bad field"}}},
			{"index":{"_id":"throttle","status":429,"error":{"type":"es_rejected","reason":"queue full"}}},
			{"index":{"_id":"srv","status":503,"error":{"type":"unavailable","reason":"node down"}}}
		]}`), nil
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), []searchmsg.Message{
		msg("ok", "a"), msg("bad", "b"), msg("throttle", "c"), msg("srv", "d"),
	})
	if err != nil {
		t.Fatalf("Bulk: %v", err)
	}
	if !res[0].OK {
		t.Fatalf("item 0 should be OK")
	}
	if res[1].OK || !res[1].Permanent() {
		t.Fatalf("item 1 (400) must be permanent failure, got %+v", res[1])
	}
	if res[2].OK || res[2].Permanent() {
		t.Fatalf("item 2 (429) must be transient (not permanent), got %+v", res[2])
	}
	if res[3].OK || res[3].Permanent() {
		t.Fatalf("item 3 (503) must be transient (not permanent), got %+v", res[3])
	}
}

// TestBulk_BatchFailure 批级失败（网络错误）→ 返回 error，每条 transient(Status=0)。
func TestBulk_BatchFailure(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), []searchmsg.Message{msg("m1", "x")})
	if err == nil {
		t.Fatalf("expected batch-level error")
	}
	if len(res) != 1 || res[0].OK || res[0].Permanent() || res[0].Status != 0 {
		t.Fatalf("batch failure items must be transient with status 0, got %+v", res)
	}
}

// TestBulk_Empty 空批 → nil 结果、nil error，不发请求。
func TestBulk_Empty(t *testing.T) {
	called := false
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		called = true
		return jsonResp(200, `{}`), nil
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), nil)
	if err != nil || res != nil {
		t.Fatalf("empty bulk: res=%v err=%v", res, err)
	}
	if called {
		t.Fatalf("empty bulk must not call ES")
	}
}

// TestBulk_Idempotent 同一 message_id 重复出现在两批 → 动作行均用同一 _id（upsert 幂等，
// 由 ES 覆盖同 doc 实现去重；此处验证写入器始终以 message_id 为 _id）。
func TestBulk_Idempotent(t *testing.T) {
	var bodies []string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		bodies = append(bodies, readAll(r.Body))
		return jsonResp(200, `{"took":1,"errors":false,"items":[{"index":{"_id":"dup","status":201}}]}`), nil
	})
	w := newTestWriter(t, rt)
	for i := 0; i < 2; i++ {
		if _, err := w.Bulk(context.Background(), []searchmsg.Message{msg("dup", "v")}); err != nil {
			t.Fatalf("Bulk #%d: %v", i, err)
		}
	}
	for _, b := range bodies {
		var action bulkActionLine
		first := strings.SplitN(b, "\n", 2)[0]
		if err := json.Unmarshal([]byte(first), &action); err != nil {
			t.Fatalf("parse action line: %v", err)
		}
		if action.Index.ID != "dup" {
			t.Fatalf("idempotent key must be message_id 'dup', got %q", action.Index.ID)
		}
	}
}

// TestBulk_MissingResponseItem 响应条目数少于请求 → 缺失条标 transient，绝不静默当成功。
func TestBulk_MissingResponseItem(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"took":1,"errors":false,"items":[{"index":{"_id":"m1","status":201}}]}`), nil
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), []searchmsg.Message{msg("m1", "a"), msg("m2", "b")})
	if err != nil {
		t.Fatalf("Bulk: %v", err)
	}
	if !res[0].OK {
		t.Fatalf("item 0 should be OK")
	}
	if res[1].OK || res[1].Permanent() || res[1].Status != 0 {
		t.Fatalf("missing item must be transient (retry), got %+v", res[1])
	}
}

// osWriterFor 暴露 *osWriter 以便测 EnsureIndex（注入 transport）。
func osWriterFor(t *testing.T, rt http.RoundTripper) *osWriter {
	t.Helper()
	w := newTestWriter(t, rt).(*osWriter)
	return w
}

// TestEnsureIndex_CreatesWhenMissing 索引不存在(404) → 发 PUT 创建，body 为内嵌 mapping。
func TestEnsureIndex_CreatesWhenMissing(t *testing.T) {
	var createBody string
	var createCalled bool
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodHead || (r.Method == http.MethodGet) {
			return jsonResp(404, `{"error":"index_not_found"}`), nil
		}
		if r.Method == http.MethodPut {
			createCalled = true
			createBody = readAll(r.Body)
			return jsonResp(200, `{"acknowledged":true}`), nil
		}
		return jsonResp(200, `{}`), nil
	})
	w := osWriterFor(t, rt)
	if err := w.EnsureIndex(context.Background()); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if !createCalled {
		t.Fatalf("expected index create (PUT) when missing")
	}
	if !strings.Contains(createBody, "ik_max_word") || !strings.Contains(createBody, `"content"`) {
		t.Fatalf("create body must carry embedded mapping/analyzer, got: %s", createBody)
	}
}

// TestEnsureIndex_NoCreateWhenExists 索引已存在(200) → 不创建（不覆盖既有 mapping）。
func TestEnsureIndex_NoCreateWhenExists(t *testing.T) {
	createCalled := false
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPut {
			createCalled = true
		}
		return jsonResp(200, `{}`), nil
	})
	w := osWriterFor(t, rt)
	if err := w.EnsureIndex(context.Background()); err != nil {
		t.Fatalf("EnsureIndex: %v", err)
	}
	if createCalled {
		t.Fatalf("must NOT create/overwrite an existing index")
	}
}

// TestIndexMappingJSON_Valid 内嵌 mapping 是合法 JSON 且含关键字段（防 embed 损坏）。
func TestIndexMappingJSON_Valid(t *testing.T) {
	var m map[string]any
	if err := json.Unmarshal(IndexMappingJSON(), &m); err != nil {
		t.Fatalf("embedded mapping must be valid JSON: %v", err)
	}
	if _, ok := m["mappings"]; !ok {
		t.Fatalf("mapping JSON missing 'mappings'")
	}
}
