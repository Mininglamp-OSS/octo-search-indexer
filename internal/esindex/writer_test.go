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
			{"index":{"_id":"101","status":201}},
			{"index":{"_id":"102","status":200}}
		]}`), nil
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), []searchmsg.Message{msg("101", "hi"), msg("102", "世界")})
	if err != nil {
		t.Fatalf("Bulk: %v", err)
	}
	if len(res) != 2 || !res[0].OK || !res[1].OK {
		t.Fatalf("expected both OK, got %+v", res)
	}
	// 校验 NDJSON 动作行带 _id（upsert 幂等键 = message_id）。
	if !strings.Contains(gotBody, `"_id":"101"`) || !strings.Contains(gotBody, `"_id":"102"`) {
		t.Fatalf("bulk body missing _id action lines: %s", gotBody)
	}
	// 校验文档行：正文嵌套在 payload.text.content（reader 契约），messageId 为数值 long。
	if !strings.Contains(gotBody, `"payload"`) || !strings.Contains(gotBody, "世界") {
		t.Fatalf("bulk body missing nested payload content: %s", gotBody)
	}
	if !strings.Contains(gotBody, `"messageId":101`) {
		t.Fatalf("bulk body must carry numeric messageId (reader long): %s", gotBody)
	}
}

// TestBulk_MixedStatuses 混合状态：成功/4xx(permanent)/429(transient)/5xx(transient)。
func TestBulk_MixedStatuses(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"took":1,"errors":true,"items":[
			{"index":{"_id":"201","status":201}},
			{"index":{"_id":"400","status":400,"error":{"type":"mapper_parsing_exception","reason":"bad field"}}},
			{"index":{"_id":"429","status":429,"error":{"type":"es_rejected","reason":"queue full"}}},
			{"index":{"_id":"503","status":503,"error":{"type":"unavailable","reason":"node down"}}}
		]}`), nil
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), []searchmsg.Message{
		msg("201", "a"), msg("400", "b"), msg("429", "c"), msg("503", "d"),
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
	res, err := w.Bulk(context.Background(), []searchmsg.Message{msg("301", "x")})
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
		return jsonResp(200, `{"took":1,"errors":false,"items":[{"index":{"_id":"555","status":201}}]}`), nil
	})
	w := newTestWriter(t, rt)
	for i := 0; i < 2; i++ {
		if _, err := w.Bulk(context.Background(), []searchmsg.Message{msg("555", "v")}); err != nil {
			t.Fatalf("Bulk #%d: %v", i, err)
		}
	}
	for _, b := range bodies {
		var action bulkActionLine
		first := strings.SplitN(b, "\n", 2)[0]
		if err := json.Unmarshal([]byte(first), &action); err != nil {
			t.Fatalf("parse action line: %v", err)
		}
		if action.Index.ID != "555" {
			t.Fatalf("idempotent key must be normalized messageId '555', got %q", action.Index.ID)
		}
	}
}

// TestBulk_MissingResponseItem 响应条目数少于请求 → 缺失条标 transient，绝不静默当成功。
func TestBulk_MissingResponseItem(t *testing.T) {
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"took":1,"errors":false,"items":[{"index":{"_id":"701","status":201}}]}`), nil
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), []searchmsg.Message{msg("701", "a"), msg("702", "b")})
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

// TestBulk_NonNumericIDIsPermanentNoES 非数值 message_id → 该条 permanent(400)，
// 且仍把同批可转条目正常写 ES（混批不被毒丸拖垮）。
func TestBulk_NonNumericIDIsPermanentNoES(t *testing.T) {
	var sentBody string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sentBody = readAll(r.Body)
		// ES 只应收到 1 条（合法的 808）。
		return jsonResp(200, `{"took":1,"errors":false,"items":[{"index":{"_id":"808","status":201}}]}`), nil
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), []searchmsg.Message{msg("oops", "x"), msg("808", "y")})
	if err != nil {
		t.Fatalf("Bulk: %v", err)
	}
	if res[0].OK || !res[0].Permanent() || res[0].Status != 400 {
		t.Fatalf("non-numeric id must be permanent(400), got %+v", res[0])
	}
	if !res[1].OK {
		t.Fatalf("valid sibling must still index, got %+v", res[1])
	}
	// 毒丸不得出现在发往 ES 的 NDJSON 里。
	if strings.Contains(sentBody, "oops") {
		t.Fatalf("non-numeric id must NOT be sent to ES: %s", sentBody)
	}
	if !strings.Contains(sentBody, `"_id":"808"`) {
		t.Fatalf("valid doc must be sent with normalized _id: %s", sentBody)
	}
}

// TestBulk_AllNonNumericNoESCall 整批都非数值 → 不发任何 ES 请求，全 permanent。
func TestBulk_AllNonNumericNoESCall(t *testing.T) {
	called := false
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		called = true
		return jsonResp(200, `{}`), nil
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), []searchmsg.Message{msg("a", "x"), msg("b", "y")})
	if err != nil {
		t.Fatalf("Bulk: %v", err)
	}
	if called {
		t.Fatalf("no convertible docs must mean no ES call")
	}
	for i := range res {
		if res[i].OK || !res[i].Permanent() {
			t.Fatalf("all non-numeric must be permanent: %+v", res[i])
		}
	}
}

// TestBulkDocs_SplitsByByteSize 🔴 §4.4（codex 补）：payloadRaw 内嵌后，按字节切子批，避免单次
// _bulk body 超 http.max_content_length 被当 transient 无限重试卡死。构造多条大 payloadRaw doc，
// 断言 BulkDocs 拆成多次 _bulk 请求，且 per-item 结果按入参顺序齐全。
func TestBulkDocs_SplitsByByteSize(t *testing.T) {
	bulkCalls := 0
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := readAll(r.Body)
		// 数请求里的动作行条数（每条一行 {"index":...}），按数生成等量成功 items。
		n := strings.Count(body, `{"index":`)
		bulkCalls++
		var sb strings.Builder
		sb.WriteString(`{"took":1,"errors":false,"items":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(`{"index":{"status":201}}`)
		}
		sb.WriteString(`]}`)
		return jsonResp(200, sb.String()), nil
	})
	w := osWriterFor(t, rt)

	// 每条 doc 带 ~20MB payloadRaw → 3 条 = ~60MB > 50MB 阈值 → 至少切 2 个子批。
	big := make([]byte, 20<<20)
	for i := range big {
		big[i] = 'x'
	}
	rawBig := mustRawObject(big)
	docs := make([]Doc, 3)
	for i := range docs {
		docs[i] = Doc{MessageID: int64(1000 + i), ChannelID: "g", ChannelType: 2, PayloadRaw: rawBig}
	}
	res, err := w.BulkDocs(context.Background(), docs)
	if err != nil {
		t.Fatalf("BulkDocs: %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 per-item results in input order, got %d", len(res))
	}
	for i := range res {
		if !res[i].OK {
			t.Fatalf("doc %d expected OK, got %+v", i, res[i])
		}
	}
	if bulkCalls < 2 {
		t.Fatalf("oversized total must be split into >=2 _bulk requests, got %d", bulkCalls)
	}
}

// TestBulkDocs_SubBatchEncodeFailureKeepsAlignment 🔴 (返工 R1)：子批 encodeBulkBody 失败时，
// bulkOnce 必须返回与子批等长、位置对齐的 batchFailure，绝不返回 nil。否则 BulkDocs 的 out
// 会比 docs 短，上层 Bulk() 按位 docRes[j]↔docIdx[j] 归属时整体错位 → 一条编码失败 doc 被贴上
// 另一条的 success → 误判已索引、既不进 DLQ 也搜不到（静默丢消息）。
//
// 构造：doc0 大且合法（独占第一子批，成功）；doc1 大且合法 + doc2 非法 payloadRaw（同处第二
// 子批，encode 失败）。断言结果切片长度与入参对齐、失败条目按入参顺序正确归属、且失败子批不打 ES。
func TestBulkDocs_SubBatchEncodeFailureKeepsAlignment(t *testing.T) {
	bulkCalls := 0
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body := readAll(r.Body)
		n := strings.Count(body, `{"index":`)
		bulkCalls++
		var sb strings.Builder
		sb.WriteString(`{"took":1,"errors":false,"items":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(`{"index":{"status":201}}`)
		}
		sb.WriteString(`]}`)
		return jsonResp(200, sb.String()), nil
	})
	w := osWriterFor(t, rt)

	// doc0/doc1 各 ~30MB 合法 → 二者相加 >50MB 阈值 → doc0 独占第一子批。
	big := make([]byte, 30<<20)
	for i := range big {
		big[i] = 'x'
	}
	rawBig := mustRawObject(big)
	// doc2 携带非法 JSON payloadRaw → json.Marshal 失败 → encodeBulkBody 失败；
	// encodedDocSize 对其返回 0，故它落在 doc1 所在的第二子批里，使该子批整体编码失败。
	docs := []Doc{
		{MessageID: 1001, ChannelID: "g", ChannelType: 2, PayloadRaw: rawBig},
		{MessageID: 1002, ChannelID: "g", ChannelType: 2, PayloadRaw: rawBig},
		{MessageID: 1003, ChannelID: "g", ChannelType: 2, PayloadRaw: json.RawMessage([]byte("{not valid json"))},
	}

	res, err := w.BulkDocs(context.Background(), docs)
	if err == nil {
		t.Fatalf("expected a batch error from the encode-failing sub-batch, got nil")
	}
	// 核心回归守卫：结果切片长度必须与入参对齐（修复前会比 docs 短 → 上层错位丢消息）。
	if len(res) != len(docs) {
		t.Fatalf("result slice length must align with input (regression: encode-fail returned nil); got %d want %d", len(res), len(docs))
	}
	// 失败条目正确归属：每条结果的 MessageID 必须对应同下标的入参 doc。
	for i := range docs {
		if res[i].MessageID != docs[i].idString() {
			t.Fatalf("result[%d] misattributed: MessageID=%q want %q", i, res[i].MessageID, docs[i].idString())
		}
	}
	// doc0 成功（独占第一子批，已打 ES）。
	if !res[0].OK {
		t.Fatalf("doc0 expected OK, got %+v", res[0])
	}
	// doc1/doc2 同处编码失败子批 → 全部 transient(Status=0, OK=false)，让调用方整批退避重试。
	for _, i := range []int{1, 2} {
		if res[i].OK || res[i].Status != 0 || res[i].Err == nil {
			t.Fatalf("doc%d expected transient failure (OK=false, Status=0, Err!=nil), got %+v", i, res[i])
		}
		if res[i].Permanent() {
			t.Fatalf("doc%d encode-failure must be transient, not permanent", i)
		}
	}
	// 第二子批在 encode 阶段失败，绝不应打到 ES（只有 doc0 那一次成功请求）。
	if bulkCalls != 1 {
		t.Fatalf("encode-failing sub-batch must not hit ES; expected exactly 1 bulk call (doc0), got %d", bulkCalls)
	}
}

// mustRawObject 把一段大字节塞进一个合法 JSON 对象的字段值（用于构造大 payloadRaw）。
func mustRawObject(filler []byte) []byte {
	// {"type":2,"blob":"<filler>"}
	b := make([]byte, 0, len(filler)+32)
	b = append(b, []byte(`{"type":2,"blob":"`)...)
	b = append(b, filler...)
	b = append(b, []byte(`"}`)...)
	return b
}

// TestBulkDocs_SingleOversizedDocIsPermanent §4.4（codex P2 补）：单条 doc 编码自身超 _bulk 上限
// → 不发 ES、直接判 permanent(413)，让 consumer/backfill 路由 DLQ 而非无限 transient 重试。
func TestBulkDocs_SingleOversizedDocIsPermanent(t *testing.T) {
	bulkCalls := 0
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		bulkCalls++
		return jsonResp(200, `{"took":1,"errors":false,"items":[{"index":{"status":201}}]}`), nil
	})
	w := osWriterFor(t, rt)
	// 一条 doc 的 payloadRaw 超过 maxBulkBodyBytes 自身。
	huge := make([]byte, maxBulkBodyBytes+1024)
	for i := range huge {
		huge[i] = 'x'
	}
	docs := []Doc{{MessageID: 1, ChannelID: "g", ChannelType: 2, PayloadRaw: mustRawObject(huge)}}
	res, err := w.BulkDocs(context.Background(), docs)
	if err != nil {
		t.Fatalf("BulkDocs must not return a batch error for a poison doc (it's per-item permanent): %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 result, got %d", len(res))
	}
	if res[0].OK || !res[0].Permanent() {
		t.Fatalf("oversized single doc must be permanent (DLQ-routed), got OK=%v status=%d", res[0].OK, res[0].Status)
	}
	if bulkCalls != 0 {
		t.Fatalf("oversized single doc must NOT hit ES (would 413), got %d bulk calls", bulkCalls)
	}
}
