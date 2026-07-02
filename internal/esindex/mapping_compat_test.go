package esindex

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// mappingTransport 模拟 GET <index>/_mapping，返回注入的 live mapping body。
type mappingTransport struct {
	body string
}

func (m *mappingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "_mapping") {
		return mkResp(200, m.body), nil
	}
	return mkResp(200, "{}"), nil
}

func mappingWriter(t *testing.T, rt http.RoundTripper) *osWriter {
	t.Helper()
	w, err := NewWriter(Config{Addresses: []string{"http://os.test:9200"}, Index: "octo-message", Transport: rt})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	return w.(*osWriter)
}

// liveMappingBody 包装 properties 成 GET _mapping 的应答形态。
func liveMappingBody(propsJSON string) string {
	return `{"octo-message":{"mappings":{"dynamic":"strict","properties":` + propsJSON + `}}}`
}

// TestMappingCompat_FullEmbeddedMappingPasses 用内嵌规范 mapping 当 live mapping → 断言通过
// （本期所有新字段路径齐备 + payloadRaw enabled:false）。
func TestMappingCompat_FullEmbeddedMappingPasses(t *testing.T) {
	var embedded struct {
		Mappings struct {
			Properties json.RawMessage `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal(IndexMappingJSON(), &embedded); err != nil {
		t.Fatalf("parse embedded mapping: %v", err)
	}
	rt := &mappingTransport{body: liveMappingBody(string(embedded.Mappings.Properties))}
	w := mappingWriter(t, rt)
	if err := w.AssertLiveMappingCompatible(context.Background()); err != nil {
		t.Fatalf("full embedded mapping must pass compat assertion, got %v", err)
	}
}

// TestMappingCompat_MissingPayloadRawFails 🔴 §9 S7：live mapping 缺 payloadRaw → 断言失败
// （调用方据此拒启动，不静默向 dynamic:strict 灌 4xx）。
func TestMappingCompat_MissingPayloadRawFails(t *testing.T) {
	// 含 mergeForward.from/timestamp + richText，但**无** payloadRaw。
	props := `{
		"messageId":{"type":"long"},
		"payload":{"type":"object","properties":{
			"type":{"type":"integer"},
			"mergeForward":{"type":"object","properties":{"msgs":{"type":"object","properties":{
				"from":{"type":"keyword"},"timestamp":{"type":"date","format":"epoch_second"}}}}},
			"richText":{"type":"object","properties":{"searchText":{"type":"text"}}}
		}}
	}`
	rt := &mappingTransport{body: liveMappingBody(props)}
	w := mappingWriter(t, rt)
	err := w.AssertLiveMappingCompatible(context.Background())
	if err == nil {
		t.Fatal("missing payloadRaw must FAIL the compat assertion (refuse to start)")
	}
	if !strings.Contains(err.Error(), "payloadRaw") {
		t.Fatalf("error must name payloadRaw, got %v", err)
	}
}

// TestMappingCompat_MissingMergeForwardFromFails live mapping 缺 mergeForward.msgs.from → 失败。
func TestMappingCompat_MissingMergeForwardFromFails(t *testing.T) {
	props := `{
		"messageId":{"type":"long"},
		"payload":{"type":"object","properties":{
			"type":{"type":"integer"},
			"mergeForward":{"type":"object","properties":{"msgs":{"type":"object","properties":{
				"timestamp":{"type":"date","format":"epoch_second"}}}}},
			"richText":{"type":"object","properties":{"searchText":{"type":"text"}}}
		}},
		"payloadRaw":{"type":"object","enabled":false}
	}`
	rt := &mappingTransport{body: liveMappingBody(props)}
	w := mappingWriter(t, rt)
	err := w.AssertLiveMappingCompatible(context.Background())
	if err == nil || !strings.Contains(err.Error(), "payload.mergeForward.msgs.from") {
		t.Fatalf("missing mergeForward.msgs.from must fail naming the path, got %v", err)
	}
}

// TestMappingCompat_PayloadRawMustBeDisabled payloadRaw 若被声明为普通 object（enabled 非 false）
// → 视为缺失（必须是 enabled:false BLOB 才不会让任意子键触发 dynamic:strict 4xx）。
func TestMappingCompat_PayloadRawMustBeDisabled(t *testing.T) {
	props := `{
		"messageId":{"type":"long"},
		"payload":{"type":"object","properties":{
			"type":{"type":"integer"},
			"mergeForward":{"type":"object","properties":{"msgs":{"type":"object","properties":{
				"from":{"type":"keyword"},"timestamp":{"type":"date","format":"epoch_second"}}}}},
			"richText":{"type":"object","properties":{"searchText":{"type":"text"}}}
		}},
		"payloadRaw":{"type":"object"}
	}`
	rt := &mappingTransport{body: liveMappingBody(props)}
	w := mappingWriter(t, rt)
	if err := w.AssertLiveMappingCompatible(context.Background()); err == nil {
		t.Fatal("payloadRaw declared as an enabled object must FAIL (must be enabled:false BLOB)")
	}
}

// TestMappingCompat_MissingFileContentFails v1.12：live mapping 缺 payload.file.content →
// 断言失败（保证 file-extractor 写入前 live mapping 已升级）。
func TestMappingCompat_MissingFileContentFails(t *testing.T) {
	// 完整 v1.11 mapping（含 payloadRaw + mergeForward + richText + virtual/subSeq）+
	// payload.file 只声明 v1.11 之前 5 字段，**缺** content + contentMeta。
	props := `{
		"messageId":{"type":"long"},
		"parentMessageId":{"type":"long"},
		"parentPayloadType":{"type":"integer"},
		"virtual":{"type":"boolean"},
		"subSeq":{"type":"integer"},
		"payload":{"type":"object","properties":{
			"type":{"type":"integer"},
			"file":{"type":"object","properties":{
				"url":{"type":"keyword"},"name":{"type":"text"},"caption":{"type":"text"},
				"size":{"type":"long"},"extension":{"type":"keyword"}}},
			"mergeForward":{"type":"object","properties":{"msgs":{"type":"object","properties":{
				"from":{"type":"keyword"},"timestamp":{"type":"date","format":"epoch_second"}}}}},
			"richText":{"type":"object","properties":{"searchText":{"type":"text"}}}
		}},
		"payloadRaw":{"type":"object","enabled":false}
	}`
	rt := &mappingTransport{body: liveMappingBody(props)}
	w := mappingWriter(t, rt)
	err := w.AssertLiveMappingCompatible(context.Background())
	if err == nil || !strings.Contains(err.Error(), "payload.file.content") {
		t.Fatalf("missing payload.file.content must fail naming the path, got %v", err)
	}
}

// TestMappingCompat_MissingFileContentMetaFails v1.12：live mapping 有 content 但缺 contentMeta.extractedAt
// → 断言失败（保证 contentMeta object 已声明 properties，file-extractor 才能写 extractedAt/extractor/etc.）。
func TestMappingCompat_MissingFileContentMetaFails(t *testing.T) {
	props := `{
		"messageId":{"type":"long"},
		"parentMessageId":{"type":"long"},
		"parentPayloadType":{"type":"integer"},
		"virtual":{"type":"boolean"},
		"subSeq":{"type":"integer"},
		"payload":{"type":"object","properties":{
			"type":{"type":"integer"},
			"file":{"type":"object","properties":{
				"url":{"type":"keyword"},"name":{"type":"text"},"caption":{"type":"text"},
				"size":{"type":"long"},"extension":{"type":"keyword"},
				"content":{"type":"text"}}},
			"mergeForward":{"type":"object","properties":{"msgs":{"type":"object","properties":{
				"from":{"type":"keyword"},"timestamp":{"type":"date","format":"epoch_second"}}}}},
			"richText":{"type":"object","properties":{"searchText":{"type":"text"}}}
		}},
		"payloadRaw":{"type":"object","enabled":false}
	}`
	rt := &mappingTransport{body: liveMappingBody(props)}
	w := mappingWriter(t, rt)
	err := w.AssertLiveMappingCompatible(context.Background())
	if err == nil || !strings.Contains(err.Error(), "payload.file.contentMeta.extractedAt") {
		t.Fatalf("missing payload.file.contentMeta.extractedAt must fail naming the path, got %v", err)
	}
}

// TestRequiredMappingFieldPaths_IncludesV112Fields v1.12：明确覆盖 requiredMappingFieldPaths
// 常量包含新加两条路径（配 IDX-2 加字段动作 + IDX-4 file-extractor 上线前置校验）。
func TestRequiredMappingFieldPaths_IncludesV112Fields(t *testing.T) {
	want := map[string]bool{
		"payload.file.content":                true,
		"payload.file.contentMeta.extractedAt": true,
	}
	found := map[string]bool{}
	for _, p := range requiredMappingFieldPaths {
		if want[p] {
			found[p] = true
		}
	}
	for p := range want {
		if !found[p] {
			t.Errorf("requiredMappingFieldPaths missing v1.12 path %q", p)
		}
	}
}
