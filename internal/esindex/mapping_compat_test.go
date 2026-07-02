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

// liveMappingBodyWithExcludes 包装 properties + _source.excludes（v1.13 Blocker #3 复发回归）。
func liveMappingBodyWithExcludes(t *testing.T, propsJSON string, excludes []string) string {
	t.Helper()
	excJSON, err := json.Marshal(excludes)
	if err != nil {
		t.Fatalf("marshal excludes: %v", err)
	}
	return `{"octo-message":{"mappings":{"dynamic":"strict","_source":{"excludes":` + string(excJSON) + `},"properties":` + propsJSON + `}}}`
}

// TestMappingCompat_SourceExcludesContentFails 🔴 v1.13 Blocker #3 复发回归 (P2-2)：live mapping
// `_source.excludes` 里含 payload.file.content → scripted_upsert preserve 语义失效
// （script 从 ctx._source 读永远 null）→ AssertLiveMappingCompatible 必须 loud crash。
func TestMappingCompat_SourceExcludesContentFails(t *testing.T) {
	var embedded struct {
		Mappings struct {
			Properties json.RawMessage `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal(IndexMappingJSON(), &embedded); err != nil {
		t.Fatalf("parse embedded mapping: %v", err)
	}
	rt := &mappingTransport{body: liveMappingBodyWithExcludes(t, string(embedded.Mappings.Properties), []string{"payload.file.content"})}
	w := mappingWriter(t, rt)
	err := w.AssertLiveMappingCompatible(context.Background())
	if err == nil {
		t.Fatal("expected loud crash for _source.excludes containing payload.file.content, got nil")
	}
	if !strings.Contains(err.Error(), "_source.excludes contains") {
		t.Errorf("error must mention _source.excludes pollution, got %v", err)
	}
	if !strings.Contains(err.Error(), "payload.file.content") {
		t.Errorf("error must name the offending path, got %v", err)
	}
	if !strings.Contains(err.Error(), "Blocker #3 revive") {
		t.Errorf("error must reference Blocker #3 revive for operator context, got %v", err)
	}
}

// TestMappingCompat_SourceExcludesContentMetaFails 同上但针对 contentMeta（forbiddenSourceExcludes
// 覆盖 preservedFilePaths 全集，防漂移）。
func TestMappingCompat_SourceExcludesContentMetaFails(t *testing.T) {
	var embedded struct {
		Mappings struct {
			Properties json.RawMessage `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal(IndexMappingJSON(), &embedded); err != nil {
		t.Fatalf("parse embedded mapping: %v", err)
	}
	rt := &mappingTransport{body: liveMappingBodyWithExcludes(t, string(embedded.Mappings.Properties), []string{"payload.file.contentMeta"})}
	w := mappingWriter(t, rt)
	err := w.AssertLiveMappingCompatible(context.Background())
	if err == nil || !strings.Contains(err.Error(), "payload.file.contentMeta") {
		t.Fatalf("expected loud crash for excludes containing contentMeta, got %v", err)
	}
}

// TestMappingCompat_SourceExcludesUnrelatedPasses excludes 里含**其他**未在 forbiddenSourceExcludes
// 里的字段（未来若加过滤字段）不阻止启动。
func TestMappingCompat_SourceExcludesUnrelatedPasses(t *testing.T) {
	var embedded struct {
		Mappings struct {
			Properties json.RawMessage `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal(IndexMappingJSON(), &embedded); err != nil {
		t.Fatalf("parse embedded mapping: %v", err)
	}
	rt := &mappingTransport{body: liveMappingBodyWithExcludes(t, string(embedded.Mappings.Properties), []string{"someOtherField"})}
	w := mappingWriter(t, rt)
	if err := w.AssertLiveMappingCompatible(context.Background()); err != nil {
		t.Fatalf("unrelated excludes must not block, got %v", err)
	}
}

// TestMappingCompat_ForbiddenExcludesCoversPreservedPaths **契约锁死**：forbiddenSourceExcludes
// 必须覆盖 preservedFilePaths 全集。future 加 preserve 字段时不更新 forbidden 列表 → CI 挂。
func TestMappingCompat_ForbiddenExcludesCoversPreservedPaths(t *testing.T) {
	forbidden := make(map[string]bool, len(forbiddenSourceExcludes))
	for _, p := range forbiddenSourceExcludes {
		forbidden[p] = true
	}
	for _, p := range preservedFilePaths {
		if !forbidden[p] {
			t.Errorf("preservedFilePaths %q must also be in forbiddenSourceExcludes (else scripted_upsert preserve silently fails)", p)
		}
	}
}

// TestEmbeddedMapping_NoSourceExcludes 内嵌 mapping 本身**不能**再声明 `_source.excludes`
// （Blocker #3 复发 fix：已彻底移除该配置；防未来某人无意加回）。
func TestEmbeddedMapping_NoSourceExcludes(t *testing.T) {
	var top struct {
		Mappings struct {
			Source *struct {
				Excludes []string `json:"excludes"`
			} `json:"_source,omitempty"`
		} `json:"mappings"`
	}
	if err := json.Unmarshal(IndexMappingJSON(), &top); err != nil {
		t.Fatalf("parse embedded mapping: %v", err)
	}
	if top.Mappings.Source != nil && len(top.Mappings.Source.Excludes) > 0 {
		t.Fatalf("embedded mapping must NOT declare _source.excludes (Blocker #3 revive gate); got %v", top.Mappings.Source.Excludes)
	}
}

// TestMappingCompat_MissingTombstoneStatusFails 🔴 Round-3 Blocker B (yujiawei P1 / Jerry-Xin #2)：
// live mapping 缺 payload.file.contentMeta.status → tombstone 写入会因 dynamic:strict 4xx →
// permanent-fail 文件无 tombstone → backfill rerun 重复 DLQ。启动期 loud crash 拦下部署顺序错。
func TestMappingCompat_MissingTombstoneStatusFails(t *testing.T) {
	// 含所有必需字段 **except** contentMeta.status
	props := `{
		"messageId":{"type":"long"},
		"channelId":{"type":"keyword"},
		"timestamp":{"type":"date","format":"epoch_second"},
		"parentMessageId":{"type":"long"},
		"parentPayloadType":{"type":"integer"},
		"virtual":{"type":"boolean"},
		"subSeq":{"type":"integer"},
		"payload":{"properties":{
			"mergeForward":{"properties":{"msgs":{"properties":{"from":{"type":"keyword"},"timestamp":{"type":"date"}}}}},
			"richText":{"properties":{"searchText":{"type":"text"}}},
			"file":{"properties":{
				"content":{"type":"text"},
				"contentMeta":{"properties":{
					"extractedAt":{"type":"date"},
					"reason":{"type":"keyword"}
				}}
			}}
		}},
		"payloadRaw":{"enabled":false,"type":"object"}
	}`
	rt := &mappingTransport{body: liveMappingBody(props)}
	w := mappingWriter(t, rt)
	err := w.AssertLiveMappingCompatible(context.Background())
	if err == nil || !strings.Contains(err.Error(), "payload.file.contentMeta.status") {
		t.Fatalf("expected loud crash naming missing contentMeta.status field, got %v", err)
	}
}

// TestMappingCompat_MissingTombstoneReasonFails 同上但缺 contentMeta.reason。
func TestMappingCompat_MissingTombstoneReasonFails(t *testing.T) {
	props := `{
		"messageId":{"type":"long"},
		"channelId":{"type":"keyword"},
		"timestamp":{"type":"date","format":"epoch_second"},
		"parentMessageId":{"type":"long"},
		"parentPayloadType":{"type":"integer"},
		"virtual":{"type":"boolean"},
		"subSeq":{"type":"integer"},
		"payload":{"properties":{
			"mergeForward":{"properties":{"msgs":{"properties":{"from":{"type":"keyword"},"timestamp":{"type":"date"}}}}},
			"richText":{"properties":{"searchText":{"type":"text"}}},
			"file":{"properties":{
				"content":{"type":"text"},
				"contentMeta":{"properties":{
					"extractedAt":{"type":"date"},
					"status":{"type":"keyword"}
				}}
			}}
		}},
		"payloadRaw":{"enabled":false,"type":"object"}
	}`
	rt := &mappingTransport{body: liveMappingBody(props)}
	w := mappingWriter(t, rt)
	err := w.AssertLiveMappingCompatible(context.Background())
	if err == nil || !strings.Contains(err.Error(), "payload.file.contentMeta.reason") {
		t.Fatalf("expected loud crash naming missing contentMeta.reason field, got %v", err)
	}
}
