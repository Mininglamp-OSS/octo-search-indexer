package esindex

// scripted_upsert_test.go — Blocker #3 修复回归 test（v1.13）。
// 目标：验证 es-indexer 的 _bulk update+scripted_upsert 写法能：
//   1. 首次写入（doc 不存在，走 upsert）：全量落 params.doc，无 preserve 需求
//   2. 重复写入（doc 已存在）：保留 file-extractor 写的 payload.file.content + contentMeta，
//      其他字段可被 es-indexer 覆盖
//   3. 请求 body 编码正确（含 preserve 字段路径、script lang=painless、scripted_upsert=true）
//   4. Painless script 常量含 preservedFilePaths 列出的字段路径（未来扩字段防漂移）

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// TestBulk_UpdateActionEncoding 单条 doc 编码：action 行必须 update + retry_on_conflict=3，
// body 必须含 scripted_upsert=true + script.lang=painless + params.doc + upsert。
func TestBulk_UpdateActionEncoding(t *testing.T) {
	var gotBody string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotBody = readAll(r.Body)
		return jsonResp(200, `{"took":1,"errors":false,"items":[{"update":{"_id":"901","status":201}}]}`), nil
	})
	w := newTestWriter(t, rt)
	if _, err := w.Bulk(context.Background(), []searchmsg.Message{msg("901", "hello")}); err != nil {
		t.Fatalf("Bulk: %v", err)
	}
	lines := strings.Split(strings.TrimRight(gotBody, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines (action + body), got %d: %s", len(lines), gotBody)
	}
	// 校验动作行 JSON 结构
	var action bulkActionLine
	if err := json.Unmarshal([]byte(lines[0]), &action); err != nil {
		t.Fatalf("parse action line: %v", err)
	}
	if action.Update.ID != "901" {
		t.Fatalf("action._id must be message_id, got %q", action.Update.ID)
	}
	if action.Update.RetryOnConflict != 3 {
		t.Fatalf("action.retry_on_conflict must be 3 (align with file-extractor oswriter), got %d", action.Update.RetryOnConflict)
	}
	// 校验 body 行 JSON 结构
	var body bulkUpdateBody
	if err := json.Unmarshal([]byte(lines[1]), &body); err != nil {
		t.Fatalf("parse body line: %v", err)
	}
	if !body.ScriptedUpsert {
		t.Fatalf("body.scripted_upsert must be true")
	}
	if body.Script.Lang != "painless" {
		t.Fatalf("body.script.lang must be 'painless', got %q", body.Script.Lang)
	}
	if body.Script.Source != preservePainless {
		t.Fatalf("body.script.source must == preservePainless const")
	}
	if _, ok := body.Script.Params["doc"]; !ok {
		t.Fatalf("body.script.params must contain 'doc' key")
	}
	// upsert 段必须与 params.doc 同源（同一 Doc 结构）
	if body.Upsert.MessageID != 901 {
		t.Fatalf("body.upsert must carry same doc (messageId=901), got %d", body.Upsert.MessageID)
	}
}

// TestPreservePainless_ContainsAllPreservedPaths Painless script 常量必须**明确提及**
// preservedFilePaths 列出的所有字段路径。未来 file-extractor 扩字段时，要么同步扩 script
// 常量 + 更新 preservedFilePaths + 本 test 通过；要么本 test 失败提醒改动方同步。
func TestPreservePainless_ContainsAllPreservedPaths(t *testing.T) {
	// preservedFilePaths 里的 "payload.file.content" / "payload.file.contentMeta"，
	// 需在 painless script 里能识别（用 field name 后缀断言，避免路径 delimiter 差异）。
	for _, p := range preservedFilePaths {
		// 取最后一级字段名（例如 "payload.file.content" → "content"）
		parts := strings.Split(p, ".")
		leaf := parts[len(parts)-1]
		if !strings.Contains(preservePainless, leaf) {
			t.Fatalf("preservePainless must reference field %q (from preservedFilePaths %q); if you added a preserved path, update the script or drop the path", leaf, p)
		}
	}
	// 反向：script 里出现的 preserve 目标字段名必须都在 preservedFilePaths（防 script 悄悄多 preserve）。
	// 这里只做最基本的保底：script 必须显式 preserve payload.file
	if !strings.Contains(preservePainless, "payload") || !strings.Contains(preservePainless, "file") {
		t.Fatalf("preservePainless must reference 'payload' + 'file' path (preserve target)")
	}
	// script 必须处理 ctx.op == 'create' 分支
	if !strings.Contains(preservePainless, "ctx.op == 'create'") {
		t.Fatalf("preservePainless must handle scripted_upsert create branch (ctx.op == 'create')")
	}
	// script 必须处理 ctx._source = params.doc（全量替换）
	if !strings.Contains(preservePainless, "ctx._source = params.doc") {
		t.Fatalf("preservePainless must do full-replace ctx._source = params.doc")
	}
}

// TestPreservedFilePaths_NotEmpty preservedFilePaths 常量非空（防被误清）。
func TestPreservedFilePaths_NotEmpty(t *testing.T) {
	if len(preservedFilePaths) == 0 {
		t.Fatalf("preservedFilePaths must not be empty (Blocker #3 fix relies on this)")
	}
	// 必含 file.content 路径（file-extractor 的核心写入字段）
	seenContent := false
	for _, p := range preservedFilePaths {
		if strings.HasSuffix(p, ".content") {
			seenContent = true
			break
		}
	}
	if !seenContent {
		t.Fatalf("preservedFilePaths must include a path ending with '.content' (file-extractor writes payload.file.content)")
	}
}

// TestBulk_ScriptPreservesContent_MockedResponse 模拟一次 bulk 写入：doc 结构本身不带 content
// (buildraw 逻辑：es-indexer 主流程不填 payload.file.content)，但发出的请求 body 里 script
// 常量已包含 preserve 逻辑 —— 由 OS 端 painless script 在真实 update 时执行 preserve。
// 本 test 覆盖：请求 body **一致地**发送 script（不因 doc 没 content 而短路），
// 让 OS 端 script 有条件 preserve 已有 content。
func TestBulk_ScriptPreservesContent_MockedResponse(t *testing.T) {
	var gotBody string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotBody = readAll(r.Body)
		return jsonResp(200, `{"took":1,"errors":false,"items":[{"update":{"_id":"902","status":200}}]}`), nil
	})
	w := newTestWriter(t, rt)
	// text 消息（type=1）—— buildraw 分支不填 payload.file.*，doc 里不会出现 payload.file 段。
	res, err := w.Bulk(context.Background(), []searchmsg.Message{msg("902", "content-not-set")})
	if err != nil || !res[0].OK {
		t.Fatalf("Bulk: err=%v res=%+v", err, res)
	}
	// 断言 body 无 payload.file 段（text 消息本无 file 字段；es-indexer 也无权写 file.content）
	if strings.Contains(gotBody, `"file":`) {
		t.Fatalf("es-indexer body must NOT carry payload.file segment for text msg, got: %s", gotBody)
	}
	// 断言 script 常量已发送 (依赖 OS 端 painless 保留已有 content)
	if !strings.Contains(gotBody, `savedContent`) {
		t.Fatalf("script must send preserve logic (savedContent keyword) so OS side preserves existing content, got: %s", gotBody)
	}
	if !strings.Contains(gotBody, `savedMeta`) {
		t.Fatalf("script must send preserve logic (savedMeta keyword) so OS side preserves existing contentMeta, got: %s", gotBody)
	}
}

// TestBulk_DerivativesUsePreserveScript 富文本虚拟子文档也必须走 scripted_upsert。
// 富文本 doc 派生的子 doc（例如 image block）与父 doc 同批写入，也可能被 file-extractor
// 覆盖 content（未来富文本嵌 file block 时），故子 doc 也走 preserve script。
func TestBulk_DerivativesUsePreserveScript(t *testing.T) {
	var gotBody string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotBody = readAll(r.Body)
		return jsonResp(200, `{"took":1,"errors":false,"items":[
			{"update":{"_id":"1000","status":201}},
			{"update":{"_id":"1000-rt1","status":201}}
		]}`), nil
	})
	w := osWriterFor(t, rt)
	parent := Doc{
		MessageID: 1000, ChannelID: "g", ChannelType: 2,
		Derivatives: []Doc{
			{MessageID: 1000, ChannelID: "g", ChannelType: 2, idOverride: "1000-rt1", Virtual: true},
		},
	}
	if _, err := w.BulkDocs(context.Background(), []Doc{parent}); err != nil {
		t.Fatalf("BulkDocs: %v", err)
	}
	// body 应该有 2 组 (action, body) 行 — 父 + 子都走 update+scripted_upsert
	// 每组 body 都含 scripted_upsert=true
	scriptedUpsertCount := strings.Count(gotBody, `"scripted_upsert":true`)
	if scriptedUpsertCount != 2 {
		t.Fatalf("expected 2 scripted_upsert entries (parent + derivative), got %d in body: %s", scriptedUpsertCount, gotBody)
	}
	// 每组 action 都是 update
	updateCount := strings.Count(gotBody, `"update":{"_id":`)
	if updateCount != 2 {
		t.Fatalf("expected 2 update actions (parent + derivative), got %d", updateCount)
	}
}

// TestBulk_ResponseUpdateKeyParsed mock OS 返回 items[i]["update"]，writer 必须能正确读 status。
// 老版本 items[i]["index"] 应该 fail（无 "update" 字段 → transient status=0）。
func TestBulk_ResponseUpdateKeyParsed(t *testing.T) {
	// 用旧 "index" key 模拟响应格式漂移（应触发 transient，不静默当成功）
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return jsonResp(200, `{"took":1,"errors":false,"items":[{"index":{"_id":"777","status":201}}]}`), nil
	})
	w := newTestWriter(t, rt)
	res, err := w.Bulk(context.Background(), []searchmsg.Message{msg("777", "x")})
	if err != nil {
		t.Fatalf("Bulk: %v", err)
	}
	if res[0].OK || res[0].Status != 0 {
		t.Fatalf("response with wrong action key ('index' vs 'update') must be transient not OK, got %+v", res[0])
	}
	if res[0].Err == nil || !strings.Contains(res[0].Err.Error(), "no update action") {
		t.Fatalf("expected error mentioning 'no update action', got %v", res[0].Err)
	}
}

// TestBulk_LargeDocSizeEstimateUpdated encodedSingleDocSize 反映 update body 膨胀。
// 老估算 ≈ doc size + overhead；新估算 ≈ 2×doc + script const + overhead。
// 若估算未跟着改，subBatchEnd 会按小值切子批，导致单批实际 > threshold 被 OS 413。
func TestBulk_LargeDocSizeEstimateUpdated(t *testing.T) {
	d := Doc{MessageID: 1, ChannelID: "g", ChannelType: 2, PayloadRaw: json.RawMessage([]byte(`{"a":"` + strings.Repeat("x", 1<<20) + `"}`))}
	docJSON, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	estimated := encodedSingleDocSize(d)
	// 新估算至少 = 2 * doc + script const（不含 overhead）
	minExpected := 2*len(docJSON) + len(preservePainless)
	if estimated < minExpected {
		t.Fatalf("encodedSingleDocSize=%d must reflect update body doubling (>=2×docJSON+script=%d)", estimated, minExpected)
	}
}
