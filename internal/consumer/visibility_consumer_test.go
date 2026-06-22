package consumer

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// branchAMsgBytes 构造一条方案 B 分支 A 消息字节（非加密新形态，带 RawPayload 整包，不预填 visibles）。
//
// rawPayload 必须本身是合法 JSON（才能内联进 Message.raw_payload 并通过 decodeAndValidate 的整体
// json.Unmarshal）。**结构性损坏的 payload（非合法 JSON）在线格上根本无法作为合法 Message 的
// raw_payload 到达消费侧——它会让整条消息 json.Unmarshal 失败 → 走 schema-invalid 毒丸路径**，
// 故不属于 visibility 预检的输入域。返回 ok=false 表示该向量不可作为 RawPayload 注入（跳过）。
func branchAMsgBytes(t *testing.T, id, rawPayload string) ([]byte, bool) {
	t.Helper()
	if !json.Valid([]byte(rawPayload)) {
		return nil, false
	}
	b, err := json.Marshal(searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     id,
		ChannelID:     "g_1",
		ChannelType:   2,
		FromUID:       "u_1",
		Source:        searchmsg.SourceETLMessageTable,
		RawPayload:    json.RawMessage(rawPayload),
	})
	if err != nil {
		return nil, false
	}
	return b, true
}

// decodeDLQ 解析 fakeDLQSink 记录的一条 DLQ 信封。
func decodeDLQ(t *testing.T, b []byte) dlqRecord {
	t.Helper()
	var rec dlqRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatalf("decode dlq record: %v", err)
	}
	return rec
}

// mustBranchA 是 branchAMsgBytes 的「rawPayload 必为合法 JSON」便捷封装（断言用例）。
func mustBranchA(t *testing.T, id, rawPayload string) []byte {
	t.Helper()
	b, ok := branchAMsgBytes(t, id, rawPayload)
	if !ok {
		t.Fatalf("rawPayload must be valid JSON for branch A: %s", rawPayload)
	}
	return b
}

// TestProcessBatch_S1_SharedVisibilityVectors 🔴 §3.7 S1（升级为 STOP）：消费侧遍历 octo-lib 共享
// 向量 FailClosedVisibilityVectors()（仅取本身是合法 JSON、能作为 RawPayload 到达消费侧的子集；
// 非合法 JSON 的向量在线格上走 schema-invalid，不属 visibility 预检输入域）。WantErr 向量
// （valid-but-empty 等结构性损坏）→ 该条进 DLQ(reason=visibility_untrusted)、doc 不写（不进 bulk）；
// 放行向量 → 进 bulk、不进 DLQ。
func TestProcessBatch_S1_SharedVisibilityVectors(t *testing.T) {
	covered := 0
	for _, vec := range searchmsg.FailClosedVisibilityVectors() {
		t.Run(vec.Name, func(t *testing.T) {
			value, ok := branchAMsgBytes(t, "100", string(vec.Payload))
			if !ok {
				t.Skipf("payload not valid JSON; arrives as schema-invalid, not a visibility-precheck input")
			}
			covered++
			src := &fakeSource{}
			w := &fakeWriter{}
			sink := &fakeDLQSink{}
			alert := &recordAlerter{}
			p := newProc(t, src, w, sink, alert, "")
			batch := []fetchedMessage{fm(0, value)}
			if err := p.processBatch(context.Background(), batch); err != nil {
				t.Fatalf("processBatch: %v", err)
			}
			if vec.WantErr {
				// fail-closed：进 DLQ，绝不进 bulk。
				if w.bulkCalls != 0 {
					t.Fatalf("[%s] fail-closed vector must NOT reach bulk (doc must not be written)", vec.Name)
				}
				if len(sink.records) != 1 {
					t.Fatalf("[%s] fail-closed vector must produce exactly 1 DLQ record, got %d", vec.Name, len(sink.records))
				}
				rec := decodeDLQ(t, sink.records[0])
				if rec.Reason != "visibility_untrusted" {
					t.Fatalf("[%s] DLQ reason = %q, want visibility_untrusted", vec.Name, rec.Reason)
				}
				if rec.Status != 0 {
					t.Fatalf("[%s] visibility DLQ status must be 0 (not 4xx), got %d", vec.Name, rec.Status)
				}
			} else {
				// 放行：进 bulk，不进 DLQ。
				if len(sink.records) != 0 {
					t.Fatalf("[%s] passing vector must NOT produce a DLQ record, got %d", vec.Name, len(sink.records))
				}
				if w.bulkCalls == 0 {
					t.Fatalf("[%s] passing vector must reach bulk", vec.Name)
				}
			}
		})
	}
}

// TestProcessBatch_S2_BranchA_VisiblesParsedFromRaw 分支 A：合法 visibles → 进 bulk，且解析值
// 来自 ExtractVisibility(RawPayload)（消费侧不盲信契约）。
func TestProcessBatch_S2_BranchA_ValidVisibles(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	raw := `{"type":1,"content":"hi","visibles":["u_admin"]}`
	batch := []fetchedMessage{fm(0, mustBranchA(t, "100", raw))}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if len(sink.records) != 0 {
		t.Fatalf("valid visibles must not DLQ")
	}
	if len(w.lastBulk) != 1 || len(w.lastBulk[0].Visibles) != 1 || w.lastBulk[0].Visibles[0] != "u_admin" {
		t.Fatalf("branch A must backfill visibles from ExtractVisibility(RawPayload): %+v", w.lastBulk)
	}
}

// TestProcessBatch_S2_BranchA_EmptyVisiblesDLQ 分支 A：visibles=[] (valid-but-empty) → 落 DLQ、
// 不写空 visibles（核心 fail-closed 回归）。
func TestProcessBatch_S2_BranchA_EmptyVisiblesDLQ(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	raw := `{"type":1,"content":"hi","visibles":[]}`
	batch := []fetchedMessage{fm(0, mustBranchA(t, "100", raw))}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if w.bulkCalls != 0 {
		t.Fatalf("empty visibles must NOT reach bulk (no empty-visibles doc written)")
	}
	if len(sink.records) != 1 || decodeDLQ(t, sink.records[0]).Reason != "visibility_untrusted" {
		t.Fatalf("empty visibles must DLQ with visibility_untrusted: %v", sink.records)
	}
}

// TestProcessBatch_S2_BranchA_UnknownTypeGroupStillParsesVisibility 🔴 P0-B 核心回归：非加密未知
// type 群消息（RawPayload>0 + 投不出 typed 子对象）**仍解析 visibility**（不因 RawExcluded 走跳过
// 分支）。合法 visibles → 进 bulk 且 visibles 来自解析；空 visibles → DLQ。这堵住 fail-OPEN 空洞。
func TestProcessBatch_S2_BranchA_UnknownTypeGroupVisibility(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	// type=99 未知 type（投不出 typed 子对象 → doc.RawExcluded=true），但带 visibles 白名单。
	raw := `{"type":99,"content":"群定向系统消息","visibles":["u_admin"]}`
	batch := []fetchedMessage{fm(0, mustBranchA(t, "100", raw))}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if len(sink.records) != 0 {
		t.Fatalf("unknown-type group msg with valid visibles must not DLQ")
	}
	if len(w.lastBulk) != 1 || len(w.lastBulk[0].Visibles) != 1 || w.lastBulk[0].Visibles[0] != "u_admin" {
		t.Fatalf("P0-B regression: unknown-type group msg must STILL parse visibility (not skipped via RawExcluded): %+v", w.lastBulk)
	}
}

// TestProcessBatch_BranchB_EncryptedSkipsVisibility 分支 B：加密 DM（RawPayload 缺 + RawExcluded=true）
// → 不调 ExtractVisibility、不进 DLQ、进 bulk、visibles 留空。
func TestProcessBatch_BranchB_EncryptedSkips(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	enc := mustJSON(searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     "100",
		ChannelID:     "uidA@uidB",
		ChannelType:   1,
		RawExcluded:   true, // 加密：无 RawPayload
	})
	batch := []fetchedMessage{fm(0, enc)}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if len(sink.records) != 0 {
		t.Fatalf("encrypted DM must not DLQ (ciphertext not fed to parser)")
	}
	if len(w.lastBulk) != 1 || len(w.lastBulk[0].Visibles) != 0 {
		t.Fatalf("encrypted DM must leave visibles empty (reader fail-closed safe): %+v", w.lastBulk)
	}
}

// TestProcessBatch_BranchC_LegacyTrustsContract 分支 C：在飞老 v2（无 RawPayload、非加密）→ 信契约
// visibles，进 bulk（不重解析、不 DLQ）。
func TestProcessBatch_BranchC_LegacyTrustsContract(t *testing.T) {
	src := &fakeSource{}
	w := &fakeWriter{}
	sink := &fakeDLQSink{}
	alert := &recordAlerter{}
	p := newProc(t, src, w, sink, alert, "")
	c := "hi"
	legacy := mustJSON(searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     "100",
		ChannelID:     "g_1",
		ChannelType:   2,
		Content:       &c,
		ContentType:   1,
		Visibles:      []string{"u_admin"}, // producer 富化的契约 visibles
	})
	batch := []fetchedMessage{fm(0, legacy)}
	if err := p.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("processBatch: %v", err)
	}
	if len(sink.records) != 0 {
		t.Fatalf("legacy v2 must not DLQ")
	}
	if len(w.lastBulk) != 1 || len(w.lastBulk[0].Visibles) != 1 || w.lastBulk[0].Visibles[0] != "u_admin" {
		t.Fatalf("branch C must trust contract visibles: %+v", w.lastBulk)
	}
}

// TestBuildDLQRecord_B5_ConsumerEnvelopeTruncation 🔴 §4.4.3 B-5：带大 RawPayload 的毒丸落 consumer
// DLQ 时，Value 被截断（payload_truncated=true、不含完整字节），保证 DLQ 写入不超限卡死。
func TestBuildDLQRecord_B5_ConsumerEnvelopeTruncation(t *testing.T) {
	big := make([]byte, maxDLQRawValueBytes+1)
	for i := range big {
		big[i] = 'x'
	}
	m := fetchedMessage{Topic: "t", Partition: 0, Offset: 5, Key: []byte("100"), Value: big}
	rec := buildDLQRecord(reasonVisibilityUntrusted, m, esindex.BulkItemResult{})
	if !rec.PayloadTruncated {
		t.Fatalf("oversized consumer DLQ envelope must set PayloadTruncated=true")
	}
	if rec.Value != nil {
		t.Fatalf("oversized consumer DLQ envelope must drop Value bytes, got %d", len(rec.Value))
	}
	if !strings.Contains(rec.Detail, "truncated") {
		t.Fatalf("truncation marker must be in Detail: %q", rec.Detail)
	}
	// marshal 后体积必须远低于 1MiB（不会触发 broker 硬限）。
	b := mustJSON(rec)
	if len(b) > 100_000 {
		t.Fatalf("truncated DLQ record must be small, got %d bytes", len(b))
	}

	// 对照：小 Value 不截断。
	small := fetchedMessage{Topic: "t", Offset: 1, Key: []byte("1"), Value: []byte(`{"x":1}`)}
	rec2 := buildDLQRecord(reasonSchemaInvalid, small, esindex.BulkItemResult{})
	if rec2.PayloadTruncated || rec2.Value == nil {
		t.Fatalf("small DLQ record must retain Value verbatim")
	}
}
