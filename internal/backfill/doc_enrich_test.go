package backfill

import (
	"encoding/json"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// payloadWith 构造一条带 space_id/visibles 的文本 payload（明文 JSON，模拟 enrichPayloadWithSpaceID 产物）。
func payloadWith(t *testing.T, content, spaceID string, visibles []string) []byte {
	t.Helper()
	m := map[string]interface{}{"type": contentTypeText, "content": content}
	if spaceID != "" {
		m["space_id"] = spaceID
	}
	if visibles != nil {
		m["visibles"] = visibles
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// TestDocFromRow_EnrichesSpaceIDVisiblesSeq 🔴 V1(b)+V3b 解红的根：backfill 从原始 MySQL
// payload 自源填 reader 必读的 spaceId/visibles + message_seq 列。
func TestDocFromRow_EnrichesSpaceIDVisiblesSeq(t *testing.T) {
	row := &srcMessageRow{
		ID: 1, MessageID: "1001", MessageSeq: 77, ChannelType: 1, // p2p
		Payload: payloadWith(t, "hello", "space-A", []string{"admin1", "admin2"}),
	}
	doc, outcome, err := docFromRow(row)
	if err != nil {
		t.Fatalf("docFromRow: %v", err)
	}
	if outcome != outcomeOK {
		t.Fatalf("want outcomeOK, got %v", outcome)
	}
	if doc.SpaceID != "space-A" {
		t.Fatalf("spaceId not enriched (V1b would stay red): %q", doc.SpaceID)
	}
	if len(doc.Visibles) != 2 || doc.Visibles[0] != "admin1" || doc.Visibles[1] != "admin2" {
		t.Fatalf("visibles not enriched (V3b would fail-OPEN): %+v", doc.Visibles)
	}
	if doc.MessageSeq != 77 {
		t.Fatalf("messageSeq not carried (reader channel_offset gate): %d", doc.MessageSeq)
	}
	if doc.MessageID != 1001 {
		t.Fatalf("messageId mismatch: %d", doc.MessageID)
	}
}

// TestDocFromRow_NoVisibilityFields payload 无 space_id/visibles → 字段留空（reader p2p fail-closed / 无 gate）。
func TestDocFromRow_NoVisibilityFields(t *testing.T) {
	row := &srcMessageRow{ID: 2, MessageID: "1002", ChannelType: 2, Payload: payloadWith(t, "hi", "", nil)}
	doc, outcome, err := docFromRow(row)
	if err != nil || outcome != outcomeOK {
		t.Fatalf("docFromRow: outcome=%v err=%v", outcome, err)
	}
	if doc.SpaceID != "" || len(doc.Visibles) != 0 {
		t.Fatalf("absent visibility fields must stay empty: spaceId=%q visibles=%+v", doc.SpaceID, doc.Visibles)
	}
}

// TestDocFromRow_SignalEncryptedNoVisibility 加密消息不解析 payload → spaceId/visibles 留空（fail-closed）。
func TestDocFromRow_SignalEncryptedNoVisibility(t *testing.T) {
	row := &srcMessageRow{ID: 3, MessageID: "1003", Signal: 1, ChannelType: 1, Payload: []byte("CIPHERTEXT")}
	doc, outcome, err := docFromRow(row)
	if err != nil {
		t.Fatalf("docFromRow: %v", err)
	}
	if outcome != outcomeRawExcluded {
		t.Fatalf("signal msg must be raw_excluded, got %v", outcome)
	}
	if doc.SpaceID != "" || len(doc.Visibles) != 0 {
		t.Fatalf("encrypted msg must not leak visibility fields: %+v", doc)
	}
}

// TestDocFromRow_NonNumericMessageIDToDLQ message_id 非数值 → outcomeDLQ + err（不静默落 0）。
func TestDocFromRow_NonNumericMessageIDToDLQ(t *testing.T) {
	row := &srcMessageRow{ID: 4, MessageID: "abc", ChannelType: 2, Payload: payloadWith(t, "hi", "", nil)}
	_, outcome, err := docFromRow(row)
	if outcome != outcomeDLQ || err == nil {
		t.Fatalf("non-numeric message_id must be DLQ with error: outcome=%v err=%v", outcome, err)
	}
}

// TestDocFromRow_BadJSONToDLQ payload 真异常 → outcomeDLQ（与 extract 口径一致）。
func TestDocFromRow_BadJSONToDLQ(t *testing.T) {
	row := &srcMessageRow{ID: 5, MessageID: "1005", ChannelType: 2, Payload: []byte("{not json")}
	_, outcome, err := docFromRow(row)
	if outcome != outcomeDLQ {
		t.Fatalf("bad json must be DLQ, got %v (err=%v)", outcome, err)
	}
}

// TestBackfillRunsSharedFailClosedVectors 🔴 验收门(ii) 同口径锁：backfill 富化路径必须跑
// **与 producer 完全相同**的 searchmsg.FailClosedVisibilityVectors() 向量，证明两仓用同一个
// octo-lib searchmsg.ExtractVisibility（票2 共享 fail-closed parser），口径不分叉（防 #1124）。
//
// 第一段直接对 searchmsg.ExtractVisibility 跑向量（锁 parser 本体口径）；第二段把每条向量喂进
// backfill 的 docFromRow（锁 backfill 实际接线：fail-closed 向量必落 DLQ 绝不写空 visibles，
// 放行向量 visibles 完整落 doc），证明 backfill 没有在 parser 之外再放水。
func TestBackfillRunsSharedFailClosedVectors(t *testing.T) {
	for _, v := range searchmsg.FailClosedVisibilityVectors() {
		t.Run("parser/"+v.Name, func(t *testing.T) {
			sid, vis, err := searchmsg.ExtractVisibility(v.Payload)
			if v.WantErr {
				if err == nil {
					t.Fatalf("vector %q: want fail-closed (err), got spaceID=%q visibles=%+v", v.Name, sid, vis)
				}
				return
			}
			if err != nil {
				t.Fatalf("vector %q: want pass, got err=%v", v.Name, err)
			}
			if sid != v.WantSpaceID {
				t.Fatalf("vector %q: spaceID=%q want %q", v.Name, sid, v.WantSpaceID)
			}
			if !equalStrings(vis, v.WantVisibles) {
				t.Fatalf("vector %q: visibles=%+v want %+v", v.Name, vis, v.WantVisibles)
			}
		})
	}
}

// TestDocFromRow_SharedVectorsWiredFailClosed 把共享向量喂进 backfill docFromRow，断言接线正确：
//   - WantErr 向量（含 valid-but-empty visibles：[] / null / 全非字符串 / 全空串）→ outcomeDLQ，
//     该行**绝不写入** ES（fail-closed）。这正是验收门(ii) 真正闭合处：旧自实现对 [] / null / 全非
//     字符串放行 fail-OPEN，共享 parser 一律 fail-closed 落 DLQ。
//   - 放行向量 → 非 DLQ（OK 或 raw_excluded，取决于 payload 是否带 type=text），且 doc 的
//     SpaceID/Visibles 与向量期望一致（visibles 被忠实富化进 doc，不放空）。
//
// 注：向量是为 ExtractVisibility 本体设计的，部分 payload 不带 type=1 文本正文，故经 extractMessage
// 内容门后落 raw_excluded（仍写 doc、仍富化 visibles）属预期；本测试只锁「fail-closed 必落 DLQ /
// 放行必富化 visibles」这条接线不变量，不锁正文 outcome 的 OK/raw_excluded 细分。
func TestDocFromRow_SharedVectorsWiredFailClosed(t *testing.T) {
	for _, v := range searchmsg.FailClosedVisibilityVectors() {
		t.Run(v.Name, func(t *testing.T) {
			// 非加密文本行（payload = 向量字节），message_id 数值可解析。
			row := &srcMessageRow{ID: 1, MessageID: "1001", ChannelType: 2, Payload: v.Payload}
			doc, outcome, err := docFromRow(row)
			if v.WantErr {
				// fail-closed：该行必须落 DLQ（永不写入 ES），绝不产出带空 visibles 的可写 doc。
				if outcome != outcomeDLQ {
					t.Fatalf("vector %q: fail-closed must route to DLQ (never written), got outcome=%v err=%v doc.Visibles=%+v",
						v.Name, outcome, err, doc.Visibles)
				}
				return
			}
			// 放行向量：不得落 DLQ；visibles 必须被富化进 doc（不放空 → reader 不会 fail-OPEN）。
			if outcome == outcomeDLQ {
				t.Fatalf("vector %q: legitimate visibility must not DLQ, got err=%v", v.Name, err)
			}
			if err != nil {
				t.Fatalf("vector %q: unexpected err=%v", v.Name, err)
			}
			if doc.SpaceID != v.WantSpaceID {
				t.Fatalf("vector %q: doc.SpaceID=%q want %q", v.Name, doc.SpaceID, v.WantSpaceID)
			}
			if !equalStrings(doc.Visibles, v.WantVisibles) {
				t.Fatalf("vector %q: doc.Visibles=%+v want %+v", v.Name, doc.Visibles, v.WantVisibles)
			}
		})
	}
}

// equalStrings 比较两个字符串切片（nil 与空切片视为相等：reader 对二者均「无 gate」）。
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDocFromRow_NonStringSpaceIDKeepsVisibles 🔴 端到端 V3b：backfill doc 富化路径下，
// 非字符串 space_id 的合法行**绝不**产出空 visibles —— 要么保留 visibles，要么 fail-closed。
// 本用例锁定「保留 visibles」（visibles 合法时不该 fail-closed 把好行也丢了）。
func TestDocFromRow_NonStringSpaceIDKeepsVisibles(t *testing.T) {
	// 手工拼一条 space_id 为数字、visibles 合法的明文 payload。
	raw := []byte(`{"type":1,"content":"hi","space_id":12345,"visibles":["admin1","admin2"]}`)
	row := &srcMessageRow{ID: 9, MessageID: "2001", ChannelType: 2, Payload: raw}
	doc, outcome, err := docFromRow(row)
	if err != nil {
		t.Fatalf("legitimate visibles row must not error: %v", err)
	}
	if outcome != outcomeOK {
		t.Fatalf("want outcomeOK (visibles legit), got %v", outcome)
	}
	if len(doc.Visibles) != 2 || doc.Visibles[0] != "admin1" || doc.Visibles[1] != "admin2" {
		t.Fatalf("V3b fail-OPEN: weird space_id cleared legitimate visibles: %+v", doc.Visibles)
	}
	if doc.SpaceID != "" {
		t.Fatalf("non-string space_id must degrade to empty (p2p fail-closed): %q", doc.SpaceID)
	}
}

// TestDocFromRow_BrokenVisibilityFailClosed visibles 结构损坏（非数组）→ 该行 fail-closed 落 DLQ，
// 绝不写空 visibles（fail-OPEN）。
func TestDocFromRow_BrokenVisibilityFailClosed(t *testing.T) {
	raw := []byte(`{"type":1,"content":"hi","space_id":"s1","visibles":"u1,u2"}`)
	row := &srcMessageRow{ID: 10, MessageID: "2002", ChannelType: 2, Payload: raw}
	_, outcome, err := docFromRow(row)
	if outcome != outcomeDLQ || err == nil {
		t.Fatalf("broken visibles must fail-closed to DLQ (never empty visibles): outcome=%v err=%v", outcome, err)
	}
}
