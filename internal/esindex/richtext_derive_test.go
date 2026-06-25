package esindex

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// richTextRaw 构造一条 type=14 富文本原始 payload（含给定 content blocks）。
func richTextRaw(blocks string) string {
	return `{"type":14,"plain":"x","content":` + blocks + `}`
}

// TestRichTextDerivatives_ImagesOnly_FileIgnored 富文本含 1 文本 + 2 图 + 1 file block：
// 只派生 2 个 image 子文档（type=2）；file block 被忽略（octo-lib/octo-web 契约：file 未打开）。
// 子文档各带 virtual=true + parentMessageId=父，_id 复合键，subSeq=block序号+1。
func TestRichTextDerivatives_ImagesOnly_FileIgnored(t *testing.T) {
	raw := richTextRaw(`[
		{"type":"text","text":"前言"},
		{"type":"image","url":"http://x/a.png","name":"a.png","width":100,"height":200},
		{"type":"file","url":"http://x/b.pdf","name":"b.pdf","size":1234,"extension":"pdf"},
		{"type":"image","url":"http://x/c.png","name":"c.png","width":50,"height":60}
	]`)
	d, err := DocFromMessage(branchAMsg("2062443880774537216", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	// file block (idx2) 不派生 → 只有 2 个 image 子文档（idx1, idx3）。
	if len(d.Derivatives) != 2 {
		t.Fatalf("want 2 derivatives (images only, file ignored), got %d", len(d.Derivatives))
	}

	img1 := d.Derivatives[0]
	if img1.Payload == nil || img1.Payload.Type == nil || *img1.Payload.Type != payloadTypeImage {
		t.Fatalf("derivative[0] want image type=2, got %+v", img1.Payload)
	}
	if img1.Payload.Image == nil || img1.Payload.Image.URL != "http://x/a.png" ||
		img1.Payload.Image.Name != "a.png" || img1.Payload.Image.Width != 100 || img1.Payload.Image.Height != 200 {
		t.Fatalf("image1 projection mismatch: %+v", img1.Payload.Image)
	}
	// 富文本 image 无 caption，不应被填充。
	if img1.Payload.Image.Caption != "" {
		t.Fatalf("richtext image must not carry caption, got %q", img1.Payload.Image.Caption)
	}
	if !img1.Virtual || img1.ParentMessageID != d.MessageID || img1.ParentPayloadType != payloadTypeRichText {
		t.Fatalf("image1 parent-tracking mismatch: virtual=%v parent=%d ptype=%d", img1.Virtual, img1.ParentMessageID, img1.ParentPayloadType)
	}
	if img1.MessageID != d.MessageID {
		t.Fatalf("derivative messageId must equal parent: got %d want %d", img1.MessageID, d.MessageID)
	}
	// _id 复合键：第一张图在 block index=1。
	if got := img1.idString(); got != "2062443880774537216-rt1" {
		t.Fatalf("image1 _id want <parent>-rt1, got %q", got)
	}
	// subSeq = block序号 i+1（父独占 0）。idx1 → subSeq=2。
	if img1.SubSeq != 2 {
		t.Fatalf("image1 subSeq want 2 (block idx 1 +1), got %d", img1.SubSeq)
	}

	img2 := d.Derivatives[1]
	if img2.Payload == nil || img2.Payload.Type == nil || *img2.Payload.Type != payloadTypeImage {
		t.Fatalf("derivative[1] want image type=2, got %+v", img2.Payload)
	}
	if img2.Payload.Image == nil || img2.Payload.Image.URL != "http://x/c.png" {
		t.Fatalf("image2 projection mismatch: %+v", img2.Payload.Image)
	}
	// 第二张图在 block index=3（file 在 idx2 不派生，但 _id/subSeq 仍用原始 block 下标）。
	if got := img2.idString(); got != "2062443880774537216-rt3" {
		t.Fatalf("image2 _id want <parent>-rt3 (original block idx), got %q", got)
	}
	if img2.SubSeq != 4 {
		t.Fatalf("image2 subSeq want 4 (block idx 3 +1), got %d", img2.SubSeq)
	}
	// 父 doc subSeq 独占 0。
	if d.SubSeq != 0 {
		t.Fatalf("parent richtext doc subSeq want 0, got %d", d.SubSeq)
	}
	// 继承父可见性字段。
	if img2.ChannelID != d.ChannelID || img2.From != d.From || img2.Timestamp != d.Timestamp {
		t.Fatalf("image2 did not inherit parent fields: %+v", img2)
	}
	// 子文档不写 payloadRaw，不携带自己的 Derivatives。
	if len(img2.PayloadRaw) != 0 || len(img2.Derivatives) != 0 {
		t.Fatalf("child must clear payloadRaw/Derivatives, got rawLen=%d derivs=%d", len(img2.PayloadRaw), len(img2.Derivatives))
	}
}

// TestRichTextDerivatives_EmptyURLImageSkipped 空 url 的 image block 被跳过（防御分支），
// 且跳过不打乱后续 block 的编号：content=[text(idx0), image无url(idx1), image有url(idx2)]，
// 只派生 1 个子文档（idx2），其 subSeq=3、_id 后缀 "-rt2"（仍按原始 block 下标 i=2 计）。
func TestRichTextDerivatives_EmptyURLImageSkipped(t *testing.T) {
	raw := richTextRaw(`[
		{"type":"text","text":"前言"},
		{"type":"image","url":"","name":"empty.png","width":10,"height":20},
		{"type":"image","url":"http://x/c.png","name":"c.png","width":50,"height":60}
	]`)
	d, err := DocFromMessage(branchAMsg("2062443880774537216", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	// 空 url image (idx1) 被跳过 → 只有 1 个 image 子文档（idx2）。
	if len(d.Derivatives) != 1 {
		t.Fatalf("want 1 derivative (empty-url image skipped), got %d", len(d.Derivatives))
	}

	img := d.Derivatives[0]
	if img.Payload == nil || img.Payload.Type == nil || *img.Payload.Type != payloadTypeImage {
		t.Fatalf("derivative[0] want image type=2, got %+v", img.Payload)
	}
	if img.Payload.Image == nil || img.Payload.Image.URL != "http://x/c.png" {
		t.Fatalf("derivative projection mismatch: %+v", img.Payload.Image)
	}
	// _id/subSeq 仍按原始 block 下标 i=2 计（跳过不打乱后续编号）。
	if got := img.idString(); got != "2062443880774537216-rt2" {
		t.Fatalf("derivative _id want <parent>-rt2 (original block idx), got %q", got)
	}
	if img.SubSeq != 3 {
		t.Fatalf("derivative subSeq want 3 (block idx 2 +1), got %d", img.SubSeq)
	}
}

// TestRichTextDerivatives_InheritVisiblesSpaceID 安全回归门：子文档必须完整继承父的可见性字段
// （Visibles 白名单 + SpaceID）。deriveChild 走「复制父再覆盖」，这两个最安全相关的字段不应被漏。
func TestRichTextDerivatives_InheritVisiblesSpaceID(t *testing.T) {
	raw := richTextRaw(`[
		{"type":"text","text":"前言"},
		{"type":"image","url":"http://x/a.png","name":"a.png","width":100,"height":200}
	]`)
	msg := branchAMsg("2062443880774537216", raw)
	// 构造带非空可见性字段的富文本父消息（branch A 直接消费 msg.SpaceID/msg.Visibles 回填值）。
	msg.SpaceID = "space_42"
	msg.Visibles = []string{"u_a", "u_b"}

	d, err := DocFromMessage(msg)
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	// 父 doc 自身先带上可见性字段（前置校验，确保 fixture 有效）。
	if d.SpaceID != "space_42" || len(d.Visibles) != 2 {
		t.Fatalf("setup: parent must carry spaceId+visibles, got spaceId=%q visibles=%v", d.SpaceID, d.Visibles)
	}
	if len(d.Derivatives) != 1 {
		t.Fatalf("want 1 derivative (single image), got %d", len(d.Derivatives))
	}

	child := d.Derivatives[0]
	if child.SpaceID != d.SpaceID {
		t.Fatalf("child must inherit parent spaceId: got %q want %q", child.SpaceID, d.SpaceID)
	}
	if len(child.Visibles) != len(d.Visibles) {
		t.Fatalf("child visibles length mismatch: got %v want %v", child.Visibles, d.Visibles)
	}
	for i := range d.Visibles {
		if child.Visibles[i] != d.Visibles[i] {
			t.Fatalf("child visibles[%d] mismatch: got %q want %q", i, child.Visibles[i], d.Visibles[i])
		}
	}
}

// TestRichTextDerivatives_TextOnlyNoDerivatives 纯文本富文本（无内嵌媒体）→ 不派生子文档。
func TestRichTextDerivatives_TextOnlyNoDerivatives(t *testing.T) {
	raw := richTextRaw(`[{"type":"text","text":"只有文字"}]`)
	d, err := DocFromMessage(branchAMsg("100", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	if len(d.Derivatives) != 0 {
		t.Fatalf("text-only richtext must have 0 derivatives, got %d", len(d.Derivatives))
	}
}

// TestRichTextDerivatives_NonRichTextNoDerivatives 普通图片消息(type=2)不派生子文档。
func TestRichTextDerivatives_NonRichTextNoDerivatives(t *testing.T) {
	raw := `{"type":2,"url":"http://x/a.png","name":"a.png"}`
	d, err := DocFromMessage(branchAMsg("101", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	if len(d.Derivatives) != 0 {
		t.Fatalf("non-richtext must have 0 derivatives, got %d", len(d.Derivatives))
	}
	// 普通消息 doc subSeq 显式 = 0。
	if d.SubSeq != 0 {
		t.Fatalf("normal doc subSeq want 0, got %d", d.SubSeq)
	}
}

// TestEncodeBulkBody_ParentChildAdjacent 父 doc 后紧跟子 doc 编进同一 body（父子相邻 + 复合 _id）。
func TestEncodeBulkBody_ParentChildAdjacent(t *testing.T) {
	raw := richTextRaw(`[{"type":"image","url":"http://x/a.png","name":"a.png"}]`)
	parent, err := DocFromMessage(branchAMsg("500", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	body, err := encodeBulkBody([]Doc{parent})
	if err != nil {
		t.Fatalf("encodeBulkBody: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	// 父 action+doc + 子 action+doc = 4 行。
	if len(lines) != 4 {
		t.Fatalf("want 4 NDJSON lines (parent+child action/doc), got %d:\n%s", len(lines), string(body))
	}
	if !strings.Contains(lines[0], `"_id":"500"`) {
		t.Fatalf("line0 want parent action _id=500, got %q", lines[0])
	}
	if !strings.Contains(lines[2], `"_id":"500-rt0"`) {
		t.Fatalf("line2 want child action _id=500-rt0, got %q", lines[2])
	}
	// 子文档行带 virtual=true。
	if !bytes.Contains([]byte(lines[3]), []byte(`"virtual":true`)) {
		t.Fatalf("child doc line must carry virtual:true, got %q", lines[3])
	}
}

// TestMapBulkResults_ParentChildOnlyParentReported 父+N子混在响应里，结果只返回父；
// 子全 OK → 父 OK；任一子失败 → 父被判失败（父子原子）。
func TestMapBulkResults_ParentChildOnlyParentReported(t *testing.T) {
	raw := richTextRaw(`[{"type":"image","url":"http://x/a.png"},{"type":"image","url":"http://x/b.png"}]`)
	parent, err := DocFromMessage(branchAMsg("700", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	if len(parent.Derivatives) != 2 {
		t.Fatalf("setup: want 2 derivatives, got %d", len(parent.Derivatives))
	}

	// 全成功：响应 3 项（父+2子）都 201。
	respAllOK := mkBulkResp(201, 201, 201)
	out := mapBulkResults([]Doc{parent}, respAllOK)
	if len(out) != 1 {
		t.Fatalf("result slice must be parent-count (1), got %d", len(out))
	}
	if !out[0].OK {
		t.Fatalf("all-ok bulk: parent must be OK, got %+v", out[0])
	}

	// 子失败（第 2 子 = 第 3 项 400）：父被判失败。
	respChildFail := mkBulkResp(201, 201, 400)
	out2 := mapBulkResults([]Doc{parent}, respChildFail)
	if out2[0].OK {
		t.Fatalf("child-failed bulk: parent must be reported failed (atomic), got OK")
	}
	if out2[0].Status != 400 {
		t.Fatalf("child-failed bulk: parent status want 400 (lifted from child), got %d", out2[0].Status)
	}
}

// TestSubSeq_NormalDocSerializedExplicitly 普通 doc subSeq=0 必须显式序列化出来
// （去 omitempty 的关键意图：reader 不赌「缺失=0」）。
func TestSubSeq_NormalDocSerializedExplicitly(t *testing.T) {
	raw := `{"type":2,"url":"http://x/a.png","name":"a.png"}`
	d, err := DocFromMessage(branchAMsg("900", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Contains(b, []byte(`"subSeq":0`)) {
		t.Fatalf("normal doc must serialize subSeq:0 explicitly (no omitempty), got %s", b)
	}
}

// TestRichTextDerivatives_ClampOversizeWidthHeight 🔴 #26：image block 的 width/height 超出
// ES `integer`（int32）范围时，派生子文档必须把超限值钳为 0（omitempty 不落盘），
// 保证子文档写 _bulk 不会因 int32 溢出 4xx 失败（那会留下可搜孤儿父 + 破坏 DLQ 对账）。
// 正常范围的另一维保留不变。
func TestRichTextDerivatives_ClampOversizeWidthHeight(t *testing.T) {
	// width 超 int32（math.MaxInt32 = 2147483647），height 合法。
	raw := richTextRaw(`[
		{"type":"image","url":"http://x/big.png","name":"big.png","width":9999999999,"height":480}
	]`)
	d, err := DocFromMessage(branchAMsg("2062443880774537300", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	if len(d.Derivatives) != 1 {
		t.Fatalf("want 1 derivative, got %d", len(d.Derivatives))
	}
	img := d.Derivatives[0].Payload.Image
	if img == nil {
		t.Fatalf("derivative image payload nil")
	}
	// 超 int32 的 width 钳为 0。
	if img.Width != 0 {
		t.Fatalf("oversize width must clamp to 0 (int32-safe), got %d", img.Width)
	}
	// 合法 height 保留。
	if img.Height != 480 {
		t.Fatalf("valid height must be preserved, got %d", img.Height)
	}
	// width=0 受 omitempty 不落盘。
	b, err := json.Marshal(d.Derivatives[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytes.Contains(b, []byte(`"width":`)) {
		t.Fatalf("clamped-to-0 width must be omitted from _source, got %s", b)
	}
}

// mkBulkResp 构造一个 _bulk 响应（每个 status 一项 index action）。
func mkBulkResp(statuses ...int) *opensearchapi.BulkResp {
	resp := &opensearchapi.BulkResp{}
	for _, st := range statuses {
		item := opensearchapi.BulkRespItem{Status: st}
		resp.Items = append(resp.Items, map[string]opensearchapi.BulkRespItem{"index": item})
	}
	return resp
}
