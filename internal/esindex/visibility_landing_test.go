package esindex

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// 本文件是方案 B §3.5（visibility 唯一权威解析落点）+ §3.6（gate 语义重定义防漂移）的静态守门，
// 防止弱执行者把 visibility 直读 / fail-OPEN 回归 / stale 注释漂移悄悄带回。

// readSrc 读取本包某源文件正文（去掉 _test.go 自身，避免测试断言串匹配到测试代码）。
func readSrc(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

// funcBody 粗粒度抽取一个顶层函数体（从 "func <name>(" 到下一个顶层 "\nfunc " 或文件末尾）。
func funcBody(src, fnSig string) string {
	idx := strings.Index(src, fnSig)
	if idx < 0 {
		return ""
	}
	rest := src[idx+len(fnSig):]
	if next := strings.Index(rest, "\nfunc "); next >= 0 {
		return rest[:next]
	}
	return rest
}

// TestBuildPayloadFromRaw_NoVisibilityRead 🔴 §3.5 STOP-2 静态守：buildPayloadFromRaw 是纯正文
// 投影，函数体内**绝不**出现 ExtractVisibility / 直读 "visibles" / "space_id"。出现 = fail-OPEN 回归。
func TestBuildPayloadFromRaw_NoVisibilityRead(t *testing.T) {
	body := funcBody(readSrc(t, "buildraw.go"), "func buildPayloadFromRaw(")
	if body == "" {
		t.Fatal("buildPayloadFromRaw not found in buildraw.go")
	}
	for _, banned := range []string{"ExtractVisibility", `"visibles"`, `"space_id"`} {
		if strings.Contains(body, banned) {
			t.Fatalf("§3.5 STOP-2 regression: buildPayloadFromRaw must not reference %q (visibility is parsed only in the consumer/backfill pre-check; this func is pure body projection)", banned)
		}
	}
}

// TestDocFromMessage_NoVisibilityParse 🔴 §3.5：DocFromMessage 只消费回填值，函数体内**不**出现
// ExtractVisibility（唯一权威解析落点在 consumer.processBatch 预检 / backfill docFromRow）。
func TestDocFromMessage_NoVisibilityParse(t *testing.T) {
	body := funcBody(readSrc(t, "doc.go"), "func DocFromMessage(")
	if body == "" {
		t.Fatal("DocFromMessage not found in doc.go")
	}
	if strings.Contains(body, "ExtractVisibility") {
		t.Fatal("§3.5 regression: DocFromMessage must not call ExtractVisibility (single authoritative parse landing point is the consumer pre-check / backfill, not here)")
	}
}

// TestGateComments_NoStaleSchemaV1 🔴 §3.6 S-gate lint（必跑门）：本包源文件不得再出现「契约是 v1 /
// SchemaVersion==1 / 契约仍是 v1」这类 stale 误导注释；且 doc.go 必须出现「安全来自消费侧」语义关键字。
func TestGateComments_NoStaleSchemaV1(t *testing.T) {
	// stale 锚点：宣称契约 v1 / SchemaVersion==1 不带安全字段（误导后人改 schema）。
	staleRe := regexp.MustCompile(`SchemaVersion *== *1|SchemaVersion 1|契约是 v1|契约仍是 v1`)
	// §3.6 点名的 stale 锚点宿主文件（doc.go const/函数注释 + 两个 test 文件）。
	for _, f := range []string{"doc.go", "v2gate_test.go", "safetygate_test.go"} {
		src := readSrc(t, f)
		if loc := staleRe.FindString(src); loc != "" {
			t.Fatalf("§3.6 stale gate comment found in %s (%q): the contract is v2; rewrite to 'safety comes from the consumer-side pre-check'", f, loc)
		}
	}
	// doc.go 必须出现重定义后的语义关键字（安全来自消费侧预检）。
	docSrc := readSrc(t, "doc.go")
	if !strings.Contains(docSrc, "消费侧") || !strings.Contains(docSrc, "ExtractVisibility") {
		t.Fatal("§3.6 gate comment in doc.go must state the visibility safety now comes from the consumer-side ExtractVisibility pre-check")
	}
}
