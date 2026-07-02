package esindex

import (
	"encoding/json"
	"strings"
	"testing"
)

// boolPtr 是 v1.13 P2-5 后 Truncated 变 *bool 的构造辅助。
func boolPtr(b bool) *bool { return &b }

// TestFilePayload_ContentSerialization v1.12：FilePayload 带 Content + ContentMeta 后，
// 序列化字段名/嵌套/类型逐字段对齐 mapping octo-message.json 的 payload.file 段。
func TestFilePayload_ContentSerialization(t *testing.T) {
	fp := &FilePayload{
		URL:       "https://cdn.deepminer.com.cn/im-test-xming/chat/2026Q2.pdf",
		Name:      "2026Q2.pdf",
		Extension: ".pdf",
		Size:      131072,
		Content:   "第二季度营收增长 15%，净利润 5000 万元",
		ContentMeta: &FileContentMeta{
			ExtractedAt: 1727712345,
			Extractor:   "tika/3.3.0",
			Truncated:   nil, // v1.13 P2-5：*bool nil → 走 omitempty 不落盘（bool 零值语义）
			ExtractMs:   187,
		},
	}
	b, err := json.Marshal(fp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	// 校验字段名严格对齐 mapping 声明（大小写/驼峰锁死）。
	for _, want := range []string{
		`"url":`, `"name":`, `"extension":`, `"size":`,
		`"content":"第二季度营收增长 15%，净利润 5000 万元"`,
		`"contentMeta":{`,
		`"extractedAt":1727712345`,
		`"extractor":"tika/3.3.0"`,
		`"extractMs":187`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("marshal missing %q; got: %s", want, s)
		}
	}
	// Truncated=nil 走 omitempty 不落盘。
	if strings.Contains(s, `"truncated":`) {
		t.Errorf("truncated=nil should be omitted by omitempty; got: %s", s)
	}
}

// TestFilePayload_EmptyContentOmitted Content="" + ContentMeta=nil 时字段被 omitempty 剪掉，
// 保证 file-extractor 未跑（或抽出空串走 DLQ 未回写）的 doc _source 里不出现 content/contentMeta。
func TestFilePayload_EmptyContentOmitted(t *testing.T) {
	fp := &FilePayload{
		URL:       "https://cdn.deepminer.com.cn/x.pdf",
		Name:      "x.pdf",
		Extension: ".pdf",
		Size:      1024,
	}
	b, err := json.Marshal(fp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	if strings.Contains(s, `"content":`) {
		t.Errorf("empty Content must be omitted; got: %s", s)
	}
	if strings.Contains(s, `"contentMeta":`) {
		t.Errorf("nil ContentMeta must be omitted; got: %s", s)
	}
}

// TestFileContentMeta_TruncatedTrue Truncated=&true 时字段落盘（非 nil ptr 不被 omitempty 剪掉），
// 保证运维可以从 _source 里读到 "本条 content 被截断"。
func TestFileContentMeta_TruncatedTrue(t *testing.T) {
	m := &FileContentMeta{
		ExtractedAt: 1,
		Extractor:   "tika/3.3.0",
		Truncated:   boolPtr(true),
		ExtractMs:   30000,
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	if !strings.Contains(s, `"truncated":true`) {
		t.Errorf("truncated=true must be present; got: %s", s)
	}
}

// TestFileContentMeta_TruncatedFalseSerializes v1.13 P2-5 回归：*bool false 要显式落盘，
// 让 partial _update 能把 stale true 清成 false（老 bool+omitempty 无法做到）。
func TestFileContentMeta_TruncatedFalseSerializes(t *testing.T) {
	m := &FileContentMeta{
		ExtractedAt: 1,
		Extractor:   "tika/3.3.0",
		Truncated:   boolPtr(false),
		ExtractMs:   30000,
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(b)
	if !strings.Contains(s, `"truncated":false`) {
		t.Errorf("truncated=false must be explicitly serialized (P2-5), got: %s", s)
	}
}

// TestFileContentMeta_RoundTrip 序列化/反序列化 round-trip：字段名/类型全对齐。
func TestFileContentMeta_RoundTrip(t *testing.T) {
	orig := &FileContentMeta{
		ExtractedAt: 1727712345,
		Extractor:   "tika/3.3.0",
		Truncated:   boolPtr(true),
		ExtractMs:   187,
	}
	b, err := json.Marshal(orig)
	if err != nil { // v1.13 CI lint errcheck
		t.Fatalf("marshal: %v", err)
	}
	var back FileContentMeta
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// *bool 不能直接比较，逐字段核（v1.13 P2-5）
	if back.ExtractedAt != orig.ExtractedAt ||
		back.Extractor != orig.Extractor ||
		back.ExtractMs != orig.ExtractMs {
		t.Errorf("round-trip mismatch on primitives: got %+v want %+v", back, *orig)
	}
	if back.Truncated == nil || orig.Truncated == nil || *back.Truncated != *orig.Truncated {
		t.Errorf("round-trip mismatch on Truncated ptr: got %v want %v", back.Truncated, orig.Truncated)
	}
}
