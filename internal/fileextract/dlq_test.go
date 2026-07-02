package fileextract

import (
	"encoding/json"
	"testing"
)

// TestDLQReason_ConstantsExist v1.12 §6.1 表格里的 8 种 reason 常量必须存在（避免误删/改名）。
func TestDLQReason_ConstantsExist(t *testing.T) {
	want := []string{
		"parse_error", "oversize", "blacklist_ext",
		"download_failed", "extract_timeout", "encrypted",
		"empty_extract", "extract_error",
	}
	got := []string{
		ReasonParseError, ReasonOversize, ReasonBlacklistExt,
		ReasonDownloadFailed, ReasonExtractTimeout, ReasonEncrypted,
		ReasonEmptyExtract, ReasonExtractError,
	}
	if len(want) != len(got) {
		t.Fatalf("count mismatch: want %d reasons, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("reason[%d]: got %q want %q", i, got[i], want[i])
		}
	}
}

// TestDLQRecord_SerializationRoundtrip dlqRecord 序列化/反序列化字段完整对齐。
func TestDLQRecord_SerializationRoundtrip(t *testing.T) {
	orig := dlqRecord{
		Reason:           ReasonEncrypted,
		Topic:            "octo.message.v1",
		Partition:        3,
		Offset:           12345,
		Key:              []byte("42"),
		Value:            []byte("original message bytes"),
		MessageID:        "42",
		FileURL:          "https://cdn.deepminer.com.cn/x.pdf",
		FileExt:          ".pdf",
		FileSize:         1024,
		Detail:           "org.apache.tika.exception.EncryptedDocumentException",
		SpilledAt:        1727712345,
		PayloadTruncated: false,
	}
	b, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back dlqRecord
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Reason != orig.Reason || back.MessageID != orig.MessageID || back.FileURL != orig.FileURL {
		t.Errorf("round-trip mismatch: got %+v", back)
	}
}

// TestTruncateValueIfNeeded 小 value 不截断；超阈值截断 + 标记 true。
func TestTruncateValueIfNeeded(t *testing.T) {
	small := make([]byte, 1024)
	out, trunc := truncateValueIfNeeded(small)
	if trunc || len(out) != len(small) {
		t.Errorf("small value should not truncate: trunc=%v len=%d", trunc, len(out))
	}
	big := make([]byte, maxDLQRawValueBytes+1)
	out, trunc = truncateValueIfNeeded(big)
	if !trunc || len(out) != maxDLQRawValueBytes {
		t.Errorf("big value should truncate to %d: trunc=%v len=%d", maxDLQRawValueBytes, trunc, len(out))
	}
}
