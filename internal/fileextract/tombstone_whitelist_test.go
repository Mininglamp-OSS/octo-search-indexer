package fileextract

// tombstone_whitelist_test.go — Round-4 Should-Fix (lml2468 review) 回归：
// tombstone 只对 permanent 类 DLQ reason 写；transient 类 (download_failed / extract_timeout)
// 不写，保留 backfill recovery safety net。

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestTombstoneReasons_Whitelist 契约锁：tombstoneReasons 集合必须包含全部**文件本身永久不可抽取**
// 类 reason，且**不含**任何 transient 类 reason。future 加新 DLQ reason 时必须审慎归类。
func TestTombstoneReasons_Whitelist(t *testing.T) {
	// permanent 类必须在白名单内
	permanent := []string{
		ReasonBlacklistExt,
		ReasonOversize,
		ReasonEncrypted,
		ReasonEmptyExtract,
		ReasonExtractError,
	}
	for _, r := range permanent {
		if !isTombstoneReason(r) {
			t.Errorf("reason %q must be in tombstoneReasons whitelist (permanent DLQ), got not tombstoned", r)
		}
	}
	// transient 类必须**不在**白名单内
	transient := []string{
		ReasonDownloadFailed, // CDN 5xx / 网络抖动
		ReasonExtractTimeout, // Tika 临时资源紧张
	}
	for _, r := range transient {
		if isTombstoneReason(r) {
			t.Errorf("reason %q must NOT be in tombstoneReasons (transient can retry via backfill), got tombstoned", r)
		}
	}
	// consumer.processBatch 直接写 DLQ 的 reason (不经 extractor.defer) 白名单里也不能有
	// (避免误导；这些 reason extractor 层永远不返，不会触发 defer)
	notReturnedByExtractor := []string{
		ReasonParseError,     // Kafka unmarshal — doc 不存在 OS 也无需 tombstone
		ReasonRetryExhausted, // in-place retry 耗尽 — transient 保守，让 backfill 重试
		ReasonOSPermanent,    // OS 4xx — 编程 bug；tombstone 由 consumer 层单独处理（audit-flagged）
	}
	for _, r := range notReturnedByExtractor {
		if isTombstoneReason(r) {
			t.Errorf("reason %q is not produced by extractor.ExtractAndWrite; should not appear in whitelist to avoid confusion, got tombstoned", r)
		}
	}
	// 未知 reason 不 tombstone (保守默认)
	if isTombstoneReason("") {
		t.Error("empty reason must not be tombstoned")
	}
	if isTombstoneReason("some_new_unknown_reason") {
		t.Error("unknown reason must default to NOT tombstoned (conservative)")
	}
}

// TestExtractor_TransientDLQ_NoTombstone Round-4 Should-Fix 核心回归：download_failed
// (transient CDN 5xx) 走 permanent DLQ 分支时，**不写** tombstone，保留下次 backfill 兜底能力。
func TestExtractor_TransientDLQ_NoTombstone_DownloadFailed(t *testing.T) {
	var tombstoneCalls atomic.Int32
	var tombstoneBody string
	// mock OS：任何 update 请求都记录（如果 defer 写 tombstone 会被这里捕获）
	osSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tombstoneCalls.Add(1)
		b, rerr := io.ReadAll(r.Body)
		if rerr != nil {
			t.Errorf("read body: %v", rerr)
		}
		tombstoneBody = string(b)
		_, _ = w.Write([]byte(`{"result":"updated"}`)) //nolint:errcheck // test handler write
	}))
	defer osSrv.Close()

	// mock CDN：一直 5xx → extractor 走 download_failed 分支
	cdnSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer cdnSrv.Close()

	cfg := ServiceConfig{
		ESAddresses:            []string{osSrv.URL},
		ESIndex:                "octo-message",
		DownloadTimeout:        200 * time.Millisecond,
		ExtractTimeout:         200 * time.Millisecond,
		MaxFileSize:            1024,
		MaxContentBytes:        1024,
		HTTPRetries:            1, // 加速 test：只 2 次总尝试
		AllowedDownloadHosts:   []string{"127.0.0.1"},
		AllowedDownloadSchemes: []string{"http", "https"},
		SSRFAllowLoopback:      true,
	}
	e, err := NewExtractor(cfg)
	if err != nil {
		t.Fatalf("NewExtractor: %v", err)
	}
	fp := &filePayload{URL: cdnSrv.URL + "/x.pdf", Name: "x.pdf", Extension: ".pdf"}
	reason, cause, err := e.ExtractAndWrite(context.Background(), "42", fp)
	if err != nil {
		t.Fatalf("ExtractAndWrite: err=%v cause=%v", err, cause)
	}
	if reason != ReasonDownloadFailed {
		t.Fatalf("expected download_failed reason, got %q (cause=%v)", reason, cause)
	}
	// 🔴 关键断言：transient reason 不应写 tombstone
	if tombstoneCalls.Load() != 0 {
		t.Errorf("download_failed is transient; tombstone must NOT be written (would break backfill retry), got %d writes body=%s", tombstoneCalls.Load(), tombstoneBody)
	}
}

// TestExtractor_TransientDLQ_NoTombstone_ExtractTimeout Round-4 Should-Fix：extract_timeout
// (Tika 超时) 也不写 tombstone。
func TestExtractor_TransientDLQ_NoTombstone_ExtractTimeout(t *testing.T) {
	var tombstoneCalls atomic.Int32
	osSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tombstoneCalls.Add(1)
		_, _ = w.Write([]byte(`{"result":"updated"}`)) //nolint:errcheck // test handler write
	}))
	defer osSrv.Close()

	cdnSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		if _, werr := w.Write([]byte("pdf bytes")); werr != nil {
			t.Errorf("cdn write: %v", werr)
		}
	}))
	defer cdnSrv.Close()

	// Tika 故意慢 → per-request timeout 触发 → errExtractTimeout → ReasonExtractTimeout
	tikaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer tikaSrv.Close()

	cfg := ServiceConfig{
		ESAddresses:            []string{osSrv.URL},
		ESIndex:                "octo-message",
		TikaURL:                tikaSrv.URL,
		DownloadTimeout:        time.Second,
		ExtractTimeout:         50 * time.Millisecond, // 短到必超
		MaxFileSize:            1024,
		MaxContentBytes:        1024,
		HTTPRetries:            1,
		AllowedDownloadHosts:   []string{"127.0.0.1"},
		AllowedDownloadSchemes: []string{"http", "https"},
		SSRFAllowLoopback:      true,
	}
	e, err := NewExtractor(cfg)
	if err != nil {
		t.Fatalf("NewExtractor: %v", err)
	}
	fp := &filePayload{URL: cdnSrv.URL + "/x.pdf", Name: "x.pdf", Extension: ".pdf"}
	reason, cause, err := e.ExtractAndWrite(context.Background(), "42", fp)
	if err != nil {
		t.Fatalf("ExtractAndWrite: err=%v cause=%v", err, cause)
	}
	if reason != ReasonExtractTimeout {
		t.Fatalf("expected extract_timeout reason, got %q (cause=%v)", reason, cause)
	}
	if !errors.Is(cause, errExtractTimeout) {
		t.Errorf("cause must wrap errExtractTimeout, got %v", cause)
	}
	if tombstoneCalls.Load() != 0 {
		t.Errorf("extract_timeout is transient; tombstone must NOT be written, got %d writes", tombstoneCalls.Load())
	}
}

// TestExtractor_PermanentDLQ_WritesTombstone 交叉验证：permanent 类 (encrypted) 依然写 tombstone
// —— 白名单没误伤 permanent 类。
func TestExtractor_PermanentDLQ_WritesTombstone_Encrypted(t *testing.T) {
	var tombstoneBody string
	osSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, rerr := io.ReadAll(r.Body)
		if rerr != nil {
			t.Errorf("read body: %v", rerr)
		}
		tombstoneBody = string(b)
		_, _ = w.Write([]byte(`{"result":"updated"}`)) //nolint:errcheck // test handler write
	}))
	defer osSrv.Close()

	cdnSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		if _, werr := w.Write([]byte("pdf bytes")); werr != nil {
			t.Errorf("cdn write: %v", werr)
		}
	}))
	defer cdnSrv.Close()

	// Tika 返 500 + EncryptedDocumentException → errEncrypted → ReasonEncrypted (permanent)
	tikaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		if _, werr := w.Write([]byte("org.apache.tika.exception.EncryptedDocumentException: encrypted PDF")); werr != nil {
			t.Errorf("tika write: %v", werr)
		}
	}))
	defer tikaSrv.Close()

	cfg := ServiceConfig{
		ESAddresses:            []string{osSrv.URL},
		ESIndex:                "octo-message",
		TikaURL:                tikaSrv.URL,
		DownloadTimeout:        time.Second,
		ExtractTimeout:         time.Second,
		MaxFileSize:            1024,
		MaxContentBytes:        1024,
		HTTPRetries:            1,
		AllowedDownloadHosts:   []string{"127.0.0.1"},
		AllowedDownloadSchemes: []string{"http", "https"},
		SSRFAllowLoopback:      true,
	}
	e, err := NewExtractor(cfg)
	if err != nil {
		t.Fatalf("NewExtractor: %v", err)
	}
	fp := &filePayload{URL: cdnSrv.URL + "/x.pdf", Name: "x.pdf", Extension: ".pdf"}
	reason, cause, err := e.ExtractAndWrite(context.Background(), "42", fp)
	if err != nil {
		t.Fatalf("ExtractAndWrite: err=%v cause=%v", err, cause)
	}
	if reason != ReasonEncrypted {
		t.Fatalf("expected encrypted reason, got %q (cause=%v)", reason, cause)
	}
	if !strings.Contains(tombstoneBody, `"status":"unextractable"`) {
		t.Errorf("encrypted (permanent) must write tombstone, got body: %s", tombstoneBody)
	}
	if !strings.Contains(tombstoneBody, `"reason":"encrypted"`) {
		t.Errorf("tombstone must carry reason=encrypted, got: %s", tombstoneBody)
	}
}
