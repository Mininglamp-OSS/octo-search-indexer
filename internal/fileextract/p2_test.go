package fileextract

// p2_test.go — v1.13 P2 修复的定向回归 test（补充 idx4_test.go / retry_test.go / ssrf_test.go
// 未覆盖的 P2 项）：
//   P2-6 isPermanentDownloadErr 使用 errCDNPermanent sentinel（errors.Is 而非字符串匹配）
//   P2-7 empty_extract 覆盖 whitespace-only content
//   P2-9 Tika timeout ctx-driven（parent cancel vs per-request timeout 区分）

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestIsPermanentDownloadErr_UsesSentinel v1.13 P2-6：sentinel-based error 分类
// （老代码用 strings.Contains "cdn permanent status"，重构 err 格式即静默把 4xx 归成 transient）。
// v1.13 §2.3 rereview：删除对 legacy string-form 的兜底断言（生产已无外部构造该字符串路径）。
func TestIsPermanentDownloadErr_UsesSentinel(t *testing.T) {
	// wrap sentinel → 应识别为 permanent
	err := fmt.Errorf("%w: status 404", errCDNPermanent)
	if !isPermanentDownloadErr(err) {
		t.Errorf("wrapped errCDNPermanent must be permanent (via errors.Is): %v", err)
	}
	// nested wrap 也识别
	err2 := fmt.Errorf("outer: %w", err)
	if !isPermanentDownloadErr(err2) {
		t.Errorf("nested wrap must be permanent: %v", err2)
	}
	// 老字符串格式**不再**识别（sentinel-only）：确保未来重构 err 格式不会静默变行为
	err3 := errors.New("cdn permanent status 403")
	if isPermanentDownloadErr(err3) {
		t.Errorf("legacy string-form err must NOT be recognized (sentinel-only, §2.3 rereview): %v", err3)
	}
	// nil / 其他 err 不应识别
	if isPermanentDownloadErr(nil) {
		t.Errorf("nil must not be permanent")
	}
	if isPermanentDownloadErr(errors.New("some transient network err")) {
		t.Errorf("unrelated err must not be permanent")
	}
}

// TestExtractor_WhitespaceOnlyIsEmptyExtract v1.13 P2-7：whitespace-only Tika 输出 → DLQ
// empty_extract（老代码只判 == ""，"\n\n" 或空格串会漏过 → 无意义 content 被误 commit）。
func TestExtractor_WhitespaceOnlyIsEmptyExtract(t *testing.T) {
	cases := []struct {
		name string
		out  string
	}{
		{"empty string", ""},
		{"newlines only", "\n\n"},
		{"spaces only", "   "},
		{"mixed whitespace", " \t\n\r "},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// mock Tika 返 c.out；mock CDN 返 pdf bytes；mock OS 记录写入
			cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("pdf bytes")) //nolint:errcheck // test handler write; not the SUT
			}))
			defer cdn.Close()
			tika := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(c.out)) //nolint:errcheck // test handler write; not the SUT
			}))
			defer tika.Close()
			osHit := false
			os := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				osHit = true
				_, _ = w.Write([]byte(`{"result":"updated"}`)) //nolint:errcheck // test handler write; not the SUT
			}))
			defer os.Close()

			cfg := ServiceConfig{
				ESAddresses:            []string{os.URL},
				ESIndex:                "octo-message",
				TikaURL:                tika.URL,
				DownloadTimeout:        time.Second,
				ExtractTimeout:         time.Second,
				MaxFileSize:            1024,
				MaxContentBytes:        1024,
				HTTPRetries:            2,
				AllowedDownloadHosts:   []string{"127.0.0.1"},
				AllowedDownloadSchemes: []string{"http", "https"},
				SSRFAllowLoopback:      true,
			}
			e, err := NewExtractor(cfg)
			if err != nil {
				t.Fatalf("NewExtractor: %v", err)
			}
			fp := &filePayload{URL: cdn.URL + "/x.pdf", Name: "x.pdf", Extension: ".pdf"}
			reason, cause, err := e.ExtractAndWrite(context.Background(), "42", fp)
			if err != nil {
				t.Fatalf("ExtractAndWrite: err=%v cause=%v", err, cause)
			}
			if reason != ReasonEmptyExtract {
				t.Errorf("reason=%q want empty_extract (whitespace should DLQ, cause=%v)", reason, cause)
			}
			if osHit {
				t.Errorf("OS must NOT be written when content is empty/whitespace")
			}
		})
	}
}

// TestTika_TimeoutFiresDeadlineExceeded v1.13 P2-9：per-request context.WithTimeout 触发时，
// err 正确分类为 errExtractTimeout（老代码用 http.Client.Timeout 时 ctx.Err()=nil，分类为
// errExtractGeneric → DLQ extract_error，扰乱 timeout 统计与 retry 语义）。
func TestTika_TimeoutFiresDeadlineExceeded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 服务端故意慢，让 Tika client per-request timeout 触发
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte("late")) //nolint:errcheck // test handler write; not the SUT
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{
		TikaURL:         srv.URL,
		ExtractTimeout:  50 * time.Millisecond, // 短到必超
		MaxContentBytes: 128,
	})
	_, _, err := tc.Extract(context.Background(), []byte("x"), "x.pdf", ".pdf")
	if !errors.Is(err, errExtractTimeout) {
		t.Fatalf("per-request timeout must map to errExtractTimeout, got %v", err)
	}
}

// TestTika_ParentCancelDistinctFromTimeout v1.13 P2-9：parent ctx 取消（SIGTERM 场景）→
// 上抛 ctx.Err()（context.Canceled），**不** map 成 errExtractTimeout。
// 保证 caller 能区分"外部关停"与"本次 Tika 慢"。
func TestTika_ParentCancelDistinctFromTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte("late")) //nolint:errcheck // test handler write; not the SUT
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{
		TikaURL:         srv.URL,
		ExtractTimeout:  time.Second, // 够长，不因 timeout 触发
		MaxContentBytes: 128,
	})
	parentCtx, cancel := context.WithCancel(context.Background())
	// 让 parent 在 50ms 后取消 → 应比 per-request timeout（1s）先触发
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, _, err := tc.Extract(parentCtx, []byte("x"), "x.pdf", ".pdf")
	// 应返 ctx.Canceled 而非 errExtractTimeout
	if errors.Is(err, errExtractTimeout) {
		t.Errorf("parent cancel must NOT map to errExtractTimeout, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("parent cancel must surface as context.Canceled, got %v", err)
	}
}

// TestTika_LimitReadResponse v1.13 P2-4：Tika 谎报大 body → io.LimitReader 兜住不 OOM。
// 服务端返 100KB body，client MaxContentBytes=1KB → 只应读 1KB+4 字节（LimitReader 上限）。
func TestTika_LimitReadResponse(t *testing.T) {
	huge := make([]byte, 100*1024)
	for i := range huge {
		huge[i] = 'a'
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(huge) //nolint:errcheck // test handler write; not the SUT
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{
		TikaURL:         srv.URL,
		ExtractTimeout:  time.Second,
		MaxContentBytes: 1024, // 1KB
	})
	content, truncated, err := tc.Extract(context.Background(), []byte("x"), "x.pdf", ".pdf")
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if !truncated {
		t.Errorf("truncated flag should be true (LimitReader wraps read past cap)")
	}
	// 抽出 content 应 <= MaxContentBytes（LimitReader 严格截 + truncateContent 处理 utf8 边界）
	if len(content) > 1024 {
		t.Errorf("content len=%d exceeds MaxContentBytes=1024 (LimitReader failed)", len(content))
	}
}
