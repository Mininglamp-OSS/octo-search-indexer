package fileextract

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

// mkCfgForTest 造小超时的测试 config，避免测试跑太久。
func mkCfgForTest(maxSize int64) ServiceConfig {
	return ServiceConfig{
		DownloadTimeout: 500 * time.Millisecond,
		ExtractTimeout:  500 * time.Millisecond,
		MaxFileSize:     maxSize,
		MaxContentBytes: 128, // 短便于测截断
		HTTPRetries:     2,   // 3 次尝试
	}
}

// TestDownload_HappyPath GET 200 返 bytes + Content-Type。
func TestDownload_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		w.Write([]byte("%PDF-1.7 hello"))
	}))
	defer srv.Close()
	dc := newDownloadClient(mkCfgForTest(1024))
	body, ct, err := dc.Fetch(context.Background(), srv.URL+"/x.pdf")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(body) != "%PDF-1.7 hello" || ct != "application/pdf" {
		t.Errorf("unexpected: body=%q ct=%q", string(body), ct)
	}
}

// TestDownload_5xxTransientThenSuccess 前两次 500，第三次 200 → 抽取成功（覆盖重试策略）。
func TestDownload_5xxTransientThenSuccess(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			w.WriteHeader(500)
			return
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	cfg := mkCfgForTest(1024)
	// 缩短 backoff 使测试更快
	dc := &downloadClient{
		hc:           &http.Client{Timeout: 500 * time.Millisecond},
		maxSize:      1024,
		retries:      3,
		retryBackoff: 10 * time.Millisecond,
	}
	_ = cfg
	body, _, err := dc.Fetch(context.Background(), srv.URL+"/x")
	if err != nil {
		t.Fatalf("Fetch: %v (calls=%d)", err, calls.Load())
	}
	if string(body) != "ok" {
		t.Errorf("body: %q", string(body))
	}
	if calls.Load() != 3 {
		t.Errorf("expected 3 calls (2 fail + 1 succeed), got %d", calls.Load())
	}
}

// TestDownload_5xxRetryExhausted 三次都 500 → errDownloadFailed。
func TestDownload_5xxRetryExhausted(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(500)
	}))
	defer srv.Close()
	dc := &downloadClient{
		hc:           &http.Client{Timeout: 500 * time.Millisecond},
		maxSize:      1024,
		retries:      2, // 共 3 次尝试
		retryBackoff: 5 * time.Millisecond,
	}
	_, _, err := dc.Fetch(context.Background(), srv.URL+"/x")
	if !errors.Is(err, errDownloadFailed) {
		t.Fatalf("expected errDownloadFailed, got %v", err)
	}
	if calls.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", calls.Load())
	}
}

// TestDownload_4xxPermanent 404 直接 errDownloadFailed，不重试。
func TestDownload_4xxPermanent(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(404)
	}))
	defer srv.Close()
	dc := &downloadClient{
		hc:           &http.Client{Timeout: 500 * time.Millisecond},
		maxSize:      1024,
		retries:      3,
		retryBackoff: 5 * time.Millisecond,
	}
	_, _, err := dc.Fetch(context.Background(), srv.URL+"/x")
	if !errors.Is(err, errDownloadFailed) {
		t.Fatalf("expected errDownloadFailed, got %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("4xx should not retry, got %d attempts", calls.Load())
	}
}

// TestDownload_ContentLengthOversize Content-Length > MaxFileSize → 立即 errOversize，不读 body。
func TestDownload_ContentLengthOversize(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "999999")
		w.WriteHeader(200)
		w.Write(make([]byte, 999999))
	}))
	defer srv.Close()
	dc := newDownloadClient(mkCfgForTest(1024))
	_, _, err := dc.Fetch(context.Background(), srv.URL+"/big")
	if !errors.Is(err, errOversize) {
		t.Fatalf("expected errOversize, got %v", err)
	}
}

// TestDownload_LiedContentLength 服务端谎报 Content-Length（无 header 但 body 超大）→ 兜底截断 + errOversize。
func TestDownload_LiedContentLength(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write(make([]byte, 2048)) // 超过 maxSize=1024
	}))
	defer srv.Close()
	dc := newDownloadClient(mkCfgForTest(1024))
	_, _, err := dc.Fetch(context.Background(), srv.URL+"/x")
	if !errors.Is(err, errOversize) {
		t.Fatalf("expected errOversize, got %v", err)
	}
}

// TestDownload_ContextCancelled ctx 取消 → 立即返 ctx.Err。
func TestDownload_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()
	dc := &downloadClient{
		hc:           &http.Client{Timeout: time.Second},
		maxSize:      1024,
		retries:      3,
		retryBackoff: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	_, _, err := dc.Fetch(ctx, srv.URL+"/x")
	if err == nil {
		t.Fatal("expected error on ctx cancel")
	}
}

// ---- tika client tests ----

// TestTika_Success 200 + body → 内容返回，truncated=false。
func TestTika_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/tika" {
			t.Errorf("unexpected req: %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Accept"); got != "text/plain" {
			t.Errorf("Accept header: got %q", got)
		}
		w.Write([]byte("extracted text 抽出内容"))
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{TikaURL: srv.URL, ExtractTimeout: time.Second, MaxContentBytes: 128})
	content, trunc, err := tc.Extract(context.Background(), []byte("pdf bytes"), "x.pdf", "")
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if content != "extracted text 抽出内容" || trunc {
		t.Errorf("got content=%q trunc=%v", content, trunc)
	}
}

// TestTika_ContentTruncated 抽出内容超过 MaxContentBytes → 截断 + truncated=true。
func TestTika_ContentTruncated(t *testing.T) {
	longText := strings.Repeat("abc", 200) // 600 bytes
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(longText))
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{TikaURL: srv.URL, ExtractTimeout: time.Second, MaxContentBytes: 128})
	content, trunc, err := tc.Extract(context.Background(), []byte("data"), "x.pdf", "")
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if !trunc {
		t.Error("expected truncated=true")
	}
	if len(content) > 128 {
		t.Errorf("content len %d > 128", len(content))
	}
}

// TestTika_Utf8SafeTruncate 中文内容超长截断不产生半字符（unicode/utf8.RuneStart 边界）。
func TestTika_Utf8SafeTruncate(t *testing.T) {
	longChinese := strings.Repeat("中文字符", 100) // 每个 "中" = 3 bytes UTF-8
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(longChinese))
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{TikaURL: srv.URL, ExtractTimeout: time.Second, MaxContentBytes: 50})
	content, trunc, err := tc.Extract(context.Background(), []byte("data"), "x.pdf", "")
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if !trunc {
		t.Error("expected truncated=true")
	}
	// 关键：截断后的字符串必须是合法 UTF-8（不含半字符）
	for i, r := range content {
		if r == 0xFFFD {
			t.Errorf("truncated content has invalid UTF-8 rune at index %d", i)
			break
		}
	}
}

// TestTika_EncryptedDocument500Body 500 body 含 EncryptedDocumentException → errEncrypted。
// v2 §7 #2 关键契约锁：Tika 版本升级 body 格式若变化，本 test 需同步 review。
func TestTika_EncryptedDocument500Body(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		// 真实 Tika 3.3.0 加密 PDF 抛的 Java stack trace 采样：
		w.Write([]byte("org.apache.tika.exception.EncryptedDocumentException: encrypted PDF"))
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{TikaURL: srv.URL, ExtractTimeout: time.Second, MaxContentBytes: 128})
	_, _, err := tc.Extract(context.Background(), []byte("pdf"), "x.pdf", "")
	if !errors.Is(err, errEncrypted) {
		t.Fatalf("expected errEncrypted, got %v", err)
	}
}

// TestTika_500Generic 500 但非 encrypted → errExtractGeneric。
func TestTika_500Generic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("org.apache.tika.exception.TikaException: some other error"))
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{TikaURL: srv.URL, ExtractTimeout: time.Second, MaxContentBytes: 128})
	_, _, err := tc.Extract(context.Background(), []byte("data"), "x.pdf", "")
	if !errors.Is(err, errExtractGeneric) {
		t.Fatalf("expected errExtractGeneric, got %v", err)
	}
}

// TestTika_422Unsupported Tika 明确不支持某格式返 422 → errExtractGeneric。
func TestTika_422Unsupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(422)
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{TikaURL: srv.URL, ExtractTimeout: time.Second, MaxContentBytes: 128})
	_, _, err := tc.Extract(context.Background(), []byte("data"), "x.xyz", "")
	if !errors.Is(err, errExtractGeneric) {
		t.Fatalf("expected errExtractGeneric on 422, got %v", err)
	}
}

// TestTika_Timeout 服务端 sleep > timeout → errExtractTimeout。
func TestTika_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{TikaURL: srv.URL, ExtractTimeout: 100 * time.Millisecond, MaxContentBytes: 128})
	_, _, err := tc.Extract(context.Background(), []byte("data"), "x.pdf", "")
	if err == nil {
		t.Fatal("expected timeout error")
	}
	// http.Client Timeout 超时的 err 可能不是 context.DeadlineExceeded 但会被包装成 errExtractGeneric；
	// 若单元 test 里通过 ctx.WithTimeout 走则是 errExtractTimeout。这里两种都接受。
	if !errors.Is(err, errExtractGeneric) && !errors.Is(err, errExtractTimeout) {
		t.Fatalf("expected errExtractTimeout or errExtractGeneric, got %v", err)
	}
}

// TestTika_EmptyBody 200 空 body → content="" err=nil（让上层判 empty_extract）。
func TestTika_EmptyBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	tc := newTikaClient(ServiceConfig{TikaURL: srv.URL, ExtractTimeout: time.Second, MaxContentBytes: 128})
	content, trunc, err := tc.Extract(context.Background(), []byte("data"), "x.pdf", "")
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if content != "" || trunc {
		t.Errorf("empty body: got content=%q trunc=%v", content, trunc)
	}
}

// ---- extractor tests (end-to-end w/ mock CDN + Tika + OS) ----

// TestExtractor_BlacklistExtDLQ .mp4 扩展名 → DLQ blacklist_ext，不下载。
func TestExtractor_BlacklistExtDLQ(t *testing.T) {
	e := &Extractor{
		download:       &downloadClient{}, // 不会被调
		tika:           &tikaClient{},
		os:             &osWriter{},
		maxFileSize:    1024,
		extractorLabel: "tika/test",
	}
	fp := &filePayload{URL: "http://x/y.mp4", Name: "y.mp4", Extension: ".mp4", Size: 100}
	reason, cause, err := e.ExtractAndWrite(context.Background(), "42", fp)
	if reason != ReasonBlacklistExt {
		t.Errorf("reason: got %q want blacklist_ext", reason)
	}
	if cause == nil || err != nil {
		t.Errorf("cause=%v err=%v", cause, err)
	}
}

// TestExtractor_OversizeDLQ size > cutoff → DLQ oversize，不下载。
func TestExtractor_OversizeDLQ(t *testing.T) {
	e := &Extractor{maxFileSize: 1024, extractorLabel: "tika/test"}
	fp := &filePayload{URL: "http://x/y.pdf", Name: "y.pdf", Extension: ".pdf", Size: 2048}
	reason, _, err := e.ExtractAndWrite(context.Background(), "42", fp)
	if reason != ReasonOversize {
		t.Errorf("reason: got %q want oversize", reason)
	}
	if err != nil {
		t.Errorf("err: %v", err)
	}
}

// TestExtractor_HappyPath 下载 + Tika + OS 全流程走通（mock CDN + Tika + OS）。
func TestExtractor_HappyPath(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pdf bytes"))
	}))
	defer cdn.Close()
	tika := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("extracted content 抽出内容"))
	}))
	defer tika.Close()

	var osCalls atomic.Int32
	os := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		osCalls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"content":"extracted content 抽出内容"`) {
			t.Errorf("update body missing content: %s", string(body))
		}
		if !strings.Contains(string(body), `"doc_as_upsert":false`) {
			t.Errorf("update body must have doc_as_upsert=false: %s", string(body))
		}
		w.Write([]byte(`{"_id":"42","_version":2,"result":"updated","_shards":{"total":2,"successful":2,"failed":0}}`))
	}))
	defer os.Close()

	// 手工装配 Extractor 避开 newOSWriter（真实构造函数会因 http:// 前缀警告但可用）
	cfg := ServiceConfig{
		ESAddresses:     []string{os.URL},
		ESIndex:         "octo-message",
		TikaURL:         tika.URL,
		DownloadTimeout: time.Second,
		ExtractTimeout:  time.Second,
		MaxFileSize:     1024 * 1024,
		MaxContentBytes: 1024 * 1024,
		HTTPRetries:     2,
	}
	e, err := NewExtractor(cfg)
	if err != nil {
		t.Fatalf("NewExtractor: %v", err)
	}
	fp := &filePayload{URL: cdn.URL + "/x.pdf", Name: "x.pdf", Extension: ".pdf", Size: 100}
	reason, cause, err := e.ExtractAndWrite(context.Background(), "42", fp)
	if err != nil {
		t.Fatalf("ExtractAndWrite: err=%v cause=%v", err, cause)
	}
	if reason != "" {
		t.Errorf("expected success, got reason=%q cause=%v", reason, cause)
	}
	if osCalls.Load() != 1 {
		t.Errorf("expected 1 OS call, got %d", osCalls.Load())
	}
}

// TestNormalizeExt 覆盖归一化各种输入形态。
func TestNormalizeExt(t *testing.T) {
	cases := []struct {
		ext, name, want string
	}{
		{".pdf", "x.pdf", ".pdf"},          // 传入的 ext 优先
		{"pdf", "x.pdf", ".pdf"},           // 无前导 dot 加上
		{"PDF", "x.pdf", ".pdf"},           // 大写归一化
		{"", "x.docx", ".docx"},            // fallback filename
		{"", "no_extension_file", ""},      // 均无
		{"  .txt  ", "x.txt", ".txt"},      // trim
	}
	for _, c := range cases {
		if got := normalizeExt(c.ext, c.name); got != c.want {
			t.Errorf("normalizeExt(%q,%q): got %q want %q", c.ext, c.name, got, c.want)
		}
	}
}

// TestIsBlacklistedExt 采样几个扩展名判断黑名单。
func TestIsBlacklistedExt(t *testing.T) {
	cases := []struct {
		ext  string
		want bool
	}{
		{".mp4", true}, {".zip", true}, {".png", true}, {".dmg", true},
		{".pdf", false}, {".docx", false}, {".txt", false}, {".unknown", false}, {"", false},
	}
	for _, c := range cases {
		if got := isBlacklistedExt(c.ext); got != c.want {
			t.Errorf("isBlacklistedExt(%q): got %v want %v", c.ext, got, c.want)
		}
	}
}

// TestTika_FilenameCRLFInjection 🔴 M2 回归：filename 含 CR/LF/双引号/控制字符时
// 必须被 sanitize，Tika HTTP 请求实际收到的 Content-Disposition 不含这些字符。
// filename 源头是 octo-server 上传用户名，未 sanitize 会造 CRLF header smuggling。
func TestTika_FilenameCRLFInjection(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("Content-Disposition")
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	dangerous := []struct {
		in       string
		mustHave string // 期望 sanitize 后 header 里包含的干净部分
		mustNot  []string
	}{
		{
			in:       "attack.pdf\r\nX-Injected: yes",
			mustHave: "attack.pdf",
			// 关键：CRLF 必须被剔除，否则 HTTP header smuggling；X-Injected 关键字本身是合法字符
			// （sanitize 只剔除结构性危险字符，不做关键字黑名单），CRLF 剔除即防住注入。
			mustNot: []string{"\r", "\n"},
		},
		{
			in:       `bad"name.pdf`,
			mustHave: "badname.pdf",
			mustNot:  []string{`"bad"`, `"name`}, // 双引号被剔除
		},
		{
			in:       "\x00\x01evil.pdf",
			mustHave: "evil.pdf",
			mustNot:  []string{"\x00", "\x01"},
		},
		{
			in:       "中文名.pdf", // 合法中文名保留
			mustHave: "中文名.pdf",
			mustNot:  nil,
		},
	}
	for _, c := range dangerous {
		gotHeader = ""
		tc := newTikaClient(ServiceConfig{TikaURL: srv.URL, ExtractTimeout: time.Second, MaxContentBytes: 128})
		_, _, err := tc.Extract(context.Background(), []byte("data"), c.in, "")
		if err != nil {
			t.Errorf("filename=%q: unexpected err: %v", c.in, err)
			continue
		}
		if !strings.Contains(gotHeader, c.mustHave) {
			t.Errorf("filename=%q: header %q missing expected substring %q", c.in, gotHeader, c.mustHave)
		}
		for _, forbidden := range c.mustNot {
			if strings.Contains(gotHeader, forbidden) {
				t.Errorf("filename=%q: header %q must not contain %q", c.in, gotHeader, forbidden)
			}
		}
	}
}

// TestSanitizeFilename 单元覆盖：控制字符/双引号/反斜杠剔除，合法字符保留。
func TestSanitizeFilename(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"foo.pdf", "foo.pdf"},
		{"a\rb\nc", "abc"},
		{`a"b`, "ab"},
		{"a\\b", "ab"},
		{"a\x00b\x1fc\x7fd", "abcd"},
		{"中文 文件.pdf", "中文 文件.pdf"},
	}
	for _, c := range cases {
		if got := sanitizeFilename(c.in); got != c.want {
			t.Errorf("sanitizeFilename(%q): got %q want %q", c.in, got, c.want)
		}
	}
}

// TestMimeTypeForExtension 🔴 生产 blocker 修复配套：19 种 v2 §6 白名单扩展名映射到 Tika 认识的 MIME。
// 未知扩展 / 空 ext → application/octet-stream（Tika 走 magic-number auto-detect）。
// 大小写不敏感，带/不带 '.' 前缀都能识别。
func TestMimeTypeForExtension(t *testing.T) {
	cases := []struct{ in, want string }{
		// Office 全家
		{".pdf", "application/pdf"},
		{".doc", "application/msword"},
		{".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
		{".xls", "application/vnd.ms-excel"},
		{".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
		{".ppt", "application/vnd.ms-powerpoint"},
		{".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
		// 文本 / 富文本
		{".txt", "text/plain"},
		{".csv", "text/csv"},
		{".rtf", "application/rtf"},
		{".md", "text/markdown"},
		// OpenDocument
		{".odt", "application/vnd.oasis.opendocument.text"},
		{".ods", "application/vnd.oasis.opendocument.spreadsheet"},
		// Web / 结构化
		{".html", "text/html"},
		{".htm", "text/html"},
		{".json", "application/json"},
		{".xml", "application/xml"},
		{".yaml", "application/x-yaml"},
		{".yml", "application/x-yaml"},
		// 大小写 + 无点前缀 normalize
		{".PDF", "application/pdf"},
		{"pdf", "application/pdf"},
		{"DOCX", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
		{"  .xlsx  ", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
		// fallback
		{"", "application/octet-stream"},
		{".unknown", "application/octet-stream"},
		{".exe", "application/octet-stream"},
	}
	for _, c := range cases {
		if got := mimeTypeForExtension(c.in); got != c.want {
			t.Errorf("mimeTypeForExtension(%q): got %q want %q", c.in, got, c.want)
		}
	}
}

// TestTika_ContentTypeHeaderSet 🔴 生产 blocker 回归：Tika PUT 请求必须显式送 Content-Type，
// 否则 Tika fallback 到 EmptyParser 返 0 字节。断言不同 filename/extension 组合下 header 值正确。
// Max 2026-07-02 部 Tika 到 dmwork-test 实测复现（送 Content-Disposition filename="x.pdf" 但不送
// Content-Type → Tika 返 200 但 body 为空）。
func TestTika_ContentTypeHeaderSet(t *testing.T) {
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	cases := []struct {
		name      string
		filename  string
		extension string
		want      string
	}{
		// extension 参数优先
		{"extension_wins_over_filename", "anything", ".pdf", "application/pdf"},
		{"extension_without_dot", "anything", "docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
		// extension 空 → 从 filename 后缀 fallback
		{"filename_fallback_pdf", "report.PDF", "", "application/pdf"},
		{"filename_fallback_xlsx", "budget.xlsx", "", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
		{"filename_fallback_pptx", "deck.pptx", "", "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
		// 生产真实场景：URL 里没扩展名，靠 extension 字段兜底
		{"no_ext_in_filename_use_extension", "24DC4FDFD6E7680E18E66359A29F34FDpptx", ".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
		// 未知扩展 → octet-stream（Tika auto-detect）
		{"unknown_ext_fallback", "mystery.xyz", "", "application/octet-stream"},
		// 都为空 → octet-stream
		{"all_empty", "", "", "application/octet-stream"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotCT = ""
			tc := newTikaClient(ServiceConfig{TikaURL: srv.URL, ExtractTimeout: time.Second, MaxContentBytes: 128})
			_, _, err := tc.Extract(context.Background(), []byte("data"), c.filename, c.extension)
			if err != nil {
				t.Fatalf("Extract: %v", err)
			}
			if gotCT != c.want {
				t.Errorf("Content-Type: got %q want %q", gotCT, c.want)
			}
		})
	}
}
