package fileextract

// ssrf_test.go — Blocker #1 修复回归 test（v1.13）。
// 覆盖 SSRF 双闸门：
//   闸门 1 validateURL：scheme + host allowlist 前置校验（无 net I/O）
//   闸门 2 ssrfRestrictedDialer：dial 时解析 IP + 拒 private/link-local/metadata
//   Redirect 时 ssrfCheckRedirect 重跑闸门 1

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestValidateURL_HTTPSAllowedHost 白名单 host + https → pass。
func TestValidateURL_HTTPSAllowedHost(t *testing.T) {
	err := validateURL("https://cdn.deepminer.com.cn/x.pdf", nil, nil) // 用默认
	if err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
}

// TestValidateURL_HTTPRejectedByScheme http scheme 拒（默认只 https）。
func TestValidateURL_HTTPRejectedByScheme(t *testing.T) {
	err := validateURL("http://cdn.deepminer.com.cn/x.pdf", nil, nil)
	if !errors.Is(err, errSSRFScheme) {
		t.Fatalf("expected errSSRFScheme, got %v", err)
	}
}

// TestValidateURL_UnknownHostRejected 非白名单 host 拒。
func TestValidateURL_UnknownHostRejected(t *testing.T) {
	err := validateURL("https://evil.example.com/x.pdf", nil, nil)
	if !errors.Is(err, errSSRFHost) {
		t.Fatalf("expected errSSRFHost, got %v", err)
	}
}

// TestValidateURL_MetadataHostRejected metadata IP 作为 host 也拒（host allowlist 层拦）。
func TestValidateURL_MetadataHostRejected(t *testing.T) {
	err := validateURL("https://169.254.169.254/latest/meta-data/", nil, nil)
	if !errors.Is(err, errSSRFHost) {
		t.Fatalf("expected errSSRFHost, got %v", err)
	}
}

// TestValidateURL_MalformedURL parse 失败 → errSSRFScheme（不 panic）。
func TestValidateURL_MalformedURL(t *testing.T) {
	err := validateURL("://bad-url", nil, nil)
	if !errors.Is(err, errSSRFScheme) {
		t.Fatalf("expected errSSRFScheme for parse error, got %v", err)
	}
}

// TestValidateURL_EmptyHost 无 host（例如 "https:///path"）→ errSSRFHost。
func TestValidateURL_EmptyHost(t *testing.T) {
	err := validateURL("https:///path", nil, nil)
	if !errors.Is(err, errSSRFHost) {
		t.Fatalf("expected errSSRFHost for empty host, got %v", err)
	}
}

// TestValidateURL_CustomAllowlist env 传入自定义 allowlist → 两个 host 都放行。
func TestValidateURL_CustomAllowlist(t *testing.T) {
	hosts := []string{"a.example.com", "b.example.com"}
	if err := validateURL("https://a.example.com/x", hosts, nil); err != nil {
		t.Fatalf("a.example.com should pass: %v", err)
	}
	if err := validateURL("https://b.example.com/x", hosts, nil); err != nil {
		t.Fatalf("b.example.com should pass: %v", err)
	}
	if err := validateURL("https://c.example.com/x", hosts, nil); !errors.Is(err, errSSRFHost) {
		t.Fatalf("c.example.com should fail: %v", err)
	}
}

// TestIsBlockedIP_CoversAllTargets 逐个校验 isBlockedIP 覆盖所有 SSRF 目标网段。
func TestIsBlockedIP_CoversAllTargets(t *testing.T) {
	blocked := []string{
		"10.0.0.1",             // RFC 1918
		"172.16.0.1",           // RFC 1918
		"192.168.1.1",          // RFC 1918
		"127.0.0.1",            // loopback
		"169.254.169.254",      // cloud metadata (link-local)
		"169.254.1.1",          // link-local
		"0.0.0.0",              // unspecified
		"100.64.0.1",           // CGNAT
		"100.127.255.255",      // CGNAT 上界
		"::1",                  // IPv6 loopback
		"fe80::1",              // IPv6 link-local
		"fc00::1",              // IPv6 ULA
		"fd12:3456:789a::1",    // IPv6 ULA
		"::",                   // IPv6 unspecified
	}
	for _, s := range blocked {
		ip := net.ParseIP(s)
		if !isBlockedIP(ip) {
			t.Errorf("expected %s to be blocked", s)
		}
	}
	allowed := []string{
		"1.1.1.1",              // public
		"8.8.8.8",              // public
		"2606:4700:4700::1111", // Cloudflare IPv6
	}
	for _, s := range allowed {
		ip := net.ParseIP(s)
		if isBlockedIP(ip) {
			t.Errorf("expected %s to NOT be blocked", s)
		}
	}
	// nil IP → 视为 blocked（防御性）
	if !isBlockedIP(nil) {
		t.Errorf("nil IP must be blocked")
	}
}

// TestSSRFRestrictedDialer_PrivateIPBlocked mock resolver 让 host 解析到 private IP → 拒。
// 由于 real DNS 依赖测试环境，本 test 用一个已知会解析到公网的域名 + 断言 dialer 行为
// 走公网路径 —— 无法直接注入 mock resolver，故用 direct-IP address 触发校验。
func TestSSRFRestrictedDialer_DirectPrivateIPBlocked(t *testing.T) {
	dial := ssrfRestrictedDialer(&net.Dialer{Timeout: time.Second}, false)
	// 直接给一个 private IP（DNS 解析步骤会返 [10.0.0.1] 因为已经是 IP literal）
	_, err := dial(context.Background(), "tcp", "10.0.0.1:80")
	if !errors.Is(err, errSSRFPrivateIP) {
		t.Fatalf("expected errSSRFPrivateIP for 10.0.0.1, got %v", err)
	}
}

// TestSSRFRestrictedDialer_MetadataIPBlocked cloud metadata IP 拒（关键 attack vector）。
func TestSSRFRestrictedDialer_MetadataIPBlocked(t *testing.T) {
	dial := ssrfRestrictedDialer(&net.Dialer{Timeout: time.Second}, false)
	_, err := dial(context.Background(), "tcp", "169.254.169.254:80")
	if !errors.Is(err, errSSRFPrivateIP) {
		t.Fatalf("expected errSSRFPrivateIP for cloud metadata, got %v", err)
	}
}

// TestSSRFRestrictedDialer_LoopbackBypassWhenAllowed allowLoopback=true 时放行 127.0.0.1
// （测试专用），但仍拒 10.x/169.254.x（非 loopback 的其他 blocked 类别）。
func TestSSRFRestrictedDialer_LoopbackBypassWhenAllowed(t *testing.T) {
	dial := ssrfRestrictedDialer(&net.Dialer{Timeout: time.Second}, true)
	// 127.0.0.1 应被允许（会真实 dial，因无监听会返 connection refused / no listener 而非 errSSRFPrivateIP）
	_, err := dial(context.Background(), "tcp", "127.0.0.1:1")
	if err != nil && errors.Is(err, errSSRFPrivateIP) {
		t.Fatalf("loopback bypass expected: 127.0.0.1 must pass SSRF, got %v", err)
	}
	// 10.0.0.1 应仍被拒（loopback bypass 不放行其他 private）
	_, err = dial(context.Background(), "tcp", "10.0.0.1:80")
	if !errors.Is(err, errSSRFPrivateIP) {
		t.Fatalf("loopback bypass must NOT allow 10.0.0.1, got %v", err)
	}
	// 169.254.169.254 metadata 也应仍被拒
	_, err = dial(context.Background(), "tcp", "169.254.169.254:80")
	if !errors.Is(err, errSSRFPrivateIP) {
		t.Fatalf("loopback bypass must NOT allow metadata IP, got %v", err)
	}
}

// TestFetch_SSRFPrecheckBlocksMalformedURL Fetch 入口 pre-check 拦 malformed URL，
// 不重试（scheme/host 不变重试无意义）。
func TestFetch_SSRFPrecheckBlocksMalformedURL(t *testing.T) {
	dc := newDownloadClient(ServiceConfig{
		DownloadTimeout: 100 * time.Millisecond,
		HTTPRetries:     3,
		MaxFileSize:     1024,
	})
	_, _, err := dc.Fetch(context.Background(), "http://cdn.deepminer.com.cn/x") // http 被默认 scheme 拒
	if !errors.Is(err, errDownloadFailed) {
		t.Fatalf("expected errDownloadFailed wrapping SSRF, got %v", err)
	}
}

// TestFetch_SSRFDialerBlocksMetadataViaURL 通过 URL 传 metadata IP → dialer 层拒。
// 前置：scheme=https + host=169.254.169.254（要过 validateURL）。默认 hosts 不含
// metadata IP，所以先 pre-check 拒；本 test 通过给 allowlist 加 metadata host 直接绕过闸门 1
// 触发闸门 2 —— 校验 dialer 层的独立拒绝能力。
func TestFetch_SSRFDialerBlocksMetadataViaURL(t *testing.T) {
	dc := newDownloadClient(ServiceConfig{
		DownloadTimeout:        100 * time.Millisecond,
		HTTPRetries:            0, // 不重试加速
		MaxFileSize:            1024,
		AllowedDownloadHosts:   []string{"169.254.169.254"}, // 故意放行 host，让闸门 2 拦
		AllowedDownloadSchemes: []string{"http", "https"},
	})
	_, _, err := dc.Fetch(context.Background(), "http://169.254.169.254/latest/meta-data/")
	if !errors.Is(err, errDownloadFailed) {
		t.Fatalf("expected errDownloadFailed wrapping SSRF dialer block, got %v", err)
	}
	if !strings.Contains(err.Error(), "private/link-local/metadata") {
		t.Fatalf("expected dialer SSRF error mentioning IP block, got %v", err)
	}
}

// TestSSRFCheckRedirect_BlocksNonWhitelistedRedirect redirect 到非白名单 host 时拒。
// 用 httptest 起两个 server：srv1 返 302 → srv2；srv2 不在白名单 → CheckRedirect 拒。
func TestSSRFCheckRedirect_BlocksNonWhitelistedRedirect(t *testing.T) {
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("should not reach")) //nolint:errcheck // handler write; test verifies redirect is blocked
	}))
	defer srv2.Close()

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// redirect 到 srv2（http://127.0.0.1:another_port/）—— 假设 allowlist 只放行
		// specific srv1 host+port 会复杂；简化：只放行 "127.0.0.1" host 但校验 scheme
		http.Redirect(w, r, "https://evil.example.com/target", http.StatusFound)
	}))
	defer srv1.Close()

	dc := newDownloadClient(ServiceConfig{
		DownloadTimeout:        500 * time.Millisecond,
		HTTPRetries:            0,
		MaxFileSize:            1024,
		AllowedDownloadHosts:   []string{"127.0.0.1"},
		AllowedDownloadSchemes: []string{"http", "https"},
		SSRFAllowLoopback:      true,
	})
	_, _, err := dc.Fetch(context.Background(), srv1.URL+"/x")
	if err == nil {
		t.Fatalf("expected redirect to be blocked (evil.example.com not in allowlist)")
	}
	// redirect 拒会走 http.Client.Do 返 err；被 tryFetch 包 err
	if !strings.Contains(err.Error(), "redirect blocked") && !strings.Contains(err.Error(), "url host") {
		t.Fatalf("expected redirect-related SSRF error, got %v", err)
	}
}
