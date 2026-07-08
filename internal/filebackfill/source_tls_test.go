package filebackfill

// source_tls_test.go — 验证 filebackfill.Config.ESTLSInsecureSkipVerify 正确传到
// newOSScrollSource 的 opensearch.Transport，backfill Job 也能连自签证书 OS。
// 与 internal/fileextract/oswriter_tls_test.go 同一 bug 覆盖（PR-C）。

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestNewOSScrollSource_TLSInsecureAllowsSelfSignedServer 证明：
// ESTLSInsecureSkipVerify=true 时 scroll source 能连 httptest.NewTLSServer。
func TestNewOSScrollSource_TLSInsecureAllowsSelfSignedServer(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 首批 scroll = POST /<index>/_search?scroll=<TTL>
		w.Header().Set("Content-Type", "application/json")
		if _, werr := w.Write([]byte(`{"_scroll_id":"s1","hits":{"hits":[]}}`)); werr != nil {
			t.Errorf("write: %v", werr)
		}
	}))
	defer srv.Close()

	cfg := Config{
		ESAddresses:             []string{srv.URL},
		ESIndex:                 "octo-message",
		ESTLSInsecureSkipVerify: true,
	}
	src, err := newOSScrollSource(cfg)
	if err != nil {
		t.Fatalf("newOSScrollSource: %v", err)
	}
	if _, err := src.Next(context.Background()); err != nil {
		t.Fatalf("Next against TLS test server: %v", err)
	}
}

// TestNewOSScrollSource_TLSStrictRejectsSelfSignedServer 反证：flag=false 走默认 transport，
// 必须因 x509 校验失败而报错。防未来重构误把 skip 变默认。
func TestNewOSScrollSource_TLSStrictRejectsSelfSignedServer(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := Config{
		ESAddresses:             []string{srv.URL},
		ESIndex:                 "octo-message",
		ESTLSInsecureSkipVerify: false,
	}
	src, err := newOSScrollSource(cfg)
	if err != nil {
		t.Fatalf("newOSScrollSource: %v", err)
	}
	_, err = src.Next(context.Background())
	if err == nil {
		t.Fatalf("expected TLS handshake error, got nil")
	}
	if !strings.Contains(err.Error(), "certificate") && !strings.Contains(err.Error(), "x509") && !strings.Contains(err.Error(), "unknown authority") {
		t.Fatalf("expected x509/certificate error, got: %v", err)
	}
}

// TestConfig_ToExtractorConfig_ForwardsTLSFlag 证明 backfill Config → fileextract.ServiceConfig
// 时 ESTLSInsecureSkipVerify 被转发（防漏字段导致 backfill Job 的 extractor 侧写 OS 又崩）。
func TestConfig_ToExtractorConfig_ForwardsTLSFlag(t *testing.T) {
	cfg := Config{ESTLSInsecureSkipVerify: true}
	out := cfg.ToExtractorConfig()
	if !out.ESTLSInsecureSkipVerify {
		t.Fatalf("expected ESTLSInsecureSkipVerify=true forwarded, got false")
	}
}
