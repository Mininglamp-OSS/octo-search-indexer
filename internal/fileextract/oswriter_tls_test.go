package fileextract

// oswriter_tls_test.go — 验证 ESTLSInsecureSkipVerify 开关正确切换到 esindex.InsecureSkipVerifyTransport。
// 覆盖 v1.13 生产 bug：file-extractor 起来后 mapping-compat gate 校验 OS 时
// x509 "certificate signed by unknown authority" → CrashLoop（file-extractor
// 之前不读 ES_TLS_INSECURE_SKIP_VERIFY env，与 es-indexer 不对称）。

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// TestNewOSWriter_TLSInsecureAllowsSelfSignedServer 证明：ESTLSInsecureSkipVerify=true
// 时 newOSWriter 构造出的 client 能直连 httptest.NewTLSServer (self-signed cert)，
// 校验 helper 已被真正注入到 opensearch.Config.Transport 链上。
func TestNewOSWriter_TLSInsecureAllowsSelfSignedServer(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Update API path: /<index>/_update/<id>
		if !strings.Contains(r.URL.Path, "/_update/") {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, werr := w.Write([]byte(`{"_id":"m1","result":"updated"}`)); werr != nil {
			t.Errorf("write: %v", werr)
		}
	}))
	defer srv.Close()

	cfg := ServiceConfig{
		ESAddresses:             []string{srv.URL},
		ESIndex:                 "octo-message",
		ESTLSInsecureSkipVerify: true,
	}
	w, err := newOSWriter(cfg)
	if err != nil {
		t.Fatalf("newOSWriter: %v", err)
	}
	if err := w.UpdateContent(context.Background(), "m1", "hello", esindex.FileContentMeta{}); err != nil {
		t.Fatalf("UpdateContent against TLS test server: %v", err)
	}
}

// TestNewOSWriter_TLSStrictRejectsSelfSignedServer 反证：ESTLSInsecureSkipVerify=false
// 时使用默认 transport，dial httptest self-signed 服务器必须 x509 校验失败。
// 保护 flag=false 走默认 transport（校验开启），避免未来重构误把 skip 变成默认。
func TestNewOSWriter_TLSStrictRejectsSelfSignedServer(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := ServiceConfig{
		ESAddresses:             []string{srv.URL},
		ESIndex:                 "octo-message",
		ESTLSInsecureSkipVerify: false,
	}
	w, err := newOSWriter(cfg)
	if err != nil {
		t.Fatalf("newOSWriter: %v", err)
	}
	err = w.UpdateContent(context.Background(), "m1", "hello", esindex.FileContentMeta{})
	if err == nil {
		t.Fatalf("expected TLS handshake error against self-signed server, got nil")
	}
	if !strings.Contains(err.Error(), "certificate") && !strings.Contains(err.Error(), "x509") && !strings.Contains(err.Error(), "unknown authority") {
		t.Fatalf("expected x509/certificate error, got: %v", err)
	}
}
