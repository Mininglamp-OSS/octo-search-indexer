package esindex

import (
	"net/http"
	"testing"
)

// TestInsecureSkipVerifyTransport 验证 helper 返回的 transport 确实跳过证书校验，
// 且保留了默认 transport 的 Proxy 配置（克隆而非裸构造）。
func TestInsecureSkipVerifyTransport(t *testing.T) {
	rt := InsecureSkipVerifyTransport()
	tr, ok := rt.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", rt)
	}
	if tr.TLSClientConfig == nil || !tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify=true, got %+v", tr.TLSClientConfig)
	}
	// 克隆默认 transport 应保留 Proxy（裸 &http.Transport{} 会丢）。
	if def, ok := http.DefaultTransport.(*http.Transport); ok && def.Proxy != nil && tr.Proxy == nil {
		t.Fatalf("expected cloned transport to preserve Proxy from http.DefaultTransport")
	}
}
