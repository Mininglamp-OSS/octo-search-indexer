package esindex

import (
	"crypto/tls"
	"net/http"
)

// InsecureSkipVerifyTransport 返回一个跳过 TLS 证书校验的 http.RoundTripper，
// 供连接「自签证书 HTTPS OpenSearch」场景使用（由各入口的显式开关控制）。
//
// 它克隆 http.DefaultTransport 后仅覆盖 TLSClientConfig，以保留 Proxy /
// 连接池 / 超时等默认配置；仅当默认 transport 已被替换为非 *http.Transport
// 时退回到一个带 ProxyFromEnvironment 的新 transport（标准运行时不会发生）。
//
// 注意：InsecureSkipVerify 会同时关闭 CA 链与主机名校验，存在 MITM 风险，
// 仅应在受信任的私网 + 显式 opt-in 下使用。
func InsecureSkipVerifyTransport() http.RoundTripper {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // 自签证书场景，由调用方显式开关控制
		}
	}
	t := base.Clone()
	t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // 自签证书场景，由调用方显式开关控制
	return t
}
