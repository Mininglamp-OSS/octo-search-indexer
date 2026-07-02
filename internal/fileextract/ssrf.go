package fileextract

// ssrf.go — URL 前置校验 + SSRF-restricted transport（v1.13 Blocker #1 fix）。
// 双闸门防御 SSRF：
//   1. validateURL：scheme ∈ AllowedDownloadSchemes + host ∈ AllowedDownloadHosts
//      前置校验（无 net I/O，最快拒绝）
//   2. newSSRFRestrictedDialer：dial 时解析 IP，拒 private/link-local/loopback/metadata
//      （防 DNS rebinding + 白名单绕过）
// Redirect 时 http.Client.CheckRedirect 重跑 validateURL（防跳板攻击）。

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// SSRF 校验错误 sentinel，供 errors.Is 分类。
var (
	// errSSRFScheme：URL scheme 不在白名单（默认只允 https）。
	errSSRFScheme = errors.New("fileextract: url scheme not allowed")
	// errSSRFHost：URL host 不在白名单（默认只允 cdn.deepminer.com.cn）。
	errSSRFHost = errors.New("fileextract: url host not in allowlist")
	// errSSRFPrivateIP：dial 解析后的 IP 是 private/link-local/loopback/metadata。
	errSSRFPrivateIP = errors.New("fileextract: resolved IP is private/link-local/metadata")
)

// defaultAllowedDownloadHosts 是 SSRF host allowlist 的默认值（cfg 未设时用）。
// 生产 file 消息 payload.file.url 由 octo-server 上传时写入，全部来自公网 CDN
// cdn.deepminer.com.cn。future 切内网 COS 时通过 env ALLOWED_DOWNLOAD_HOSTS 扩展。
var defaultAllowedDownloadHosts = []string{"cdn.deepminer.com.cn"}

// defaultAllowedDownloadSchemes 是 URL scheme 白名单默认值。生产 CDN 走 https。
var defaultAllowedDownloadSchemes = []string{"https"}

// validateURL 前置校验 URL（scheme + host allowlist）。无 net I/O。
// allowedHosts / allowedSchemes 为空时用 default。
func validateURL(rawURL string, allowedHosts, allowedSchemes []string) error {
	if len(allowedHosts) == 0 {
		allowedHosts = defaultAllowedDownloadHosts
	}
	if len(allowedSchemes) == 0 {
		allowedSchemes = defaultAllowedDownloadSchemes
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%w: parse: %v", errSSRFScheme, err)
	}
	// scheme 校验（大小写不敏感）
	scheme := strings.ToLower(u.Scheme)
	schemeOK := false
	for _, s := range allowedSchemes {
		if scheme == strings.ToLower(s) {
			schemeOK = true
			break
		}
	}
	if !schemeOK {
		return fmt.Errorf("%w: got %q, allowed %v", errSSRFScheme, u.Scheme, allowedSchemes)
	}
	// host 校验：Hostname 去 port 后比较（allowlist 里存主机名，不含 port）
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return fmt.Errorf("%w: empty host in %q", errSSRFHost, rawURL)
	}
	hostOK := false
	for _, h := range allowedHosts {
		if host == strings.ToLower(strings.TrimSpace(h)) {
			hostOK = true
			break
		}
	}
	if !hostOK {
		return fmt.Errorf("%w: %q not in allowlist %v", errSSRFHost, host, allowedHosts)
	}
	return nil
}

// isBlockedIP 判 IP 是否在 SSRF 黑名单：
//   - private (RFC 1918)：10./172.16-31./192.168.
//   - loopback：127./ ::1
//   - link-local：169.254./ fe80::/10（含 cloud metadata 169.254.169.254）
//   - unspecified：0.0.0.0 / ::
//   - CGNAT：100.64.0.0/10（Go IsPrivate 不含）
//   - IPv6 ULA：fc00::/7（Go IsPrivate 不含）
//
// net.IP.IsPrivate 只覆盖 RFC 1918，本函数显式扩展覆盖所有 SSRF 目标网段。
func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true // 无效 IP 拒
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsInterfaceLocalMulticast() || ip.IsPrivate() {
		return true
	}
	// CGNAT 100.64.0.0/10
	if v4 := ip.To4(); v4 != nil {
		if v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
			return true
		}
	}
	// IPv6 ULA fc00::/7
	if len(ip) == net.IPv6len && (ip[0]&0xfe) == 0xfc {
		return true
	}
	return false
}

// ssrfRestrictedDialer 返回一个 DialContext，dial 前把 host 解析成 IP 集合，任一 IP 在
// SSRF 黑名单则拒绝；防 DNS rebinding（第一次解析走白名单，dial 时另一 IP 已被切走）。
//
// 传入的 base 是底层 net.Dialer（Timeout/KeepAlive 等），本函数只在其外面套 IP 校验层。
// allowLoopback：**仅测试用**，httptest server 走 127.0.0.1 需打开；生产必须 false。
func ssrfRestrictedDialer(base *net.Dialer, allowLoopback bool) func(ctx context.Context, network, addr string) (net.Conn, error) {
	if base == nil {
		base = &net.Dialer{Timeout: 30 * time.Second}
	}
	resolver := net.DefaultResolver
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("fileextract: split host port: %w", err)
		}
		ips, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("fileextract: resolve %s: %w", host, err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("%w: no IPs resolved for %s", errSSRFPrivateIP, host)
		}
		// 任一 IP 在黑名单 → 拒绝（保守：不选剩下的白名单 IP dial，防 DNS 双重返回）
		for _, ipAddr := range ips {
			if isBlockedIP(ipAddr.IP) {
				// allowLoopback + IP 只是 loopback（非其他 blocked 类）时放行
				if allowLoopback && ipAddr.IP.IsLoopback() && !isNonLoopbackBlocked(ipAddr.IP) {
					continue
				}
				return nil, fmt.Errorf("%w: %s → %s", errSSRFPrivateIP, host, ipAddr.IP)
			}
		}
		// 选第一个白名单 IP 直接 dial（避免 Transport 内再解析一次触发 TOCTOU）
		return base.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}
}

// isNonLoopbackBlocked 判 IP 是否是"非 loopback 的"其他 blocked 类别（private/link-local/
// metadata 等）。用于 SSRFAllowLoopback 场景下仍要拒非 loopback 的 blocked IP。
func isNonLoopbackBlocked(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() {
		return false
	}
	return isBlockedIP(ip)
}

// ssrfCheckRedirect 是 http.Client.CheckRedirect：redirect 时重跑 validateURL，
// 防跳板攻击（第一跳合法，redirect 到 metadata IP 或非白名单 host）。
func ssrfCheckRedirect(allowedHosts, allowedSchemes []string) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if err := validateURL(req.URL.String(), allowedHosts, allowedSchemes); err != nil {
			return fmt.Errorf("redirect blocked: %w", err)
		}
		if len(via) >= 10 { // Go 默认上限也是 10
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
}
