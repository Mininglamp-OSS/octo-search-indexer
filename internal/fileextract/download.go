package fileextract

// download.go — 从 CDN URL 拉文件 bytes，带超时 + 指数退避重试 + size cutoff。
// 走公网 CDN (cdn.deepminer.com.cn) 直连（v2 §16.2 决策 (d)，Max prod 实测通过）。
//
// 错误分类：
//   - HTTP 200 → (bytes, contentType, nil)
//   - HTTP 200 但 Content-Length > MaxFileSize → errOversize（permanent, DLQ reason=oversize）
//   - HTTP 5xx / net 超时 / DNS 失败 → transient，触发指数退避重试
//   - HTTP 4xx (非 429) / 3 次重试耗尽 → errDownloadFailed（permanent, DLQ reason=download_failed）
//   - ctx 取消 → ctx.Err() 立即返

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// errCDNPermanent 是 tryFetch 遇到 CDN 4xx (非 429) 返回的 sentinel（v1.13 P2-6）。
// 老代码用字符串 "cdn permanent status" 分类，重构 err.Error() 格式即静默把 4xx 归成 transient
// 丢消息。改成 sentinel + errors.Is，无字符串耦合。
var errCDNPermanent = errors.New("fileextract: cdn permanent status")

// errOversize 是文件超过 MaxFileSize 阈值（不下载 body 直接返，节省带宽）。
var errOversize = errors.New("fileextract: file size exceeds MaxFileSize cutoff")

// errDownloadFailed 是重试耗尽或 4xx 非 429 permanent 失败（触发 DLQ reason=download_failed）。
var errDownloadFailed = errors.New("fileextract: download exhausted retries or 4xx permanent")

// errInvalidURL 是 SSRF pre-check（validateURL）拒绝 URL 的 sentinel（触发 DLQ reason=invalid_url）。
// 与 errDownloadFailed 分开：pre-check 拒是 policy 硬约束（scheme/host allowlist 不变，同 URL
// 无论重试或 backfill rerun 结果不变），归 permanent tombstone；errDownloadFailed 保留给
// CDN 5xx / 4xx expired 等 transient/URL-specific 故障（rerun 可能成功，不写 tombstone）。
var errInvalidURL = errors.New("fileextract: url rejected by SSRF pre-check")

// downloadClient 从 URL 拉文件 bytes。
type downloadClient struct {
	hc             *http.Client
	maxSize        int64
	retries        int
	retryBackoff   time.Duration
	allowedHosts   []string // v1.13 Blocker #1：SSRF host allowlist（pre-check）
	allowedSchemes []string
}

// newDownloadClient 用 stdlib http.Client（Timeout 已含拨号+读体总耗时）。
// v1.13 Blocker #1：Transport 挂 SSRF-restricted dialer + CheckRedirect 校验，防
// (1) 消息 payload.file.url 指向 metadata IP (169.254.169.254) 或内网服务
// (2) DNS rebinding 绕过 host allowlist
// (3) redirect 到 blocked host / IP
func newDownloadClient(cfg ServiceConfig) *downloadClient {
	maxSize := cfg.MaxFileSize
	if maxSize <= 0 {
		maxSize = 20 * 1024 * 1024
	}
	retries := cfg.HTTPRetries
	if retries <= 0 {
		retries = 3
	}
	backoff := cfg.RetryBackoffBase()
	allowedHosts := cfg.AllowedDownloadHosts
	allowedSchemes := cfg.AllowedDownloadSchemes
	baseDialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		DialContext:         ssrfRestrictedDialer(baseDialer, cfg.SSRFAllowLoopback),
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	return &downloadClient{
		hc: &http.Client{
			Timeout:       cfg.DownloadTimeout,
			Transport:     transport,
			CheckRedirect: ssrfCheckRedirect(allowedHosts, allowedSchemes),
		},
		maxSize:        maxSize,
		retries:        retries,
		retryBackoff:   backoff,
		allowedHosts:   allowedHosts,
		allowedSchemes: allowedSchemes,
	}
}

// RetryBackoffBase 是 IDX-4 复用型 backoff base（1s），指数退避 1s / 4s / 16s。
// 挂 ServiceConfig 上便于测试注入更短值加速 test。
func (c ServiceConfig) RetryBackoffBase() time.Duration {
	// 未来加 config field 时改这里；目前固定 1s。
	return time.Second
}

// Fetch 拉 URL 到 bytes 数组。重试策略：transient 错误按 base * 2^attempt 退避，共 retries+1 次尝试。
// v1.13 Blocker #1：入口前置 SSRF 校验，scheme/host 不合法直接返 errDownloadFailed（不重试，
// URL 不变重试无意义）。
func (d *downloadClient) Fetch(ctx context.Context, url string) ([]byte, string, error) {
	if err := validateURL(url, d.allowedHosts, d.allowedSchemes); err != nil {
		// SSRF pre-check 拒绝 → invalid_url（不重试；host/scheme allowlist 是硬约束，同 URL rerun 无益）。
		// 归 permanent tombstone reason（extractor.go tombstoneReasons），backfill scroll 会过滤，
		// 避免老 URL（如已下线的历史 COS 直连）被 rerun 无限重试。
		return nil, "", fmt.Errorf("%w: %v", errInvalidURL, err)
	}
	var lastErr error
	for attempt := 0; attempt <= d.retries; attempt++ {
		body, ct, err := d.tryFetch(ctx, url)
		if err == nil {
			return body, ct, nil
		}
		// ctx 取消快速返，caller 走优雅退出（不消耗 backoff 预算）
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, "", err
		}
		// permanent 错误立即返，不重试
		if errors.Is(err, errOversize) {
			return nil, "", err
		}
		if isPermanentDownloadErr(err) {
			return nil, "", errDownloadFailed
		}
		lastErr = err
		if attempt == d.retries {
			break
		}
		wait := d.retryBackoff * time.Duration(1<<attempt) // 1s / 2s / 4s / 8s (base=1s, retries=3 → 1s/2s/4s)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	return nil, "", fmt.Errorf("%w: %v", errDownloadFailed, lastErr)
}

// tryFetch 单次 GET 尝试。返回：
//   - 成功：(body, contentType, nil)
//   - 文件超大：(nil, "", errOversize)（permanent）
//   - transient (5xx/net 错)：(nil, "", 具体 err)
//   - permanent (4xx 非 429)：(nil, "", 具体 err)
func (d *downloadClient) tryFetch(ctx context.Context, url string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := d.hc.Do(req)
	if err != nil {
		// ctx 取消/超时快速返，caller 不必再走 backoff
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, "", ctxErr
		}
		return nil, "", err // 网络/超时都进 transient 分支
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // close-on-read: nothing to do with close err

	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return nil, "", fmt.Errorf("cdn transient status %d", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		// v1.13 P2-6：用 sentinel errCDNPermanent（下方 isPermanentDownloadErr 走 errors.Is）
		return nil, "", fmt.Errorf("%w: status %d", errCDNPermanent, resp.StatusCode)
	}
	if resp.ContentLength > d.maxSize {
		return nil, "", errOversize
	}
	// 限流 body 读取避免读到 > maxSize+1 字节浪费内存（服务端谎报 Content-Length 的兜底）
	lr := io.LimitReader(resp.Body, d.maxSize+1)
	body, err := io.ReadAll(lr)
	if err != nil {
		return nil, "", err
	}
	if int64(len(body)) > d.maxSize {
		return nil, "", errOversize
	}
	return body, resp.Header.Get("Content-Type"), nil
}

// isPermanentDownloadErr 判 err 是否 permanent（4xx 非 429，不重试）。
// v1.13 P2-6：改 sentinel + errors.Is，不依赖 err.Error() 字符串（重构风险）。
func isPermanentDownloadErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, errCDNPermanent)
}
