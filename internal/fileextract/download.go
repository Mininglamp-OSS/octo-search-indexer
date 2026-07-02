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
	"net/http"
	"strings"
	"time"
)

// errOversize 是文件超过 MaxFileSize 阈值（不下载 body 直接返，节省带宽）。
var errOversize = errors.New("fileextract: file size exceeds MaxFileSize cutoff")

// errDownloadFailed 是重试耗尽或 4xx 非 429 permanent 失败（触发 DLQ reason=download_failed）。
var errDownloadFailed = errors.New("fileextract: download exhausted retries or 4xx permanent")

// downloadClient 从 URL 拉文件 bytes。
type downloadClient struct {
	hc           *http.Client
	maxSize      int64
	retries      int
	retryBackoff time.Duration
}

// newDownloadClient 用 stdlib http.Client（Timeout 已含拨号+读体总耗时）。生产 sidecar
// 走公网 CDN，不需要额外 DNS/连接池调优。
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
	return &downloadClient{
		hc:           &http.Client{Timeout: cfg.DownloadTimeout},
		maxSize:      maxSize,
		retries:      retries,
		retryBackoff: backoff,
	}
}

// RetryBackoffBase 是 IDX-4 复用型 backoff base（1s），指数退避 1s / 4s / 16s。
// 挂 ServiceConfig 上便于测试注入更短值加速 test。
func (c ServiceConfig) RetryBackoffBase() time.Duration {
	// 未来加 config field 时改这里；目前固定 1s。
	return time.Second
}

// Fetch 拉 URL 到 bytes 数组。重试策略：transient 错误按 base * 2^attempt 退避，共 retries+1 次尝试。
func (d *downloadClient) Fetch(ctx context.Context, url string) ([]byte, string, error) {
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
	defer resp.Body.Close()

	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return nil, "", fmt.Errorf("cdn transient status %d", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return nil, "", fmt.Errorf("cdn permanent status %d", resp.StatusCode)
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
func isPermanentDownloadErr(err error) bool {
	if err == nil {
		return false
	}
	// 只有 tryFetch 返 "cdn permanent status <N>" 才 permanent
	return strings.Contains(err.Error(), "cdn permanent status")
}
