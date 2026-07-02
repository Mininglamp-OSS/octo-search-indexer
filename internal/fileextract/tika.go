package fileextract

// tika.go — Tika Server HTTP client（PUT /tika + Accept: text/plain）。
// Tika 官方 docker `apache/tika:3.3.0.0` minimal 镜像 (165MB 压缩) 起服务监听 9998。
//
// 错误分类（v2 §6.1 DLQ reason 对齐）：
//   - HTTP 200 + non-empty body → (content, truncated, nil)
//   - HTTP 200 + empty body     → ("", false, nil)      → 上层判 empty_extract
//   - HTTP 500 body 含 "EncryptedDocumentException" → errEncrypted → DLQ encrypted
//   - HTTP 500 其他             → errExtractGeneric → DLQ extract_error
//   - HTTP 422 (unsupported)   → errExtractGeneric（Tika 明确不支持）
//   - ctx.DeadlineExceeded     → errExtractTimeout → DLQ extract_timeout
//
// 长文本截断：抽出 > MaxContentBytes 时截取 utf-8 边界安全的前缀 + truncated=true。

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

// tika 错误哨兵（sentinel），上层用 errors.Is 分类。
var (
	errEncrypted      = errors.New("fileextract: tika encrypted document")
	errExtractTimeout = errors.New("fileextract: tika extract timeout")
	errExtractGeneric = errors.New("fileextract: tika extract error")
)

// tikaClient 调 Tika Server。
type tikaClient struct {
	hc              *http.Client
	baseURL         string // e.g. "http://localhost:9998"
	timeout         time.Duration
	maxContentBytes int
}

// newTikaClient 构造。HTTP client 复用同 stdlib idiom，Timeout 覆盖整个请求生命周期。
func newTikaClient(cfg ServiceConfig) *tikaClient {
	max := cfg.MaxContentBytes
	if max <= 0 {
		max = 256 * 1024
	}
	url := cfg.TikaURL
	if url == "" {
		url = "http://localhost:9998"
	}
	return &tikaClient{
		hc:              &http.Client{Timeout: cfg.ExtractTimeout},
		baseURL:         url,
		timeout:         cfg.ExtractTimeout,
		maxContentBytes: max,
	}
}

// Extract 上传 file bytes 到 Tika（PUT /tika），返回抽出的纯文本。
// filename 用作 sanitize 后的 Content-Disposition hint；extension 用于反查 MIME 设 Content-Type。
//
// 🔴 生产 blocker：Tika 3.3.0 收到 PUT /tika 请求若不带 Content-Type，会 fallback 到
// EmptyParser 返 0 字节 body（HTTP 200 + Content-Length: 0），Content-Disposition 里的
// filename 后缀 **不参与** Tika parser 选择。因此必须显式送 Content-Type。
// Max 2026-07-02 部 Tika 到 dmwork-test 实测复现。
//
// filename 中的 CR/LF/双引号/控制字符会被剔除，避免 HTTP header 注入（filename 源头是
// octo-server 上传时用户可控字段，未 sanitize 会破坏 Content-Disposition 头或走 CRLF smuggling）。
// extension 为空时 fallback filename 后缀；仍为空 → application/octet-stream 让 Tika auto-detect。
func (t *tikaClient) Extract(ctx context.Context, fileBytes []byte, filename, extension string) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, t.baseURL+"/tika", bytes.NewReader(fileBytes))
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Accept", "text/plain")
	ext := extension
	if ext == "" {
		ext = filepath.Ext(filename)
	}
	req.Header.Set("Content-Type", mimeTypeForExtension(ext))
	if safeName := sanitizeFilename(filename); safeName != "" {
		// Tika 官方 header：Content-Disposition 传文件名 hint
		req.Header.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, safeName))
	}
	resp, err := t.hc.Do(req)
	if err != nil {
		// ctx 取消/超时都视为 timeout，让 caller 走优雅退出（不进 extract_error DLQ）
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", false, errExtractTimeout
		}
		return "", false, fmt.Errorf("%w: %v", errExtractGeneric, err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", false, fmt.Errorf("%w: read body: %v", errExtractGeneric, readErr)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return truncateContent(string(body), t.maxContentBytes)
	case http.StatusInternalServerError:
		// Tika Server 5xx body 是 Java stack trace 纯文本；判 EncryptedDocumentException 字符串。
		// v2 §7 #2：Tika 版本升级 body 格式可能变化 → 本单测锁死该字符串契约，升 Tika 主版本时同步 review。
		if strings.Contains(string(body), "EncryptedDocumentException") {
			return "", false, errEncrypted
		}
		return "", false, fmt.Errorf("%w: tika 500: %s", errExtractGeneric, snippet(body, 200))
	case http.StatusUnprocessableEntity: // 422
		return "", false, fmt.Errorf("%w: tika 422 unsupported media", errExtractGeneric)
	default:
		return "", false, fmt.Errorf("%w: tika status %d", errExtractGeneric, resp.StatusCode)
	}
}

// sanitizeFilename 剔除 filename 中的 CR/LF/双引号/控制字符，防 HTTP header 注入。
// 空 filename → 返 ""（caller 跳过设 Content-Disposition）。
func sanitizeFilename(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == '"' || r == '\r' || r == '\n' || r == '\\' || r < 0x20 || r == 0x7f {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// truncateContent 按 utf-8 边界截断到 maxBytes 之内，避免半个 UTF-8 字符导致后续 IK 分词失效。
// 返回 (content, truncated, nil)。空 content 也返 (nil, false, nil) 由上层判 empty_extract。
func truncateContent(s string, maxBytes int) (string, bool, error) {
	if len(s) <= maxBytes {
		return s, false, nil
	}
	// 从 maxBytes 位置回退到最近的 utf-8 rune 边界
	end := maxBytes
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end], true, nil
}

// snippet 截 body 前 N 字节做错误 detail（避免 Tika 5xx stack trace 塞爆日志）。
func snippet(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// mimeTypeForExtension 反查文件扩展名到 MIME type，用于 Tika PUT 请求的 Content-Type。
// 覆盖 v2 §6 白名单 19 种可抽取扩展名。ext 大小写不敏感，允许带/不带 '.' 前缀。
//
// Fallback 策略（v2 §7 #4 "宁抽多不漏"）：未知扩展名 → application/octet-stream。
// Tika 收到 octet-stream 会走 magic-number auto-detect（AutoDetectParser），能识别文件真实类型
// 后走对应 parser。相比不送 Content-Type（Tika fallback 到 EmptyParser 返 0 字节），auto-detect
// 至少给了一次机会；识别失败也是走正常的 EmptyParser → 由 caller 判 empty_extract 走 DLQ。
func mimeTypeForExtension(ext string) string {
	e := strings.ToLower(strings.TrimSpace(ext))
	if e == "" {
		return "application/octet-stream"
	}
	if !strings.HasPrefix(e, ".") {
		e = "." + e
	}
	switch e {
	case ".pdf":
		return "application/pdf"
	case ".doc":
		return "application/msword"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".xls":
		return "application/vnd.ms-excel"
	case ".xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case ".ppt":
		return "application/vnd.ms-powerpoint"
	case ".pptx":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case ".txt":
		return "text/plain"
	case ".csv":
		return "text/csv"
	case ".rtf":
		return "application/rtf"
	case ".odt":
		return "application/vnd.oasis.opendocument.text"
	case ".ods":
		return "application/vnd.oasis.opendocument.spreadsheet"
	case ".md":
		return "text/markdown"
	case ".html", ".htm":
		return "text/html"
	case ".json":
		return "application/json"
	case ".xml":
		return "application/xml"
	case ".yaml", ".yml":
		return "application/x-yaml"
	default:
		return "application/octet-stream"
	}
}
