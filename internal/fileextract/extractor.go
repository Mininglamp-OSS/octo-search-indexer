package fileextract

// extractor.go — 抽取核心可复用 struct（IDX-4 从 consumer.processOne 抽出，IDX-5 backfill Job 复用）。
//
// 一个 Extractor 组合 downloadClient + tikaClient + osWriter + 扩展名白名单，暴露一个方法：
// ExtractAndWrite(ctx, messageID, filePayload) → (dlqReason, cause, err)
//   - dlqReason 非空 → 抽取失败，caller 应投 DLQ（consumer 走 kafka DLQ；backfill 走 spill 或 log）
//   - err 非 nil → OS 写失败（含 errDocNotYet），caller 应重试整批（consumer）或跳过该 doc（backfill）

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
)

// Extractor 抽取核心。跨 consumer + backfill 复用。
type Extractor struct {
	download        *downloadClient
	tika            *tikaClient
	os              *osWriter
	maxFileSize     int64
	extractorLabel  string // 写进 contentMeta.extractor (e.g. "tika/3.3.0")
}

// NewExtractor 构造。所有下游 client 均在这里装配，consumer.Service / backfill Runner 各自 New 一份。
func NewExtractor(cfg ServiceConfig) (*Extractor, error) {
	os, err := newOSWriter(cfg)
	if err != nil {
		return nil, err
	}
	return &Extractor{
		download:       newDownloadClient(cfg),
		tika:           newTikaClient(cfg),
		os:             os,
		maxFileSize:    firstNonZero(cfg.MaxFileSize, 20*1024*1024),
		extractorLabel: "tika/3.3.0",
	}, nil
}

func firstNonZero(a, b int64) int64 {
	if a > 0 {
		return a
	}
	return b
}

// ExtractAndWrite 拉文件 → Tika 抽取 → OS partial update。
// 返回：
//   - (dlqReason="", cause=nil, err=nil) → 成功
//   - (dlqReason="oversize"/"blacklist_ext"/... , cause=err, err=nil) → 抽取 permanent 失败，caller 投 DLQ
//   - (dlqReason="", cause=nil, err=errDocNotYet) → OS 主 doc 未落，caller 应触发本批重试
//   - (dlqReason="", cause=nil, err=其他) → OS 或网络 transient 错，caller 决定重试策略
func (e *Extractor) ExtractAndWrite(ctx context.Context, messageID string, fp *filePayload) (dlqReason string, cause error, err error) {
	// 1. 扩展名白名单前置校验（黑名单 → skip 不 DLQ，白名单外 → DLQ blacklist_ext）
	ext := normalizeExt(fp.Extension, fp.Name)
	if isBlacklistedExt(ext) {
		return ReasonBlacklistExt, errors.New("blacklisted extension " + ext), nil
	}
	// 2. size cutoff（>MaxFileSize 直接 DLQ 不下载）
	if fp.Size > 0 && fp.Size > e.maxFileSize {
		return ReasonOversize, errors.New("file size exceeds cutoff"), nil
	}
	// 3. 下载
	body, _, derr := e.download.Fetch(ctx, fp.URL)
	if derr != nil {
		if errors.Is(derr, errOversize) {
			return ReasonOversize, derr, nil
		}
		if errors.Is(derr, errDownloadFailed) {
			return ReasonDownloadFailed, derr, nil
		}
		// context 取消/网络重试耗尽后仍然算 download_failed
		if ctx.Err() != nil {
			return "", nil, ctx.Err()
		}
		return ReasonDownloadFailed, derr, nil
	}
	// 4. Tika 抽取（ext 已 normalize 好，传给 tika 用于设 Content-Type）
	start := time.Now()
	content, truncated, terr := e.tika.Extract(ctx, body, fp.Name, ext)
	extractMs := time.Since(start).Milliseconds()
	if terr != nil {
		switch {
		case errors.Is(terr, errEncrypted):
			return ReasonEncrypted, terr, nil
		case errors.Is(terr, errExtractTimeout):
			return ReasonExtractTimeout, terr, nil
		default:
			return ReasonExtractError, terr, nil
		}
	}
	if content == "" {
		return ReasonEmptyExtract, errors.New("tika returned empty content"), nil
	}
	// 5. OS partial update
	meta := esindex.FileContentMeta{
		ExtractedAt: time.Now().Unix(),
		Extractor:   e.extractorLabel,
		Truncated:   truncated,
		ExtractMs:   extractMs,
	}
	if uerr := e.os.UpdateContent(ctx, messageID, content, meta); uerr != nil {
		// 主 doc 未落 → 上抛让 caller 重试整批（consumer 走 kafka rebalance 重取）
		return "", nil, uerr
	}
	return "", nil, nil
}

// normalizeExt 归一化扩展名：优先信 payload.file.extension 字段（octo-server 上传时磁数存的），
// fallback filename 后缀。全部小写 + 保证前导 '.'。
func normalizeExt(ext, name string) string {
	e := strings.ToLower(strings.TrimSpace(ext))
	if e == "" {
		e = strings.ToLower(filepath.Ext(name))
	}
	if e == "" {
		return ""
	}
	if !strings.HasPrefix(e, ".") {
		e = "." + e
	}
	return e
}

// blacklistedExtensions v2 §6 表格的抽取黑名单（二进制媒体/压缩/图片，抽取无意义）。
var blacklistedExtensions = map[string]bool{
	".mp4": true, ".mov": true, ".avi": true, ".mkv": true, ".webm": true, ".flv": true, ".wmv": true, ".m4v": true,
	".mp3": true, ".wav": true, ".aac": true, ".flac": true, ".ogg": true, ".wma": true, ".m4a": true, ".amr": true,
	".zip": true, ".rar": true, ".7z": true, ".tar": true, ".gz": true, ".bz2": true, ".xz": true,
	".dmg": true, ".pkg": true, ".deb": true, ".rpm": true, ".appimage": true,
	".jpg": true, ".jpeg": true, ".png": true, ".gif": true, ".bmp": true, ".webp": true, ".ico": true,
}

// isBlacklistedExt 判扩展名是否在黑名单。
// **注**：扩展名为空也返 false（v2 §7 #4 "宁抽多不漏"策略，让 Tika 兜底判是否可抽）。
func isBlacklistedExt(ext string) bool {
	return blacklistedExtensions[ext]
}
