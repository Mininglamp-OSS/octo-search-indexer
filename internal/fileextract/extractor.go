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
	"log"
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

// tombstoneReasons 是"文件本身永久不可抽取"类 DLQ 原因集合；只有这些 reason 会写 tombstone
// (contentMeta.status="unextractable")。
//
// Round-4 Should-Fix (lml2468 review)：老实现对**所有** dlqReason 都写 tombstone (包括
// download_failed / extract_timeout 等 transient 类)，永久移除 backfill recovery safety net —
// download_failed 是 CDN 5xx / 网络抖动，extract_timeout 是 Tika 临时资源紧张，下次 rerun 可能
// 成功。写 tombstone 后 backfill scroll `must_not term contentMeta.status=unextractable` 直接
// 跳过 → **永远不再重试**。commit message 声称的"permanent DLQ"列表也漏了 extract_timeout →
// intent vs impl 不一致，本轮修 impl 匹配 intent（不改 intent）。
//
// 白名单只覆盖 extractor.ExtractAndWrite 内部返回的 permanent 类（不含 ParseError /
// RetryExhausted / OSPermanent — 这些 reason 由 consumer.processBatch 直接 writeDLQ，不经
// extractor.defer；ParseError 场景 doc 不存在于 OS 也不需要 tombstone）。
var tombstoneReasons = map[string]bool{
	ReasonBlacklistExt: true, // 扩展名黑名单：永远不该抽
	ReasonOversize:     true, // 文件超大：不会自己变小
	ReasonEncrypted:    true, // 加密 PDF：无密码无解
	ReasonEmptyExtract: true, // Tika 抽出空/纯空白：文件真无文本
	ReasonExtractError: true, // Tika 报 exception：内容/格式问题，重试无益
	// 明确排除：ReasonDownloadFailed（CDN 5xx / 网络抖动，rerun 可能成功）
	// 明确排除：ReasonExtractTimeout（Tika 临时资源紧张 / 大文件超时，rerun 可能成功）
}

// isTombstoneReason 报告某 DLQ reason 是否属于"文件永久不可抽取"类，值得写 tombstone。
// 供 extractor.defer 过滤 transient 类不写 tombstone（保留 backfill recovery safety net）。
func isTombstoneReason(reason string) bool {
	return tombstoneReasons[reason]
}

// ExtractAndWrite 拉文件 → Tika 抽取 → OS partial update。
// 返回：
//   - (dlqReason="", cause=nil, err=nil) → 成功
//   - (dlqReason="oversize"/"blacklist_ext"/... , cause=err, err=nil) → 抽取 permanent 失败，caller 投 DLQ
//   - (dlqReason="", cause=nil, err=errDocNotYet) → OS 主 doc 未落，caller 应触发本批重试
//   - (dlqReason="", cause=nil, err=其他) → OS 或网络 transient 错，caller 决定重试策略
//
// Round-3 Blocker B (yujiawei P1 / Jerry-Xin #2)：permanent DLQ 返回前 defer 写 tombstone
// (contentMeta.status="unextractable" + reason=<dlqReason>)，让 backfill scroll query 通过
// `must_not term contentMeta.status=unextractable` 过滤，避免 rerun 无限重复 DLQ 同一文件。
// tombstone 写失败不阻塞主 DLQ 路径（下次 backfill/consumer 兜底）。
//
// Round-4 Should-Fix (lml2468)：defer 加 isTombstoneReason 白名单过滤，只对**文件本身永久
// 不可抽取**类 reason 写 tombstone；transient 类 (download_failed / extract_timeout) 不写，
// 保留 backfill recovery safety net。
func (e *Extractor) ExtractAndWrite(ctx context.Context, messageID string, fp *filePayload) (dlqReason string, cause error, err error) {
	// Round-3 Blocker B: 命名返回值 + defer 统一处理 tombstone 写入，避免每个 return 点重复代码。
	// Round-4 Should-Fix: 加白名单过滤，只对 permanent 类写 tombstone；transient 类
	// (download_failed / extract_timeout) 保留 backfill recovery 语义。
	// err != nil 分支 (errDocNotYet / OS transient / ctx 取消) 依然不写（caller 会重试整批）。
	defer func() {
		if dlqReason == "" || err != nil {
			return
		}
		if !isTombstoneReason(dlqReason) {
			// transient 类 DLQ reason (download_failed / extract_timeout) 不写 tombstone，
			// 保留 backfill 下次 rerun 兜底重试的能力。
			return
		}
		if terr := e.os.WriteTombstone(ctx, messageID, dlqReason); terr != nil {
			// tombstone 写失败不阻塞：主 DLQ 路径依然让 caller 投 DLQ，下次 backfill 兜底重跑
			// （若主 doc 未落 → 404 = errDocNotYet 也算 tombstone 无法写；等 es-indexer 落主 doc 后
			//  backfill 再次拉到同 doc 时会重新触发 tombstone 写入）。
			log.Printf("file-extractor: WriteTombstone messageID=%s reason=%s failed (backfill will retry): %v",
				messageID, dlqReason, terr)
		}
	}()

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
	if content == "" || strings.TrimSpace(content) == "" {
		// v1.13 P2-7：whitespace-only 也视为 empty_extract（老代码只判 == ""，
		// scanned/empty PDF 常返 "\n\n" / 空格串，会漏过 → 无意义 doc 被误 commit）
		return ReasonEmptyExtract, errors.New("tika returned empty or whitespace-only content"), nil
	}
	// 5. OS partial update
	meta := esindex.FileContentMeta{
		ExtractedAt: time.Now().Unix(),
		Extractor:   e.extractorLabel,
		Truncated:   &truncated, // v1.13 P2-5：指针便于清除 stale true
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
