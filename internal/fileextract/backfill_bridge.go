package fileextract

// backfill_bridge.go — 为 internal/filebackfill 提供一个跨包边界安全的 wrapper：
// 因为 filePayload 是本包 unexported struct（保持解析细节内部封装），backfill 复用抽取核心
// 时不能直接构造 *filePayload，故用这个纯值参数的桥梁函数代替。
//
// 语义与 Extractor.ExtractAndWrite 完全一致，只是把 *filePayload 拆成入参字段。

import "context"

// ExtractAndWriteForBackfill 为 backfill 场景包装 Extractor.ExtractAndWrite。
// 返回签名同 Extractor.ExtractAndWrite：
//   - (reason="", cause=nil, err=nil) → 成功
//   - (reason=非空, cause=err, err=nil) → 应投 DLQ
//   - (reason="", cause=nil, err=非空) → OS transient (含 errDocNotYet)，caller 决定重试策略
func ExtractAndWriteForBackfill(ctx context.Context, e *Extractor, messageID, url, name, ext string, size int64) (string, error, error) {
	fp := &filePayload{
		URL:       url,
		Name:      name,
		Extension: ext,
		Size:      size,
	}
	return e.ExtractAndWrite(ctx, messageID, fp)
}
