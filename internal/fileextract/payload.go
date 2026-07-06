package fileextract

// payload.go 提供 file-extractor 内的轻量 payload 解析（只解 type + payload.file 子对象）。
// 不 import internal/esindex（避免循环依赖 + fileextract 保持独立可测）；实现比 esindex/buildraw.go
// 精简得多，只覆盖本服务需要的两件事：
//   1. 判 payload.type 是否 = 8 (File)，非 8 直接跳过
//   2. type=8 时从 raw payload 抽 url/name/extension/size 供下载 + DLQ 记录使用

import (
	"bytes"
	"encoding/json"
)

// PayloadTypeFile 对应 octo-lib common.File 常量值 8（与 esindex/buildraw.go payloadTypeFile 一致）。
// 不 import octo-lib common 避免拖 zap/redis/grpc 200+ 依赖（同 producer/extract.go 复制常量策略）。
const PayloadTypeFile = 8

// filePayload 是本服务用到的 payload.file 子对象字段（对齐 octo-server FilePayload 上传契约）。
type filePayload struct {
	URL       string
	Name      string
	Extension string
	Size      int64
}

// extractContentTypeFile 从 Kafka 消息 RawPayload 里判 type 是否 = 8。
// 返回：
//   - (payload, true)  — 是 type=8 file 消息，payload 已抽字段
//   - (nil,     false) — 不是 file 消息（或 RawPayload 空/损坏，视为非 file 跳过）
//
// 只解顶层对象 + type/url/name/extension/size 5 个字段；不做完整 payload 校验（那是 es-indexer
// 的活）。这样吞吐≈Kafka 消费速度，非 file 消息（占 99.3%）零解析开销。
func extractContentTypeFile(rawPayload []byte) (*filePayload, bool) {
	if len(rawPayload) == 0 {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(rawPayload))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, false
	}
	t, ok := readType(m["type"])
	if !ok || t != PayloadTypeFile {
		return nil, false
	}
	p := &filePayload{}
	if u, ok := m["url"].(string); ok {
		p.URL = u
	}
	if n, ok := m["name"].(string); ok {
		p.Name = n
	}
	if e, ok := m["extension"].(string); ok {
		p.Extension = e
	}
	if sz, ok := readInt64(m["size"]); ok {
		p.Size = sz
	}
	return p, true
}

// readType 兼容 float64 / int / json.Number 三种 JSON 数字解出形态（同 esindex/buildraw.go extractType）。
func readType(v any) (int, bool) {
	switch x := v.(type) {
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return int(n), true
		}
	case float64:
		return int(x), true
	case int:
		return x, true
	case int64:
		return int(x), true
	}
	return 0, false
}

// readInt64 兼容 float64 / int / int64 / json.Number 抽 int64 值。
func readInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n, true
		}
	case float64:
		return int64(x), true
	case int:
		return int64(x), true
	case int64:
		return x, true
	}
	return 0, false
}
