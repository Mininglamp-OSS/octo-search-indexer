package esindex

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// 本文件是方案 B（CDC 式写入）正文投影的核心：照搬 CDC 蓝本
// wukongim-message-indexer/internal/transform/doc.go::ToDoc 的全类型投影（text=1/image=2/
// gif=3/voice=4/video=5/file=8/mergeForward=11/richText=14），命名校准到 reader 契约
// （camelCase + mergeForward.msgs[].from，非 CDC fromUid）。
//
// 🔴 与蓝本的唯一例外（§3.4/§3.5 钉死）：CDC ToDoc 从 raw **直读** visibles/space_id 写进 doc、
// **无 fail-closed**（fail-OPEN）。本文件**绝不照搬**那两段——buildPayloadFromRaw 是**纯正文
// 投影**函数，签名里不含也不返回 spaceId/visibles，函数体内不出现 ExtractVisibility、不读
// raw["visibles"]/raw["space_id"]。visibility 的唯一权威解析落点在 consumer.processBatch 预检
// （实时路径）/ backfill docFromRow（存量路径），见 §3。
//
// 🔴 雪花精度（§5.3 U-3）：解析 RawPayload 及嵌套 mergeForward.msgs[].messageId 必须用
// json.Decoder + UseNumber()，否则默认 float64 会截断 >2^53 的雪花 id。

// payload.type 常量，语义对齐 octo-lib common/msg.go ContentType（与 reader source.go 一致）。
const (
	payloadTypeImage        = 2
	payloadTypeGIF          = 3
	payloadTypeVoice        = 4
	payloadTypeVideo        = 5
	payloadTypeFile         = 8
	payloadTypeMergeForward = 11
	payloadTypeRichText     = 14
)

// channelTypePerson 是 p2p 频道类型（reversePeer 反解对端 To 用），语义对齐 octo-lib
// common.ChannelType Person。
const channelTypePerson uint32 = 1

// videoExtensions 是「文件消息(type=8)按文件名后缀识别为视频」的后缀白名单（照搬蓝本）。
// 命中后投成 video(type=5) 只填 url（对齐原 video 业务协议）；payloadRaw 仍存原始 type=8 对象。
var videoExtensions = map[string]bool{
	"mp4": true, "avi": true, "mov": true, "mkv": true,
	"m4v": true, "flv": true, "wmv": true, "webm": true, "3gp": true, "ts": true,
}

// buildPayloadFromRaw 把原始 payload 整包投影成 reader 可读的 Payload + 留底 rawForStore。
//
// 返回：
//   - payload：typed 投影（type + text/image/gif/voice/video/file/mergeForward/richText 子对象）。
//     payload 非合法 JSON 对象时为 nil（visibility 预检已对非对象 fail-closed 落 DLQ，不会走到这；
//     此处为防御性兜底）。
//   - rawForStore：原始 payload 整包（仅当顶层是 JSON 对象时写，否则 nil；§6.1 形态约束）。
//   - projected：是否产出了至少一个 typed 可搜子对象（RawExcluded 重定义依据，§5.5）。
//
// ⚠️ 本函数是**纯正文投影**：不解析 visibility、不读 visibles/space_id、不产出 To（见文件头）。
func buildPayloadFromRaw(rawPayload json.RawMessage) (payload *Payload, rawForStore json.RawMessage, projected bool) {
	if len(rawPayload) == 0 {
		return nil, nil, false
	}
	raw, ok := decodeObjectUseNumber(rawPayload)
	if !ok {
		// 顶层非 JSON 对象（数组/标量/损坏）：不写 payloadRaw（object mapping 装不下），不投影。
		return nil, nil, false
	}
	// 顶层是对象 → 原样留底（json.RawMessage 内联存储，不索引：mapping payloadRaw enabled:false）。
	rawForStore = rawPayload

	t, ok := extractType(raw)
	if !ok {
		// 合法对象但缺 type：payloadRaw 已留底，Payload 不挂（保持「至少抽到 type 才挂 Payload」语义）。
		return nil, rawForStore, false
	}
	parsed := &Payload{Type: &t}
	switch t {
	case payloadTypeText:
		p := &TextPayload{}
		if c, ok := raw["content"].(string); ok {
			p.Content = c
		}
		parsed.Text = p
	case payloadTypeImage:
		p := &ImagePayload{}
		if u, ok := raw["url"].(string); ok {
			p.URL = u
		}
		if c, ok := raw["caption"].(string); ok {
			p.Caption = c
		}
		if n, ok := raw["name"].(string); ok {
			p.Name = n
		}
		// width/height 钳进 int32（ES `integer`）；异常 payload 超界会触发 _bulk 永久 4xx（见 #26 / #31）。
		if w, ok := extractInt(raw, "width"); ok {
			p.Width = clampInt32(w)
		}
		if h, ok := extractInt(raw, "height"); ok {
			p.Height = clampInt32(h)
		}
		parsed.Image = p
	case payloadTypeGIF:
		p := &GifPayload{}
		if u, ok := raw["url"].(string); ok {
			p.URL = u
		}
		parsed.Gif = p
	case payloadTypeVoice:
		p := &VoicePayload{}
		if u, ok := raw["url"].(string); ok {
			p.URL = u
		}
		parsed.Voice = p
	case payloadTypeVideo:
		p := &VideoPayload{}
		if u, ok := raw["url"].(string); ok {
			p.URL = u
		}
		if c, ok := raw["cover"].(string); ok {
			p.Cover = c
		}
		// width/height/second 均映射为 ES `integer`（int32），异常 payload 超界会触发 _bulk 永久 4xx（见 #31）。
		if w, ok := extractInt(raw, "width"); ok {
			p.Width = clampInt32(w)
		}
		if h, ok := extractInt(raw, "height"); ok {
			p.Height = clampInt32(h)
		}
		if s, ok := extractInt(raw, "second"); ok {
			p.Second = clampInt32(s)
		}
		parsed.Video = p
	case payloadTypeFile:
		// 文件名/extension 后缀命中视频白名单 → 改写成 video(type=5) 只填 url；payloadRaw 不动。
		extName, _ := raw["name"].(string)
		extRaw, _ := raw["extension"].(string)
		if videoExtensions[fileExt(extRaw, extName)] {
			vt := payloadTypeVideo
			parsed.Type = &vt
			v := &VideoPayload{}
			if u, ok := raw["url"].(string); ok {
				v.URL = u
			}
			parsed.Video = v
			break
		}
		p := &FilePayload{}
		if u, ok := raw["url"].(string); ok {
			p.URL = u
		}
		if n, ok := raw["name"].(string); ok {
			p.Name = n
		}
		if c, ok := raw["caption"].(string); ok {
			p.Caption = c
		}
		if sz, ok := extractInt64(raw, "size"); ok {
			p.Size = sz
		}
		if ext, ok := raw["extension"].(string); ok {
			p.Extension = ext
		}
		parsed.File = p
	case payloadTypeMergeForward:
		parsed.MergeForward = buildMergeForward(raw)
	case payloadTypeRichText:
		parsed.RichText = buildRichText(raw)
	}
	// projected = 产出了至少一个 typed 可搜子对象（除了 type 本身）。已知 type 但无子对象
	// （如 gif/voice 仅 url、或未知 type case 未命中）仍按是否挂了子对象判定。
	projected = parsed.hasTypedSubObject()
	return parsed, rawForStore, projected
}

// hasTypedSubObject 报告 Payload 是否挂了至少一个 typed 子对象（用于 RawExcluded 重定义）。
func (p *Payload) hasTypedSubObject() bool {
	if p == nil {
		return false
	}
	return p.Text != nil || p.Image != nil || p.Gif != nil || p.Voice != nil ||
		p.Video != nil || p.File != nil || p.MergeForward != nil || p.RichText != nil
}

// decodeObjectUseNumber 把 payload 字节解成顶层 map[string]any，数字走 json.Number（UseNumber，
// 防雪花精度截断，§5.3）。顶层不是对象（数组/标量/null/损坏）→ ok=false。
func decodeObjectUseNumber(b []byte) (map[string]any, bool) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil || raw == nil {
		return nil, false
	}
	return raw, true
}

// buildMergeForward 把 type=11 raw 里的 msgs([]any) 收敛成 MergeForwardPayload。
// 每条内嵌 msg 取 message_id(snake_case) + from(锚 reader `from`) + timestamp + type + searchText。
// 命名校准：发送人字段写 reader 契约的 from（非 CDC fromUid），但**读取**仍兼容子消息原生
// from_uid（octo payload 内嵌字段名）。元素非 object / 缺字段 → 静默跳过该字段（zero）。
// raw 缺 msgs 或为空 → nil（omitempty 不输出）。
func buildMergeForward(raw map[string]any) *MergeForwardPayload {
	arr, ok := raw["msgs"].([]any)
	if !ok || len(arr) == 0 {
		return nil
	}
	msgs := make([]MergeForwardMsg, 0, len(arr))
	for _, item := range arr {
		sub, ok := item.(map[string]any)
		if !ok {
			continue
		}
		m := MergeForwardMsg{}
		if id, ok := extractInt64(sub, "message_id"); ok {
			m.MessageID = id
		}
		// 发送人：payload 内嵌原生字段名是 from_uid（octo-web MergeforwardContent schema），
		// 投影到 reader 契约字段名 from（PR#425）。
		if v, ok := sub["from_uid"].(string); ok {
			m.From = v
		}
		if ts, ok := extractInt64(sub, "timestamp"); ok {
			m.Timestamp = ts
		}
		subPayload, _ := sub["payload"].(map[string]any)
		if subPayload != nil {
			if t, ok := extractType(subPayload); ok {
				m.Type = t
				m.SearchText = buildSearchText(subPayload, t)
			}
		}
		msgs = append(msgs, m)
	}
	if len(msgs) == 0 {
		return nil
	}
	return &MergeForwardPayload{ChildCount: len(msgs), Msgs: msgs}
}

// searchableTextFields 严格对齐外层 payload.<X> 子对象里 type:text + ik 分词字段，让
// mergeForward 内嵌 msgs[i].searchText 与外层可搜文本字段集一致（照搬蓝本 SearchableTextFields）。
var searchableTextFields = map[int][]string{
	payloadTypeText:  {"content"},
	payloadTypeImage: {"caption", "name"},
	payloadTypeFile:  {"name", "caption"},
}

// buildSearchText 按 searchableTextFields 取字段拼接（空格连接，跳空）。type=14 走
// richTextSearchText 特判（与顶层 buildRichText 同源）。
func buildSearchText(payload map[string]any, t int) string {
	if t == payloadTypeRichText {
		return richTextSearchText(payload)
	}
	keys, ok := searchableTextFields[t]
	if !ok {
		return ""
	}
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if v, ok := payload[k].(string); ok && v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, " ")
}

// richText 占位符与 octo-web / octo-lib 严格对齐（wire-format 不可本地化）。
const (
	richTextImagePlaceholder = "[图片]"
	richTextFilePlaceholder  = "[文件]"
)

// richTextSearchText 是 type=14 可搜文本的单源计算（顶层 buildRichText 与合并转发内嵌共用）：
// plain（优先 raw["plain"]，缺则现场 buildRichTextPlain 回填）+ 各 image/file block 的 name/caption。
func richTextSearchText(raw map[string]any) string {
	blocks := richTextBlocks(raw["content"])
	plain, _ := raw["plain"].(string)
	if strings.TrimSpace(plain) == "" {
		plain = buildRichTextPlain(blocks)
	}
	parts := make([]string, 0, len(blocks)+1)
	if plain != "" {
		parts = append(parts, plain)
	}
	for _, blk := range blocks {
		if blk["type"] == "image" || blk["type"] == "file" {
			if n, ok := blk["name"].(string); ok && n != "" {
				parts = append(parts, n)
			}
			if c, ok := blk["caption"].(string); ok && c != "" {
				parts = append(parts, c)
			}
		}
	}
	return strings.Join(parts, " ")
}

// buildRichText 把 type=14 raw 收敛成 RichTextPayload；searchText 为空 → nil（omitempty 不输出）。
func buildRichText(raw map[string]any) *RichTextPayload {
	searchText := richTextSearchText(raw)
	if searchText == "" {
		return nil
	}
	return &RichTextPayload{SearchText: searchText}
}

// richTextBlocks 归一 content 为 block 数组（[]any 只留 map；string 向后兼容归一为单 text block）。
func richTextBlocks(content any) []map[string]any {
	switch v := content.(type) {
	case []any:
		out := make([]map[string]any, 0, len(v))
		for _, item := range v {
			if blk, ok := item.(map[string]any); ok {
				out = append(out, blk)
			}
		}
		return out
	case string:
		if v == "" {
			return nil
		}
		return []map[string]any{{"type": "text", "text": v}}
	}
	return nil
}

// buildRichTextPlain 遍历 blocks 生成纯文本（text 取 text；image 注 [图片]；file 注 [文件]+名）。
// 仅在 server plain 缺失时回填（server 是 plain 权威源）。
func buildRichTextPlain(blocks []map[string]any) string {
	var sb strings.Builder
	for _, blk := range blocks {
		t, _ := blk["type"].(string)
		text, _ := blk["text"].(string)
		switch t {
		case "image":
			sb.WriteString(richTextImagePlaceholder)
		case "file":
			if name, ok := blk["name"].(string); ok && name != "" {
				sb.WriteString(richTextFilePlaceholder + " " + name)
			} else {
				sb.WriteString(richTextFilePlaceholder)
			}
		case "text":
			sb.WriteString(text)
		default:
			sb.WriteString(text) // 未知 type：有 text 取 text，无则空串
		}
	}
	return sb.String()
}

// reversePeer 从 fakeChannelId("uidA@uidB"，hash 排序) 反解对端 uid。拆不出两段 → 空串。
// 入参取自**契约** ChannelID/From（非 payloadRaw），防恶意 payloadRaw 篡改 To（§5.2 U-2）。
func reversePeer(channelID, from string) string {
	parts := strings.SplitN(channelID, "@", 3)
	if len(parts) != 2 {
		return ""
	}
	a, b := parts[0], parts[1]
	if from == a {
		return b
	}
	return a
}

// fileExt 从文件消息提取小写后缀（照搬蓝本，对齐 octo-web getExtension）：优先 name 取后缀
// （排除 dotfile 前导点 / 尾点），fallback extension 字段。
func fileExt(extension, name string) string {
	if name != "" {
		if dot := strings.LastIndex(name, "."); dot > 0 && dot < len(name)-1 {
			return strings.ToLower(name[dot+1:])
		}
	}
	return strings.ToLower(extension)
}

// extractType 从 payload map 抽 type（兼容 float64/int/json.Number）。
func extractType(payload map[string]any) (int, bool) {
	return extractInt(payload, "type")
}

// extractInt 按 key 抽 int，兼容 JSON 默认 float64 / json.Number / int*。
func extractInt(m map[string]any, key string) (int, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return int(i), true
	}
	return 0, false
}

// extractInt64 按 key 抽 int64，兼容 float64 / json.Number / int* / string。string 分支处理
// 内嵌 msgs[].message_id 在原始 JSON 里是字符串（雪花 ID）的情形。
func extractInt64(m map[string]any, key string) (int64, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	case int64:
		return n, true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return i, true
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	}
	return 0, false
}
