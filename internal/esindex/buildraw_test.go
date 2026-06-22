package esindex

import (
	"encoding/json"
	"testing"
)

// readerProjDoc 镜像 octo-server reader source.go::Doc 的全 payload 投影（驱动「写入→reader 反序列化」
// 契约断言）。用 reader 的 json tag 反序列化 indexer 写出的 doc。
type readerProjDoc struct {
	MessageID   int64  `json:"messageId"`
	To          string `json:"to"`
	RawExcluded bool   `json:"rawExcluded"`
	Payload     *struct {
		Type  *int                      `json:"type"`
		Text  *struct{ Content string } `json:"text"`
		Image *struct {
			URL, Caption, Name string
			Width, Height      int
		} `json:"image"`
		Gif   *struct{ URL string } `json:"gif"`
		Voice *struct{ URL string } `json:"voice"`
		Video *struct {
			URL, Cover            string
			Width, Height, Second int
		} `json:"video"`
		File *struct {
			URL, Name, Caption, Extension string
			Size                          int64
		} `json:"file"`
		MergeForward *struct {
			ChildCount int `json:"childCount"`
			Msgs       []struct {
				MessageID  int64  `json:"messageId"`
				Type       int    `json:"type"`
				SearchText string `json:"searchText"`
				From       string `json:"from"`
				Timestamp  int64  `json:"timestamp"`
			} `json:"msgs"`
		} `json:"mergeForward"`
		RichText *struct {
			SearchText string `json:"searchText"`
		} `json:"richText"`
	} `json:"payload"`
	PayloadRaw json.RawMessage `json:"payloadRaw"`
}

// projectViaDoc 走完整 DocFromMessage（分支 A）拿到 reader 可读 doc。
func projectViaDoc(t *testing.T, raw string) readerProjDoc {
	t.Helper()
	d, err := DocFromMessage(branchAMsg("100", raw))
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var rd readerProjDoc
	if err := json.Unmarshal(b, &rd); err != nil {
		t.Fatalf("reader unmarshal: %v", err)
	}
	return rd
}

// TestProject_Text type=1 文本投影 payload.text.content。
func TestProject_Text(t *testing.T) {
	rd := projectViaDoc(t, `{"type":1,"content":"你好 world"}`)
	if rd.Payload == nil || rd.Payload.Text == nil || rd.Payload.Text.Content != "你好 world" {
		t.Fatalf("text.content not projected: %+v", rd.Payload)
	}
	if rd.RawExcluded {
		t.Fatalf("text must not be rawExcluded")
	}
	if len(rd.PayloadRaw) == 0 {
		t.Fatalf("payloadRaw must be retained")
	}
}

// TestProject_Image type=2 image.{caption,name}+url/宽高。
func TestProject_Image(t *testing.T) {
	rd := projectViaDoc(t, `{"type":2,"name":"合同.png","caption":"季度合同","url":"https://x/y.png","width":800,"height":600}`)
	img := rd.Payload.Image
	if img == nil || img.Name != "合同.png" || img.Caption != "季度合同" || img.URL != "https://x/y.png" || img.Width != 800 || img.Height != 600 {
		t.Fatalf("image not fully projected: %+v", img)
	}
	if rd.RawExcluded {
		t.Fatalf("image must not be rawExcluded (searchable)")
	}
}

// TestProject_GIF type=3 留底 gif.url。
func TestProject_GIF(t *testing.T) {
	rd := projectViaDoc(t, `{"type":3,"url":"https://x/a.gif"}`)
	if rd.Payload.Gif == nil || rd.Payload.Gif.URL != "https://x/a.gif" {
		t.Fatalf("gif.url not projected: %+v", rd.Payload.Gif)
	}
}

// TestProject_Voice type=4 留底 voice.url。
func TestProject_Voice(t *testing.T) {
	rd := projectViaDoc(t, `{"type":4,"url":"https://x/a.mp3"}`)
	if rd.Payload.Voice == nil || rd.Payload.Voice.URL != "https://x/a.mp3" {
		t.Fatalf("voice.url not projected: %+v", rd.Payload.Voice)
	}
}

// TestProject_Video type=5 video.*。
func TestProject_Video(t *testing.T) {
	rd := projectViaDoc(t, `{"type":5,"url":"https://x/v.mp4","cover":"https://x/c.jpg","width":1920,"height":1080,"second":42}`)
	v := rd.Payload.Video
	if v == nil || v.URL != "https://x/v.mp4" || v.Cover != "https://x/c.jpg" || v.Width != 1920 || v.Height != 1080 || v.Second != 42 {
		t.Fatalf("video not fully projected: %+v", v)
	}
}

// TestProject_File type=8 file.{name,caption,extension}+url/size。
func TestProject_File(t *testing.T) {
	rd := projectViaDoc(t, `{"type":8,"name":"年报.pdf","caption":"2025 年报","url":"https://x/f.pdf","size":123456,"extension":"pdf"}`)
	f := rd.Payload.File
	if f == nil || f.Name != "年报.pdf" || f.Caption != "2025 年报" || f.Extension != "pdf" || f.URL != "https://x/f.pdf" || f.Size != 123456 {
		t.Fatalf("file not fully projected: %+v", f)
	}
}

// TestProject_FileToVideo type=8 文件名后缀命中视频白名单 → 投成 video(type=5) 只填 url；payloadRaw 仍存原 type=8。
func TestProject_FileToVideo(t *testing.T) {
	rd := projectViaDoc(t, `{"type":8,"name":"clip.mp4","url":"https://x/clip.mp4","size":999,"extension":"mp4"}`)
	if rd.Payload.Type == nil || *rd.Payload.Type != payloadTypeVideo {
		t.Fatalf("file→video must rewrite payload.type to 5, got %+v", rd.Payload.Type)
	}
	if rd.Payload.Video == nil || rd.Payload.Video.URL != "https://x/clip.mp4" {
		t.Fatalf("file→video must project video.url: %+v", rd.Payload.Video)
	}
	if rd.Payload.File != nil {
		t.Fatalf("file→video must not also project file: %+v", rd.Payload.File)
	}
	// payloadRaw 仍是原始 type=8 对象。
	var raw map[string]any
	if err := json.Unmarshal(rd.PayloadRaw, &raw); err != nil {
		t.Fatalf("payloadRaw parse: %v", err)
	}
	if tp, _ := extractInt(raw, "type"); tp != payloadTypeFile {
		t.Fatalf("payloadRaw must retain original type=8, got %v", raw["type"])
	}
}

// TestProject_MergeForward type=11 msgs[].{searchText,from,timestamp,messageId}，from 锚 reader（非 fromUid）。
func TestProject_MergeForward(t *testing.T) {
	raw := `{"type":11,"msgs":[
		{"message_id":"9007199254740993","from_uid":"u_sender","timestamp":1700000000,"payload":{"type":1,"content":"季度总结"}},
		{"message_id":123,"from_uid":"u_b","timestamp":1700000001,"payload":{"type":8,"name":"附件.docx"}}
	]}`
	rd := projectViaDoc(t, raw)
	mf := rd.Payload.MergeForward
	if mf == nil || mf.ChildCount != 2 || len(mf.Msgs) != 2 {
		t.Fatalf("mergeForward not projected: %+v", mf)
	}
	m0 := mf.Msgs[0]
	if m0.From != "u_sender" {
		t.Fatalf("mergeForward msgs[].from must anchor reader `from` (not fromUid): %+v", m0)
	}
	if m0.MessageID != 9007199254740993 {
		t.Fatalf("snowflake precision lost in nested messageId: got %d", m0.MessageID)
	}
	if m0.Timestamp != 1700000000 {
		t.Fatalf("mergeForward msgs[].timestamp missing: %+v", m0)
	}
	if m0.SearchText != "季度总结" {
		t.Fatalf("mergeForward inner text searchText missing: %q", m0.SearchText)
	}
	if mf.Msgs[1].SearchText != "附件.docx" {
		t.Fatalf("mergeForward inner file name searchText missing: %q", mf.Msgs[1].SearchText)
	}
}

// TestProject_RichText type=14 richText.searchText（前瞻）。
func TestProject_RichText(t *testing.T) {
	raw := `{"type":14,"content":[{"type":"text","text":"看这张"},{"type":"image","name":"图.png"}]}`
	rd := projectViaDoc(t, raw)
	if rd.Payload.RichText == nil || rd.Payload.RichText.SearchText == "" {
		t.Fatalf("richText.searchText not projected: %+v", rd.Payload.RichText)
	}
	// plain 回填应含文本 + 图片占位 + 附件名。
	if !containsSub(rd.Payload.RichText.SearchText, "看这张") || !containsSub(rd.Payload.RichText.SearchText, "图.png") {
		t.Fatalf("richText searchText incomplete: %q", rd.Payload.RichText.SearchText)
	}
}

// TestProject_PayloadRawRetained payloadRaw 整包原样留底（_source 备查）。
func TestProject_PayloadRawRetained(t *testing.T) {
	raw := `{"type":2,"name":"x.png","custom_field":"keep_me","nested":{"a":1}}`
	rd := projectViaDoc(t, raw)
	var got map[string]any
	if err := json.Unmarshal(rd.PayloadRaw, &got); err != nil {
		t.Fatalf("payloadRaw parse: %v", err)
	}
	if got["custom_field"] != "keep_me" {
		t.Fatalf("payloadRaw must retain unprojected fields verbatim: %+v", got)
	}
}

// TestProject_SnowflakePrecision 顶层 + 嵌套 messageId 雪花精度（2^53+1）不被 float64 截断。
func TestProject_SnowflakePrecision(t *testing.T) {
	// 嵌套 msgs[].message_id 在 octo payload 里是 string（避 JS 精度丢失）；extractInt64 string 分支处理。
	raw := `{"type":11,"msgs":[{"message_id":"9007199254740993","from_uid":"u","payload":{"type":1,"content":"x"}}]}`
	rd := projectViaDoc(t, raw)
	if rd.Payload.MergeForward.Msgs[0].MessageID != 9007199254740993 {
		t.Fatalf("nested snowflake messageId truncated: %d", rd.Payload.MergeForward.Msgs[0].MessageID)
	}
}

// TestProject_UnknownTypeRawExcluded 未知 type（投不出任何 typed 子对象）→ RawExcluded=true，但
// 仍写 payloadRaw（留底）。
func TestProject_UnknownTypeRawExcluded(t *testing.T) {
	rd := projectViaDoc(t, `{"type":99,"content":"系统通知"}`)
	if !rd.RawExcluded {
		t.Fatalf("unknown type with no typed sub-object must be rawExcluded=true")
	}
	if len(rd.PayloadRaw) == 0 {
		t.Fatalf("unknown type must still retain payloadRaw")
	}
}

// TestProject_NonObjectPayloadNoRaw payload 顶层非对象（数组）→ 不写 payloadRaw、不投影（visibility
// 预检在上游已对此 fail-closed；此处防御性兜底）。
func TestProject_NonObjectPayload(t *testing.T) {
	p, raw, projected := buildPayloadFromRaw(json.RawMessage(`["a","b"]`))
	if p != nil || raw != nil || projected {
		t.Fatalf("non-object payload must yield nil payload/raw and projected=false: p=%v raw=%s", p, raw)
	}
}
