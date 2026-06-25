package esindex

import (
	"fmt"
	"math"
)

// 富文本(type=14)内嵌媒体虚拟子文档（B2 方案，richtext-virtual-docs-indexer-dev.md）。
//
// 目标：让富文本里内嵌的 image 能被 reader 的 _search_media 搜到。
// 对每个内嵌 image block，额外产出一个独立 OS doc，长得就像普通图片(type=2)消息，
// 带 virtual=true + parentMessageId。reader 媒体端点白名单（payload.type∈{2,5,8}）直接命中，
// octo-server 几乎不用改。本期富文本只能内嵌 image，故只会产 type=2。
//
// 范围（本期，与上游契约收敛）：
//   - 只派生 image(→type=2)。富文本(type=14) block 类型**仅 text/image**（octo-lib
//     common/richtext.go 锁定：RichTextBlockType 只有 text/image，ValidateRichTextBlocks 对其他
//     type 返 ErrRichTextUnknownBlock 拒入库；octo-web 发送侧只有 makeImageBlock/makeTextBlock，
//     buildRichTextMixedCandidate 遇 file 直接 return null）。**file block 全链路未打开**
//     （前端仅前向兼容接收渲染，不发送；后端不收），故现实中不会产生 file 富文本消息。
//     待 octo-lib/后端正式打开 file block 契约后，再扩展 file→type=8 派生。
//   - 撤回/编辑联动**不做**（路线甲：reader 用 parentMessageId 回 MySQL join 判可见性/撤回）。
//   - 子文档继承父的标识/时序/可见性字段；ES _id = "<父messageId>-rt<i>"（i 按 blocks 顺序，从 0 起）。

// richTextDerivatives 从父 doc 的 RawPayload 解析 type=14 的 image block，产出虚拟子文档列表。
//
// 仅当父 doc 确为富文本（Payload.Type==14）且 RawPayload 非空时产出；否则返回 nil。
// 子文档幂等键 _id 用复合键，重跑 backfill/consumer 重投覆盖同一条，不重复创建。
func richTextDerivatives(parent Doc) []Doc {
	if parent.Payload == nil || parent.Payload.Type == nil || *parent.Payload.Type != payloadTypeRichText {
		return nil
	}
	if len(parent.PayloadRaw) == 0 {
		return nil
	}
	raw, ok := decodeObjectUseNumber(parent.PayloadRaw)
	if !ok {
		return nil
	}
	blocks := richTextBlocks(raw["content"])
	if len(blocks) == 0 {
		return nil
	}

	var out []Doc
	for i, blk := range blocks {
		t, _ := blk["type"].(string)
		if t != "image" {
			continue // text / 未知 block：不派生媒体子文档（file 契约未打开）
		}
		img := imageFromBlock(blk)
		if img.URL == "" {
			continue // 空 url image block（契约上 image url 必填）：不索空壳媒体子文档
		}
		sub := &Payload{Type: intPtr(payloadTypeImage), Image: img}
		out = append(out, deriveChild(parent, sub, i))
	}
	return out
}

// deriveChild 构造一个虚拟子文档：**复制父再覆盖**（而非逐字段枚举），保证父的标识/
// 时序/可见性字段（含以后新增的）都被继承，不会漏。然后显式覆盖/清零非继承项。
// ES _id = "<父messageId>-rt<i>"（字符串复合键，messageId 字段仍保留父值供 reader 游标排序）。
func deriveChild(parent Doc, sub *Payload, i int) Doc {
	child := parent // 继承父全部字段（含 SchemaVersion/MessageID/MessageSeq/From/To/Channel*/SpaceID/Visibles/Timestamp/CreatedAt/Source 及未来新增可见性字段）

	// 显式清零非继承项（关键）：
	child.PayloadRaw = nil   // 子文档不写 payloadRaw（契约）
	child.Derivatives = nil  // 切断对父 Derivatives 切片的别名引用；当前 encodeBulkBody 只展开一层，此为防御性清零
	child.RawExcluded = false

	// 虚拟子文档自身字段：
	child.Payload = sub
	child.ParentMessageID = parent.MessageID
	child.ParentPayloadType = payloadTypeRichText
	child.Virtual = true
	// SubSeq 父独占 0，子从 1 递增（block 序号 i+1），保 (messageId, subSeq) 不跟父及兄弟撞。
	child.SubSeq = i + 1
	// _id 复合键（字符串），不影响 messageId 字段保持 long
	child.idOverride = fmt.Sprintf("%d-rt%d", parent.MessageID, i)
	return child
}

// imageFromBlock 把 richText image block 投影成 ImagePayload（与普通图片消息一致）。
// 字段严格对齐 octo-lib RichTextBlock 与 reader ImagePayload 形态：url/name/width/height。
// （富文本 image block 无 caption；reader ImagePayload 无 size 字段，故不投 size。）
func imageFromBlock(blk map[string]any) *ImagePayload {
	p := &ImagePayload{}
	if u, ok := blk["url"].(string); ok {
		p.URL = u
	}
	if n, ok := blk["name"].(string); ok {
		p.Name = n
	}
	if w, ok := extractInt(blk, "width"); ok {
		p.Width = clampInt32(w)
	}
	if h, ok := extractInt(blk, "height"); ok {
		p.Height = clampInt32(h)
	}
	return p
}

// clampInt32 把 width/height 钳进 ES `integer`（int32）可表示范围（见 #26）。
//
// octo-lib RichTextBlock.Width/Height 是 Go int（64 位）且 ValidateRichTextBlocks 仅校验 >0、
// **无上限**；ES mapping octo-message.json 把 image width/height 声明为 `integer`（int32）。正常
// 发送链路 width/height 取自浏览器 naturalWidth/naturalHeight，远小于 int32，但伪造/异常 payload
// 可塞入超 int32 值 → 派生子文档写 _bulk 时 4xx 永久失败，而父在 disabled payloadRaw 下仍可成功
// 索引 → 源行被判 DLQ 却留下可搜的孤儿父，破坏 ESDocs == SourceRows - DLQ 对账不变量。
//
// 兜底口径：超出 int32 范围（含负数，理论上 extractInt 不产负但防御）→ 置 0（omitempty 不落盘），
// 「尺寸不可信则不写」，既保证子文档一定能进 ES，又不向 reader 注入污染排版的假尺寸。
func clampInt32(v int) int {
	if v < 0 || v > math.MaxInt32 {
		return 0
	}
	return v
}

func intPtr(v int) *int { return &v }
