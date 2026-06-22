package esindex

import (
	"reflect"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// safetyFieldNames 是 reader 必读的三个安全/正确性字段在 searchmsg.Message 上的预期字段名
// （契约升到 v2 时由 octo-lib 添加）。映射到 esindex.Doc 上的目标字段名。
var safetyFieldNames = []struct {
	msgField string // searchmsg.Message 上的字段名（v2 契约新增）
	docField string // esindex.Doc 上对应字段名
}{
	{"SpaceID", "SpaceID"},
	{"Visibles", "Visibles"},
	{"MessageSeq", "MessageSeq"},
}

// TestV2Gate_DocFromMessageWiresSafetyFields 🔴 v2-gate latent fail-open 防回归门（Jerry 两轮点名）。
//
// 背景（方案 B 后语义，§3）：契约已是 v2（SchemaVersion=2）。DocFromMessage 对**分支 C**（在飞
// 老 v2 消息：无 RawPayload、非加密）信任契约带的 spaceId/visibles/messageSeq 并 copy 进 Doc。
// 隐患：若有人删了 DocFromMessage 对这三字段的 copy，分支 C 就会写出**空** visibles 的 doc，
// reader 直接 fail-OPEN（普通成员搜出群管才可见的系统消息）。本门钉死分支 C 的接线。
//
// 注意区分：**分支 A**（带 RawPayload 的新形态）的 visibility 来自消费侧 processBatch 预检
// （ExtractVisibility 回填进 msg），DocFromMessage 同样只是 copy 已回填值——本测试不带 RawPayload，
// 故走分支 C，验证「契约带的安全字段被忠实搬运」。
//
// 本测试用**反射**写哨兵值（历史上为兼容 v1 契约无字段而用反射，现 v2 字段已在，反射仍可用）。
func TestV2Gate_DocFromMessageWiresSafetyFields(t *testing.T) {
	if !LiveContractCarriesSafetyFields() {
		// 契约版本低于 v2 下限（理论上不应发生，当前 SchemaVersion 已 ==2）：实时闸保持关闭
		// （由 TestLiveContractSafetyGate 钉死）。本门在契约 ≥ v2 时生效。
		t.Skipf("contract below v2 floor (SchemaVersion=%d < %d); v2-gate inactive",
			searchmsg.SchemaVersion, SafetyFieldsSchemaVersion)
	}

	// ── 分支 C（无 RawPayload）：强制要求 DocFromMessage 把三个安全字段从契约 copy 进 Doc ──
	msg := searchmsg.Message{
		SchemaVersion: searchmsg.SchemaVersion,
		MessageID:     "123456789012345678",
		ChannelID:     "g_1",
		ChannelType:   2,
	}

	// 反射写入三个安全字段的哨兵值。
	mv := reflect.ValueOf(&msg).Elem()
	const (
		sentSpace = "space-sentinel"
		sentVis   = "vis-sentinel"
		sentSeq   = uint64(424242)
	)
	for _, f := range safetyFieldNames {
		fv := mv.FieldByName(f.msgField)
		if !fv.IsValid() {
			t.Fatalf("v2 contract must carry field %q (SpaceID/Visibles/MessageSeq)", f.msgField)
		}
		setSentinel(t, fv, f.msgField, sentSpace, sentVis, sentSeq)
	}

	doc, err := DocFromMessage(msg)
	if err != nil {
		t.Fatalf("DocFromMessage: %v", err)
	}

	// 逐字段断言 Doc 已被填全（未接线则为零值 → 转红）。
	if doc.SpaceID != sentSpace {
		t.Fatalf("v2-gate: DocFromMessage did not copy SpaceID from the contract (latent fail-closed/open): got %q", doc.SpaceID)
	}
	if len(doc.Visibles) != 1 || doc.Visibles[0] != sentVis {
		t.Fatalf("v2-gate: DocFromMessage did not copy Visibles from the contract — reader would fail-OPEN: got %+v", doc.Visibles)
	}
	if doc.MessageSeq != sentSeq {
		t.Fatalf("v2-gate: DocFromMessage did not copy MessageSeq from the contract: got %d", doc.MessageSeq)
	}
}

// TestV2Gate_DocRetainsSafetyFields 始终生效（v1 也跑）：esindex.Doc 必须保留三个安全字段为
// 反射门的目标字段。若有人重命名/删掉 Doc.SpaceID/Visibles/MessageSeq，本门立刻转红——否则上面
// 的 v2-gate 反射断言会因目标字段消失而**静默失效**（gate 烂掉而无人知）。
func TestV2Gate_DocRetainsSafetyFields(t *testing.T) {
	dt := reflect.TypeOf(Doc{})
	for _, f := range safetyFieldNames {
		if _, ok := dt.FieldByName(f.docField); !ok {
			t.Fatalf("esindex.Doc lost safety field %q; the v2 latent-fail-open gate depends on it", f.docField)
		}
	}
}

// setSentinel 按字段 kind 写入合适类型的哨兵值（string / []string / 无符号整数）。
func setSentinel(t *testing.T, fv reflect.Value, name, sentStr, sentVis string, sentSeq uint64) {
	t.Helper()
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(sentStr)
	case reflect.Slice:
		if fv.Type().Elem().Kind() != reflect.String {
			t.Fatalf("v2-gate: field %q expected []string, got %s", name, fv.Type())
		}
		fv.Set(reflect.ValueOf([]string{sentVis}))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fv.SetUint(sentSeq)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fv.SetInt(int64(sentSeq))
	default:
		t.Fatalf("v2-gate: field %q has unexpected kind %s", name, fv.Kind())
	}
}
