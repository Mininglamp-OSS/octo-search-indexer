package esindex

import (
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// TestLiveContractSafetyGate 钉住实时写入安全闸的口径（§3.6 语义重定义后）：本 gate 仅是
// **契约版本下限闸**——契约 SchemaVersion ≥ SafetyFieldsSchemaVersion(=2，带 RawPayload 投影能力)
// 即放行实时写入。当前契约已 ==2 → gate 恒 true。**visibility fail-closed 安全本身来自消费侧
// processBatch 预检调 ExtractVisibility（§3.4），不由本 gate 保证。** 不 bump SchemaVersion。
func TestLiveContractSafetyGate(t *testing.T) {
	want := searchmsg.SchemaVersion >= SafetyFieldsSchemaVersion
	if got := LiveContractCarriesSafetyFields(); got != want {
		t.Fatalf("LiveContractCarriesSafetyFields()=%v want %v", got, want)
	}
	// 当前契约已携带版本下限（SchemaVersion>=2），gate 应放行。
	if searchmsg.SchemaVersion < SafetyFieldsSchemaVersion && LiveContractCarriesSafetyFields() {
		t.Fatalf("gate must stay closed while contract is below the v2 floor")
	}
}
