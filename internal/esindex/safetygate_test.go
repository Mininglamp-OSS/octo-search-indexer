package esindex

import (
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/contract/searchmsg"
)

// TestLiveContractSafetyGate 钉住实时写入安全闸的口径：当前 Kafka 契约（SchemaVersion=1）
// 不带 spaceId/visibles/messageSeq，故实时路径必须封锁（防 reader visibles fail-OPEN）。
// octo-lib 升到 SafetyFieldsSchemaVersion(=2) + producer 富化后才解封（阶段 9 前置）。
func TestLiveContractSafetyGate(t *testing.T) {
	want := searchmsg.SchemaVersion >= SafetyFieldsSchemaVersion
	if got := LiveContractCarriesSafetyFields(); got != want {
		t.Fatalf("LiveContractCarriesSafetyFields()=%v want %v", got, want)
	}
	// 本期断言：契约尚未携带安全字段（若 octo-lib 已 bump，此处会提示更新 pin / 解封实时路径）。
	if searchmsg.SchemaVersion < SafetyFieldsSchemaVersion && LiveContractCarriesSafetyFields() {
		t.Fatalf("guard must stay closed while contract lacks safety fields")
	}
}
