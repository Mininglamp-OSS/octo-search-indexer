package esindex

import (
	"encoding/json"
	"os"
	"testing"
)

// 本文件是方案 B §8 CI 防漂移门（indexer 侧 + STOP-8 单仓 fallback）：断言 reader 实查的 OS doc
// 字段集 ⊆ indexer 写出（mapping 可索引）字段集，违则红。防止 reader 加字段而 indexer 漏写，
// 重蹈写读不对称覆辙。
//
// 跨仓同步（STOP-8）：理想是 indexer CI 用 gh api 拉 octo-server reader_fields.json diff 本仓副本；
// 无跨仓 CI 凭证时降级为本仓 checked-in 快照 testdata/reader_fields.json（人工维护 + 文件头互相注释
// 同步纪律）。本测试在单仓内有效（mapping ⊇ 快照）；跨仓自动同步是 follow-up。
//
// ⚠️ richText 前瞻字段（§8.4）：reader 当前未查 richText，故 reader_fields.json 不含它，本门对
// richText **天然不覆盖**（reader 空集）。依赖人工在 reader 加 richText 查询时同步加 indexer 投影/
// mapping。不要因为本门绿就认为 richText 投影被保护。

// TestDriftGate_ReaderFieldsSubsetOfMapping 断言 reader_fields.json ⊆ mapping 可索引字段集
// （拍平 mapping，跳过 enabled:false 的 payloadRaw）。任一 reader 字段缺失 → 红。
func TestDriftGate_ReaderFieldsSubsetOfMapping(t *testing.T) {
	mappingFields, err := MappingIndexableFieldPaths()
	if err != nil {
		t.Fatalf("MappingIndexableFieldPaths: %v", err)
	}
	b, err := os.ReadFile("testdata/reader_fields.json")
	if err != nil {
		t.Fatalf("read reader_fields.json: %v", err)
	}
	var readerFields []string
	if err := json.Unmarshal(b, &readerFields); err != nil {
		t.Fatalf("parse reader_fields.json: %v", err)
	}
	if len(readerFields) == 0 {
		t.Fatal("reader_fields.json is empty — the drift gate would be vacuously green")
	}
	for _, f := range readerFields {
		if !mappingFields[f] {
			t.Fatalf("DRIFT: reader field %q is NOT in the indexer mapping (write-read asymmetry). "+
				"Add it to mapping/octo-message.json + the projection, or update reader_fields.json if reader dropped it.", f)
		}
	}
}

// TestDriftGate_PayloadRawNotIndexable payloadRaw 是 enabled:false BLOB，**不应**出现在可索引字段
// 集里（拍平须跳过 enabled:false 子树）——否则 drift 门会错误地要求 reader 查它。
func TestDriftGate_PayloadRawNotIndexable(t *testing.T) {
	mappingFields, err := MappingIndexableFieldPaths()
	if err != nil {
		t.Fatalf("MappingIndexableFieldPaths: %v", err)
	}
	for f := range mappingFields {
		if f == "payloadRaw" || len(f) >= 11 && f[:11] == "payloadRaw." {
			t.Fatalf("payloadRaw (enabled:false) must be excluded from indexable fields, found %q", f)
		}
	}
}

// TestDriftGate_RequiredNewFieldsInMapping 断言本期新增字段在 mapping 可索引集（mergeForward
// from/timestamp + richText.searchText），这与启动期 mapping-compat 断言（requiredMappingFieldPaths）
// 同源，确保两道门口径一致。
func TestDriftGate_RequiredNewFieldsInMapping(t *testing.T) {
	mappingFields, err := MappingIndexableFieldPaths()
	if err != nil {
		t.Fatalf("MappingIndexableFieldPaths: %v", err)
	}
	for _, f := range requiredMappingFieldPaths {
		if !mappingFields[f] {
			t.Fatalf("required Plan B field %q missing from embedded mapping", f)
		}
	}
}
