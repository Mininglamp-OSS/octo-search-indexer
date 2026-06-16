package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/backfill"
	"github.com/Mininglamp-OSS/octo-search-indexer/internal/recon"
)

// fakeSampleSrc / fakeESFetch 模拟 standalone reconcile 的两个抽样依赖（MySQL 源 + ES doc）。
type fakeSampleSrc struct{ rows []recon.SampleRow }

func (f *fakeSampleSrc) SampleRows(_ context.Context, _, _ int64, limit int) ([]recon.SampleRow, error) {
	if limit < len(f.rows) {
		return f.rows[:limit], nil
	}
	return f.rows, nil
}

type fakeESFetch struct{ docs map[string]recon.ESDocFields }

func (f *fakeESFetch) FetchDocs(_ context.Context, ids []string) (map[string]recon.ESDocFields, error) {
	out := map[string]recon.ESDocFields{}
	for _, id := range ids {
		if d, ok := f.docs[id]; ok {
			out[id] = d
		}
	}
	return out, nil
}

// writeSpillWithDLQRow 落一行 DLQ spill 记录（NDJSON + 已 fsync 的 offset sidecar），返回 spill dir。
// 直接按 backfill spill 的磁盘格式写，模拟 backfill job 跑完后留在盘上、供 standalone reconcile 只读
// 加载的文件（DLQSpill 的内部写入器不导出，故测试按已固定的盘上契约构造）。
func writeSpillWithDLQRow(t *testing.T, messageID string, createdAt int64) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "spill")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir spill: %v", err)
	}
	line := fmt.Sprintf("{\"reason\":\"backfill_payload_unparseable\",\"message_id\":%q,\"created_at\":%d}\n", messageID, createdAt)
	if err := os.WriteFile(filepath.Join(dir, "backfill-dlq.ndjson"), []byte(line), 0o640); err != nil {
		t.Fatalf("write spill: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "backfill-dlq.synced"), []byte(fmt.Sprintf("%d", len(line))), 0o640); err != nil {
		t.Fatalf("write sidecar: %v", err)
	}
	return dir
}

// TestStandaloneReconcile_DLQRowNotMissing 🔴 R3 核心回归：standalone reconcile 的字段级抽样门
// 抽到一个合法 DLQ 行（已在 backfill spill / 排除集内）时，必须不计 sample_missing、不触发 exit 2。
// 复刻 cmd/reconcile run() 的抽样链：LoadDLQMessageIDsInWindow(spillDir) → CompareSamplesExcluding。
func TestStandaloneReconcile_DLQRowNotMissing(t *testing.T) {
	spillDir := writeSpillWithDLQRow(t, "99", 150) // 合法 DLQ 行：坏 payload，故意不进 ES

	src := &fakeSampleSrc{rows: []recon.SampleRow{
		{MessageID: "10", ChannelID: "g1", ChannelType: 2}, // 正常：ES 有 doc
		{MessageID: "99", ChannelID: "gx", ChannelType: 2}, // DLQ：ES 无 doc，应被排除
	}}
	es := &fakeESFetch{docs: map[string]recon.ESDocFields{
		"10": {MessageID: 10, ChannelID: "g1", ChannelType: 2},
	}}

	excluded, err := backfill.LoadDLQMessageIDsInWindow(spillDir, 0, 1000)
	if err != nil {
		t.Fatalf("load exclusion: %v", err)
	}
	res, err := recon.CompareSamplesExcluding(context.Background(), src, es, 0, 1000, 100, 50, excluded)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if res.Missing != 0 {
		t.Fatalf("legit DLQ row must NOT be sample_missing (would false exit 2): %+v", res)
	}
	if res.Sampled != 1 || res.Mismatch != 0 {
		t.Fatalf("only the non-DLQ row should be compared cleanly: %+v", res)
	}
}

// TestStandaloneReconcile_NoSpillDirStillFailsOnRealMissing 没传 spill dir（空排除集）时，
// 真正漏灌的行仍按 sample_missing 计——确保排除逻辑不会掩盖真实缺失（fail-closed 不被削弱）。
func TestStandaloneReconcile_NoSpillDirStillFailsOnRealMissing(t *testing.T) {
	src := &fakeSampleSrc{rows: []recon.SampleRow{
		{MessageID: "10", ChannelID: "g1", ChannelType: 2}, // 真漏灌：ES 无 doc
	}}
	es := &fakeESFetch{docs: map[string]recon.ESDocFields{}}

	excluded, err := backfill.LoadDLQMessageIDsInWindow("", 0, 1000)
	if err != nil {
		t.Fatalf("load exclusion: %v", err)
	}
	res, err := recon.CompareSamplesExcluding(context.Background(), src, es, 0, 1000, 100, 50, excluded)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if res.Missing != 1 {
		t.Fatalf("real missing row must still be flagged (gate must stay fail-closed): %+v", res)
	}
}
