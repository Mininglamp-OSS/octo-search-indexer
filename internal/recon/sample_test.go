package recon

import (
	"context"
	"testing"
)

type fakeSampleSrc struct{ rows []SampleRow }

func (f *fakeSampleSrc) SampleRows(_ context.Context, _, _ int64, limit int) ([]SampleRow, error) {
	if limit < len(f.rows) {
		return f.rows[:limit], nil
	}
	return f.rows, nil
}

type fakeESFetch struct{ docs map[string]ESDocFields }

func (f *fakeESFetch) FetchDocs(_ context.Context, ids []string) (map[string]ESDocFields, error) {
	out := map[string]ESDocFields{}
	for _, id := range ids {
		if d, ok := f.docs[id]; ok {
			out[id] = d
		}
	}
	return out, nil
}

// TestCompareSamples_AllMatch 全部字段一致 → 0 失配 0 缺失。
func TestCompareSamples_AllMatch(t *testing.T) {
	src := &fakeSampleSrc{rows: []SampleRow{
		{MessageID: "10", MessageSeq: 5, ChannelID: "g1", ChannelType: 2, SpaceID: "sA", Visibles: []string{"a", "b"}},
	}}
	es := &fakeESFetch{docs: map[string]ESDocFields{
		"10": {MessageID: 10, MessageSeq: 5, ChannelID: "g1", ChannelType: 2, SpaceID: "sA", Visibles: []string{"b", "a"}},
	}}
	res, err := CompareSamples(context.Background(), src, es, 0, 100, 100, 50)
	if err != nil {
		t.Fatalf("CompareSamples: %v", err)
	}
	if res.Sampled != 1 || res.Mismatch != 0 || res.Missing != 0 {
		t.Fatalf("want clean match: %+v", res)
	}
}

// TestCompareSamples_MissingDoc MySQL 有 ES 无 → missing 计数（少 doc 的字段级证据）。
func TestCompareSamples_MissingDoc(t *testing.T) {
	src := &fakeSampleSrc{rows: []SampleRow{{MessageID: "11", ChannelID: "g", ChannelType: 1}}}
	es := &fakeESFetch{docs: map[string]ESDocFields{}}
	res, err := CompareSamples(context.Background(), src, es, 0, 100, 100, 50)
	if err != nil {
		t.Fatalf("CompareSamples: %v", err)
	}
	if res.Missing != 1 || res.Mismatch != 0 {
		t.Fatalf("missing doc must be counted: %+v", res)
	}
}

// TestCompareSamplesExcluding_DLQRowNotMissing 已知 DLQ 行（本就不该在 ES）被抽样命中时
// 不计 missing、不计 Sampled——避免 inline backfill 对账门把合法 DLQ 行误判成漏灌 false MISMATCH。
func TestCompareSamplesExcluding_DLQRowNotMissing(t *testing.T) {
	src := &fakeSampleSrc{rows: []SampleRow{
		{MessageID: "10", ChannelID: "g1", ChannelType: 2},  // 正常：ES 有 doc
		{MessageID: "99", ChannelID: "gx", ChannelType: 2},  // DLQ 真异常：ES 无 doc，预期排除
	}}
	es := &fakeESFetch{docs: map[string]ESDocFields{
		"10": {MessageID: 10, ChannelID: "g1", ChannelType: 2},
	}}
	excluded := map[string]bool{"99": true}
	res, err := CompareSamplesExcluding(context.Background(), src, es, 0, 100, 100, 50, excluded)
	if err != nil {
		t.Fatalf("CompareSamplesExcluding: %v", err)
	}
	if res.Sampled != 1 || res.Missing != 0 || res.Mismatch != 0 {
		t.Fatalf("excluded DLQ row must be skipped (not missing, not sampled): %+v", res)
	}
}

// TestCompareSamplesExcluding_NilSameAsCompare excluded 为 nil 时行为与 CompareSamples 完全一致
// （DLQ 行仍按 missing 计——无排除集时该行确实是漏灌证据）。
func TestCompareSamplesExcluding_NilSameAsCompare(t *testing.T) {
	src := &fakeSampleSrc{rows: []SampleRow{{MessageID: "11", ChannelID: "g", ChannelType: 1}}}
	es := &fakeESFetch{docs: map[string]ESDocFields{}}
	res, err := CompareSamplesExcluding(context.Background(), src, es, 0, 100, 100, 50, nil)
	if err != nil {
		t.Fatalf("CompareSamplesExcluding: %v", err)
	}
	if res.Missing != 1 || res.Sampled != 1 {
		t.Fatalf("nil excluded must match CompareSamples semantics: %+v", res)
	}
}

// TestCompareSamplesExcluding_SkipsEmptyMessageID 空 message_id 源行不可与 ES doc(_id) 对齐，
// 也进不了 DLQ 排除集（去重键退化为 table:id），故既不计 Sampled 也不计 Missing——避免合法
// backfill run 被这类行 false-fail（codex R2）。
func TestCompareSamplesExcluding_SkipsEmptyMessageID(t *testing.T) {
	src := &fakeSampleSrc{rows: []SampleRow{
		{MessageID: "", ChannelID: "g", ChannelType: 2},   // 空 id：不可比对，跳过
		{MessageID: "10", ChannelID: "g", ChannelType: 2}, // 正常
	}}
	es := &fakeESFetch{docs: map[string]ESDocFields{
		"10": {MessageID: 10, ChannelID: "g", ChannelType: 2},
	}}
	res, err := CompareSamplesExcluding(context.Background(), src, es, 0, 100, 100, 50, nil)
	if err != nil {
		t.Fatalf("CompareSamplesExcluding: %v", err)
	}
	if res.Sampled != 1 || res.Missing != 0 || res.Mismatch != 0 {
		t.Fatalf("empty message_id row must be skipped (not missing): %+v", res)
	}
}

// TestCompareSamplesExcluding_OversamplesForCoverage 当窗内前若干行恰好全是 DLQ 行时，过采样
// 应补回足额非排除行，使有效比对数稳定 ≈ limit（codex P2：不削弱高 DLQ 窗的内容门覆盖）。
func TestCompareSamplesExcluding_OversamplesForCoverage(t *testing.T) {
	// limit=2；前 2 行（升序 message_id）是 DLQ 行，后 2 行是正常行。
	src := &fakeSampleSrc{rows: []SampleRow{
		{MessageID: "10", ChannelID: "g", ChannelType: 2}, // DLQ
		{MessageID: "20", ChannelID: "g", ChannelType: 2}, // DLQ
		{MessageID: "30", ChannelID: "g", ChannelType: 2}, // 正常
		{MessageID: "40", ChannelID: "g", ChannelType: 2}, // 正常
	}}
	es := &fakeESFetch{docs: map[string]ESDocFields{
		"30": {MessageID: 30, ChannelID: "g", ChannelType: 2},
		"40": {MessageID: 40, ChannelID: "g", ChannelType: 2},
	}}
	excluded := map[string]bool{"10": true, "20": true}
	res, err := CompareSamplesExcluding(context.Background(), src, es, 0, 100, 2, 50, excluded)
	if err != nil {
		t.Fatalf("CompareSamplesExcluding: %v", err)
	}
	// 过采样补偿后应比对到 2 个正常行（30,40），0 missing 0 mismatch——而非被前两行掏空成 0。
	if res.Sampled != 2 || res.Missing != 0 || res.Mismatch != 0 {
		t.Fatalf("oversampling must preserve coverage (want sampled=2 clean): %+v", res)
	}
}

// TestCompareSamples_SpaceIDVisiblesMismatch spaceId / visibles 字段错位 → mismatch（V1b/V3b 的对账闸）。
func TestCompareSamples_SpaceIDVisiblesMismatch(t *testing.T) {
	src := &fakeSampleSrc{rows: []SampleRow{
		{MessageID: "12", ChannelID: "g", ChannelType: 1, SpaceID: "sA", Visibles: []string{"admin"}},
	}}
	es := &fakeESFetch{docs: map[string]ESDocFields{
		"12": {MessageID: 12, ChannelID: "g", ChannelType: 1, SpaceID: "", Visibles: nil},
	}}
	res, err := CompareSamples(context.Background(), src, es, 0, 100, 100, 50)
	if err != nil {
		t.Fatalf("CompareSamples: %v", err)
	}
	if res.Mismatch != 1 {
		t.Fatalf("spaceId/visibles drift must be a mismatch: %+v", res)
	}
	// 一条多字段失配只计 1 条 mismatch，但 details 含每个字段。
	if len(res.Details) < 2 {
		t.Fatalf("details must list both spaceId and visibles: %+v", res.Details)
	}
}

// TestCompareSamples_SignalSkipsVisibility 加密消息不比对 spaceId/visibles（两侧均空，预期）。
func TestCompareSamples_SignalSkipsVisibility(t *testing.T) {
	src := &fakeSampleSrc{rows: []SampleRow{
		{MessageID: "13", ChannelID: "g", ChannelType: 1, Signal: true},
	}}
	es := &fakeESFetch{docs: map[string]ESDocFields{
		"13": {MessageID: 13, ChannelID: "g", ChannelType: 1},
	}}
	res, err := CompareSamples(context.Background(), src, es, 0, 100, 100, 50)
	if err != nil {
		t.Fatalf("CompareSamples: %v", err)
	}
	if res.Mismatch != 0 {
		t.Fatalf("encrypted msg must not mismatch on absent visibility: %+v", res)
	}
}

// TestFullReport_HealthyAndPayload 对平时 Healthy + 退出码语义；push 载荷逐字段对齐 octo-server。
func TestFullReport_HealthyAndPayload(t *testing.T) {
	var fr FullReport
	fr.Count = Reconcile(Counts{SourceRows: 100, ESDocs: 100, DLQ: 0})
	fr.RanAtUnixSeconds = 1700000000
	if !fr.Healthy() {
		t.Fatalf("count-OK + no sample drift must be healthy")
	}
	p := fr.PushPayload()
	if p.ESDocCount != 100 || p.MySQLRowCount != 100 || p.SampleMismatch != 0 || p.RanAtUnixSeconds != 1700000000 {
		t.Fatalf("push payload mismatch: %+v", p)
	}
}

// TestFullReport_UnhealthyOnSampleMismatch count 对平但抽样失配 → 整体不健康（内容 drift）。
func TestFullReport_UnhealthyOnSampleMismatch(t *testing.T) {
	var fr FullReport
	fr.Count = Reconcile(Counts{SourceRows: 100, ESDocs: 100})
	fr.Sample = SampleResult{Sampled: 10, Mismatch: 1}
	if fr.Healthy() {
		t.Fatalf("sample mismatch must make report unhealthy even when counts tie")
	}
	if fr.PushPayload().SampleMismatch != 1 {
		t.Fatalf("push payload must surface sample mismatch")
	}
}

// TestCompareSamples_MessageIDPrecisionDrift _id 对但 _source.messageId 被截断/置 0 →
// mismatch（reader 用 _source.messageId 做 cursor tiebreaker，必须全精度对齐）。
func TestCompareSamples_MessageIDPrecisionDrift(t *testing.T) {
	src := &fakeSampleSrc{rows: []SampleRow{
		{MessageID: "9223372036854775807", ChannelID: "g", ChannelType: 2},
	}}
	es := &fakeESFetch{docs: map[string]ESDocFields{
		// _id 命中（FetchDocs 按 _id 返回），但 _source.messageId 为 0（缺/截断）。
		"9223372036854775807": {MessageID: 0, ChannelID: "g", ChannelType: 2},
	}}
	res, err := CompareSamples(context.Background(), src, es, 0, 100, 100, 50)
	if err != nil {
		t.Fatalf("CompareSamples: %v", err)
	}
	if res.Mismatch != 1 {
		t.Fatalf("messageId _source drift must be a mismatch: %+v", res)
	}
	var sawField bool
	for _, d := range res.Details {
		if d.Field == "messageId" {
			sawField = true
		}
	}
	if !sawField {
		t.Fatalf("mismatch detail must name messageId: %+v", res.Details)
	}
}
