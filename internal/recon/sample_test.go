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
