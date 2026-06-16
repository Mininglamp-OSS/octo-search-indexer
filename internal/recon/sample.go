// 抽样字段比对（YUJ-4682 步骤 5）：count 对账只证「条数」一致，不证「内容」一致。
// 本文件加一层抽样比对：从 MySQL 取一批样本行，按 message_id 拉对应 ES doc，逐字段核对
// reader 契约里鉴权/正确性关键字段（messageId/channelId/channelType/spaceId/visibles/messageSeq），
// 检出「条数对得上但字段错位」的静默 drift。失配数回填 search_recon_sample_mismatch gauge。
package recon

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// SampleRow 是抽样比对所需的 MySQL 源字段（reader 契约关键字段的权威值）。
type SampleRow struct {
	MessageID   string
	MessageSeq  int64
	ChannelID   string
	ChannelType int
	SpaceID     string   // 从 payload.space_id 解出（非加密文本消息）
	Visibles    []string // 从 payload.visibles 解出
	Signal      bool     // 加密消息：payload 不解析，spaceId/visibles 不参与比对
}

// MismatchDetail 描述一条抽样失配（哪个 message_id 的哪个字段对不上）。
type MismatchDetail struct {
	MessageID string `json:"message_id"`
	Field     string `json:"field"`
	MySQL     string `json:"mysql"`
	ES        string `json:"es"`
}

// SampleResult 是一次抽样比对的结论。
type SampleResult struct {
	Sampled    int              `json:"sampled"`  // 实际比对的样本数
	Missing    int              `json:"missing"`  // MySQL 有但 ES 查不到 doc 的样本数（少 doc 的字段级证据）
	Mismatch   int              `json:"mismatch"` // 字段失配的样本数（去重到「条」，一条多字段失配只计 1）
	Details    []MismatchDetail `json:"details"`  // 失配明细（capped）
	maxDetails int
}

// SampleSourceReader 取一批抽样源行（按 message_id 升序的确定性抽样，便于复算）。
type SampleSourceReader interface {
	// SampleRows 从时间窗内按 message_id 升序取至多 limit 行（确定性，非随机：可复算 + 稳定阈值）。
	SampleRows(ctx context.Context, fromUnix, toUnix int64, limit int) ([]SampleRow, error)
}

// ESDocFetcher 按 message_id 批量拉 ES doc（reader 契约形态的子集）。
type ESDocFetcher interface {
	// FetchDocs 按 message_id（= _id）批量取 doc，返回 message_id→docFields（缺失即不在 map）。
	FetchDocs(ctx context.Context, messageIDs []string) (map[string]ESDocFields, error)
}

// ESDocFields 是从 ES doc _source 投影出的 reader 契约关键字段（用 reader 字段名反序列化，
// 确保字段名/类型逐字段对齐——字段名写错会直接导致这里读不到值而判失配）。
type ESDocFields struct {
	MessageID   int64    `json:"messageId"`
	MessageSeq  uint64   `json:"messageSeq"`
	ChannelID   string   `json:"channelId"`
	ChannelType uint32   `json:"channelType"`
	SpaceID     string   `json:"spaceId"`
	Visibles    []string `json:"visibles"`
	RawExcluded bool     `json:"rawExcluded"`
}

// CompareSamples 拉样本 + 对应 ES doc，逐字段比对，产出 SampleResult。
// maxDetails<=0 时取默认 50（防失配明细把报告撑爆）。
func CompareSamples(ctx context.Context, src SampleSourceReader, es ESDocFetcher, fromUnix, toUnix int64, limit, maxDetails int) (SampleResult, error) {
	return CompareSamplesExcluding(ctx, src, es, fromUnix, toUnix, limit, maxDetails, nil)
}

// CompareSamplesExcluding 与 CompareSamples 相同，但额外接受一个「已知不该在 ES 正文索引里」
// 的 message_id 集合（excluded）——典型是 backfill 落 DLQ spill 的真异常 / 永久拒绝行。这些行
// 被抽样命中时「ES 无 doc」是**预期**（它们本就被记账为 DLQ），故不计 missing、也不计 Sampled，
// 直接跳过：否则 inline backfill 对账门会把合法 DLQ 行误判成漏灌 false MISMATCH（count 门已用
// DLQ 计数抵消同一批行，抽样门若再算成 missing 就口径冲突）。excluded 为 nil/空时行为与
// CompareSamples 完全一致。
//
// 🔴 为不削弱抽样覆盖：源行 limit 在排除**之前**生效，若窗内前若干行恰好全是 DLQ 行，过滤后
// 实际比对数会少于 limit（甚至为 0）——正是高 DLQ 窗最需要内容门时被掏空。故这里**过采样**：
// 多取 len(excluded) 行（DLQ 量级极小），过滤掉排除行后再把真正比对的非排除行截断到 limit，
// 使有效比对数稳定 ≈ limit。源抽样确定性（按 message_id 升序取前 N）保证过采样仍可复算。
func CompareSamplesExcluding(ctx context.Context, src SampleSourceReader, es ESDocFetcher, fromUnix, toUnix int64, limit, maxDetails int, excluded map[string]bool) (SampleResult, error) {
	if maxDetails <= 0 {
		maxDetails = 50
	}
	res := SampleResult{maxDetails: maxDetails}
	if limit <= 0 {
		return res, nil
	}

	// 过采样补偿排除行，使过滤后有效比对数 ≈ limit。
	effLimit := limit + len(excluded)
	rows, err := src.SampleRows(ctx, fromUnix, toUnix, effLimit)
	if err != nil {
		return res, fmt.Errorf("recon: sample source rows: %w", err)
	}
	if len(rows) == 0 {
		return res, nil
	}

	// 取前 limit 个**可比对**行作为本次比对集（截断到 limit，确定性可复算）。
	cmp := make([]SampleRow, 0, limit)
	for i := range rows {
		id := rows[i].MessageID
		// 空 message_id 行不可与 ES doc（_id=message_id）对齐——既无法查 doc、也无法进 DLQ
		// 排除集（去重键退化为 table:id）。这类行（源 schema 上 message_id NOT NULL，理论不该出现）
		// 既不计 Sampled 也不计 Missing：硬当 missing 会让合法 backfill run false-fail（codex R2）。
		if id == "" {
			continue
		}
		if excluded[id] {
			continue // 已知 DLQ 行：本就不该有 doc，不纳入比对
		}
		cmp = append(cmp, rows[i])
		if len(cmp) >= limit {
			break
		}
	}
	if len(cmp) == 0 {
		return res, nil
	}

	ids := make([]string, 0, len(cmp))
	for i := range cmp {
		ids = append(ids, cmp[i].MessageID)
	}
	docs, err := es.FetchDocs(ctx, ids)
	if err != nil {
		return res, fmt.Errorf("recon: fetch ES sample docs: %w", err)
	}

	for i := range cmp {
		r := cmp[i]
		res.Sampled++
		doc, ok := docs[r.MessageID]
		if !ok {
			res.Missing++
			res.addDetail(MismatchDetail{MessageID: r.MessageID, Field: "_doc", MySQL: "present", ES: "missing"})
			continue
		}
		if d, mismatched := compareRow(r, doc); mismatched {
			res.Mismatch++
			for _, det := range d {
				res.addDetail(det)
			}
		}
	}
	return res, nil
}

// compareRow 比对单条样本的关键字段；返回失配明细 + 是否有任一字段失配。
func compareRow(r SampleRow, doc ESDocFields) ([]MismatchDetail, bool) {
	var dets []MismatchDetail
	add := func(field, my, es string) {
		dets = append(dets, MismatchDetail{MessageID: r.MessageID, Field: field, MySQL: my, ES: es})
	}

	// _source.messageId 必须 == MySQL message_id 全精度（reader 从 _source 读 int64 做 cursor
	// tiebreaker；_id 对但 _source.messageId 缺/0/被 float64 截断会静默打断 cursor）。
	if r.MessageID != fmtInt(doc.MessageID) {
		add("messageId", r.MessageID, fmtInt(doc.MessageID))
	}
	if fmtInt(r.MessageSeq) != fmtUint(doc.MessageSeq) {
		add("messageSeq", fmtInt(r.MessageSeq), fmtUint(doc.MessageSeq))
	}
	if r.ChannelID != doc.ChannelID {
		add("channelId", r.ChannelID, doc.ChannelID)
	}
	if int64(r.ChannelType) != int64(doc.ChannelType) {
		add("channelType", fmtInt(int64(r.ChannelType)), fmtUint(uint64(doc.ChannelType)))
	}
	// spaceId / visibles 仅对非加密消息比对（加密消息 payload 不解析，两侧均应留空）。
	if !r.Signal {
		if r.SpaceID != doc.SpaceID {
			add("spaceId", r.SpaceID, doc.SpaceID)
		}
		if !sameStringSet(r.Visibles, doc.Visibles) {
			add("visibles", fmt.Sprint(sortedCopy(r.Visibles)), fmt.Sprint(sortedCopy(doc.Visibles)))
		}
	}
	return dets, len(dets) > 0
}

func (s *SampleResult) addDetail(d MismatchDetail) {
	if len(s.Details) < s.maxDetails {
		s.Details = append(s.Details, d)
	}
}

func fmtInt(v int64) string   { return fmt.Sprintf("%d", v) }
func fmtUint(v uint64) string { return fmt.Sprintf("%d", v) }

// sameStringSet 比较两个字符串集合是否相等（顺序无关，reader visibles 是集合语义）。
func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as, bs := sortedCopy(a), sortedCopy(b)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// ── MySQL / ES 实现 ─────────────────────────────────────────────────────────────

// MySQLSampleReader 从 message 分表确定性抽样（按 message_id 升序），并就地解 payload 的
// space_id/visibles（与 backfill docFromRow 同口径）。
type MySQLSampleReader struct {
	db     *sql.DB
	tables []string
}

// NewMySQLSampleReader 构造。
func NewMySQLSampleReader(db *sql.DB, tables []string) *MySQLSampleReader {
	return &MySQLSampleReader{db: db, tables: tables}
}

// SampleRows 跨分表各取窗内按 message_id 升序的前若干行，合并后再截断到 limit（确定性）。
func (s *MySQLSampleReader) SampleRows(ctx context.Context, fromUnix, toUnix int64, limit int) ([]SampleRow, error) {
	if limit <= 0 {
		return nil, nil
	}
	var all []SampleRow
	for _, t := range s.tables {
		if !safeTableName(t) {
			return nil, fmt.Errorf("recon: unsafe table name %q", t)
		}
		q := fmt.Sprintf(
			"SELECT message_id, message_seq, channel_id, channel_type, setting, `signal`, payload "+
				"FROM `%s` WHERE UNIX_TIMESTAMP(created_at) BETWEEN ? AND ? ORDER BY message_id ASC LIMIT ?", t)
		rows, err := s.db.QueryContext(ctx, q, fromUnix, toUnix, limit)
		if err != nil {
			return nil, fmt.Errorf("recon: sample query %s: %w", t, err)
		}
		batch, err := scanSampleRows(rows)
		if err != nil {
			return nil, err
		}
		all = append(all, batch...)
	}
	// 跨分表合并后按 message_id 升序，截断到 limit（确定性，可复算）。
	sort.Slice(all, func(i, j int) bool { return all[i].MessageID < all[j].MessageID })
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func scanSampleRows(rows *sql.Rows) (out []SampleRow, err error) {
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("recon: close sample rows: %w", cerr)
		}
	}()
	for rows.Next() {
		var (
			r       SampleRow
			setting uint8
			signal  int
			payload []byte
		)
		if err := rows.Scan(&r.MessageID, &r.MessageSeq, &r.ChannelID, &r.ChannelType, &setting, &signal, &payload); err != nil {
			return nil, fmt.Errorf("recon: scan sample row: %w", err)
		}
		r.Signal = signal != 0 || setting&signalSettingMask != 0
		if !r.Signal {
			r.SpaceID, r.Visibles = parsePayloadVisibility(payload)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("recon: iterate sample rows: %w", err)
	}
	return out, nil
}

// signalSettingMask 是 setting 字节里 Signal 加密位（bit5），与 backfill/extract 同口径。
const signalSettingMask = 1 << 5

// parsePayloadVisibility 解出 payload.space_id / payload.visibles（与 backfill extractVisibility 同口径）。
func parsePayloadVisibility(payload []byte) (spaceID string, visibles []string) {
	var v struct {
		SpaceID  string            `json:"space_id"`
		Visibles []json.RawMessage `json:"visibles"`
	}
	if err := json.Unmarshal(payload, &v); err != nil {
		return "", nil
	}
	spaceID = v.SpaceID
	for _, raw := range v.Visibles {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			continue
		}
		if s != "" {
			visibles = append(visibles, s)
		}
	}
	return spaceID, visibles
}

// OSDocFetcher 用 _search terms(_id) 批量拉 doc（reader 契约字段投影）。
type OSDocFetcher struct {
	client *opensearchapi.Client
	index  string
}

// NewOSDocFetcher 构造。
func NewOSDocFetcher(client *opensearchapi.Client, index string) *OSDocFetcher {
	return &OSDocFetcher{client: client, index: index}
}

// FetchDocs 用 terms(_id) 一次查回样本 doc（size = len(ids)）。
func (f *OSDocFetcher) FetchDocs(ctx context.Context, messageIDs []string) (map[string]ESDocFields, error) {
	out := make(map[string]ESDocFields, len(messageIDs))
	if len(messageIDs) == 0 {
		return out, nil
	}
	body := map[string]any{
		"size": len(messageIDs),
		"query": map[string]any{
			"ids": map[string]any{"values": messageIDs},
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("recon: marshal sample search: %w", err)
	}
	resp, err := f.client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{f.index},
		Body:    bytes.NewReader(b),
	})
	if err != nil {
		return nil, fmt.Errorf("recon: ES sample search: %w", err)
	}
	if resp.Shards.Failed != 0 || resp.Shards.Successful != resp.Shards.Total {
		return nil, fmt.Errorf("recon: ES sample search incomplete shards (total=%d successful=%d failed=%d)",
			resp.Shards.Total, resp.Shards.Successful, resp.Shards.Failed)
	}
	for i := range resp.Hits.Hits {
		var fields ESDocFields
		if err := json.Unmarshal(resp.Hits.Hits[i].Source, &fields); err != nil {
			return nil, fmt.Errorf("recon: decode sample doc _id=%s: %w", resp.Hits.Hits[i].ID, err)
		}
		out[resp.Hits.Hits[i].ID] = fields
	}
	return out, nil
}
