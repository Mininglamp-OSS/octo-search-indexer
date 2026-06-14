package recon

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// SourceCounter 给出 MySQL message 表在时间窗内的行数（按 created_at 纪元秒区间）。
// 抽成接口便于单测与阶段 6 backfill 复用（可注入不同分表枚举/连接）。
type SourceCounter interface {
	// CountRows 统计 created_at ∈ [fromUnix, toUnix] 的 message 行数（跨全部分表）。
	CountRows(ctx context.Context, fromUnix, toUnix int64) (int64, error)
}

// ESCounter 给出 OpenSearch 索引内时间窗的 doc 数。
type ESCounter interface {
	// CountDocs 统计 created_at ∈ [fromUnix, toUnix] 的 doc 数。
	CountDocs(ctx context.Context, fromUnix, toUnix int64) (int64, error)
	// CountRawExcluded 统计窗内 raw_excluded=true 的 doc 数（观测用，不影响对平算术）。
	CountRawExcluded(ctx context.Context, fromUnix, toUnix int64) (int64, error)
}

// MySQLSourceCounter 用 database/sql 统计 message 分表行数。
type MySQLSourceCounter struct {
	db     *sql.DB
	tables []string
}

// NewMySQLSourceCounter 构造。tables 为 message 分表名集合（如 message, message1..4）。
func NewMySQLSourceCounter(db *sql.DB, tables []string) *MySQLSourceCounter {
	return &MySQLSourceCounter{db: db, tables: tables}
}

// CountRows 跨分表累加 created_at ∈ [from,to] 行数。表名来自受控配置（非用户输入），
// 但仍用反引号包裹并禁止注入字符；时间参数走占位符。
func (c *MySQLSourceCounter) CountRows(ctx context.Context, fromUnix, toUnix int64) (int64, error) {
	var total int64
	for _, t := range c.tables {
		if !safeTableName(t) {
			return 0, fmt.Errorf("recon: unsafe table name %q", t)
		}
		// UNIX_TIMESTAMP(created_at) 与索引 created_at 字段口径一致。
		q := fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE UNIX_TIMESTAMP(created_at) BETWEEN ? AND ?", t)
		var n int64
		if err := c.db.QueryRowContext(ctx, q, fromUnix, toUnix).Scan(&n); err != nil {
			return 0, fmt.Errorf("recon: count %s: %w", t, err)
		}
		total += n
	}
	return total, nil
}

// safeTableName 仅允许字母数字下划线（防御性，表名来自配置）。
func safeTableName(t string) bool {
	if t == "" {
		return false
	}
	for _, r := range t {
		if !isTableNameChar(r) {
			return false
		}
	}
	return true
}

// isTableNameChar 报告 r 是否为合法表名字符（[A-Za-z0-9_]）。
func isTableNameChar(r rune) bool {
	switch {
	case r == '_':
		return true
	case r >= 'a' && r <= 'z':
		return true
	case r >= 'A' && r <= 'Z':
		return true
	case r >= '0' && r <= '9':
		return true
	default:
		return false
	}
}

// OSCounter 用 opensearch-go count API 统计索引 doc 数。
type OSCounter struct {
	client *opensearchapi.Client
	index  string
}

// NewOSCounter 构造。
func NewOSCounter(client *opensearchapi.Client, index string) *OSCounter {
	return &OSCounter{client: client, index: index}
}

// CountDocs 统计 created_at ∈ [from,to] 的 doc 数（range query + _count）。
func (c *OSCounter) CountDocs(ctx context.Context, fromUnix, toUnix int64) (int64, error) {
	return c.count(ctx, rangeQuery(fromUnix, toUnix))
}

// CountRawExcluded 统计窗内 raw_excluded=true 的 doc 数。
func (c *OSCounter) CountRawExcluded(ctx context.Context, fromUnix, toUnix int64) (int64, error) {
	body := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": []any{
					rangeFilter(fromUnix, toUnix),
					map[string]any{"term": map[string]any{"raw_excluded": true}},
				},
			},
		},
	}
	return c.count(ctx, body)
}

func (c *OSCounter) count(ctx context.Context, query map[string]any) (int64, error) {
	b, err := json.Marshal(query)
	if err != nil {
		return 0, fmt.Errorf("recon: marshal count query: %w", err)
	}
	resp, err := c.client.Indices.Count(ctx, &opensearchapi.IndicesCountReq{
		Indices: []string{c.index},
		Body:    bytes.NewReader(b),
	})
	if err != nil {
		return 0, fmt.Errorf("recon: opensearch count: %w", err)
	}
	// 🔴 对账 gate 不得对「部分分片失败」的计数对平——_count 可能 HTTP 200 但 _shards.failed>0
	// 或 successful<total，此时计数不可信，宁可报错（避免掩盖真实数据缺失，产生 false OK）。
	if resp.Shards.Failed != 0 || resp.Shards.Successful != resp.Shards.Total {
		return 0, fmt.Errorf("recon: opensearch count had incomplete shards (total=%d successful=%d failed=%d) — count unreliable",
			resp.Shards.Total, resp.Shards.Successful, resp.Shards.Failed)
	}
	return int64(resp.Count), nil
}

// rangeQuery 构造 created_at ∈ [from,to] 的 _count 查询体。
func rangeQuery(fromUnix, toUnix int64) map[string]any {
	return map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": []any{rangeFilter(fromUnix, toUnix)},
			},
		},
	}
}

func rangeFilter(fromUnix, toUnix int64) map[string]any {
	return map[string]any{
		"range": map[string]any{
			"created_at": map[string]any{"gte": fromUnix, "lte": toUnix},
		},
	}
}
