package filenamebackfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// requestJSON bounds metadata responses and never logs credentials or source text.
func requestJSON(ctx context.Context, c *opensearchapi.Client, method, path string, body any, out any) (err error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return err
		}
	}
	req, err := opensearch.BuildRequest(method, path, &buf, nil, nil)
	if err != nil {
		return err
	}
	res, err := c.Client.Perform(req.WithContext(ctx))
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := res.Body.Close(); err == nil {
			err = closeErr
		}
	}()
	if res.StatusCode >= 300 {
		return fmt.Errorf("filename backfill %s %s: HTTP %d", method, path, res.StatusCode)
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
		return err
	}
	return json.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(out)
}

type mappingNode struct {
	Type           string                 `json:"type"`
	Analyzer       string                 `json:"analyzer"`
	SearchAnalyzer string                 `json:"search_analyzer"`
	IndexOptions   string                 `json:"index_options"`
	Enabled        *bool                  `json:"enabled"`
	Index          *bool                  `json:"index"`
	Properties     map[string]mappingNode `json:"properties"`
	Source         struct {
		Enabled  *bool    `json:"enabled"`
		Includes []string `json:"includes"`
		Excludes []string `json:"excludes"`
	} `json:"_source"`
}

// preflight requires one concrete index and the exact character-position
// contract. No writes are allowed before this succeeds, even on an empty index.
func preflight(ctx context.Context, c *opensearchapi.Client, index string) error {
	if index == "" || strings.ContainsAny(index, "/*?,# :\\\t\r\n") || strings.HasPrefix(index, "_") {
		return fmt.Errorf("a concrete target index is required (no aliases, patterns or lists)")
	}
	var mappings map[string]struct {
		Mappings mappingNode `json:"mappings"`
	}
	if err := requestJSON(ctx, c, http.MethodGet, "/"+index+"/_mapping", nil, &mappings); err != nil {
		return err
	}
	entry, ok := mappings[index]
	if !ok || len(mappings) != 1 {
		return fmt.Errorf("target must resolve to exactly the named concrete index %q", index)
	}
	root := entry.Mappings
	// A filtered _source can silently omit existing values or break updates that
	// preserve extracted content. This migration requires the complete source.
	if root.Source.Enabled != nil && !*root.Source.Enabled || len(root.Source.Includes) > 0 || len(root.Source.Excludes) > 0 {
		return fmt.Errorf("target requires complete enabled _source without includes/excludes")
	}
	for _, node := range []mappingNode{root, root.Properties["payload"], root.Properties["payload"].Properties["file"]} {
		if node.Enabled != nil && !*node.Enabled || node.Type != "" && node.Type != "object" {
			return fmt.Errorf("filename ancestors must be enabled plain objects")
		}
	}
	payload := root.Properties["payload"]
	typ := payload.Properties["type"]
	name := payload.Properties["file"].Properties["name"]
	if typ.Type != "integer" || typ.Index != nil && !*typ.Index || name.Type != "text" || name.Index != nil && !*name.Index {
		return fmt.Errorf("audit requires indexed integer payload.type and indexed text payload.file.name")
	}
	f := root.Properties["payload"].Properties["file"].Properties["nameLiteral"]
	if f.Type != "text" || f.Analyzer != "filename_literal" || (f.SearchAnalyzer != "" && f.SearchAnalyzer != "filename_literal") || f.Index != nil && !*f.Index || (f.IndexOptions != "" && f.IndexOptions != "positions" && f.IndexOptions != "offsets") {
		return fmt.Errorf("nameLiteral must be indexed text with positions and filename_literal index/search analyzer")
	}
	var settings map[string]struct {
		Settings struct {
			Index struct {
				Analysis struct {
					Analyzer map[string]struct {
						Type       string   `json:"type"`
						Tokenizer  string   `json:"tokenizer"`
						Filter     []string `json:"filter"`
						CharFilter []string `json:"char_filter"`
					} `json:"analyzer"`
					Tokenizer map[string]struct {
						Type  string   `json:"type"`
						Min   any      `json:"min_gram"`
						Max   any      `json:"max_gram"`
						Chars []string `json:"token_chars"`
					} `json:"tokenizer"`
				} `json:"analysis"`
			} `json:"index"`
		} `json:"settings"`
	}
	if err := requestJSON(ctx, c, http.MethodGet, "/"+index+"/_settings", nil, &settings); err != nil {
		return err
	}
	a := settings[index].Settings.Index.Analysis.Analyzer["filename_literal"]
	tok := settings[index].Settings.Index.Analysis.Tokenizer[a.Tokenizer]
	if a.Type != "custom" || len(a.Filter) != 0 || len(a.CharFilter) != 0 || tok.Type != "ngram" || fmt.Sprint(tok.Min) != "1" || fmt.Sprint(tok.Max) != "1" || len(tok.Chars) != 0 {
		return fmt.Errorf("filename_literal must retain all characters at one position per code point, without filters")
	}
	const probe = "A ．.*?😀ß\t中"
	var result struct {
		Tokens []struct {
			Token    string `json:"token"`
			Position int    `json:"position"`
		} `json:"tokens"`
	}
	if err := requestJSON(ctx, c, http.MethodPost, "/"+index+"/_analyze", map[string]any{"field": "payload.file.nameLiteral", "text": probe}, &result); err != nil {
		return err
	}
	runes := []rune(probe)
	if len(result.Tokens) != len(runes) {
		return fmt.Errorf("filename analyzer probe returned wrong token count")
	}
	for i, r := range runes {
		if result.Tokens[i].Token != string(r) || result.Tokens[i].Position != i {
			return fmt.Errorf("filename analyzer probe changed a character or position at %d", i)
		}
	}
	return nil
}

// verifyIndexed checks postings as well as _source: a value stored before its
// mapping was added can exist in _source without ever having been indexed.
func verifyIndexed(ctx context.Context, c *opensearchapi.Client, index string, expected int, samples []filenameSample) error {
	filters := []any{map[string]any{"term": map[string]any{"payload.type": 8}}, map[string]any{"exists": map[string]any{"field": "payload.file.name"}}, map[string]any{"exists": map[string]any{"field": "payload.file.nameLiteral"}}}
	count := func(query any) (int, error) {
		var result struct {
			Count  int `json:"count"`
			Shards struct {
				Failed int `json:"failed"`
			} `json:"_shards"`
		}
		if err := requestJSON(ctx, c, "POST", "/"+index+"/_count", map[string]any{"query": query}, &result); err != nil {
			return 0, err
		}
		if result.Shards.Failed != 0 {
			return 0, fmt.Errorf("incomplete filename coverage count")
		}
		return result.Count, nil
	}
	got, err := count(map[string]any{"bool": map[string]any{"filter": filters}})
	if err != nil {
		return err
	}
	if got != expected {
		return fmt.Errorf("filename postings coverage %d does not match %d nonempty source names; reindex before cutover", got, expected)
	}
	for _, sample := range samples {
		chars := []rune(sample.folded)
		// Exercise an interior substring when possible, bounded to the reader's limit.
		if len(chars) > 8 {
			chars = chars[1:min(len(chars), 17)]
		}
		query := map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"ids": map[string]any{"values": []string{sample.id}}},
			map[string]any{"match_phrase": map[string]any{"payload.file.nameLiteral": map[string]any{"query": string(chars), "slop": 0}}},
		}}}
		n, err := count(query)
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("filename literal search sample %s failed; reindex before cutover", sample.id)
		}
	}
	return nil
}

type filenameSample struct{ id, folded string }
