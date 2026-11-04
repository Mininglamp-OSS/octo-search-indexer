// Package filenamebackfill maintains only the derived filename field in existing
// OpenSearch documents. It never replays stale visibility or body data.
package filenamebackfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// Compare the current name before updating; no upsert or stale source replay.
const updateScript = `if (ctx._source.payload != null && ctx._source.payload.type == 8 && ctx._source.payload.file != null && ctx._source.payload.file.name == params.name && ctx._source.payload.file.nameLiteral != params.folded) {
 ctx._source.payload.file.nameLiteral = params.folded;
} else { ctx.op = 'noop'; }`

const maxBulkBytes = 1 << 20

type Stats struct {
	Scanned, NeedsUpdate, Updated, Changed, Deleted, Batches int
	Verified, Invalid                                        int
}

type Options struct {
	PageSize, Rate int
	DryRun         bool
}

func Run(ctx context.Context, client *opensearchapi.Client, index string, size, rate int) (Stats, error) {
	return RunWithOptions(ctx, client, index, Options{PageSize: size, Rate: rate})
}

// RunWithOptions validates the index before any write and verifies every eligible
// derived value after refreshing. DryRun is read-only and estimates all repairs,
// including incorrect values that an exists-only check would miss.
func RunWithOptions(ctx context.Context, client *opensearchapi.Client, index string, opt Options) (stats Stats, err error) {
	if opt.PageSize < 1 || opt.PageSize > 500 || opt.Rate < 1 || opt.Rate > 1000 {
		return stats, fmt.Errorf("page-size must be 1..500 and rate 1..1000")
	}
	if err = preflight(ctx, client, index); err != nil {
		return stats, err
	}
	var batch []pendingUpdate
	bytesInBatch := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		// One in-flight request, bounded burst and average update rate. Waiting is
		// cancellable; scroll keepalive is much longer than the maximum one-second wait.
		timer := time.NewTimer(time.Duration(len(batch)) * time.Second / time.Duration(opt.Rate))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		err := writeBatch(ctx, client, index, batch, &stats)
		batch = nil
		bytesInBatch = 0
		return err
	}
	err = walk(ctx, client, index, opt.PageSize, func(hits []opensearchapi.SearchHit) error {
		for _, hit := range hits {
			stats.Scanned++
			name, literal, err := decodeFilename(hit)
			if err != nil {
				return err
			}
			folded := esindex.FoldFilename(name)
			if literal == folded {
				continue
			}
			stats.NeedsUpdate++
			if opt.DryRun {
				continue
			}
			update, err := encodeUpdate(hit.ID, name, folded)
			if err != nil {
				return err
			}
			if len(update) > maxBulkBytes {
				return fmt.Errorf("filename update %s exceeds %d-byte bulk limit", hit.ID, maxBulkBytes)
			}
			if len(batch) > 0 && (bytesInBatch+len(update) > maxBulkBytes || len(batch) >= min(opt.PageSize, opt.Rate)) {
				if err := flush(); err != nil {
					return err
				}
			}
			batch = append(batch, pendingUpdate{id: hit.ID, body: update})
			bytesInBatch += len(update)
		}
		return flush()
	})
	if err != nil || opt.DryRun {
		return stats, err
	}
	var refreshed struct {
		Shards struct {
			Failed int `json:"failed"`
		} `json:"_shards"`
	}
	if err = requestJSON(ctx, client, "POST", "/"+index+"/_refresh", nil, &refreshed); err != nil {
		return stats, err
	}
	if refreshed.Shards.Failed > 0 {
		return stats, fmt.Errorf("incomplete filename refresh")
	}
	expectedIndexed := 0
	var samples []filenameSample
	err = walk(ctx, client, index, opt.PageSize, func(hits []opensearchapi.SearchHit) error {
		for _, hit := range hits {
			name, literal, err := decodeFilename(hit)
			if err != nil {
				return err
			}
			stats.Verified++
			if name != "" {
				expectedIndexed++
				if len(samples) < 8 {
					samples = append(samples, filenameSample{id: hit.ID, folded: esindex.FoldFilename(name)})
				}
			}
			if literal != esindex.FoldFilename(name) {
				stats.Invalid++
			}
		}
		return nil
	})
	if err == nil && stats.Invalid > 0 {
		err = fmt.Errorf("%d of %d filenames failed verification; pause old writers and rerun before cutover", stats.Invalid, stats.Verified)
	}
	if err == nil {
		err = verifyIndexed(ctx, client, index, expectedIndexed, samples)
	}
	return stats, err
}

type pendingUpdate struct {
	id   string
	body []byte
}

func encodeUpdate(id, name, folded string) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(map[string]any{"update": map[string]any{"_id": id, "retry_on_conflict": 3}}); err != nil {
		return nil, err
	}
	err := enc.Encode(map[string]any{"script": map[string]any{"lang": "painless", "source": updateScript, "params": map[string]string{"name": name, "folded": folded}}})
	return buf.Bytes(), err
}

func writeBatch(ctx context.Context, client *opensearchapi.Client, index string, batch []pendingUpdate, stats *Stats) error {
	var body bytes.Buffer
	for _, item := range batch {
		body.Write(item.body)
	}
	resp, err := client.Bulk(ctx, opensearchapi.BulkReq{Index: index, Body: &body})
	if err != nil {
		return err
	}
	stats.Batches++
	if len(resp.Items) != len(batch) {
		return fmt.Errorf("incomplete filename bulk response")
	}
	var failure error
	for i, item := range resp.Items {
		result, ok := item["update"]
		if !ok || result.ID != batch[i].id || result.Index != index {
			return fmt.Errorf("filename bulk response identity mismatch")
		}
		switch {
		case result.Status == 404 && result.Error != nil && result.Error.Type == "document_missing_exception":
			stats.Deleted++
		case result.Status >= 200 && result.Status < 300 && result.Error == nil:
			if result.Result == "noop" {
				stats.Changed++
			} else {
				stats.Updated++
			}
		default:
			failure = fmt.Errorf("filename bulk item %s failed with status %d; rerun safely after resolving the failure", batch[i].id, result.Status)
		}
	}
	return failure
}

func decodeFilename(hit opensearchapi.SearchHit) (string, string, error) {
	var doc struct {
		Payload struct {
			File struct {
				Name    string `json:"name"`
				Literal string `json:"nameLiteral"`
			} `json:"file"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(hit.Source, &doc); err != nil {
		return "", "", fmt.Errorf("decode filename %s: %w", hit.ID, err)
	}
	return doc.Payload.File.Name, doc.Payload.File.Literal, nil
}

// Offline migration audit only: bounded filename-only pages, never file bodies.
// Scanning all eligible names also finds stale nonempty derived values on reruns.
func walk(ctx context.Context, client *opensearchapi.Client, index string, size int, visit func([]opensearchapi.SearchHit) error) error {
	query := map[string]any{"size": size, "sort": []string{"_doc"}, "_source": []string{"payload.file.name", "payload.file.nameLiteral", "payload.type"}, "query": map[string]any{"match_all": map[string]any{}}}
	body, _ := json.Marshal(query)
	first, err := client.Search(ctx, &opensearchapi.SearchReq{Indices: []string{index}, Body: bytes.NewReader(body), Params: opensearchapi.SearchParams{Scroll: 15 * time.Minute}})
	if err != nil {
		return err
	}
	scrollID := ""
	if first.ScrollID != nil {
		scrollID = *first.ScrollID
	}
	defer func() {
		if scrollID != "" {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			b, _ := json.Marshal(map[string]any{"scroll_id": []string{scrollID}})
			_, _ = client.Scroll.Delete(cleanup, opensearchapi.ScrollDeleteReq{Body: bytes.NewReader(b)})
		}
	}()
	if first.Timeout || first.Shards.Failed > 0 {
		return fmt.Errorf("incomplete filename scroll")
	}
	hits := first.Hits.Hits
	for len(hits) > 0 {
		for _, hit := range hits {
			if hit.Index != index {
				return fmt.Errorf("filename scroll returned unexpected concrete index")
			}
		}
		// Select from source, independently of indexed eligibility fields, so an
		// indexed exists/term predicate cannot silently hide historical files.
		eligible := make([]opensearchapi.SearchHit, 0, len(hits))
		for _, hit := range hits {
			var source struct {
				Payload struct {
					Type int `json:"type"`
					File struct {
						Name *string `json:"name"`
					} `json:"file"`
				} `json:"payload"`
			}
			if err := json.Unmarshal(hit.Source, &source); err != nil {
				return err
			}
			if source.Payload.Type == 8 && source.Payload.File.Name != nil {
				eligible = append(eligible, hit)
			}
		}
		if err := visit(eligible); err != nil {
			return err
		}
		if scrollID == "" {
			return fmt.Errorf("missing filename scroll ID")
		}
		next, err := client.Scroll.Get(ctx, opensearchapi.ScrollGetReq{ScrollID: scrollID, Params: opensearchapi.ScrollGetParams{Scroll: 15 * time.Minute}})
		if err != nil {
			return err
		}
		if next.ScrollID != nil {
			scrollID = *next.ScrollID
		}
		if next.Timeout || next.Shards.Failed > 0 {
			return fmt.Errorf("incomplete filename scroll")
		}
		hits = next.Hits.Hits
	}
	return nil
}
