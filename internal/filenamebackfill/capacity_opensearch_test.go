package filenamebackfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

// Opt-in capacity probe, not a production benchmark. Compare the old serial
// update loop and bounded bulk on the same instance, same corpus and rate cap.
func TestFilenameBackfillCapacity(t *testing.T) {
	endpoint := os.Getenv("F3_OPENSEARCH_URL")
	n, err := strconv.Atoi(os.Getenv("F3_CAPACITY_DOCS"))
	if endpoint == "" || err != nil || n <= 0 {
		t.Skip("set isolated F3_OPENSEARCH_URL and F3_CAPACITY_DOCS")
	}
	if n > 10000 {
		t.Fatal("capacity probe is capped at 10000 documents per index")
	}
	client, err := opensearchapi.NewClient(opensearchapi.Config{Client: opensearch.Config{Addresses: []string{endpoint}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	prefix := fmt.Sprintf("test-f3-capacity-%d", time.Now().UnixNano())
	bodyText := strings.Repeat("project review schedule ", 360) // 8,640 bytes of extracted body.
	for _, mode := range []string{"serial", "bulk"} {
		index := prefix + "-" + mode
		var mapping map[string]any
		_ = json.Unmarshal(esindex.IndexMappingJSON(), &mapping)
		mapping["settings"].(map[string]any)["index"].(map[string]any)["number_of_replicas"] = 0
		if err := requestJSON(ctx, client, "PUT", "/"+index, mapping, nil); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_ = requestJSON(cleanup, client, "DELETE", "/"+index, nil, nil)
		})
		for from := 0; from < n; from += 200 {
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for i := from; i < min(from+200, n); i++ {
				_ = enc.Encode(map[string]any{"index": map[string]any{"_id": strconv.Itoa(i)}})
				_ = enc.Encode(map[string]any{"messageId": i + 1, "timestamp": 100, "channelId": "g", "channelType": 2, "payload": map[string]any{"type": 8, "file": map[string]any{"name": fmt.Sprintf("ＯＣＴＯ-排期-v1.2-%d.pdf", i), "content": bodyText}}})
			}
			res, err := client.Bulk(ctx, opensearchapi.BulkReq{Index: index, Body: &buf})
			if err != nil {
				t.Fatal(err)
			}
			if res.Errors {
				t.Fatal("fixture bulk failed")
			}
		}
		if err := requestJSON(ctx, client, "POST", "/"+index+"/_refresh", nil, nil); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		requests := 0
		if mode == "serial" {
			if err := preflight(ctx, client, index); err != nil {
				t.Fatal(err)
			}
			err := walk(ctx, client, index, 200, func(hits []opensearchapi.SearchHit) error {
				for _, hit := range hits {
					name, _, err := decodeFilename(hit)
					if err != nil {
						return err
					}
					timer := time.NewTimer(time.Millisecond)
					select {
					case <-ctx.Done():
						timer.Stop()
						return ctx.Err()
					case <-timer.C:
					}
					payload, _ := json.Marshal(map[string]any{"script": map[string]any{"lang": "painless", "source": updateScript, "params": map[string]string{"name": name, "folded": esindex.FoldFilename(name)}}})
					if _, err := client.Update(ctx, opensearchapi.UpdateReq{Index: index, DocumentID: hit.ID, Body: bytes.NewReader(payload)}); err != nil {
						return err
					}
					requests++
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := requestJSON(ctx, client, "POST", "/"+index+"/_refresh", nil, nil); err != nil {
				t.Fatal(err)
			}
		} else {
			stats, err := Run(ctx, client, index, 200, 1000)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Updated != n || stats.Verified != n || stats.Invalid != 0 {
				t.Fatal(stats)
			}
			requests = stats.Batches
			if requests != (n+199)/200 {
				t.Fatalf("unexpected bulk request count %d", requests)
			}
		}
		elapsed := time.Since(start)
		t.Logf("%s documents=%d body_bytes_per_document=%d update_requests=%d elapsed=%s docs_per_second=%.1f", mode, n, len(bodyText), requests, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
	}
}

func TestPreflightLiveWrongAnalyzer(t *testing.T) {
	endpoint := os.Getenv("F3_OPENSEARCH_URL")
	if endpoint == "" {
		t.Skip("set isolated F3_OPENSEARCH_URL")
	}
	c, err := opensearchapi.NewClient(opensearchapi.Config{Client: opensearch.Config{Addresses: []string{endpoint}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	index := fmt.Sprintf("test-f3-wrong-analyzer-%d", time.Now().UnixNano())
	var mapping map[string]any
	_ = json.Unmarshal(esindex.IndexMappingJSON(), &mapping)
	mapping["settings"].(map[string]any)["analysis"].(map[string]any)["tokenizer"].(map[string]any)["filename_chars"].(map[string]any)["token_chars"] = []string{"letter"}
	if err := requestJSON(ctx, c, http.MethodPut, "/"+index, mapping, nil); err != nil {
		t.Fatal(err)
	}
	defer requestJSON(context.Background(), c, http.MethodDelete, "/"+index, nil, nil)
	if _, err := Run(ctx, c, index, 2, 1000); err == nil {
		t.Fatal("wrong analyzer accepted even on empty index")
	}
}

func TestPreflightPopulatedUnindexedName(t *testing.T) {
	endpoint := os.Getenv("F3_OPENSEARCH_URL")
	if endpoint == "" {
		t.Skip("set F3_OPENSEARCH_URL")
	}
	c, err := opensearchapi.NewClient(opensearchapi.Config{Client: opensearch.Config{Addresses: []string{endpoint}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	index := fmt.Sprintf("test-f3-unindexed-%d", time.Now().UnixNano())
	var mapping map[string]any
	if err := json.Unmarshal(esindex.IndexMappingJSON(), &mapping); err != nil {
		t.Fatal(err)
	}
	mapping["mappings"].(map[string]any)["properties"].(map[string]any)["payload"].(map[string]any)["properties"].(map[string]any)["file"].(map[string]any)["properties"].(map[string]any)["name"].(map[string]any)["index"] = false
	if err := requestJSON(ctx, c, "PUT", "/"+index, mapping, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = requestJSON(ctx, c, "DELETE", "/"+index, nil, nil) })
	doc := map[string]any{"payload": map[string]any{"type": 8, "file": map[string]any{"name": "historical.pdf"}}}
	if err := requestJSON(ctx, c, "PUT", "/"+index+"/_doc/1?refresh=true", doc, nil); err != nil {
		t.Fatal(err)
	}
	stats, err := Run(ctx, c, index, 2, 1000)
	if err == nil || stats.Updated != 0 {
		t.Fatalf("incompatible populated index certified: %+v %v", stats, err)
	}
}
