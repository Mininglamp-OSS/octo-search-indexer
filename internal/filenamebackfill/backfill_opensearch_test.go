package filenamebackfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

type beforeUpdateTransport struct {
	before func(*http.Request)
	base   http.RoundTripper
}

func (tr beforeUpdateTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasSuffix(r.URL.Path, "/_bulk") {
		tr.before(r)
	}
	return tr.base.RoundTrip(r)
}

func TestBackfillOpenSearchConcurrency(t *testing.T) {
	endpoint := os.Getenv("F3_OPENSEARCH_URL")
	if endpoint == "" {
		t.Skip("set F3_OPENSEARCH_URL to isolated OpenSearch + IK")
	}
	index := fmt.Sprintf("test-f3-backfill-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	request := func(method, path string, body []byte) map[string]any {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, endpoint+"/"+index+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode >= 300 {
			t.Fatalf("%s %s: %s", method, path, b)
		}
		var out map[string]any
		if err = json.Unmarshal(b, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	request("PUT", "", esindex.IndexMappingJSON())
	defer request("DELETE", "", nil)
	writer, err := esindex.NewWriter(esindex.Config{Addresses: []string{endpoint}, Index: index})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.AssertLiveMappingCompatible(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"1", "2", "3", "4"} {
		request("PUT", "/_doc/"+id, []byte(`{"messageId":123,"timestamp":100,"channelId":"g","channelType":2,"spaceId":"s","visibles":["original"],"revoked":false,"payload":{"type":8,"file":{"name":"ＯＣＴＯ v1.2.pdf","content":"body stays","contentMeta":{"truncated":true}}}}`))
	}
	request("PUT", "/_doc/5", []byte(`{"messageId":5,"timestamp":100,"channelId":"g","channelType":2,"payload":{"type":8,"file":{"name":"ｶﾞｲﾄﾞ.pdf","nameLiteral":"カ\u3099イト\u3099.pdf"}}}`))
	request("POST", "/_refresh", nil)
	tr := beforeUpdateTransport{base: http.DefaultTransport, before: func(r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		for i := 0; i < len(lines); i += 2 {
			var action struct {
				Update struct {
					ID string `json:"_id"`
				} `json:"update"`
			}
			if err := json.Unmarshal([]byte(lines[i]), &action); err != nil {
				t.Fatal(err)
			}
			switch action.Update.ID {
			case "2":
				request("POST", "/_update/2", []byte(`{"doc":{"payload":{"file":{"name":"new.pdf"}}}}`))
			case "3":
				request("DELETE", "/_doc/3", nil)
			case "4":
				request("POST", "/_update/4", []byte(`{"doc":{"visibles":["restricted"],"revoked":true}}`))
			}
		}
	}}
	client, err := opensearchapi.NewClient(opensearchapi.Config{Client: opensearch.Config{Addresses: []string{endpoint}, Transport: tr}})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := Run(ctx, client, index, 2, 1000)
	if err == nil || (stats.Scanned != 5 || stats.Updated != 3 || stats.Changed != 1 || stats.Deleted != 1 || stats.Invalid != 1) {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	request("POST", "/_refresh", nil)
	plain, err := opensearchapi.NewClient(opensearchapi.Config{Client: opensearch.Config{Addresses: []string{endpoint}}})
	if err != nil {
		t.Fatal(err)
	}
	stats, err = Run(ctx, plain, index, 2, 1000)
	if err != nil || (stats.Scanned != 4 || stats.Updated != 1 || stats.Verified != 4 || stats.Invalid != 0) {
		t.Fatalf("rerun stats=%+v err=%v", stats, err)
	}
	request("POST", "/_refresh", nil)
	stats, err = Run(ctx, plain, index, 2, 1000)
	if err != nil || (stats.Scanned != 4 || stats.NeedsUpdate != 0 || stats.Batches != 0 || stats.Verified != 4 || stats.Invalid != 0) {
		t.Fatalf("idempotence stats=%+v err=%v", stats, err)
	}
	doc := request("GET", "/_doc/4", nil)["_source"].(map[string]any)
	file := doc["payload"].(map[string]any)["file"].(map[string]any)
	if doc["revoked"] != true || doc["visibles"].([]any)[0] != "restricted" || file["content"] != "body stays" || file["nameLiteral"] != "octo v1.2.pdf" || file["contentMeta"].(map[string]any)["truncated"] != true {
		t.Fatalf("backfill clobbered source: %v", doc)
	}
	kana := request("GET", "/_doc/5", nil)["_source"].(map[string]any)["payload"].(map[string]any)["file"].(map[string]any)
	if kana["name"] != "ｶﾞｲﾄﾞ.pdf" || kana["nameLiteral"] != "ガイド.pdf" {
		t.Fatalf("old non-NFC field not repaired: %v", kana)
	}
	for _, query := range []string{"ガイド", "ｶﾞｲﾄﾞ"} {
		body, err := json.Marshal(map[string]any{"query": map[string]any{"match_phrase": map[string]any{"payload.file.nameLiteral": esindex.FoldFilename(query)}}})
		if err != nil {
			t.Fatal(err)
		}
		hits := request("POST", "/_search", body)["hits"].(map[string]any)["hits"].([]any)
		if len(hits) != 1 || hits[0].(map[string]any)["_id"] != "5" {
			t.Fatalf("kana search: %v", hits)
		}
	}
	renamed := request("GET", "/_doc/2", nil)["_source"].(map[string]any)["payload"].(map[string]any)["file"].(map[string]any)
	if renamed["name"] != "new.pdf" || renamed["nameLiteral"] != "new.pdf" {
		t.Fatalf("stale name restored: %v", renamed)
	}
}
