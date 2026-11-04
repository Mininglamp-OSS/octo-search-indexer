package filenamebackfill

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Mininglamp-OSS/octo-search-indexer/internal/esindex"
	"github.com/opensearch-project/opensearch-go/v3"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

func mockMetadata(t *testing.T, w http.ResponseWriter, r *http.Request, mutate func(string, map[string]any)) bool {
	t.Helper()
	var embedded map[string]any
	if err := json.Unmarshal(esindex.IndexMappingJSON(), &embedded); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	switch r.URL.Path {
	case "/target/_mapping":
		body = map[string]any{"target": map[string]any{"mappings": embedded["mappings"]}}
	case "/target/_settings":
		body = map[string]any{"target": map[string]any{"settings": map[string]any{"index": map[string]any{"analysis": embedded["settings"].(map[string]any)["analysis"]}}}}
	case "/target/_analyze":
		var tokens []any
		for i, r := range []rune("A ．.*?😀ß\t中") {
			tokens = append(tokens, map[string]any{"token": string(r), "position": i})
		}
		body = map[string]any{"tokens": tokens}
	default:
		return false
	}
	if mutate != nil {
		mutate(r.URL.Path, body)
	}
	_ = json.NewEncoder(w).Encode(body)
	return true
}
func testClient(t *testing.T, handler http.HandlerFunc) *opensearchapi.Client {
	t.Helper()
	s := httptest.NewServer(handler)
	t.Cleanup(s.Close)
	c, err := opensearchapi.NewClient(opensearchapi.Config{Client: opensearch.Config{Addresses: []string{s.URL}, DisableRetry: true}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRunBulkAndVerification(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		result, kind string
		wantErr      bool
	}{
		{"updated", 200, "updated", "", false}, {"rename", 200, "noop", "", true},
		{"deleted", 404, "", "document_missing_exception", false},
		{"index-missing", 404, "", "index_not_found_exception", true},
		{"conflict", 409, "", "version_conflict_engine_exception", true},
		{"throttled", 429, "", "rejected_execution_exception", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refreshed := false
			bulkCount := 0
			cleared := 0
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if mockMetadata(t, w, r, nil) {
					return
				}
				switch {
				case r.URL.Path == "/target/_search":
					var q map[string]any
					_ = json.NewDecoder(r.Body).Decode(&q)
					if _, ok := q["query"].(map[string]any)["match_all"]; !ok {
						t.Error("source audit must not depend on indexed eligibility")
					}
					fields := q["_source"].([]any)
					if len(fields) != 3 || fields[0] != "payload.file.name" || fields[1] != "payload.file.nameLiteral" {
						t.Errorf("unbounded source: %v", fields)
					}
					name, literal := "ＯＣＴＯ v1.2.pdf", "wrong-existing-value"
					if refreshed && tc.name == "updated" {
						literal = "octo v1.2.pdf"
					}
					if refreshed && tc.name == "rename" {
						name = "new.pdf"
					}
					hits := []any{map[string]any{"_index": "target", "_id": "123-rt2", "_source": map[string]any{"payload": map[string]any{"type": 8, "file": map[string]string{"name": name, "nameLiteral": literal}}}}}
					if refreshed && tc.name == "deleted" {
						hits = nil
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"_scroll_id": "scroll", "hits": map[string]any{"hits": hits}})
				case r.URL.Path == "/target/_bulk":
					bulkCount++
					dec := json.NewDecoder(r.Body)
					var action, body map[string]any
					if err := dec.Decode(&action); err != nil {
						t.Error(err)
					}
					if err := dec.Decode(&body); err != nil {
						t.Error(err)
					}
					if action["update"].(map[string]any)["_id"] != "123-rt2" || len(body) != 1 || body["script"] == nil {
						t.Errorf("must be exact ID script-only update: %v %v", action, body)
					}
					params := body["script"].(map[string]any)["params"].(map[string]any)
					if params["folded"] != "octo v1.2.pdf" {
						t.Error(params)
					}
					item := map[string]any{"_index": "target", "_id": "123-rt2", "status": tc.status, "result": tc.result}
					if tc.kind != "" {
						item["error"] = map[string]string{"type": tc.kind}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"errors": tc.status >= 400, "items": []any{map[string]any{"update": item}}})
				case r.URL.Path == "/target/_count":
					if tc.name == "deleted" {
						fmt.Fprint(w, `{"count":0}`)
					} else {
						fmt.Fprint(w, `{"count":1}`)
					}
				case r.URL.Path == "/target/_refresh":
					refreshed = true
					fmt.Fprint(w, `{"_shards":{"failed":0}}`)
				case r.URL.Path == "/_search/scroll" && r.Method == "DELETE":
					cleared++
					fmt.Fprint(w, `{"succeeded":true}`)
				case r.URL.Path == "/_search/scroll":
					fmt.Fprint(w, `{"_scroll_id":"scroll","hits":{"hits":[]}}`)
				default:
					t.Errorf("unexpected %s", r.URL)
					w.WriteHeader(500)
				}
			})
			stats, err := Run(context.Background(), c, "target", 2, 1000)
			if (err != nil) != tc.wantErr || bulkCount != 1 || cleared == 0 {
				t.Fatalf("stats=%+v err=%v bulk=%d cleanup=%d", stats, err, bulkCount, cleared)
			}
			if tc.name == "rename" && stats.Invalid != 1 {
				t.Fatal("rename was not caught by verification")
			}
			if tc.name == "updated" && (stats.Updated != 1 || stats.Verified != 1 || stats.Invalid != 0) {
				t.Fatal(stats)
			}
		})
	}
}

func TestPreflightRejectsBeforeSearchOrWrite(t *testing.T) {
	for _, which := range []string{"name-index", "type-index", "ancestor", "alias", "field", "positions", "source", "tokenizer", "filter", "probe"} {
		t.Run(which, func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if mockMetadata(t, w, r, func(path string, b map[string]any) {
					if path == "/target/_mapping" {
						if which == "alias" {
							b["other"] = b["target"]
							delete(b, "target")
							return
						}
						m := b["target"].(map[string]any)["mappings"].(map[string]any)
						payload := m["properties"].(map[string]any)["payload"].(map[string]any)
						if which == "ancestor" {
							payload["type"] = "nested"
						}
						if which == "type-index" {
							payload["properties"].(map[string]any)["type"].(map[string]any)["index"] = false
						}
						if which == "name-index" {
							payload["properties"].(map[string]any)["file"].(map[string]any)["properties"].(map[string]any)["name"].(map[string]any)["index"] = false
						}

						if which == "source" {
							m["_source"] = map[string]any{"excludes": []string{"payload.file.nameLiteral"}}
						}
						f := m["properties"].(map[string]any)["payload"].(map[string]any)["properties"].(map[string]any)["file"].(map[string]any)["properties"].(map[string]any)["nameLiteral"].(map[string]any)
						if which == "field" {
							f["type"] = "keyword"
						}
						if which == "positions" {
							f["index_options"] = "docs"
						}
					}
					if path == "/target/_settings" {
						a := b["target"].(map[string]any)["settings"].(map[string]any)["index"].(map[string]any)["analysis"].(map[string]any)
						if which == "tokenizer" {
							a["tokenizer"].(map[string]any)["filename_chars"].(map[string]any)["token_chars"] = []string{"letter"}
						}
						if which == "filter" {
							a["analyzer"].(map[string]any)["filename_literal"].(map[string]any)["filter"] = []string{"lowercase"}
						}
					}
					if path == "/target/_analyze" && which == "probe" {
						b["tokens"] = []any{}
					}
				}) {
					return
				}
				t.Errorf("unexpected data operation before rejection: %s", r.URL)
				w.WriteHeader(500)
			})
			if _, err := Run(context.Background(), c, "target", 2, 1000); err == nil {
				t.Fatal("invalid mapping accepted")
			}
		})
	}
}

func TestDryRunNeverWrites(t *testing.T) {
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if mockMetadata(t, w, r, nil) {
			return
		}
		switch r.URL.Path {
		case "/target/_search":
			fmt.Fprint(w, `{"_scroll_id":"s","hits":{"hits":[{"_index":"target","_id":"1","_source":{"payload":{"type":8,"file":{"name":"ＯＣＴＯ","nameLiteral":"wrong"}}}}]}}`)
		case "/_search/scroll":
			if r.Method == "DELETE" {
				fmt.Fprint(w, `{"succeeded":true}`)
			} else {
				fmt.Fprint(w, `{"hits":{"hits":[]}}`)
			}
		default:
			t.Errorf("dry-run mutated data: %s", r.URL)
			w.WriteHeader(500)
		}
	})
	s, err := RunWithOptions(context.Background(), c, "target", Options{PageSize: 2, Rate: 1000, DryRun: true})
	if err != nil || s.NeedsUpdate != 1 || s.Updated != 0 || s.Batches != 0 {
		t.Fatalf("stats=%+v err=%v", s, err)
	}
}

func TestRunRejectsIncompleteSearch(t *testing.T) {
	for _, response := range []string{`{"timed_out":true,"hits":{"hits":[]}}`, `{"_shards":{"failed":1},"hits":{"hits":[]}}`} {
		c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if !mockMetadata(t, w, r, nil) {
				fmt.Fprint(w, response)
			}
		})
		if _, err := Run(context.Background(), c, "target", 1, 1000); err == nil {
			t.Fatal("incomplete scan accepted")
		}
	}
}

func TestBulkByteLimit(t *testing.T) {
	// One oversized name fails before issuing a bulk request.
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if mockMetadata(t, w, r, nil) {
			return
		}
		if r.URL.Path == "/_search/scroll" && r.Method == "DELETE" {
			fmt.Fprint(w, `{"succeeded":true}`)
			return
		}
		if r.URL.Path != "/target/_search" {
			t.Errorf("oversized update sent: %s", r.URL)
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"_scroll_id": "s", "hits": map[string]any{"hits": []any{map[string]any{"_index": "target", "_id": "1", "_source": map[string]any{"payload": map[string]any{"type": 8, "file": map[string]string{"name": strings.Repeat("x", maxBulkBytes)}}}}}}})
	})
	if _, err := Run(context.Background(), c, "target", 1, 1000); err == nil {
		t.Fatal("oversized update accepted")
	}
}

func TestVerificationRequiresIndexedCoverageAndSearch(t *testing.T) {
	for _, tc := range []struct {
		name             string
		coverage, sample int
		failed           bool
		wantError        bool
	}{
		{"source-only", 0, 0, false, true}, {"wrong-postings", 1, 0, false, true}, {"partial-count", 1, 1, true, true}, {"valid", 1, 1, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				n := tc.coverage
				if calls > 1 {
					n = tc.sample
				}
				failed := 0
				if tc.failed {
					failed = 1
				}
				fmt.Fprintf(w, `{"count":%d,"_shards":{"failed":%d}}`, n, failed)
			})
			err := verifyIndexed(context.Background(), c, "target", 1, []filenameSample{{id: "1", folded: "octo v1.2.pdf"}})
			if (err != nil) != tc.wantError {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestBulkPartialFailureReportsAllItems(t *testing.T) {
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"errors":true,"items":[{"update":{"_index":"target","_id":"1","status":200,"result":"updated"}},{"update":{"_index":"target","_id":"2","status":409,"error":{"type":"version_conflict_engine_exception"}}},{"update":{"_index":"target","_id":"3","status":404,"error":{"type":"document_missing_exception"}}}]}`)
	})
	var stats Stats
	batch := []pendingUpdate{{id: "1"}, {id: "2"}, {id: "3"}}
	if err := writeBatch(context.Background(), c, "target", batch, &stats); err == nil {
		t.Fatal("partial bulk failure accepted")
	}
	if stats.Updated != 1 || stats.Deleted != 1 || stats.Batches != 1 {
		t.Fatal(stats)
	}
}
