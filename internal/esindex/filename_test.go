package esindex

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestFilenameSerialization(t *testing.T) {
	for _, tc := range []struct{ name, folded string }{
		{"ＯＣＴＯ方案ｖ１．２.pdf", "octo方案v1.2.pdf"},
		{"排期  评审.PDF", "排期  评审.pdf"},
		{"100%_完成*?.pdf", "100%_完成*?.pdf"},
		{"😀Straße.pdf", "😀strasse.pdf"},
		{"ｶﾀｶﾅ.pdf", "カタカナ.pdf"},
		{"①繁體.pdf", "①繁體.pdf"},
		{" leading trailing ", " leading trailing "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := FilePayload{Name: tc.name, Content: "original body", Caption: "caption", Size: 42}
			b, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(b, &fields); err != nil {
				t.Fatal(err)
			}
			if fields["nameLiteral"] != tc.folded || fields["name"] != tc.name || fields["content"] != f.Content || fields["caption"] != f.Caption || fields["size"] != float64(42) {
				t.Fatalf("incorrect filename projection: %s", b)
			}
		})
	}
}

func TestFilenameMappingContract(t *testing.T) {
	var mapping map[string]any
	if err := json.Unmarshal(IndexMappingJSON(), &mapping); err != nil {
		t.Fatal(err)
	}
	analysis := mapping["settings"].(map[string]any)["analysis"].(map[string]any)
	tokenizer := analysis["tokenizer"].(map[string]any)["filename_chars"].(map[string]any)
	if tokenizer["min_gram"] != float64(1) || tokenizer["max_gram"] != float64(1) || len(tokenizer["token_chars"].([]any)) != 0 {
		t.Fatalf("literal positions must retain every character: %v", tokenizer)
	}
	analyzer := analysis["analyzer"].(map[string]any)["filename_literal"].(map[string]any)
	if analyzer["tokenizer"] != "filename_chars" || len(analyzer["filter"].([]any)) != 0 {
		t.Fatalf("analyzer must not strip punctuation/space: %v", analyzer)
	}
}

func TestFilenameBulkSerialization(t *testing.T) {
	fileType := 8
	doc := Doc{MessageID: 1, Payload: &Payload{Type: &fileType, File: &FilePayload{Name: "ＯＣＴＯ v1.2.pdf"}}}
	doc.Derivatives = []Doc{{MessageID: 1, SubSeq: 2, idOverride: "1-rt2", Payload: &Payload{Type: &fileType, File: &FilePayload{Name: "Straße.pdf"}}}}
	body, err := encodeBulkBody([]Doc{doc})
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	for i, want := range []string{"octo v1.2.pdf", "strasse.pdf"} {
		var update map[string]any
		if err = json.Unmarshal([]byte(lines[i*2+1]), &update); err != nil {
			t.Fatal(err)
		}
		scriptDoc := update["script"].(map[string]any)["params"].(map[string]any)["doc"]
		for _, v := range []any{scriptDoc, update["upsert"]} {
			name := v.(map[string]any)["payload"].(map[string]any)["file"].(map[string]any)["nameLiteral"]
			if name != want {
				t.Fatalf("bulk path nameLiteral=%v want=%s", name, want)
			}
		}
	}
}

func TestFilenameLiveMappingGuard(t *testing.T) {
	for _, tc := range []struct {
		field, value string
		ok           bool
	}{
		{"search_analyzer", "", true}, // OpenSearch omits an identical search analyzer.
		{"search_analyzer", "ik_smart", false},
		{"analyzer", "standard", false},
		{"type", "keyword", false},
	} {
		t.Run(tc.field+"="+tc.value, func(t *testing.T) {
			var embedded map[string]any
			if err := json.Unmarshal(IndexMappingJSON(), &embedded); err != nil {
				t.Fatal(err)
			}
			mapping := embedded["mappings"].(map[string]any)
			field := mapping["properties"].(map[string]any)["payload"].(map[string]any)["properties"].(map[string]any)["file"].(map[string]any)["properties"].(map[string]any)["nameLiteral"].(map[string]any)
			if tc.value == "" {
				delete(field, tc.field)
			} else {
				field[tc.field] = tc.value
			}
			body, _ := json.Marshal(map[string]any{"octo-message": map[string]any{"mappings": mapping}})
			writer := mappingWriter(t, &mappingTransport{body: string(body)})
			if err := writer.AssertLiveMappingCompatible(context.Background()); (err == nil) != tc.ok {
				t.Fatalf("mapping check: %v", err)
			}
		})
	}
}
