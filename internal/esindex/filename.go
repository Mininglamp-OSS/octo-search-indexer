package esindex

import (
	"encoding/json"

	"golang.org/x/text/cases"
	"golang.org/x/text/width"
)

// FoldFilename is the S14 T1 contract shared with octo-server. Keep punctuation
// and whitespace, fold only character width and case (not general NFKC).
func FoldFilename(name string) string {
	return cases.Fold().String(width.Fold.String(name))
}

// MarshalJSON derives the search field at the serialization boundary so live
// ingestion, historical backfill, and virtual file documents cannot drift. The
// original name and all body/extraction metadata retain their existing values.
func (f FilePayload) MarshalJSON() ([]byte, error) {
	type plain FilePayload
	return json.Marshal(struct {
		plain
		NameLiteral string `json:"nameLiteral,omitempty"`
	}{plain: plain(f), NameLiteral: FoldFilename(f.Name)})
}
