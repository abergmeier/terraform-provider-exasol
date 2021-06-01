package computed

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseColumn(t *testing.T) {
	columns, err := parseColumnsString("VA COMMENT IS 'FOO'")
	if err != nil {
		t.Fatal("Unexpected error:", err)
	}
	expectedColumns := []ViewColumn{{
		Name:    "VA",
		Comment: "FOO",
	}}
	d := cmp.Diff(columns, expectedColumns)
	if d != "" {
		t.Fatal("Unexpected columns:", d)
	}
}
