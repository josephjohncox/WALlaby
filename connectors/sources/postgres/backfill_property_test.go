package postgres

import (
	"testing"

	"pgregory.net/rapid"
)

func TestSplitTableRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schema := rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "schema")
		table := rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "table")
		choice := rapid.IntRange(0, 2).Draw(t, "choice")

		var input string
		switch choice {
		case 0:
			input = table
			gotSchema, gotTable, err := splitTable(input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotSchema != "public" || gotTable != table {
				t.Fatalf("unexpected split for %q", input)
			}
		case 1:
			input = schema + "." + table
			gotSchema, gotTable, err := splitTable(input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotSchema != schema || gotTable != table {
				t.Fatalf("unexpected split for %q", input)
			}
		case 2:
			input = schema + "." + table + ".extra"
			if _, _, err := splitTable(input); err == nil {
				t.Fatalf("expected error for %q", input)
			}
		}
	})
}
