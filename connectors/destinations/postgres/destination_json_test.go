package postgres

import (
	"encoding/json"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRecordColumnsJSONArrays(t *testing.T) {
	schema := connector.Schema{
		Columns: []connector.Column{
			{Name: "payload", Type: "jsonb"},
			{Name: "plain", Type: "text"},
		},
	}

	payload := []any{"a", map[string]any{"b": 1}}
	plain := []any{"x", "y"}

	record := connector.Record{
		After: map[string]any{
			"payload": payload,
			"plain":   plain,
		},
	}

	cols, vals, exprs, err := recordColumns(schema, record)
	if err != nil {
		t.Fatalf("recordColumns: %v", err)
	}
	if len(cols) != 2 || len(vals) != 2 || len(exprs) != 2 {
		t.Fatalf("unexpected lengths: cols=%d vals=%d exprs=%d", len(cols), len(vals), len(exprs))
	}

	expPayload, _ := json.Marshal(payload)
	expPlain, _ := json.Marshal(plain)

	if vals[0] != string(expPayload) {
		t.Fatalf("payload val = %v, want %s", vals[0], string(expPayload))
	}
	if vals[1] != string(expPlain) {
		t.Fatalf("plain val = %v, want %s", vals[1], string(expPlain))
	}

	if exprs[0] != "CAST($1 AS jsonb)" {
		t.Fatalf("payload expr = %q", exprs[0])
	}
	if exprs[1] != "$2" {
		t.Fatalf("plain expr = %q", exprs[1])
	}
}
