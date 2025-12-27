package snowflake

import (
	"encoding/json"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRecordColumnsJSONArrays(t *testing.T) {
	schema := connector.Schema{
		Columns: []connector.Column{
			{Name: "payload", Type: "VARIANT"},
			{Name: "arr", Type: "ARRAY"},
			{Name: "plain", Type: "TEXT"},
		},
	}

	payload := map[string]any{"items": []any{"a", 1}}
	arr := []any{1, 2, 3}
	plain := []any{"x", "y"}

	record := connector.Record{
		After: map[string]any{
			"payload": payload,
			"arr":     arr,
			"plain":   plain,
		},
	}

	cols, vals, exprs, err := recordColumns(schema, record)
	if err != nil {
		t.Fatalf("recordColumns: %v", err)
	}
	if len(cols) != 3 || len(vals) != 3 || len(exprs) != 3 {
		t.Fatalf("unexpected lengths: cols=%d vals=%d exprs=%d", len(cols), len(vals), len(exprs))
	}

	if exprs[0] != "PARSE_JSON(?)" {
		t.Fatalf("payload expr = %q, want PARSE_JSON(?)", exprs[0])
	}
	if exprs[1] != "PARSE_JSON(?)" {
		t.Fatalf("arr expr = %q, want PARSE_JSON(?)", exprs[1])
	}
	if exprs[2] != "?" {
		t.Fatalf("plain expr = %q, want ?", exprs[2])
	}

	expPayload, _ := json.Marshal(payload)
	expArr, _ := json.Marshal(arr)
	expPlain, _ := json.Marshal(plain)

	if vals[0] != string(expPayload) {
		t.Fatalf("payload val = %v, want %s", vals[0], string(expPayload))
	}
	if vals[1] != string(expArr) {
		t.Fatalf("arr val = %v, want %s", vals[1], string(expArr))
	}
	if vals[2] != string(expPlain) {
		t.Fatalf("plain val = %v, want %s", vals[2], string(expPlain))
	}
}
