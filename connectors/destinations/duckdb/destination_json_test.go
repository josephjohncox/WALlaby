package duckdb

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRecordColumnsJSONArrays(t *testing.T) {
	schema := connector.Schema{
		Columns: []connector.Column{
			{Name: "doc", Type: "JSON"},
			{Name: "arr", Type: "INTEGER[]"},
			{Name: "plain", Type: "VARCHAR"},
		},
	}

	doc := map[string]any{"items": []any{1, 2}}
	arr := []any{int64(1), int64(2)}
	plain := []any{"x", "y"}

	record := connector.Record{
		After: map[string]any{
			"doc":   doc,
			"arr":   arr,
			"plain": plain,
		},
	}

	cols, vals, exprs, err := recordColumns(schema, record)
	if err != nil {
		t.Fatalf("recordColumns: %v", err)
	}
	if len(cols) != 3 || len(vals) != 3 || len(exprs) != 3 {
		t.Fatalf("unexpected lengths: cols=%d vals=%d exprs=%d", len(cols), len(vals), len(exprs))
	}

	if exprs[0] != "CAST(? AS JSON)" {
		t.Fatalf("doc expr = %q, want CAST(? AS JSON)", exprs[0])
	}
	if exprs[1] != "?" {
		t.Fatalf("arr expr = %q, want ?", exprs[1])
	}
	if exprs[2] != "?" {
		t.Fatalf("plain expr = %q, want ?", exprs[2])
	}

	expDoc, _ := json.Marshal(doc)
	if vals[0] != string(expDoc) {
		t.Fatalf("doc val = %v, want %s", vals[0], string(expDoc))
	}

	if !reflect.DeepEqual(vals[1], arr) {
		t.Fatalf("arr val = %#v, want %#v", vals[1], arr)
	}

	expPlain, _ := json.Marshal(plain)
	if vals[2] != string(expPlain) {
		t.Fatalf("plain val = %v, want %s", vals[2], string(expPlain))
	}
}
