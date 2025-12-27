package clickhouse

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRecordColumnsJSONArrays(t *testing.T) {
	schema := connector.Schema{
		Columns: []connector.Column{
			{Name: "arr", Type: "Array(Int32)"},
			{Name: "payload", Type: "String"},
			{Name: "doc", Type: "JSON"},
		},
	}

	arr := []any{int64(1), int64(2)}
	payload := []any{"a", "b"}
	doc := map[string]any{"items": []any{1, 2}}

	record := connector.Record{
		After: map[string]any{
			"arr":     arr,
			"payload": payload,
			"doc":     doc,
		},
	}

	cols, vals, err := recordColumns(schema, record)
	if err != nil {
		t.Fatalf("recordColumns: %v", err)
	}
	if len(cols) != 3 || len(vals) != 3 {
		t.Fatalf("unexpected lengths: cols=%d vals=%d", len(cols), len(vals))
	}

	if !reflect.DeepEqual(vals[0], arr) {
		t.Fatalf("arr val = %#v, want %#v", vals[0], arr)
	}

	expPayload, _ := json.Marshal(payload)
	if vals[1] != string(expPayload) {
		t.Fatalf("payload val = %v, want %s", vals[1], string(expPayload))
	}

	expDoc, _ := json.Marshal(doc)
	if vals[2] != string(expDoc) {
		t.Fatalf("doc val = %v, want %s", vals[2], string(expDoc))
	}
}
