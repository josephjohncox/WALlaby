package wire

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestBuildArrowRecordJSONArrays(t *testing.T) {
	schema := connector.Schema{
		Name:      "items",
		Namespace: "public",
		Version:   1,
		Columns: []connector.Column{
			{Name: "payload", Type: "jsonb"},
		},
	}

	payload := []any{map[string]any{"a": 1}, "b"}
	batch := connector.Batch{
		Schema: schema,
		Records: []connector.Record{
			{
				Operation:     connector.OpInsert,
				SchemaVersion: 1,
				Table:         "items",
				Timestamp:     time.Now().UTC(),
				After: map[string]any{
					"payload": payload,
				},
			},
		},
	}

	rec, err := buildArrowRecord(batch)
	if err != nil {
		t.Fatalf("buildArrowRecord: %v", err)
	}
	if rec == nil {
		t.Fatal("expected non-nil record")
	}
	defer rec.Release()

	idx := -1
	for i, field := range rec.Schema().Fields() {
		if field.Name == "payload" {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatal("payload field not found")
	}

	col, ok := rec.Column(idx).(*array.Binary)
	if !ok {
		t.Fatalf("payload column type = %T, want *array.Binary", rec.Column(idx))
	}
	if col.Len() != 1 {
		t.Fatalf("payload column len = %d, want 1", col.Len())
	}
	if col.IsNull(0) {
		t.Fatal("payload column is null")
	}

	expected, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if got := col.Value(0); string(got) != string(expected) {
		t.Fatalf("payload value = %q, want %q", string(got), string(expected))
	}
}
