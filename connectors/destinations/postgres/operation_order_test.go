package postgres

import (
	"reflect"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresTargetPreservesSameKeyOperationOrder(t *testing.T) {
	schema := connector.Schema{
		Namespace: "public",
		Name:      "widgets",
		Columns: []connector.Column{
			{Name: "id", Type: "bigint"},
			{Name: "value", Type: "text"},
		},
	}
	records := []connector.Record{
		{Table: "widgets", Operation: connector.OpDelete, Key: []byte(`{"id":1}`)},
		{Table: "widgets", Operation: connector.OpInsert, Key: []byte(`{"id":1}`), After: map[string]any{"id": 1, "value": "first"}},
		{Table: "widgets", Operation: connector.OpUpdate, Key: []byte(`{"id":1}`), Before: map[string]any{"id": 1, "value": "first"}, After: map[string]any{"id": 1, "value": "second"}},
		{Table: "widgets", Operation: connector.OpDelete, Key: []byte(`{"id":1}`)},
	}

	groups, err := planTargetOperations(schema, records)
	if err != nil {
		t.Fatal(err)
	}
	var got []connector.Operation
	for _, group := range groups {
		for _, record := range group.records {
			got = append(got, record.Operation)
		}
	}
	want := []connector.Operation{connector.OpDelete, connector.OpInsert, connector.OpUpdate, connector.OpDelete}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("operation order=%v, want %v", got, want)
	}
	if len(groups) != 3 || groups[0].kind != targetOperationDelete || groups[1].kind != targetOperationUpsert || groups[2].kind != targetOperationDelete {
		t.Fatalf("operation groups=%+v, want delete/upsert/delete", groups)
	}
}

func TestPostgresTargetSeparatesChainedKeyChanges(t *testing.T) {
	schema := connector.Schema{
		Namespace: "public",
		Name:      "widgets",
		Columns: []connector.Column{
			{Name: "id", Type: "bigint"},
			{Name: "value", Type: "text"},
		},
	}
	records := []connector.Record{
		{Table: "widgets", Operation: connector.OpUpdate, Key: []byte(`{"id":1}`), Before: map[string]any{"id": 1}, After: map[string]any{"id": 2}},
		{Table: "widgets", Operation: connector.OpUpdate, Key: []byte(`{"id":2}`), Before: map[string]any{"id": 2}, After: map[string]any{"id": 3}},
	}

	groups, err := planTargetOperations(schema, records)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("key-change groups=%d, want 2 separately ordered statements", len(groups))
	}
	for index, group := range groups {
		if group.kind != targetOperationKeyChange || len(group.records) != 1 || !reflect.DeepEqual(group.records[0].After, records[index].After) {
			t.Fatalf("group %d=%+v, want one ordered key change", index, group)
		}
	}
}
