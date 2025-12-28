package stream

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestTransformBatchForDestination_TypeMappings(t *testing.T) {
	base := map[string]string{
		"jsonb": "VARIANT",
		"int8":  "NUMBER",
	}
	spec := connector.Spec{
		Name: "dest",
		Type: connector.EndpointSnowflake,
		Options: map[string]string{
			"type_mappings": `{"jsonb":"STRING"}`,
		},
	}

	batch := connector.Batch{
		Schema: connector.Schema{
			Name:      "events",
			Namespace: "public",
			Version:   1,
			Columns: []connector.Column{
				{Name: "id", Type: "int8"},
				{Name: "payload", Type: "jsonb"},
			},
		},
		Records: []connector.Record{
			{Table: "events", Operation: connector.OpInsert},
		},
	}

	updated, changed, err := transformBatchForDestination(batch, spec, base)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if !changed {
		t.Fatalf("expected schema to be transformed")
	}

	cols := map[string]string{}
	for _, col := range updated.Schema.Columns {
		cols[col.Name] = col.Type
	}
	if cols["id"] != "NUMBER" {
		t.Fatalf("expected id to map to NUMBER, got %q", cols["id"])
	}
	if cols["payload"] != "STRING" {
		t.Fatalf("expected payload override to STRING, got %q", cols["payload"])
	}
}
