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

func TestTransformBatchForDestination_ExtensionTypeMappings(t *testing.T) {
	base := map[string]string{
		"ext:postgis.geometry": "GEOGRAPHY",
		"vector":               "ARRAY",
	}
	spec := connector.Spec{Name: "dest", Type: connector.EndpointSnowflake}

	batch := connector.Batch{
		Schema: connector.Schema{
			Name:      "geo",
			Namespace: "public",
			Version:   1,
			Columns: []connector.Column{
				{
					Name: "shape",
					Type: "geometry",
					TypeMetadata: map[string]string{
						"extension":   "postgis",
						"type_schema": "postgis",
					},
				},
				{
					Name: "embedding",
					Type: "vector",
					TypeMetadata: map[string]string{
						"extension": "vector",
					},
				},
			},
		},
		Records: []connector.Record{
			{Table: "geo", Operation: connector.OpInsert},
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
	if cols["shape"] != "GEOGRAPHY" {
		t.Fatalf("expected shape to map to GEOGRAPHY, got %q", cols["shape"])
	}
	if cols["embedding"] != "ARRAY" {
		t.Fatalf("expected embedding to map to ARRAY, got %q", cols["embedding"])
	}
}
