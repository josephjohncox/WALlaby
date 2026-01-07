package stream

import (
	"testing"

	"github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	"github.com/josephjohncox/wallaby/connectors/destinations/duckdb"
	"github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestTransformBatchForDestination_ExtensionMappingsPerDestination(t *testing.T) {
	tests := []struct {
		name string
		spec connector.Spec
		want map[string]string
	}{
		{
			name: "snowflake",
			spec: connector.Spec{Name: "sf", Type: connector.EndpointSnowflake},
			want: map[string]string{
				"shape":     "STRING",
				"props":     "OBJECT",
				"embedding": "ARRAY",
				"title":     "STRING",
				"path":      "STRING",
			},
		},
		{
			name: "clickhouse",
			spec: connector.Spec{Name: "ch", Type: connector.EndpointClickHouse},
			want: map[string]string{
				"shape":     "String",
				"props":     "String",
				"embedding": "Array(Float32)",
				"title":     "String",
				"path":      "String",
			},
		},
		{
			name: "duckdb",
			spec: connector.Spec{Name: "duck", Type: connector.EndpointDuckDB},
			want: map[string]string{
				"shape":     "VARCHAR",
				"props":     "JSON",
				"embedding": "FLOAT[]",
				"title":     "VARCHAR",
				"path":      "VARCHAR",
			},
		},
	}

	batch := connector.Batch{
		Schema: connector.Schema{
			Name:      "widgets",
			Namespace: "public",
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
					Name: "props",
					Type: "hstore",
					TypeMetadata: map[string]string{
						"extension": "hstore",
					},
				},
				{
					Name: "embedding",
					Type: "vector",
					TypeMetadata: map[string]string{
						"extension": "vector",
					},
				},
				{
					Name: "title",
					Type: "citext",
					TypeMetadata: map[string]string{
						"extension": "citext",
					},
				},
				{
					Name: "path",
					Type: "ltree",
					TypeMetadata: map[string]string{
						"extension": "ltree",
					},
				},
			},
		},
		Records: []connector.Record{{Table: "widgets", Operation: connector.OpInsert}},
	}

	for _, tc := range tests {
		var base map[string]string
		switch tc.spec.Type {
		case connector.EndpointSnowflake:
			base = (&snowflake.Destination{}).TypeMappings()
		case connector.EndpointClickHouse:
			base = (&clickhouse.Destination{}).TypeMappings()
		case connector.EndpointDuckDB:
			base = (&duckdb.Destination{}).TypeMappings()
		default:
			t.Fatalf("unsupported dest %s", tc.spec.Type)
		}

		updated, changed, err := transformBatchForDestination(batch, tc.spec, base)
		if err != nil {
			t.Fatalf("%s transform: %v", tc.name, err)
		}
		if !changed {
			t.Fatalf("%s expected schema to be transformed", tc.name)
		}

		got := map[string]string{}
		for _, col := range updated.Schema.Columns {
			got[col.Name] = col.Type
		}
		for col, wantType := range tc.want {
			if got[col] != wantType {
				t.Fatalf("%s expected %s to map to %s, got %s", tc.name, col, wantType, got[col])
			}
		}
	}
}
