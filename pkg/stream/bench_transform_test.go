package stream

import (
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func BenchmarkTransformMetadata(b *testing.B) {
	spec := connector.Spec{
		Name: "bench",
		Type: connector.EndpointClickHouse,
		Options: map[string]string{
			"meta_enabled":     "true",
			"append_mode":      "true",
			"soft_delete":      "false",
			"watermark_source": "timestamp",
		},
	}

	batch := sampleTransformBatch(500)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := transformBatchForDestination(batch, spec, nil); err != nil {
			b.Fatalf("transform: %v", err)
		}
	}
}

func sampleTransformBatch(rows int) connector.Batch {
	schema := connector.Schema{
		Name:      "events",
		Namespace: "public",
		Version:   1,
		Columns: []connector.Column{
			{Name: "id", Type: "int8"},
			{Name: "name", Type: "text"},
			{Name: "amount", Type: "float8"},
		},
	}

	records := make([]connector.Record, 0, rows)
	base := time.Now().UTC()
	for i := 0; i < rows; i++ {
		records = append(records, connector.Record{
			Table:         "events",
			Operation:     connector.OpUpdate,
			SchemaVersion: 1,
			After: map[string]any{
				"id":     int64(i),
				"name":   "test",
				"amount": float64(i) * 2,
			},
			Timestamp: base,
		})
	}

	return connector.Batch{
		Records:    records,
		Schema:     schema,
		Checkpoint: connector.Checkpoint{LSN: "bench"},
	}
}
