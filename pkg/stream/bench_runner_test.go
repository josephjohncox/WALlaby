package stream

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/josephjohncox/ductstream/pkg/connector"
)

func BenchmarkStreamThroughput1Dest(b *testing.B) { benchmarkStream(b, 1, 1000) }
func BenchmarkStreamThroughput4Dest(b *testing.B) { benchmarkStream(b, 4, 1000) }
func BenchmarkStreamThroughput8Dest(b *testing.B) { benchmarkStream(b, 8, 1000) }

func benchmarkStream(b *testing.B, destCount, batchSize int) {
	ctx := context.Background()
	batch := sampleStreamBatch(batchSize)

	dests := make([]DestinationConfig, 0, destCount)
	for i := 0; i < destCount; i++ {
		spec := connector.Spec{
			Name: "dest",
			Type: connector.EndpointClickHouse,
			Options: map[string]string{
				"meta_enabled":     "true",
				"append_mode":      "true",
				"watermark_source": "timestamp",
			},
		}
		dests = append(dests, DestinationConfig{Spec: spec, Dest: &benchDestination{}})
	}

	runner := Runner{
		Destinations: dests,
		Parallelism:  destCount,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := runner.writeDestinations(ctx, batch); err != nil {
			b.Fatalf("write destinations: %v", err)
		}
	}
}

type benchDestination struct {
	count int64
}

func (d *benchDestination) Open(context.Context, connector.Spec) error { return nil }

func (d *benchDestination) Write(_ context.Context, batch connector.Batch) error {
	atomic.AddInt64(&d.count, int64(len(batch.Records)))
	return nil
}

func (d *benchDestination) Close(context.Context) error { return nil }

func (d *benchDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsBulkLoad:      true,
	}
}

func sampleStreamBatch(rows int) connector.Batch {
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
