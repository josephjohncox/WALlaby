package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func BenchmarkStreamHarness(b *testing.B) {
	ctx := context.Background()
	parallelismLevels := []int{1, 2, 4, 8}
	widths := []int{4, 16, 64}
	batchSize := 1000

	batches := make(map[int]connector.Batch, len(widths))
	for _, width := range widths {
		batches[width] = sampleStreamBatchWidth(batchSize, width)
	}

	for _, width := range widths {
		batch := batches[width]
		for _, parallelism := range parallelismLevels {
			label := fmt.Sprintf("parallel-%d/cols-%d", parallelism, width)
			b.Run(label, func(b *testing.B) {
				dests := make([]DestinationConfig, 0, parallelism)
				for i := 0; i < parallelism; i++ {
					spec := connector.Spec{
						Name: fmt.Sprintf("dest-%d", i),
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
					Parallelism:  parallelism,
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := runner.writeDestinations(ctx, batch, runner.Destinations); err != nil {
						b.Fatalf("write destinations: %v", err)
					}
				}
			})
		}
	}
}

func sampleStreamBatchWidth(rows, width int) connector.Batch {
	if width < 1 {
		width = 1
	}
	columns := make([]connector.Column, 0, width)
	for i := 0; i < width; i++ {
		columns = append(columns, connector.Column{
			Name: fmt.Sprintf("col_%02d", i+1),
			Type: "int8",
		})
	}

	schema := connector.Schema{
		Name:      "bench_events",
		Namespace: "public",
		Version:   1,
		Columns:   columns,
	}

	records := make([]connector.Record, 0, rows)
	base := time.Now().UTC()
	for i := 0; i < rows; i++ {
		after := make(map[string]any, width)
		for colIdx, col := range columns {
			after[col.Name] = int64(i*width + colIdx)
		}
		records = append(records, connector.Record{
			Table:         "bench_events",
			Operation:     connector.OpUpdate,
			SchemaVersion: 1,
			After:         after,
			Timestamp:     base,
		})
	}

	return connector.Batch{
		Records:    records,
		Schema:     schema,
		Checkpoint: connector.Checkpoint{LSN: "bench"},
	}
}
