package connector

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkBatchContentHash(b *testing.B) {
	batch := hashBenchmarkBatch(1_000)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := BatchContentHash(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func hashBenchmarkBatch(records int) Batch {
	batch := Batch{
		Schema: Schema{
			Name:      "events",
			Namespace: "public",
			Version:   4,
			Columns: []Column{
				{Name: "id", Type: "int8"},
				{Name: "name", Type: "text"},
				{Name: "amount", Type: "float8"},
			},
		},
		Checkpoint: Checkpoint{LSN: "0/1000", Metadata: map[string]string{"txid": "42"}},
		Records:    make([]Record, 0, records),
	}
	for index := 0; index < records; index++ {
		batch.Records = append(batch.Records, Record{
			Table:         "events",
			Operation:     OpUpdate,
			SchemaVersion: 4,
			Key:           []byte(fmt.Sprintf("key-%08d", index)),
			After: map[string]any{
				"id":     int64(index),
				"name":   fmt.Sprintf("event-%d", index),
				"amount": float64(index) * 1.25,
			},
			Timestamp: time.Unix(int64(index), 0).UTC(),
		})
	}
	return batch
}
