package wire

import (
	"fmt"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func BenchmarkCodecEncodeArrow(b *testing.B)   { benchmarkCodec(b, connector.WireFormatArrow) }
func BenchmarkCodecEncodeParquet(b *testing.B) { benchmarkCodec(b, connector.WireFormatParquet) }
func BenchmarkCodecEncodeAvro(b *testing.B)    { benchmarkCodec(b, connector.WireFormatAvro) }
func BenchmarkCodecEncodeProto(b *testing.B)   { benchmarkCodec(b, connector.WireFormatProto) }
func BenchmarkCodecEncodeJSON(b *testing.B)    { benchmarkCodec(b, connector.WireFormatJSON) }

func benchmarkCodec(b *testing.B, format connector.WireFormat) {
	codec, err := NewCodec(string(format))
	if err != nil {
		b.Fatalf("new codec: %v", err)
	}
	batch := sampleBatch(500)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.Encode(batch); err != nil {
			b.Fatalf("encode: %v", err)
		}
	}
}

func sampleBatch(rows int) connector.Batch {
	schema := connector.Schema{
		Name:      "events",
		Namespace: "public",
		Version:   1,
		Columns: []connector.Column{
			{Name: "id", Type: "int8"},
			{Name: "name", Type: "text"},
			{Name: "amount", Type: "float8"},
			{Name: "active", Type: "boolean"},
			{Name: "created_at", Type: "timestamptz"},
		},
	}

	records := make([]connector.Record, 0, rows)
	base := time.Now().UTC()
	for i := 0; i < rows; i++ {
		records = append(records, connector.Record{
			Table:         "events",
			Operation:     connector.OpInsert,
			SchemaVersion: 1,
			Key:           []byte(fmt.Sprintf("{\"id\":%d}", i)),
			After: map[string]any{
				"id":         int64(i),
				"name":       fmt.Sprintf("event-%d", i),
				"amount":     float64(i) * 1.5,
				"active":     i%2 == 0,
				"created_at": base.Add(time.Duration(i) * time.Millisecond),
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
