package wire

import (
	"bytes"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestArrowBackedCodecsSortTypeMetadata(t *testing.T) {
	t.Parallel()

	for _, format := range []string{"arrow", "parquet"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			codec, err := NewCodec(format)
			if err != nil {
				t.Fatal(err)
			}
			first := deterministicMetadataBatch(map[string]string{
				"extension":  "vector",
				"dimensions": "3",
				"oid":        "12345",
			})
			second := deterministicMetadataBatch(map[string]string{
				"oid":        "12345",
				"dimensions": "3",
				"extension":  "vector",
			})

			want, err := codec.Encode(first)
			if err != nil {
				t.Fatal(err)
			}
			for iteration := 0; iteration < 20; iteration++ {
				got, err := codec.Encode(second)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("%s encoding changed with metadata map order on iteration %d", format, iteration)
				}
			}
		})
	}
}

func deterministicMetadataBatch(metadata map[string]string) connector.Batch {
	return connector.Batch{
		Schema: connector.Schema{
			Name:      "embeddings",
			Namespace: "public",
			Version:   1,
			Columns: []connector.Column{{
				Name:         "embedding",
				Type:         "vector",
				TypeMetadata: metadata,
			}},
		},
		Records: []connector.Record{{
			Table:         "embeddings",
			Operation:     connector.OpInsert,
			SchemaVersion: 1,
			After:         map[string]any{"embedding": []byte("[1,2,3]")},
			Timestamp:     time.Unix(100, 0).UTC(),
		}},
		Checkpoint: connector.Checkpoint{LSN: "0/10"},
	}
}
