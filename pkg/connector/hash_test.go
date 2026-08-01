package connector

import (
	"math"
	"strings"
	"testing"
	"time"
)

type hashValueWithPrivateTime struct {
	observedAt time.Time
}

func TestBatchContentHashCompatibility(t *testing.T) {
	t.Parallel()

	batch := Batch{
		Schema: Schema{
			Name:      "events",
			Namespace: "public",
			Version:   4,
			Columns: []Column{
				{Name: "id", Type: "int8"},
				{Name: "name", Type: "text"},
			},
		},
		Records: []Record{{
			Table:         "events",
			Operation:     OpUpdate,
			SchemaVersion: 4,
			Key:           []byte("key-1"),
			After: map[string]any{
				"id":   int64(1),
				"name": "alpha",
				"nan":  math.Float64frombits(0x7ff8000000000001),
			},
			Timestamp: time.Unix(100, 200).UTC(),
		}},
		Checkpoint: Checkpoint{
			LSN:       "0/10",
			Timestamp: time.Unix(999, 0).UTC(),
			Metadata:  map[string]string{"txid": "42", "source": "primary"},
		},
		WireFormat: WireFormatJSON,
	}

	got, err := BatchContentHash(batch)
	if err != nil {
		t.Fatal(err)
	}
	const want = "3c59c3fc46892f238671201a5c3d161fb8fc5b7dbfb13923b2dd80e0672da1f0"
	if got != want {
		t.Fatalf("BatchContentHash() = %s, want compatibility hash %s", got, want)
	}
}

func TestBatchContentHashRejectsInaccessibleTimeWithoutPanicking(t *testing.T) {
	t.Parallel()

	batch := Batch{
		Schema: Schema{Name: "events", Version: 1},
		Records: []Record{{
			Table:         "events",
			Operation:     OpInsert,
			SchemaVersion: 1,
			After: map[string]any{
				"value": hashValueWithPrivateTime{observedAt: time.Unix(1, 0).UTC()},
			},
		}},
		Checkpoint: Checkpoint{LSN: "0/10"},
	}

	_, err := BatchContentHash(batch)
	if err == nil || !strings.Contains(err.Error(), "inaccessible time value") {
		t.Fatalf("BatchContentHash() error = %v, want inaccessible time value", err)
	}
}
