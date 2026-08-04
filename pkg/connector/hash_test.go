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

func TestBatchContentHashCurrentContract(t *testing.T) {
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
		t.Fatalf("BatchContentHash() = %s, want current hash %s", got, want)
	}

	batch.WritePolicy.KeyColumns = []string{}
	emptyPolicy, err := BatchContentHash(batch)
	if err != nil {
		t.Fatal(err)
	}
	if emptyPolicy != got {
		t.Fatalf("semantic zero policy changed frozen hash: %s != %s", emptyPolicy, got)
	}

	batch.WritePolicy = TableWritePolicy{Mode: ResolvedWriteUpsert, KeyColumns: []string{"id"}, ProjectionFingerprint: "projection-v1"}
	withPolicy, err := BatchContentHash(batch)
	if err != nil {
		t.Fatal(err)
	}
	if withPolicy == got {
		t.Fatal("resolved write policy and projection fingerprint did not change batch identity")
	}
}

func TestProjectedBatchContentHashIsReplayStable(t *testing.T) {
	t.Parallel()
	batch := Batch{
		Schema:      Schema{Name: "events", Namespace: "public", Version: 7, Columns: []Column{{Name: "id", Type: "int8"}}},
		Records:     []Record{{Table: "events", Operation: OpInsert, SchemaVersion: 7, After: map[string]any{"id": int64(1)}, Timestamp: time.Unix(10, 20).UTC()}},
		Checkpoint:  Checkpoint{LSN: "0/20", Timestamp: time.Unix(30, 40).UTC(), Metadata: map[string]string{"recovery_attempt": "one"}},
		WritePolicy: TableWritePolicy{Mode: ResolvedWriteUpsert, KeyColumns: []string{"id"}, ProjectionFingerprint: "mapping-v1"},
	}
	first, err := BatchContentHash(batch)
	if err != nil {
		t.Fatal(err)
	}
	replayed := batch
	replayed.Schema.Version = 999
	replayed.Records = append([]Record(nil), batch.Records...)
	replayed.Records[0].SchemaVersion = 999
	replayed.Records[0].Timestamp = time.Unix(999, 0).UTC()
	replayed.Checkpoint.Timestamp = time.Unix(1000, 0).UTC()
	replayed.Checkpoint.Metadata = map[string]string{"recovery_attempt": "two", "artifact_publication_id": "runtime"}
	second, err := BatchContentHash(replayed)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("projected replay hash changed: %s != %s", first, second)
	}
	replayed.Records[0].After = map[string]any{"id": int64(2)}
	changed, err := BatchContentHash(replayed)
	if err != nil {
		t.Fatal(err)
	}
	if changed == first {
		t.Fatal("logical projected record change did not change hash")
	}
	appendBatch := batch
	appendBatch.WritePolicy = TableWritePolicy{Mode: ResolvedWriteAppend, ProjectionFingerprint: "mapping-v1"}
	appendHash, err := BatchContentHash(appendBatch)
	if err != nil {
		t.Fatal(err)
	}
	appendBatch.WritePolicy.KeyColumns = []string{}
	emptyKeyHash, err := BatchContentHash(appendBatch)
	if err != nil {
		t.Fatal(err)
	}
	if appendHash != emptyKeyHash {
		t.Fatalf("nil/empty projected key columns changed hash: %s != %s", appendHash, emptyKeyHash)
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
