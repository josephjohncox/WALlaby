package checkpoint

import (
	"math"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestHashOutboxBatchIsDeterministicAndTypeSensitive(t *testing.T) {
	t.Parallel()
	base := connector.Batch{
		Checkpoint: connector.Checkpoint{LSN: "000a/000ff", Timestamp: time.Unix(10, 20), Metadata: map[string]string{"b": "2", "a": "1"}},
		Records: []connector.Record{{After: map[string]any{
			"id": int64(1), "nan": math.Float64frombits(0x7ff8000000000001),
		}}},
	}
	reordered := base
	reordered.Checkpoint.Timestamp = time.Unix(99, 0)
	reordered.Checkpoint.Metadata = map[string]string{"a": "1", "b": "2"}
	reordered.Records = []connector.Record{{After: map[string]any{
		"nan": math.Float64frombits(0x7ff8000000000001), "id": int64(1),
	}}}

	first, err := hashOutboxBatch(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := hashOutboxBatch(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("equivalent batch hashes differ: %s != %s", first, second)
	}

	differentType := reordered
	differentType.Records = []connector.Record{{After: map[string]any{
		"nan": math.Float64frombits(0x7ff8000000000001), "id": float64(1),
	}}}
	third, err := hashOutboxBatch(differentType)
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatalf("int64 and float64 batches shared hash %s", first)
	}
}
