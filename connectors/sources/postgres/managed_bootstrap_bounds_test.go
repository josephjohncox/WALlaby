package postgres

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// The batch size reaches make([]connector.Record, 0, limit); every hostile
// value must be rejected before the nil transaction is touched or any
// allocation happens.
func TestQueryManagedSnapshotBatchRejectsHostileLimitBeforeAllocation(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{math.MinInt, -1, 0, maxManagedSnapshotBatchSize + 1, math.MaxInt32, math.MaxInt} {
		_, _, _, err := queryManagedSnapshotBatch(context.Background(), nil, bootstrap.SnapshotTask{}, nil, limit)
		if err == nil || !strings.Contains(err.Error(), "batch_size") {
			t.Fatalf("limit %d: err = %v", limit, err)
		}
	}
}

func TestValidateManagedSnapshotBatchSizeBoundaries(t *testing.T) {
	t.Parallel()
	for _, ok := range []int{1, 100, maxManagedSnapshotBatchSize} {
		if err := validateManagedSnapshotBatchSize(ok); err != nil {
			t.Fatalf("%d rejected: %v", ok, err)
		}
	}
}

func TestManagedSnapshotCursorSizeIsBoundedOnDecode(t *testing.T) {
	t.Parallel()
	task := bootstrap.SnapshotTask{KeyColumns: []string{"id"}}
	oversized := make([]byte, maxManagedSnapshotCursorBytes+1)
	if _, err := decodeManagedSnapshotCursor(task, oversized); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("oversized durable cursor decoded: %v", err)
	}
}

func TestManagedSnapshotCursorEncodeRejectsCursorThatCannotBeRead(t *testing.T) {
	t.Parallel()
	task := bootstrap.SnapshotTask{
		KeyColumns: []string{"id"},
		Schema:     connector.Schema{Columns: []connector.Column{{Name: "id", Type: "text"}}},
	}
	row := map[string]any{"id": strings.Repeat("x", maxManagedSnapshotCursorBytes)}
	if _, err := encodeManagedSnapshotCursor(task, row); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("unreadable cursor was encoded: %v", err)
	}
}
