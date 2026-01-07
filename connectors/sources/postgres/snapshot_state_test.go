package postgres

import (
	"context"
	"path/filepath"
	"testing"
)

func TestFileSnapshotStateStore_RoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "snapshot_state.json")
	store, err := newFileSnapshotStateStore(path)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	state := snapshotTaskState{
		FlowID:         "flow-1",
		Table:          "public.widgets",
		PartitionIndex: 0,
		PartitionCount: 2,
		Cursor:         "42",
		Status:         snapshotStatusRunning,
	}
	if err := store.Upsert(ctx, state); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	loaded, err := store.Load(ctx, "flow-1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	key := snapshotTaskKey{Table: "public.widgets", PartitionIndex: 0, PartitionCount: 2}
	got, ok := loaded[key]
	if !ok {
		t.Fatalf("expected state for %v", key)
	}
	if got.Cursor != "42" || got.Status != snapshotStatusRunning {
		t.Fatalf("unexpected state: %#v", got)
	}

	// Reload from disk to ensure persistence.
	reloaded, err := newFileSnapshotStateStore(path)
	if err != nil {
		t.Fatalf("reload store: %v", err)
	}
	loaded, err = reloaded.Load(ctx, "flow-1")
	if err != nil {
		t.Fatalf("reload load: %v", err)
	}
	if _, ok := loaded[key]; !ok {
		t.Fatalf("expected state after reload")
	}
}
