package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Requires TEST_PG_DSN (a disposable PostgreSQL); skipped otherwise.
func TestPostgresSnapshotStateStoreRoundTripsEmptyCursorAsNull(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := fmt.Sprintf("wallaby_state_test_%d", time.Now().UnixNano())
	store, err := newPostgresSnapshotStateStore(ctx, dsn, schema, "snapshot_state", nil)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()
	t.Cleanup(func() {
		pool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			return
		}
		defer pool.Close()
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})

	done := snapshotTaskState{FlowID: "flow-1", Table: "public.widgets", PartitionCount: 1, Status: snapshotStatusDone}
	if err := store.Upsert(ctx, done); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	loaded, err := store.Load(ctx, "flow-1")
	if err != nil {
		t.Fatalf("load with NULL cursor: %v", err)
	}
	got, ok := loaded[snapshotTaskKey{Table: "public.widgets", PartitionCount: 1}]
	if !ok || got.Cursor != "" || got.Status != snapshotStatusDone {
		t.Fatalf("loaded = %#v", loaded)
	}

	// A durable row that no backfill task can produce must fail the load.
	tableIdent := pgx.Identifier{schema, "snapshot_state"}.Sanitize()
	if _, err := store.pool.Exec(ctx, "INSERT INTO "+tableIdent+" (flow_id, table_name, partition_index, partition_count, status) VALUES ('flow-2', 'public.t', 5, 2, 'running')"); err != nil {
		t.Fatalf("insert hostile row: %v", err)
	}
	if _, err := store.Load(ctx, "flow-2"); err == nil {
		t.Fatal("hostile durable row accepted")
	}
}
