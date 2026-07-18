package registry

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresCatalogChangeAllocatesVersions(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	suffix := time.Now().UnixNano()
	namespace := fmt.Sprintf("catalog_%d", suffix)
	name := "events"
	defer func() {
		_, _ = store.pool.Exec(ctx, "DELETE FROM schema_versions WHERE namespace = $1 AND name = $2", namespace, name)
	}()

	baseline := connector.Schema{
		Namespace: namespace,
		Name:      name,
		Version:   0,
		Columns:   []connector.Column{{Name: "id", Type: "bigint"}},
	}
	if err := store.RegisterSchema(ctx, baseline); err != nil {
		t.Fatal(err)
	}

	plan := schema.Plan{Changes: []schema.Change{{
		Type:      schema.ChangeAddColumn,
		Namespace: namespace,
		Table:     name,
		Column:    "payload",
		ToType:    "text",
	}}}
	snapshot := connector.Schema{
		Namespace: namespace,
		Name:      name,
		Version:   0, // Simulate a scanner restart with stale in-memory version state.
		Columns: []connector.Column{
			{Name: "id", Type: "bigint"},
			{Name: "payload", Type: "text"},
		},
	}

	firstEventID, err := store.RecordCatalogChange(ctx, snapshot, plan, StatusApproved)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = store.pool.Exec(ctx, "DELETE FROM ddl_events WHERE id = $1", firstEventID) }()

	snapshot.Columns = append(snapshot.Columns, connector.Column{Name: "source", Type: "text"})
	secondEventID, err := store.RecordCatalogChange(ctx, snapshot, plan, StatusPending)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = store.pool.Exec(ctx, "DELETE FROM ddl_events WHERE id = $1", secondEventID) }()

	for version, columnCount := range map[int64]int{0: 1, 1: 2, 2: 3} {
		var storedSchema connector.Schema
		if err := store.pool.QueryRow(ctx,
			"SELECT schema_json FROM schema_versions WHERE namespace = $1 AND name = $2 AND version = $3",
			namespace, name, version,
		).Scan(&storedSchema); err != nil {
			t.Fatalf("read schema version %d: %v", version, err)
		}
		if storedSchema.Version != version || len(storedSchema.Columns) != columnCount {
			t.Fatalf("stored schema version %d = %+v, want %d columns", version, storedSchema, columnCount)
		}
	}

	firstEvent, err := store.GetDDL(ctx, firstEventID)
	if err != nil {
		t.Fatal(err)
	}
	if firstEvent.Status != StatusApproved || len(firstEvent.Plan.Changes) != 1 {
		t.Fatalf("stored first DDL event = %+v", firstEvent)
	}
	secondEvent, err := store.GetDDL(ctx, secondEventID)
	if err != nil {
		t.Fatal(err)
	}
	if secondEvent.Status != StatusPending || len(secondEvent.Plan.Changes) != 1 {
		t.Fatalf("stored second DDL event = %+v", secondEvent)
	}
}

func TestPostgresCatalogChangeSerializesVersions(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	namespace := fmt.Sprintf("catalog_concurrent_%d", time.Now().UnixNano())
	name := "events"
	defer func() {
		_, _ = store.pool.Exec(ctx, "DELETE FROM schema_versions WHERE namespace = $1 AND name = $2", namespace, name)
	}()
	if err := store.RegisterSchema(ctx, connector.Schema{
		Namespace: namespace,
		Name:      name,
		Version:   0,
		Columns:   []connector.Column{{Name: "id", Type: "bigint"}},
	}); err != nil {
		t.Fatal(err)
	}

	const writers = 8
	start := make(chan struct{})
	ids := make(chan int64, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id, err := store.RecordCatalogChange(ctx, connector.Schema{
				Namespace: namespace,
				Name:      name,
				Version:   0,
				Columns: []connector.Column{
					{Name: "id", Type: "bigint"},
					{Name: fmt.Sprintf("column_%d", writer), Type: "text"},
				},
			}, schema.Plan{}, StatusPending)
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}
	close(start)
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Errorf("concurrent catalog change: %v", err)
	}
	eventIDs := make([]int64, 0, writers)
	for id := range ids {
		eventIDs = append(eventIDs, id)
	}
	defer func() {
		for _, eventID := range eventIDs {
			_, _ = store.pool.Exec(ctx, "DELETE FROM ddl_events WHERE id = $1", eventID)
		}
	}()
	if t.Failed() {
		return
	}

	rows, err := store.pool.Query(ctx,
		"SELECT version, schema_json FROM schema_versions WHERE namespace = $1 AND name = $2 ORDER BY version",
		namespace, name,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	version := int64(0)
	for rows.Next() {
		var storedVersion int64
		var storedSchema connector.Schema
		if err := rows.Scan(&storedVersion, &storedSchema); err != nil {
			t.Fatal(err)
		}
		if storedVersion != version || storedSchema.Version != version {
			t.Fatalf("schema row version=(%d,%d), want %d", storedVersion, storedSchema.Version, version)
		}
		version++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if version != writers+1 {
		t.Fatalf("schema version count=%d, want %d", version, writers+1)
	}

	var latest connector.Schema
	if err := store.pool.QueryRow(ctx,
		"SELECT schema_json FROM schema_versions WHERE namespace = $1 AND name = $2 ORDER BY version DESC LIMIT 1",
		namespace, name,
	).Scan(&latest); err != nil {
		t.Fatal(err)
	}
	duplicateIDs := make(chan int64, writers)
	duplicateErrs := make(chan error, writers)
	start = make(chan struct{})
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id, err := store.RecordCatalogChange(ctx, latest, schema.Plan{}, StatusPending)
			if err != nil {
				duplicateErrs <- err
				return
			}
			duplicateIDs <- id
		}()
	}
	close(start)
	wg.Wait()
	close(duplicateIDs)
	close(duplicateErrs)
	for err := range duplicateErrs {
		t.Errorf("duplicate catalog change: %v", err)
	}
	for id := range duplicateIDs {
		if id != 0 {
			t.Errorf("duplicate catalog change event id=%d, want 0", id)
		}
	}
	var finalCount int
	if err := store.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM schema_versions WHERE namespace = $1 AND name = $2",
		namespace, name,
	).Scan(&finalCount); err != nil {
		t.Fatal(err)
	}
	if finalCount != writers+1 {
		t.Fatalf("schema version count after duplicates=%d, want %d", finalCount, writers+1)
	}
}
