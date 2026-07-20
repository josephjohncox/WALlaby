package registry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresDDLExecutionAdvisoryLockSerializesOwners(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx := context.Background()
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	store, err := NewPostgresStore(ctx, dsn+separator+"pool_max_conns=1")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	acquired := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- store.WithDDLExecutionLock(ctx, "flow", "destination", func() error {
			close(acquired)
			<-release
			return nil
		})
	}()
	select {
	case <-acquired:
	case err := <-firstDone:
		t.Fatalf("first lock owner failed before entering: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	secondEntered := false
	err = store.WithDDLExecutionLock(waitCtx, "flow", "destination", func() error {
		secondEntered = true
		return nil
	})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contending lock error=%v, want context deadline", err)
	}
	if secondEntered {
		t.Fatal("contending DDL execution entered before the first owner released")
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := store.WithDDLExecutionLock(ctx, "flow", "destination", func() error {
		secondEntered = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !secondEntered {
		t.Fatal("next DDL execution owner did not enter after release")
	}
}

func TestPostgresDDLExecutionReceiptsGateAppliedStatus(t *testing.T) {
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

	flowID := fmt.Sprintf("receipt-flow-%d", time.Now().UnixNano())
	lsn := fmt.Sprintf("receipt-lsn-%d", time.Now().UnixNano())
	ddl := "ALTER TABLE events ADD COLUMN receipt_test text"
	eventID, err := store.RecordDDL(ctx, flowID, ddl, schema.Plan{}, lsn, StatusApproved)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = store.pool.Exec(ctx, "DELETE FROM ddl_events WHERE id = $1", eventID) }()

	if err := store.SetDDLStatus(ctx, eventID, StatusApplied); !errors.Is(err, ErrExecutionReceiptRequired) {
		t.Fatalf("SetDDLStatus(applied) error=%v, want execution receipt requirement", err)
	}
	expected := []string{"destination-a", "destination-b"}
	if err := store.RecordDDLExecution(ctx, flowID, lsn, ddl, "destination-a", expected); !errors.Is(err, ErrDDLExecutionNotPrepared) {
		t.Fatalf("unprepared receipt error=%v, want preparation requirement", err)
	}
	state, err := store.PrepareDDLExecution(ctx, flowID, lsn, "destination-a", expected)
	if err != nil || state != connector.DDLExecutionNew {
		t.Fatalf("initial execution state=%v error=%v, want new", state, err)
	}
	retryState, err := store.PrepareDDLExecution(ctx, flowID, lsn, "destination-a", expected)
	if err != nil || retryState != connector.DDLExecutionRetry {
		t.Fatalf("repeated execution state=%v error=%v, want retry", retryState, err)
	}
	if err := store.SetDDLStatus(ctx, eventID, StatusRejected); !errors.Is(err, ErrDDLExecutionStarted) {
		t.Fatalf("SetDDLStatus(rejected) after execution start error=%v, want immutable execution status", err)
	}
	if err := store.RecordDDLExecution(ctx, flowID, lsn, ddl, "destination-a", expected); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareDDLExecution(ctx, flowID, lsn, "destination-a", []string{"destination-a"}); !errors.Is(err, ErrExecutionManifestChanged) {
		t.Fatalf("changed manifest preflight error=%v, want immutable manifest failure", err)
	}
	event, err := store.GetDDL(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	if event.Status != StatusApproved {
		t.Fatalf("status after partial receipts=%s, want approved", event.Status)
	}
	state, err = store.PrepareDDLExecution(ctx, flowID, lsn, "destination-b", expected)
	if err != nil || state != connector.DDLExecutionNew {
		t.Fatalf("second destination state=%v error=%v, want new", state, err)
	}
	if err := store.RecordDDLExecution(ctx, flowID, lsn, ddl, "destination-b", expected); err != nil {
		t.Fatal(err)
	}
	event, err = store.GetDDL(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	if event.Status != StatusApplied || event.AppliedAt.IsZero() {
		t.Fatalf("completed receipt event=%+v, want applied with timestamp", event)
	}
	for _, destination := range expected {
		state, err := store.PrepareDDLExecution(ctx, flowID, lsn, destination, expected)
		if err != nil || state != connector.DDLExecutionComplete {
			t.Fatalf("receipt %s state=%v error=%v, want complete", destination, state, err)
		}
	}
	reopened, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if state, err := reopened.PrepareDDLExecution(ctx, flowID, lsn, "destination-a", expected); err != nil || state != connector.DDLExecutionComplete {
		t.Fatalf("reopened receipt state=%v error=%v, want complete", state, err)
	}
	if err := store.RecordDDLExecution(ctx, flowID, lsn, ddl, "destination-a", expected); err != nil {
		t.Fatalf("idempotent receipt replay: %v", err)
	}
	if err := store.RecordDDLExecution(ctx, flowID, lsn, ddl, "destination-a", []string{"destination-a"}); err == nil {
		t.Fatal("changed execution manifest accepted")
	}
	for _, administrativeStatus := range []string{StatusApproved, StatusRejected} {
		if err := store.SetDDLStatus(ctx, eventID, administrativeStatus); !errors.Is(err, ErrAppliedStatusImmutable) {
			t.Fatalf("SetDDLStatus(%s) error=%v, want immutable applied status", administrativeStatus, err)
		}
	}
	event, err = store.GetDDL(ctx, eventID)
	if err != nil {
		t.Fatal(err)
	}
	if event.Status != StatusApplied || event.AppliedAt.IsZero() {
		t.Fatalf("administrative transition changed receipt-backed event: %+v", event)
	}

	legacyLSN := lsn + "-legacy"
	if _, err := store.RecordDDL(ctx, flowID, ddl, schema.Plan{}, legacyLSN, StatusApplied); !errors.Is(err, ErrExecutionReceiptRequired) {
		t.Fatalf("RecordDDL(applied) error=%v, want execution receipt requirement", err)
	}
	var legacyID int64
	if err := store.pool.QueryRow(ctx,
		`INSERT INTO ddl_events (flow_id, ddl, plan_json, lsn, status, applied_at)
		 VALUES ($1, $2, '{}'::jsonb, $3, $4, now()) RETURNING id`,
		flowID, ddl, legacyLSN, StatusApplied,
	).Scan(&legacyID); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = store.pool.Exec(ctx, "DELETE FROM ddl_events WHERE id = $1", legacyID) }()
	if _, err := store.PrepareDDLExecution(ctx, flowID, legacyLSN, "destination-a", expected); !errors.Is(err, ErrAppliedReceiptMissing) {
		t.Fatalf("legacy applied receipt check error=%v, want fail-closed missing receipt", err)
	}
}

func TestPostgresDDLPreparationSerializesStatus(t *testing.T) {
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

	flowID := fmt.Sprintf("receipt-race-flow-%d", time.Now().UnixNano())
	const attempts = 16
	for attempt := range attempts {
		lsn := fmt.Sprintf("receipt-race-lsn-%d-%d", time.Now().UnixNano(), attempt)
		eventID, err := store.RecordDDL(ctx, flowID, "ALTER TABLE events ADD COLUMN race_test text", schema.Plan{}, lsn, StatusApproved)
		if err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		prepareResult := make(chan error, 1)
		statusResult := make(chan error, 1)
		go func() {
			<-start
			_, prepareErr := store.PrepareDDLExecution(ctx, flowID, lsn, "destination", []string{"destination"})
			prepareResult <- prepareErr
		}()
		go func() {
			<-start
			statusResult <- store.SetDDLStatus(ctx, eventID, StatusRejected)
		}()
		close(start)
		prepareErr := <-prepareResult
		statusErr := <-statusResult
		_, isDDLGate := connector.AsDDLGate(prepareErr)

		switch {
		case prepareErr == nil && errors.Is(statusErr, ErrDDLExecutionStarted):
		case statusErr == nil && isDDLGate:
		default:
			t.Fatalf("attempt %d produced non-serialized results: prepare=%v status=%v", attempt, prepareErr, statusErr)
		}
		if _, err := store.pool.Exec(ctx, "DELETE FROM ddl_events WHERE id = $1", eventID); err != nil {
			t.Fatal(err)
		}
	}
}

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
