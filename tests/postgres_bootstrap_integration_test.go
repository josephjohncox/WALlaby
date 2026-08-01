package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
)

func TestLogicalSlotExportedSnapshotContract(t *testing.T) {
	ctx, dsn, engine, pool, authorityStore := setupBootstrapControl(t)
	defer engine.Close()
	defer pool.Close()
	flowID := fmt.Sprintf("bootstrap-contract-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	fence := createRunningFence(t, ctx, engine, authorityStore, flowID)
	prepareBootstrapRelation(t, ctx, pool)
	defer cleanupBootstrapRelation(ctx, pool)

	bootstrapper, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	session, err := bootstrapper.Start(ctx, fence, "wallaby_bootstrap_publication", "manifest-v1")
	if err != nil {
		t.Fatal(err)
	}
	if !session.Alive() {
		t.Fatal("snapshot exporter is not alive after Start")
	}
	var durableLSN, durableSnapshot, phase string
	if err := pool.QueryRow(ctx, `
SELECT consistent_lsn,snapshot_name,phase
FROM source_bootstraps WHERE bootstrap_id=$1`, session.Snapshot.BootstrapID).Scan(&durableLSN, &durableSnapshot, &phase); err != nil {
		t.Fatal(err)
	}
	if durableLSN != session.Snapshot.ConsistentLSN.String() || durableSnapshot != session.Snapshot.SnapshotName || phase != "snapshotting" {
		t.Fatalf("durable bootstrap=(%s,%s,%s), session=(%s,%s,snapshotting)", durableLSN, durableSnapshot, phase, session.Snapshot.ConsistentLSN, session.Snapshot.SnapshotName)
	}

	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapper.ImportSnapshot(ctx, fence, session); err == nil {
		t.Fatal("expected importer to reject a lost exporter")
	}
	if err := bootstrapper.Abandon(ctx, fence, session.Snapshot, "exporter lost"); err != nil {
		t.Fatal(err)
	}
	var slotCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1`, session.Snapshot.SlotName).Scan(&slotCount); err != nil {
		t.Fatal(err)
	}
	if slotCount != 0 {
		t.Fatalf("abandoned slot count=%d, want 0", slotCount)
	}
	replacement, err := bootstrapper.Start(ctx, fence, "wallaby_bootstrap_publication", "manifest-v1")
	if err != nil {
		t.Fatalf("restart abandoned bootstrap: %v", err)
	}
	if replacement.Snapshot.BootstrapGeneration == session.Snapshot.BootstrapGeneration || replacement.Snapshot.SlotName == session.Snapshot.SlotName {
		t.Fatal("replacement bootstrap reused the abandoned physical generation")
	}
	_ = replacement.Close(ctx)
	_, _ = pool.Exec(ctx, "SELECT pg_catalog.pg_drop_replication_slot($1)", replacement.Snapshot.SlotName)
}

func TestBootstrapConcurrentWritesBoundary(t *testing.T) {
	ctx, dsn, engine, pool, authorityStore := setupBootstrapControl(t)
	defer engine.Close()
	defer pool.Close()
	flowID := fmt.Sprintf("bootstrap-boundary-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	fence := createRunningFence(t, ctx, engine, authorityStore, flowID)
	prepareBootstrapRelation(t, ctx, pool)
	defer cleanupBootstrapRelation(ctx, pool)

	bootstrapper, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	session, err := bootstrapper.Start(ctx, fence, "wallaby_bootstrap_publication", "manifest-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = session.Close(context.Background())
		_, _ = pool.Exec(context.Background(), "SELECT pg_catalog.pg_drop_replication_slot($1)", session.Snapshot.SlotName)
	}()

	firstImporter, err := bootstrapper.ImportSnapshot(ctx, fence, session)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE public.wallaby_bootstrap_source SET value='after' WHERE id=1; INSERT INTO public.wallaby_bootstrap_source VALUES(2,'new')`); err != nil {
		t.Fatal(err)
	}
	assertBootstrapSnapshotRows(t, ctx, firstImporter)
	if err := firstImporter.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// A replacement worker imports the same live exporter snapshot and sees the
	// same cut despite writes committed after slot creation.
	secondImporter, err := bootstrapper.ImportSnapshot(ctx, fence, session)
	if err != nil {
		t.Fatal(err)
	}
	assertBootstrapSnapshotRows(t, ctx, secondImporter)
	if err := secondImporter.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var relationID uint32
	if err := pool.QueryRow(ctx, `SELECT 'public.wallaby_bootstrap_source'::regclass::oid`).Scan(&relationID); err != nil {
		t.Fatal(err)
	}
	if err := bootstrapper.RecordTaskReceipt(ctx, fence, session.Snapshot, relationID, "full-table", []byte(`{"last_id":1}`), "snapshot-rowset-v1"); err != nil {
		t.Fatal(err)
	}

	if err := bootstrapper.RecordPublication(ctx, fence, session.Snapshot, "postgres-target-v1", "snapshot-content-v1", uuid.New()); err != nil {
		t.Fatal(err)
	}
	checkpointValue, err := bootstrapper.Handoff(ctx, fence, session.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if checkpointValue.LSN != session.Snapshot.ConsistentLSN.String() {
		t.Fatalf("handoff checkpoint=%s, want consistent point %s", checkpointValue.LSN, session.Snapshot.ConsistentLSN)
	}
	var phase string
	if err := pool.QueryRow(ctx, `SELECT phase FROM source_bootstraps WHERE bootstrap_id=$1`, session.Snapshot.BootstrapID).Scan(&phase); err != nil {
		t.Fatal(err)
	}
	if phase != "streaming" {
		t.Fatalf("bootstrap phase=%q, want streaming", phase)
	}
}

func TestBootstrapRecoveryFailpoints(t *testing.T) {
	ctx, dsn, engine, pool, authorityStore := setupBootstrapControl(t)
	defer engine.Close()
	defer pool.Close()
	flowID := fmt.Sprintf("bootstrap-failpoint-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	fence := createRunningFence(t, ctx, engine, authorityStore, flowID)
	prepareBootstrapRelation(t, ctx, pool)
	defer cleanupBootstrapRelation(ctx, pool)

	injected := errors.New("injected after slot creation")
	var created bootstrap.ExportedSnapshot
	bootstrapper, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{
		AfterSlotCreated: func(_ context.Context, snapshot bootstrap.ExportedSnapshot) error {
			created = snapshot
			return injected
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapper.Start(ctx, fence, "wallaby_bootstrap_publication", "manifest-v1"); !errors.Is(err, injected) {
		t.Fatalf("Start error=%v, want injected failure", err)
	}
	var bootstrapCount, slotCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_bootstraps WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&bootstrapCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1`, created.SlotName).Scan(&slotCount); err != nil {
		t.Fatal(err)
	}
	if bootstrapCount != 0 || slotCount != 0 {
		t.Fatalf("after slot failpoint bootstrap rows=%d slots=%d, want 0/0", bootstrapCount, slotCount)
	}
}

func setupBootstrapControl(t *testing.T) (context.Context, string, *workflow.PostgresEngine, *pgxpool.Pool, *authority.PostgresStore) {
	t.Helper()
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		engine.Close()
		t.Fatal(err)
	}
	store, err := authority.NewPostgresStore(pool)
	if err != nil {
		engine.Close()
		pool.Close()
		t.Fatal(err)
	}
	checkpointStore, err := checkpoint.NewPostgresStore(ctx, dsn)
	if err != nil {
		engine.Close()
		pool.Close()
		t.Fatal(err)
	}
	checkpointStore.Close()
	if _, err := delivery.NewCoordinator(ctx, pool); err != nil {
		engine.Close()
		pool.Close()
		t.Fatal(err)
	}
	return ctx, dsn, engine, pool, store
}

func createRunningFence(t *testing.T, ctx context.Context, engine *workflow.PostgresEngine, store *authority.PostgresStore, flowID string) authority.RunFence {
	t.Helper()
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireProducer(ctx, flowID, "bootstrap-worker", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return fence
}

func prepareBootstrapRelation(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
DROP PUBLICATION IF EXISTS wallaby_bootstrap_publication;
DROP TABLE IF EXISTS public.wallaby_bootstrap_source;
CREATE TABLE public.wallaby_bootstrap_source (id bigint PRIMARY KEY, value text);
INSERT INTO public.wallaby_bootstrap_source VALUES (1,'before');
CREATE PUBLICATION wallaby_bootstrap_publication FOR TABLE public.wallaby_bootstrap_source`); err != nil {
		t.Fatal(err)
	}
}

func cleanupBootstrapRelation(ctx context.Context, pool *pgxpool.Pool) {
	_, _ = pool.Exec(ctx, `DROP PUBLICATION IF EXISTS wallaby_bootstrap_publication; DROP TABLE IF EXISTS public.wallaby_bootstrap_source`)
}

func assertBootstrapSnapshotRows(t *testing.T, ctx context.Context, tx interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) {
	t.Helper()
	var count int
	var value string
	if err := tx.QueryRow(ctx, `SELECT count(*),min(value) FROM public.wallaby_bootstrap_source`).Scan(&count, &value); err != nil {
		t.Fatal(err)
	}
	if count != 1 || value != "before" {
		t.Fatalf("imported snapshot rows=(%d,%q), want (1,before)", count, value)
	}
}
