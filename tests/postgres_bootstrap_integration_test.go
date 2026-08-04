package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/controlstore"
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

func TestManagedTerminalStopHardCrashSlotBeforePersist(t *testing.T) {
	if os.Getenv("WALLABY_TEST_BOOTSTRAP_SIGKILL_HELPER") == "1" {
		runBootstrapSIGKILLHelper(t)
		return
	}

	ctx, dsn, engine, pool, authorityStore := setupBootstrapControl(t)
	defer engine.Close()
	defer pool.Close()
	flowID := fmt.Sprintf("bootstrap-sigkill-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	fence := createRunningFence(t, ctx, engine, authorityStore, flowID)

	encodedFence, err := json.Marshal(fence)
	if err != nil {
		t.Fatal(err)
	}
	snapshotFile := t.TempDir() + "/created-snapshot.json"
	command := exec.Command(os.Args[0], "-test.run=^TestManagedTerminalStopHardCrashSlotBeforePersist$", "-test.v")
	command.Env = append(os.Environ(),
		"WALLABY_TEST_BOOTSTRAP_SIGKILL_HELPER=1",
		"WALLABY_TEST_BOOTSTRAP_DSN="+dsn,
		"WALLABY_TEST_BOOTSTRAP_FENCE="+string(encodedFence),
		"WALLABY_TEST_BOOTSTRAP_SNAPSHOT_FILE="+snapshotFile,
	)
	if err := command.Run(); err == nil {
		t.Fatal("SIGKILL helper unexpectedly returned successfully")
	} else if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 86 {
		t.Fatalf("SIGKILL helper error=%v, want deterministic exit 86", err)
	}
	encodedSnapshot, err := os.ReadFile(snapshotFile)
	if err != nil {
		t.Fatal(err)
	}
	var orphan bootstrap.ExportedSnapshot
	if err := json.Unmarshal(encodedSnapshot, &orphan); err != nil {
		t.Fatal(err)
	}
	var slotExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)`, orphan.SlotName).Scan(&slotExists); err != nil {
		t.Fatal(err)
	}
	if !slotExists {
		t.Fatalf("hard-killed helper did not leave prepared physical slot %q", orphan.SlotName)
	}
	var prepared int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resource_operations WHERE flow_incarnation_id=$1 AND resource_kind='slot' AND operation='create' AND physical_name=$2 AND status='prepared'`, fence.FlowIncarnationID, orphan.SlotName).Scan(&prepared); err != nil {
		t.Fatal(err)
	}
	if prepared != 1 {
		t.Fatalf("prepared orphan operations=%d, want 1", prepared)
	}

	if err := authorityStore.FinishProducer(ctx, fence, "hard_crash_observed"); err != nil {
		t.Fatal(err)
	}
	_, stopControl, err := engine.RequestStop(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	cleanupFence, err := authorityStore.AcquireCleanupFence(ctx, flowID, stopControl.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	terminalCoordinator, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if err := terminalCoordinator.CleanupOwnedResources(ctx, cleanupFence); err != nil {
		t.Fatalf("terminal cleanup did not reconcile hard-killed slot: %v", err)
	}
	if err := authorityStore.FinishCleanup(ctx, cleanupFence, "test_cleanup_complete"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.CompleteStopGeneration(ctx, flowID, stopControl.Generation); err != nil {
		t.Fatal(err)
	}
	var orphanExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)`, orphan.SlotName).Scan(&orphanExists); err != nil {
		t.Fatal(err)
	}
	var rejected int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resource_operations WHERE flow_incarnation_id=$1 AND physical_name=$2 AND status='rejected' AND external_evidence->>'resource_absent'='true'`, fence.FlowIncarnationID, orphan.SlotName).Scan(&rejected); err != nil {
		t.Fatal(err)
	}
	if orphanExists || rejected != 1 {
		t.Fatalf("terminal orphan reconciliation slot_exists=%t rejected_receipts=%d", orphanExists, rejected)
	}
}

func runBootstrapSIGKILLHelper(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_BOOTSTRAP_DSN")
	var fence authority.RunFence
	if err := json.Unmarshal([]byte(os.Getenv("WALLABY_TEST_BOOTSTRAP_FENCE")), &fence); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	controlstore.ConfigurePool(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	coordinator, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{
		AfterSlotCreated: func(_ context.Context, snapshot bootstrap.ExportedSnapshot) error {
			encoded, marshalErr := json.Marshal(snapshot)
			if marshalErr != nil {
				return marshalErr
			}
			if writeErr := os.WriteFile(os.Getenv("WALLABY_TEST_BOOTSTRAP_SNAPSHOT_FILE"), encoded, 0o600); writeErr != nil {
				return writeErr
			}
			os.Exit(86) // bypass every defer after the external slot side effect
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = coordinator.Start(ctx, fence, "wallaby_bootstrap_publication", "manifest-v1")
	t.Fatal("bootstrap helper reached code after the hard-kill boundary")
}

func TestManagedTerminalStopHardCrashPublicationBeforePublish(t *testing.T) {
	if os.Getenv("WALLABY_TEST_PUBLICATION_SIGKILL_HELPER") == "1" {
		runPublicationSIGKILLHelper(t)
		return
	}

	ctx, dsn, engine, pool, authorityStore := setupBootstrapControl(t)
	defer engine.Close()
	defer pool.Close()
	flowID := fmt.Sprintf("publication-stop-crash-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	fence := createRunningFence(t, ctx, engine, authorityStore, flowID)
	prepareBootstrapRelation(t, ctx, pool)
	defer cleanupBootstrapRelation(context.Background(), pool)
	publication := fmt.Sprintf("wallaby_stop_crash_pub_%d", time.Now().UnixNano())
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP PUBLICATION IF EXISTS `+pgx.Identifier{publication}.Sanitize())
	}()

	encodedFence, err := json.Marshal(fence)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestManagedTerminalStopHardCrashPublicationBeforePublish$", "-test.v")
	command.Env = append(os.Environ(),
		"WALLABY_TEST_PUBLICATION_SIGKILL_HELPER=1",
		"WALLABY_TEST_BOOTSTRAP_DSN="+dsn,
		"WALLABY_TEST_BOOTSTRAP_FENCE="+string(encodedFence),
		"WALLABY_TEST_PUBLICATION_NAME="+publication,
	)
	if err := command.Run(); err == nil {
		t.Fatal("publication SIGKILL helper unexpectedly returned successfully")
	} else if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 87 {
		t.Fatalf("publication SIGKILL helper error=%v, want deterministic exit 87", err)
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_publication WHERE pubname=$1)`, publication).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("hard-killed helper did not leave the publication side effect")
	}
	var prepared int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resource_operations WHERE flow_incarnation_id=$1 AND resource_kind='publication' AND operation='create' AND physical_name=$2 AND status='prepared'`, fence.FlowIncarnationID, publication).Scan(&prepared); err != nil {
		t.Fatal(err)
	}
	if prepared != 1 {
		t.Fatalf("prepared publication operations=%d, want 1", prepared)
	}
	// Exercise terminal operation-only reconciliation as well as the normal
	// prepared resource path. The immutable create journal remains exact
	// ownership authority for the external publication.
	tag, err := pool.Exec(ctx, `
DELETE FROM source_resources resource
WHERE flow_incarnation_id=$1 AND resource_kind='publication' AND physical_name=$2
  AND state='prepared'
  AND EXISTS (
    SELECT 1 FROM source_resource_operations operation
    WHERE operation.flow_incarnation_id=resource.flow_incarnation_id
      AND operation.resource_kind=resource.resource_kind
      AND operation.resource_id=resource.resource_id
      AND operation.operation='create' AND operation.status='prepared'
  )`, fence.FlowIncarnationID, publication)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("removed prepared publication resource rows=%d, want operation-only journal", tag.RowsAffected())
	}

	if err := authorityStore.FinishProducer(ctx, fence, "hard_crash_observed"); err != nil {
		t.Fatal(err)
	}
	_, stopControl, err := engine.RequestStop(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	cleanupFence, err := authorityStore.AcquireCleanupFence(ctx, flowID, stopControl.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	terminalCoordinator, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if err := terminalCoordinator.CleanupOwnedResources(ctx, cleanupFence); err != nil {
		t.Fatalf("terminal cleanup did not reconcile hard-killed publication: %v", err)
	}
	if err := authorityStore.FinishCleanup(ctx, cleanupFence, "test_cleanup_complete"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.CompleteStopGeneration(ctx, flowID, stopControl.Generation); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_publication WHERE pubname=$1)`, publication).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	var closed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resource_operations WHERE flow_incarnation_id=$1 AND physical_name=$2 AND operation='create' AND status='rejected' AND external_evidence->>'resource_absent'='true'`, fence.FlowIncarnationID, publication).Scan(&closed); err != nil {
		t.Fatal(err)
	}
	if exists || closed != 1 {
		t.Fatalf("terminal publication reconciliation exists=%t closed_operations=%d", exists, closed)
	}
}

func runPublicationSIGKILLHelper(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_BOOTSTRAP_DSN")
	publication := os.Getenv("WALLABY_TEST_PUBLICATION_NAME")
	var fence authority.RunFence
	if err := json.Unmarshal([]byte(os.Getenv("WALLABY_TEST_BOOTSTRAP_FENCE")), &fence); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	controlstore.ConfigurePool(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var sourceSystem, databaseName string
	if err := pool.QueryRow(ctx, `SELECT system_identifier::text,current_database() FROM pg_catalog.pg_control_system()`).Scan(&sourceSystem, &databaseName); err != nil {
		t.Fatal(err)
	}
	var oid uint32
	if err := pool.QueryRow(ctx, `SELECT 'public.wallaby_bootstrap_source'::regclass::oid`).Scan(&oid); err != nil {
		t.Fatal(err)
	}
	relations := []bootstrap.PublicationRelation{{OID: oid, Namespace: "public", Table: "wallaby_bootstrap_source", RelationKind: "r"}}
	revision := bootstrap.ExpectedPublicationRevision(publication, relations)
	coordinator, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{
		AfterPublicationCreated: func(context.Context, string) error {
			os.Exit(87) // bypass every defer after the external publication side effect
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = coordinator.EnsurePublication(ctx, fence, bootstrap.ExportedSnapshot{SourceSystem: sourceSystem, DatabaseName: databaseName}, publication, revision, relations, true)
	t.Fatal("publication helper reached code after the hard-kill boundary")
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
	dropInjected := errors.New("injected immediate orphan slot drop failure")
	var created bootstrap.ExportedSnapshot
	bootstrapper, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{
		AfterSlotCreated: func(_ context.Context, snapshot bootstrap.ExportedSnapshot) error {
			created = snapshot
			return injected
		},
		DropSlot: func(context.Context, string) error { return dropInjected },
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
	if bootstrapCount != 0 || slotCount != 1 {
		t.Fatalf("after immediate drop failure bootstrap rows=%d slots=%d, want 0/1", bootstrapCount, slotCount)
	}
	var indeterminate int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resource_operations WHERE flow_incarnation_id=$1 AND physical_name=$2 AND operation='create' AND status='indeterminate'`, fence.FlowIncarnationID, created.SlotName).Scan(&indeterminate); err != nil {
		t.Fatal(err)
	}
	if indeterminate != 1 {
		t.Fatalf("discoverable indeterminate orphan operations=%d, want 1", indeterminate)
	}
	replacementCoordinator, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := replacementCoordinator.Start(ctx, fence, "wallaby_bootstrap_publication", "manifest-v1")
	if err != nil {
		t.Fatalf("replacement did not reconcile indeterminate orphan: %v", err)
	}
	defer func() {
		_ = replacement.Close(context.Background())
		_, _ = pool.Exec(context.Background(), `SELECT pg_catalog.pg_drop_replication_slot($1)`, replacement.Snapshot.SlotName)
	}()
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1`, created.SlotName).Scan(&slotCount); err != nil {
		t.Fatal(err)
	}
	var rejected int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resource_operations WHERE flow_incarnation_id=$1 AND physical_name=$2 AND status='rejected'`, fence.FlowIncarnationID, created.SlotName).Scan(&rejected); err != nil {
		t.Fatal(err)
	}
	if slotCount != 0 || rejected != 1 {
		t.Fatalf("replacement orphan reconciliation slots=%d rejected=%d, want 0/1", slotCount, rejected)
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
	if _, err := engine.Create(ctx, currentTestFlow(flow.Flow{ID: flowID})); err != nil {
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

func cleanupBootstrapSlotsForFlow(t *testing.T, pool *pgxpool.Pool, flowID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := pool.Query(ctx, `
SELECT slot_name
FROM source_bootstraps
WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID)
	if err != nil {
		t.Errorf("list bootstrap slots for cleanup: %v", err)
		return
	}
	var slotNames []string
	for rows.Next() {
		var slotName string
		if err := rows.Scan(&slotName); err != nil {
			rows.Close()
			t.Errorf("scan bootstrap slot for cleanup: %v", err)
			return
		}
		slotNames = append(slotNames, slotName)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Errorf("list bootstrap slots for cleanup: %v", err)
		return
	}
	for _, slotName := range slotNames {
		for {
			var active bool
			err := pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&active)
			if errors.Is(err, pgx.ErrNoRows) {
				break
			}
			if err != nil {
				t.Errorf("inspect bootstrap slot %s for cleanup: %v", slotName, err)
				break
			}
			if !active {
				if _, err := pool.Exec(ctx, `SELECT pg_catalog.pg_drop_replication_slot($1)`, slotName); err != nil {
					t.Errorf("drop bootstrap slot %s during cleanup: %v", slotName, err)
				}
				break
			}
			select {
			case <-ctx.Done():
				t.Errorf("bootstrap slot %s remained active during cleanup: %v", slotName, ctx.Err())
				return
			case <-time.After(25 * time.Millisecond):
			}
		}
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
