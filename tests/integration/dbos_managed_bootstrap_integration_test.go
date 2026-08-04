package integration_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/orchestrator"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDBOSManagedBootstrapProductionWiring(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_DBOS_DSN"))
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	}
	if dsn == "" {
		t.Skip("WALLABY_TEST_DBOS_DSN or TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	control, err := controlstore.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open control store: %v", err)
	}
	defer control.Close()
	pool := control.Pool()
	if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("apply control migrations: %v", err)
	}
	var walLevel string
	if err := pool.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		t.Fatalf("read wal_level: %v", err)
	}
	if walLevel != "logical" {
		t.Skipf("wal_level must be logical (got %s)", walLevel)
	}
	var sourceSystemIdentifier string
	if err := pool.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&sourceSystemIdentifier); err != nil {
		t.Fatalf("read PostgreSQL system identifier: %v", err)
	}

	suffix := time.Now().UnixNano()
	const sourceSchema = "dbos_managed_source"
	const targetSchema = "dbos_managed_target"
	const publication = "wallaby_dbos_managed_publication"
	const slot = "wallaby_dbos_managed_slot"
	flowID := fmt.Sprintf("dbos-managed-flow-%d", suffix)
	appName := fmt.Sprintf("wallaby-managed-%d", suffix)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active", slot)
		_, _ = pool.Exec(cleanupCtx, "DROP PUBLICATION IF EXISTS wallaby_dbos_managed_publication")
		_, _ = pool.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS dbos_managed_source CASCADE")
		_, _ = pool.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS dbos_managed_target CASCADE")
	}()
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS dbos_managed_source CASCADE; DROP SCHEMA IF EXISTS dbos_managed_target CASCADE; DROP PUBLICATION IF EXISTS wallaby_dbos_managed_publication"); err != nil {
		t.Fatalf("reset managed DBOS fixture: %v", err)
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA dbos_managed_source; CREATE SCHEMA dbos_managed_target"); err != nil {
		t.Fatalf("create fixture schemas: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE dbos_managed_source.events (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE dbos_managed_target.events (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
		t.Fatalf("create target table: %v", err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO dbos_managed_source.events VALUES (1,'snapshot')"); err != nil {
		t.Fatalf("insert snapshot row: %v", err)
	}

	engine, err := workflow.NewPostgresEngineWithPool(ctx, pool)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer func() { _ = engine.Delete(context.Background(), flowID) }()
	checkpoints, err := checkpoint.NewPostgresStoreWithPool(ctx, pool)
	if err != nil {
		t.Fatalf("create checkpoint store: %v", err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatalf("create authority store: %v", err)
	}
	deliveries, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		t.Fatalf("create delivery coordinator: %v", err)
	}
	var publicationCrash atomic.Bool
	factory := runner.Factory{
		ManagedControl: pool, ManagedAuthority: authorityStore,
		BootstrapHooks: bootstrap.Hooks{AfterPublication: func(context.Context, bootstrap.ExportedSnapshot) error {
			if publicationCrash.CompareAndSwap(false, true) {
				return connector.ErrDeliveryIndeterminate
			}
			return nil
		}},
	}
	destinationSpec := connector.Spec{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{"dsn": dsn, "managed": "true", "batch_mode": "target", "destination_revision_id": "dbos-live-target-v1", "synchronous_commit": "on"}}
	mappings := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{Destination: destinationSpec.Name, FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude}, Tables: []flow.TableMapping{{SourceSchema: sourceSchema, SourceTable: "events", Action: flow.MappingActionInclude, TargetSchema: targetSchema, TargetTable: "events", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Columns: []flow.ColumnMapping{{SourceColumn: "id", Action: flow.MappingActionInclude, TargetColumn: "id"}, {SourceColumn: "payload", Action: flow.MappingActionInclude, TargetColumn: "payload"}}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}}}}}}}
	created, err := engine.Create(ctx, flow.Flow{ID: flowID, Name: "dbos-managed-bootstrap", State: flow.StateCreated, Parallelism: 2, Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{"dsn": dsn, "managed": "true", "bootstrap": "required", "slot": slot, "publication": publication, "publication_tables": sourceSchema + ".events", "ensure_publication": "true", "source_system_identifier": sourceSystemIdentifier, "source_lineage_id": "dbos-live-lineage", "publication_revision": "dbos-live-publication-v1", "batch_size": "100", "batch_timeout": "100ms"}}, Destinations: []connector.Spec{destinationSpec}, Config: flow.Config{TableMappings: mappings}})
	if err != nil {
		t.Fatalf("create managed flow: %v", err)
	}
	if _, err := engine.Start(ctx, created.ID); err != nil {
		t.Fatalf("start managed flow: %v", err)
	}

	orch, err := orchestrator.NewDBOSOrchestrator(ctx, orchestrator.Config{
		AppName: appName, DatabaseURL: dsn, Queue: "wallaby", MaxEmptyReads: 5,
		DefaultWire: connector.WireFormatJSON, Authority: authorityStore, Deliveries: deliveries,
	}, engine, checkpoints, factory)
	if err != nil {
		t.Fatalf("create DBOS orchestrator: %v", err)
	}
	defer orch.Shutdown(5 * time.Second)
	flowControl, err := engine.Control(ctx, flowID)
	if err != nil {
		t.Fatalf("read flow control: %v", err)
	}
	if err := orch.EnqueueRunOnce(ctx, flowID, flowControl.Generation); err != nil {
		t.Fatalf("enqueue managed run: %v", err)
	}
	completed := false
	defer func() {
		if !completed {
			logDBOSDiagnostics(t, context.Background(), pool, flowID, "")
		}
	}()

	waitFor(t, 45*time.Second, 100*time.Millisecond, func() (bool, error) {
		var count int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM dbos_managed_target.events").Scan(&count)
		return count == 1, err
	})
	waitFor(t, 15*time.Second, 100*time.Millisecond, func() (bool, error) {
		var running, liveLeases int
		err := pool.QueryRow(ctx, `
SELECT
 (SELECT count(*) FROM flow_executions WHERE flow_id=$1 AND generation=$2 AND status='running'),
 (SELECT count(*) FROM producer_leases lease
  JOIN flows flow ON flow.incarnation_id=lease.incarnation_id
  WHERE flow.id=$1 AND lease.generation=$2
    AND lease.lease_expires_at>clock_timestamp())`, flowID, flowControl.Generation).Scan(&running, &liveLeases)
		return running == 0 && liveLeases == 0, err
	})
	var interruptedBootstrapID, interruptedPhase string
	var destinationMarkers, prematureControlReceipts int
	if err := pool.QueryRow(ctx, `SELECT bootstrap_id::text,phase FROM source_bootstraps WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) ORDER BY bootstrap_generation DESC LIMIT 1`, flowID).Scan(&interruptedBootstrapID, &interruptedPhase); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallaby.managed_bootstrap_publications WHERE bootstrap_id=$1`, interruptedBootstrapID).Scan(&destinationMarkers); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM snapshot_publication_receipts WHERE bootstrap_id=$1::uuid`, interruptedBootstrapID).Scan(&prematureControlReceipts); err != nil {
		t.Fatal(err)
	}
	if !publicationCrash.Load() || interruptedPhase != "snapshotting" || destinationMarkers != 1 || prematureControlReceipts != 0 {
		t.Fatalf("DBOS recovery boundary crash=%t phase=%s marker=%d premature_receipts=%d", publicationCrash.Load(), interruptedPhase, destinationMarkers, prematureControlReceipts)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO dbos_managed_source.events VALUES (2,'stream')"); err != nil {
		t.Fatalf("insert streaming row: %v", err)
	}
	if err := orch.EnqueueRunOnce(ctx, flowID, flowControl.Generation); err != nil {
		t.Fatalf("enqueue DBOS recovery run: %v", err)
	}
	waitFor(t, 30*time.Second, 100*time.Millisecond, func() (bool, error) {
		var count int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM dbos_managed_target.events").Scan(&count)
		return count == 2, err
	})

	var bootstrapID, cutLSN, phase, manifestHash string
	var bootstrapGeneration int64
	if err := pool.QueryRow(ctx, `
SELECT bootstrap_id::text,bootstrap_generation,consistent_lsn,phase,manifest_hash
FROM source_bootstraps
WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)
ORDER BY bootstrap_generation DESC LIMIT 1`, flowID).Scan(&bootstrapID, &bootstrapGeneration, &cutLSN, &phase, &manifestHash); err != nil {
		t.Fatalf("read exact DBOS bootstrap cut: %v", err)
	}
	if bootstrapGeneration <= 0 || bootstrapID != interruptedBootstrapID || phase != "streaming" || cutLSN == "" || manifestHash == "" {
		t.Fatalf("DBOS bootstrap id=%q interrupted_id=%q generation=%d cut=%q phase=%q manifest=%q", bootstrapID, interruptedBootstrapID, bootstrapGeneration, cutLSN, phase, manifestHash)
	}
	var checkpointLSN string
	if err := pool.QueryRow(ctx, `SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&checkpointLSN); err != nil {
		t.Fatalf("read DBOS checkpoint: %v", err)
	}
	var checkpointAtOrAfterCut bool
	if err := pool.QueryRow(ctx, `SELECT $1::pg_lsn >= $2::pg_lsn`, checkpointLSN, cutLSN).Scan(&checkpointAtOrAfterCut); err != nil {
		t.Fatal(err)
	}
	if !checkpointAtOrAfterCut {
		t.Fatalf("DBOS checkpoint %s precedes bootstrap cut %s", checkpointLSN, cutLSN)
	}
	var publicationReceipts, snapshotRowReceipts, cdcReceipts, cutAckIntents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM snapshot_publication_receipts WHERE bootstrap_id=$1::uuid AND content_hash=$2 AND destination_revision_id='dbos-live-target-v1' AND authority_origin='fenced'`, bootstrapID, manifestHash).Scan(&publicationReceipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM snapshot_delivery_receipts receipt
JOIN snapshot_delivery_attempts attempt USING(attempt_id)
JOIN snapshot_delivery_evidence evidence USING(attempt_id)
WHERE receipt.bootstrap_id=$1::uuid
 AND receipt.content_hash=attempt.content_hash
 AND receipt.content_hash=evidence.content_hash
 AND receipt.durable_cursor #>> '{keys,0,name}'='id'
 AND receipt.durable_cursor #>> '{keys,0,value}'='1'`, bootstrapID).Scan(&snapshotRowReceipts); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 15*time.Second, 100*time.Millisecond, func() (bool, error) {
		err := pool.QueryRow(ctx, `
SELECT count(*) FROM delivery_manifests manifest
JOIN delivery_receipts receipt USING(flow_incarnation_id,destination_revision_id,position_id)
WHERE manifest.flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)
 AND manifest.checkpoint_lsn::pg_lsn>$2::pg_lsn
 AND manifest.content_hash=receipt.content_hash`, flowID, cutLSN).Scan(&cdcReceipts)
		return cdcReceipts >= 1, err
	})
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) AND checkpoint_lsn::pg_lsn=$2::pg_lsn`, flowID, cutLSN).Scan(&cutAckIntents); err != nil {
		t.Fatal(err)
	}
	if publicationReceipts != 1 || snapshotRowReceipts != 1 || cdcReceipts < 1 || cutAckIntents != 1 {
		t.Fatalf("DBOS boundary audit publication=%d snapshot_row=%d cdc=%d cut_ack=%d", publicationReceipts, snapshotRowReceipts, cdcReceipts, cutAckIntents)
	}

	var bootstrapCount, acquisitions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_bootstraps WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&bootstrapCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_acquisitions WHERE incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) AND generation=$2 AND backend='dbos'`, flowID, flowControl.Generation).Scan(&acquisitions); err != nil {
		t.Fatal(err)
	}
	if bootstrapCount != 1 || acquisitions < 2 {
		t.Fatalf("DBOS recovery bootstrap_generations=%d acquisitions=%d, want 1/at-least-2", bootstrapCount, acquisitions)
	}

	var owned int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resources WHERE flow_id=$1 AND ownership='owned' AND resource_kind IN ('slot','publication')`, flowID).Scan(&owned); err != nil {
		t.Fatalf("read resource ownership: %v", err)
	}
	if owned != 2 {
		t.Fatalf("owned resources=%d, want slot and publication", owned)
	}

	// Exercise the production stop bridge on every repetition so fixed physical
	// names cannot leave authority rows or logical slots for the next run.
	managedLifecycle := workflow.NewOrchestratedEngine(engine, orch, nil, runner.ManagedSourceCleanup{
		Authority: authorityStore,
		Factory:   factory,
	})
	stopped, err := managedLifecycle.Stop(ctx, flowID)
	if err != nil {
		t.Fatalf("stop managed DBOS flow with owned-resource cleanup: %v", err)
	}
	if stopped.State != flow.StateStopped {
		t.Fatalf("managed DBOS flow state=%s after stop, want stopped", stopped.State)
	}
	var remainingOwned, remainingPhysical int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resources WHERE flow_id=$1 AND ownership='owned' AND state<>'retired'`, flowID).Scan(&remainingOwned); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT
 (SELECT count(*) FROM pg_replication_slots WHERE slot_name IN (SELECT physical_name FROM source_resources WHERE flow_id=$1 AND resource_kind='slot'))+
 (SELECT count(*) FROM pg_publication WHERE pubname IN (SELECT physical_name FROM source_resources WHERE flow_id=$1 AND resource_kind='publication'))`, flowID).Scan(&remainingPhysical); err != nil {
		t.Fatal(err)
	}
	if remainingOwned != 0 || remainingPhysical != 0 {
		t.Fatalf("managed DBOS cleanup remaining owned=%d physical=%d", remainingOwned, remainingPhysical)
	}
	completed = true
}
