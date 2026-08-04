package tests

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestAuthorityV2CatalogAndRepresentativeMutations(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	expectedTables := []string{
		"flows", "flow_incarnations", "flow_state_events", "flow_executions", "execution_acquisitions", "producer_leases", "work_claims",
		"checkpoints", "checkpoint_outbox", "authoritative_checkpoints", "authoritative_checkpoint_outbox",
		"schema_versions", "ddl_events", "ddl_execution_attempts", "ddl_execution_receipts", "ddl_execution_manifests", "ddl_execution_run_attempts", "schema_publication_operations",
		"destination_revisions", "delivery_manifests", "delivery_attempts", "delivery_attempt_evidence", "delivery_receipts", "source_ack_intents", "source_ack_receipts",
		"source_bootstraps", "source_bootstrap_tasks", "snapshot_publication_receipts", "source_resources", "source_resource_operations", "snapshot_delivery_attempts", "snapshot_delivery_evidence", "snapshot_delivery_receipts",
		"canonical_schemas", "artifact_streams", "artifact_objects", "artifact_upload_attempts", "artifact_publications", "artifact_publication_objects", "artifact_deliveries", "artifact_quota_accounts", "artifact_quota_reservations", "artifact_gc_claims", "artifact_delivery_attempts", "artifact_delivery_receipts",
	}
	for _, table := range expectedTables {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_trigger AS trigger JOIN pg_proc AS procedure ON procedure.oid=trigger.tgfoid WHERE trigger.tgrelid=to_regclass($1) AND NOT trigger.tgisinternal AND procedure.proname='wallaby_require_authority_protocol_v2'`, table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Errorf("table %s authority-v2 trigger count=%d, want 1", table, count)
		}
	}

	staleCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	staleCfg.ConnConfig.RuntimeParams["wallaby.authority_protocol"] = "v1"
	stale, err := pgxpool.NewWithConfig(ctx, staleCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer stale.Close()
	suffix := fmt.Sprint(time.Now().UnixNano())
	engine, err := workflow.NewPostgresEngineWithPool(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := "authority-v2-workflow-" + suffix
	if _, err := engine.Create(ctx, currentTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	if _, err := stale.Exec(ctx, `UPDATE flows SET updated_at=clock_timestamp() WHERE id=$1`, flowID); err == nil || !isSQLState(err, "42501") {
		t.Fatalf("workflow v1 mutation error=%v, want SQLSTATE 42501", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE flows SET updated_at=clock_timestamp() WHERE id=$1`, flowID); err != nil {
		t.Fatalf("workflow v2 mutation rejected: %v", err)
	}
	tests := []struct {
		name string
		sql  string
		args []any
	}{
		{name: "checkpoint", sql: `INSERT INTO checkpoints(flow_id,lsn,metadata) VALUES($1,'0/1','{}')`, args: []any{"authority-v2-checkpoint-" + suffix}},
		{name: "registry", sql: `INSERT INTO schema_versions(namespace,name,version,schema_json) VALUES($1,'table',1,'{}')`, args: []any{"authority_v2_registry_" + suffix}},
		{name: "delivery", sql: `INSERT INTO destination_revisions(destination_revision_id,destination_name,config_fingerprint) VALUES($1,'target','hash')`, args: []any{"authority-v2-delivery-" + suffix}},
		{name: "artifact", sql: `INSERT INTO canonical_schemas(schema_id,projection_id,mapping_fingerprint,schema_json) VALUES($1,'canonical_cdc_parquet_v2',$2,'{}')`, args: []any{"authority-v2-artifact-" + suffix, strings.Repeat("a", 64)}},
		{name: "bootstrap", sql: `INSERT INTO source_bootstraps(bootstrap_id,flow_incarnation_id,flow_id,generation,bootstrap_generation,acquisition_id,lease_epoch,source_system_id,database_name,slot_name,publication_name,plugin,consistent_lsn,snapshot_name,manifest_hash,selection_hash,phase,owner_generation,owner_acquisition_id,owner_lease_epoch) VALUES($1,$2,'flow',1,1,$3,1,'system','database',$4,'publication','pgoutput','0/1','snapshot','hash','selection','abandoned',1,$3,1)`, args: []any{uuid.New(), uuid.New(), uuid.New(), "authority_v2_slot_" + suffix}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := stale.Exec(ctx, tt.sql, tt.args...); err == nil || !isSQLState(err, "42501") {
				t.Fatalf("v1 mutation error=%v, want SQLSTATE 42501", err)
			}
			if _, err := pool.Exec(ctx, tt.sql, tt.args...); err != nil {
				t.Fatalf("v2 mutation rejected: %v", err)
			}
		})
	}
	_, _ = pool.Exec(ctx, `DELETE FROM checkpoints WHERE flow_id=$1`, "authority-v2-checkpoint-"+suffix)
	_, _ = pool.Exec(ctx, `DELETE FROM schema_versions WHERE namespace=$1`, "authority_v2_registry_"+suffix)
	_, _ = pool.Exec(ctx, `DELETE FROM destination_revisions WHERE destination_revision_id=$1`, "authority-v2-delivery-"+suffix)
	_, _ = pool.Exec(ctx, `DELETE FROM canonical_schemas WHERE schema_id=$1`, "authority-v2-artifact-"+suffix)
	_, _ = pool.Exec(ctx, `DELETE FROM source_bootstraps WHERE slot_name=$1`, "authority_v2_slot_"+suffix)
}

func isSQLState(err error, state string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == state
}

func TestExternalCheckpointPutSerializesProducerAcquisition(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	checkpointStore, err := checkpoint.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer checkpointStore.Close()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	authorityStore, _ := authority.NewPostgresStore(pool)
	flowID := fmt.Sprintf("external-checkpoint-atomic-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	if _, err := engine.Create(ctx, currentTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.MarkDispatched(ctx, flowID, control.Generation); err != nil {
		t.Fatal(err)
	}
	blocker, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close(context.Background())
	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_lock(hashtext($1))`, flowID); err != nil {
		t.Fatal(err)
	}
	putDone := make(chan error, 1)
	go func() { putDone <- checkpointStore.PutExternal(ctx, flowID, connector.Checkpoint{LSN: "0/10"}) }()
	waitForAdvisoryWaiters(t, ctx, pool, blocker.PgConn().PID(), 1)
	type acquisitionResult struct {
		fence authority.RunFence
		err   error
	}
	acquireDone := make(chan acquisitionResult, 1)
	go func() {
		fence, acquireErr := authorityStore.AcquireProducer(ctx, flowID, "worker", "test", control.Generation, time.Minute)
		acquireDone <- acquisitionResult{fence: fence, err: acquireErr}
	}()
	waitForAdvisoryWaiters(t, ctx, pool, blocker.PgConn().PID(), 2)
	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, flowID); err != nil {
		t.Fatal(err)
	}
	if err := <-putDone; err != nil {
		t.Fatalf("PutExternal: %v", err)
	}
	acquired := <-acquireDone
	if acquired.err != nil {
		t.Fatalf("AcquireProducer after external checkpoint commit: %v", acquired.err)
	}
	if err := authorityStore.FinishProducer(ctx, acquired.fence, "test complete"); err != nil {
		t.Fatal(err)
	}
}

func waitForAdvisoryWaiters(t *testing.T, ctx context.Context, pool *pgxpool.Pool, holderPID uint32, want int) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		err := pool.QueryRow(ctx, `
SELECT count(*)
FROM pg_locks AS waiter
JOIN pg_locks AS holder
  ON holder.locktype=waiter.locktype AND holder.database IS NOT DISTINCT FROM waiter.database
 AND holder.classid IS NOT DISTINCT FROM waiter.classid AND holder.objid IS NOT DISTINCT FROM waiter.objid
 AND holder.objsubid IS NOT DISTINCT FROM waiter.objsubid
WHERE holder.pid=$1 AND holder.locktype='advisory' AND holder.granted AND NOT waiter.granted`, holderPID).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count >= want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %d advisory waiters: %v", want, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestPostgresGenerationFenceRejectsStaleCommit(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	flowID := fmt.Sprintf("authority-stale-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, currentTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.AcquireProducer(ctx, flowID, "worker-1", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// Force expiry in PostgreSQL; no timing-based sleep participates in the
	// ownership transition.
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE incarnation_id = $1`, first.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	if err := store.RenewProducer(ctx, first, time.Minute); !errors.Is(err, authority.ErrLeaseExpired) {
		t.Fatalf("expired RenewProducer error=%v, want ErrLeaseExpired", err)
	}
	second, err := store.AcquireProducer(ctx, flowID, "worker-2", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.LeaseEpoch != first.LeaseEpoch+1 || second.AcquisitionID == first.AcquisitionID {
		t.Fatalf("takeover fence=(epoch:%d acquisition:%s), want epoch %d and new acquisition", second.LeaseEpoch, second.AcquisitionID, first.LeaseEpoch+1)
	}
	if err := store.FinishProducer(ctx, first, "stale"); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale FinishProducer error=%v, want ErrFenceRejected", err)
	}
	if err := store.FailFlow(ctx, first, "stale"); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale FailFlow error=%v, want ErrFenceRejected", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE flows SET lifecycle_generation = lifecycle_generation + 1 WHERE id = $1`, flowID); err != nil {
		t.Fatal(err)
	}
	if err := store.RenewProducer(ctx, second, time.Minute); !errors.Is(err, authority.ErrLeaseExpired) {
		t.Fatalf("old-generation RenewProducer error=%v, want ErrLeaseExpired", err)
	}
	if err := store.FailFlow(ctx, second, "stale generation"); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("old-generation FailFlow error=%v, want ErrFenceRejected", err)
	}
}

func TestPostgresRunFenceValidationSerializesTakeover(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, _ := authority.NewPostgresStore(pool)
	flowID := fmt.Sprintf("authority-linearizable-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, currentTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.AcquireProducer(ctx, flowID, "linearizable-old", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	guarded, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = guarded.Rollback(context.Background()) }()
	if err := authority.ValidateRunFence(ctx, guarded, first); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, first.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	takeoverCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	takeoverCfg.ConnConfig.RuntimeParams["lock_timeout"] = "100ms"
	takeoverPool, err := pgxpool.NewWithConfig(ctx, takeoverCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer takeoverPool.Close()
	takeoverStore, _ := authority.NewPostgresStore(takeoverPool)
	if _, err := takeoverStore.AcquireProducer(ctx, flowID, "linearizable-new", "test", control.Generation, time.Minute); err == nil {
		t.Fatal("takeover committed while a validated authoritative transaction held the flow lock")
	}
	if _, err := guarded.Exec(ctx, `UPDATE flows SET updated_at=clock_timestamp() WHERE id=$1 AND incarnation_id=$2`, flowID, first.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	if err := guarded.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := store.AcquireProducer(ctx, flowID, "linearizable-new", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.LeaseEpoch != first.LeaseEpoch+1 {
		t.Fatalf("takeover epoch=%d, want %d", second.LeaseEpoch, first.LeaseEpoch+1)
	}
}

func TestPostgresCheckpointGenerationFenceRejectsStaleCommit(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	checkpointStore, err := checkpoint.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer checkpointStore.Close()

	flowID := fmt.Sprintf("checkpoint-fence-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, currentTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	oldFence, err := authorityStore.AcquireProducer(ctx, flowID, "checkpoint-old", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpointStore.PutFenced(ctx, oldFence, connector.Checkpoint{LSN: "0/10"}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, oldFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	newFence, err := authorityStore.AcquireProducer(ctx, flowID, "checkpoint-new", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpointStore.PutFenced(ctx, oldFence, connector.Checkpoint{LSN: "0/20"}); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale checkpoint error=%v, want ErrFenceRejected", err)
	}
	if err := checkpointStore.PutFenced(ctx, newFence, connector.Checkpoint{LSN: "0/20"}); err != nil {
		t.Fatal(err)
	}
	got, err := checkpointStore.GetFenced(ctx, newFence)
	if err != nil {
		t.Fatal(err)
	}
	if got.LSN != "0/20" {
		t.Fatalf("fenced checkpoint=%q, want 0/20", got.LSN)
	}
	var acquisitionID string
	if err := pool.QueryRow(ctx, `SELECT acquisition_id::text FROM authoritative_checkpoints WHERE flow_incarnation_id=$1`, newFence.FlowIncarnationID).Scan(&acquisitionID); err != nil {
		t.Fatal(err)
	}
	if acquisitionID != newFence.AcquisitionID.String() {
		t.Fatalf("checkpoint acquisition=%s, want current owner %s", acquisitionID, newFence.AcquisitionID)
	}
}

func TestPostgresFlowIDReuseDoesNotRestoreOldState(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	flowID := fmt.Sprintf("authority-reuse-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, currentTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	oldFence, err := store.AcquireProducer(ctx, flowID, "old-worker", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapper, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	oldBootstrap, err := bootstrapper.Start(ctx, oldFence, "old_incarnation_publication", "old-manifest")
	if err != nil {
		t.Fatal(err)
	}
	if err := oldBootstrap.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bootstrapper.Abandon(ctx, oldFence, oldBootstrap.Snapshot, "flow reuse test"); err != nil {
		t.Fatal(err)
	}
	oldExecutionFence, err := engine.RegisterExecutionFence(ctx, flowID, "kubernetes-stable-id", "kubernetes", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	staleGeneration := oldExecutionFence
	staleGeneration.Generation++
	if err := engine.RenewExecutionFence(ctx, staleGeneration, time.Minute); !errors.Is(err, workflow.ErrInvalidState) {
		t.Fatalf("stale compatibility generation renew error=%v, want ErrInvalidState", err)
	}
	if err := engine.FinishExecutionFence(ctx, oldExecutionFence, "old incarnation complete"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE incarnation_id = $1`, oldFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE flows SET state='stopped', lifecycle_target='stopped', dispatch_pending=FALSE WHERE id=$1`, flowID); err != nil {
		t.Fatal(err)
	}
	if err := engine.Delete(ctx, flowID); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Create(ctx, currentTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	_, newControl, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	newFence, err := store.AcquireProducer(ctx, flowID, "new-worker", "test", newControl.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RegisterExecutionFence(ctx, flowID, "kubernetes-stable-id", "kubernetes", newControl.Generation, time.Minute); err != nil {
		t.Fatalf("recreated incarnation could not reuse deterministic execution ID: %v", err)
	}
	if err := engine.FinishExecutionFence(ctx, oldExecutionFence, "stale old incarnation"); !errors.Is(err, workflow.ErrInvalidState) {
		t.Fatalf("recreated flow accepted old compatibility execution fence: %v", err)
	}
	if newFence.FlowIncarnationID == oldFence.FlowIncarnationID {
		t.Fatalf("recreated flow reused incarnation %s", newFence.FlowIncarnationID)
	}
	if _, _, err := bootstrapper.LoadLatest(ctx, newFence); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("new incarnation restored old bootstrap: %v", err)
	}
	if err := store.RenewProducer(ctx, oldFence, time.Minute); !errors.Is(err, authority.ErrLeaseExpired) {
		t.Fatalf("old incarnation RenewProducer error=%v, want ErrLeaseExpired", err)
	}
}

func TestAuthorityProtocolGateRejectsStaleBinarySession(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	flowID := fmt.Sprintf("authority-protocol-%d", time.Now().UnixNano())
	if _, err := engine.Create(ctx, currentTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	currentPool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer currentPool.Close()
	defer cleanupAuthorityTest(ctx, currentPool, flowID)

	staleConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	staleConfig.ConnConfig.RuntimeParams["wallaby.authority_protocol"] = "v1"
	stalePool, err := pgxpool.NewWithConfig(ctx, staleConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer stalePool.Close()
	_, err = stalePool.Exec(ctx, `INSERT INTO flow_executions (flow_id,execution_id,status) VALUES ($1,'stale-worker','running')`, flowID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("stale session error=%v, want authority protocol rejection SQLSTATE 42501", err)
	}
}

func newAuthorityTestPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	controlstore.ConfigurePool(cfg)
	return pgxpool.NewWithConfig(ctx, cfg)
}

// currentTestFlow binds live fixtures to the mandatory current mapping contract
// without changing the behavior under test. Connector-specific fixtures may
// replace the generated append policies with explicit upsert policies.
func currentTestFlow(definition flow.Flow) flow.Flow {
	if definition.Source.Type == "" {
		definition.Source = connector.Spec{Name: "source", Type: connector.EndpointPostgres}
	}
	if definition.Source.Name == "" {
		definition.Source.Name = "source"
	}
	if len(definition.Destinations) == 0 {
		definition.Destinations = []connector.Spec{{Name: "target", Type: connector.EndpointPostgres}}
	}
	for index := range definition.Destinations {
		if definition.Destinations[index].Name == "" {
			definition.Destinations[index].Name = fmt.Sprintf("target-%d", index+1)
		}
		if definition.Destinations[index].Options != nil {
			options := maps.Clone(definition.Destinations[index].Options)
			definition.Destinations[index].Options = options
		}
	}
	if definition.Config.TableMappings.Version != 0 {
		return definition
	}
	definition.Config.TableMappings = flow.NewTableMappings(definition.Destinations)
	tables := strings.TrimSpace(definition.Source.Options["tables"])
	if tables == "" {
		return definition
	}
	sourceName := strings.TrimSpace(strings.Split(tables, ",")[0])
	sourceSchema, sourceTable := "public", sourceName
	if parts := strings.SplitN(sourceName, ".", 2); len(parts) == 2 {
		sourceSchema, sourceTable = parts[0], parts[1]
	}
	for index := range definition.Destinations {
		destination := &definition.Destinations[index]
		targetSchema, targetTable := sourceSchema, sourceTable
		if value := strings.TrimSpace(destination.Options["schema"]); value != "" {
			targetSchema = value
		}
		if value := strings.TrimSpace(destination.Options["table"]); value != "" {
			targetTable = value
		}
		delete(destination.Options, "schema")
		delete(destination.Options, "table")
		delete(destination.Options, "write_mode")
		mapping := &definition.Config.TableMappings.Destinations[index]
		mapping.FutureTables = flow.FutureTableMapping{Action: flow.MappingActionExclude}
		policy := flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}
		if destination.Type == connector.EndpointPostgres && definition.Source.Options["managed"] == "true" {
			policy = flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}}
		}
		mapping.Tables = []flow.TableMapping{{SourceSchema: sourceSchema, SourceTable: sourceTable, Action: flow.MappingActionInclude, TargetSchema: targetSchema, TargetTable: targetTable, FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Write: policy}}
	}
	return definition
}

func bindTestUpsertPolicy(transaction *connector.SourceTransaction, keys ...string) {
	for index := range transaction.Fragments {
		transaction.Fragments[index].Batch.WritePolicy = connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: append([]string(nil), keys...)}
	}
}
func testUpsertPolicy(keys ...string) connector.TableWritePolicy {
	return connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: append([]string(nil), keys...)}
}
func testAppendPolicy() connector.TableWritePolicy {
	return connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}
}

func cleanupAuthorityTest(ctx context.Context, pool *pgxpool.Pool, flowID string) {
	var incarnations []string
	rows, err := pool.Query(ctx, `SELECT incarnation_id::text FROM flow_incarnations WHERE flow_id=$1`, flowID)
	if err == nil {
		for rows.Next() {
			var incarnation string
			if rows.Scan(&incarnation) == nil {
				incarnations = append(incarnations, incarnation)
			}
		}
		rows.Close()
	}
	_, _ = pool.Exec(ctx, "DELETE FROM flows WHERE id=$1", flowID)
	for _, incarnation := range incarnations {
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_delivery_receipts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_delivery_attempts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_deliveries WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_barriers WHERE publication_id IN (SELECT publication_id FROM artifact_publications WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_publication_objects WHERE publication_id IN (SELECT publication_id FROM artifact_publications WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_publications WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_gc_claims WHERE artifact_id IN (SELECT artifact_id FROM artifact_objects WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_upload_attempts WHERE artifact_id IN (SELECT artifact_id FROM artifact_objects WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_quota_reservations WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_objects WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_quota_accounts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_streams WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM snapshot_publication_receipts WHERE bootstrap_id IN (SELECT bootstrap_id FROM source_bootstraps WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM snapshot_delivery_receipts WHERE bootstrap_id IN (SELECT bootstrap_id FROM source_bootstraps WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM snapshot_delivery_evidence WHERE attempt_id IN (SELECT attempt_id FROM snapshot_delivery_attempts WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM snapshot_delivery_attempts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM source_bootstrap_tasks WHERE bootstrap_id IN (SELECT bootstrap_id FROM source_bootstraps WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM source_resource_operations WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM source_resources WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM source_bootstraps WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM schema_publication_operations WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM ddl_execution_run_attempts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM ddl_execution_receipts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM ddl_execution_attempts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM ddl_execution_manifests WHERE event_id IN (SELECT id FROM ddl_events WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM ddl_events WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM schema_versions WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM source_ack_receipts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM source_ack_intents WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM delivery_receipts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM delivery_attempt_evidence WHERE attempt_id IN (SELECT attempt_id FROM delivery_attempts WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM delivery_attempts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM delivery_manifests WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM authoritative_checkpoint_outbox WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM authoritative_checkpoints WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM work_claims WHERE incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM producer_leases WHERE incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM execution_acquisitions WHERE incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM flow_executions WHERE incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM flow_state_events WHERE incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM flow_incarnations WHERE incarnation_id=$1", incarnation)
	}
}
