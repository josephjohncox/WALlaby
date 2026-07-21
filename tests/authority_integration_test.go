package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

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
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID}); err != nil {
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
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID}); err != nil {
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
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID}); err != nil {
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
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID}); err != nil {
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
	if err := engine.RegisterExecutionGeneration(ctx, flowID, "kubernetes-stable-id", "kubernetes", control.Generation, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := engine.FinishExecutionReason(ctx, flowID, "kubernetes-stable-id", "old incarnation complete"); err != nil {
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
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID}); err != nil {
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
	if err := engine.RegisterExecutionGeneration(ctx, flowID, "kubernetes-stable-id", "kubernetes", newControl.Generation, time.Minute); err != nil {
		t.Fatalf("recreated incarnation could not reuse deterministic execution ID: %v", err)
	}
	if newFence.FlowIncarnationID == oldFence.FlowIncarnationID {
		t.Fatalf("recreated flow reused incarnation %s", newFence.FlowIncarnationID)
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
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID}); err != nil {
		t.Fatal(err)
	}
	currentPool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer currentPool.Close()
	defer cleanupAuthorityTest(ctx, currentPool, flowID)

	stalePool, err := pgxpool.New(ctx, dsn)
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
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_publication_objects WHERE publication_id IN (SELECT publication_id FROM artifact_publications WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_publications WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_gc_claims WHERE artifact_id IN (SELECT artifact_id FROM artifact_objects WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_upload_attempts WHERE artifact_id IN (SELECT artifact_id FROM artifact_objects WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_quota_reservations WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_objects WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_quota_accounts WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM artifact_streams WHERE flow_incarnation_id=$1", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM snapshot_publication_receipts WHERE bootstrap_id IN (SELECT bootstrap_id FROM source_bootstraps WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM source_bootstrap_tasks WHERE bootstrap_id IN (SELECT bootstrap_id FROM source_bootstraps WHERE flow_incarnation_id=$1)", incarnation)
		_, _ = pool.Exec(ctx, "DELETE FROM source_bootstraps WHERE flow_incarnation_id=$1", incarnation)
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
