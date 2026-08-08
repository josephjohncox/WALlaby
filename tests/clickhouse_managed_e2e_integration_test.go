package tests

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	chclient "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	clickhousedest "github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestPostgresToClickHouseManagedProfileRecoveryContract(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	fixture := newClickHouseManagedFixtureWithInsertRows(t, 180, 1)
	if err := fixture.destination.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Second)
	defer cancel()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	const publication = "wallaby_clickhouse_managed_e2e_publication"
	if _, err := pool.Exec(ctx, `
DROP PUBLICATION IF EXISTS wallaby_clickhouse_managed_e2e_publication;
DROP TABLE IF EXISTS public.wallaby_clickhouse_managed_e2e_source;
CREATE TABLE public.wallaby_clickhouse_managed_e2e_source (id bigint PRIMARY KEY, value text, payload jsonb, tags text[]);
CREATE PUBLICATION wallaby_clickhouse_managed_e2e_publication FOR TABLE public.wallaby_clickhouse_managed_e2e_source`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `
DROP PUBLICATION IF EXISTS wallaby_clickhouse_managed_e2e_publication;
DROP TABLE IF EXISTS public.wallaby_clickhouse_managed_e2e_source`)
	}()

	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	checkpoints, err := checkpoint.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer checkpoints.Close()
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}

	flowID := "clickhouse-managed-e2e-" + suffix
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	placeholderDestination := connector.RuntimeSpec{Name: "clickhouse-managed-e2e", Type: connector.EndpointClickHouse}
	created, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(placeholderDestination), Config: flow.Config{AckPolicy: stream.AckPolicyAll, TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{placeholderDestination})}})
	if err != nil {
		t.Fatal(err)
	}
	var incarnationID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT incarnation_id FROM flows WHERE id=$1`, flowID).Scan(&incarnationID); err != nil {
		t.Fatal(err)
	}
	slotName := bootstrap.GenerationSlotName(flowID, incarnationID, 1)
	defer func() {
		_, _ = pool.Exec(context.Background(), "SELECT pg_catalog.pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slotName)
	}()
	var sourceSystemID string
	if err := pool.QueryRow(ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&sourceSystemID); err != nil {
		t.Fatal(err)
	}
	publicationRevision, err := pgsource.PublicationFingerprint(ctx, pool, publication)
	if err != nil {
		t.Fatal(err)
	}
	created.Source = testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "slot": slotName, "publication": publication,
		"ensure_publication": "false", "sync_publication": "false", "create_slot": "false", "ensure_state": "false",
		"managed_profile": connector.ManagedProfilePostgresToClickHouseAppendV1, "bootstrap": "never", "streaming_transactions": "true",
		"status_interval": "10ms", "batch_timeout": "10ms",
		"max_transaction_records": "100000", "max_transaction_bytes": "134217728", "max_transaction_fragments": "128",
		"source_system_identifier": sourceSystemID, "source_lineage_id": sourceSystemID + ":" + publication + ":v1",
		"publication_revision": publicationRevision,
	}})
	destinationRevisionID := "clickhouse-managed-e2e-" + suffix
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM destination_revisions WHERE destination_revision_id=$1", destinationRevisionID)
	}()
	destinationSpec := fixture.spec
	destinationSpec.Name = "clickhouse-managed-e2e"
	destinationSpec.Options = cloneStringMap(fixture.spec.Options)
	destinationSpec.Options["destination_revision_id"] = destinationRevisionID
	destinationSpec.Options["managed_max_rows_per_batch"] = "1"
	created.Destinations = testFlowDestinations(destinationSpec)

	started, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	started.Source = created.Source
	started.Destinations = created.Destinations
	started.Config.AckPolicy = stream.AckPolicyAll

	var provisionedSlot, initialLSN string
	if err := pool.QueryRow(ctx, `SELECT slot_name,lsn::text FROM pg_catalog.pg_create_logical_replication_slot($1,'pgoutput')`, slotName).Scan(&provisionedSlot, &initialLSN); err != nil {
		t.Fatal(err)
	}
	if provisionedSlot != slotName || initialLSN == "" {
		t.Fatalf("provisioned slot=(%q,%q), want exact slot and cut", provisionedSlot, initialLSN)
	}
	seedFence, err := authorityStore.AcquireProducer(ctx, flowID, "clickhouse-e2e-seed", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.AuthorizeAck(ctx, seedFence, connector.Checkpoint{LSN: initialLSN}, emptyManagedBaselinePayload(t, flowID+":source")); err != nil {
		t.Fatal(err)
	}
	if err := authorityStore.FinishProducer(ctx, seedFence, "checkpoint_seeded"); err != nil {
		t.Fatal(err)
	}

	failingDestination := &clickhousedest.Destination{}

	runCtx, stopRun := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		flowRunner := runner.FlowRunner{
			Engine: engine, Checkpoints: checkpoints, DDLPolicyDefaults: noAutomaticDDLDefaults(), Authority: authorityStore, Deliveries: coordinator, SchemaBaselines: mustManagedSchemaBaselines(t, pool),
			ExpectedGeneration: control.Generation, ExecutionID: "clickhouse-e2e-first", ExecutionBackend: "test",
		}
		runErr <- flowRunner.Run(runCtx, started, &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}, []stream.DestinationConfig{{Spec: destinationSpec, Dest: failingDestination}})
	}()
	defer stopRun()

	waitForCondition(t, ctx, runErr, "ClickHouse managed source slot activation", func() (bool, error) {
		var active bool
		err := pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&active)
		return active, err
	})
	waitForCondition(t, ctx, runErr, "initial source ACK receipt", func() (bool, error) {
		var count int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=$1`, incarnationID).Scan(&count)
		return count == 1, err
	})
	var firstAcquisition uuid.UUID
	var firstLeaseEpoch int64
	if err := pool.QueryRow(ctx, `SELECT acquisition_id,lease_epoch FROM producer_leases WHERE incarnation_id=$1`, incarnationID).Scan(&firstAcquisition, &firstLeaseEpoch); err != nil {
		t.Fatal(err)
	}
	scaleClickHouseHarnessDeployment(t, "wallaby-it-clickhouse-keeper", 0)
	t.Cleanup(func() { scaleClickHouseHarnessDeployment(t, "wallaby-it-clickhouse-keeper", 1) })
	waitForClickHouseKeeperUnavailable(t, fixture, 45*time.Second)
	if _, err := pool.Exec(ctx, `
INSERT INTO public.wallaby_clickhouse_managed_e2e_source (id,value,payload,tags) VALUES
  (1,'first','{"nested":{"count":1}}'::jsonb,ARRAY['alpha','beta']),
  (2,'second','[1,2,3]'::jsonb,ARRAY['gamma'])`); err != nil {
		t.Fatal(err)
	}
	assertClickHouseAuthorityNotAdvanced(t, ctx, pool, incarnationID, fixture, initialLSN, 1)
	// Keeper loss is retryable. Observe a bounded interval in which the runner
	// stays alive while authority remains unchanged, then cancel it so Keeper is
	// restored before later profile cells begin.
	select {
	case err := <-runErr:
		t.Fatalf("managed runner terminated during retryable Keeper outage: %v", err)
	case <-time.After(2 * time.Second):
	}
	assertClickHouseAuthorityNotAdvanced(t, ctx, pool, incarnationID, fixture, initialLSN, 1)
	stopRun()
	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("managed runner cancellation error=%v, want context canceled", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("managed runner did not stop after cancellation during Keeper outage")
	}
	assertClickHouseAuthorityNotAdvanced(t, ctx, pool, incarnationID, fixture, initialLSN, 1)

	scaleClickHouseHarnessDeployment(t, "wallaby-it-clickhouse-keeper", 1)
	recoveredKeeperAddress := restartClickHouseKeeperHarnessPortForward(t)
	fixture.keeperProxy.SetTarget(recoveredKeeperAddress)
	waitForClickHouseManagedReplicas(t, fixture, 90*time.Second)

	restartCtx, stopRestart := context.WithCancel(ctx)
	restartErr := make(chan error, 1)
	recoveredDestination := &clickhousedest.Destination{}
	go func() {
		flowRunner := runner.FlowRunner{
			Engine: engine, Checkpoints: checkpoints, DDLPolicyDefaults: noAutomaticDDLDefaults(), Authority: authorityStore, Deliveries: coordinator, SchemaBaselines: mustManagedSchemaBaselines(t, pool),
			ExpectedGeneration: control.Generation, ExecutionID: "clickhouse-e2e-recovery", ExecutionBackend: "test",
		}
		restartErr <- flowRunner.Run(restartCtx, started, &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}, []stream.DestinationConfig{{Spec: destinationSpec, Dest: recoveredDestination}})
	}()
	defer stopRestart()
	waitForCondition(t, ctx, restartErr, "ClickHouse managed slot takeover", func() (bool, error) {
		var active bool
		err := pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&active)
		return active, err
	})
	var secondAcquisition uuid.UUID
	var secondLeaseEpoch int64
	if err := pool.QueryRow(ctx, `SELECT acquisition_id,lease_epoch FROM producer_leases WHERE incarnation_id=$1`, incarnationID).Scan(&secondAcquisition, &secondLeaseEpoch); err != nil {
		t.Fatal(err)
	}
	if secondAcquisition == firstAcquisition || secondLeaseEpoch <= firstLeaseEpoch {
		t.Fatalf("producer lease did not advance across recovery: first=%s/%d second=%s/%d", firstAcquisition, firstLeaseEpoch, secondAcquisition, secondLeaseEpoch)
	}

	waitForCondition(t, ctx, restartErr, "ClickHouse target receipt and converged rows", func() (bool, error) {
		var receipts, rows int
		if err := fixture.db.QueryRowContext(ctx, "SELECT count() FROM {database:Identifier}.{table:Identifier} FINAL", chclient.Named("database", fixture.database), chclient.Named("table", fixture.receiptsTable)).Scan(&receipts); err != nil {
			return false, err
		}
		if err := fixture.db.QueryRowContext(ctx, "SELECT count() FROM {database:Identifier}.{view:Identifier}", chclient.Named("database", fixture.database), chclient.Named("view", fixture.finalView)).Scan(&rows); err != nil {
			return false, err
		}
		return receipts == 1 && rows == 2, nil
	})
	var checkpointLSN string
	waitForCondition(t, ctx, restartErr, "PostgreSQL receipt checkpoint and source ACK", func() (bool, error) {
		var deliveryReceipts, ackReceipts int
		err := pool.QueryRow(ctx, `
SELECT checkpoint.lsn,
  (SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=checkpoint.flow_incarnation_id),
  (SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=checkpoint.flow_incarnation_id)
FROM authoritative_checkpoints AS checkpoint
WHERE checkpoint.flow_incarnation_id=$1`, incarnationID).Scan(&checkpointLSN, &deliveryReceipts, &ackReceipts)
		return checkpointLSN != initialLSN && deliveryReceipts == 1 && ackReceipts == 2, err
	})
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_attempts WHERE flow_incarnation_id=$1`, incarnationID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts < 1 {
		t.Fatalf("delivery attempts=%d, want at least the recovered delivery attempt", attempts)
	}
	checkpointPosition, err := pglogrepl.ParseLSN(checkpointLSN)
	if err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, ctx, restartErr, "source confirmed flush equals recovered checkpoint", func() (bool, error) {
		var confirmed string
		if err := pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&confirmed); err != nil {
			return false, err
		}
		confirmedPosition, err := pglogrepl.ParseLSN(confirmed)
		return err == nil && confirmedPosition == checkpointPosition, err
	})

	if _, err := pool.Exec(ctx, `
BEGIN;
ALTER TABLE public.wallaby_clickhouse_managed_e2e_source ADD COLUMN extra integer;
INSERT INTO public.wallaby_clickhouse_managed_e2e_source (id,value,payload,tags,extra)
VALUES (3,'evolved','{"schema":"v2"}'::jsonb,ARRAY['delta','epsilon'],7);
COMMIT`); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, ctx, restartErr, "real PostgreSQL schema barrier and typed row", func() (bool, error) {
		var receipts, rows, ddlRows, insertRows, typedRows, evolvedRows, structuredPlans, schemaIdentities int
		if err := fixture.db.QueryRowContext(ctx, "SELECT count() FROM {database:Identifier}.{table:Identifier} FINAL", chclient.Named("database", fixture.database), chclient.Named("table", fixture.receiptsTable)).Scan(&receipts); err != nil {
			return false, err
		}
		if err := fixture.db.QueryRowContext(ctx, `
SELECT count(),countIf(operation='ddl'),countIf(operation='insert'),
       countIf(operation='insert' AND position(after_json,'\"payload\":{\"nested\":{\"count\":1}}') > 0 AND position(after_json,'\"tags\":[\"alpha\",\"beta\"]') > 0),
       countIf(operation='insert' AND position(after_json,'\"payload\":{\"schema\":\"v2\"}') > 0 AND position(after_json,'\"tags\":[\"delta\",\"epsilon\"]') > 0 AND position(after_json,'\"extra\":7') > 0),
       countIf(operation='ddl' AND isValidJSON(ddl_plan) AND position(ddl_plan,'add_column') > 0),
       uniqExact(schema_fingerprint)
FROM {database:Identifier}.{view:Identifier}`,
			chclient.Named("database", fixture.database), chclient.Named("view", fixture.finalView),
		).Scan(&rows, &ddlRows, &insertRows, &typedRows, &evolvedRows, &structuredPlans, &schemaIdentities); err != nil {
			return false, err
		}
		return receipts == 2 && rows == 4 && ddlRows == 1 && insertRows == 3 && typedRows == 1 && evolvedRows == 1 && structuredPlans == 1 && schemaIdentities >= 2, nil
	})
	waitForCondition(t, ctx, restartErr, "schema transaction PostgreSQL authority and ACK", func() (bool, error) {
		var deliveryReceipts, ackReceipts int
		err := pool.QueryRow(ctx, `
SELECT checkpoint.lsn,
  (SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=checkpoint.flow_incarnation_id),
  (SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=checkpoint.flow_incarnation_id)
FROM authoritative_checkpoints AS checkpoint
WHERE checkpoint.flow_incarnation_id=$1`, incarnationID).Scan(&checkpointLSN, &deliveryReceipts, &ackReceipts)
		return checkpointLSN != initialLSN && deliveryReceipts == 2 && ackReceipts == 3, err
	})
	finalCheckpointPosition, err := pglogrepl.ParseLSN(checkpointLSN)
	if err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, ctx, restartErr, "source confirmed flush equals schema checkpoint", func() (bool, error) {
		var confirmed string
		if err := pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&confirmed); err != nil {
			return false, err
		}
		confirmedPosition, err := pglogrepl.ParseLSN(confirmed)
		return err == nil && confirmedPosition == finalCheckpointPosition, err
	})

	stopRestart()
	select {
	case err := <-restartErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("recovered ClickHouse managed runner did not stop: %v", ctx.Err())
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func assertClickHouseAuthorityNotAdvanced(t *testing.T, ctx context.Context, pool *pgxpool.Pool, incarnationID uuid.UUID, fixture *clickHouseManagedFixture, initialLSN string, wantAckReceipts int) {
	t.Helper()
	var checkpointLSN string
	if err := pool.QueryRow(ctx, `SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=$1`, incarnationID).Scan(&checkpointLSN); err != nil {
		t.Fatal(err)
	}
	var deliveryReceipts, ackReceipts int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=$1),(SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=$1)`, incarnationID).Scan(&deliveryReceipts, &ackReceipts); err != nil {
		t.Fatal(err)
	}
	var targetReceipts int
	if err := fixture.db.QueryRowContext(ctx, "SELECT count() FROM {database:Identifier}.{table:Identifier} FINAL", chclient.Named("database", fixture.database), chclient.Named("table", fixture.receiptsTable)).Scan(&targetReceipts); err != nil {
		t.Fatal(err)
	}
	if checkpointLSN != initialLSN || deliveryReceipts != 0 || ackReceipts != wantAckReceipts || targetReceipts != 0 {
		t.Fatalf("authority advanced before completion: checkpoint=%s delivery_receipts=%d ack_receipts=%d target_receipts=%d", checkpointLSN, deliveryReceipts, ackReceipts, targetReceipts)
	}
}

func waitForClickHouseKeeperUnavailable(t *testing.T, fixture *clickHouseManagedFixture, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var unavailable uint64
		err := fixture.db.QueryRowContext(context.Background(), `
SELECT countIf(is_readonly=1 OR is_session_expired=1)
FROM system.replicas
WHERE database={database:String} AND table IN ({changelog:String},{receipts:String})`,
			chclient.Named("database", fixture.database), chclient.Named("changelog", fixture.changelogTable), chclient.Named("receipts", fixture.receiptsTable),
		).Scan(&unavailable)
		if err == nil && unavailable > 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("ClickHouse replicas did not expose Keeper unavailability")
}

func scaleClickHouseHarnessDeployment(t *testing.T, deployment string, replicas int) {
	t.Helper()
	kubeconfig := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_KUBECONFIG"))
	namespace := strings.TrimSpace(os.Getenv("WALLABY_TEST_K8S_NAMESPACE"))
	if kubeconfig == "" || namespace == "" {
		t.Fatal("ClickHouse recovery gate requires the Kubernetes harness")
	}
	args := []string{"--kubeconfig", kubeconfig, "-n", namespace, "scale", "deployment/" + deployment, "--replicas=" + strconv.Itoa(replicas)}
	if output, err := exec.Command("kubectl", args...).CombinedOutput(); err != nil {
		t.Fatalf("scale %s to %d replicas: %v: %s", deployment, replicas, err, output)
	}
	if replicas > 0 {
		args = []string{"--kubeconfig", kubeconfig, "-n", namespace, "rollout", "status", "deployment/" + deployment, "--timeout=120s"}
		if output, err := exec.Command("kubectl", args...).CombinedOutput(); err != nil {
			t.Fatalf("wait for %s rollout: %v: %s", deployment, err, output)
		}
		return
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		args = []string{"--kubeconfig", kubeconfig, "-n", namespace, "get", "deployment/" + deployment, "-o", "jsonpath={.status.replicas}"}
		output, err := exec.Command("kubectl", args...).CombinedOutput()
		if err == nil {
			count, _ := strconv.Atoi(strings.TrimSpace(string(output)))
			if count == 0 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("deployment %s did not scale to zero", deployment)
}
