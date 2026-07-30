package tests

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
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

func TestPostgresToPostgresManagedRecoveryContract(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
DROP PUBLICATION IF EXISTS wallaby_managed_e2e_publication;
DROP TABLE IF EXISTS public.wallaby_managed_source;
DROP TABLE IF EXISTS public.wallaby_managed_target;
CREATE TABLE public.wallaby_managed_source (id bigint PRIMARY KEY, value text, payload text, counter bigint NOT NULL DEFAULT 0);
CREATE TABLE public.wallaby_managed_target (id bigint PRIMARY KEY, value text, payload text, counter bigint NOT NULL DEFAULT 0);
CREATE PUBLICATION wallaby_managed_e2e_publication FOR TABLE public.wallaby_managed_source`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `
DROP PUBLICATION IF EXISTS wallaby_managed_e2e_publication;
DROP TABLE IF EXISTS public.wallaby_managed_source;
DROP TABLE IF EXISTS public.wallaby_managed_target`)
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

	flowID := "postgres-managed-e2e-" + uuid.NewString()
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	created, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: connector.Spec{Type: connector.EndpointPostgres}, Destinations: []connector.Spec{{Type: connector.EndpointPostgres}}, Config: flow.Config{AckPolicy: stream.AckPolicyAll}})
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
	publicationRevision, err := pgsource.PublicationFingerprint(ctx, pool, "wallaby_managed_e2e_publication")
	if err != nil {
		t.Fatal(err)
	}
	created.Source = connector.Spec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "slot": slotName, "publication": "wallaby_managed_e2e_publication",
		"ensure_publication": "false", "sync_publication": "false", "create_slot": "false",
		"managed": "true", "bootstrap": "never",
		"status_interval": "10ms", "batch_timeout": "10ms", "ensure_state": "false",
		"source_system_identifier": sourceSystemID, "source_lineage_id": sourceSystemID + ":wallaby_managed_e2e_publication:v1",
		"publication_revision": publicationRevision,
	}}
	destinationRevisionID := "postgres-managed-e2e-" + uuid.NewString()
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM destination_revisions WHERE destination_revision_id=$1", destinationRevisionID)
	}()
	created.Destinations = []connector.Spec{{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "schema": "public", "table": "wallaby_managed_target",
		"write_mode": "target", "batch_mode": "target", "meta_table_enabled": "false",
		"synchronous_commit": "on", "destination_revision_id": destinationRevisionID,
	}}}
	started, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	started.Source = created.Source
	started.Destinations = created.Destinations
	started.Config.AckPolicy = stream.AckPolicyAll

	var provisionedSlot, provisionedLSN string
	if err := pool.QueryRow(ctx, `SELECT slot_name,lsn::text FROM pg_catalog.pg_create_logical_replication_slot($1,'pgoutput')`, slotName).Scan(&provisionedSlot, &provisionedLSN); err != nil {
		t.Fatal(err)
	}
	if provisionedSlot != slotName || provisionedLSN == "" {
		t.Fatalf("provisioned slot=(%q,%q), want exact slot and cut", provisionedSlot, provisionedLSN)
	}
	seedFence, err := authorityStore.AcquireProducer(ctx, flowID, "managed-e2e-seed", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.AuthorizeAck(ctx, seedFence, connector.Checkpoint{LSN: provisionedLSN}); err != nil {
		t.Fatal(err)
	}
	if err := authorityStore.FinishProducer(ctx, seedFence, "checkpoint_seeded"); err != nil {
		t.Fatal(err)
	}

	runCtx, stopRun := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		flowRunner := runner.FlowRunner{
			Engine: engine, Checkpoints: checkpoints, Authority: authorityStore, Deliveries: coordinator,
			ExpectedGeneration: control.Generation, ExecutionID: "managed-e2e", ExecutionBackend: "test",
		}
		runErr <- flowRunner.Run(runCtx, started, &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}, []stream.DestinationConfig{{Spec: started.Destinations[0], Dest: &pgdest.Destination{}}})
	}()

	waitForCondition(t, ctx, runErr, "managed replication slot activation", func() (bool, error) {
		var active bool
		err := pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&active)
		return active, err
	})
	oldPayload := externalToastPayload("old")
	if _, err := pool.Exec(ctx, `INSERT INTO public.wallaby_managed_source(id,value,payload) VALUES (1,'managed',$1)`, oldPayload); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, ctx, runErr, "managed target row", func() (bool, error) {
		var value, payload string
		err := pool.QueryRow(ctx, `SELECT value,payload FROM public.wallaby_managed_target WHERE id=1`).Scan(&value, &payload)
		return value == "managed" && payload == oldPayload, err
	})

	var checkpointLSN string
	waitForCondition(t, ctx, runErr, "managed checkpoint and source ACK receipt", func() (bool, error) {
		var receipts int
		err := pool.QueryRow(ctx, `
SELECT checkpoint.lsn,(
  SELECT count(*) FROM source_ack_receipts AS receipt
  WHERE receipt.flow_incarnation_id=checkpoint.flow_incarnation_id
    AND receipt.checkpoint_lsn=checkpoint.lsn
)
FROM authoritative_checkpoints AS checkpoint
WHERE checkpoint.flow_incarnation_id=$1`, incarnationID).Scan(&checkpointLSN, &receipts)
		return receipts == 1 && checkpointLSN != "", err
	})
	checkpointPosition, err := pglogrepl.ParseLSN(checkpointLSN)
	if err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, ctx, runErr, "slot confirmed flush does not exceed checkpoint", func() (bool, error) {
		var confirmed string
		err := pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&confirmed)
		if err != nil || confirmed == "" {
			return false, err
		}
		confirmedPosition, err := pglogrepl.ParseLSN(confirmed)
		if err != nil {
			return false, err
		}
		if confirmedPosition > checkpointPosition {
			return false, errors.New("slot confirmed_flush_lsn exceeded committed checkpoint")
		}
		return confirmedPosition == checkpointPosition, nil
	})

	newPayload := externalToastPayload("new")
	sourceTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceTx.Exec(ctx, `UPDATE public.wallaby_managed_source SET payload=$1 WHERE id=1`, newPayload); err != nil {
		_ = sourceTx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := sourceTx.Exec(ctx, `UPDATE public.wallaby_managed_source SET counter=counter+1 WHERE id=1`); err != nil {
		_ = sourceTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := sourceTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, ctx, runErr, "same-transaction TOAST and counter updates", func() (bool, error) {
		var payload string
		var counter int64
		err := pool.QueryRow(ctx, `SELECT payload,counter FROM public.wallaby_managed_target WHERE id=1`).Scan(&payload, &counter)
		return payload == newPayload && counter == 1, err
	})
	var updatedCheckpointLSN string
	waitForCondition(t, ctx, runErr, "same-transaction updates checkpoint and ACK", func() (bool, error) {
		var receipts int
		err := pool.QueryRow(ctx, `
SELECT checkpoint.lsn,(
  SELECT count(*) FROM source_ack_receipts AS receipt
  WHERE receipt.flow_incarnation_id=checkpoint.flow_incarnation_id
    AND receipt.checkpoint_lsn=checkpoint.lsn
)
FROM authoritative_checkpoints AS checkpoint
WHERE checkpoint.flow_incarnation_id=$1`, incarnationID).Scan(&updatedCheckpointLSN, &receipts)
		return receipts == 1 && updatedCheckpointLSN != "" && updatedCheckpointLSN != checkpointLSN, err
	})

	stopRun()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("managed runner did not stop: %v", ctx.Err())
	}

	restartCtx, stopRestart := context.WithCancel(ctx)
	restartErr := make(chan error, 1)
	go func() {
		flowRunner := runner.FlowRunner{
			Engine: engine, Checkpoints: checkpoints, Authority: authorityStore, Deliveries: coordinator,
			ExpectedGeneration: control.Generation, ExecutionID: "managed-e2e-restart", ExecutionBackend: "test",
		}
		restartErr <- flowRunner.Run(restartCtx, started, &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}, []stream.DestinationConfig{{Spec: started.Destinations[0], Dest: &pgdest.Destination{}}})
	}()
	waitForCondition(t, ctx, restartErr, "managed replication slot reactivation from authoritative checkpoint", func() (bool, error) {
		var active bool
		err := pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&active)
		return active, err
	})
	if _, err := pool.Exec(ctx, `INSERT INTO public.wallaby_managed_source VALUES (2,'restarted')`); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, ctx, restartErr, "managed target row after restart", func() (bool, error) {
		var value string
		err := pool.QueryRow(ctx, `SELECT value FROM public.wallaby_managed_target WHERE id=2`).Scan(&value)
		return value == "restarted", err
	})
	stopRestart()
	select {
	case err := <-restartErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("restarted managed runner did not stop: %v", ctx.Err())
	}
}

func externalToastPayload(prefix string) string {
	var payload strings.Builder
	for range 512 {
		payload.WriteString(prefix)
		payload.WriteByte('-')
		payload.WriteString(uuid.NewString())
	}
	return payload.String()
}

func waitForCondition(t *testing.T, ctx context.Context, runErr <-chan error, description string, check func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := check()
		if ok {
			return
		}
		if err != nil && !strings.Contains(err.Error(), "no rows") {
			t.Fatalf("%s: %v", description, err)
		}
		select {
		case err := <-runErr:
			t.Fatalf("%s: managed runner stopped early: %v", description, err)
		case <-ctx.Done():
			t.Fatalf("%s: %v", description, ctx.Err())
		case <-ticker.C:
		}
	}
}
