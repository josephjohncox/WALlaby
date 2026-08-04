package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

type abandonmentRecordingPostgresDestination struct {
	*pgdest.Destination
	abandonCalls atomic.Int32
}

func (d *abandonmentRecordingPostgresDestination) AbandonBootstrap(ctx context.Context, intent connector.BootstrapIntent, tables []connector.BootstrapTable) error {
	d.abandonCalls.Add(1)
	return d.Destination.AbandonBootstrap(ctx, intent, tables)
}

func TestManagedBootstrapWorkerWiringConcurrentBoundary(t *testing.T) {
	t.Run("generated_column_snapshot_to_cdc", runManagedBootstrapWorkerWiringConcurrentBoundary)
}

func TestPostgresManagedProfileSourceSchemaEvolutionAfterRestart(t *testing.T) {
	runManagedBootstrapWorkerWiringConcurrentBoundary(t)
}

// describeBootstrapWiringShape renders the live source and destination column
// shapes for a convergence failure. A missing destination column cannot be
// diagnosed from the assertion alone, and these tests only run against live
// PostgreSQL in CI, so the failure itself has to carry the evidence.
func describeBootstrapWiringShape(ctx context.Context, pool *pgxpool.Pool) string {
	var report strings.Builder
	for _, target := range []struct{ schema, table string }{
		{schema: "public", table: "wallaby_bootstrap_wiring_a"},
		{schema: "wallaby_bootstrap_target", table: "wallaby_bootstrap_wiring_a"},
	} {
		report.WriteString(fmt.Sprintf("shape %s.%s: ", target.schema, target.table))
		rows, err := pool.Query(ctx, `
SELECT column_name, data_type, is_generated, COALESCE(generation_expression,'')
FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position`,
			target.schema, target.table)
		if err != nil {
			report.WriteString(fmt.Sprintf("query error: %v\n", err))
			continue
		}
		columns := 0
		for rows.Next() {
			var name, dataType, generated, expression string
			if scanErr := rows.Scan(&name, &dataType, &generated, &expression); scanErr != nil {
				report.WriteString(fmt.Sprintf("scan error: %v ", scanErr))
				break
			}
			columns++
			report.WriteString(fmt.Sprintf("[%s %s generated=%s %q] ", name, dataType, generated, expression))
		}
		rows.Close()
		if rowsErr := rows.Err(); rowsErr != nil {
			report.WriteString(fmt.Sprintf("iterate error: %v ", rowsErr))
		}
		if columns == 0 {
			report.WriteString("(no columns; table absent)")
		}
		report.WriteString("\n")
	}
	return report.String()
}

func runManagedBootstrapWorkerWiringConcurrentBoundary(t *testing.T) {
	ctx, dsn, engine, pool, authorityStore := setupBootstrapControl(t)
	defer engine.Close()
	defer pool.Close()
	flowID := fmt.Sprintf("managed-bootstrap-wiring-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(context.Background(), pool, flowID)

	if _, err := pool.Exec(ctx, `
DROP SCHEMA IF EXISTS wallaby_bootstrap_target CASCADE;
DROP TABLE IF EXISTS public.wallaby_bootstrap_wiring_a;
DROP TABLE IF EXISTS public.wallaby_bootstrap_wiring_b;
CREATE TABLE public.wallaby_bootstrap_wiring_a(
  id bigint PRIMARY KEY,
  value text NOT NULL,
  rendered text GENERATED ALWAYS AS (value || '-generated') STORED
);
CREATE TABLE public.wallaby_bootstrap_wiring_b(id bigint PRIMARY KEY,value text NOT NULL);
INSERT INTO public.wallaby_bootstrap_wiring_a(id,value) VALUES(1,'snapshot');
INSERT INTO public.wallaby_bootstrap_wiring_b VALUES(10,'second-table');
CREATE SCHEMA wallaby_bootstrap_target;
CREATE TABLE wallaby_bootstrap_target.wallaby_bootstrap_wiring_a(LIKE public.wallaby_bootstrap_wiring_a INCLUDING ALL);
CREATE TABLE wallaby_bootstrap_target.wallaby_bootstrap_wiring_b(LIKE public.wallaby_bootstrap_wiring_b INCLUDING ALL)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `
DROP SCHEMA IF EXISTS wallaby_bootstrap_target CASCADE;
DROP TABLE IF EXISTS public.wallaby_bootstrap_wiring_a;
DROP TABLE IF EXISTS public.wallaby_bootstrap_wiring_b`)
	}()

	var systemID string
	if err := pool.QueryRow(ctx, `SELECT system_identifier::text FROM pg_catalog.pg_control_system()`).Scan(&systemID); err != nil {
		t.Fatal(err)
	}
	destinationRevisionID := "wiring-postgres-" + flowID
	flowDef := flow.Flow{
		ID: flowID,
		Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{
			"dsn": dsn, "managed": "true", "bootstrap": "required",
			"managed_profile": connector.ManagedProfilePostgresToPostgresV1, "streaming_transactions": "true",
			"ensure_publication": "true", "ensure_state": "true",
			"tables":           "public.wallaby_bootstrap_wiring_a,public.wallaby_bootstrap_wiring_b",
			"snapshot_workers": "2", "batch_size": "1", "batch_timeout": "20ms", "status_interval": "20ms",
			"source_system_identifier": systemID, "source_lineage_id": "wiring-lineage-v1", "publication_revision": "bootstrap-pending",
		}},
		Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{
			"dsn": dsn, "schema": "wallaby_bootstrap_target", "batch_mode": "target",
			"managed_profile":         connector.ManagedProfilePostgresToPostgresV1,
			"destination_revision_id": destinationRevisionID, "synchronous_commit": "on", "meta_table_enabled": "false",
		}}},
		Config: flow.Config{AckPolicy: stream.AckPolicyAll},
	}
	if _, err := engine.Create(ctx, flowDef); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, err := checkpoint.NewPostgresStoreWithPool(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}

	slotPersisted := make(chan struct{})
	continueBootstrap := make(chan struct{})
	var exporterAttempts atomic.Int32
	var publicationCrashes atomic.Int32
	source := &pgsource.Source{
		ManagedControl: pool, ManagedAuthority: authorityStore,
		BootstrapHooks: bootstrap.Hooks{
			AfterPersisted: func(ctx context.Context, _ bootstrap.ExportedSnapshot) error {
				if exporterAttempts.Add(1) == 1 {
					return errors.New("injected exporter loss")
				}
				close(slotPersisted)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-continueBootstrap:
					return nil
				}
			},
			AfterPublication: func(context.Context, bootstrap.ExportedSnapshot) error {
				if publicationCrashes.Add(1) == 1 {
					return connector.ErrDeliveryIndeterminate
				}
				return nil
			},
		},
	}
	destination := &abandonmentRecordingPostgresDestination{Destination: &pgdest.Destination{}}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- (&runner.FlowRunner{
			Engine: engine, Checkpoints: checkpoints, ExpectedGeneration: control.Generation,
			ExecutionBackend: "integration", ExecutionID: "managed-bootstrap-wiring",
			Authority: authorityStore, Deliveries: coordinator,
		}).Run(runCtx, flowDef, source, []stream.DestinationConfig{{Spec: flowDef.Destinations[0], Dest: destination}})
	}()

	select {
	case <-slotPersisted:
	case err := <-errCh:
		t.Fatalf("runner exited before bootstrap boundary: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := pool.Exec(ctx, `
UPDATE public.wallaby_bootstrap_wiring_a SET value='stream' WHERE id=1;
INSERT INTO public.wallaby_bootstrap_wiring_a(id,value) VALUES(2,'after-cut')`); err != nil {
		t.Fatal(err)
	}
	close(continueBootstrap)

	select {
	case err := <-errCh:
		if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
			t.Fatalf("publish-before-control crash error=%v, want indeterminate", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first runner did not stop at publish-before-control crash boundary")
	}
	if calls := destination.abandonCalls.Load(); calls != 0 {
		t.Fatalf("destination abandonment calls after publication=%d, want zero", calls)
	}
	replacementSource := &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}
	replacementCtx, cancelReplacement := context.WithCancel(ctx)
	defer cancelReplacement()
	errCh = make(chan error, 1)
	go func() {
		errCh <- (&runner.FlowRunner{
			Engine: engine, Checkpoints: checkpoints, ExpectedGeneration: control.Generation,
			ExecutionBackend: "integration", ExecutionID: "managed-bootstrap-publication-replacement",
			Authority: authorityStore, Deliveries: coordinator,
		}).Run(replacementCtx, flowDef, replacementSource, []stream.DestinationConfig{{Spec: flowDef.Destinations[0], Dest: &pgdest.Destination{}}})
	}()

	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case err := <-errCh:
			t.Fatalf("managed runner exited before convergence: %v", err)
		default:
		}
		var count int
		var values, rendered string
		err := pool.QueryRow(ctx, `
SELECT count(*),COALESCE(string_agg(value,',' ORDER BY id),''),COALESCE(string_agg(rendered,',' ORDER BY id),'')
FROM wallaby_bootstrap_target.wallaby_bootstrap_wiring_a`).Scan(&count, &values, &rendered)
		if err == nil && count == 2 && values == "stream,after-cut" && rendered == "stream-generated,after-cut-generated" {
			var second string
			if err := pool.QueryRow(ctx, `SELECT value FROM wallaby_bootstrap_target.wallaby_bootstrap_wiring_b WHERE id=10`).Scan(&second); err == nil && second == "second-table" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("managed bootstrap/CDC boundary did not converge: count=%d values=%q rendered=%q err=%v\n%s", count, values, rendered, err, describeBootstrapWiringShape(ctx, pool))
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Destination convergence can become visible before the fenced checkpoint
	// transaction commits. Wait for the authoritative cursor itself to cross the
	// bootstrap cut before pausing, otherwise repetition can race the worker at
	// exactly the boundary this test is intended to prove.
	var cutLSN, advancedCheckpoint string
	if err := pool.QueryRow(ctx, `SELECT consistent_lsn FROM source_bootstraps WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) ORDER BY bootstrap_generation DESC LIMIT 1`, flowID).Scan(&cutLSN); err != nil {
		t.Fatal(err)
	}
	cut, err := pglogrepl.ParseLSN(cutLSN)
	if err != nil {
		t.Fatal(err)
	}
	waitForCheckpointReceipt := func(minimum pglogrepl.LSN, deadline time.Time, label string) string {
		t.Helper()
		for {
			var checkpointLSN string
			var flushRecorded bool
			err := pool.QueryRow(ctx, `
SELECT checkpoint.lsn,
       EXISTS(
         SELECT 1 FROM source_ack_receipts AS receipt
         WHERE receipt.flow_incarnation_id=checkpoint.flow_incarnation_id
           AND receipt.checkpoint_lsn=checkpoint.lsn
           AND receipt.observed_flush_lsn IS NOT NULL
       )
FROM authoritative_checkpoints AS checkpoint
WHERE checkpoint.flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&checkpointLSN, &flushRecorded)
			if err == nil {
				checkpointPosition, parseErr := pglogrepl.ParseLSN(checkpointLSN)
				if parseErr != nil {
					t.Fatal(parseErr)
				}
				if checkpointPosition > minimum && flushRecorded {
					return checkpointLSN
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s checkpoint %s did not advance beyond %s with a source flush receipt: %v", label, checkpointLSN, minimum, err)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	advancedCheckpoint = waitForCheckpointReceipt(cut, time.Now().Add(10*time.Second), "pre-pause")

	_, pauseControl, err := engine.RequestPause(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	cancelReplacement()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("managed runner exit during pause: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("managed runner did not quiesce for pause")
	}
	paused, err := engine.CompletePause(ctx, flowID, pauseControl.Generation)
	if err != nil || paused.State != flow.StatePaused {
		t.Fatalf("complete managed pause=(%s,%v)", paused.State, err)
	}

	var phase, slot, publication, bootstrapID, slotOwnership, publicationOwnership string
	if err := pool.QueryRow(ctx, `SELECT phase,slot_name,publication_name,consistent_lsn,bootstrap_id::text FROM source_bootstraps WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) ORDER BY bootstrap_generation DESC LIMIT 1`, flowID).Scan(&phase, &slot, &publication, &cutLSN, &bootstrapID); err != nil {
		t.Fatal(err)
	}
	if phase != "streaming" {
		t.Fatalf("bootstrap phase=%q, want streaming", phase)
	}
	var abandoned, streaming int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER(WHERE phase='abandoned'),count(*) FILTER(WHERE phase='streaming')
FROM source_bootstraps WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&abandoned, &streaming); err != nil {
		t.Fatal(err)
	}
	if abandoned != 1 || streaming != 1 || exporterAttempts.Load() != 2 || publicationCrashes.Load() != 1 {
		t.Fatalf("bootstrap generations abandoned=%d streaming=%d exporter attempts=%d publication crashes=%d, want 1/1/2/1", abandoned, streaming, exporterAttempts.Load(), publicationCrashes.Load())
	}
	var publicationMarkers, publicationReceipts, snapshotReceipts, postCutCDCReceipts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallaby.managed_bootstrap_publications WHERE bootstrap_id=$1`, bootstrapID).Scan(&publicationMarkers); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM snapshot_publication_receipts WHERE bootstrap_id=$1`, bootstrapID).Scan(&publicationReceipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM snapshot_delivery_receipts WHERE bootstrap_id=$1`, bootstrapID).Scan(&snapshotReceipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_manifests WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) AND checkpoint_lsn::pg_lsn>$2::pg_lsn`, flowID, cutLSN).Scan(&postCutCDCReceipts); err != nil {
		t.Fatal(err)
	}
	if publicationMarkers != 1 || publicationReceipts != 1 || snapshotReceipts < 2 || postCutCDCReceipts < 1 {
		t.Fatalf("receipt audit marker=%d publication=%d snapshot=%d post-cut-cdc=%d", publicationMarkers, publicationReceipts, snapshotReceipts, postCutCDCReceipts)
	}

	if err := pool.QueryRow(ctx, `SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&advancedCheckpoint); err != nil {
		t.Fatal(err)
	}
	advanced, err := pglogrepl.ParseLSN(advancedCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	if advanced <= cut {
		t.Fatalf("authoritative checkpoint %s did not advance beyond bootstrap cut %s before pause", advanced, cut)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO public.wallaby_bootstrap_wiring_a(id,value) VALUES(3,'resumed')`); err != nil {
		t.Fatal(err)
	}
	_, resumeControl, err := engine.PlanStart(ctx, flowID, true)
	if err != nil {
		t.Fatal(err)
	}
	if resumeControl.Generation != pauseControl.Generation+1 {
		t.Fatalf("resume generation=%d, want %d", resumeControl.Generation, pauseControl.Generation+1)
	}
	resumedSource := &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}
	resumedCtx, cancelResumed := context.WithCancel(ctx)
	resumedErrCh := make(chan error, 1)
	go func() {
		resumedErrCh <- (&runner.FlowRunner{
			Engine: engine, Checkpoints: checkpoints, ExpectedGeneration: resumeControl.Generation,
			ExecutionBackend: "integration", ExecutionID: "managed-bootstrap-resumed",
			Authority: authorityStore, Deliveries: coordinator,
		}).Run(resumedCtx, flowDef, resumedSource, []stream.DestinationConfig{{Spec: flowDef.Destinations[0], Dest: &pgdest.Destination{}}})
	}()
	resumeDeadline := time.Now().Add(20 * time.Second)
	for {
		var resumedValue, resumedRendered string
		err := pool.QueryRow(ctx, `SELECT value,rendered FROM wallaby_bootstrap_target.wallaby_bootstrap_wiring_a WHERE id=3`).Scan(&resumedValue, &resumedRendered)
		if err == nil && resumedValue == "resumed" && resumedRendered == "resumed-generated" {
			break
		}
		select {
		case runErr := <-resumedErrCh:
			t.Fatalf("resumed runner exited before CDC convergence: %v", runErr)
		default:
		}
		if time.Now().After(resumeDeadline) {
			t.Fatalf("generation+1 did not continue CDC: value=%q rendered=%q err=%v", resumedValue, resumedRendered, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	resumedInitial, ok := resumedSource.InitialCheckpoint()
	if !ok || resumedInitial.LSN != advancedCheckpoint {
		t.Fatalf("resumed source initial checkpoint=(%s,%t), want advanced authoritative %s", resumedInitial.LSN, ok, advancedCheckpoint)
	}
	if exporterAttempts.Load() != 2 {
		t.Fatalf("resume regressed into bootstrap: exporter attempts=%d", exporterAttempts.Load())
	}
	preEvolutionCheckpoint := waitForCheckpointReceipt(advanced, time.Now().Add(10*time.Second), "pre-schema-restart")
	cancelResumed()
	select {
	case err := <-resumedErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("resumed managed runner exit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resumed managed runner did not stop")
	}

	// Change the source schema while no decoder is running. The replacement's
	// first Relation message contains only the new shape, so successful target
	// evolution proves that the authoritative checkpoint schema baseline—not a
	// process-local cache—drives the diff after restart.
	if _, err := pool.Exec(ctx, `
ALTER TABLE public.wallaby_bootstrap_wiring_a ADD COLUMN note text;
UPDATE public.wallaby_bootstrap_wiring_a SET value='evolved-after-restart',note='durable-baseline' WHERE id=3`); err != nil {
		t.Fatal(err)
	}
	evolutionSource := &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}
	evolutionCtx, cancelEvolution := context.WithCancel(ctx)
	evolutionErrCh := make(chan error, 1)
	go func() {
		evolutionErrCh <- (&runner.FlowRunner{
			Engine: engine, Checkpoints: checkpoints, ExpectedGeneration: resumeControl.Generation,
			ExecutionBackend: "integration", ExecutionID: "managed-schema-evolution-restart",
			Authority: authorityStore, Deliveries: coordinator,
		}).Run(evolutionCtx, flowDef, evolutionSource, []stream.DestinationConfig{{Spec: flowDef.Destinations[0], Dest: &pgdest.Destination{}}})
	}()
	evolutionDeadline := time.Now().Add(20 * time.Second)
	for {
		var value, note, rendered string
		err := pool.QueryRow(ctx, `SELECT value,note,rendered FROM wallaby_bootstrap_target.wallaby_bootstrap_wiring_a WHERE id=3`).Scan(&value, &note, &rendered)
		if err == nil && value == "evolved-after-restart" && note == "durable-baseline" && rendered == "evolved-after-restart-generated" {
			break
		}
		select {
		case runErr := <-evolutionErrCh:
			t.Fatalf("restart schema-evolution runner exited before convergence: %v", runErr)
		default:
		}
		if time.Now().After(evolutionDeadline) {
			t.Fatalf("restart schema evolution did not converge: value=%q note=%q rendered=%q err=%v", value, note, rendered, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	initial, ok := evolutionSource.InitialCheckpoint()
	if !ok {
		t.Fatal("schema-evolution restart did not restore an authoritative checkpoint")
	}
	preEvolutionPosition, err := pglogrepl.ParseLSN(preEvolutionCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	initialPosition, err := pglogrepl.ParseLSN(initial.LSN)
	if err != nil {
		t.Fatal(err)
	}
	if initialPosition < preEvolutionPosition {
		t.Fatalf("schema-evolution restart checkpoint=%s, want at least %s", initial.LSN, preEvolutionCheckpoint)
	}
	for {
		var checkpointLSN, baselineJSON string
		var flushRecorded bool
		err := pool.QueryRow(ctx, `
SELECT checkpoint.lsn,
       COALESCE(checkpoint.metadata->>$2,''),
       EXISTS(
         SELECT 1 FROM source_ack_receipts AS receipt
         WHERE receipt.flow_incarnation_id=checkpoint.flow_incarnation_id
           AND receipt.checkpoint_lsn=checkpoint.lsn
           AND receipt.observed_flush_lsn IS NOT NULL
       )
FROM authoritative_checkpoints AS checkpoint
WHERE checkpoint.flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID, connector.ManagedSchemaBaselinesMetadataKey).Scan(&checkpointLSN, &baselineJSON, &flushRecorded)
		if err == nil {
			checkpointPosition, parseErr := pglogrepl.ParseLSN(checkpointLSN)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			if checkpointPosition > preEvolutionPosition && flushRecorded && strings.Contains(baselineJSON, "note") {
				break
			}
		}
		select {
		case runErr := <-evolutionErrCh:
			t.Fatalf("schema-evolution runner exited before checkpoint and source receipt: %v", runErr)
		default:
		}
		if time.Now().After(evolutionDeadline) {
			t.Fatalf("schema-evolution checkpoint/flush did not converge after target commit: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancelEvolution()
	select {
	case err := <-evolutionErrCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("schema-evolution managed runner exit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("schema-evolution managed runner did not stop")
	}

	if err := pool.QueryRow(ctx, `SELECT ownership FROM source_resources WHERE physical_name=$1 AND resource_kind='slot'`, slot).Scan(&slotOwnership); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT ownership FROM source_resources WHERE physical_name=$1 AND resource_kind='publication'`, publication).Scan(&publicationOwnership); err != nil {
		t.Fatal(err)
	}
	if slotOwnership != "owned" || publicationOwnership != "owned" {
		t.Fatalf("resource ownership slot=%s publication=%s", slotOwnership, publicationOwnership)
	}

	// Exercise the production lifecycle cleanup bridge rather than manually
	// dropping resources in test teardown. Cleanup runs after the runner has
	// quiesced and before stopped is published.
	injectedDrop := errors.New("injected immediate slot drop failure")
	failingCleanupFactory := runner.Factory{
		ManagedControl: pool, ManagedAuthority: authorityStore,
		BootstrapHooks: bootstrap.Hooks{DropSlot: func(context.Context, string) error { return injectedDrop }},
	}
	failingLifecycle := workflow.NewOrchestratedEngine(engine, workflow.PassiveDispatcher{}, nil, runner.ManagedSourceCleanup{
		Factory: failingCleanupFactory, Authority: authorityStore,
	})
	if _, err := failingLifecycle.Stop(ctx, flowID); !errors.Is(err, injectedDrop) {
		t.Fatalf("first production stop cleanup error=%v, want injected drop failure", err)
	}
	current, err := engine.Get(ctx, flowID)
	if err != nil || current.State != flow.StateStopping {
		t.Fatalf("flow after indeterminate cleanup=(%s,%v), want stopping", current.State, err)
	}
	var indeterminate int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resource_operations WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) AND resource_kind='slot' AND operation='drop' AND status='indeterminate'`, flowID).Scan(&indeterminate); err != nil {
		t.Fatal(err)
	}
	if indeterminate != 1 {
		t.Fatalf("discoverable indeterminate slot drops=%d, want 1", indeterminate)
	}

	cleanupFactory := runner.Factory{ManagedControl: pool, ManagedAuthority: authorityStore}
	lifecycle := workflow.NewOrchestratedEngine(engine, workflow.PassiveDispatcher{}, nil, runner.ManagedSourceCleanup{
		Factory: cleanupFactory, Authority: authorityStore,
	})
	stopped, err := lifecycle.Stop(ctx, flowID)
	if err != nil {
		t.Fatalf("retry production managed stop cleanup: %v", err)
	}
	if stopped.State != flow.StateStopped {
		t.Fatalf("managed stop state=%s, want stopped", stopped.State)
	}
	var slotExists, publicationExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)`, slot).Scan(&slotExists); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_publication WHERE pubname=$1)`, publication).Scan(&publicationExists); err != nil {
		t.Fatal(err)
	}
	if slotExists || publicationExists {
		t.Fatalf("stopped flow retained owned resources slot=%t publication=%t", slotExists, publicationExists)
	}
	var retired int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resources WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) AND ownership='owned' AND state='retired' AND physical_name IN ($2,$3)`, flowID, slot, publication).Scan(&retired); err != nil {
		t.Fatal(err)
	}
	if retired != 2 {
		t.Fatalf("retired owned resources=%d, want slot and publication", retired)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resource_operations WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) AND operation='drop' AND status='indeterminate'`, flowID).Scan(&indeterminate); err != nil {
		t.Fatal(err)
	}
	if indeterminate != 0 {
		t.Fatalf("retry left indeterminate terminal operations=%d", indeterminate)
	}
}
