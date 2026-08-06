package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestManagedBootstrapSnapshotBatchCommitBeforeReceiptRecovery(t *testing.T) {
	runManagedBootstrapBoundaryRecovery(t, "snapshot_batch")
}

func TestManagedBootstrapPublicationReceiptBeforeHandoffRecovery(t *testing.T) {
	runManagedBootstrapBoundaryRecovery(t, "publication_receipt")
}

func TestManagedBootstrapHandoffBeforeCDCOpenRecovery(t *testing.T) {
	runManagedBootstrapBoundaryRecovery(t, "handoff")
}

func runManagedBootstrapBoundaryRecovery(t *testing.T, boundary string) {
	t.Helper()
	ctx, dsn, engine, pool, authorityStore := setupBootstrapControl(t)
	defer engine.Close()
	defer pool.Close()
	suffix := time.Now().UnixNano()
	flowID := fmt.Sprintf("managed-boundary-%s-%d", boundary, suffix)
	sourceTable := fmt.Sprintf("wallaby_boundary_%d", suffix)
	targetSchema := fmt.Sprintf("wallaby_boundary_target_%d", suffix)
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	defer cleanupBootstrapSlotsForFlow(t, pool, flowID)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quoteTestIdentifier(targetSchema)+" CASCADE")
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS public."+quoteTestIdentifier(sourceTable))
	}()
	if _, err := pool.Exec(ctx, "CREATE TABLE public."+quoteTestIdentifier(sourceTable)+"(id bigint PRIMARY KEY,value text NOT NULL); INSERT INTO public."+quoteTestIdentifier(sourceTable)+" VALUES(1,'known-pre-cut'); CREATE SCHEMA "+quoteTestIdentifier(targetSchema)+"; CREATE TABLE "+quoteTestIdentifier(targetSchema)+"."+quoteTestIdentifier(sourceTable)+"(id bigint PRIMARY KEY,value text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	var systemID string
	if err := pool.QueryRow(ctx, `SELECT system_identifier::text FROM pg_catalog.pg_control_system()`).Scan(&systemID); err != nil {
		t.Fatal(err)
	}
	destinationRevisionID := "boundary-postgres-" + flowID
	flowDef := flow.Flow{
		ID: flowID,
		Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{
			"dsn": dsn, "managed": "true", "bootstrap": "required", "ensure_publication": "true", "ensure_state": "true",
			"tables": "public." + sourceTable, "snapshot_workers": "1", "batch_size": "100", "batch_timeout": "20ms", "status_interval": "20ms",
			"source_system_identifier": systemID, "source_lineage_id": "boundary-lineage-v1", "publication_revision": "bootstrap-pending",
		}},
		Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{"dsn": dsn, "batch_mode": "target", "destination_revision_id": destinationRevisionID, "synchronous_commit": "on", "meta_table_enabled": "false"}}},
		Config:       flow.Config{AckPolicy: stream.AckPolicyAll, TableMappings: flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{Destination: "target", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude}, Tables: []flow.TableMapping{{SourceSchema: "public", SourceTable: sourceTable, Action: flow.MappingActionInclude, TargetSchema: targetSchema, TargetTable: sourceTable, FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{{ .Column }}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}}}}}}}},
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

	cutReady := make(chan struct{})
	continueBootstrap := make(chan struct{})
	var injected atomic.Bool
	injectedErr := fmt.Errorf("%s: %w", boundary, connector.ErrDeliveryIndeterminate)
	hooks := bootstrap.Hooks{
		AfterPersisted: func(hookCtx context.Context, _ bootstrap.ExportedSnapshot) error {
			close(cutReady)
			select {
			case <-hookCtx.Done():
				return hookCtx.Err()
			case <-continueBootstrap:
				return nil
			}
		},
	}
	inject := func() error {
		if injected.CompareAndSwap(false, true) {
			return injectedErr
		}
		return nil
	}
	switch boundary {
	case "snapshot_batch":
		hooks.AfterSnapshotBatchApply = func(context.Context, bootstrap.ExportedSnapshot, bootstrap.SnapshotTask, int64) error {
			return inject()
		}
	case "publication_receipt":
		hooks.AfterPublicationReceipt = func(context.Context, bootstrap.ExportedSnapshot) error { return inject() }
	case "handoff":
		hooks.AfterHandoff = func(context.Context, bootstrap.ExportedSnapshot) error { return inject() }
	default:
		t.Fatalf("unknown boundary %q", boundary)
	}

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- (&runner.FlowRunner{
			Engine: engine, Checkpoints: checkpoints, DDLPolicyDefaults: noAutomaticDDLDefaults(), ExpectedGeneration: control.Generation,
			ExecutionBackend: "integration", ExecutionID: "boundary-first-" + boundary,
			Authority: authorityStore, Deliveries: coordinator, SchemaBaselines: mustManagedSchemaBaselines(t, pool),
		}).Run(ctx, flowDef, &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore, BootstrapHooks: hooks}, []stream.DestinationConfig{{Spec: flowDef.Destinations[0], Dest: &pgdest.Destination{}}})
	}()
	select {
	case <-cutReady:
	case err := <-firstErr:
		t.Fatalf("runner exited before exported cut: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	insertPostCut := func() {
		t.Helper()
		if _, err := pool.Exec(ctx, "INSERT INTO public."+quoteTestIdentifier(sourceTable)+" VALUES(2,'known-post-cut')"); err != nil {
			t.Fatal(err)
		}
	}
	if boundary != "snapshot_batch" {
		insertPostCut()
	}
	close(continueBootstrap)
	select {
	case err := <-firstErr:
		if !errors.Is(err, injectedErr) {
			t.Fatalf("first runner error=%v, want injected boundary", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("first runner did not stop at deterministic boundary")
	}
	if !injected.Load() {
		t.Fatal("deterministic crash hook was not reached")
	}

	replacementCtx, cancelReplacement := context.WithCancel(ctx)
	defer cancelReplacement()
	replacementErr := make(chan error, 1)
	replacementSource := &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}
	var replacementCut, continueReplacement chan struct{}
	if boundary == "snapshot_batch" {
		replacementCut = make(chan struct{})
		continueReplacement = make(chan struct{})
		replacementSource.BootstrapHooks.AfterPersisted = func(hookCtx context.Context, _ bootstrap.ExportedSnapshot) error {
			close(replacementCut)
			select {
			case <-hookCtx.Done():
				return hookCtx.Err()
			case <-continueReplacement:
				return nil
			}
		}
	}
	go func() {
		replacementErr <- (&runner.FlowRunner{
			Engine: engine, Checkpoints: checkpoints, DDLPolicyDefaults: noAutomaticDDLDefaults(), ExpectedGeneration: control.Generation,
			ExecutionBackend: "integration", ExecutionID: "boundary-replacement-" + boundary,
			Authority: authorityStore, Deliveries: coordinator, SchemaBaselines: mustManagedSchemaBaselines(t, pool),
		}).Run(replacementCtx, flowDef, replacementSource, []stream.DestinationConfig{{Spec: flowDef.Destinations[0], Dest: &pgdest.Destination{}}})
	}()
	if boundary == "snapshot_batch" {
		select {
		case <-replacementCut:
			insertPostCut()
			close(continueReplacement)
		case runErr := <-replacementErr:
			t.Fatalf("replacement exited before its exported cut: %v", runErr)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	deadline := time.Now().Add(25 * time.Second)
	for {
		var values string
		err := pool.QueryRow(ctx, "SELECT COALESCE(string_agg(value,',' ORDER BY id),'') FROM "+quoteTestIdentifier(targetSchema)+"."+quoteTestIdentifier(sourceTable)).Scan(&values)
		var streamingBootstraps, cdcManifests int
		if err == nil {
			err = pool.QueryRow(ctx, `
SELECT
 (SELECT count(*) FROM source_bootstraps WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) AND phase='streaming'),
 (SELECT count(*) FROM delivery_manifests manifest
   JOIN delivery_receipts receipt USING(flow_incarnation_id,destination_revision_id,position_id)
   JOIN delivery_attempts attempt ON attempt.attempt_id=receipt.attempt_id
   JOIN delivery_attempt_evidence evidence ON evidence.attempt_id=attempt.attempt_id
   WHERE manifest.flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)
     AND manifest.content_hash=receipt.content_hash
     AND manifest.content_hash=attempt.content_hash
     AND manifest.content_hash=evidence.content_hash)`, flowID).Scan(&streamingBootstraps, &cdcManifests)
		}
		if err == nil && values == "known-pre-cut,known-post-cut" && streamingBootstraps == 1 && cdcManifests >= 1 {
			break
		}
		select {
		case runErr := <-replacementErr:
			t.Fatalf("replacement exited before convergence: %v", runErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("boundary recovery did not converge: values=%q err=%v", values, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancelReplacement()
	select {
	case err := <-replacementErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("replacement exit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replacement did not stop")
	}

	var bootstrapID, cutLSN, phase, manifestHash string
	if err := pool.QueryRow(ctx, `
SELECT bootstrap_id::text,consistent_lsn,phase,manifest_hash
FROM source_bootstraps
WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)
  AND phase='streaming'
ORDER BY bootstrap_generation DESC LIMIT 1`, flowID).Scan(&bootstrapID, &cutLSN, &phase, &manifestHash); err != nil {
		t.Fatal(err)
	}
	if phase != "streaming" || bootstrapID == "" || manifestHash == "" {
		t.Fatalf("bootstrap audit id=%q phase=%q manifest=%q", bootstrapID, phase, manifestHash)
	}
	var snapshotAudit int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM snapshot_delivery_receipts receipt
JOIN snapshot_delivery_attempts attempt USING(attempt_id)
JOIN snapshot_delivery_evidence evidence USING(attempt_id)
WHERE receipt.bootstrap_id=$1::uuid
  AND receipt.position_id LIKE 'bootstrap/'||$1||'/%'
  AND receipt.content_hash=attempt.content_hash
  AND receipt.content_hash=evidence.content_hash
  AND receipt.logical_batch_id=attempt.logical_batch_id
  AND receipt.logical_batch_id=evidence.logical_batch_id
  AND receipt.logical_batch_id LIKE 'logical-batch:%'
  AND receipt.durable_cursor #>> '{keys,0,name}'='id'
  AND receipt.durable_cursor #>> '{keys,0,value}'='1'`, bootstrapID).Scan(&snapshotAudit); err != nil {
		t.Fatal(err)
	}
	if snapshotAudit != 1 {
		t.Fatalf("known pre-cut row snapshot audit receipts=%d, want 1", snapshotAudit)
	}
	var publicationAudit int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM snapshot_publication_receipts
WHERE bootstrap_id=$1::uuid AND content_hash=$2
  AND destination_revision_id=$3 AND authority_origin='fenced'`, bootstrapID, manifestHash, destinationRevisionID).Scan(&publicationAudit); err != nil {
		t.Fatal(err)
	}
	if publicationAudit != 1 {
		t.Fatalf("publication receipt audit=%d, want 1", publicationAudit)
	}
	var cdcAudit int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM delivery_manifests manifest
JOIN delivery_receipts receipt USING(flow_incarnation_id,destination_revision_id,position_id)
JOIN delivery_attempts attempt ON attempt.attempt_id=receipt.attempt_id
JOIN delivery_attempt_evidence evidence ON evidence.attempt_id=attempt.attempt_id
WHERE manifest.flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)
  AND manifest.checkpoint_lsn::pg_lsn>$2::pg_lsn
  AND manifest.source_transaction_id<>''
  AND manifest.content_hash=receipt.content_hash
  AND manifest.content_hash=attempt.content_hash
  AND manifest.content_hash=evidence.content_hash`, flowID, cutLSN).Scan(&cdcAudit); err != nil {
		t.Fatal(err)
	}
	if cdcAudit != 1 {
		t.Fatalf("known post-cut transaction CDC audit receipts=%d, want 1", cdcAudit)
	}
	var cutCheckpoint, cutAck int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) AND lsn::pg_lsn>=$2::pg_lsn`, flowID, cutLSN).Scan(&cutCheckpoint); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) AND checkpoint_lsn::pg_lsn=$2::pg_lsn`, flowID, cutLSN).Scan(&cutAck); err != nil {
		t.Fatal(err)
	}
	if cutCheckpoint != 1 || cutAck != 1 {
		t.Fatalf("handoff cut audit checkpoint=%d cut_ack=%d", cutCheckpoint, cutAck)
	}
}

func quoteTestIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
