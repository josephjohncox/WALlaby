package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedReceiptReconcilesLogicalAndPositionIdentities(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts`); err != nil {
		t.Fatal(err)
	}
	destination := &Destination{pool: pool}
	if err := destination.ensureManagedReceiptTable(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts`)
	}()

	incarnation := uuid.NewString()
	exact := managedReceiptTestIntent(t, incarnation, "position-one", "content-one")
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.insertManagedReceipt(ctx, tx, exact, postgresDeliveryMarkerID(exact)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("exact_match_adopts", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if hash, err := destination.loadManagedReceipt(ctx, tx, exact); err != nil || hash != exact.ContentHash {
			t.Fatalf("exact receipt hash=%q err=%v", hash, err)
		}
	})

	t.Run("position_key_conflict", func(t *testing.T) {
		conflict := managedReceiptTestIntent(t, incarnation, exact.PositionID, "different-content-and-policy")
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := destination.loadManagedReceipt(ctx, tx, conflict); !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("position conflict error=%v", err)
		}
	})

	t.Run("position_conflict_precedes_dml", func(t *testing.T) {
		table := "wallaby_receipt_conflict_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE public.%s(event_id bigint,payload text,__wallaby_operation text,__wallaby_deleted boolean,__wallaby_source_position text)`, quoteIdent(table))); err != nil {
			t.Fatal(err)
		}
		defer pool.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS public.%s`, quoteIdent(table)))
		schema := connector.Schema{Namespace: "public", Name: table, Columns: []connector.Column{{Name: "event_id", Type: "bigint"}, {Name: "payload", Type: "text"}, {Name: connector.AppendOperationColumn, Type: "text"}, {Name: connector.AppendDeletedColumn, Type: "boolean"}, {Name: connector.AppendSourcePositionColumn, Type: "text"}}}
		batch := connector.Batch{Schema: schema, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: "different-policy"}, Records: []connector.Record{{Table: table, Operation: connector.OpInsert, After: map[string]any{"event_id": int64(1), "payload": "must-not-apply", connector.AppendOperationColumn: "insert", connector.AppendDeletedColumn: false, connector.AppendSourcePositionColumn: "0/20"}, SourcePosition: "0/20"}}}
		hash, err := connector.BatchContentHash(batch)
		if err != nil {
			t.Fatal(err)
		}
		conflict := managedReceiptTestIntent(t, incarnation, exact.PositionID, hash)
		destination.syncCommit = "on"
		destination.batchMode = batchModeTarget
		if _, err := destination.Apply(ctx, conflict, batch); !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("pre-DML receipt conflict error=%v", err)
		}
		var rows int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM public.%s`, quoteIdent(table))).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatalf("conflicting delivery applied %d rows", rows)
		}
	})

	t.Run("logical_key_conflict", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `DELETE FROM wallaby_meta.__delivery_receipts; ALTER TABLE wallaby_meta.__delivery_receipts DROP CONSTRAINT wallaby_delivery_receipts_logical_batch_current`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO wallaby_meta.__delivery_receipts(marker_id,flow_id,flow_incarnation_id,generation,acquisition_id,lease_epoch,destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash) VALUES('corrupt-marker',$1,$2,1,'acquisition',1,$3,$4,$5,'other-position',$6)`, exact.FlowID, exact.FlowIncarnationID, exact.DestinationRevisionID, exact.SourceLineageID, exact.LogicalBatchID, exact.ContentHash); err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := destination.loadManagedReceipt(ctx, tx, exact); !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("logical conflict error=%v", err)
		}
	})
}

func TestManagedReceiptInsertUniqueViolationClassifiesConflictAndIndeterminate(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts`); err != nil {
		t.Fatal(err)
	}
	destination := &Destination{pool: pool}
	if err := destination.ensureManagedReceiptTable(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts`)
	}()

	incarnation := uuid.NewString()
	exact := managedReceiptTestIntent(t, incarnation, "position-one", "content-one")
	insert := func(t *testing.T, intent connector.DeliveryIntent) error {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		return destination.insertManagedReceipt(ctx, tx, intent, postgresDeliveryMarkerID(intent))
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.insertManagedReceipt(ctx, tx, exact, postgresDeliveryMarkerID(exact)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := insert(t, exact); !errors.Is(err, connector.ErrDeliveryIndeterminate) || errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("exact concurrent insert error=%v", err)
	}
	positionConflict := managedReceiptTestIntent(t, incarnation, exact.PositionID, "different-content-and-policy")
	if err := insert(t, positionConflict); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("position unique conflict error=%v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM wallaby_meta.__delivery_receipts;ALTER TABLE wallaby_meta.__delivery_receipts DROP CONSTRAINT wallaby_delivery_receipts_logical_batch_current`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO wallaby_meta.__delivery_receipts(marker_id,flow_id,flow_incarnation_id,generation,acquisition_id,lease_epoch,destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash) VALUES('corrupt-logical-marker',$1,$2,1,'acquisition',1,$3,$4,$5,'other-position',$6)`, exact.FlowID, exact.FlowIncarnationID, exact.DestinationRevisionID, exact.SourceLineageID, exact.LogicalBatchID, exact.ContentHash); err != nil {
		t.Fatal(err)
	}
	if err := insert(t, exact); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("logical unique conflict error=%v", err)
	}
}

func TestManagedAppendBootstrapRetainsEarlierReceiptAcrossRestart(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts`); err != nil {
		t.Fatal(err)
	}
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	table := "wallaby_bootstrap_receipts_" + suffix
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE TABLE public.%s(event_id bigint,payload text,__wallaby_operation text,__wallaby_deleted boolean,__wallaby_source_position text)`, quoteIdent(table))); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS public.%s`, quoteIdent(table)))
		_, _ = admin.Exec(context.Background(), `DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts`)
	}()
	open := func(name string) *Destination {
		d := &Destination{}
		if err := d.Open(ctx, connector.Spec{Name: name, Type: connector.EndpointPostgres, Options: map[string]string{"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1, "batch_mode": "target", "synchronous_commit": "on", "meta_table_enabled": "false", "flow_id": "bootstrap-receipt-flow"}}); err != nil {
			t.Fatal(err)
		}
		return d
	}
	destination := open("bootstrap-first-process")
	schema := connector.Schema{Namespace: "public", Name: table, Columns: []connector.Column{{Name: "event_id", Type: "bigint"}, {Name: "payload", Type: "text"}, {Name: connector.AppendOperationColumn, Type: "text"}, {Name: connector.AppendDeletedColumn, Type: "boolean"}, {Name: connector.AppendSourcePositionColumn, Type: "text"}}}
	policy := connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: "bootstrap-append-v1"}
	bootstrap := connector.BootstrapIntent{FlowID: "bootstrap-receipt-flow", FlowIncarnationID: uuid.NewString(), SourceLineageID: "bootstrap-receipt-lineage", BootstrapID: uuid.NewString(), BootstrapGeneration: 1, Generation: 1, AcquisitionID: uuid.NewString(), LeaseEpoch: 1, DestinationRevisionID: "bootstrap-receipt-revision", ManifestHash: "bootstrap-receipt-manifest"}
	if err := destination.PrepareBootstrap(ctx, bootstrap, []connector.BootstrapTable{{Schema: schema, WritePolicy: policy}}); err != nil {
		t.Fatal(err)
	}
	batch := func(lsn, payload string) connector.Batch {
		return connector.Batch{Schema: schema, Checkpoint: connector.Checkpoint{LSN: lsn}, WritePolicy: policy, Records: []connector.Record{{Table: table, Operation: connector.OpLoad, After: map[string]any{"event_id": int64(1), "payload": payload, connector.AppendOperationColumn: "load", connector.AppendDeletedColumn: false, connector.AppendSourcePositionColumn: lsn}, SourcePosition: lsn}}}
	}
	intent := func(batch connector.Batch) connector.DeliveryIntent {
		hash, err := connector.BatchContentHash(batch)
		if err != nil {
			t.Fatal(err)
		}
		position, err := connector.CheckpointPositionID(batch.Checkpoint)
		if err != nil {
			t.Fatal(err)
		}
		logical, err := connector.DeliveryLogicalBatchID(bootstrap.SourceLineageID, position, hash)
		if err != nil {
			t.Fatal(err)
		}
		return connector.DeliveryIntent{FlowID: bootstrap.FlowID, FlowIncarnationID: bootstrap.FlowIncarnationID, SourceLineageID: bootstrap.SourceLineageID, Generation: bootstrap.Generation, AcquisitionID: bootstrap.AcquisitionID, LeaseEpoch: bootstrap.LeaseEpoch, DestinationRevisionID: bootstrap.DestinationRevisionID, LogicalBatchID: logical, PositionID: position, ContentHash: hash}
	}
	first, second := batch("0/10", "first"), batch("0/20", "second")
	firstIntent, secondIntent := intent(first), intent(second)
	if _, err := destination.ApplyBootstrap(ctx, bootstrap, firstIntent, first); err != nil {
		t.Fatal(err)
	}
	// The target commit above is intentionally left without a control-store
	// receipt, modeling a crash/failure at that boundary.
	if _, err := destination.ApplyBootstrap(ctx, bootstrap, secondIntent, second); err != nil {
		t.Fatal(err)
	}
	if err := destination.Close(ctx); err != nil {
		t.Fatal(err)
	}
	destination = open("bootstrap-restarted-process")
	defer destination.Close(context.Background())
	disposition, _, err := destination.ReconcileBootstrap(ctx, bootstrap, firstIntent)
	if err != nil || disposition != connector.DeliveryApplied {
		t.Fatalf("reconcile first bootstrap receipt disposition=%v err=%v", disposition, err)
	}
	if _, err := destination.ApplyBootstrap(ctx, bootstrap, firstIntent, first); err != nil {
		t.Fatalf("adopt first bootstrap receipt: %v", err)
	}
	stageSchema, _, stageTable := destination.bootstrapTableCoordinates(bootstrap, schema)
	stage := quoteIdent(stageTable)
	if stageSchema != "" {
		stage = quoteIdent(stageSchema) + "." + stage
	}
	var stageRows, receipts int
	if err := admin.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, stage)).Scan(&stageRows); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM wallaby_meta.__delivery_receipts WHERE flow_incarnation_id=$1 AND destination_revision_id=$2`, bootstrap.FlowIncarnationID, bootstrap.DestinationRevisionID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if stageRows != 2 || receipts != 2 {
		t.Fatalf("bootstrap stage rows/receipts=%d/%d, want 2/2", stageRows, receipts)
	}
	if _, err := destination.PublishBootstrap(ctx, bootstrap, []connector.BootstrapTable{{Schema: schema, WritePolicy: policy}}); err != nil {
		t.Fatal(err)
	}
	var targetRows, retainedReceipts int
	if err := admin.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM public.%s`, quoteIdent(table))).Scan(&targetRows); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM wallaby_meta.__delivery_receipts WHERE flow_incarnation_id=$1 AND destination_revision_id=$2`, bootstrap.FlowIncarnationID, bootstrap.DestinationRevisionID).Scan(&retainedReceipts); err != nil {
		t.Fatal(err)
	}
	if targetRows != 2 || retainedReceipts != 2 {
		t.Fatalf("published bootstrap rows/retained receipts=%d/%d, want 2/2", targetRows, retainedReceipts)
	}
}

func managedReceiptTestIntent(t *testing.T, incarnation, position, content string) connector.DeliveryIntent {
	t.Helper()
	logical, err := connector.DeliveryLogicalBatchID("receipt-lineage", position, content)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{FlowID: "receipt-flow", FlowIncarnationID: incarnation, SourceLineageID: "receipt-lineage", Generation: 1, AcquisitionID: uuid.NewString(), LeaseEpoch: 1, DestinationRevisionID: "receipt-revision", LogicalBatchID: logical, PositionID: position, ContentHash: content}
}
