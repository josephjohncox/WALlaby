package postgres

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresManagedTargetReceiptRollingCompatibility(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `
DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts;
CREATE SCHEMA IF NOT EXISTS wallaby_meta;
CREATE TABLE wallaby_meta.__delivery_receipts (
  marker_id TEXT NOT NULL UNIQUE,
  flow_id TEXT NOT NULL,
  flow_incarnation_id TEXT NOT NULL,
  generation BIGINT NOT NULL,
  acquisition_id TEXT NOT NULL,
  lease_epoch BIGINT NOT NULL,
  destination_revision_id TEXT NOT NULL,
  source_lineage_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_incarnation_id,destination_revision_id,position_id)
)`); err != nil {
		t.Fatal(err)
	}
	flowIncarnationID := uuid.NewString()
	if _, err := admin.Exec(ctx, `
INSERT INTO wallaby_meta.__delivery_receipts (
  marker_id,flow_id,flow_incarnation_id,generation,acquisition_id,lease_epoch,
  destination_revision_id,source_lineage_id,position_id,content_hash
) VALUES ('legacy-marker','rolling-target',$1,1,'legacy-acquisition',1,'rolling-revision','rolling-lineage','legacy-position','legacy-hash')`, flowIncarnationID); err != nil {
		t.Fatal(err)
	}
	destination := &Destination{}
	if err := destination.Open(ctx, connector.Spec{Name: "rolling-target", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1,
		"write_mode": "target", "batch_mode": "target", "synchronous_commit": "on", "meta_table_enabled": "false",
	}}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(ctx)
	if _, err := admin.Exec(ctx, `
INSERT INTO wallaby_meta.__delivery_receipts (
  marker_id,flow_id,flow_incarnation_id,generation,acquisition_id,lease_epoch,
  destination_revision_id,source_lineage_id,position_id,content_hash
) VALUES ('old-writer-after-upgrade','rolling-target',$1,1,'legacy-acquisition',1,'rolling-revision','rolling-lineage','second-position','second-hash')`, flowIncarnationID); err != nil {
		t.Fatalf("checkpoint-1 target writer rejected after additive upgrade: %v", err)
	}
	intent := connector.DeliveryIntent{
		FlowID: "rolling-target", FlowIncarnationID: flowIncarnationID, SourceLineageID: "rolling-lineage",
		Generation: 1, AcquisitionID: "current-acquisition", LeaseEpoch: 2,
		DestinationRevisionID: "rolling-revision", LogicalBatchID: "logical-batch:current", PositionID: "legacy-position", ContentHash: "legacy-hash",
	}
	disposition, _, err := destination.Reconcile(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if disposition != connector.DeliveryApplied {
		t.Fatalf("legacy target receipt disposition=%v, want applied", disposition)
	}
	var logicalBatchID string
	if err := admin.QueryRow(ctx, `
SELECT logical_batch_id FROM wallaby_meta.__delivery_receipts
WHERE flow_incarnation_id=$1 AND position_id='legacy-position'`, flowIncarnationID).Scan(&logicalBatchID); err != nil {
		t.Fatal(err)
	}
	if logicalBatchID != intent.LogicalBatchID {
		t.Fatalf("adopted legacy target logical batch=%q, want %q", logicalBatchID, intent.LogicalBatchID)
	}
}

func TestPostgresManagedProfilePoolExhaustion(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	destination := &Destination{}
	if err := destination.Open(ctx, connector.Spec{Name: "pool-exhaustion", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1,
		"write_mode": "target", "batch_mode": "target", "synchronous_commit": "on",
		"meta_table_enabled": "true", "pool_max_conns": "1", "flow_id": "pool-exhaustion",
	}}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(ctx)
	if _, err := destination.pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_profile_pool_exhaustion; CREATE TABLE public.wallaby_profile_pool_exhaustion (id bigint PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = destination.pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.wallaby_profile_pool_exhaustion`)
	}()

	const recordCount = 512
	records := make([]connector.Record, 0, recordCount)
	for id := 1; id <= recordCount; id++ {
		key, err := json.Marshal(map[string]any{"id": id})
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, connector.Record{
			Table: "wallaby_profile_pool_exhaustion", Operation: connector.OpInsert, SchemaVersion: 1,
			Key: key, After: map[string]any{"id": int64(id)},
		})
	}
	transaction := connector.SourceTransaction{
		SourceLineageID: "pool-exhaustion-lineage", TransactionID: 2001,
		BeginLSN: "0/400", CommitLSN: "0/480", EndLSN: "0/488", Checkpoint: connector.Checkpoint{LSN: "0/488"},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{
				Schema: connector.Schema{
					Namespace: "public", Name: "wallaby_profile_pool_exhaustion", Version: 1,
					Columns: []connector.Column{{
						Name: "id", Type: "bigint",
						TypeMetadata: map[string]string{"primary_key": "true", "replica_identity": "true"},
					}},
				},
				Records: records,
			},
		}},
	}
	contentHash, err := connector.SourceTransactionContentHash(transaction)
	if err != nil {
		t.Fatal(err)
	}
	logicalBatchID, err := connector.SourceTransactionLogicalBatchID(transaction)
	if err != nil {
		t.Fatal(err)
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	intent := connector.DeliveryIntent{
		FlowID: "pool-exhaustion", FlowIncarnationID: uuid.NewString(),
		SourceLineageID: transaction.SourceLineageID, Generation: 1, AcquisitionID: "pool-exhaustion-acquisition", LeaseEpoch: 1,
		DestinationRevisionID: "pool-exhaustion-revision", LogicalBatchID: logicalBatchID, PositionID: positionID, ContentHash: contentHash,
	}
	if _, err := destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("managed transaction deadlocked or exhausted locks with one connection and metadata enabled: %v", err)
	}
	var rows int
	if err := destination.pool.QueryRow(ctx, `SELECT count(*) FROM public.wallaby_profile_pool_exhaustion`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != recordCount {
		t.Fatalf("delivered rows=%d, want %d", rows, recordCount)
	}

	secondKey, err := json.Marshal(map[string]any{"id": recordCount + 1})
	if err != nil {
		t.Fatal(err)
	}
	second := transaction
	second.TransactionID = 2002
	second.BeginLSN, second.CommitLSN, second.EndLSN = "0/490", "0/498", "0/4A0"
	second.Checkpoint = connector.Checkpoint{LSN: second.EndLSN}
	second.Fragments = []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{
		Schema: transaction.Fragments[0].Batch.Schema,
		Records: []connector.Record{{
			Table: "wallaby_profile_pool_exhaustion", Operation: connector.OpInsert, SchemaVersion: 1,
			Key: secondKey, After: map[string]any{"id": int64(recordCount + 1)},
		}},
	}}}
	secondHash, secondLogicalBatchID, err := connector.SourceTransactionIdentity(second)
	if err != nil {
		t.Fatal(err)
	}
	secondPositionID, err := connector.CheckpointPositionID(second.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	secondIntent := intent
	secondIntent.ContentHash = secondHash
	secondIntent.LogicalBatchID = secondLogicalBatchID
	secondIntent.PositionID = secondPositionID
	if _, err := destination.ApplyTransaction(ctx, secondIntent, second); err != nil {
		t.Fatalf("deliver second managed transaction: %v", err)
	}
	var targetReceipts int
	if err := destination.pool.QueryRow(ctx, `
SELECT count(*) FROM wallaby_meta.__delivery_receipts
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2`, intent.FlowIncarnationID, intent.DestinationRevisionID).Scan(&targetReceipts); err != nil {
		t.Fatal(err)
	}
	if targetReceipts != 1 {
		t.Fatalf("retained target delivery markers=%d, want one high-watermark marker", targetReceipts)
	}
}
