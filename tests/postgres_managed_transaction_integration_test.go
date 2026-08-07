package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestPostgresManagedFullTransactionPreservesFragmentsAndMarker(t *testing.T) {
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

	if _, err := pool.Exec(ctx, `
CREATE SCHEMA IF NOT EXISTS audit;
DROP TABLE IF EXISTS public.wallaby_managed_widgets;
DROP TABLE IF EXISTS audit.wallaby_managed_events;
CREATE TABLE public.wallaby_managed_widgets (id bigint PRIMARY KEY,value text NOT NULL);
CREATE TABLE audit.wallaby_managed_events (id bigint PRIMARY KEY,widget_id bigint NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.wallaby_managed_widgets; DROP TABLE IF EXISTS audit.wallaby_managed_events`)
	}()

	destination := &pgdest.Destination{}
	if err := destination.Open(ctx, connector.RuntimeSpec{Name: "managed-full-transaction", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1,
		"batch_mode": "target", "meta_table_enabled": "false", "synchronous_commit": "on",
	}}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(ctx)

	widgets := managedTransactionSchema("public", "wallaby_managed_widgets", connector.Column{Name: "value", Type: "text"})
	events := managedTransactionSchema("audit", "wallaby_managed_events", connector.Column{Name: "widget_id", Type: "bigint"})
	key := recordKey(t, map[string]any{"id": 1})
	transaction := connector.SourceTransaction{
		SourceLineageID: "source-lineage-full-transaction",
		TransactionID:   901,
		BeginLSN:        "0/100",
		CommitLSN:       "0/180",
		EndLSN:          "0/188",
		Checkpoint:      connector.Checkpoint{LSN: "0/188"},
		Fragments: []connector.TransactionFragment{
			{
				Ordinal: 0,
				Batch: connector.Batch{Schema: widgets, Records: []connector.Record{{
					Table: widgets.Name, Operation: connector.OpInsert, SchemaVersion: 1, Key: key,
					After: map[string]any{"id": int64(1), "value": "first"},
				}}},
			},
			{
				Ordinal: 1,
				Batch: connector.Batch{Schema: events, Records: []connector.Record{{
					Table: events.Name, Operation: connector.OpInsert, SchemaVersion: 1, Key: key,
					After: map[string]any{"id": int64(1), "widget_id": int64(1)},
				}}},
			},
			{
				Ordinal: 2,
				Batch: connector.Batch{Schema: widgets, Records: []connector.Record{{
					Table: widgets.Name, Operation: connector.OpUpdate, SchemaVersion: 1, Key: key,
					Before: map[string]any{"id": int64(1), "value": "first"},
					After:  map[string]any{"id": int64(1), "value": "second"},
				}}},
			},
			{
				Ordinal: 3,
				Batch: connector.Batch{Schema: widgets, Records: []connector.Record{{
					Table: widgets.Name, Operation: connector.OpDelete, SchemaVersion: 1, Key: key,
				}}},
			},
			{
				Ordinal: 4,
				Batch: connector.Batch{Schema: widgets, Records: []connector.Record{{
					Table: widgets.Name, Operation: connector.OpInsert, SchemaVersion: 1, Key: key,
					After: map[string]any{"id": int64(1), "value": "final"},
				}}},
			},
		},
	}
	for index := range transaction.Fragments {
		transaction.Fragments[index].Batch.WritePolicy = connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}}
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
		FlowID: "managed-full-transaction", FlowIncarnationID: fmt.Sprintf("incarnation-%d", time.Now().UnixNano()),
		SourceLineageID: transaction.SourceLineageID, Generation: 1, AcquisitionID: "acquisition-1", LeaseEpoch: 1,
		DestinationRevisionID: "postgres-managed-profile-v1", LogicalBatchID: logicalBatchID, PositionID: positionID, ContentHash: contentHash,
	}

	evidence, err := destination.ApplyTransaction(ctx, intent, transaction)
	if err != nil {
		t.Fatal(err)
	}
	disposition, reconciled, err := destination.Reconcile(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if disposition != connector.DeliveryApplied || reconciled != evidence {
		t.Fatalf("reconcile=(%v,%+v), want applied/%+v", disposition, reconciled, evidence)
	}
	if _, err := destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("replay transaction: %v", err)
	}

	var value string
	if err := pool.QueryRow(ctx, `SELECT value FROM public.wallaby_managed_widgets WHERE id=1`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "final" {
		t.Fatalf("widget value=%q, want final operation-order result", value)
	}
	var eventsCount, receipts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit.wallaby_managed_events`).Scan(&eventsCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallaby_meta.__delivery_receipts WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, intent.FlowIncarnationID, logicalBatchID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if eventsCount != 1 || receipts != 1 {
		t.Fatalf("events=%d receipts=%d, want 1/1 after replay", eventsCount, receipts)
	}
}

func TestPostgresManagedTransactionCommitBeforeReceiptReconciles(t *testing.T) {
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
	coordinator, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("transaction-reconcile-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	oldFence, err := authorityStore.AcquireProducer(ctx, flowID, "old-transaction", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	const table = "wallaby_transaction_commit_reconcile"
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_transaction_commit_reconcile; CREATE TABLE public.wallaby_transaction_commit_reconcile (id bigint PRIMARY KEY,value text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.wallaby_transaction_commit_reconcile`)
	}()
	target := &pgdest.Destination{}
	if err := target.Open(ctx, connector.RuntimeSpec{Name: "transaction-reconcile", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1,
		"batch_mode": "target", "meta_table_enabled": "false", "synchronous_commit": "on",
	}}); err != nil {
		t.Fatal(err)
	}
	defer target.Close(ctx)
	revisionID := fmt.Sprintf("transaction-reconcile-revision-%d", time.Now().UnixNano())
	if err := coordinator.RegisterDestinationRevision(ctx, oldFence, revisionID, "transaction-reconcile", "profile-v1"); err != nil {
		t.Fatal(err)
	}

	transaction := connector.SourceTransaction{
		SourceLineageID: "transaction-reconcile-lineage", TransactionID: 902,
		BeginLSN: "0/700", CommitLSN: "0/780", EndLSN: "0/788", Checkpoint: connector.Checkpoint{LSN: "0/788"},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}},
				Schema: managedTransactionSchema("public", table, connector.Column{Name: "value", Type: "text"}),
				Records: []connector.Record{{
					Table: table, Operation: connector.OpInsert, SchemaVersion: 1,
					Key: recordKey(t, map[string]any{"id": 1}), After: map[string]any{"id": int64(1), "value": "committed"},
				}},
			},
		}},
	}
	oldIntent := transactionIntentForFence(t, oldFence, revisionID, transaction)
	driver := &commitBeforeReceiptTransactionDriver{ManagedTransactionDestination: target}
	if _, err := coordinator.DeliverTransaction(ctx, oldFence, oldIntent, transaction, managedBaselinePayload(t, transaction), driver); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("commit-before-receipt error=%v, want ErrDeliveryIndeterminate", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, oldFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	newFence, err := authorityStore.AcquireProducer(ctx, flowID, "new-transaction", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	driver.rejectValidation = true
	newIntent := transactionIntentForFence(t, newFence, revisionID, transaction)
	grant, err := coordinator.DeliverTransaction(ctx, newFence, newIntent, transaction, managedBaselinePayload(t, transaction), driver)
	if err != nil {
		t.Fatalf("reconcile committed transaction before target revalidation: %v", err)
	}
	if grant.Checkpoint.LSN != transaction.EndLSN || driver.applyCalls != 1 {
		t.Fatalf("reconciled grant/apply_calls=(%s,%d), want %s/1", grant.Checkpoint.LSN, driver.applyCalls, transaction.EndLSN)
	}
	var rows, receipts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.wallaby_transaction_commit_reconcile WHERE id=1 AND value='committed'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, newFence.FlowIncarnationID, newIntent.LogicalBatchID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || receipts != 1 {
		t.Fatalf("target rows/control receipts=%d/%d, want 1/1", rows, receipts)
	}
}

func TestPostgresManagedOverlappingTakeoverAdoptsConcurrentCommit(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
	coordinator, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("transaction-overlap-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	oldFence, err := authorityStore.AcquireProducer(ctx, flowID, "old-overlap", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	const advisoryKey = int64(731337)
	if _, err := pool.Exec(ctx, `
DROP TABLE IF EXISTS public.wallaby_transaction_overlap;
DROP FUNCTION IF EXISTS public.wallaby_transaction_overlap_block();
CREATE TABLE public.wallaby_transaction_overlap (id bigint PRIMARY KEY,value text NOT NULL);
CREATE FUNCTION public.wallaby_transaction_overlap_block() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_advisory_xact_lock(731337);
  RETURN NEW;
END
$$;
CREATE TRIGGER wallaby_transaction_overlap_block
BEFORE INSERT ON public.wallaby_transaction_overlap
FOR EACH STATEMENT EXECUTE FUNCTION public.wallaby_transaction_overlap_block()`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.wallaby_transaction_overlap; DROP FUNCTION IF EXISTS public.wallaby_transaction_overlap_block()`)
	}()
	target := &pgdest.Destination{}
	if err := target.Open(ctx, connector.RuntimeSpec{Name: "transaction-overlap", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1,
		"batch_mode": "target", "meta_table_enabled": "false", "synchronous_commit": "on",
	}}); err != nil {
		t.Fatal(err)
	}
	defer target.Close(ctx)
	revisionID := fmt.Sprintf("transaction-overlap-revision-%d", time.Now().UnixNano())
	if err := coordinator.RegisterDestinationRevision(ctx, oldFence, revisionID, "transaction-overlap", "profile-v1"); err != nil {
		t.Fatal(err)
	}
	transaction := connector.SourceTransaction{
		SourceLineageID: "transaction-overlap-lineage", TransactionID: 903,
		BeginLSN: "0/800", CommitLSN: "0/880", EndLSN: "0/888", Checkpoint: connector.Checkpoint{LSN: "0/888"},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}},
			Schema: managedTransactionSchema("public", "wallaby_transaction_overlap", connector.Column{Name: "value", Type: "text"}),
			Records: []connector.Record{{
				Table: "wallaby_transaction_overlap", Operation: connector.OpInsert, SchemaVersion: 1,
				Key: recordKey(t, map[string]any{"id": 1}), After: map[string]any{"id": int64(1), "value": "committed"},
			}},
		}}},
	}

	blocker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Release()
	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryKey); err != nil {
		t.Fatal(err)
	}
	oldIntent := transactionIntentForFence(t, oldFence, revisionID, transaction)
	oldResult := make(chan error, 1)
	go func() {
		_, deliverErr := coordinator.DeliverTransaction(ctx, oldFence, oldIntent, transaction, managedBaselinePayload(t, transaction), target)
		oldResult <- deliverErr
	}()
	waitForAdvisoryWaiters(t, ctx, pool, blocker.Conn().PgConn().PID(), 1)
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, oldFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	newFence, err := authorityStore.AcquireProducer(ctx, flowID, "new-overlap", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	newIntent := transactionIntentForFence(t, newFence, revisionID, transaction)
	newResult := make(chan error, 1)
	go func() {
		_, deliverErr := coordinator.DeliverTransaction(ctx, newFence, newIntent, transaction, managedBaselinePayload(t, transaction), target)
		newResult <- deliverErr
	}()
	waitForAdvisoryWaiters(t, ctx, pool, blocker.Conn().PgConn().PID(), 2)
	if _, err := blocker.Exec(ctx, `SELECT pg_advisory_unlock($1)`, advisoryKey); err != nil {
		t.Fatal(err)
	}
	if err := <-oldResult; !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("old overlapping owner error=%v, want stale-fence rejection after target commit", err)
	}
	if err := <-newResult; !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("new overlapping owner error=%v, want indeterminate receipt collision", err)
	}
	grant, err := coordinator.DeliverTransaction(ctx, newFence, newIntent, transaction, managedBaselinePayload(t, transaction), target)
	if err != nil {
		t.Fatalf("adopt concurrently committed marker: %v", err)
	}
	if grant.Checkpoint.LSN != transaction.EndLSN {
		t.Fatalf("adopted checkpoint=%s, want %s", grant.Checkpoint.LSN, transaction.EndLSN)
	}
	var rows, receipts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.wallaby_transaction_overlap WHERE id=1 AND value='committed'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, newFence.FlowIncarnationID, newIntent.LogicalBatchID).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || receipts != 1 {
		t.Fatalf("overlap rows/control receipts=%d/%d, want 1/1", rows, receipts)
	}
}

type commitBeforeReceiptTransactionDriver struct {
	connector.ManagedTransactionDestination
	applyCalls       int
	rejectValidation bool
}

func (d *commitBeforeReceiptTransactionDriver) ValidateTransaction(ctx context.Context, transaction connector.SourceTransaction) error {
	if d.rejectValidation {
		return errors.New("target schema changed after committed marker")
	}
	return d.ManagedTransactionDestination.ValidateTransaction(ctx, transaction)
}

func (d *commitBeforeReceiptTransactionDriver) ApplyTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	d.applyCalls++
	evidence, err := d.ManagedTransactionDestination.ApplyTransaction(ctx, intent, transaction)
	if err != nil {
		return evidence, err
	}
	return evidence, fmt.Errorf("%w: injected transport loss after target commit", connector.ErrDeliveryIndeterminate)
}

func managedTransactionSchema(namespace, table string, extra connector.Column) connector.Schema {
	return connector.Schema{Namespace: namespace, Name: table, Version: 1, Columns: []connector.Column{
		{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"primary_key": "true", "replica_identity": "true"}},
		extra,
	}}
}

func TestManagedProfileKeepsGenericPrimaryAndAllPoliciesExplicit(t *testing.T) {
	if stream.AckPolicyAll != "all" || stream.AckPolicyPrimary != "primary" {
		t.Fatalf("ack policy wire values changed: all=%q primary=%q", stream.AckPolicyAll, stream.AckPolicyPrimary)
	}
}
