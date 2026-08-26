package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/partauthority"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type partReservationFixture struct {
	ctx         context.Context
	pool        *pgxpool.Pool
	engine      *workflow.PostgresEngine
	coordinator *delivery.Coordinator
	fence       authority.RunFence
	flowID      string
	revisions   []string
}

func newPartReservationFixture(t *testing.T, hooks delivery.CoordinatorHooks) *partReservationFixture {
	t.Helper()
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx := context.Background()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		engine.Close()
		t.Fatal(err)
	}
	coordinator, err := delivery.NewCoordinator(ctx, pool, delivery.WithCoordinatorHooks(hooks))
	if err != nil {
		pool.Close()
		engine.Close()
		t.Fatal(err)
	}
	store, err := authority.NewPostgresStore(pool)
	if err != nil {
		pool.Close()
		engine.Close()
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("clickhouse-part-reservation-%d", time.Now().UnixNano())
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		pool.Close()
		engine.Close()
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		pool.Close()
		engine.Close()
		t.Fatal(err)
	}
	fence, err := store.AcquireProducer(ctx, flowID, "part-reservation-owner", "test", control.Generation, time.Minute)
	if err != nil {
		pool.Close()
		engine.Close()
		t.Fatal(err)
	}
	fixture := &partReservationFixture{ctx: ctx, pool: pool, engine: engine, coordinator: coordinator, fence: fence, flowID: flowID}
	t.Cleanup(func() {
		cleanupAuthorityTest(context.Background(), pool, flowID)
		for _, revision := range fixture.revisions {
			_, _ = pool.Exec(context.Background(), "DELETE FROM destination_revisions WHERE destination_revision_id=$1", revision)
		}
		pool.Close()
		engine.Close()
	})
	return fixture
}

func (f *partReservationFixture) takeover(t *testing.T) authority.RunFence {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, f.fence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	store, err := authority.NewPostgresStore(f.pool)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireProducer(f.ctx, f.flowID, "part-reservation-takeover", "test", f.fence.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	f.fence = fence
	return fence
}

func (f *partReservationFixture) register(t *testing.T, revision string) {
	t.Helper()
	if err := f.coordinator.RegisterDestinationRevision(f.ctx, f.fence, revision, "clickhouse", "managed-parts-v1"); err != nil {
		t.Fatal(err)
	}
	f.revisions = append(f.revisions, revision)
}

type reservationTestState struct {
	mu             sync.Mutex
	prepared       map[string]int
	receiptApplied map[string]bool
	partApplied    map[string]map[string]bool
	activeParts    uint64
}

type reservationTestDriver struct {
	connector.ManagedTransactionDestination
	capacity       uint64
	failAfter      int
	observationErr error
	state          *reservationTestState
}

func newReservationTestDriver(capacity uint64) *reservationTestDriver {
	return newReservationTestDriverWithState(capacity, &reservationTestState{prepared: make(map[string]int), receiptApplied: make(map[string]bool), partApplied: make(map[string]map[string]bool)})
}

func newReservationTestDriverWithState(capacity uint64, state *reservationTestState) *reservationTestDriver {
	return &reservationTestDriver{capacity: capacity, state: state}
}

func (d *reservationTestDriver) PrepareTransaction(_ context.Context, intent connector.DeliveryIntent, _ connector.SourceTransaction) (connector.PreparedManagedTransaction, error) {
	d.state.mu.Lock()
	d.state.prepared[intent.LogicalBatchID]++
	attempt := d.state.prepared[intent.LogicalBatchID]
	d.state.mu.Unlock()
	parts := []connector.ManagedPartIdentity{
		{Kind: "changelog", Ordinal: 0, QueryID: "test-fragment-" + intent.LogicalBatchID},
		{Kind: "receipt", Ordinal: 0, QueryID: "test-receipt-" + intent.LogicalBatchID},
	}
	failAfter := 0
	if attempt == 1 {
		failAfter = d.failAfter
	}
	planHash, err := connector.ManagedPartPlanHash(parts)
	if err != nil {
		return nil, err
	}
	return &reservationTestPrepared{driver: d, intent: intent, request: connector.ManagedPartReservationRequest{
		Resource: connector.ManagedPartResourceClickHouseActivePartsV1, DestinationRevisionID: intent.DestinationRevisionID,
		SourceLineageID: intent.SourceLineageID, LogicalBatchID: intent.LogicalBatchID, PositionID: intent.PositionID,
		ContentHash: intent.ContentHash, PlanHash: planHash, Capacity: d.capacity, Parts: parts,
	}, failAfter: failAfter}, nil
}

func (d *reservationTestDriver) ValidateTransaction(context.Context, connector.SourceTransaction) error {
	return nil
}
func (d *reservationTestDriver) InitializeManagedDelivery(context.Context) error { return nil }
func (d *reservationTestDriver) ApplyTransaction(context.Context, connector.DeliveryIntent, connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, errors.New("direct transaction apply is forbidden in reservation test driver")
}
func (d *reservationTestDriver) ObserveManagedPartReservation(_ context.Context, intent connector.DeliveryIntent, requireAbsent bool) (connector.ManagedPartReservationObservation, error) {
	return d.observe(intent, requireAbsent)
}
func (d *reservationTestDriver) observe(intent connector.DeliveryIntent, requireAbsent bool) (connector.ManagedPartReservationObservation, error) {
	d.state.mu.Lock()
	defer d.state.mu.Unlock()
	if d.observationErr != nil {
		return connector.ManagedPartReservationObservation{}, d.observationErr
	}
	absent := len(d.state.partApplied[intent.LogicalBatchID]) == 0
	if requireAbsent && !absent {
		return connector.ManagedPartReservationObservation{}, fmt.Errorf("%w: test endpoints retain batch evidence", connector.ErrDeliveryIndeterminate)
	}
	return connector.ManagedPartReservationObservation{ServerActiveParts: d.state.activeParts, EndpointCount: 2, Quiescent: true, BatchAbsent: absent}, nil
}
func (d *reservationTestDriver) Reconcile(_ context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	d.state.mu.Lock()
	applied := d.state.receiptApplied[intent.LogicalBatchID]
	d.state.mu.Unlock()
	if applied {
		return connector.DeliveryApplied, connector.DeliveryEvidence{ExternalID: "test-external-" + intent.LogicalBatchID, ContentHash: intent.ContentHash}, nil
	}
	return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
}

type reservationTestPrepared struct {
	driver      *reservationTestDriver
	intent      connector.DeliveryIntent
	request     connector.ManagedPartReservationRequest
	reservation *partauthority.Grant
	failAfter   int
}

func (p *reservationTestPrepared) PartReservationRequest() (connector.ManagedPartReservationRequest, error) {
	return p.request, p.request.Validate()
}
func (p *reservationTestPrepared) ObservePartReservation(_ context.Context, requireAbsent bool) (connector.ManagedPartReservationObservation, error) {
	return p.driver.observe(p.intent, requireAbsent)
}
func (p *reservationTestPrepared) BindPartReservation(reservation *partauthority.Grant) error {
	if reservation == nil || reservation.ReservationID() == "" {
		return errors.New("reservation is required")
	}
	p.reservation = reservation
	return nil
}
func (p *reservationTestPrepared) Apply(ctx context.Context) (connector.DeliveryEvidence, error) {
	if p.reservation == nil {
		return connector.DeliveryEvidence{}, errors.New("write attempted without reservation")
	}
	for index, part := range p.request.Parts {
		if err := p.reservation.GuardPartWrite(ctx, part, func(context.Context) error {
			p.driver.state.mu.Lock()
			if p.driver.state.partApplied[p.intent.LogicalBatchID] == nil {
				p.driver.state.partApplied[p.intent.LogicalBatchID] = make(map[string]bool)
			}
			if !p.driver.state.partApplied[p.intent.LogicalBatchID][part.QueryID] {
				p.driver.state.activeParts++
			}
			p.driver.state.partApplied[p.intent.LogicalBatchID][part.QueryID] = true
			if part.Kind == "receipt" {
				p.driver.state.receiptApplied[p.intent.LogicalBatchID] = true
			}
			p.driver.state.mu.Unlock()
			if p.failAfter == index+1 {
				return fmt.Errorf("%w: injected crash after external %s success before progress commit", connector.ErrDeliveryIndeterminate, part.Kind)
			}
			return nil
		}); err != nil {
			return connector.DeliveryEvidence{}, err
		}
	}
	return connector.DeliveryEvidence{ExternalID: "test-external-" + p.intent.LogicalBatchID, ContentHash: p.intent.ContentHash}, nil
}

func TestClickHousePartReservationSerializesConcurrentWriters(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	fixture := newPartReservationFixture(t, delivery.CoordinatorHooks{AfterPartReservationLock: func(ctx context.Context, _ authority.RunFence, _ connector.ManagedPartReservationRequest) error {
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	secondCoordinator, err := delivery.NewCoordinator(fixture.ctx, fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	revision := fmt.Sprintf("clickhouse-parts-concurrent-%d", time.Now().UnixNano())
	fixture.register(t, revision)
	shared := &reservationTestState{prepared: make(map[string]int), receiptApplied: make(map[string]bool), partApplied: make(map[string]map[string]bool)}
	firstDriver := newReservationTestDriverWithState(2, shared)
	secondDriver := newReservationTestDriverWithState(2, shared)
	first := retentionTransaction("parts_first", 901, "0/901", 1)
	second := retentionTransaction("parts_second", 902, "0/902", 2)
	firstIntent := transactionIntentForFence(t, fixture.fence, revision, first)
	secondIntent := transactionIntentForFence(t, fixture.fence, revision, second)
	firstErr := make(chan error, 1)
	secondErr := make(chan error, 1)
	go func() {
		_, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, firstIntent, first, managedBaselinePayload(t, first), firstDriver)
		firstErr <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first coordinator did not hold the destination budget lock")
	}
	go func() {
		_, err := secondCoordinator.DeliverTransaction(fixture.ctx, fixture.fence, secondIntent, second, managedBaselinePayload(t, second), secondDriver)
		secondErr <- err
	}()
	select {
	case err := <-secondErr:
		t.Fatalf("second coordinator bypassed the held budget lock: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-firstErr; err != nil {
		t.Fatalf("first writer: %v", err)
	}
	if err := <-secondErr; err == nil || !strings.Contains(err.Error(), "capacity=2") {
		t.Fatalf("second writer error=%v, want fresh locked active-part rejection", err)
	}
	var completed, reserved int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FILTER (WHERE reservation_state='completed_pending_observation'),count(*) FILTER (WHERE reservation_state='reserved') FROM managed_part_reservations WHERE destination_revision_id=$1`, revision).Scan(&completed, &reserved); err != nil {
		t.Fatal(err)
	}
	if completed != 1 || reserved != 0 {
		t.Fatalf("completed/reserved=%d/%d, want conservative charge retention when the observing admission rolls back", completed, reserved)
	}
}

func TestClickHousePartReservationCrashAfterReservation(t *testing.T) {
	crash := true
	fixture := newPartReservationFixture(t, delivery.CoordinatorHooks{AfterPartReservationCommit: func(_ context.Context, _ authority.RunFence, _ connector.DeliveryIntent, _ string) error {
		if crash {
			crash = false
			return errors.New("injected crash after reservation commit")
		}
		return nil
	}})
	revision := fmt.Sprintf("clickhouse-parts-reservation-crash-%d", time.Now().UnixNano())
	fixture.register(t, revision)
	driver := newReservationTestDriver(2)
	transaction := retentionTransaction("parts_reservation_crash", 909, "0/909", 9)
	intent := transactionIntentForFence(t, fixture.fence, revision, transaction)
	baseline := managedBaselinePayload(t, transaction)
	if _, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, intent, transaction, baseline, driver); err == nil || !strings.Contains(err.Error(), "crash after reservation") {
		t.Fatalf("first delivery error=%v, want injected post-reservation crash", err)
	}
	var reservations, durable int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*),(SELECT count(*) FROM managed_part_reservation_parts part JOIN managed_part_reservations reservation USING(reservation_id) WHERE reservation.logical_batch_id=$1 AND reservation.destination_revision_id=$2 AND part.part_state='durable') FROM managed_part_reservations WHERE logical_batch_id=$1 AND destination_revision_id=$2 AND reservation_state='reserved'`, intent.LogicalBatchID, revision).Scan(&reservations, &durable); err != nil {
		t.Fatal(err)
	}
	if reservations != 1 || durable != 0 {
		t.Fatalf("after reservation crash reservations/durable=%d/%d, want 1/0", reservations, durable)
	}
	if _, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, intent, transaction, baseline, driver); err != nil {
		t.Fatalf("reservation adoption delivery: %v", err)
	}
	var total, completed int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*),count(*) FILTER (WHERE reservation_state='completed_pending_observation') FROM managed_part_reservations WHERE logical_batch_id=$1 AND destination_revision_id=$2`, intent.LogicalBatchID, revision).Scan(&total, &completed); err != nil {
		t.Fatal(err)
	}
	if total != 1 || completed != 1 {
		t.Fatalf("adopted reservation total/completed=%d/%d, want 1/1", total, completed)
	}
}

func TestClickHousePartReservationReclaimRequiresProvenAbsence(t *testing.T) {
	crash := true
	fixture := newPartReservationFixture(t, delivery.CoordinatorHooks{AfterPartReservationCommit: func(_ context.Context, _ authority.RunFence, _ connector.DeliveryIntent, _ string) error {
		if crash {
			crash = false
			return errors.New("injected abandonment after reservation")
		}
		return nil
	}})
	revision := fmt.Sprintf("clickhouse-parts-reclaim-%d", time.Now().UnixNano())
	fixture.register(t, revision)
	driver := newReservationTestDriver(2)
	transaction := retentionTransaction("parts_reclaim", 908, "0/908", 8)
	intent := transactionIntentForFence(t, fixture.fence, revision, transaction)
	if _, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, intent, transaction, managedBaselinePayload(t, transaction), driver); err == nil {
		t.Fatal("reservation abandonment was not injected")
	}
	takeoverFence := fixture.takeover(t)
	intent = transactionIntentForFence(t, takeoverFence, revision, transaction)
	driver.observationErr = fmt.Errorf("%w: one endpoint unavailable", connector.ErrDeliveryIndeterminate)
	if err := fixture.coordinator.ReclaimManagedPartReservation(fixture.ctx, takeoverFence, intent, driver); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("indeterminate reclaim error=%v", err)
	}
	var pending int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM managed_part_reservations WHERE logical_batch_id=$1 AND destination_revision_id=$2 AND reservation_state='reclaim_pending'`, intent.LogicalBatchID, revision).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("indeterminate reclaim pending reservations=%d, want 1", pending)
	}
	driver.observationErr = nil
	if err := fixture.coordinator.ReclaimManagedPartReservation(fixture.ctx, takeoverFence, intent, driver); err != nil {
		t.Fatal(err)
	}
	var released int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM managed_part_reservations WHERE logical_batch_id=$1 AND destination_revision_id=$2 AND reservation_state='released'`, intent.LogicalBatchID, revision).Scan(&released); err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("proven absence released reservations=%d, want 1", released)
	}
	if _, err := fixture.coordinator.DeliverTransaction(fixture.ctx, takeoverFence, intent, transaction, managedBaselinePayload(t, transaction), driver); err != nil {
		t.Fatalf("re-reserve exact released identity: %v", err)
	}
	var epoch, rereservedEvents int64
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT reservation_epoch FROM managed_part_reservations WHERE logical_batch_id=$1 AND destination_revision_id=$2`, intent.LogicalBatchID, revision).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM managed_part_reservation_events AS event JOIN managed_part_reservations AS reservation USING(reservation_id) WHERE reservation.logical_batch_id=$1 AND reservation.destination_revision_id=$2 AND event.event_kind='rereserved'`, intent.LogicalBatchID, revision).Scan(&rereservedEvents); err != nil {
		t.Fatal(err)
	}
	if epoch != 2 || rereservedEvents != 1 {
		t.Fatalf("re-reservation epoch/events=%d/%d, want 2/1", epoch, rereservedEvents)
	}
}

func TestClickHousePartReservationCrashRecovery(t *testing.T) {
	for _, test := range []struct {
		name      string
		failAfter int
	}{
		{name: "after fragment", failAfter: 1},
		{name: "after receipt", failAfter: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPartReservationFixture(t, delivery.CoordinatorHooks{})
			revision := fmt.Sprintf("clickhouse-parts-crash-%d", time.Now().UnixNano())
			fixture.register(t, revision)
			driver := newReservationTestDriver(4)
			driver.failAfter = test.failAfter
			transaction := retentionTransaction("parts_crash", uint32(910+test.failAfter), fmt.Sprintf("0/91%d", test.failAfter), int64(test.failAfter))
			intent := transactionIntentForFence(t, fixture.fence, revision, transaction)
			baseline := managedBaselinePayload(t, transaction)
			if _, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, intent, transaction, baseline, driver); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
				t.Fatalf("first delivery error=%v, want indeterminate crash", err)
			}
			var reservations, durable, completed int
			if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*),(SELECT count(*) FROM managed_part_reservation_parts part JOIN managed_part_reservations reservation USING(reservation_id) WHERE reservation.logical_batch_id=$1 AND reservation.destination_revision_id=$2 AND part.part_state='durable'),count(*) FILTER (WHERE reservation_state='completed_pending_observation') FROM managed_part_reservations WHERE logical_batch_id=$1 AND destination_revision_id=$2`, intent.LogicalBatchID, revision).Scan(&reservations, &durable, &completed); err != nil {
				t.Fatal(err)
			}
			wantDurable := test.failAfter - 1
			if reservations != 1 || durable != wantDurable || completed != 0 {
				t.Fatalf("after crash reservations/durable/completed=%d/%d/%d, want 1/%d/0 with only the crashing part progress rolled back", reservations, durable, completed, wantDurable)
			}
			if _, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, intent, transaction, baseline, driver); err != nil {
				t.Fatalf("recovery delivery: %v", err)
			}
			if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM managed_part_reservations WHERE logical_batch_id=$1 AND destination_revision_id=$2 AND reservation_state='completed_pending_observation'`, intent.LogicalBatchID, revision).Scan(&completed); err != nil {
				t.Fatal(err)
			}
			if completed != 1 {
				t.Fatalf("completed reservations=%d, want 1 pending a later observation", completed)
			}
		})
	}
}

func TestClickHousePartReservationRetentionDeletesChildrenBeforeParent(t *testing.T) {
	fixture := newPartReservationFixture(t, delivery.CoordinatorHooks{})
	revision := fmt.Sprintf("clickhouse-parts-retention-%d", time.Now().UnixNano())
	fixture.register(t, revision)
	driver := newReservationTestDriver(4)

	first := retentionTransaction("parts_retention", 920, "0/920", 20)
	firstIntent := transactionIntentForFence(t, fixture.fence, revision, first)
	firstGrant, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, firstIntent, first, managedBaselinePayload(t, first), driver)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.coordinator.CommitSourceFeedback(fixture.ctx, fixture.fence, firstGrant, &flushEvidenceTestSource{}); err != nil {
		t.Fatal(err)
	}

	second := retentionTransaction("parts_retention", 930, "0/930", 21)
	secondIntent := transactionIntentForFence(t, fixture.fence, revision, second)
	secondGrant, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, secondIntent, second, managedBaselinePayload(t, second), driver)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.coordinator.CommitSourceFeedback(fixture.ctx, fixture.fence, secondGrant, &flushEvidenceTestSource{}); err != nil {
		t.Fatal(err)
	}

	var reservationID, reservationState string
	var eventCount, partCount int
	if err := fixture.pool.QueryRow(fixture.ctx, `
SELECT reservation.reservation_id::text,reservation.reservation_state,
       (SELECT count(*) FROM managed_part_reservation_events AS event WHERE event.reservation_id=reservation.reservation_id),
       (SELECT count(*) FROM managed_part_reservation_parts AS part WHERE part.reservation_id=reservation.reservation_id)
FROM managed_part_reservations AS reservation
WHERE reservation.flow_incarnation_id=$1
  AND reservation.destination_revision_id=$2
  AND reservation.logical_batch_id=$3`, fixture.fence.FlowIncarnationID, revision, firstIntent.LogicalBatchID).Scan(&reservationID, &reservationState, &eventCount, &partCount); err != nil {
		t.Fatal(err)
	}
	if reservationState != "released" || eventCount == 0 || partCount == 0 {
		t.Fatalf("released reservation state/events/parts=%s/%d/%d, want released/nonzero/nonzero", reservationState, eventCount, partCount)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE delivery_manifests SET created_at=clock_timestamp()-interval '2 hours' WHERE flow_incarnation_id=$1 AND logical_batch_id=$2`, fixture.fence.FlowIncarnationID, firstIntent.LogicalBatchID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.coordinator.PruneTerminalDeliveryState(fixture.ctx, fixture.fence, time.Hour, 10); err != nil {
		t.Fatal(err)
	}

	var retainedEvents, retainedParts, retainedReservation, retainedManifest, currentManifest int
	if err := fixture.pool.QueryRow(fixture.ctx, `
SELECT
  (SELECT count(*) FROM managed_part_reservation_events WHERE reservation_id=$1::uuid),
  (SELECT count(*) FROM managed_part_reservation_parts WHERE reservation_id=$1::uuid),
  (SELECT count(*) FROM managed_part_reservations WHERE reservation_id=$1::uuid),
  (SELECT count(*) FROM delivery_manifests WHERE flow_incarnation_id=$2 AND logical_batch_id=$3),
  (SELECT count(*) FROM delivery_manifests WHERE flow_incarnation_id=$2 AND logical_batch_id=$4)`, reservationID, fixture.fence.FlowIncarnationID, firstIntent.LogicalBatchID, secondIntent.LogicalBatchID).Scan(&retainedEvents, &retainedParts, &retainedReservation, &retainedManifest, &currentManifest); err != nil {
		t.Fatal(err)
	}
	if retainedEvents != 0 || retainedParts != 0 || retainedReservation != 0 || retainedManifest != 0 || currentManifest != 1 {
		t.Fatalf("retained event/part/reservation/old-manifest/current-manifest=%d/%d/%d/%d/%d, want 0/0/0/0/1", retainedEvents, retainedParts, retainedReservation, retainedManifest, currentManifest)
	}
}
