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

func (f *partReservationFixture) register(t *testing.T, revision string) {
	t.Helper()
	if err := f.coordinator.RegisterDestinationRevision(f.ctx, f.fence, revision, "clickhouse", "managed-parts-v1"); err != nil {
		t.Fatal(err)
	}
	f.revisions = append(f.revisions, revision)
}

type reservationTestDriver struct {
	connector.ManagedTransactionDestination
	capacity    uint64
	activeParts uint64
	failAfter   int
	proveErr    error

	mu             sync.Mutex
	prepared       map[string]int
	receiptApplied map[string]bool
}

func newReservationTestDriver(capacity uint64) *reservationTestDriver {
	return &reservationTestDriver{capacity: capacity, prepared: make(map[string]int), receiptApplied: make(map[string]bool)}
}

func (d *reservationTestDriver) PrepareTransaction(_ context.Context, intent connector.DeliveryIntent, _ connector.SourceTransaction) (connector.PreparedManagedTransaction, error) {
	d.mu.Lock()
	d.prepared[intent.LogicalBatchID]++
	attempt := d.prepared[intent.LogicalBatchID]
	d.mu.Unlock()
	parts := []connector.ManagedPartIdentity{
		{Kind: "changelog", Ordinal: 0, QueryID: "test-fragment-" + intent.LogicalBatchID},
		{Kind: "receipt", Ordinal: 0, QueryID: "test-receipt-" + intent.LogicalBatchID},
	}
	failAfter := 0
	if attempt == 1 {
		failAfter = d.failAfter
	}
	return &reservationTestPrepared{driver: d, intent: intent, request: connector.ManagedPartReservationRequest{
		Resource: connector.ManagedPartResourceClickHouseActivePartsV1, DestinationRevisionID: intent.DestinationRevisionID,
		LogicalBatchID: intent.LogicalBatchID, ContentHash: intent.ContentHash,
		ServerActiveParts: d.activeParts, Capacity: d.capacity, Parts: parts,
	}, failAfter: failAfter}, nil
}

func (d *reservationTestDriver) ValidateTransaction(context.Context, connector.SourceTransaction) error {
	return nil
}
func (d *reservationTestDriver) InitializeManagedDelivery(context.Context) error { return nil }
func (d *reservationTestDriver) ApplyTransaction(context.Context, connector.DeliveryIntent, connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, errors.New("direct transaction apply is forbidden in reservation test driver")
}
func (d *reservationTestDriver) ProvePartReservationAbsent(context.Context, connector.DeliveryIntent) error {
	return d.proveErr
}
func (d *reservationTestDriver) Reconcile(_ context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	d.mu.Lock()
	applied := d.receiptApplied[intent.LogicalBatchID]
	d.mu.Unlock()
	if applied {
		return connector.DeliveryApplied, connector.DeliveryEvidence{ExternalID: "test-external-" + intent.LogicalBatchID, ContentHash: intent.ContentHash}, nil
	}
	return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
}

type reservationTestPrepared struct {
	driver      *reservationTestDriver
	intent      connector.DeliveryIntent
	request     connector.ManagedPartReservationRequest
	reservation connector.ManagedPartReservation
	failAfter   int
}

func (p *reservationTestPrepared) PartReservationRequest() (connector.ManagedPartReservationRequest, error) {
	return p.request, p.request.Validate()
}
func (p *reservationTestPrepared) BindPartReservation(reservation connector.ManagedPartReservation) error {
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
		if err := p.reservation.MarkPartDurable(ctx, part); err != nil {
			return connector.DeliveryEvidence{}, err
		}
		if part.Kind == "receipt" {
			p.driver.mu.Lock()
			p.driver.receiptApplied[p.intent.LogicalBatchID] = true
			p.driver.mu.Unlock()
		}
		if p.failAfter == index+1 {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: injected crash after %s part", connector.ErrDeliveryIndeterminate, part.Kind)
		}
	}
	return connector.DeliveryEvidence{ExternalID: "test-external-" + p.intent.LogicalBatchID, ContentHash: p.intent.ContentHash}, nil
}

func TestClickHousePartReservationSerializesConcurrentWriters(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var firstBatch string
	fixture := newPartReservationFixture(t, delivery.CoordinatorHooks{AfterPartReservationCommit: func(ctx context.Context, _ authority.RunFence, intent connector.DeliveryIntent, _ string) error {
		if firstBatch == "" {
			firstBatch = intent.LogicalBatchID
		}
		if intent.LogicalBatchID != firstBatch {
			return nil
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	revision := fmt.Sprintf("clickhouse-parts-concurrent-%d", time.Now().UnixNano())
	fixture.register(t, revision)
	driver := newReservationTestDriver(2)
	first := retentionTransaction("parts_first", 901, "0/901", 1)
	second := retentionTransaction("parts_second", 902, "0/902", 2)
	firstIntent := transactionIntentForFence(t, fixture.fence, revision, first)
	secondIntent := transactionIntentForFence(t, fixture.fence, revision, second)
	firstErr := make(chan error, 1)
	go func() {
		_, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, firstIntent, first, managedBaselinePayload(t, first), driver)
		firstErr <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first writer did not persist and hold its reservation")
	}
	_, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, secondIntent, second, managedBaselinePayload(t, second), driver)
	if err == nil || !strings.Contains(err.Error(), "reserved parts=2") {
		t.Fatalf("second writer error=%v, want atomic reserved-part rejection", err)
	}
	close(release)
	if err := <-firstErr; err != nil {
		t.Fatalf("first writer: %v", err)
	}
	var released, reserved int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FILTER (WHERE reservation_state='released'),count(*) FILTER (WHERE reservation_state='reserved') FROM managed_part_reservations WHERE destination_revision_id=$1`, revision).Scan(&released, &reserved); err != nil {
		t.Fatal(err)
	}
	if released != 1 || reserved != 0 {
		t.Fatalf("released/reserved=%d/%d, want 1/0", released, reserved)
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
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*),(SELECT count(*) FROM managed_part_reservation_parts part JOIN managed_part_reservations reservation USING(reservation_id) WHERE reservation.logical_batch_id=$1 AND part.part_state='durable') FROM managed_part_reservations WHERE logical_batch_id=$1 AND reservation_state='reserved'`, intent.LogicalBatchID).Scan(&reservations, &durable); err != nil {
		t.Fatal(err)
	}
	if reservations != 1 || durable != 0 {
		t.Fatalf("after reservation crash reservations/durable=%d/%d, want 1/0", reservations, durable)
	}
	if _, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, intent, transaction, baseline, driver); err != nil {
		t.Fatalf("reservation adoption delivery: %v", err)
	}
	var total, released int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*),count(*) FILTER (WHERE reservation_state='released') FROM managed_part_reservations WHERE logical_batch_id=$1`, intent.LogicalBatchID).Scan(&total, &released); err != nil {
		t.Fatal(err)
	}
	if total != 1 || released != 1 {
		t.Fatalf("adopted reservation total/released=%d/%d, want 1/1", total, released)
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
	driver.proveErr = fmt.Errorf("%w: one endpoint unavailable", connector.ErrDeliveryIndeterminate)
	if err := fixture.coordinator.ReclaimManagedPartReservation(fixture.ctx, fixture.fence, intent, driver); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("indeterminate reclaim error=%v", err)
	}
	var reserved int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM managed_part_reservations WHERE logical_batch_id=$1 AND reservation_state='reserved'`, intent.LogicalBatchID).Scan(&reserved); err != nil {
		t.Fatal(err)
	}
	if reserved != 1 {
		t.Fatalf("indeterminate reclaim retained reservations=%d, want 1", reserved)
	}
	driver.proveErr = nil
	if err := fixture.coordinator.ReclaimManagedPartReservation(fixture.ctx, fixture.fence, intent, driver); err != nil {
		t.Fatal(err)
	}
	var released int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM managed_part_reservations WHERE logical_batch_id=$1 AND reservation_state='released'`, intent.LogicalBatchID).Scan(&released); err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("proven absence released reservations=%d, want 1", released)
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
			var reservations, durable, released int
			if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*),(SELECT count(*) FROM managed_part_reservation_parts part JOIN managed_part_reservations reservation USING(reservation_id) WHERE reservation.logical_batch_id=$1 AND part.part_state='durable'),count(*) FILTER (WHERE reservation_state='released') FROM managed_part_reservations WHERE logical_batch_id=$1`, intent.LogicalBatchID).Scan(&reservations, &durable, &released); err != nil {
				t.Fatal(err)
			}
			if reservations != 1 || durable != test.failAfter || released != 0 {
				t.Fatalf("after crash reservations/durable/released=%d/%d/%d, want 1/%d/0", reservations, durable, released, test.failAfter)
			}
			if _, err := fixture.coordinator.DeliverTransaction(fixture.ctx, fixture.fence, intent, transaction, baseline, driver); err != nil {
				t.Fatalf("recovery delivery: %v", err)
			}
			if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM managed_part_reservations WHERE logical_batch_id=$1 AND reservation_state='released'`, intent.LogicalBatchID).Scan(&released); err != nil {
				t.Fatal(err)
			}
			if released != 1 {
				t.Fatalf("released reservations=%d, want 1", released)
			}
		})
	}
}
