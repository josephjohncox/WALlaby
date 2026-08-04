package tests

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresAuthorizedSourceFlushRejectsStaleWorker(t *testing.T) {
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

	flowID := fmt.Sprintf("source-flush-fence-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres}, Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres}}, Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.Spec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	oldFence, err := authorityStore.AcquireProducer(ctx, flowID, "old-feedback", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := coordinator.AuthorizeAck(ctx, oldFence, connector.Checkpoint{LSN: "0/D0"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, oldFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	newFence, err := authorityStore.AcquireProducer(ctx, flowID, "new-feedback", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	source := &flushEvidenceTestSource{}
	if err := coordinator.CommitSourceFeedback(ctx, oldFence, grant, source); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale feedback error=%v, want ErrFenceRejected", err)
	}
	if source.calls != 0 {
		t.Fatalf("stale source feedback calls=%d, want zero", source.calls)
	}
	if err := coordinator.CommitSourceFeedback(ctx, newFence, grant, source); err != nil {
		t.Fatal(err)
	}
	if source.calls != 1 {
		t.Fatalf("current source feedback calls=%d, want one", source.calls)
	}
	var observed string
	var acquisition string
	if err := pool.QueryRow(ctx, `
SELECT observed_flush_lsn,acquisition_id::text
FROM source_ack_receipts
WHERE flow_incarnation_id=$1 AND position_id=$2`, newFence.FlowIncarnationID, grant.PositionID).Scan(&observed, &acquisition); err != nil {
		t.Fatal(err)
	}
	if observed != "0/D0" || acquisition != newFence.AcquisitionID.String() {
		t.Fatalf("source flush receipt=(%s,%s), want 0/D0/%s", observed, acquisition, newFence.AcquisitionID)
	}

	grantAfterCrash, err := coordinator.AuthorizeAck(ctx, newFence, connector.Checkpoint{LSN: "0/E0"})
	if err != nil {
		t.Fatal(err)
	}
	injectedCrash := errors.New("injected crash after source flush before ACK receipt")
	crashCoordinator, err := delivery.NewCoordinator(ctx, pool, delivery.WithCoordinatorHooks(delivery.CoordinatorHooks{
		AfterSourceFlush: func(context.Context, authority.RunFence, connector.AckGrant, string) error {
			return injectedCrash
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	crashSource := &flushEvidenceTestSource{}
	if err := crashCoordinator.CommitSourceFeedback(ctx, newFence, grantAfterCrash, crashSource); !errors.Is(err, injectedCrash) {
		t.Fatalf("source-flush crash boundary error=%v, want injected crash", err)
	}
	if crashSource.calls != 1 {
		t.Fatalf("source flush calls before injected crash=%d, want 1", crashSource.calls)
	}
	var receiptsBeforeRecovery int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=$1 AND position_id=$2`, newFence.FlowIncarnationID, grantAfterCrash.PositionID).Scan(&receiptsBeforeRecovery); err != nil {
		t.Fatal(err)
	}
	if receiptsBeforeRecovery != 0 {
		t.Fatalf("ACK receipts after injected crash=%d, want 0", receiptsBeforeRecovery)
	}
	if err := coordinator.CommitSourceFeedback(ctx, newFence, grantAfterCrash, crashSource); err != nil {
		t.Fatalf("repair flushed source checkpoint without receipt: %v", err)
	}
	var repairedObserved string
	if err := pool.QueryRow(ctx, `SELECT observed_flush_lsn FROM source_ack_receipts WHERE flow_incarnation_id=$1 AND position_id=$2`, newFence.FlowIncarnationID, grantAfterCrash.PositionID).Scan(&repairedObserved); err != nil {
		t.Fatal(err)
	}
	if repairedObserved != "0/E0" {
		t.Fatalf("repaired source flush receipt=%s, want 0/E0", repairedObserved)
	}
}

type flushEvidenceTestSource struct{ calls int }

func (*flushEvidenceTestSource) Open(context.Context, connector.Spec) error { return nil }
func (*flushEvidenceTestSource) Read(context.Context) (connector.Batch, error) {
	return connector.Batch{}, io.EOF
}
func (*flushEvidenceTestSource) Ack(context.Context, connector.Checkpoint) error { return nil }
func (s *flushEvidenceTestSource) AckWithEvidence(_ context.Context, checkpoint connector.Checkpoint) (connector.SourceFlushEvidence, error) {
	s.calls++
	return connector.SourceFlushEvidence{ObservedFlushLSN: checkpoint.LSN}, nil
}
func (*flushEvidenceTestSource) Close(context.Context) error { return nil }
func (*flushEvidenceTestSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, SupportsStreaming: true}
}
