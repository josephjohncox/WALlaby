package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRegistryAndDDLReceiptsRejectStaleTakeover(t *testing.T) {
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
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	registryStore, err := registry.NewPostgresStoreWithPool(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("registry-fence-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres}, Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres}}, Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.Spec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	first, err := authorityStore.AcquireProducer(ctx, flowID, "registry-worker-1", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	firstHook := &registry.Hook{Store: registryStore, FlowID: flowID}
	if err := firstHook.BindRunFence(first); err != nil {
		t.Fatal(err)
	}
	firstLSN := pglogrepl.LSN(0x100)
	if err := firstHook.OnDDL(ctx, "ALTER TABLE public.events ADD COLUMN first text", firstLSN); err != nil {
		t.Fatal(err)
	}
	firstEvent, err := registryStore.GetDDLByLSN(ctx, flowID, firstLSN.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := registryStore.SetDDLStatus(ctx, firstEvent.ID, registry.StatusApproved); err != nil {
		t.Fatal(err)
	}
	staleExecutions, err := registryStore.ForRunFence(first)
	if err != nil {
		t.Fatal(err)
	}
	if state, err := staleExecutions.PrepareDDLExecution(ctx, flowID, firstLSN.String(), "target", []string{"target"}); err != nil || state != connector.DDLExecutionNew {
		t.Fatalf("first DDL preparation state=%v error=%v", state, err)
	}

	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, first.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	second, err := authorityStore.AcquireProducer(ctx, flowID, "registry-worker-2", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstHook.OnDDL(ctx, "ALTER TABLE public.events ADD COLUMN stale text", pglogrepl.LSN(0x110)); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale registry write error=%v, want ErrFenceRejected", err)
	}
	if _, err := staleExecutions.PrepareDDLExecution(ctx, flowID, firstLSN.String(), "target", []string{"target"}); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale DDL attempt error=%v, want ErrFenceRejected", err)
	}

	secondExecutions, err := registryStore.ForRunFence(second)
	if err != nil {
		t.Fatal(err)
	}
	state, err := secondExecutions.PrepareDDLExecution(ctx, flowID, firstLSN.String(), "target", []string{"target"})
	if err != nil {
		t.Fatal(err)
	}
	if state != connector.DDLExecutionRetry {
		t.Fatalf("takeover DDL attempt state=%v, want retry after prior acquisition attempt", state)
	}
	if err := staleExecutions.RecordDDLExecution(ctx, flowID, firstLSN.String(), firstEvent.DDL, "target", []string{"target"}); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale DDL receipt error=%v, want ErrFenceRejected", err)
	}
	if err := secondExecutions.RecordDDLExecution(ctx, flowID, firstLSN.String(), firstEvent.DDL, "target", []string{"target"}); err != nil {
		t.Fatal(err)
	}
	applied, err := registryStore.GetDDL(ctx, firstEvent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if applied.Status != registry.StatusApplied {
		t.Fatalf("DDL status=%q, want applied", applied.Status)
	}

	rows, err := pool.Query(ctx, `
SELECT acquisition_id::text
FROM ddl_execution_run_attempts
WHERE event_id=$1 AND destination='target'
ORDER BY started_at,attempt_id`, firstEvent.ID)
	if err != nil {
		t.Fatal(err)
	}
	var attemptAcquisitions []string
	for rows.Next() {
		var acquisition string
		if err := rows.Scan(&acquisition); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		attemptAcquisitions = append(attemptAcquisitions, acquisition)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wantAttempts := map[string]int{first.AcquisitionID.String(): 1, second.AcquisitionID.String(): 1}
	gotAttempts := make(map[string]int, len(attemptAcquisitions))
	for _, acquisition := range attemptAcquisitions {
		gotAttempts[acquisition]++
	}
	if len(attemptAcquisitions) != 2 || gotAttempts[first.AcquisitionID.String()] != wantAttempts[first.AcquisitionID.String()] || gotAttempts[second.AcquisitionID.String()] != wantAttempts[second.AcquisitionID.String()] {
		t.Fatalf("append-only DDL attempt provenance=%v (counts=%v), want exactly one attempt from each acquisition %v", attemptAcquisitions, gotAttempts, wantAttempts)
	}
	var receiptAcquisition string
	if err := pool.QueryRow(ctx, `SELECT acquisition_id::text FROM ddl_execution_receipts WHERE event_id=$1 AND destination='target'`, firstEvent.ID).Scan(&receiptAcquisition); err != nil {
		t.Fatal(err)
	}
	if receiptAcquisition != second.AcquisitionID.String() {
		t.Fatalf("replacement DDL receipt provenance=%s, want %s", receiptAcquisition, second.AcquisitionID)
	}
}
