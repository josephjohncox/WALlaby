package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestFencedSchemaRegistrationRejectsForeignProvenance(t *testing.T) {
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

	suffix := time.Now().UnixNano()
	flowID := fmt.Sprintf("schema-fence-%d", suffix)
	otherFlowID := fmt.Sprintf("schema-fence-other-%d", suffix)
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	defer cleanupAuthorityTest(context.Background(), pool, otherFlowID)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM schema_versions WHERE namespace LIKE $1", fmt.Sprintf("schema_fence_%d_%%", suffix))
	}()

	acquire := func(id, execution string) authority.RunFence {
		t.Helper()
		if _, err := engine.Create(ctx, flow.Flow{ID: id}); err != nil {
			t.Fatal(err)
		}
		_, control, err := engine.PlanStart(ctx, id, false)
		if err != nil {
			t.Fatal(err)
		}
		fence, err := authorityStore.AcquireProducer(ctx, id, execution, "test", control.Generation, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return fence
	}
	first := acquire(flowID, "schema-worker-1")
	other := acquire(otherFlowID, "schema-worker-other")
	firstHook := &registry.Hook{Store: registryStore, FlowID: flowID}
	if err := firstHook.BindRunFence(first); err != nil {
		t.Fatal(err)
	}
	otherHook := &registry.Hook{Store: registryStore, FlowID: otherFlowID}
	if err := otherHook.BindRunFence(other); err != nil {
		t.Fatal(err)
	}
	newSchema := func(namespace string) connector.Schema {
		return connector.Schema{
			Namespace: namespace,
			Name:      "events",
			Version:   1,
			Columns:   []connector.Column{{Name: "id", Type: "bigint"}},
		}
	}

	global := newSchema(fmt.Sprintf("schema_fence_%d_global", suffix))
	if err := registryStore.RegisterSchema(ctx, global); err != nil {
		t.Fatal(err)
	}
	if err := firstHook.OnSchema(ctx, global); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("global schema collision error=%v, want ErrDeliveryConflict", err)
	}

	foreign := newSchema(fmt.Sprintf("schema_fence_%d_foreign", suffix))
	if err := otherHook.OnSchema(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	if err := firstHook.OnSchema(ctx, foreign); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("other-incarnation schema collision error=%v, want ErrDeliveryConflict", err)
	}

	idempotent := newSchema(fmt.Sprintf("schema_fence_%d_idempotent", suffix))
	if err := firstHook.OnSchema(ctx, idempotent); err != nil {
		t.Fatal(err)
	}
	if err := firstHook.OnSchema(ctx, idempotent); err != nil {
		t.Fatalf("same-acquisition idempotent registration: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, first.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	replacement, err := authorityStore.AcquireProducer(ctx, flowID, "schema-worker-2", "test", first.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	replacementHook := &registry.Hook{Store: registryStore, FlowID: flowID}
	if err := replacementHook.BindRunFence(replacement); err != nil {
		t.Fatal(err)
	}
	if err := replacementHook.OnSchema(ctx, idempotent); err != nil {
		t.Fatalf("same-incarnation replacement idempotent registration: %v", err)
	}
	changed := idempotent
	changed.Columns = append(changed.Columns, connector.Column{Name: "payload", Type: "text"})
	if err := replacementHook.OnSchema(ctx, changed); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("same-incarnation different-content collision error=%v, want ErrDeliveryConflict", err)
	}
}
