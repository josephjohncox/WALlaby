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
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestFencedSchemaRegistrationScopesCatalogAndFlowProvenance(t *testing.T) {
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
		if _, err := engine.Create(ctx, flow.Flow{ID: id, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
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

	// The same WAL position may exist in two flows. Fenced plan replay must use
	// both the active incarnation and flow identity rather than adopting another
	// flow's approved plan.
	sharedLSN := pglogrepl.LSN(0x90)
	firstPlan := internalschema.Plan{Changes: []internalschema.Change{{Type: internalschema.ChangeAddColumn, Namespace: "public", Table: "events", Column: "first_only", ToType: "text"}}}
	otherPlan := internalschema.Plan{Changes: []internalschema.Change{{Type: internalschema.ChangeAddColumn, Namespace: "public", Table: "events", Column: "other_only", ToType: "text"}}}
	if err := firstHook.OnSchemaChangeAtLSN(ctx, firstPlan, sharedLSN); err != nil {
		t.Fatal(err)
	}
	firstEvent, err := registryStore.GetDDLByLSN(ctx, flowID, sharedLSN.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := registryStore.SetDDLStatus(ctx, firstEvent.ID, registry.StatusApproved); err != nil {
		t.Fatal(err)
	}
	if err := otherHook.OnSchemaChangeAtLSN(ctx, otherPlan, sharedLSN); err != nil {
		t.Fatal(err)
	}
	otherEvent, err := registryStore.GetDDLByLSN(ctx, otherFlowID, sharedLSN.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := registryStore.SetDDLStatus(ctx, otherEvent.ID, registry.StatusApproved); err != nil {
		t.Fatal(err)
	}
	resolvedFirst, err := firstHook.ResolveSchemaChangeAtLSN(ctx, internalschema.Plan{}, sharedLSN)
	if err != nil {
		t.Fatal(err)
	}
	resolvedOther, err := otherHook.ResolveSchemaChangeAtLSN(ctx, internalschema.Plan{}, sharedLSN)
	if err != nil {
		t.Fatal(err)
	}
	if len(resolvedFirst.Changes) != 1 || resolvedFirst.Changes[0].Column != "first_only" {
		t.Fatalf("first flow resolved plan=%+v, want first_only", resolvedFirst)
	}
	if len(resolvedOther.Changes) != 1 || resolvedOther.Changes[0].Column != "other_only" {
		t.Fatalf("other flow resolved plan=%+v, want other_only", resolvedOther)
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
	if err := firstHook.OnSchema(ctx, global); err != nil {
		t.Fatalf("flow-scoped schema must coexist with catalog scope: %v", err)
	}
	if _, found, err := registryStore.LatestSchema(ctx, global.Namespace, global.Name); err != nil || !found {
		t.Fatalf("catalog schema lookup found=%t err=%v", found, err)
	}
	if _, found, err := registryStore.LatestSchemaForFlow(ctx, flowID, global.Namespace, global.Name); err != nil || !found {
		t.Fatalf("flow schema lookup found=%t err=%v", found, err)
	}

	foreign := newSchema(fmt.Sprintf("schema_fence_%d_foreign", suffix))
	foreign.Columns = append(foreign.Columns, connector.Column{Name: "other_flow_only", Type: "text"})
	if err := otherHook.OnSchema(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	if _, found, err := firstHook.SchemaBaseline(ctx, foreign.Namespace, foreign.Name); err != nil || found {
		t.Fatalf("foreign flow baseline leaked into first flow: found=%t err=%v", found, err)
	}
	firstForeign := newSchema(foreign.Namespace)
	if err := firstHook.OnSchema(ctx, firstForeign); err != nil {
		t.Fatalf("same relation identity in a different flow must not collide: %v", err)
	}
	loadedFirst, found, err := firstHook.SchemaBaseline(ctx, foreign.Namespace, foreign.Name)
	if err != nil || !found || len(loadedFirst.Columns) != 1 {
		t.Fatalf("first flow baseline=%+v found=%t err=%v, want isolated one-column schema", loadedFirst, found, err)
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
