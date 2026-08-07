package grpc

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestReconfigureFlowOptionalBooleansDriveEngineLifecycle(t *testing.T) {
	ctx := context.Background()
	falseValue := false
	for _, tt := range []struct {
		name                    string
		pauseFirst, resumeAfter *bool
		wantPause, wantResume   int
	}{
		{name: "omitted defaults pause and resume", wantPause: 1, wantResume: 1},
		{name: "explicit false keeps runner active", pauseFirst: &falseValue, resumeAfter: &falseValue},
	} {
		t.Run(tt.name, func(t *testing.T) {
			engine := &recordingLifecycleEngine{MemoryEngine: workflow.NewMemoryEngine()}
			definition := mappedGRPCTestFlow(flow.Flow{ID: "reconfigure-defaults", Name: "before", Source: testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{"sync_publication": "false"}})})
			if _, err := engine.Create(ctx, definition); err != nil {
				t.Fatal(err)
			}
			if _, err := engine.Start(ctx, definition.ID); err != nil {
				t.Fatal(err)
			}
			definition.Name = "after"
			response, err := NewFlowService(engine, nil).ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{
				Flow: flowToProtoForTest(definition), PauseFirst: tt.pauseFirst, ResumeAfter: tt.resumeAfter, SyncPublication: &falseValue,
			})
			if err != nil {
				t.Fatal(err)
			}
			if engine.pauseCalls != tt.wantPause || engine.resumeCalls != tt.wantResume {
				t.Fatalf("Pause/Resume calls=%d/%d, want %d/%d", engine.pauseCalls, engine.resumeCalls, tt.wantPause, tt.wantResume)
			}
			if response.State != wallabypb.FlowState_FLOW_STATE_RUNNING || response.Name != "after" {
				t.Fatalf("reconfigured flow=%+v, want running updated flow", response)
			}
			stored, err := engine.Get(ctx, definition.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Source.GetPostgresSource().GetSyncPublication() {
				t.Fatal("stored sync_publication=true, want explicit false")
			}
		})
	}
}

func TestReconfigureFlowSyncPublicationExplicitFalseOverridesStoredTrue(t *testing.T) {
	ctx := context.Background()
	falseValue := false
	newFlow := func(id string) flow.Flow {
		return mappedGRPCTestFlow(flow.Flow{ID: id, Name: "before", Source: testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{
			"dsn": "postgres://unreachable.invalid/wallaby", "publication": "wallaby_publication", "publication_tables": "public.widgets", "sync_publication": "true",
		}})})
	}

	t.Run("explicit false bypasses ownership guard and network and persists", func(t *testing.T) {
		engine := workflow.NewMemoryEngine()
		definition := newFlow("sync-publication-explicit-false")
		if _, err := engine.Create(ctx, definition); err != nil {
			t.Fatal(err)
		}
		definition.Name = "after"
		response, err := NewFlowService(engine, nil).ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{
			Flow: flowToProtoForTest(definition), PauseFirst: &falseValue, ResumeAfter: &falseValue, SyncPublication: &falseValue,
		})
		if err != nil {
			t.Fatalf("explicit false reached source ownership/network synchronization: %v", err)
		}
		responseSource, err := endpointcodec.Decode(response.Source, endpointcodec.RoleSource)
		if err != nil {
			t.Fatal(err)
		}
		if responseSource.Options["sync_publication"] != "false" {
			t.Fatalf("response sync_publication=%q, want false", responseSource.Options["sync_publication"])
		}
		stored, err := engine.Get(ctx, definition.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Source.GetPostgresSource().GetSyncPublication() {
			t.Fatal("stored sync_publication=true, want false")
		}
	})

	t.Run("omitted inherits stored true and invokes ownership guard", func(t *testing.T) {
		engine := workflow.NewMemoryEngine()
		definition := newFlow("sync-publication-omitted")
		if _, err := engine.Create(ctx, definition); err != nil {
			t.Fatal(err)
		}
		_, err := NewFlowService(engine, nil).ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{
			Flow: flowToProtoForTest(definition), PauseFirst: &falseValue, ResumeAfter: &falseValue,
		})
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "ownership guard") {
			t.Fatalf("omitted sync_publication error=%v, want stored-true ownership guard", err)
		}
		stored, getErr := engine.Get(ctx, definition.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if !stored.Source.GetPostgresSource().GetSyncPublication() || stored.Name != "before" {
			t.Fatalf("guarded flow mutated: %+v", stored)
		}
	})
}

type recordingLifecycleEngine struct {
	*workflow.MemoryEngine
	pauseCalls  int
	resumeCalls int
}

func (e *recordingLifecycleEngine) Pause(ctx context.Context, flowID string) (flow.Flow, error) {
	e.pauseCalls++
	return e.MemoryEngine.Pause(ctx, flowID)
}

func (e *recordingLifecycleEngine) Resume(ctx context.Context, flowID string) (flow.Flow, error) {
	e.resumeCalls++
	return e.MemoryEngine.Resume(ctx, flowID)
}

func TestCleanupFlowOptionalBooleansDrivePostgresDrops(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
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

	falseValue, trueValue := false, true
	for _, tt := range []struct {
		name                                       string
		dropSlot, dropPublication, dropSourceState *bool
		wantSlot, wantPublication, wantSourceState bool
	}{
		{name: "omitted drops slot and state but retains publication", wantPublication: true},
		{name: "explicit values retain slot and state but drop publication", dropSlot: &falseValue, dropPublication: &trueValue, dropSourceState: &falseValue, wantSlot: true, wantSourceState: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			suffix := fmt.Sprintf("%d", time.Now().UnixNano())
			slot := "wallaby_cleanup_slot_" + suffix
			publication := "wallaby_cleanup_publication_" + suffix
			stateSchema := "wallaby_cleanup_state_" + suffix
			if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s", publication)); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, "SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1,'pgoutput')", slot); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s; CREATE TABLE %s.source_state (id TEXT PRIMARY KEY,source_name TEXT,slot_name TEXT NOT NULL,publication_name TEXT NOT NULL,state TEXT NOT NULL,options JSONB NOT NULL DEFAULT '{}'::jsonb,last_lsn TEXT,last_ack_at TIMESTAMPTZ,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`, stateSchema, stateSchema)); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.source_state(id,slot_name,publication_name,state) VALUES ($1,$1,$2,'ready')`, stateSchema), slot, publication); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_, _ = pool.Exec(context.Background(), "SELECT pg_catalog.pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1 AND NOT active)", slot)
				_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publication))
				_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", stateSchema))
			}()
			engine := workflow.NewMemoryEngine()
			definition := mappedGRPCTestFlow(flow.Flow{ID: "cleanup-defaults-" + suffix, Source: testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{
				"dsn": dsn, "slot": slot, "publication": publication, "state_schema": stateSchema, "state_table": "source_state",
			}})})
			if _, err := engine.Create(ctx, definition); err != nil {
				t.Fatal(err)
			}
			_, err := NewFlowService(engine, nil).CleanupFlow(ctx, &wallabypb.CleanupFlowRequest{
				FlowId: definition.ID, DropSlot: tt.dropSlot, DropPublication: tt.dropPublication, DropSourceState: tt.dropSourceState,
			})
			if err != nil {
				t.Fatal(err)
			}
			var slotExists, publicationExists, sourceStateExists bool
			if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)", slot).Scan(&slotExists); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_publication WHERE pubname=$1)", publication).Scan(&publicationExists); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s.source_state WHERE id=$1)", stateSchema), slot).Scan(&sourceStateExists); err != nil {
				t.Fatal(err)
			}
			if slotExists != tt.wantSlot || publicationExists != tt.wantPublication || sourceStateExists != tt.wantSourceState {
				t.Fatalf("slot/publication/source-state exists=%t/%t/%t, want %t/%t/%t", slotExists, publicationExists, sourceStateExists, tt.wantSlot, tt.wantPublication, tt.wantSourceState)
			}
		})
	}
}

func TestDirectDSNResourceMutationsFailBeforeNetwork(t *testing.T) {
	ctx := context.Background()
	service := NewFlowService(workflow.NewMemoryEngine(), nil)
	tests := []func() error{
		func() error {
			_, err := service.DropReplicationSlot(ctx, &wallabypb.DropReplicationSlotRequest{Dsn: "postgres://127.0.0.1:1/unreachable", Slot: "slot"})
			return err
		},
		func() error {
			_, err := service.AddPublicationTables(ctx, &wallabypb.AddPublicationTablesRequest{Dsn: "postgres://127.0.0.1:1/unreachable", Publication: "publication", Tables: []string{"public.t"}})
			return err
		},
		func() error {
			_, err := service.SyncPublicationTables(ctx, &wallabypb.SyncPublicationTablesRequest{Dsn: "postgres://127.0.0.1:1/unreachable", Publication: "publication"})
			return err
		},
		func() error {
			_, err := service.ScrapePublicationTables(ctx, &wallabypb.ScrapePublicationTablesRequest{Dsn: "postgres://127.0.0.1:1/unreachable", Publication: "publication", Schemas: []string{"public"}, Apply: true})
			return err
		},
	}
	for index, call := range tests {
		if code := status.Code(call()); code != codes.FailedPrecondition {
			t.Fatalf("mutation %d status=%s, want FailedPrecondition", index, code)
		}
	}
}

func TestFlowBoundResourceMutationRejectsOverridesBeforeNetwork(t *testing.T) {
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	f := mappedGRPCTestFlow(flow.Flow{ID: "legacy-admin", Source: testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": "postgres://127.0.0.1:1/unreachable", "slot": "configured_slot", "publication": "configured_publication",
	}})})
	if _, err := engine.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	service := NewFlowService(engine, nil)
	_, err := service.DropReplicationSlot(ctx, &wallabypb.DropReplicationSlotRequest{FlowId: f.ID, Dsn: f.Source.GetPostgresSource().GetConnection().GetDsn(), Slot: "configured_slot"})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("DSN override status=%s, want InvalidArgument", code)
	}
	_, err = service.AddPublicationTables(ctx, &wallabypb.AddPublicationTablesRequest{FlowId: f.ID, Publication: "other_publication", Tables: []string{"public.t"}})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("physical-name override status=%s, want InvalidArgument", code)
	}
	_, err = service.SyncPublicationTables(ctx, &wallabypb.SyncPublicationTablesRequest{FlowId: f.ID, Publication: "configured_publication", Tables: []string{"public.t"}})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("exact physical-name override status=%s, want InvalidArgument", code)
	}
}

type rejectingResourceGuardEngine struct {
	*workflow.MemoryEngine
	sourceSystemID string
	databaseName   string
	resourceKind   string
	physicalName   string
}

func (e *rejectingResourceGuardEngine) CheckLegacySourceResourceMutation(_ context.Context, sourceSystemID, databaseName, resourceKind, physicalName string) error {
	e.sourceSystemID, e.databaseName, e.resourceKind, e.physicalName = sourceSystemID, databaseName, resourceKind, physicalName
	return errors.New("managed resource exists")
}

func TestLegacyMutationConsultsManagedResourceOwnershipBeforeNetwork(t *testing.T) {
	ctx := context.Background()
	engine := &rejectingResourceGuardEngine{MemoryEngine: workflow.NewMemoryEngine()}
	f := mappedGRPCTestFlow(flow.Flow{ID: "guarded-admin", Source: testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": "postgres://host/exact_database", "slot": "exact_slot", "source_system_identifier": "exact_system",
	}})})
	if _, err := engine.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	_, err := NewFlowService(engine, nil).DropReplicationSlot(ctx, &wallabypb.DropReplicationSlotRequest{FlowId: f.ID})
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Fatalf("status=%s, want FailedPrecondition", code)
	}
	if engine.sourceSystemID != "exact_system" || engine.databaseName != "exact_database" || engine.resourceKind != "slot" || engine.physicalName != "exact_slot" {
		t.Fatalf("guard identity=(%q,%q,%q,%q)", engine.sourceSystemID, engine.databaseName, engine.resourceKind, engine.physicalName)
	}
}

func TestReconfigurePublicationConsultsGuardBeforeUpdateOrNetwork(t *testing.T) {
	ctx := context.Background()
	engine := &rejectingResourceGuardEngine{MemoryEngine: workflow.NewMemoryEngine()}
	existing := flow.Flow{ID: "guarded-reconfigure", Source: testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn":                      "postgres://127.0.0.1:1/exact_database",
		"publication":              "exact_publication",
		"publication_tables":       "public.original",
		"source_system_identifier": "exact_system",
		"sync_publication":         "true",
	}})}
	existing = mappedGRPCTestFlow(existing)
	if _, err := engine.Create(ctx, existing); err != nil {
		t.Fatal(err)
	}
	requested := flow.Clone(existing)
	requested.Source.GetPostgresSource().PublicationTables = []string{"public.changed"}
	_, err := NewFlowService(engine, nil).ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{Flow: flowToProtoForTest(requested)})
	if code := status.Code(err); code != codes.FailedPrecondition {
		t.Fatalf("status=%s, want FailedPrecondition before update or source network access", code)
	}
	if engine.sourceSystemID != "exact_system" || engine.databaseName != "exact_database" || engine.resourceKind != "publication" || engine.physicalName != "exact_publication" {
		t.Fatalf("guard identity=(%q,%q,%q,%q)", engine.sourceSystemID, engine.databaseName, engine.resourceKind, engine.physicalName)
	}
	stored, err := engine.Get(ctx, existing.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.Source.GetPostgresSource().GetPublicationTables(); len(got) != 1 || got[0] != "public.original" {
		t.Fatalf("flow was updated before ownership guard: publication_tables=%v", got)
	}
}

func TestManagedProfileOnlyUpdateAndReconfigureFailClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, tt := range []struct {
		name             string
		existingManaged  bool
		requestedManaged bool
	}{
		{name: "existing profile", existingManaged: true, requestedManaged: true},
		{name: "requested profile", requestedManaged: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			engine := workflow.NewMemoryEngine()
			existing := flow.Flow{ID: "profile-update-" + strings.ReplaceAll(tt.name, " ", "-"), Source: testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{}})}
			if tt.existingManaged {
				existing.Source.GetPostgresSource().ManagedProfile = wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRES_TO_POSTGRES_V1
			}
			existing = mappedGRPCTestFlow(existing)
			if _, err := engine.Create(ctx, existing); err != nil {
				t.Fatal(err)
			}
			requested := flow.Clone(existing)
			if tt.requestedManaged {
				requested.Source.GetPostgresSource().ManagedProfile = wallabypb.ManagedProfile_MANAGED_PROFILE_POSTGRES_TO_POSTGRES_V1
			}
			service := NewFlowService(engine, nil)
			if _, err := service.UpdateFlow(ctx, &wallabypb.UpdateFlowRequest{Flow: flowToProtoForTest(requested)}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("UpdateFlow status=%s, want FailedPrecondition", status.Code(err))
			}
			if _, err := service.ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{Flow: flowToProtoForTest(requested)}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("ReconfigureFlow status=%s, want FailedPrecondition", status.Code(err))
			}
		})
	}
}

func TestManagedAdministrativeResourceMutationsFailClosed(t *testing.T) {
	testManagedAdministrativeResourceMutationsFailClosed(t, map[string]string{"managed": "true"})
}

func TestManagedProfileOnlyAdministrativeResourceMutationsFailClosed(t *testing.T) {
	testManagedAdministrativeResourceMutationsFailClosed(t, map[string]string{"managed_profile": connector.ManagedProfilePostgresToPostgresV1})
}

func testManagedAdministrativeResourceMutationsFailClosed(t *testing.T, managedOptions map[string]string) {
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	options := maps.Clone(managedOptions)
	options["dsn"] = "postgres://unused"
	options["slot"] = "owned_slot"
	options["publication"] = "owned_publication"
	managed := mappedGRPCTestFlow(flow.Flow{
		ID:     "managed-admin-" + strings.ReplaceAll(t.Name(), "/", "-"),
		Source: testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: options}),
	})
	if _, err := engine.Create(ctx, managed); err != nil {
		t.Fatal(err)
	}
	beforeControl, err := engine.Control(ctx, managed.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeFlow, err := engine.Get(ctx, managed.ID)
	if err != nil {
		t.Fatal(err)
	}
	service := NewFlowService(engine, nil)
	tests := []struct {
		name string
		call func() error
	}{
		{name: "cleanup", call: func() error {
			_, err := service.CleanupFlow(ctx, &wallabypb.CleanupFlowRequest{FlowId: managed.ID})
			return err
		}},
		{name: "drop slot", call: func() error {
			_, err := service.DropReplicationSlot(ctx, &wallabypb.DropReplicationSlotRequest{FlowId: managed.ID, Slot: "owned_slot"})
			return err
		}},
		{name: "add publication tables", call: func() error {
			_, err := service.AddPublicationTables(ctx, &wallabypb.AddPublicationTablesRequest{FlowId: managed.ID, Publication: "owned_publication", Tables: []string{"public.extra"}})
			return err
		}},
		{name: "drop publication tables", call: func() error {
			_, err := service.DropPublicationTables(ctx, &wallabypb.DropPublicationTablesRequest{FlowId: managed.ID, Publication: "owned_publication", Tables: []string{"public.extra"}})
			return err
		}},
		{name: "sync publication tables", call: func() error {
			_, err := service.SyncPublicationTables(ctx, &wallabypb.SyncPublicationTablesRequest{FlowId: managed.ID, Publication: "owned_publication", Tables: []string{"public.extra"}})
			return err
		}},
		{name: "scrape apply", call: func() error {
			_, err := service.ScrapePublicationTables(ctx, &wallabypb.ScrapePublicationTablesRequest{FlowId: managed.ID, Publication: "owned_publication", Schemas: []string{"public"}, Apply: true})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if code := status.Code(tt.call()); code != codes.FailedPrecondition {
				t.Fatalf("status=%s, want FailedPrecondition", code)
			}
		})
	}
	afterControl, err := engine.Control(ctx, managed.ID)
	if err != nil {
		t.Fatal(err)
	}
	afterFlow, err := engine.Get(ctx, managed.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterControl.Generation != beforeControl.Generation || afterControl.Target != beforeControl.Target || afterFlow.State != beforeFlow.State || !proto.Equal(afterFlow.Source, beforeFlow.Source) {
		t.Fatalf("rejected managed mutations changed ownership generation or flow: before=%+v/%+v after=%+v/%+v", beforeControl, beforeFlow, afterControl, afterFlow)
	}
}
