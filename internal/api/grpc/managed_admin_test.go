package grpc

import (
	"context"
	"errors"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	f := flow.Flow{ID: "legacy-admin", Source: connector.Spec{Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": "postgres://127.0.0.1:1/unreachable", "slot": "configured_slot", "publication": "configured_publication",
	}}}
	if _, err := engine.Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	service := NewFlowService(engine, nil)
	_, err := service.DropReplicationSlot(ctx, &wallabypb.DropReplicationSlotRequest{FlowId: f.ID, Dsn: f.Source.Options["dsn"], Slot: "configured_slot"})
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
	f := flow.Flow{ID: "guarded-admin", Source: connector.Spec{Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": "postgres://host/exact_database", "slot": "exact_slot", "source_system_identifier": "exact_system",
	}}}
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
	existing := flow.Flow{ID: "guarded-reconfigure", Source: connector.Spec{Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn":                      "postgres://127.0.0.1:1/exact_database",
		"publication":              "exact_publication",
		"publication_tables":       "public.original",
		"source_system_identifier": "exact_system",
		"sync_publication":         "true",
	}}}
	if _, err := engine.Create(ctx, existing); err != nil {
		t.Fatal(err)
	}
	requested := existing
	requested.Source.Options = make(map[string]string, len(existing.Source.Options))
	for key, value := range existing.Source.Options {
		requested.Source.Options[key] = value
	}
	requested.Source.Options["publication_tables"] = "public.changed"
	_, err := NewFlowService(engine, nil).ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{Flow: flowToProto(requested)})
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
	if got := stored.Source.Options["publication_tables"]; got != "public.original" {
		t.Fatalf("flow was updated before ownership guard: publication_tables=%q", got)
	}
}

func TestManagedAdministrativeResourceMutationsFailClosed(t *testing.T) {
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	managed := flow.Flow{
		ID: "managed-admin",
		Source: connector.Spec{Type: connector.EndpointPostgres, Options: map[string]string{
			"managed": "true", "dsn": "postgres://unused", "slot": "owned_slot", "publication": "owned_publication",
		}},
	}
	if _, err := engine.Create(ctx, managed); err != nil {
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
}
