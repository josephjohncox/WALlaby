package grpc

import (
	"context"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func admissionProtoFlow(id string) *wallabypb.Flow {
	destination := connector.RuntimeSpec{Name: "warehouse", Type: connector.EndpointPostgres}
	definition := flow.Flow{
		ID:           id,
		Source:       testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres}),
		Destinations: []*wallabypb.Endpoint{testDestinationEndpoint(destination)},
	}
	definition.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{destination})
	return flowToProtoForTest(definition)
}

func TestFlowServiceAdmitsOnlySafeFlowIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	service := NewFlowService(engine, nil)

	if _, err := service.CreateFlow(ctx, &wallabypb.CreateFlowRequest{Flow: admissionProtoFlow("valid-flow.1")}); err != nil {
		t.Fatalf("valid flow ID rejected: %v", err)
	}
	if _, err := service.CreateFlow(ctx, &wallabypb.CreateFlowRequest{Flow: admissionProtoFlow("")}); err != nil {
		t.Fatalf("server-assigned ID rejected: %v", err)
	}
	for _, id := range []string{"..", ".", "../../escape", "a/b", `a\b`, "a\x00b", "id\n", ".hidden", strings.Repeat("a", 129)} {
		if _, err := service.ValidateFlow(ctx, &wallabypb.ValidateFlowRequest{Flow: admissionProtoFlow(id)}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("ValidateFlow(%q) error=%v, want InvalidArgument", id, err)
		}
		if _, err := service.CreateFlow(ctx, &wallabypb.CreateFlowRequest{Flow: admissionProtoFlow(id)}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("CreateFlow(%q) error=%v, want InvalidArgument", id, err)
		}
		if _, err := service.UpdateFlow(ctx, &wallabypb.UpdateFlowRequest{Flow: admissionProtoFlow(id)}); err == nil {
			t.Errorf("UpdateFlow(%q) succeeded", id)
		}
		if _, err := service.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: id}); err == nil {
			t.Errorf("GetFlow(%q) returned a flow", id)
		}
	}
	listed, err := engine.List(ctx)
	if err != nil || len(listed) != 2 {
		t.Fatalf("persisted flows=%d err=%v, want only the two valid creates", len(listed), err)
	}
}

func TestFlowServiceRejectsPersistedFileSnapshotState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	service := NewFlowService(workflow.NewMemoryEngine(), nil)

	typedFile := func(id string) *wallabypb.Flow {
		definition := admissionProtoFlow(id)
		definition.Source.GetPostgresSource().SnapshotState = &wallabypb.SnapshotStateConfig{
			Backend: &wallabypb.SnapshotStateConfig_FilePath{FilePath: "/etc/cron.d/wallaby"},
		}
		return definition
	}
	if _, err := service.ValidateFlow(ctx, &wallabypb.ValidateFlowRequest{Flow: typedFile("snapshot-file")}); status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "snapshot_state") {
		t.Fatalf("ValidateFlow() error=%v, want persisted snapshot_state rejection", err)
	}
	if _, err := service.CreateFlow(ctx, &wallabypb.CreateFlowRequest{Flow: typedFile("snapshot-file")}); status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "snapshot_state") {
		t.Fatalf("CreateFlow() error=%v, want persisted snapshot_state rejection", err)
	}
	if _, err := service.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: "snapshot-file"}); status.Code(err) != codes.NotFound {
		t.Fatalf("rejected flow was persisted: %v", err)
	}

	if _, err := service.CreateFlow(ctx, &wallabypb.CreateFlowRequest{Flow: admissionProtoFlow("snapshot-ok")}); err != nil {
		t.Fatal(err)
	}
	update := typedFile("snapshot-ok")
	if _, err := service.UpdateFlow(ctx, &wallabypb.UpdateFlowRequest{Flow: update}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateFlow() error=%v, want InvalidArgument", err)
	}
}
