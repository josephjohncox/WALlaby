package workflow

import (
	"context"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func admissionTestFlow(id string, sourceOptions map[string]string) flow.Flow {
	options := map[string]string{"dsn": "postgres://source.example/db"}
	for key, value := range sourceOptions {
		options[key] = value
	}
	return mappedTestFlow(flow.Flow{
		ID:           id,
		Source:       workflowTestSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres, Options: options}),
		Destinations: []*wallabypb.Endpoint{workflowTestDestination(connector.RuntimeSpec{Name: "dest", Type: connector.EndpointPostgres})},
	})
}

func TestMemoryEngineFailsClosedOnUnsafeFlowIDsAndFileSnapshotState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	engine := NewMemoryEngine()
	if _, err := engine.Create(ctx, admissionTestFlow("flow-1", nil)); err != nil {
		t.Fatalf("valid create: %v", err)
	}
	if _, err := engine.Create(ctx, admissionTestFlow("", nil)); err == nil || !strings.Contains(err.Error(), "flow id is required") {
		t.Fatalf("empty ID Create error=%v, want existing required-ID behavior", err)
	}
	for _, id := range []string{"..", "../escape", "a/b", "a\x00b", "id\n", strings.Repeat("a", 129)} {
		if _, err := engine.Create(ctx, admissionTestFlow(id, nil)); err == nil || !strings.Contains(err.Error(), "flow id") {
			t.Errorf("Create(%q) error=%v, want flow id rejection", id, err)
		}
		if _, err := engine.Update(ctx, admissionTestFlow(id, nil)); err == nil || !strings.Contains(err.Error(), "flow id") {
			t.Errorf("Update(%q) error=%v, want flow id rejection", id, err)
		}
		if _, err := engine.Get(ctx, id); err == nil {
			t.Errorf("Get(%q) returned a flow", id)
		}
	}
	file := map[string]string{"snapshot_state_backend": "file", "snapshot_state_path": "/etc/cron.d/wallaby"}
	if _, err := engine.Create(ctx, admissionTestFlow("flow-2", file)); err == nil || !strings.Contains(err.Error(), "snapshot_state") {
		t.Fatalf("Create(file snapshot) error=%v", err)
	}
	if _, err := engine.Update(ctx, admissionTestFlow("flow-1", file)); err == nil || !strings.Contains(err.Error(), "snapshot_state") {
		t.Fatalf("Update(file snapshot) error=%v", err)
	}
	listed, err := engine.List(ctx)
	if err != nil || len(listed) != 1 || listed[0].ID != "flow-1" {
		t.Fatalf("stored flows=%v err=%v, want only flow-1", listed, err)
	}
}

// Persisted rows are validated on every load, so a row written by an older
// binary or directly into the database cannot reach a worker.
func TestPersistedFlowRowsFailClosedOnUnsafeIDAndFileSnapshotState(t *testing.T) {
	t.Parallel()
	decode := func(definition flow.Flow) error {
		source, destinations, config, err := marshalFlowFields(definition)
		if err != nil {
			t.Fatal(err)
		}
		loaded := flow.Flow{ID: definition.ID}
		return decodeFlowWithRegistry(&loaded, source, destinations, config, string(flow.StateCreated), nil, 1, connector.DefaultRegistry)
	}
	if err := decode(admissionTestFlow("flow-1", nil)); err != nil {
		t.Fatalf("valid persisted row: %v", err)
	}
	for _, id := range []string{"..", "../escape", "a/b", "a\x00b"} {
		if err := decode(admissionTestFlow(id, nil)); err == nil || !strings.Contains(err.Error(), "flow id") {
			t.Errorf("persisted ID %q error=%v, want flow id rejection", id, err)
		}
	}
	file := map[string]string{"snapshot_state_backend": "file", "snapshot_state_path": "/etc/cron.d/wallaby"}
	if err := decode(admissionTestFlow("flow-1", file)); err == nil || !strings.Contains(err.Error(), "snapshot_state") {
		t.Errorf("persisted file snapshot row error=%v, want snapshot_state rejection", err)
	}
}
