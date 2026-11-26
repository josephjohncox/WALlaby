package flow

import (
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func admissionEndpoint(t *testing.T, role endpointcodec.Role, spec connector.RuntimeSpec) *wallabypb.Endpoint {
	t.Helper()
	endpoint, err := endpointcodec.Encode(spec, role)
	if err != nil {
		t.Fatal(err)
	}
	return endpoint
}

func admissionFlow(t *testing.T, id string, sourceOptions map[string]string) Flow {
	t.Helper()
	options := map[string]string{"mode": connector.SourceModeCDC}
	for key, value := range sourceOptions {
		options[key] = value
	}
	destination := connector.RuntimeSpec{Name: "dest", Type: connector.EndpointPostgres}
	definition := Flow{
		ID:           id,
		Source:       admissionEndpoint(t, endpointcodec.RoleSource, connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: options}),
		Destinations: []*wallabypb.Endpoint{admissionEndpoint(t, endpointcodec.RoleDestination, destination)},
	}
	definition.Config.TableMappings = NewTableMappings([]connector.RuntimeSpec{destination})
	return definition
}

func TestValidateIDAcceptsOnlyBoundedSafeASCII(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"a", "0", "flow-1", "Flow_2.v3", "a..b", "a.", "9f0c1e2a-0000-4000-8000-000000000000", strings.Repeat("a", 128)} {
		if err := ValidateID(id); err != nil {
			t.Errorf("ValidateID(%q) = %v, want nil", id, err)
		}
	}
	for _, id := range []string{
		"", ".", "..", ".hidden", "-lead", "_lead", "../x", "a/b", `a\b`, "/abs", "a b", "a\x00b", "a\nb", "id\n", "a\tb", "a\x7f",
		"caf\u00e9", "\u202e", strings.Repeat("a", 129),
	} {
		if err := ValidateID(id); err == nil {
			t.Errorf("ValidateID(%q) = nil, want rejection", id)
		}
	}
}

func TestValidateDefinitionFlowIDAdmission(t *testing.T) {
	t.Parallel()
	if err := ValidateDefinition(admissionFlow(t, "", nil)); err != nil {
		t.Fatalf("empty ID must remain valid for pre-create authoring validation: %v", err)
	}
	if err := ValidateDefinition(admissionFlow(t, "flow-1", nil)); err != nil {
		t.Fatalf("valid ID rejected: %v", err)
	}
	for _, id := range []string{"..", ".", "../../etc/cron.d/x", "a/b", "a\x00b", "id\n", strings.Repeat("a", 129)} {
		if err := ValidateDefinition(admissionFlow(t, id, nil)); err == nil || !strings.Contains(err.Error(), "flow id") {
			t.Errorf("ValidateDefinition(id=%q) error=%v, want flow id rejection", id, err)
		}
	}
}

func TestValidateDefinitionRejectsPersistedFileSnapshotState(t *testing.T) {
	t.Parallel()
	for name, options := range map[string]map[string]string{
		"file backend with path": {"snapshot_state_backend": "file", "snapshot_state_path": "/etc/cron.d/wallaby"},
		"file backend":           {"snapshot_state_backend": "file"},
	} {
		if err := ValidateDefinition(admissionFlow(t, "flow-1", options)); err == nil || !strings.Contains(err.Error(), "snapshot_state") {
			t.Errorf("%s: error=%v, want persisted snapshot_state rejection", name, err)
		}
	}
	// Durable and disabled backends remain valid persisted configuration.
	for name, options := range map[string]map[string]string{
		"postgres backend": {"snapshot_state_backend": "postgres", "snapshot_state_dsn": "postgres://state.example/db"},
		"disabled backend": {"snapshot_state_backend": "none"},
	} {
		if err := ValidateDefinition(admissionFlow(t, "flow-1", options)); err != nil {
			t.Errorf("%s rejected: %v", name, err)
		}
	}
}
