package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type nonBindableSchemaHook struct{}

func (nonBindableSchemaHook) OnSchema(context.Context, connector.Schema) error { return nil }
func (nonBindableSchemaHook) OnSchemaChange(context.Context, internalschema.Plan) error {
	return nil
}
func (nonBindableSchemaHook) OnDDL(context.Context, string, pglogrepl.LSN) error { return nil }

func TestManagedSourceBindRunFenceRejectsNonBindableSchemaHook(t *testing.T) {
	fence := connector.RunFence{
		FlowID: "flow", FlowIncarnationID: uuid.New(), Generation: 1,
		ExecutionID: "execution", AcquisitionID: uuid.New(), LeaseEpoch: 1,
	}
	err := (&Source{SchemaHook: nonBindableSchemaHook{}}).BindRunFence(fence)
	if err == nil || !strings.Contains(err.Error(), "does not accept") {
		t.Fatalf("BindRunFence error=%v, want fail-closed schema-hook rejection", err)
	}
}

func TestManagedSourceOpenRejectsMutationBeforeNetwork(t *testing.T) {
	for _, mode := range []struct {
		name  string
		key   string
		value string
	}{
		{name: "legacy", key: optManaged, value: "true"},
		{name: "profile only", key: "managed_profile", value: "postgres_to_postgres_v1"},
	} {
		t.Run(mode.name, func(t *testing.T) {
			base := map[string]string{
				optDSN: "postgres://127.0.0.1:1/unreachable", optSlot: "slot", optPublication: "publication", mode.key: mode.value,
				optCreateSlot: "false", optEnsureState: "false", optEnsurePublication: "false", optSyncPublication: "false",
			}
			for _, option := range []string{optCreateSlot, optEnsureState, optEnsurePublication, optSyncPublication} {
				t.Run(option, func(t *testing.T) {
					options := make(map[string]string, len(base))
					for key, value := range base {
						options[key] = value
					}
					options[option] = "true"
					err := (&Source{}).Open(context.Background(), connector.Spec{Type: connector.EndpointPostgres, Options: options})
					if err == nil || !strings.Contains(err.Error(), option+"=false") {
						t.Fatalf("error=%v, want local %s mutation rejection", err, option)
					}
				})
			}
		})
	}
}
