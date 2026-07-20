package stream

import (
	"context"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestValidateDestinationContracts(t *testing.T) {
	t.Parallel()

	safePrimary := connector.Capabilities{Delivery: connector.DeliverySemantics{
		Declared:           true,
		TransactionalBatch: true,
		IdempotentReplay:   true,
		ReplaySafe:         true,
		ExecutesDDL:        true,
	}}
	safeSecondary := safePrimary
	safeSecondary.Delivery.TransactionalBatch = false

	tests := []struct {
		name       string
		dests      []DestinationConfig
		ack        AckPolicy
		primary    string
		requireDDL bool
		wantError  string
	}{
		{
			name: "all acknowledgement accepts declared non-lossy at-least-once destination",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "sink", Type: connector.EndpointHTTP},
				Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Declared: true}}},
			}},
		},
		{
			name: "typed destination must declare semantics",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "sink", Type: connector.EndpointHTTP},
				Dest: contractDestination{},
			}},
			wantError: "does not declare delivery semantics",
		},
		{
			name: "lossy destination cannot be acknowledged",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "sink"},
				Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Declared: true, Lossy: true}}},
			}},
			wantError: "may drop records",
		},
		{
			name: "auto apply requires DDL execution",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "sink"},
				Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Declared: true}}},
			}},
			requireDDL: true,
			wantError:  "cannot execute DDL",
		},
		{
			name: "auto apply requires DDL reconciliation",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "sink"},
				Dest: contractDestination{capabilities: safePrimary},
			}},
			requireDDL: true,
			wantError:  "cannot reconcile DDL",
		},
		{
			name: "auto apply accepts replay-safe DDL destination",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "sink"},
				Dest: reconcilingContractDestination{contractDestination{capabilities: safePrimary}},
			}},
			requireDDL: true,
		},
		{
			name: "primary acknowledgement requires replay safety",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "primary"},
				Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Declared: true, TransactionalBatch: true}}},
			}},
			ack:       AckPolicyPrimary,
			primary:   "primary",
			wantError: "replay-safe idempotent",
		},
		{
			name: "primary destination requires transactional batch",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "primary"},
				Dest: contractDestination{capabilities: safeSecondary},
			}},
			ack:       AckPolicyPrimary,
			primary:   "primary",
			wantError: "transactional batch writes",
		},
		{
			name: "primary acknowledgement validates secondaries",
			dests: []DestinationConfig{
				{Spec: connector.Spec{Name: "primary"}, Dest: contractDestination{capabilities: safePrimary}},
				{Spec: connector.Spec{Name: "secondary"}, Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Declared: true}}}},
			},
			ack:       AckPolicyPrimary,
			primary:   "primary",
			wantError: "destination \"secondary\"",
		},
		{
			name: "primary destination must exist",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "other"},
				Dest: contractDestination{capabilities: safeSecondary},
			}},
			ack:       AckPolicyPrimary,
			primary:   "primary",
			wantError: "not found",
		},
		{
			name: "valid primary acknowledgement contract",
			dests: []DestinationConfig{
				{Spec: connector.Spec{Name: "primary"}, Dest: contractDestination{capabilities: safePrimary}},
				{Spec: connector.Spec{Name: "secondary"}, Dest: contractDestination{capabilities: safeSecondary}},
			},
			ack:     AckPolicyPrimary,
			primary: "primary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateDestinationContracts(tt.dests, tt.ack, tt.primary, tt.requireDDL)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("ValidateDestinationContracts() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("ValidateDestinationContracts() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

type contractDestination struct {
	capabilities connector.Capabilities
}

func (contractDestination) Open(context.Context, connector.Spec) error   { return nil }
func (contractDestination) Write(context.Context, connector.Batch) error { return nil }
func (contractDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (contractDestination) TypeMappings() map[string]string { return nil }
func (contractDestination) Close(context.Context) error     { return nil }
func (d contractDestination) Capabilities() connector.Capabilities {
	return d.capabilities
}

type reconcilingContractDestination struct {
	contractDestination
}

func (reconcilingContractDestination) ReconcileDDL(context.Context, connector.Schema, connector.Record) (connector.DDLReconcileResult, error) {
	return connector.DDLReconcileNotApplied, nil
}
