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
			name: "all acknowledgement accepts one declared at-least-once destination",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "sink", Type: connector.EndpointHTTP},
				Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Declared: true}}},
			}},
		},
		{
			name: "all acknowledgement rejects unsafe fan-out",
			dests: []DestinationConfig{
				{
					Spec: connector.Spec{Name: "safe", Type: connector.EndpointPostgres},
					Dest: contractDestination{capabilities: safeSecondary},
				},
				{
					Spec: connector.Spec{Name: "unsafe", Type: connector.EndpointHTTP},
					Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Declared: true}}},
				},
			},
			wantError: "all acknowledgement fan-out requires replay-safe idempotent",
		},
		{
			name: "all acknowledgement accepts replay-safe idempotent destination",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "sink", Type: connector.EndpointPostgres},
				Dest: contractDestination{capabilities: safeSecondary},
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
			name: "materialized acknowledgement rejects unsupported fan-out",
			dests: []DestinationConfig{
				{
					Spec: connector.Spec{Name: "first", Type: connector.EndpointHTTP},
					Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Declared: true}}},
				},
				{
					Spec: connector.Spec{Name: "second", Type: connector.EndpointHTTP},
					Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Declared: true}}},
				},
			},
			ack:       AckPolicyMaterialized,
			wantError: "exactly one destination revision",
		},
		{
			name: "materialized acknowledgement requires managed transaction destination identity",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "consumer", Type: connector.EndpointHTTP},
				Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Declared: true}}},
			}},
			ack:       AckPolicyMaterialized,
			wantError: "full-transaction reconciliation or canonical artifact consumption",
		},
		{
			name: "materialized acknowledgement accepts canonical artifact consumer",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "consumer", Type: connector.EndpointIceberg},
				Dest: artifactContractDestination{contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{
					Declared: true, IdempotentReplay: true, ReplaySafe: true,
				}}}},
			}},
			ack: AckPolicyMaterialized,
		},
		{
			name: "materialized acknowledgement accepts one managed destination revision",
			dests: []DestinationConfig{{
				Spec: connector.Spec{Name: "consumer", Type: connector.EndpointPostgres},
				Dest: managedContractDestination{contractDestination{capabilities: safePrimary}},
			}},
			ack: AckPolicyMaterialized,
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

type artifactContractDestination struct {
	contractDestination
}

func (artifactContractDestination) CanonicalArtifactConsumer() {}

type managedContractDestination struct {
	contractDestination
}

func (managedContractDestination) Apply(context.Context, connector.DeliveryIntent, connector.Batch) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, nil
}
func (managedContractDestination) Reconcile(context.Context, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
}
func (managedContractDestination) ValidateTransaction(context.Context, connector.SourceTransaction) error {
	return nil
}
func (managedContractDestination) ApplyTransaction(context.Context, connector.DeliveryIntent, connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, nil
}

type reconcilingContractDestination struct {
	contractDestination
}

func (reconcilingContractDestination) ReconcileDDL(context.Context, connector.Schema, connector.Record) (connector.DDLReconcileResult, error) {
	return connector.DDLReconcileNotApplied, nil
}
