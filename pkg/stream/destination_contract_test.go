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
			name: "all acknowledgement accepts one at-least-once destination",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "sink", Type: connector.EndpointHTTP},
				Dest: contractDestination{},
			}},
		},
		{
			name: "all acknowledgement rejects unsafe fan-out",
			dests: []DestinationConfig{
				{
					Spec: connector.RuntimeSpec{Name: "safe", Type: connector.EndpointPostgres},
					Dest: contractDestination{capabilities: safeSecondary},
				},
				{
					Spec: connector.RuntimeSpec{Name: "unsafe", Type: connector.EndpointHTTP},
					Dest: contractDestination{},
				},
			},
			wantError: "all acknowledgement fan-out requires replay-safe idempotent",
		},
		{
			name: "all acknowledgement accepts replay-safe idempotent destination",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "sink", Type: connector.EndpointPostgres},
				Dest: contractDestination{capabilities: safeSecondary},
			}},
		},
		{
			name: "lossy destination cannot be acknowledged",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "sink"},
				Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{Lossy: true}}},
			}},
			wantError: "may drop records",
		},
		{
			name: "auto apply requires DDL execution",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "sink"},
				Dest: contractDestination{},
			}},
			requireDDL: true,
			wantError:  "cannot execute DDL",
		},
		{
			name: "auto apply requires DDL reconciliation",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "sink"},
				Dest: contractDestination{capabilities: safePrimary},
			}},
			requireDDL: true,
			wantError:  "cannot reconcile DDL",
		},
		{
			name: "auto apply accepts replay-safe DDL destination",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "sink"},
				Dest: reconcilingContractDestination{contractDestination{capabilities: safePrimary}},
			}},
			requireDDL: true,
		},
		{
			name: "materialized acknowledgement rejects unsupported fan-out",
			dests: []DestinationConfig{
				{
					Spec: connector.RuntimeSpec{Name: "first", Type: connector.EndpointHTTP},
					Dest: contractDestination{},
				},
				{
					Spec: connector.RuntimeSpec{Name: "second", Type: connector.EndpointHTTP},
					Dest: contractDestination{},
				},
			},
			ack:       AckPolicyMaterialized,
			wantError: "exactly one destination revision",
		},
		{
			name: "materialized acknowledgement requires managed transaction destination identity",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "consumer", Type: connector.EndpointHTTP},
				Dest: contractDestination{},
			}},
			ack:       AckPolicyMaterialized,
			wantError: "full-transaction reconciliation or canonical artifact consumption",
		},
		{
			name: "materialized acknowledgement accepts canonical artifact consumer",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "consumer", Type: connector.EndpointIceberg},
				Dest: artifactContractDestination{contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{
					IdempotentReplay: true, ReplaySafe: true,
				}}}},
			}},
			ack: AckPolicyMaterialized,
		},
		{
			name: "materialized acknowledgement accepts one managed destination revision",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "consumer", Type: connector.EndpointPostgres},
				Dest: managedContractDestination{contractDestination{capabilities: safePrimary}},
			}},
			ack: AckPolicyMaterialized,
		},
		{
			name: "primary acknowledgement requires replay safety",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "primary"},
				Dest: contractDestination{capabilities: connector.Capabilities{Delivery: connector.DeliverySemantics{TransactionalBatch: true}}},
			}},
			ack:       AckPolicyPrimary,
			primary:   "primary",
			wantError: "replay-safe idempotent",
		},
		{
			name: "primary destination requires transactional batch",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "primary"},
				Dest: contractDestination{capabilities: safeSecondary},
			}},
			ack:       AckPolicyPrimary,
			primary:   "primary",
			wantError: "transactional batch writes",
		},
		{
			name: "primary acknowledgement validates secondaries",
			dests: []DestinationConfig{
				{Spec: connector.RuntimeSpec{Name: "primary"}, Dest: contractDestination{capabilities: safePrimary}},
				{Spec: connector.RuntimeSpec{Name: "secondary"}, Dest: contractDestination{}},
			},
			ack:       AckPolicyPrimary,
			primary:   "primary",
			wantError: "destination \"secondary\"",
		},
		{
			name: "primary destination must exist",
			dests: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "other"},
				Dest: contractDestination{capabilities: safeSecondary},
			}},
			ack:       AckPolicyPrimary,
			primary:   "primary",
			wantError: "not found",
		},
		{
			name: "valid primary acknowledgement contract",
			dests: []DestinationConfig{
				{Spec: connector.RuntimeSpec{Name: "primary"}, Dest: contractDestination{capabilities: safePrimary}},
				{Spec: connector.RuntimeSpec{Name: "secondary"}, Dest: contractDestination{capabilities: safeSecondary}},
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

func TestWriteDestinationRejectsUnclaimedPolicyBeforeExternalIO(t *testing.T) {
	destination := &writeCountingDestination{capabilities: connector.Capabilities{TableWrites: connector.TableWriteSemantics{Append: true}}}
	runner := &Runner{}
	config := DestinationConfig{Spec: connector.RuntimeSpec{Name: "append-only"}, Dest: destination}
	batch := connector.Batch{WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}}, Records: []connector.Record{{Operation: connector.OpInsert}}}
	if err := runner.writeDestination(context.Background(), config, batch); err == nil || !strings.Contains(err.Error(), "does not support explicit-key upsert") {
		t.Fatalf("writeDestination error=%v", err)
	}
	if destination.writes != 0 {
		t.Fatalf("external Write calls=%d", destination.writes)
	}
}

type writeCountingDestination struct {
	capabilities connector.Capabilities
	writes       int
}

func (*writeCountingDestination) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (d *writeCountingDestination) Write(context.Context, connector.Batch) error {
	d.writes++
	return nil
}
func (*writeCountingDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (*writeCountingDestination) TypeMappings() map[string]string { return nil }
func (*writeCountingDestination) Close(context.Context) error     { return nil }
func (d *writeCountingDestination) Capabilities() connector.Capabilities {
	return d.capabilities
}

type contractDestination struct {
	capabilities connector.Capabilities
}

func (contractDestination) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (contractDestination) Write(context.Context, connector.Batch) error      { return nil }
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
func (managedContractDestination) InitializeManagedDelivery(context.Context) error { return nil }
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
