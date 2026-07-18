package stream

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRunnerDDLReceiptPreventsReapplicationAfterReplay(t *testing.T) {
	t.Parallel()

	destination := &ddlPolicyDestination{}
	receipts := &testDDLReceiptStore{}
	runner := Runner{
		FlowID: "flow-ddl-replay",
		Destinations: []DestinationConfig{{
			Spec: connector.Spec{Name: "dest"},
			Dest: destination,
		}},
		RequireDDLExecution: true,
		DDLExecutions:       receipts,
	}
	batch := connector.Batch{
		Schema: connector.Schema{Name: "widgets"},
		Records: []connector.Record{{
			Table:     "widgets",
			Operation: connector.OpDDL,
			DDL:       "ALTER TABLE widgets ADD COLUMN extra text",
		}},
		Checkpoint: connector.Checkpoint{LSN: "0/10"},
	}
	for range 2 {
		if err := runner.writeDestination(context.Background(), runner.Destinations[0], batch); err != nil {
			t.Fatal(err)
		}
	}
	if destination.applied != 1 {
		t.Fatalf("ApplyDDL calls=%d, want 1 across replay", destination.applied)
	}
	if destination.writes != 2 {
		t.Fatalf("Write calls=%d, want 2 across replay", destination.writes)
	}
}

func TestRunnerUsesPerRecordPositionsForDDLReceipts(t *testing.T) {
	t.Parallel()

	destination := &ddlPolicyDestination{}
	receipts := &testDDLReceiptStore{}
	runner := Runner{
		FlowID: "flow-ddl-multi-position",
		Destinations: []DestinationConfig{{
			Spec: connector.Spec{Name: "dest"},
			Dest: destination,
		}},
		RequireDDLExecution: true,
		DDLExecutions:       receipts,
	}
	batch := connector.Batch{
		Schema: connector.Schema{Name: "widgets"},
		Records: []connector.Record{
			{
				Operation:      connector.OpDDL,
				DDL:            "ALTER TABLE widgets ADD COLUMN first text",
				SourcePosition: "0/10",
			},
			{
				Operation:      connector.OpDDL,
				DDL:            "ALTER TABLE widgets ADD COLUMN second text",
				SourcePosition: "0/11",
			},
		},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}

	for range 2 {
		if err := runner.writeDestination(context.Background(), runner.Destinations[0], batch); err != nil {
			t.Fatal(err)
		}
	}
	if destination.applied != 2 {
		t.Fatalf("ApplyDDL calls=%d, want one per source position across replay", destination.applied)
	}
	for _, position := range []string{"0/10", "0/11"} {
		if _, ok := receipts.receipts[ddlReceiptTestKey(runner.FlowID, position, "dest")]; !ok {
			t.Fatalf("missing DDL receipt at source position %s", position)
		}
	}
}

func TestRunnerRejectsDuplicateDDLSourcePositionsBeforeExecution(t *testing.T) {
	t.Parallel()

	destination := &ddlPolicyDestination{}
	receipts := &testDDLReceiptStore{
		onPrepare: func(string, string, string, []string) (bool, error) {
			t.Fatal("execution prepared for ambiguous DDL positions")
			return false, nil
		},
		onRecord: func(string, string, string, string, []string) error {
			t.Fatal("receipt persisted for ambiguous DDL positions")
			return nil
		},
	}
	runner := Runner{
		FlowID: "flow-ddl-duplicate-position",
		Destinations: []DestinationConfig{{
			Spec: connector.Spec{Name: "dest"},
			Dest: destination,
		}},
		RequireDDLExecution: true,
		DDLExecutions:       receipts,
	}
	batch := connector.Batch{
		Records: []connector.Record{
			{Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN first text"},
			{Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN second text"},
		},
		Checkpoint: connector.Checkpoint{LSN: "0/10"},
	}

	err := runner.writeDestination(context.Background(), runner.Destinations[0], batch)
	if err == nil || !strings.Contains(err.Error(), "multiple DDL records share source position") {
		t.Fatalf("writeDestination() error=%v, want duplicate source-position rejection", err)
	}
	if destination.applied != 0 || destination.writes != 0 {
		t.Fatalf("destination mutated before position validation: applied=%d writes=%d", destination.applied, destination.writes)
	}
}

func TestRunnerValidatesDDLManifestBeforeDestinationExecution(t *testing.T) {
	t.Parallel()

	destination := &ddlPolicyDestination{}
	manifestErr := errors.New("execution manifest changed")
	receipts := &testDDLReceiptStore{
		onPrepare: func(string, string, string, []string) (bool, error) {
			return false, manifestErr
		},
		onRecord: func(string, string, string, string, []string) error {
			t.Fatal("receipt persisted after manifest preflight failed")
			return nil
		},
	}
	runner := Runner{
		FlowID: "flow-ddl-manifest",
		Destinations: []DestinationConfig{{
			Spec: connector.Spec{Name: "dest"},
			Dest: destination,
		}},
		RequireDDLExecution: true,
		DDLExecutions:       receipts,
	}
	batch := connector.Batch{
		Records:    []connector.Record{{Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN extra text"}},
		Checkpoint: connector.Checkpoint{LSN: "0/10"},
	}

	err := runner.writeDestination(context.Background(), runner.Destinations[0], batch)
	if !errors.Is(err, manifestErr) {
		t.Fatalf("writeDestination() error=%v, want manifest preflight error", err)
	}
	if destination.applied != 0 || destination.writes != 0 {
		t.Fatalf("destination mutated before manifest validation: applied=%d writes=%d", destination.applied, destination.writes)
	}
}

func TestRunnerSkipsDDLExecutionWhenPolicyDisabled(t *testing.T) {
	t.Parallel()

	destination := &ddlPolicyDestination{}
	runner := Runner{Destinations: []DestinationConfig{{
		Spec: connector.Spec{Name: "dest"},
		Dest: destination,
	}}}
	batch := connector.Batch{
		Schema: connector.Schema{Name: "widgets"},
		Records: []connector.Record{{
			Table:     "widgets",
			Operation: connector.OpDDL,
			DDL:       "ALTER TABLE widgets ADD COLUMN extra text",
		}},
	}
	if err := runner.writeDestinations(context.Background(), batch, runner.Destinations); err != nil {
		t.Fatal(err)
	}
	if destination.applied != 0 {
		t.Fatalf("ApplyDDL calls=%d, want 0", destination.applied)
	}
}

func TestRunnerMarksDDLApplied(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		record  connector.Record
		ddlText string
	}{
		{
			name:    "raw ddl",
			record:  connector.Record{Table: "widgets", Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN extra text"},
			ddlText: "ALTER TABLE",
		},
		{
			name: "plan ddl",
			record: func() connector.Record {
				planBytes, err := json.Marshal(schema.Plan{
					Changes: []schema.Change{
						{Type: schema.ChangeAddColumn, Namespace: "public", Table: "widgets", Column: "extra", ToType: "text"},
					},
				})
				if err != nil {
					t.Fatalf("marshal ddl plan: %v", err)
				}
				return connector.Record{
					Table:         "widgets",
					SchemaVersion: 0,
					Operation:     connector.OpDDL,
					DDLPlan:       planBytes,
				}
			}(),
			ddlText: `{"Changes":`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch := connector.Batch{
				Schema:     connector.Schema{Name: "widgets"},
				Records:    []connector.Record{tc.record},
				Checkpoint: connector.Checkpoint{LSN: "0/1"},
			}
			var applied int64
			receipts := &testDDLReceiptStore{onRecord: func(_ string, lsn, ddl, destination string, expected []string) error {
				if lsn == "" || ddl == "" {
					t.Fatalf("expected lsn+ddl, got lsn=%q ddl=%q", lsn, ddl)
				}
				if tc.ddlText != "" && !strings.HasPrefix(ddl, tc.ddlText) {
					t.Fatalf("unexpected ddl payload %q", ddl)
				}
				if destination != "dest" || len(expected) != 1 || expected[0] != "dest" {
					t.Fatalf("unexpected destination receipt %q in %v", destination, expected)
				}
				atomic.AddInt64(&applied, 1)
				return nil
			}}
			dest := &benchDestination{}
			runner := Runner{
				Destinations:        []DestinationConfig{{Spec: connector.Spec{Name: "dest"}, Dest: dest}},
				RequireDDLExecution: true,
				DDLExecutions:       receipts,
			}

			if err := runner.writeDestinations(ctx, batch, runner.Destinations); err != nil {
				t.Fatalf("write destinations: %v", err)
			}
			runner.emitDDLAppliedTrace(ctx, batch.Checkpoint, ddlRecordsInBatch(batch))
			if got := atomic.LoadInt64(&applied); got != 1 {
				t.Fatalf("expected ddl applied once, got %d", got)
			}
		})
	}
}

type ddlPolicyDestination struct {
	applied int
	writes  int
}

func (*ddlPolicyDestination) Open(context.Context, connector.Spec) error { return nil }
func (d *ddlPolicyDestination) Write(context.Context, connector.Batch) error {
	d.writes++
	return nil
}
func (d *ddlPolicyDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	d.applied++
	return nil
}
func (*ddlPolicyDestination) TypeMappings() map[string]string { return nil }
func (*ddlPolicyDestination) Close(context.Context) error     { return nil }
func (*ddlPolicyDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		Delivery:    connector.DeliverySemantics{Declared: true, ExecutesDDL: true},
		SupportsDDL: true,
	}
}
