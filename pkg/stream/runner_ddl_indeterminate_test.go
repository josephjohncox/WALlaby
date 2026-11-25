package stream

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDDLIndeterminateNeverAppliesOrRecordsReceipt(t *testing.T) {
	t.Parallel()
	// All representable unknown results must take the same fail-closed path.
	for value := 0; value <= 255; value++ {
		if value == int(connector.DDLReconcileApplied) || value == int(connector.DDLReconcileNotApplied) {
			continue
		}
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			destination := &ddlPolicyDestination{reconcileResult: connector.DDLReconcileResult(value)}
			recorded := 0
			receipts := &testDDLReceiptStore{onRecord: func(string, string, string, string, []string) error {
				recorded++
				return nil
			}}
			runner := Runner{
				FlowID:              "ddl-unknown",
				Destinations:        []DestinationConfig{{Spec: connector.RuntimeSpec{Name: "dest"}, Dest: destination}},
				RequireDDLExecution: true, DDLExecutions: receipts,
			}
			batch := connector.Batch{
				Schema:     connector.Schema{Name: "widgets"},
				Records:    []connector.Record{{Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN extra text"}},
				Checkpoint: connector.Checkpoint{LSN: "0/10"},
			}
			if _, err := receipts.PrepareDDLExecution(context.Background(), runner.FlowID, "0/10", "dest", []string{"dest"}); err != nil {
				t.Fatal(err)
			}
			err := runner.writeDestination(context.Background(), runner.Destinations[0], batch)
			if !errors.Is(err, connector.ErrDDLReconciliationIndeterminate) || destination.applied != 0 || recorded != 0 {
				t.Fatalf("value=%d err=%v applies=%d receipts=%d", value, err, destination.applied, recorded)
			}
			state, err := receipts.PrepareDDLExecution(context.Background(), runner.FlowID, "0/10", "dest", []string{"dest"})
			if err != nil || state != connector.DDLExecutionRetry {
				t.Fatalf("unknown outcome lost its retry state: state=%v err=%v", state, err)
			}
			destination.reconcileResult = connector.DDLReconcileApplied
			if err := runner.writeDestination(context.Background(), runner.Destinations[0], batch); err != nil {
				t.Fatal(err)
			}
			if destination.applied != 0 || recorded != 1 {
				t.Fatalf("reconciled effect was reapplied or not receipted: applies=%d receipts=%d", destination.applied, recorded)
			}
		})
	}
}
