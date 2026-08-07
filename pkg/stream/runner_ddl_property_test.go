package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"pgregory.net/rapid"
)

func TestRunnerDDLCommitReceiptCrashBoundariesRapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		failurePoint := rapid.SampledFrom([]string{"none", "before_external_commit", "after_external_commit"}).Draw(t, "failure_point")
		destination := &modelDDLReconcileDestination{
			failApply: failurePoint == "before_external_commit",
		}
		receipts := &testDDLReceiptStore{}
		if failurePoint == "after_external_commit" {
			receiptFailures := 1
			receipts.beforeRecord = func(string, string, string, string, []string) error {
				if receiptFailures > 0 {
					receiptFailures--
					return errors.New("receipt failure")
				}
				return nil
			}
		}
		runner := Runner{
			FlowID: "flow-ddl-property",
			Destinations: []DestinationConfig{{
				Spec: connector.RuntimeSpec{Name: "destination"},
				Dest: destination,
			}},
			RequireDDLExecution: true,
			DDLExecutions:       receipts,
		}
		batch := connector.Batch{
			Schema: connector.Schema{Name: "widgets"},
			Records: []connector.Record{{
				Operation: connector.OpDDL,
				DDL:       "ALTER TABLE widgets ADD COLUMN extra text",
			}},
			Checkpoint: connector.Checkpoint{LSN: "0/50"},
		}

		err := runner.writeDestination(context.Background(), runner.Destinations[0], batch)
		if failurePoint == "none" {
			if err != nil {
				t.Fatalf("first execution: %v", err)
			}
		} else {
			if err == nil {
				t.Fatalf("failure point %s unexpectedly succeeded", failurePoint)
			}
			if err := runner.writeDestination(context.Background(), runner.Destinations[0], batch); err != nil {
				t.Fatalf("replay after %s: %v", failurePoint, err)
			}
		}

		if !destination.externalApplied {
			t.Fatal("DDL was not externally applied")
		}
		if destination.externalCommits != 1 {
			t.Fatalf("external commits=%d, want exactly one", destination.externalCommits)
		}
		state, err := receipts.PrepareDDLExecution(context.Background(), runner.FlowID, "0/50", "destination", []string{"destination"})
		if err != nil || state != connector.DDLExecutionComplete {
			t.Fatalf("receipt state=%v error=%v, want complete", state, err)
		}
		if failurePoint == "before_external_commit" && destination.reconciliations != 1 {
			t.Fatalf("reconciliations=%d, want one not-applied reconciliation", destination.reconciliations)
		}
		if failurePoint == "after_external_commit" && destination.reconciliations != 1 {
			t.Fatalf("reconciliations=%d, want one applied reconciliation", destination.reconciliations)
		}
	})
}

type modelDDLReconcileDestination struct {
	failApply       bool
	externalApplied bool
	externalCommits int
	reconciliations int
	writes          int
}

func (*modelDDLReconcileDestination) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (d *modelDDLReconcileDestination) Write(context.Context, connector.Batch) error {
	d.writes++
	return nil
}
func (d *modelDDLReconcileDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	if d.failApply {
		d.failApply = false
		return errors.New("failed before external commit")
	}
	if d.externalApplied {
		return errors.New("duplicate external DDL application")
	}
	d.externalApplied = true
	d.externalCommits++
	return nil
}
func (d *modelDDLReconcileDestination) ReconcileDDL(context.Context, connector.Schema, connector.Record) (connector.DDLReconcileResult, error) {
	d.reconciliations++
	if d.externalApplied {
		return connector.DDLReconcileApplied, nil
	}
	return connector.DDLReconcileNotApplied, nil
}
func (*modelDDLReconcileDestination) TypeMappings() map[string]string { return nil }
func (*modelDDLReconcileDestination) Close(context.Context) error     { return nil }
func (*modelDDLReconcileDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		Delivery: connector.DeliverySemantics{ExecutesDDL: true},
	}
}
