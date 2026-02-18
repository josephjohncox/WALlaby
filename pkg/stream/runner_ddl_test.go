package stream

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

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
			dest := &benchDestination{}
			runner := Runner{
				Destinations: []DestinationConfig{{Spec: connector.Spec{Name: "dest"}, Dest: dest}},
				DDLApplied: func(_ context.Context, flowID string, lsn string, ddl string) error {
					if lsn == "" || ddl == "" {
						t.Fatalf("expected lsn+ddl, got lsn=%q ddl=%q", lsn, ddl)
					}
					if tc.ddlText != "" && !strings.HasPrefix(ddl, tc.ddlText) {
						t.Fatalf("unexpected ddl payload %q", ddl)
					}
					_ = flowID
					atomic.AddInt64(&applied, 1)
					return nil
				},
			}

			if err := runner.writeDestinations(ctx, batch, runner.Destinations); err != nil {
				t.Fatalf("write destinations: %v", err)
			}
			if err := runner.markDDLApplied(ctx, batch.Checkpoint, ddlRecordsInBatch(batch)); err != nil {
				t.Fatalf("mark ddl applied: %v", err)
			}
			if got := atomic.LoadInt64(&applied); got != 1 {
				t.Fatalf("expected ddl applied once, got %d", got)
			}
		})
	}
}
