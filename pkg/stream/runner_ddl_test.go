package stream

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRunnerMarksDDLApplied(t *testing.T) {
	ctx := context.Background()
	batch := connector.Batch{
		Schema: connector.Schema{Name: "widgets"},
		Records: []connector.Record{
			{Table: "widgets", Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN extra text"},
		},
		Checkpoint: connector.Checkpoint{LSN: "0/1"},
	}

	var applied int64
	dest := &benchDestination{}
	runner := Runner{
		Destinations: []DestinationConfig{{Spec: connector.Spec{Name: "dest"}, Dest: dest}},
		DDLApplied: func(_ context.Context, lsn string, ddl string) error {
			if lsn == "" || ddl == "" {
				t.Fatalf("expected lsn+ddl, got lsn=%q ddl=%q", lsn, ddl)
			}
			atomic.AddInt64(&applied, 1)
			return nil
		},
	}

	if err := runner.writeDestinations(ctx, batch); err != nil {
		t.Fatalf("write destinations: %v", err)
	}
	if got := atomic.LoadInt64(&applied); got != 1 {
		t.Fatalf("expected ddl applied once, got %d", got)
	}
}
