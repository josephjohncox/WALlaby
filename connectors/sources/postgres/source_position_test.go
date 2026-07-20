package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestReadEmitsObservedWALPositionWhenEmptyBatchesDisabled(t *testing.T) {
	t.Parallel()

	changes := make(chan replication.Change, 1)
	changes <- replication.Change{LSN: pglogrepl.LSN(0x30)}
	source := &Source{
		changes:      changes,
		batchSize:    10,
		batchTimeout: 5 * time.Millisecond,
		emitEmpty:    false,
	}

	batch, err := source.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 0 {
		t.Fatalf("records=%d, want empty WAL advancement batch", len(batch.Records))
	}
	if batch.Checkpoint.LSN != "0/30" {
		t.Fatalf("checkpoint=%q, want observed WAL position 0/30", batch.Checkpoint.LSN)
	}
}

func TestReadPreservesPerRecordSourcePositions(t *testing.T) {
	t.Parallel()

	changes := make(chan replication.Change, 2)
	changes <- replication.Change{
		LSN:    pglogrepl.LSN(0x10),
		Record: &connector.Record{Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN first text"},
	}
	changes <- replication.Change{
		LSN:    pglogrepl.LSN(0x20),
		Record: &connector.Record{Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN second text"},
	}

	source := &Source{
		changes:      changes,
		batchSize:    2,
		batchTimeout: time.Second,
	}
	batch, err := source.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 2 {
		t.Fatalf("records=%d, want 2", len(batch.Records))
	}
	if batch.Records[0].SourcePosition != "0/10" || batch.Records[1].SourcePosition != "0/20" {
		t.Fatalf("source positions=(%q,%q), want (0/10,0/20)",
			batch.Records[0].SourcePosition,
			batch.Records[1].SourcePosition,
		)
	}
	if batch.Checkpoint.LSN != "0/20" {
		t.Fatalf("batch checkpoint=%q, want 0/20", batch.Checkpoint.LSN)
	}
}
