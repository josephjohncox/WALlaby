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

func TestReadEmitsObservedWALPositionBeforeClosedStream(t *testing.T) {
	t.Parallel()

	changes := make(chan replication.Change, 1)
	changes <- replication.Change{LSN: pglogrepl.LSN(0x40)}
	close(changes)
	source := &Source{changes: changes, batchSize: 10, batchTimeout: time.Second}

	batch, err := source.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if batch.Checkpoint.LSN != "0/40" || len(batch.Records) != 0 {
		t.Fatalf("batch = %+v, want empty durable position 0/40 before EOF", batch)
	}
}

func TestReadAggregatesHomogeneousChanges(t *testing.T) {
	t.Parallel()

	changes := make(chan replication.Change, 2)
	changes <- sourceChange(0x10, "widgets", 3, connector.OpInsert)
	changes <- sourceChange(0x20, "widgets", 3, connector.OpUpdate)

	source := &Source{changes: changes, batchSize: 2, batchTimeout: time.Second}
	batch, err := source.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := connector.ValidateBatch(batch); err != nil {
		t.Fatalf("ValidateBatch() error = %v", err)
	}
	if len(batch.Records) != 2 || batch.Checkpoint.LSN != "0/20" {
		t.Fatalf("batch records=%d checkpoint=%q, want 2 records through 0/20", len(batch.Records), batch.Checkpoint.LSN)
	}
}

func TestReadDefersTableAndSchemaBoundariesInOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		first  replication.Change
		second replication.Change
	}{
		{
			name:   "table",
			first:  sourceChange(0x10, "widgets", 3, connector.OpInsert),
			second: sourceChange(0x20, "gadgets", 3, connector.OpInsert),
		},
		{
			name:   "schema version",
			first:  sourceChange(0x10, "widgets", 3, connector.OpInsert),
			second: sourceChange(0x20, "widgets", 4, connector.OpInsert),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			changes := make(chan replication.Change, 2)
			changes <- tt.first
			changes <- tt.second

			source := &Source{changes: changes, batchSize: 10, batchTimeout: 5 * time.Millisecond}
			first, err := source.Read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			second, err := source.Read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(first.Records) != 1 || first.Checkpoint.LSN != "0/10" {
				t.Fatalf("first batch records=%d checkpoint=%q, want one record through 0/10", len(first.Records), first.Checkpoint.LSN)
			}
			if len(second.Records) != 1 || second.Checkpoint.LSN != "0/20" {
				t.Fatalf("second batch records=%d checkpoint=%q, want one record through 0/20", len(second.Records), second.Checkpoint.LSN)
			}
			if err := connector.ValidateBatch(first); err != nil {
				t.Fatalf("first ValidateBatch() error = %v", err)
			}
			if err := connector.ValidateBatch(second); err != nil {
				t.Fatalf("second ValidateBatch() error = %v", err)
			}
		})
	}
}

func TestReadIsolatesDDLFromDMLWithoutReordering(t *testing.T) {
	t.Parallel()

	changes := make(chan replication.Change, 3)
	changes <- sourceChange(0x10, "widgets", 3, connector.OpInsert)
	changes <- sourceChange(0x20, "widgets", 3, connector.OpDDL)
	changes <- sourceChange(0x30, "widgets", 3, connector.OpUpdate)

	source := &Source{changes: changes, batchSize: 10, batchTimeout: 5 * time.Millisecond}
	wantOperations := []connector.Operation{connector.OpInsert, connector.OpDDL, connector.OpUpdate}
	wantPositions := []string{"0/10", "0/20", "0/30"}
	for index := range wantOperations {
		batch, err := source.Read(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := connector.ValidateBatch(batch); err != nil {
			t.Fatalf("batch %d ValidateBatch() error = %v", index, err)
		}
		if len(batch.Records) != 1 || batch.Records[0].Operation != wantOperations[index] {
			t.Fatalf("batch %d records=%+v, want one %s", index, batch.Records, wantOperations[index])
		}
		if batch.Checkpoint.LSN != wantPositions[index] {
			t.Fatalf("batch %d checkpoint=%q, want %q", index, batch.Checkpoint.LSN, wantPositions[index])
		}
	}
}

func TestReadPreservesPerRecordSourcePositions(t *testing.T) {
	t.Parallel()

	changes := make(chan replication.Change, 2)
	changes <- replication.Change{
		LSN: pglogrepl.LSN(0x30),
		Record: &connector.Record{
			Operation:      connector.OpDDL,
			DDL:            "ALTER TABLE widgets ADD COLUMN first text",
			SourcePosition: "0/10",
		},
	}
	changes <- replication.Change{
		LSN: pglogrepl.LSN(0x30),
		Record: &connector.Record{
			Operation:      connector.OpDDL,
			DDL:            "ALTER TABLE widgets ADD COLUMN second text",
			SourcePosition: "0/20",
		},
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
	if batch.Checkpoint.LSN != "0/30" {
		t.Fatalf("batch checkpoint=%q, want transaction end 0/30", batch.Checkpoint.LSN)
	}
	if err := connector.ValidateBatch(batch); err != nil {
		t.Fatalf("tableless logical DDL batch failed validation: %v", err)
	}
}

func TestReadDoesNotCheckpointBeforeTransactionFinalFragment(t *testing.T) {
	t.Parallel()

	changes := make(chan replication.Change, 2)
	changes <- transactionChange(42, 0x10, 0x38, "widgets", false)
	changes <- transactionChange(42, 0x20, 0x38, "gadgets", true)

	source := &Source{changes: changes, batchSize: 100, batchTimeout: time.Second}
	first, err := source.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Checkpoint.LSN != "" {
		t.Fatalf("intermediate checkpoint=%q, want empty before transaction final fragment", first.Checkpoint.LSN)
	}

	second, err := source.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.Checkpoint.LSN != "0/38" {
		t.Fatalf("final checkpoint=%q, want transaction end 0/38", second.Checkpoint.LSN)
	}
}

func TestReadTransactionPreservesOrderedTableFragments(t *testing.T) {
	t.Parallel()

	changes := make(chan replication.Change, 3)
	changes <- transactionChange(77, 0x10, 0x58, "widgets", false)
	changes <- transactionChange(77, 0x20, 0x58, "gadgets", false)
	changes <- transactionChange(77, 0x30, 0x58, "widgets", true)

	source := &Source{changes: changes, batchSize: 1, batchTimeout: time.Millisecond}
	transaction, err := source.ReadTransaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if transaction.TransactionID != 77 || transaction.Checkpoint.LSN != "0/58" {
		t.Fatalf("transaction=(xid:%d checkpoint:%q), want 77/0/58", transaction.TransactionID, transaction.Checkpoint.LSN)
	}
	if len(transaction.Fragments) != 3 {
		t.Fatalf("fragments=%d, want 3 ordered table fragments", len(transaction.Fragments))
	}
	wantTables := []string{"widgets", "gadgets", "widgets"}
	for index, fragment := range transaction.Fragments {
		if fragment.Ordinal != uint64(index) || fragment.Batch.Schema.Name != wantTables[index] {
			t.Fatalf("fragment %d = ordinal:%d table:%q, want %d/%q", index, fragment.Ordinal, fragment.Batch.Schema.Name, index, wantTables[index])
		}
		if fragment.Batch.Checkpoint.LSN != "" {
			t.Fatalf("fragment %d checkpoint=%q, transaction checkpoint must be carried only by SourceTransaction", index, fragment.Batch.Checkpoint.LSN)
		}
	}
}

func TestReadTransactionPreservesMultiSchemaDDLBarrier(t *testing.T) {
	t.Parallel()
	changes := make(chan replication.Change, 3)
	first := transactionChange(88, 0, 0x68, "widgets", false)
	first.Schema = "public"
	first.SchemaDef.Namespace = "public"
	barrier := transactionChange(88, 1, 0x68, "widgets", false)
	barrier.Schema = "public"
	barrier.SchemaDef.Namespace = "public"
	barrier.Record.Operation = connector.OpDDL
	barrier.Record.DDL = "ALTER TABLE public.widgets ADD COLUMN note text"
	last := transactionChange(88, 2, 0x68, "events", true)
	last.Schema = "audit"
	last.SchemaDef.Namespace = "audit"
	changes <- first
	changes <- barrier
	changes <- last

	source := &Source{changes: changes, batchSize: 100, batchTimeout: time.Second, sourceLineage: "lineage-1"}
	transaction, err := source.ReadTransaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(transaction.Fragments) != 3 {
		t.Fatalf("fragments=%d, want DML/DDL/DML barriers", len(transaction.Fragments))
	}
	if transaction.Fragments[0].Batch.Schema.Namespace != "public" || transaction.Fragments[1].Batch.Records[0].Operation != connector.OpDDL || transaction.Fragments[2].Batch.Schema.Namespace != "audit" {
		t.Fatalf("ordered transaction fragments=%+v", transaction.Fragments)
	}
	if transaction.Checkpoint.LSN != "0/68" {
		t.Fatalf("transaction checkpoint=%q", transaction.Checkpoint.LSN)
	}
}

func transactionChange(xid uint32, ordinal uint64, end pglogrepl.LSN, table string, final bool) replication.Change {
	change := sourceChange(end, table, 1, connector.OpInsert)
	change.TransactionID = xid
	change.TransactionBeginLSN = pglogrepl.LSN(0x8)
	change.TransactionCommitLSN = end - 8
	change.TransactionEndLSN = end
	change.TransactionOrdinal = ordinal
	change.TransactionFinal = final
	return change
}

func sourceChange(lsn pglogrepl.LSN, table string, version int64, operation connector.Operation) replication.Change {
	schema := connector.Schema{Name: table, Namespace: "public", Version: version}
	record := connector.Record{Table: table, Operation: operation, SchemaVersion: version}
	if operation == connector.OpDDL {
		record.DDL = "ALTER TABLE " + table + " ADD COLUMN note text"
	} else {
		record.After = map[string]any{"id": uint64(lsn)}
	}
	return replication.Change{
		LSN:       lsn,
		Schema:    schema.Namespace,
		Table:     table,
		Operation: string(operation),
		Record:    &record,
		SchemaDef: &schema,
	}
}
