package connector

import (
	"testing"
	"time"
)

func TestSourceTransactionContentHashPreservesFragmentOrder(t *testing.T) {
	t.Parallel()

	transaction := SourceTransaction{
		SourceLineageID: "lineage-1",
		TransactionID:   42,
		BeginLSN:        "0/10",
		CommitLSN:       "0/30",
		EndLSN:          "0/38",
		Checkpoint:      Checkpoint{LSN: "0/38", Timestamp: time.Unix(10, 0)},
		Fragments: []TransactionFragment{
			{Ordinal: 0, Batch: transactionTestBatch("public", "widgets", 1)},
			{Ordinal: 1, Batch: transactionTestBatch("audit", "events", 2)},
		},
	}

	first, err := SourceTransactionContentHash(transaction)
	if err != nil {
		t.Fatal(err)
	}
	replayed := transaction
	replayed.Checkpoint.Timestamp = time.Unix(999, 0)
	second, err := SourceTransactionContentHash(replayed)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("replayed transaction hash changed: %s != %s", first, second)
	}
	versionReplayed := transaction
	versionReplayed.Fragments = append([]TransactionFragment(nil), transaction.Fragments...)
	versionReplayed.Fragments[0].Batch.Schema.Version = 99
	versionReplayed.Fragments[0].Batch.Records = append([]Record(nil), transaction.Fragments[0].Batch.Records...)
	versionReplayed.Fragments[0].Batch.Records[0].SchemaVersion = 99
	versionHash, err := SourceTransactionContentHash(versionReplayed)
	if err != nil {
		t.Fatal(err)
	}
	if first != versionHash {
		t.Fatalf("process-local schema version changed replay identity: %s != %s", first, versionHash)
	}

	reordered := transaction
	reordered.Fragments = []TransactionFragment{transaction.Fragments[1], transaction.Fragments[0]}
	reordered.Fragments[0].Ordinal = 0
	reordered.Fragments[1].Ordinal = 1
	third, err := SourceTransactionContentHash(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("fragment order must contribute to the logical transaction hash")
	}
}

func TestManagedSchemaBaselinesFollowDeliveredTransactions(t *testing.T) {
	t.Parallel()

	metadata, err := MergeManagedSchemaBaselines(nil, SourceTransaction{Fragments: []TransactionFragment{
		{Batch: transactionTestBatch("public", "widgets", 1)},
		{Batch: transactionTestBatch("audit", "events", 2)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	updated := transactionTestBatch("public", "widgets", 3).Schema
	updated.Version = 2
	updated.Columns = append(updated.Columns, Column{Name: "note", Type: "text", Nullable: true})
	metadata, err = MergeManagedSchemaBaselines(metadata, SourceTransaction{Fragments: []TransactionFragment{{Batch: Batch{
		Schema: updated, Records: []Record{{Table: "widgets", Operation: OpInsert, After: map[string]any{"id": int64(3)}}},
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	baselines, err := DecodeManagedSchemaBaselines(metadata[ManagedSchemaBaselinesMetadataKey])
	if err != nil {
		t.Fatal(err)
	}
	if len(baselines) != 2 || baselines[0].Namespace != "audit" || baselines[1].Version != 2 || len(baselines[1].Columns) != 2 {
		t.Fatalf("merged baselines=%+v, want sorted audit/events plus updated public/widgets", baselines)
	}
}

func TestSourceTransactionValidationRejectsCollapsedBarrierOrdinal(t *testing.T) {
	t.Parallel()

	transaction := SourceTransaction{
		SourceLineageID: "lineage-1",
		TransactionID:   42,
		BeginLSN:        "0/10",
		CommitLSN:       "0/30",
		EndLSN:          "0/38",
		Checkpoint:      Checkpoint{LSN: "0/38"},
		Fragments: []TransactionFragment{
			{Ordinal: 0, Batch: transactionTestBatch("public", "widgets", 1)},
			{Ordinal: 2, Batch: transactionTestBatch("public", "widgets", 2)},
		},
	}
	if err := transaction.Validate(); err == nil {
		t.Fatal("Validate() accepted a missing fragment ordinal")
	}
}

func transactionTestBatch(namespace, table string, id int64) Batch {
	return Batch{
		Schema:  Schema{Namespace: namespace, Name: table, Version: 1, Columns: []Column{{Name: "id", Type: "bigint"}}},
		Records: []Record{{Table: table, Operation: OpInsert, SchemaVersion: 1, After: map[string]any{"id": id}}},
	}
}
