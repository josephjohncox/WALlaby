package connector

import (
	"strings"
	"testing"
	"time"
)

func TestDeliveryLogicalBatchIDIsReplayStableAndPositionBound(t *testing.T) {
	t.Parallel()
	first, err := DeliveryLogicalBatchID("lineage", "bootstrap/id/task/1", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := DeliveryLogicalBatchID("lineage", "bootstrap/id/task/1", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if first != replay || !strings.HasPrefix(first, "logical-batch:") {
		t.Fatalf("logical IDs=%q/%q", first, replay)
	}
	other, err := DeliveryLogicalBatchID("lineage", "bootstrap/id/task/2", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	if other == first {
		t.Fatal("position did not bind logical batch ID")
	}
}

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
	replayed.Checkpoint.Metadata = map[string]string{"managed_postgres_schema_baselines_v1": `[{"Version":999}]`}
	replayed.Fragments = append([]TransactionFragment(nil), transaction.Fragments...)
	replayed.Fragments[0].Batch.Records = append([]Record(nil), transaction.Fragments[0].Batch.Records...)
	replayed.Fragments[0].Batch.Records[0].Timestamp = time.Unix(999, 0)
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

	runtimeReplayed := transaction
	runtimeReplayed.Checkpoint.Metadata = map[string]string{
		"artifact_publication_id":       "9d5f8653-2bc9-4a83-a967-3da7b4ca68bb",
		"artifact_publication_sequence": "41",
		"managed_schema_baselines":      `[{"namespace":"public","name":"widgets"}]`,
	}
	runtimeReplayed.Fragments = append([]TransactionFragment(nil), transaction.Fragments...)
	runtimeReplayed.Fragments[0].Batch = transaction.Fragments[0].Batch
	runtimeReplayed.Fragments[0].Batch.WireFormat = WireFormatJSON
	runtimeHash, err := SourceTransactionContentHash(runtimeReplayed)
	if err != nil {
		t.Fatal(err)
	}
	if first != runtimeHash {
		t.Fatalf("runtime checkpoint metadata or wire format changed logical identity: %s != %s", first, runtimeHash)
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

func TestSourceTransactionValidationRejectsOrdinalGapsAndReordering(t *testing.T) {
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
		t.Fatal("Validate() accepted raw fragment ordinals [0,2]")
	}
	transaction.Fragments[1].Ordinal = 0
	if err := transaction.Validate(); err == nil {
		t.Fatal("Validate() accepted duplicate/reordered fragment ordinal")
	}
}

func transactionTestBatch(namespace, table string, id int64) Batch {
	return Batch{
		Schema:  Schema{Namespace: namespace, Name: table, Version: 1, Columns: []Column{{Name: "id", Type: "bigint"}}},
		Records: []Record{{Table: table, Operation: OpInsert, SchemaVersion: 1, After: map[string]any{"id": id}}},
	}
}
