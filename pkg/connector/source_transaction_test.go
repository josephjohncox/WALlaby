package connector

import (
	"encoding/json"
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

func TestManagedSchemaBaselinePayloadCanonicalizesAndBindsExactSchema(t *testing.T) {
	t.Parallel()
	older := Schema{Namespace: "public", Name: "events", Version: 1}
	newer := Schema{Namespace: "public", Name: "events", Version: 2, Columns: []Column{{Name: "id", Type: "bigint"}}}
	other := Schema{Namespace: "audit", Name: "events", Version: 1}
	first, err := NewManagedSchemaBaselinePayload("lineage", []Schema{older, other, newer})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewManagedSchemaBaselinePayload("lineage", []Schema{newer, older, other})
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, firstFingerprint, err := first.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, secondFingerprint, err := second.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) || firstFingerprint != secondFingerprint {
		t.Fatalf("canonical baseline changed with input order: %s/%s != %s/%s", firstJSON, firstFingerprint, secondJSON, secondFingerprint)
	}
	second.Schemas[1].Columns = append(second.Schemas[1].Columns, Column{Name: "note", Type: "text"})
	_, changedFingerprint, err := second.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if changedFingerprint == firstFingerprint {
		t.Fatal("schema-baseline fingerprint did not bind exact schema payload")
	}
}

func TestManagedSchemaBaselineIdentityPreservesExactPostgresIdentifierBytes(t *testing.T) {
	t.Parallel()

	schemas := []Schema{
		{Namespace: "Exact Schema", Name: "Events", Version: 1},
		{Namespace: "Exact Schema", Name: "events", Version: 2},
		{Namespace: "Exact Schema", Name: " events ", Version: 3},
	}
	payload, err := NewManagedSchemaBaselinePayload("lineage", schemas)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload.Schemas) != len(schemas) {
		t.Fatalf("case/whitespace-distinct baselines collapsed: %+v", payload.Schemas)
	}
	seen := make(map[string]bool, len(payload.Schemas))
	for _, schema := range payload.Schemas {
		seen[schema.Name] = true
		if schema.Namespace != "Exact Schema" {
			t.Fatalf("namespace bytes changed: %q", schema.Namespace)
		}
	}
	for _, name := range []string{"Events", "events", " events "} {
		if !seen[name] {
			t.Fatalf("exact relation %q missing from canonical payload: %+v", name, payload.Schemas)
		}
	}
	upperKey := ManagedSchemaBaselineKey("Exact Schema", "Events")
	lowerKey := ManagedSchemaBaselineKey("Exact Schema", "events")
	spaceKey := ManagedSchemaBaselineKey("Exact Schema", " events ")
	if upperKey == lowerKey || upperKey == spaceKey || lowerKey == spaceKey {
		t.Fatalf("exact relation keys collided: %q %q %q", upperKey, lowerKey, spaceKey)
	}
	encoded, err := json.Marshal(payload.Schemas)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeManagedSchemaBaselineOption(string(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 3 || decoded[0].Namespace != payload.Schemas[0].Namespace || decoded[0].Name != payload.Schemas[0].Name {
		t.Fatalf("decoder option changed exact identifiers: %+v", decoded)
	}
	for _, invalid := range []Schema{
		{Namespace: "", Name: "events"},
		{Namespace: "public", Name: ""},
		{Namespace: "public\x00shadow", Name: "events"},
		{Namespace: "public", Name: "events\x00shadow"},
	} {
		if _, err := NewManagedSchemaBaselinePayload("lineage", []Schema{invalid}); err == nil {
			t.Fatalf("invalid exact identifier accepted: namespace=%q relation=%q", invalid.Namespace, invalid.Name)
		}
	}
	if _, err := NewManagedSchemaBaselinePayload("lineage", []Schema{{Namespace: " ", Name: " "}}); err != nil {
		t.Fatalf("valid quoted whitespace identifiers rejected: %v", err)
	}
}

func TestSourceTransactionSchemasExtractsWhitespaceOnlyPostgresIdentifiers(t *testing.T) {
	t.Parallel()
	transaction := SourceTransaction{SourceLineageID: "lineage", Fragments: []TransactionFragment{
		{Ordinal: 0, Batch: Batch{Schema: Schema{Namespace: " ", Name: " ", Columns: []Column{{Name: " ", Type: "text"}}}}},
		{Ordinal: 1, Batch: Batch{}},
	}}
	schemas := SourceTransactionSchemas(transaction)
	if len(schemas) != 1 || schemas[0].Namespace != " " || schemas[0].Name != " " || len(schemas[0].Columns) != 1 || schemas[0].Columns[0].Name != " " {
		t.Fatalf("source transaction extraction changed whitespace-only PostgreSQL identifiers: %+v", schemas)
	}
	payload, err := NewManagedSchemaBaselinePayload("lineage", schemas)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload.Schemas) != 1 || payload.Schemas[0].Namespace != " " || payload.Schemas[0].Name != " " {
		t.Fatalf("baseline payload changed whitespace-only relation identity: %+v", payload)
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
