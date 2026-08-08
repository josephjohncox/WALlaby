package artifactlog

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/tablemap"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"pgregory.net/rapid"
)

func TestCanonicalProjectionV1GoldenIdentity(t *testing.T) {
	t.Parallel()

	plan, err := NewEncoder().PlanTransaction(
		context.Background(),
		uuid.MustParse("44444444-4444-4444-4444-444444444444"),
		plannerTransaction(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Artifacts) != 1 {
		t.Fatalf("artifacts=%d, want 1 golden artifact", len(plan.Artifacts))
	}
	artifact := plan.Artifacts[0]
	const wantLogicalBatchID = "logical-batch:ac97d03e57fcbd0514579a462eceb209644d146ffea0c5bb65e9e4c24c76d7e1"
	const wantContentHash = "3ad2988816a831f1c0fa3ef4a81c9674906038571d87c63ab0ac07a3f35fac3f"
	const wantSchemaID = "f6b4df8e0254025c850b6be95241df78de7f68ce91a01b1ac2e2d60a1e8b3e03"
	const wantArtifactID = "f5c32f67d7df869068321358e5c92c7f0209e47789009dc4f1d694e17af5fef1"
	const wantEncodedHash = "874ae66dd0d319762a490d7f23b131e8e181057e284ed66beacf49d501bf1de3"
	const wantObjectKey = "wallaby/artifacts/44444444-4444-4444-4444-444444444444/source=3c123b9685a8bc65/namespace=public/table=widgets/schema=f6b4df8e0254025c850b6be95241df78de7f68ce91a01b1ac2e2d60a1e8b3e03/partition=unpartitioned/shard=000000/f5c32f67d7df869068321358e5c92c7f0209e47789009dc4f1d694e17af5fef1.parquet"
	if plan.LogicalBatchID != wantLogicalBatchID || plan.ContentHash != wantContentHash || artifact.SchemaID != wantSchemaID || artifact.ID != wantArtifactID || artifact.EncodedByteHash != wantEncodedHash || artifact.ObjectKey != wantObjectKey {
		t.Fatalf("golden identity changed:\nlogical=%q\ncontent=%q\nschema=%q\nartifact=%q\nencoded=%q\nkey=%q", plan.LogicalBatchID, plan.ContentHash, artifact.SchemaID, artifact.ID, artifact.EncodedByteHash, artifact.ObjectKey)
	}
}

func TestCanonicalProjectionV2MappedReplayAndStableSourceFieldIDs(t *testing.T) {
	const fingerprint = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	incarnation := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	base := plannerTransaction(2)
	base.Fragments[0].Batch.WritePolicy = connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: fingerprint}
	mapped := base
	mapped.Fragments = append([]connector.TransactionFragment(nil), base.Fragments...)
	mapped.Fragments[0].Batch = base.Fragments[0].Batch
	mapped.Fragments[0].Batch.Schema = base.Fragments[0].Batch.Schema
	mapped.Fragments[0].Batch.Schema.Namespace = "lake"
	mapped.Fragments[0].Batch.Schema.Name = "accounts"
	mapped.Fragments[0].Batch.Schema.Columns = append([]connector.Column(nil), base.Fragments[0].Batch.Schema.Columns...)
	mapped.Fragments[0].Batch.Schema.Columns[0].Name = "account_id"
	mapped.Fragments[0].Batch.Records = append([]connector.Record(nil), base.Fragments[0].Batch.Records...)
	for index := range mapped.Fragments[0].Batch.Records {
		record := &mapped.Fragments[0].Batch.Records[index]
		record.Table = "accounts"
		record.After = map[string]any{"account_id": int64(index), "value": "value"}
	}
	encoder := NewEncoder()
	first, err := encoder.PlanMappedTransaction(context.Background(), incarnation, fingerprint, mapped)
	if err != nil {
		t.Fatal(err)
	}
	const wantLogical = "logical-batch:2ff55d8ac3c6c9cb1e6f1accde0b7d069c1061f1d70322e4e7dcfc956c9b741f"
	const wantContent = "6b3bbae6c8eec101f26974e6e47c5eb0daefb45723d215acec77e80652009acb"
	const wantSchema = "e2b0e25a8a2de9bfac6b4e46175826ab7595635a3f82ba7c2b227c3443f672da"
	const wantArtifact = "7b6d9408a04c83b4cf9ae622f67483cdc70fc1fca4bbe99c8abde02b937dc126"
	const wantEncoded = "bc3ff24a63301ea153393f6daf5f549d510b16392217bfe1e26e2dc24fc4d7e9"
	const wantKey = "wallaby/artifacts-v2/44444444-4444-4444-4444-444444444444/source=3c123b9685a8bc65/mapping=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/namespace=lake/table=accounts/schema=e2b0e25a8a2de9bfac6b4e46175826ab7595635a3f82ba7c2b227c3443f672da/partition=unpartitioned/shard=000000/7b6d9408a04c83b4cf9ae622f67483cdc70fc1fca4bbe99c8abde02b937dc126.parquet"
	if first.LogicalBatchID != wantLogical || first.ContentHash != wantContent || first.Artifacts[0].SchemaID != wantSchema || first.Artifacts[0].ID != wantArtifact || first.Artifacts[0].EncodedByteHash != wantEncoded || first.Artifacts[0].ObjectKey != wantKey {
		t.Fatalf("canonical v2 golden identity changed: %+v", first.Artifacts[0])
	}
	second, err := encoder.PlanMappedTransaction(context.Background(), incarnation, fingerprint, mapped)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Artifacts) != 1 || len(second.Artifacts) != 1 || first.Artifacts[0].ID != second.Artifacts[0].ID || !bytes.Equal(first.Artifacts[0].Encoded, second.Artifacts[0].Encoded) {
		t.Fatal("v2 mapped replay is not deterministic")
	}
	var document canonicalSchemaV2
	if err := json.Unmarshal(first.Artifacts[0].SchemaJSON, &document); err != nil {
		t.Fatal(err)
	}
	if document.ProjectionID != ProjectionIDV2 || document.MappingFingerprint != fingerprint || document.Namespace != "lake" || document.Table != "accounts" {
		t.Fatalf("v2 schema identity=%+v", document)
	}
	original, err := encoder.PlanMappedTransaction(context.Background(), incarnation, fingerprint, base)
	if err != nil {
		t.Fatal(err)
	}
	var originalDocument canonicalSchemaV2
	if err := json.Unmarshal(original.Artifacts[0].SchemaJSON, &originalDocument); err != nil {
		t.Fatal(err)
	}
	fieldID := func(fields []CanonicalField, sourceColumn string) int32 {
		for _, field := range fields {
			if field.Metadata["source_column_id"] == sourceColumn {
				return field.ID
			}
		}
		return 0
	}
	if fieldID(document.Fields, "1") != fieldID(originalDocument.Fields, "1") {
		t.Fatal("target rename changed stable source field ID")
	}
	filtered := mapped
	filtered.Fragments = append([]connector.TransactionFragment(nil), mapped.Fragments...)
	filtered.Fragments[0].Batch = mapped.Fragments[0].Batch
	filtered.Fragments[0].Batch.Schema = mapped.Fragments[0].Batch.Schema
	filtered.Fragments[0].Batch.Schema.Columns = append([]connector.Column(nil), mapped.Fragments[0].Batch.Schema.Columns[:1]...)
	filtered.Fragments[0].Batch.Records = append([]connector.Record(nil), mapped.Fragments[0].Batch.Records...)
	for index := range filtered.Fragments[0].Batch.Records {
		filtered.Fragments[0].Batch.Records[index].After = map[string]any{"account_id": int64(index)}
	}
	filteredPlan, err := encoder.PlanMappedTransaction(context.Background(), incarnation, fingerprint, filtered)
	if err != nil {
		t.Fatal(err)
	}
	var filteredDocument canonicalSchemaV2
	if err := json.Unmarshal(filteredPlan.Artifacts[0].SchemaJSON, &filteredDocument); err != nil {
		t.Fatal(err)
	}
	if fieldID(filteredDocument.Fields, "1") != fieldID(document.Fields, "1") || fieldID(filteredDocument.Fields, "2") != 0 {
		t.Fatal("column filtering changed retained field identity or retained excluded field")
	}
	otherLineage := mapped
	otherLineage.SourceLineageID = "postgres-system/other-lineage"
	otherPlan, err := encoder.PlanMappedTransaction(context.Background(), incarnation, fingerprint, otherLineage)
	if err != nil {
		t.Fatal(err)
	}
	var otherDocument canonicalSchemaV2
	if err := json.Unmarshal(otherPlan.Artifacts[0].SchemaJSON, &otherDocument); err != nil {
		t.Fatal(err)
	}
	if otherDocument.SourceLineageID == document.SourceLineageID || fieldID(otherDocument.Fields, "1") == fieldID(document.Fields, "1") {
		t.Fatal("different source lineages silently aliased canonical field identity")
	}
}

func TestMappedV2ProductionAppendProjectorOwnsMetadataWithoutEnvelopeCollision(t *testing.T) {
	t.Parallel()
	destinations := []connector.RuntimeSpec{{Name: "ice", Type: connector.EndpointIceberg}}
	mappings := flow.NewTableMappings(destinations)
	projector, err := tablemap.New(mappings, "ice")
	if err != nil {
		t.Fatal(err)
	}
	projected, decision, err := projector.ProjectTransaction(plannerTransaction(2))
	if err != nil {
		t.Fatal(err)
	}
	if decision != stream.ProjectionIncluded {
		t.Fatalf("projection decision=%v", decision)
	}
	plan, err := NewEncoder().PlanMappedTransaction(context.Background(), uuid.MustParse("55555555-5555-5555-5555-555555555555"), projector.Fingerprint(), projected)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Artifacts) != 1 {
		t.Fatalf("artifacts=%d", len(plan.Artifacts))
	}
	var document canonicalSchemaV2
	if err := json.Unmarshal(plan.Artifacts[0].SchemaJSON, &document); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	identities := map[string]string{}
	for _, field := range document.Fields {
		counts[field.Name]++
		identities[field.Name] = field.SyntheticIdentity
	}
	if counts[canonicalSourcePositionColumn] != 1 {
		t.Fatalf("canonical source-position fields=%d", counts[canonicalSourcePositionColumn])
	}
	if counts[connector.AppendOperationColumn] != 1 || identities[connector.AppendOperationColumn] != "append.operation.v1" {
		t.Fatalf("append operation identity=%q", identities[connector.AppendOperationColumn])
	}
	if counts[connector.AppendDeletedColumn] != 1 || identities[connector.AppendDeletedColumn] != "append.deleted.v1" {
		t.Fatalf("append deleted identity=%q", identities[connector.AppendDeletedColumn])
	}
	if got := projected.Fragments[0].Batch.Records[0].After[connector.AppendOperationColumn]; got != "insert" {
		t.Fatalf("append operation=%v", got)
	}
	if got := projected.Fragments[0].Batch.Records[0].After[connector.AppendDeletedColumn]; got != false {
		t.Fatalf("append deleted=%v", got)
	}
}

func TestPlanTransactionDeterminismRapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		records := rapid.IntRange(1, rapidPlannerMaxRecords).Draw(t, "records")
		transaction := plannerTransaction(records)
		incarnationID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
		encoder := NewEncoder()
		encoder.targetEncoded = rapid.IntRange(1, 1<<16).Draw(t, "target")
		first, err := encoder.PlanTransaction(context.Background(), incarnationID, transaction)
		if err != nil {
			t.Fatal(err)
		}
		second, err := encoder.PlanTransaction(context.Background(), incarnationID, transaction)
		if err != nil {
			t.Fatal(err)
		}
		if first.LogicalBatchID != second.LogicalBatchID || len(first.Artifacts) != len(second.Artifacts) {
			t.Fatalf("non-deterministic plan identity/count")
		}
		var expectedOrdinal uint64
		for index := range first.Artifacts {
			left, right := first.Artifacts[index], second.Artifacts[index]
			if left.ID != right.ID || !bytes.Equal(left.Encoded, right.Encoded) {
				t.Fatalf("artifact %d differs on replay", index)
			}
			if left.FirstRecordOrdinal != expectedOrdinal {
				t.Fatalf("artifact %d first ordinal=%d, want %d", index, left.FirstRecordOrdinal, expectedOrdinal)
			}
			expectedOrdinal += left.RecordCount
		}
		if expectedOrdinal != uint64(records) {
			t.Fatalf("planned record coverage=%d, want %d", expectedOrdinal, records)
		}
	})
}

func TestPlanTransactionProducesStableLogicalAndArtifactIdentities(t *testing.T) {
	t.Parallel()

	incarnationID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	transaction := plannerTransaction(3)
	encoder := NewEncoder()
	encoder.targetEncoded = 1

	first, err := encoder.PlanTransaction(context.Background(), incarnationID, transaction)
	if err != nil {
		t.Fatal(err)
	}

	replayed := transaction
	replayed.Fragments = append([]connector.TransactionFragment(nil), transaction.Fragments...)
	replayed.Fragments[0].Batch = transaction.Fragments[0].Batch
	replayed.Fragments[0].Batch.Schema.Version = 99
	replayed.Fragments[0].Batch.Records = append([]connector.Record(nil), transaction.Fragments[0].Batch.Records...)
	for index := range replayed.Fragments[0].Batch.Records {
		replayed.Fragments[0].Batch.Records[index].SchemaVersion = 99
	}
	second, err := encoder.PlanTransaction(context.Background(), incarnationID, replayed)
	if err != nil {
		t.Fatal(err)
	}

	if first.LogicalBatchID == "" || first.LogicalBatchID != second.LogicalBatchID {
		t.Fatalf("logical batch IDs differ: %q != %q", first.LogicalBatchID, second.LogicalBatchID)
	}
	if len(first.Artifacts) < 2 || len(first.Artifacts) != len(second.Artifacts) {
		t.Fatalf("artifact counts = %d/%d, want equal sharded plans", len(first.Artifacts), len(second.Artifacts))
	}
	for index := range first.Artifacts {
		left, right := first.Artifacts[index], second.Artifacts[index]
		if left.ID != right.ID || left.SchemaID != right.SchemaID || !bytes.Equal(left.Encoded, right.Encoded) {
			t.Fatalf("artifact %d is not replay deterministic", index)
		}
		if left.LogicalBatchID != first.LogicalBatchID || left.RecordCount == 0 {
			t.Fatalf("artifact %d identity/count = %q/%d", index, left.LogicalBatchID, left.RecordCount)
		}
		if left.Partition != UnpartitionedValue || !strings.Contains(left.ObjectKey, "/partition=unpartitioned/") {
			t.Fatalf("artifact %d partition/key = %q/%q", index, left.Partition, left.ObjectKey)
		}
	}
}

func TestPlanTransactionCanonicalizesRecordPositionsBeforeIdentity(t *testing.T) {
	t.Parallel()

	transaction := plannerTransaction(1)
	canonical, err := NewEncoder().PlanTransaction(context.Background(), uuid.Nil, transaction)
	if err != nil {
		t.Fatal(err)
	}
	transaction.Fragments[0].Batch.Records[0].SourcePosition = "00000000/00000018"
	equivalent, err := NewEncoder().PlanTransaction(context.Background(), uuid.Nil, transaction)
	if err != nil {
		t.Fatal(err)
	}
	if canonical.LogicalBatchID != equivalent.LogicalBatchID || canonical.ContentHash != equivalent.ContentHash {
		t.Fatalf("equivalent source positions changed canonical identity: %s/%s != %s/%s", canonical.LogicalBatchID, canonical.ContentHash, equivalent.LogicalBatchID, equivalent.ContentHash)
	}
}

func TestPlanTransactionRejectsInvalidPerRecordPosition(t *testing.T) {
	t.Parallel()

	transaction := plannerTransaction(1)
	transaction.Fragments[0].Batch.Records[0].SourcePosition = "not-an-lsn"
	if _, err := NewEncoder().PlanTransaction(context.Background(), uuid.New(), transaction); err == nil || !strings.Contains(err.Error(), "record ordinal 0 source position") {
		t.Fatalf("PlanTransaction() error=%v, want per-record source position rejection", err)
	}
}

func TestPlanTransactionPreservesRecordOrdinalsAndDDLBarriers(t *testing.T) {
	t.Parallel()

	transaction := plannerTransaction(2)
	transaction.Fragments = append(transaction.Fragments,
		connector.TransactionFragment{Ordinal: 1, Batch: connector.Batch{
			Schema: connector.Schema{Namespace: "public", Name: "widgets", Version: 2},
			Records: []connector.Record{{
				Table: "widgets", Operation: connector.OpDDL, SchemaVersion: 2,
				DDL: "ALTER TABLE public.widgets ADD COLUMN note text", SourcePosition: "0/18",
			}},
		}},
		connector.TransactionFragment{Ordinal: 2, Batch: plannerTransaction(1).Fragments[0].Batch},
	)

	plan, err := NewEncoder().PlanTransaction(context.Background(), uuid.MustParse("22222222-2222-2222-2222-222222222222"), transaction)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Artifacts) != 2 {
		t.Fatalf("artifacts=%d, want two data artifacts around the DDL barrier", len(plan.Artifacts))
	}
	if len(plan.Barriers) != 1 || plan.Barriers[0].RecordOrdinal != 2 || plan.Barriers[0].FragmentOrdinal != 1 {
		t.Fatalf("barriers=%+v, want one ordered barrier at record ordinal 2", plan.Barriers)
	}
	if plan.Artifacts[0].FirstRecordOrdinal != 0 || plan.Artifacts[0].RecordCount != 2 {
		t.Fatalf("first artifact ordinal/count=%d/%d", plan.Artifacts[0].FirstRecordOrdinal, plan.Artifacts[0].RecordCount)
	}
	if plan.Artifacts[1].FirstRecordOrdinal != 3 || plan.Artifacts[1].RecordCount != 1 {
		t.Fatalf("second artifact ordinal/count=%d/%d", plan.Artifacts[1].FirstRecordOrdinal, plan.Artifacts[1].RecordCount)
	}
}

func plannerTransaction(records int) connector.SourceTransaction {
	changes := make([]connector.Record, 0, records)
	for index := 0; index < records; index++ {
		changes = append(changes, connector.Record{
			Table: "widgets", Operation: connector.OpInsert, SchemaVersion: 1,
			After:          map[string]any{"id": int64(index + 1), "value": strings.Repeat("x", 256)},
			SourcePosition: "0/18", Timestamp: time.Unix(100, 0).UTC(),
		})
	}
	return connector.SourceTransaction{
		SourceLineageID: "postgres-system/planner-v1", TransactionID: 7,
		BeginLSN: "0/10", CommitLSN: "0/17", EndLSN: "0/18",
		Checkpoint: connector.Checkpoint{LSN: "0/18", Timestamp: time.Unix(100, 0).UTC()},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{
				Schema: connector.Schema{Namespace: "public", Name: "widgets", Version: 1, Columns: []connector.Column{
					{Name: "id", Type: "int8", TypeMetadata: map[string]string{"source_relation_id": "42", "source_column_id": "1"}},
					{Name: "value", Type: "text", TypeMetadata: map[string]string{"source_relation_id": "42", "source_column_id": "2"}},
				}},
				Records: changes,
			},
		}},
	}
}
