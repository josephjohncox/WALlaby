package artifactlog

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/pkg/connector"
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
	const wantLogicalBatchID = "logical-batch:7870a05bbda290ab23fc18120c0a52c4603785576da1dab035c531a4642351c7"
	const wantContentHash = "cece5f6823b17193c7219394bd63b8ce668d906641555af31b78d8b21bfb2b2b"
	const wantSchemaID = "f6b4df8e0254025c850b6be95241df78de7f68ce91a01b1ac2e2d60a1e8b3e03"
	const wantArtifactID = "d371bce40250b371fe38d4593152ef4e2393b06ddec5cb639640131678e5554f"
	const wantEncodedHash = "7e1385614bd5e5f28efb6e9f7973c581365a60a624b1ce1ea3592e0a2084f3bc"
	const wantObjectKey = "wallaby/artifacts/44444444-4444-4444-4444-444444444444/source=3c123b9685a8bc65/namespace=public/table=widgets/schema=f6b4df8e0254025c850b6be95241df78de7f68ce91a01b1ac2e2d60a1e8b3e03/partition=unpartitioned/shard=000000/d371bce40250b371fe38d4593152ef4e2393b06ddec5cb639640131678e5554f.parquet"
	if plan.LogicalBatchID != wantLogicalBatchID || plan.ContentHash != wantContentHash || artifact.SchemaID != wantSchemaID || artifact.ID != wantArtifactID || artifact.EncodedByteHash != wantEncodedHash || artifact.ObjectKey != wantObjectKey {
		t.Fatalf("golden identity changed:\nlogical=%q\ncontent=%q\nschema=%q\nartifact=%q\nencoded=%q\nkey=%q", plan.LogicalBatchID, plan.ContentHash, artifact.SchemaID, artifact.ID, artifact.EncodedByteHash, artifact.ObjectKey)
	}
}

func TestPlanTransactionDeterminismRapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		records := rapid.IntRange(1, 32).Draw(t, "records")
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
