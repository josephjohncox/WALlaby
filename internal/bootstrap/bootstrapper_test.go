package bootstrap

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCleanupOwnedResourcesRequiresGuardBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()
	fence := authority.CleanupFence{RunFence: authority.RunFence{
		FlowIncarnationID: uuid.New(), FlowID: "cleanup", Generation: 1,
		AcquisitionID: uuid.New(), ExecutionID: "cleanup-execution", LeaseEpoch: 1,
	}}
	err := (&Bootstrapper{}).CleanupOwnedResources(context.Background(), fence, nil)
	if err == nil || !strings.Contains(err.Error(), "requires cleanup authority guard") {
		t.Fatalf("CleanupOwnedResources() error=%v, want mandatory guard rejection", err)
	}
}

func TestGenerationSlotNameSeparatesIncarnationsAndGenerations(t *testing.T) {
	firstIncarnation := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	secondIncarnation := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	first := GenerationSlotName("flow", firstIncarnation, 1)
	if first == GenerationSlotName("flow", firstIncarnation, 2) {
		t.Fatal("slot name reused across generations")
	}
	if first == GenerationSlotName("flow", secondIncarnation, 1) {
		t.Fatal("slot name reused across flow incarnations")
	}
	if len(first) > 63 {
		t.Fatalf("slot name length=%d exceeds PostgreSQL identifier limit", len(first))
	}
}

func TestDeliverTaskBatchRequiresFrozenManifestAndExactSchema(t *testing.T) {
	bootstrapper := &Bootstrapper{}
	sourceSchema := connector.Schema{Name: "accounts", Namespace: "public", Columns: []connector.Column{{Name: "id", Type: "int8"}, {Name: "secret", Type: "text"}}}
	destinationSchema := connector.Schema{Name: "customers", Namespace: "warehouse", Columns: []connector.Column{{Name: "customer_id", Type: "int8"}}}
	policy := connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"customer_id"}, ProjectionFingerprint: "mapping-v1"}
	task := SnapshotTask{
		RelationID: 1,
		TaskID:     "full",
		Namespace:  "public",
		Table:      "accounts",
		Schema:     sourceSchema,
		KeyColumns: []string{"id"},
		Delivery: SnapshotDeliveryContract{
			Version: SnapshotDeliveryContractV1, Schema: destinationSchema, WritePolicy: policy, ProjectionFingerprint: "mapping-v1",
		},
	}
	err := bootstrapper.DeliverTaskBatch(context.Background(), authority.ClaimFence{}, ExportedSnapshot{}, task, 1, nil, true, "destination", connector.Batch{Schema: destinationSchema, WritePolicy: policy}, nil)
	if err == nil || !strings.Contains(err.Error(), "frozen manifest identity") {
		t.Fatalf("unfrozen delivery error=%v", err)
	}
	frozen := ExportedSnapshot{SourceLineageID: "lineage", PublicationRevision: "publication-v1", ManifestHash: "manifest-v1"}
	missingContract := task
	missingContract.Delivery = SnapshotDeliveryContract{}
	err = bootstrapper.DeliverTaskBatch(context.Background(), authority.ClaimFence{}, frozen, missingContract, 1, nil, true, "destination", connector.Batch{Schema: destinationSchema, WritePolicy: policy}, nil)
	if err == nil || !strings.Contains(err.Error(), "explicit frozen destination contract") {
		t.Fatalf("missing destination contract error=%v", err)
	}
	err = bootstrapper.DeliverTaskBatch(context.Background(), authority.ClaimFence{}, frozen, task, 1, nil, true, "destination", connector.Batch{Schema: sourceSchema, WritePolicy: policy}, nil)
	if err == nil || !strings.Contains(err.Error(), "frozen destination schema or write policy") {
		t.Fatalf("source-shaped delivery error=%v", err)
	}
}

func TestSnapshotTaskAndDestinationContractPreserveWhitespaceOnlyIdentifiers(t *testing.T) {
	task := SnapshotTask{
		RelationID: 1, TaskID: "full", Namespace: " ", Table: " ",
		Schema:     connector.Schema{Namespace: " ", Name: " ", Columns: []connector.Column{{Name: "id", Type: "bigint"}}},
		KeyColumns: []string{"id"},
		Delivery: SnapshotDeliveryContract{
			Version:               SnapshotDeliveryContractV1,
			Schema:                connector.Schema{Namespace: "  ", Name: " ", Columns: []connector.Column{{Name: "mapped_id", Type: "bigint"}}},
			ProjectionFingerprint: "mapping-v1",
			WritePolicy:           connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: "mapping-v1"},
		},
	}
	if _, err := SnapshotManifestHash([]SnapshotTask{task}); err != nil {
		t.Fatalf("whitespace-only PostgreSQL identifiers rejected: %v", err)
	}
	for name, mutate := range map[string]func(*SnapshotTask){
		"empty source namespace":  func(task *SnapshotTask) { task.Namespace = "" },
		"NUL source table":        func(task *SnapshotTask) { task.Table = "bad\x00table" },
		"empty destination table": func(task *SnapshotTask) { task.Delivery.Schema.Name = "" },
		"NUL destination schema":  func(task *SnapshotTask) { task.Delivery.Schema.Namespace = "bad\x00schema" },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := task
			mutate(&invalid)
			if _, err := SnapshotManifestHash([]SnapshotTask{invalid}); err == nil {
				t.Fatal("invalid identifier admitted")
			}
		})
	}
}

func TestImportSnapshotCommandRejectsUntrustedNames(t *testing.T) {
	validNames := []string{
		"00000003-0000001B-1",
		"0000000A-000000F1-1",
	}
	for _, name := range validNames {
		want := "SET TRANSACTION SNAPSHOT '" + name + "'"
		if got, ok := importSnapshotCommand(name); !ok || got != want {
			t.Fatalf("valid snapshot command=(%q,%v), want %q", got, ok, want)
		}
	}
	if _, ok := importSnapshotCommand("x'; DROP TABLE flows; --"); ok {
		t.Fatal("expected untrusted snapshot name to be rejected")
	}
}
