package bootstrap

import (
	"testing"

	"github.com/google/uuid"
)

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
