package replication

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestManagedExistingSlotRequiresAuthoritativeCheckpoint(t *testing.T) {
	if err := validateExistingSlotAuthorization(true, 0); err == nil {
		t.Fatal("managed existing slot without checkpoint should fail")
	}
	if err := validateExistingSlotAuthorization(true, pglogrepl.LSN(0x30)); err != nil {
		t.Fatalf("managed existing slot with checkpoint: %v", err)
	}
	if err := validateExistingSlotAuthorization(false, 0); err != nil {
		t.Fatalf("legacy existing slot may use confirmed flush position: %v", err)
	}
}

func TestNewSlotStartPreservesLegacyRequestedPosition(t *testing.T) {
	requested := pglogrepl.LSN(0x20)
	consistent := pglogrepl.LSN(0x30)
	got, err := resolveNewSlotStart(false, requested, consistent)
	if err != nil {
		t.Fatal(err)
	}
	if got != requested {
		t.Fatalf("new legacy slot start=%s, want requested %s", got, requested)
	}
}

func TestManagedNewSlotRequiresExactConsistentPoint(t *testing.T) {
	consistent := pglogrepl.LSN(0x30)
	if _, err := resolveNewSlotStart(true, pglogrepl.LSN(0x20), consistent); err == nil {
		t.Fatal("managed new slot accepted a checkpoint other than its consistent point")
	}
	got, err := resolveNewSlotStart(true, 0, consistent)
	if err != nil {
		t.Fatal(err)
	}
	if got != consistent {
		t.Fatalf("new managed slot start=%s, want consistent point %s", got, consistent)
	}
}

func TestPostgresExistingSlotStartsAtAuthorizedPosition(t *testing.T) {
	state := replicationSlotState{
		SlotType:          "logical",
		Plugin:            "pgoutput",
		Database:          "wallaby",
		RestartLSN:        pglogrepl.LSN(0x20),
		ConfirmedFlushLSN: pglogrepl.LSN(0x30),
		WALStatus:         "reserved",
	}

	got, err := resolveSlotStart(state, "pgoutput", "wallaby", 0, pglogrepl.LSN(0x40))
	if err != nil {
		t.Fatal(err)
	}
	if got != pglogrepl.LSN(0x30) {
		t.Fatalf("start LSN=%s, want confirmed flush 0/30", got)
	}
}

func TestExistingSlotRejectsCheckpointBehindConfirmedFlush(t *testing.T) {
	state := replicationSlotState{
		SlotType:          "logical",
		Plugin:            "pgoutput",
		Database:          "wallaby",
		RestartLSN:        pglogrepl.LSN(0x20),
		ConfirmedFlushLSN: pglogrepl.LSN(0x30),
		WALStatus:         "reserved",
	}

	if _, err := resolveSlotStart(state, "pgoutput", "wallaby", pglogrepl.LSN(0x28), pglogrepl.LSN(0x40)); err == nil {
		t.Fatal("expected checkpoint behind confirmed_flush_lsn to fail closed")
	}
}

func TestExistingSlotRejectsWrongLineage(t *testing.T) {
	tests := []struct {
		name  string
		state replicationSlotState
	}{
		{name: "physical", state: replicationSlotState{SlotType: "physical", Plugin: "", Database: "wallaby", RestartLSN: 0x20}},
		{name: "plugin", state: replicationSlotState{SlotType: "logical", Plugin: "wal2json", Database: "wallaby", RestartLSN: 0x20}},
		{name: "database", state: replicationSlotState{SlotType: "logical", Plugin: "pgoutput", Database: "other", RestartLSN: 0x20}},
		{name: "active", state: replicationSlotState{SlotType: "logical", Plugin: "pgoutput", Database: "wallaby", Active: true, RestartLSN: 0x20}},
		{name: "lost", state: replicationSlotState{SlotType: "logical", Plugin: "pgoutput", Database: "wallaby", RestartLSN: 0x20, WALStatus: "lost"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := resolveSlotStart(tt.state, "pgoutput", "wallaby", 0, pglogrepl.LSN(0x40)); err == nil {
				t.Fatal("expected incompatible slot to be rejected")
			}
		})
	}
}
