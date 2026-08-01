package connector

import "context"

// SourceTransaction is a committed source transaction. Fragments preserve
// source order while allowing existing table-scoped Batch contracts to remain
// unchanged. Checkpoint is the transaction-end position and is the only
// position eligible for durable source feedback.
type SourceTransaction struct {
	SourceLineageID string
	TransactionID   uint32
	BeginLSN        string
	CommitLSN       string
	EndLSN          string
	Fragments       []TransactionFragment
	Checkpoint      Checkpoint
}

// TransactionFragment is a deterministic, ordered table/schema fragment of a
// committed source transaction.
type TransactionFragment struct {
	Ordinal uint64
	Batch   Batch
}

// TransactionalSource is an optional source contract for managed execution.
// Legacy Source.Read remains available for compatibility, but managed
// PostgreSQL execution consumes complete transactions through this interface.
type TransactionalSource interface {
	Source
	ReadTransaction(context.Context) (SourceTransaction, error)
}

// InitialCheckpointSource exposes the validated stream start after Open. A
// managed coordinator persists this cut and an ACK intent before the first
// source transaction, so a crash immediately after slot creation is recoverable.
type InitialCheckpointSource interface {
	Source
	InitialCheckpoint() (Checkpoint, bool)
}
