package connector

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

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

// Validate rejects incomplete or reordered committed transactions before they
// can become durable delivery identities. Fragment ordinals are contiguous so
// table, schema, DDL, and control barriers cannot be collapsed or reordered.
func (t SourceTransaction) Validate() error {
	if strings.TrimSpace(t.SourceLineageID) == "" {
		return errors.New("source transaction lineage is required")
	}
	if t.TransactionID == 0 {
		return errors.New("source transaction ID must be positive")
	}
	for name, value := range map[string]string{
		"begin_lsn":  t.BeginLSN,
		"commit_lsn": t.CommitLSN,
		"end_lsn":    t.EndLSN,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("source transaction %s is required", name)
		}
		if _, err := CanonicalizeCheckpointPosition(value); err != nil {
			return fmt.Errorf("canonicalize source transaction %s: %w", name, err)
		}
	}
	endLSN, err := CanonicalizeCheckpointPosition(t.EndLSN)
	if err != nil {
		return err
	}
	checkpointLSN, err := CanonicalizeCheckpointPosition(t.Checkpoint.LSN)
	if err != nil {
		return fmt.Errorf("canonicalize source transaction checkpoint: %w", err)
	}
	if checkpointLSN != endLSN {
		return fmt.Errorf("source transaction checkpoint %s does not match end LSN %s", checkpointLSN, endLSN)
	}
	var expectedOrdinal uint64
	for index, fragment := range t.Fragments {
		if fragment.Ordinal != expectedOrdinal {
			return fmt.Errorf("source transaction fragment ordinal %d at index %d is not contiguous", fragment.Ordinal, index)
		}
		if fragment.Batch.Checkpoint.LSN != "" || len(fragment.Batch.Checkpoint.Metadata) != 0 {
			return fmt.Errorf("source transaction fragment %d carries an independent checkpoint", index)
		}
		if len(fragment.Batch.Records) == 0 {
			return fmt.Errorf("source transaction fragment %d is empty", index)
		}
		if err := ValidateBatch(fragment.Batch); err != nil {
			return fmt.Errorf("validate source transaction fragment %d: %w", index, err)
		}
		expectedOrdinal++
	}
	return nil
}

// SourceTransactionContentHash returns the stable logical identity of a full
// committed transaction. Observation timestamps, process-local schema counters,
// and checkpoint recovery metadata do not participate, while WAL positions and
// transaction and fragment order always do.
func SourceTransactionContentHash(transaction SourceTransaction) (string, error) {
	if err := transaction.Validate(); err != nil {
		return "", fmt.Errorf("validate source transaction: %w", err)
	}
	hash := sha256.New()
	write := func(value string) {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(value))
	}
	write(transaction.SourceLineageID)
	write(fmt.Sprintf("%d", transaction.TransactionID))
	for _, lsn := range []string{transaction.BeginLSN, transaction.CommitLSN, transaction.EndLSN} {
		canonical, err := CanonicalizeCheckpointPosition(lsn)
		if err != nil {
			return "", fmt.Errorf("canonicalize source transaction LSN %q: %w", lsn, err)
		}
		write(canonical)
	}
	for index := range transaction.Fragments {
		fragment := transaction.Fragments[index]
		batch := fragment.Batch
		// pgoutput relation versions are process-local counters. The schema
		// columns and record content define the logical change; counters must
		// not make an identical WAL replay conflict with its target marker.
		batch.Schema.Version = 0
		batch.Records = append([]Record(nil), batch.Records...)
		for recordIndex := range batch.Records {
			batch.Records[recordIndex].SchemaVersion = 0
			// pgoutput's XLogData server time is an observation timestamp. The
			// same WAL transaction can receive a different value after restart,
			// so it cannot participate in immutable delivery identity.
			batch.Records[recordIndex].Timestamp = time.Time{}
		}
		batch.Checkpoint = transaction.Checkpoint
		// Managed schema baselines and other checkpoint metadata are durable
		// recovery state, not WAL content. Relation-version counters can differ
		// across process restarts; the prepared manifest remains authoritative
		// for the checkpoint payload adopted after an identical WAL replay.
		batch.Checkpoint.Metadata = nil
		batchHash, err := BatchContentHash(batch)
		if err != nil {
			return "", fmt.Errorf("hash source transaction fragment %d: %w", index, err)
		}
		write(fmt.Sprintf("%d", fragment.Ordinal))
		write(batchHash)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// SourceTransactionIdentity computes the content hash and logical batch ID in
// one pass. Callers at each trust seam can validate independently without
// hashing the same transaction twice at that seam. Process-local schema
// counters, observation timestamps, and checkpoint recovery metadata do not
// participate in the identity of an otherwise identical WAL replay.
func SourceTransactionIdentity(transaction SourceTransaction) (string, string, error) {
	contentHash, err := SourceTransactionContentHash(transaction)
	if err != nil {
		return "", "", err
	}
	position, err := CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256([]byte(transaction.SourceLineageID + "\x00" + position + "\x00" + contentHash))
	return contentHash, "logical-batch:" + hex.EncodeToString(digest[:]), nil
}

// SourceTransactionLogicalBatchID identifies one source commit independently
// from any worker generation or destination revision.
func SourceTransactionLogicalBatchID(transaction SourceTransaction) (string, error) {
	_, logicalBatchID, err := SourceTransactionIdentity(transaction)
	return logicalBatchID, err
}

// ManagedSchemaBaselinesMetadataKey stores the last delivered source schema
// set on the authoritative checkpoint. The baseline lets a restarted pgoutput
// decoder diff the first Relation message against the schema that actually
// reached the destination, rather than an empty process-local cache.
const ManagedSchemaBaselinesMetadataKey = "managed_postgres_schema_baselines_v1"

// MergeManagedSchemaBaselines returns a copied checkpoint metadata map with
// every schema observed in transaction merged into a stable, sorted encoding.
func MergeManagedSchemaBaselines(metadata map[string]string, transaction SourceTransaction) (map[string]string, error) {
	baselines, err := DecodeManagedSchemaBaselines(metadata[ManagedSchemaBaselinesMetadataKey])
	if err != nil {
		return nil, err
	}
	// Avoid adding attacker-influenced slice lengths for a capacity hint: the
	// addition can overflow even though map growth itself is safe.
	byTable := make(map[string]Schema)
	for _, schema := range baselines {
		byTable[managedSchemaBaselineKey(schema.Namespace, schema.Name)] = schema
	}
	for _, fragment := range transaction.Fragments {
		schema := fragment.Batch.Schema
		if strings.TrimSpace(schema.Name) == "" {
			continue
		}
		byTable[managedSchemaBaselineKey(schema.Namespace, schema.Name)] = schema
	}
	baselines = baselines[:0]
	for _, schema := range byTable {
		baselines = append(baselines, schema)
	}
	sort.Slice(baselines, func(i, j int) bool {
		return managedSchemaBaselineKey(baselines[i].Namespace, baselines[i].Name) < managedSchemaBaselineKey(baselines[j].Namespace, baselines[j].Name)
	})
	encoded, err := json.Marshal(baselines)
	if err != nil {
		return nil, fmt.Errorf("marshal managed schema baselines: %w", err)
	}
	result := make(map[string]string, len(metadata))
	for key, value := range metadata {
		result[key] = value
	}
	result[ManagedSchemaBaselinesMetadataKey] = string(encoded)
	return result, nil
}

// DecodeManagedSchemaBaselines validates a checkpoint baseline encoding.
func DecodeManagedSchemaBaselines(raw string) ([]Schema, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var baselines []Schema
	if err := json.Unmarshal([]byte(raw), &baselines); err != nil {
		return nil, fmt.Errorf("decode managed schema baselines: %w", err)
	}
	seen := make(map[string]struct{}, len(baselines))
	for _, schema := range baselines {
		if strings.TrimSpace(schema.Name) == "" {
			return nil, errors.New("managed schema baseline table name is required")
		}
		key := managedSchemaBaselineKey(schema.Namespace, schema.Name)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate managed schema baseline %s", key)
		}
		seen[key] = struct{}{}
	}
	return baselines, nil
}

func managedSchemaBaselineKey(namespace, name string) string {
	return strings.ToLower(strings.TrimSpace(namespace)) + "\x00" + strings.ToLower(strings.TrimSpace(name))
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

// SourceFlushEvidence is the source PostgreSQL position observed after a
// standby-status update, not merely an in-process scheduling acknowledgement.
type SourceFlushEvidence struct {
	ObservedFlushLSN string
}

// FlushEvidenceSource sends source feedback and proves the resulting logical
// slot flush position. The managed coordinator validates authority before and
// after this external source operation; feedback itself is monotonic.
type FlushEvidenceSource interface {
	Source
	AckWithEvidence(context.Context, Checkpoint) (SourceFlushEvidence, error)
}
