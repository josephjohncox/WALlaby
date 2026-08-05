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
// can become durable delivery identities. Fragment ordinals are contiguous;
// projections must renumber surviving fragments rather than admitting gaps.
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
		expectedOrdinal++
		if fragment.Batch.Checkpoint.LSN != "" || len(fragment.Batch.Checkpoint.Metadata) != 0 {
			return fmt.Errorf("source transaction fragment %d carries an independent checkpoint", index)
		}
		if len(fragment.Batch.Records) == 0 {
			return fmt.Errorf("source transaction fragment %d is empty", index)
		}
		if err := ValidateBatch(fragment.Batch); err != nil {
			return fmt.Errorf("validate source transaction fragment %d: %w", index, err)
		}
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
	canonicalEndLSN, err := CanonicalizeCheckpointPosition(transaction.EndLSN)
	if err != nil {
		return "", err
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
		// Runtime transport and checkpoint metadata are not part of the source
		// transaction. They may change after publication or across a worker
		// restart without changing the canonical rows or barriers. The prepared
		// manifest remains authoritative for the checkpoint payload adopted after
		// an identical WAL replay.
		batch.Checkpoint = Checkpoint{LSN: canonicalEndLSN}
		batch.WireFormat = ""
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
func DeliveryLogicalBatchID(sourceLineageID, positionID, contentHash string) (string, error) {
	for name, value := range map[string]string{"source_lineage_id": sourceLineageID, "position_id": positionID, "content_hash": contentHash} {
		if strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("logical batch %s is required", name)
		}
	}
	digest := sha256.Sum256([]byte(sourceLineageID + "\x00" + positionID + "\x00" + contentHash))
	return "logical-batch:" + hex.EncodeToString(digest[:]), nil
}

func SourceTransactionIdentity(transaction SourceTransaction) (string, string, error) {
	contentHash, err := SourceTransactionContentHash(transaction)
	if err != nil {
		return "", "", err
	}
	position, err := CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		return "", "", err
	}
	logicalBatchID, err := DeliveryLogicalBatchID(transaction.SourceLineageID, position, contentHash)
	if err != nil {
		return "", "", err
	}
	return contentHash, logicalBatchID, nil
}

// SourceTransactionLogicalBatchID identifies one source commit independently
// from any worker generation or destination revision.
func SourceTransactionLogicalBatchID(transaction SourceTransaction) (string, error) {
	_, logicalBatchID, err := SourceTransactionIdentity(transaction)
	return logicalBatchID, err
}

// ManagedSchemaBaselinePayload is the exact source-schema state advanced with
// one authoritative checkpoint. Its canonical encoding and fingerprint are
// bound into delivery/publication manifests before external I/O.
type ManagedSchemaBaselinePayload struct {
	SourceLineageID string   `json:"source_lineage_id"`
	Schemas         []Schema `json:"schemas"`
}

// NewManagedSchemaBaselinePayload canonicalizes one transaction's source
// schemas. Duplicate relation identities collapse to the greatest observed
// schema version and relation order is deterministic.
func NewManagedSchemaBaselinePayload(sourceLineageID string, schemas []Schema) (ManagedSchemaBaselinePayload, error) {
	if strings.TrimSpace(sourceLineageID) == "" {
		return ManagedSchemaBaselinePayload{}, errors.New("managed schema-baseline source lineage is required")
	}
	byRelation := make(map[string]Schema, len(schemas))
	for _, schema := range schemas {
		if err := validateManagedSchemaBaselineIdentity(schema.Namespace, schema.Name); err != nil {
			return ManagedSchemaBaselinePayload{}, err
		}
		key := ManagedSchemaBaselineKey(schema.Namespace, schema.Name)
		if previous, exists := byRelation[key]; !exists || schema.Version >= previous.Version {
			byRelation[key] = schema
		}
	}
	keys := make([]string, 0, len(byRelation))
	for key := range byRelation {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	payload := ManagedSchemaBaselinePayload{SourceLineageID: sourceLineageID, Schemas: make([]Schema, 0, len(keys))}
	for _, key := range keys {
		schema := byRelation[key]
		// pgoutput relation versions are process-local decoder counters and cannot
		// participate in a retry-stable durable schema identity.
		schema.Version = 0
		payload.Schemas = append(payload.Schemas, schema)
	}
	return payload, nil
}

// Canonical returns the immutable JSON payload and lowercase SHA-256 identity.
func (p ManagedSchemaBaselinePayload) Canonical() ([]byte, string, error) {
	canonical, err := NewManagedSchemaBaselinePayload(p.SourceLineageID, p.Schemas)
	if err != nil {
		return nil, "", err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return nil, "", fmt.Errorf("encode managed schema-baseline payload: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return encoded, hex.EncodeToString(digest[:]), nil
}

// SourceTransactionSchemas extracts exact source relation schemas before any
// destination projection filters or renames them.
func SourceTransactionSchemas(transaction SourceTransaction) []Schema {
	schemas := make([]Schema, 0, len(transaction.Fragments))
	for _, fragment := range transaction.Fragments {
		if fragment.Batch.Schema.Name != "" {
			schemas = append(schemas, fragment.Batch.Schema)
		}
	}
	payload, err := NewManagedSchemaBaselinePayload(transaction.SourceLineageID, schemas)
	if err != nil {
		return nil
	}
	return payload.Schemas
}

// ManagedSchemaBaselineStore is the PostgreSQL-authoritative managed decoder
// baseline read contract. Advancement is available only through the internal
// transaction-scoped upsert helper owned by checkpoint finalizers.
type ManagedSchemaBaselineStore interface {
	Load(context.Context, RunFence, string) ([]Schema, error)
}

// ManagedSchemaBaselinesOptionKey carries a fence-validated baseline snapshot
// from the runner into the in-process PostgreSQL decoder. It is never stored in
// checkpoint metadata and is not an authority source.
const ManagedSchemaBaselinesOptionKey = "managed_postgres_schema_baselines_v1"

// DecodeManagedSchemaBaselineOption validates the runner-to-decoder snapshot.
func DecodeManagedSchemaBaselineOption(raw string) ([]Schema, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var baselines []Schema
	if err := json.Unmarshal([]byte(raw), &baselines); err != nil {
		return nil, fmt.Errorf("decode managed schema baselines: %w", err)
	}
	seen := make(map[string]struct{}, len(baselines))
	for _, schema := range baselines {
		if err := validateManagedSchemaBaselineIdentity(schema.Namespace, schema.Name); err != nil {
			return nil, err
		}
		key := ManagedSchemaBaselineKey(schema.Namespace, schema.Name)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate managed schema baseline %q.%q", schema.Namespace, schema.Name)
		}
		seen[key] = struct{}{}
	}
	return baselines, nil
}

// ManagedSchemaBaselineKey encodes an exact PostgreSQL relation identity for
// in-memory indexing. PostgreSQL identifiers cannot contain NUL, so the NUL
// delimiter is unambiguous and preserves every identifier byte, including
// case and leading or trailing spaces.
func ManagedSchemaBaselineKey(namespace, name string) string {
	return namespace + "\x00" + name
}

func validateManagedSchemaBaselineIdentity(namespace, name string) error {
	if namespace == "" {
		return errors.New("managed schema baseline namespace is required")
	}
	if name == "" {
		return errors.New("managed schema baseline table name is required")
	}
	if strings.IndexByte(namespace, 0) >= 0 || strings.IndexByte(name, 0) >= 0 {
		return errors.New("managed schema baseline identifiers cannot contain NUL")
	}
	return nil
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
