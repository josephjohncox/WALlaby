package clickhouse

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

type managedTransactionPlan struct {
	Fragments    []managedFragmentPlan
	Receipt      managedReceiptRow
	RecordCount  uint64
	EncodedBytes int64
}

type managedFragmentPlan struct {
	Ordinal            uint64
	QueryID            string
	DeduplicationToken string
	Rows               []managedChangelogRow
	EncodedBytes       int64
}

type managedPlanLimits struct {
	maxFragments      int
	maxRows           int
	maxBytes          int64
	maxRowsPerInsert  int
	maxBytesPerInsert int64
}

type managedChangelogRow struct {
	FlowID                string
	FlowIncarnationID     string
	SourceLineageID       string
	DestinationRevisionID string
	LogicalBatchID        string
	ContentHash           string
	SourcePosition        string
	TransactionID         uint64
	BeginLSN              string
	CommitLSN             string
	EndLSN                string
	FragmentOrdinal       uint64
	RecordOrdinal         uint64
	SourceNamespace       string
	SourceTable           string
	SchemaVersion         int64
	SchemaFingerprint     string
	SchemaJSON            string
	Operation             string
	Tombstone             uint8
	KeyJSON               string
	BeforeJSON            string
	AfterJSON             string
	Payload               string
	DDLPlan               string
	EventTime             time.Time
	RecordHash            string
	WallabyVersion        uint64
}

type managedReceiptRow struct {
	FlowID                string
	FlowIncarnationID     string
	SourceLineageID       string
	DestinationRevisionID string
	LogicalBatchID        string
	ContentHash           string
	SourcePosition        string
	TransactionID         uint64
	FragmentCount         uint64
	RecordCount           uint64
	QueryIDs              []string
	CommittedAt           time.Time
	WallabyVersion        uint64
	ExternalID            string
	QueryID               string
	DeduplicationToken    string
}

func planManagedTransactionWithLimits(intent connector.DeliveryIntent, transaction connector.SourceTransaction, limits managedPlanLimits) (managedTransactionPlan, error) {
	if err := intent.Validate(); err != nil {
		return managedTransactionPlan{}, err
	}
	if strings.TrimSpace(intent.LogicalBatchID) == "" {
		return managedTransactionPlan{}, errors.New("managed ClickHouse delivery requires logical_batch_id")
	}
	if err := transaction.Validate(); err != nil {
		return managedTransactionPlan{}, err
	}
	if limits.maxFragments <= 0 || limits.maxRows <= 0 || limits.maxBytes <= 0 || limits.maxRowsPerInsert <= 0 || limits.maxBytesPerInsert <= 0 {
		return managedTransactionPlan{}, errors.New("managed ClickHouse plan limits must be positive")
	}
	if len(transaction.Fragments) > limits.maxFragments {
		return managedTransactionPlan{}, fmt.Errorf("managed ClickHouse transaction has %d fragments, maximum is %d", len(transaction.Fragments), limits.maxFragments)
	}
	var recordCount uint64
	for _, fragment := range transaction.Fragments {
		recordCount += uint64(len(fragment.Batch.Records))
		if recordCount > uint64(limits.maxRows) {
			return managedTransactionPlan{}, fmt.Errorf("managed ClickHouse transaction has %d rows, maximum is %d", recordCount, limits.maxRows)
		}
	}
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return managedTransactionPlan{}, fmt.Errorf("identify managed ClickHouse transaction: %w", err)
	}
	if contentHash != intent.ContentHash || logicalBatchID != intent.LogicalBatchID || transaction.SourceLineageID != intent.SourceLineageID {
		return managedTransactionPlan{}, fmt.Errorf("%w: managed ClickHouse transaction identity differs from delivery intent", connector.ErrDeliveryConflict)
	}
	version, err := managedLSNVersion(transaction.EndLSN)
	if err != nil {
		return managedTransactionPlan{}, err
	}

	plan := managedTransactionPlan{Fragments: make([]managedFragmentPlan, 0, 1), RecordCount: recordCount}
	queryIDs := make([]string, 0, (int(recordCount)+limits.maxRowsPerInsert-1)/limits.maxRowsPerInsert)
	for _, fragment := range transaction.Fragments {
		schemaJSON, schemaFingerprint, schemaVersion, err := managedSchemaIdentity(fragment.Batch.Schema)
		if err != nil {
			return managedTransactionPlan{}, fmt.Errorf("plan managed fragment %d schema: %w", fragment.Ordinal, err)
		}
		for recordOrdinal, record := range fragment.Batch.Records {
			row, err := buildManagedChangelogRow(intent, transaction, fragment, uint64(recordOrdinal), record, version, schemaJSON, schemaFingerprint, schemaVersion)
			if err != nil {
				return managedTransactionPlan{}, fmt.Errorf("plan managed fragment %d record %d: %w", fragment.Ordinal, recordOrdinal, err)
			}
			rowBytes := managedRowBytes(row)
			if rowBytes > limits.maxBytesPerInsert {
				return managedTransactionPlan{}, fmt.Errorf("managed ClickHouse single row has %d encoded bytes, per-insert maximum is %d", rowBytes, limits.maxBytesPerInsert)
			}
			if plan.EncodedBytes+rowBytes > limits.maxBytes {
				return managedTransactionPlan{}, fmt.Errorf("managed ClickHouse transaction encoded bytes exceed maximum %d", limits.maxBytes)
			}
			needsInsert := len(plan.Fragments) == 0
			if !needsInsert {
				current := &plan.Fragments[len(plan.Fragments)-1]
				needsInsert = len(current.Rows) >= limits.maxRowsPerInsert || current.EncodedBytes+rowBytes > limits.maxBytesPerInsert
			}
			if needsInsert {
				ordinal := uint64(len(plan.Fragments))
				queryID := managedQueryIdentity(intent, "fragment", ordinal)
				plan.Fragments = append(plan.Fragments, managedFragmentPlan{
					Ordinal: ordinal, QueryID: queryID,
					DeduplicationToken: managedQueryIdentity(intent, "fragment-token", ordinal),
					Rows:               make([]managedChangelogRow, 0, min(limits.maxRowsPerInsert, limits.maxRows)),
				})
				queryIDs = append(queryIDs, queryID)
			}
			current := &plan.Fragments[len(plan.Fragments)-1]
			current.Rows = append(current.Rows, row)
			current.EncodedBytes += rowBytes
			plan.EncodedBytes += rowBytes
		}
	}

	receiptQueryID := managedQueryIdentity(intent, "receipt", 0)
	plan.Receipt = managedReceiptRow{
		FlowID: intent.FlowID, FlowIncarnationID: intent.FlowIncarnationID,
		SourceLineageID: intent.SourceLineageID, DestinationRevisionID: intent.DestinationRevisionID,
		LogicalBatchID: intent.LogicalBatchID, ContentHash: intent.ContentHash, SourcePosition: intent.PositionID,
		TransactionID: uint64(transaction.TransactionID), FragmentCount: uint64(len(transaction.Fragments)), RecordCount: recordCount,
		QueryIDs: queryIDs, CommittedAt: transaction.Checkpoint.Timestamp, WallabyVersion: version,
		ExternalID: managedDeliveryExternalID(intent), QueryID: receiptQueryID,
		DeduplicationToken: managedQueryIdentity(intent, "receipt-token", 0),
	}
	if plan.Receipt.CommittedAt.IsZero() {
		plan.Receipt.CommittedAt = time.Unix(0, 0).UTC()
	}
	return plan, nil
}

func buildManagedChangelogRow(
	intent connector.DeliveryIntent,
	transaction connector.SourceTransaction,
	fragment connector.TransactionFragment,
	recordOrdinal uint64,
	record connector.Record,
	version uint64,
	schemaJSON string,
	schemaFingerprint string,
	schemaVersion int64,
) (managedChangelogRow, error) {
	if record.Operation == connector.OpDDL && strings.TrimSpace(record.DDL) != "" {
		return managedChangelogRow{}, errors.New("managed ClickHouse append profile rejects raw DDL; use a structured DDL plan barrier")
	}
	if record.Operation == connector.OpDDL && len(record.DDLPlan) == 0 {
		return managedChangelogRow{}, errors.New("managed ClickHouse append profile requires a structured DDL plan")
	}
	switch record.Operation {
	case connector.OpInsert, connector.OpUpdate, connector.OpDelete, connector.OpLoad, connector.OpDDL:
	default:
		return managedChangelogRow{}, fmt.Errorf("unsupported managed ClickHouse operation %q", record.Operation)
	}

	keyJSON, err := managedKeyJSON(record.Key)
	if err != nil {
		return managedChangelogRow{}, fmt.Errorf("encode key: %w", err)
	}
	beforeJSON, err := managedMapJSON(record.Before)
	if err != nil {
		return managedChangelogRow{}, fmt.Errorf("encode before image: %w", err)
	}
	afterJSON, err := managedMapJSON(record.After)
	if err != nil {
		return managedChangelogRow{}, fmt.Errorf("encode after image: %w", err)
	}
	eventTime := record.Timestamp
	if eventTime.IsZero() {
		eventTime = transaction.Checkpoint.Timestamp
	}
	if eventTime.IsZero() {
		eventTime = time.Unix(0, 0).UTC()
	}
	row := managedChangelogRow{
		FlowID: intent.FlowID, FlowIncarnationID: intent.FlowIncarnationID,
		SourceLineageID: intent.SourceLineageID, DestinationRevisionID: intent.DestinationRevisionID,
		LogicalBatchID: intent.LogicalBatchID, ContentHash: intent.ContentHash, SourcePosition: intent.PositionID,
		TransactionID: uint64(transaction.TransactionID), BeginLSN: transaction.BeginLSN, CommitLSN: transaction.CommitLSN, EndLSN: transaction.EndLSN,
		FragmentOrdinal: fragment.Ordinal, RecordOrdinal: recordOrdinal,
		SourceNamespace: fragment.Batch.Schema.Namespace, SourceTable: fragment.Batch.Schema.Name,
		SchemaVersion: schemaVersion, SchemaFingerprint: schemaFingerprint, SchemaJSON: schemaJSON,
		Operation: string(record.Operation), KeyJSON: keyJSON, BeforeJSON: beforeJSON, AfterJSON: afterJSON,
		Payload: string(record.Payload), DDLPlan: string(record.DDLPlan), EventTime: eventTime,
		WallabyVersion: version,
	}
	if record.Operation == connector.OpDelete {
		row.Tombstone = 1
	}
	row.RecordHash, err = managedRecordHash(row)
	if err != nil {
		return managedChangelogRow{}, err
	}
	return row, nil
}

func managedSchemaIdentity(schema connector.Schema) (string, string, int64, error) {
	// pgoutput relation versions are process-local counters. Canonical schema
	// identity is derived only from the namespace, table, and column contract so
	// replaying the same WAL after a decoder restart cannot change target rows.
	schema.Version = 0
	encoded, err := json.Marshal(schema)
	if err != nil {
		return "", "", 0, fmt.Errorf("encode source schema: %w", err)
	}
	digest := sha256.Sum256(encoded)
	version := int64(binary.BigEndian.Uint64(digest[:8]) & (^uint64(0) >> 1))
	return string(encoded), hex.EncodeToString(digest[:]), version, nil
}

func managedKeyJSON(raw []byte) (string, error) {
	if len(raw) == 0 {
		return "null", nil
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func managedMapJSON(value map[string]any) (string, error) {
	if value == nil {
		return "null", nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func managedRecordHash(row managedChangelogRow) (string, error) {
	row.RecordHash = ""
	encoded, err := json.Marshal(row)
	if err != nil {
		return "", fmt.Errorf("encode managed changelog row: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func managedQueryIdentity(intent connector.DeliveryIntent, kind string, ordinal uint64) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		intent.FlowIncarnationID, intent.SourceLineageID, intent.DestinationRevisionID,
		intent.LogicalBatchID, intent.PositionID, intent.ContentHash, kind, strconv.FormatUint(ordinal, 10),
	}, "\x00")))
	return "wallaby-ch-" + hex.EncodeToString(digest[:])
}

func managedDeliveryExternalID(intent connector.DeliveryIntent) string {
	return managedQueryIdentity(intent, "delivery", 0)
}

func managedLSNVersion(lsn string) (uint64, error) {
	canonical, err := connector.CanonicalizeCheckpointPosition(lsn)
	if err != nil {
		return 0, fmt.Errorf("canonicalize managed ClickHouse LSN: %w", err)
	}
	parts := strings.Split(canonical, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid managed ClickHouse LSN %q", canonical)
	}
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("parse managed ClickHouse LSN high word: %w", err)
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("parse managed ClickHouse LSN low word: %w", err)
	}
	return high<<32 | low, nil
}
