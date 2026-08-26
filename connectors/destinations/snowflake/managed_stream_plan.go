package snowflake

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	streamReceiptKindAppend  = "append"
	streamReceiptKindRelease = "release"
	streamStatusCommitted    = "COMMITTED"
	streamStatusReleased     = "RELEASED"
)

// managedStreamReceipt is the durable destination proof that one committed
// transaction's rows fully arrived in the streaming target. It carries the
// logical delivery identity, the deterministic append identity, and the channel
// and offset-token evidence observed at commit so reconciliation binds them
// together. The receipt is written only after SQL-observed row completeness, so
// the receipt — not any transport token — is the adoption authority.
type managedStreamReceipt struct {
	kind                  string
	profileVersion        string
	flowID                string
	flowIncarnationID     string
	sourceLineageID       string
	destinationRevisionID string
	logicalBatchID        string
	positionID            string
	contentHash           string
	schemaContractHash    string
	catalogFingerprint    string
	manifestHash          string
	externalID            string
	requestID             string
	generation            int64
	acquisitionID         string
	leaseEpoch            int64
	transactionID         uint32
	fragmentCount         int
	recordCount           int
	channelName           string
	offsetToken           string
	pipeRevision          string
	channelRevision       int64
	committedOffsetToken  string
	rowsContentHash       string
	receiptStatus         string
}

// managedStreamPlan is the immutable, replay-convergent plan for one committed
// transaction: the ordered append rows plus their deterministic identity and the
// durable receipt that a successful, SQL-observed append produces.
type managedStreamPlan struct {
	identity           managedStreamIdentity
	appendPlan         streamAppendPlan
	rows               []streamChangelogRow
	rowHashes          []string
	rowsContentHash    string
	rowCount           int
	encodedBytes       int64
	catalogFingerprint string
	receipt            managedStreamReceipt
}

func planManagedStreamTransaction(cfg streamConfig, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (managedStreamPlan, error) {
	if err := intent.Validate(); err != nil {
		return managedStreamPlan{}, err
	}
	if err := validateManagedSnowflakeIntentBounds(intent); err != nil {
		return managedStreamPlan{}, err
	}
	if intent.FlowID != cfg.flowID {
		return managedStreamPlan{}, fmt.Errorf("%w: delivery flow %q differs from admitted streaming Snowflake flow %q", connector.ErrDeliveryConflict, intent.FlowID, cfg.flowID)
	}
	if intent.DestinationRevisionID != cfg.destinationRevision {
		return managedStreamPlan{}, fmt.Errorf("%w: delivery destination revision %q differs from admitted streaming Snowflake revision %q", connector.ErrDeliveryConflict, intent.DestinationRevisionID, cfg.destinationRevision)
	}
	if err := transaction.Validate(); err != nil {
		return managedStreamPlan{}, err
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		return managedStreamPlan{}, fmt.Errorf("identify streaming Snowflake checkpoint: %w", err)
	}
	if positionID != intent.PositionID {
		return managedStreamPlan{}, fmt.Errorf("%w: streaming Snowflake transaction checkpoint position %q differs from delivery intent %q", connector.ErrDeliveryConflict, positionID, intent.PositionID)
	}
	if cfg.maxTransactionRows <= 0 || cfg.maxTransactionBytes <= 0 || cfg.maxFragments <= 0 || cfg.maxRowBytes <= 0 {
		return managedStreamPlan{}, errors.New("streaming Snowflake transaction bounds must be positive")
	}
	if len(transaction.Fragments) > cfg.maxFragments {
		return managedStreamPlan{}, fmt.Errorf("streaming Snowflake transaction has %d fragments, maximum is %d", len(transaction.Fragments), cfg.maxFragments)
	}
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return managedStreamPlan{}, fmt.Errorf("identify streaming Snowflake transaction: %w", err)
	}
	if contentHash != intent.ContentHash || logicalBatchID != intent.LogicalBatchID || transaction.SourceLineageID != intent.SourceLineageID {
		return managedStreamPlan{}, fmt.Errorf("%w: streaming Snowflake transaction identity differs from delivery intent", connector.ErrDeliveryConflict)
	}
	contractHash, err := ManagedSchemaContractHash(cfg.schemaContract)
	if err != nil {
		return managedStreamPlan{}, err
	}
	if contractHash != cfg.schemaContractHash {
		return managedStreamPlan{}, fmt.Errorf("%w: streaming Snowflake configured schema contract hash differs", connector.ErrDeliveryConflict)
	}
	keyColumns, err := managedIdentityColumns(cfg.schemaContract)
	if err != nil {
		return managedStreamPlan{}, err
	}

	appendPlan := newStreamAppendPlan(cfg, intent)
	offsetToken := streamOffsetToken(intent)

	rows := make([]streamChangelogRow, 0)
	encodedBytes := int64(0)
	appendOrdinal := uint64(0)
	for _, fragment := range transaction.Fragments {
		controlFragment := len(fragment.Batch.Records) > 0 && fragment.Batch.Records[0].Operation == connector.OpDDL
		if !controlFragment {
			if err := validateManagedRuntimeSchema(cfg.schemaContract, fragment.Batch.Schema); err != nil {
				return managedStreamPlan{}, fmt.Errorf("validate streaming Snowflake fragment %d schema: %w", fragment.Ordinal, err)
			}
		}
		for recordIndex, record := range fragment.Batch.Records {
			// #nosec G115 -- recordIndex is a non-negative slice index and each record contributes to the bounded maxTransactionRows maximum enforced below.
			boundedRecordIndex := uint64(recordIndex)
			row, size, err := buildStreamChangelogRow(cfg, intent, transaction, fragment, keyColumns, offsetToken, appendOrdinal, boundedRecordIndex, record)
			if err != nil {
				return managedStreamPlan{}, fmt.Errorf("plan streaming Snowflake fragment %d record %d: %w", fragment.Ordinal, recordIndex, err)
			}
			if size > cfg.maxRowBytes {
				return managedStreamPlan{}, fmt.Errorf("%w: streaming Snowflake row %d is %d encoded bytes, exceeding admitted per-row maximum %d", connector.ErrDeliveryConflict, appendOrdinal, size, cfg.maxRowBytes)
			}
			rows = append(rows, row)
			appendOrdinal++
			if len(rows) > cfg.maxTransactionRows {
				return managedStreamPlan{}, fmt.Errorf("streaming Snowflake transaction has more than %d records", cfg.maxTransactionRows)
			}
			encodedBytes += size
			if encodedBytes > cfg.maxTransactionBytes {
				return managedStreamPlan{}, fmt.Errorf("streaming Snowflake transaction exceeds %d encoded bytes", cfg.maxTransactionBytes)
			}
		}
	}
	if len(rows) == 0 {
		return managedStreamPlan{}, errors.New("streaming Snowflake transaction has no appendable rows")
	}

	rowsContentHash, err := streamRowsContentHash(rows)
	if err != nil {
		return managedStreamPlan{}, err
	}
	rowHashes := make([]string, 0, len(rows))
	for index := range rows {
		rowHashes = append(rowHashes, rows[index].RowHash)
	}

	identity, err := newManagedStreamIdentity(cfg, intent, appendPlan, intent.ContentHash)
	if err != nil {
		return managedStreamPlan{}, err
	}

	receipt := managedStreamReceipt{
		kind: streamReceiptKindAppend, profileVersion: cfg.profile, flowID: intent.FlowID, flowIncarnationID: intent.FlowIncarnationID,
		sourceLineageID: intent.SourceLineageID, destinationRevisionID: intent.DestinationRevisionID,
		logicalBatchID: intent.LogicalBatchID, positionID: intent.PositionID, contentHash: intent.ContentHash,
		schemaContractHash: cfg.schemaContractHash, manifestHash: identity.manifestHash, externalID: identity.externalID,
		generation: intent.Generation, acquisitionID: intent.AcquisitionID, leaseEpoch: intent.LeaseEpoch,
		transactionID: transaction.TransactionID, fragmentCount: len(transaction.Fragments), recordCount: len(rows),
		channelName: identity.channelName, offsetToken: identity.offsetToken, rowsContentHash: rowsContentHash,
		receiptStatus: streamStatusCommitted,
	}
	return managedStreamPlan{
		identity: identity, appendPlan: appendPlan, rows: rows, rowHashes: rowHashes, rowsContentHash: rowsContentHash,
		rowCount: len(rows), encodedBytes: encodedBytes, receipt: receipt,
	}, nil
}

func newStreamAppendPlan(cfg streamConfig, intent connector.DeliveryIntent) streamAppendPlan {
	return streamAppendPlan{
		target:      managedSnowflakeStreamQualified(cfg, cfg.table),
		pipeRef:     managedSnowflakeStreamQualified(cfg, cfg.pipe),
		channelName: streamChannelName(cfg, intent),
		columns:     streamChangelogColumns(),
	}
}

func buildStreamChangelogRow(cfg streamConfig, intent connector.DeliveryIntent, transaction connector.SourceTransaction, fragment connector.TransactionFragment, keyColumns []string, offsetToken string, appendOrdinal, recordOrdinal uint64, record connector.Record) (streamChangelogRow, int64, error) {
	schema := fragment.Batch.Schema
	table := record.Table
	if table == "" {
		table = schema.Name
	}
	if schema.Namespace != cfg.sourceSchema || schema.Name != cfg.sourceTable || table != cfg.sourceTable {
		return streamChangelogRow{}, 0, fmt.Errorf("source relation %s.%s/%s is outside admitted relation %s.%s", schema.Namespace, schema.Name, table, cfg.sourceSchema, cfg.sourceTable)
	}
	if record.Operation == connector.OpDDL {
		return streamChangelogRow{}, 0, fmt.Errorf("%w: managed streaming Snowflake append rejects all DDL until live crash recovery evidence exists", errManagedSnowflakeSchemaNotReconciled)
	}
	if record.Operation != connector.OpInsert && record.Operation != connector.OpUpdate && record.Operation != connector.OpDelete {
		return streamChangelogRow{}, 0, fmt.Errorf("unsupported managed streaming Snowflake operation %q", record.Operation)
	}
	key, err := managedRecordKey(schema, keyColumns, record.Key)
	if err != nil {
		return streamChangelogRow{}, 0, err
	}
	canonicalKey := make(map[string]any, len(key))
	for name, value := range key {
		canonical, err := canonicalStagedValue(value)
		if err != nil {
			return streamChangelogRow{}, 0, fmt.Errorf("canonicalize streaming key column %q: %w", name, err)
		}
		canonicalKey[name] = canonical
	}
	unchanged, err := streamUnchangedToast(schema, keyColumns, record)
	if err != nil {
		return streamChangelogRow{}, 0, err
	}
	beforeImage, err := streamValidatedImage(cfg, schema, record.Before)
	if err != nil {
		return streamChangelogRow{}, 0, fmt.Errorf("streaming before image: %w", err)
	}
	afterImage, err := streamValidatedImage(cfg, schema, record.After)
	if err != nil {
		return streamChangelogRow{}, 0, fmt.Errorf("streaming after image: %w", err)
	}
	if record.Operation == connector.OpInsert {
		if len(unchanged) != 0 {
			return streamChangelogRow{}, 0, errors.New("managed streaming Snowflake insert must not carry unchanged-TOAST columns")
		}
		if len(afterImage) != len(schema.Columns) {
			return streamChangelogRow{}, 0, errors.New("managed streaming Snowflake insert requires one value for every admitted source column")
		}
	}
	if record.Operation == connector.OpDelete {
		if afterImage != nil {
			return streamChangelogRow{}, 0, errors.New("managed streaming Snowflake delete must not carry an after image")
		}
		if len(unchanged) != 0 {
			return streamChangelogRow{}, 0, errors.New("managed streaming Snowflake delete must not carry unchanged-TOAST columns")
		}
	}
	eventTime := transaction.Checkpoint.Timestamp.UTC()
	if !record.Timestamp.IsZero() {
		eventTime = record.Timestamp.UTC()
	}
	row := streamChangelogRow{
		FlowID: intent.FlowID, FlowIncarnationID: intent.FlowIncarnationID, SourceLineageID: intent.SourceLineageID,
		DestinationRevisionID: intent.DestinationRevisionID, LogicalBatchID: intent.LogicalBatchID, ContentHash: intent.ContentHash,
		OffsetToken: offsetToken, AppendOrdinal: appendOrdinal, SourcePosition: intent.PositionID,
		TransactionID: uint64(transaction.TransactionID), BeginLSN: transaction.BeginLSN, CommitLSN: transaction.CommitLSN,
		EndLSN: transaction.EndLSN, FragmentOrdinal: fragment.Ordinal, RecordOrdinal: recordOrdinal,
		SourceNamespace: schema.Namespace, SourceTable: schema.Name, SchemaContractHash: cfg.schemaContractHash,
		Operation: string(record.Operation), Tombstone: record.Operation == connector.OpDelete,
		KeyJSON: canonicalKey, BeforeImage: beforeImage, AfterImage: afterImage, UnchangedToast: unchanged,
		EventTime: eventTime.Truncate(time.Microsecond).Format(time.RFC3339Nano),
	}
	hash, err := streamRecordHash(row)
	if err != nil {
		return streamChangelogRow{}, 0, err
	}
	row.RowHash = hash
	encoded, err := json.Marshal(row)
	if err != nil {
		return streamChangelogRow{}, 0, fmt.Errorf("size streaming append row: %w", err)
	}
	row.payload = encoded
	return row, int64(len(encoded)) + 1, nil
}

// streamUnchangedToast returns the sorted, admitted set of unchanged-TOAST
// columns for one update record. It fails closed when an unchanged column is a
// key column, is outside the admitted schema, or also carries a value in the
// after image: an unchanged-TOAST column must be represented by its absence, not
// by an ambiguous placeholder, so a replay under toast_fetch=off reconstructs
// the identical partial after image and the identical row identity.
func streamUnchangedToast(schema connector.Schema, keyColumns []string, record connector.Record) ([]string, error) {
	if len(record.Unchanged) == 0 {
		return nil, nil
	}
	if record.Operation != connector.OpUpdate {
		return nil, fmt.Errorf("managed streaming Snowflake %q record must not declare unchanged-TOAST columns", record.Operation)
	}
	columns := make(map[string]struct{}, len(schema.Columns))
	for _, column := range schema.Columns {
		columns[column.Name] = struct{}{}
	}
	keys := make(map[string]struct{}, len(keyColumns))
	for _, column := range keyColumns {
		keys[column] = struct{}{}
	}
	seen := make(map[string]struct{}, len(record.Unchanged))
	result := make([]string, 0, len(record.Unchanged))
	for _, column := range record.Unchanged {
		if _, ok := columns[column]; !ok {
			return nil, fmt.Errorf("managed streaming Snowflake unchanged-TOAST column %q is outside the admitted schema", column)
		}
		if _, ok := keys[column]; ok {
			return nil, fmt.Errorf("managed streaming Snowflake unchanged-TOAST column %q must not be a key column", column)
		}
		if record.After != nil {
			if _, present := record.After[column]; present {
				return nil, fmt.Errorf("managed streaming Snowflake unchanged-TOAST column %q must be absent from the after image", column)
			}
		}
		if _, duplicate := seen[column]; duplicate {
			return nil, fmt.Errorf("managed streaming Snowflake repeats unchanged-TOAST column %q", column)
		}
		seen[column] = struct{}{}
		result = append(result, column)
	}
	sort.Strings(result)
	return result, nil
}

// streamValidatedImage normalizes a source row image into stable JSON while
// proving every column is inside the admitted schema and carries an admitted
// primitive type. It returns nil when the image itself is nil.
func streamValidatedImage(cfg streamConfig, schema connector.Schema, image map[string]any) (map[string]any, error) {
	if image == nil {
		return nil, nil //nolint:nilnil // a nil image maps to a JSON null column, not an error.
	}
	result := make(map[string]any, len(image))
	for _, column := range schema.Columns {
		value, present := image[column.Name]
		if !present {
			continue
		}
		if !streamSourceColumnSupported(cfg.typeMappings, column) {
			return nil, fmt.Errorf("column %q type %q is outside the admitted streaming cell", column.Name, column.Type)
		}
		normalized, err := normalizeManagedSnowflakeColumnValue(column, value, false)
		if err != nil {
			return nil, fmt.Errorf("normalize column %q: %w", column.Name, err)
		}
		canonical, err := canonicalStagedValue(normalized)
		if err != nil {
			return nil, fmt.Errorf("canonicalize column %q: %w", column.Name, err)
		}
		result[column.Name] = canonical
	}
	if len(result) != len(image) {
		return nil, errors.New("managed streaming Snowflake image contains a column outside the admitted schema")
	}
	return result, nil
}
