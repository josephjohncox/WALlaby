package snowflake

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// stagedCopyPlan is the deterministic, fail-closed COPY specification for one
// staged object. There is no lossy ON_ERROR continuation: any file, row, or
// column error aborts the statement so a partial load can never be mistaken for
// a complete one.
type stagedCopyPlan struct {
	target        string
	stageRef      string
	fileFormatRef string
	relativePath  string
	columns       []string
	loadOptions   map[string]string
	formatOptions map[string]string
}

// managedStagedReceipt is the durable destination proof that one immutable stage
// object was fully loaded. It carries both the logical delivery identity and the
// physical stage-object identity so reconciliation binds them together.
type managedStagedReceipt struct {
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
	generation            int64
	acquisitionID         string
	leaseEpoch            int64
	transactionID         uint32
	fragmentCount         int
	recordCount           int
	stageName             string
	stagePath             string
	fileContentHash       string
	fileMD5               string
	loadRowCount          int
	loadStatus            string
}

const (
	stagedReceiptKindLoad    = "load"
	stagedReceiptKindRelease = "release"
	stagedLoadStatusLoaded   = "LOADED"
	stagedLoadStatusReleased = "RELEASED"
)

type managedStagedPlan struct {
	identity           managedStagedIdentity
	copyPlan           stagedCopyPlan
	fileBytes          []byte
	fileContentHash    string
	fileMD5            string
	rowCount           int
	encodedBytes       int64
	catalogFingerprint string
	receipt            managedStagedReceipt
}

func planManagedStagedTransaction(cfg stagedConfig, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (managedStagedPlan, error) {
	if err := intent.Validate(); err != nil {
		return managedStagedPlan{}, err
	}
	if err := validateManagedSnowflakeIntentBounds(intent); err != nil {
		return managedStagedPlan{}, err
	}
	if intent.FlowID != cfg.flowID {
		return managedStagedPlan{}, fmt.Errorf("%w: delivery flow %q differs from admitted staged Snowflake flow %q", connector.ErrDeliveryConflict, intent.FlowID, cfg.flowID)
	}
	if intent.DestinationRevisionID != cfg.destinationRevision {
		return managedStagedPlan{}, fmt.Errorf("%w: delivery destination revision %q differs from admitted staged Snowflake revision %q", connector.ErrDeliveryConflict, intent.DestinationRevisionID, cfg.destinationRevision)
	}
	if err := transaction.Validate(); err != nil {
		return managedStagedPlan{}, err
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		return managedStagedPlan{}, fmt.Errorf("identify staged Snowflake checkpoint: %w", err)
	}
	if positionID != intent.PositionID {
		return managedStagedPlan{}, fmt.Errorf("%w: staged Snowflake transaction checkpoint position %q differs from delivery intent %q", connector.ErrDeliveryConflict, positionID, intent.PositionID)
	}
	if cfg.maxTransactionRows <= 0 || cfg.maxTransactionBytes <= 0 || cfg.maxFragments <= 0 {
		return managedStagedPlan{}, errors.New("staged Snowflake transaction bounds must be positive")
	}
	if len(transaction.Fragments) > cfg.maxFragments {
		return managedStagedPlan{}, fmt.Errorf("staged Snowflake transaction has %d fragments, maximum is %d", len(transaction.Fragments), cfg.maxFragments)
	}
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return managedStagedPlan{}, fmt.Errorf("identify staged Snowflake transaction: %w", err)
	}
	if contentHash != intent.ContentHash || logicalBatchID != intent.LogicalBatchID || transaction.SourceLineageID != intent.SourceLineageID {
		return managedStagedPlan{}, fmt.Errorf("%w: staged Snowflake transaction identity differs from delivery intent", connector.ErrDeliveryConflict)
	}
	contractHash, err := ManagedSchemaContractHash(cfg.schemaContract)
	if err != nil {
		return managedStagedPlan{}, err
	}
	if contractHash != cfg.schemaContractHash {
		return managedStagedPlan{}, fmt.Errorf("%w: staged Snowflake configured schema contract hash differs", connector.ErrDeliveryConflict)
	}
	keyColumns, err := managedIdentityColumns(cfg.schemaContract)
	if err != nil {
		return managedStagedPlan{}, err
	}

	rows := make([]stagedChangelogRow, 0)
	encodedBytes := int64(0)
	for _, fragment := range transaction.Fragments {
		controlFragment := len(fragment.Batch.Records) > 0 && fragment.Batch.Records[0].Operation == connector.OpDDL
		if !controlFragment {
			if err := validateManagedRuntimeSchema(cfg.schemaContract, fragment.Batch.Schema); err != nil {
				return managedStagedPlan{}, fmt.Errorf("validate staged Snowflake fragment %d schema: %w", fragment.Ordinal, err)
			}
		}
		for recordIndex, record := range fragment.Batch.Records {
			// #nosec G115 -- recordIndex is a non-negative slice index and each record contributes to the bounded maxTransactionRows maximum enforced below.
			boundedRecordIndex := uint64(recordIndex)
			row, size, err := buildStagedChangelogRow(cfg, intent, transaction, fragment, keyColumns, boundedRecordIndex, record)
			if err != nil {
				return managedStagedPlan{}, fmt.Errorf("plan staged Snowflake fragment %d record %d: %w", fragment.Ordinal, recordIndex, err)
			}
			rows = append(rows, row)
			if len(rows) > cfg.maxTransactionRows {
				return managedStagedPlan{}, fmt.Errorf("staged Snowflake transaction has more than %d records", cfg.maxTransactionRows)
			}
			encodedBytes += size
			if encodedBytes > cfg.maxTransactionBytes {
				return managedStagedPlan{}, fmt.Errorf("staged Snowflake transaction exceeds %d encoded bytes", cfg.maxTransactionBytes)
			}
		}
	}

	fileBytes, fileContentHash, fileMD5, err := serializeStagedFile(rows)
	if err != nil {
		return managedStagedPlan{}, err
	}
	if int64(len(fileBytes)) > cfg.maxTransactionBytes {
		return managedStagedPlan{}, fmt.Errorf("staged Snowflake object is %d bytes, exceeding admitted %d", len(fileBytes), cfg.maxTransactionBytes)
	}

	copyPlan, err := newStagedCopyPlan(cfg)
	if err != nil {
		return managedStagedPlan{}, err
	}
	planHash := stagedPlanHash(copyPlan)
	identity, err := newManagedStagedIdentity(cfg, intent, planHash, intent.ContentHash)
	if err != nil {
		return managedStagedPlan{}, err
	}
	copyPlan.relativePath = identity.relativePath

	receipt := managedStagedReceipt{
		kind: stagedReceiptKindLoad, profileVersion: cfg.profile, flowID: intent.FlowID, flowIncarnationID: intent.FlowIncarnationID,
		sourceLineageID: intent.SourceLineageID, destinationRevisionID: intent.DestinationRevisionID,
		logicalBatchID: intent.LogicalBatchID, positionID: intent.PositionID, contentHash: intent.ContentHash,
		schemaContractHash: cfg.schemaContractHash, manifestHash: identity.manifestHash, externalID: identity.externalID,
		generation: intent.Generation, acquisitionID: intent.AcquisitionID, leaseEpoch: intent.LeaseEpoch,
		transactionID: transaction.TransactionID, fragmentCount: len(transaction.Fragments), recordCount: len(rows),
		stageName: cfg.stage, stagePath: identity.relativePath, fileContentHash: fileContentHash, fileMD5: fileMD5,
		loadRowCount: len(rows), loadStatus: stagedLoadStatusLoaded,
	}
	return managedStagedPlan{
		identity: identity, copyPlan: copyPlan, fileBytes: fileBytes, fileContentHash: fileContentHash,
		fileMD5: fileMD5, rowCount: len(rows), encodedBytes: encodedBytes, receipt: receipt,
	}, nil
}

// stagedInlineJSONFormatOptions renders the admitted JSON file-format behavior
// directly into every COPY. A named FORMAT_NAME reference would let a concurrent
// ALTER FILE FORMAT change parsing between admission and load; inlining removes
// that window entirely instead of trying to observe it. The values are derived
// from the same admitted property table the catalog validator enforces, so the
// named object and the inline options cannot silently diverge. FILE_EXTENSION is
// excluded because it only affects unloading and cannot change how this exact
// object is parsed.
func stagedInlineJSONFormatOptions() (map[string]string, error) {
	options := map[string]string{"MULTI_LINE": "FALSE"}
	for name, property := range managedStagedJSONFileFormatProperties() {
		if name == "FILE_EXTENSION" {
			continue
		}
		value, err := stagedInlineFormatValue(name, property)
		if err != nil {
			return nil, err
		}
		options[name] = value
	}
	for name, value := range options {
		if !stagedInlineFormatTokenPattern.MatchString(name) || !stagedInlineFormatTokenPattern.MatchString(value) {
			return nil, fmt.Errorf("staged Snowflake inline JSON option %q=%q is not a bare renderable token", name, value)
		}
	}
	return options, nil
}

// stagedInlineFormatTokenPattern keeps every rendered option name and value a
// bare keyword token, so the COPY statement can never be reshaped by a value.
var stagedInlineFormatTokenPattern = regexp.MustCompile(`^[A-Z0-9_()]+$`)

// stagedInlineFormatValue converts one DESCRIBE-shaped property into its COPY
// literal using the property's declared type, so a future property with an
// unconvertible shape fails in unit tests rather than in a production load.
func stagedInlineFormatValue(name string, property managedFileFormatPropertySnapshot) (string, error) {
	switch property.propertyType {
	case "Boolean":
		if property.propertyValue != "TRUE" && property.propertyValue != "FALSE" {
			return "", fmt.Errorf("staged Snowflake JSON option %s is Boolean but has value %q", name, property.propertyValue)
		}
		return property.propertyValue, nil
	case "String":
		if property.propertyValue == "" {
			return "", fmt.Errorf("staged Snowflake JSON option %s has no renderable String value", name)
		}
		return property.propertyValue, nil
	case "List":
		if property.propertyValue != "[]" {
			return "", fmt.Errorf("staged Snowflake JSON option %s admits only the empty list, got %q", name, property.propertyValue)
		}
		return "()", nil
	default:
		return "", fmt.Errorf("staged Snowflake JSON option %s has unsupported property type %q", name, property.propertyType)
	}
}

func newStagedCopyPlan(cfg stagedConfig) (stagedCopyPlan, error) {
	formatOptions, err := stagedInlineJSONFormatOptions()
	if err != nil {
		return stagedCopyPlan{}, err
	}
	return stagedCopyPlan{
		target:        managedSnowflakeStagedQualified(cfg, cfg.table),
		stageRef:      managedSnowflakeStagedQualified(cfg, cfg.stage),
		fileFormatRef: managedSnowflakeStagedQualified(cfg, cfg.fileFormat),
		columns:       stagedChangelogColumns(),
		formatOptions: formatOptions,
		loadOptions: map[string]string{
			"ON_ERROR":             "ABORT_STATEMENT",
			"FORCE":                "FALSE",
			"PURGE":                "FALSE",
			"MATCH_BY_COLUMN_NAME": "CASE_SENSITIVE",
		},
	}, nil
}

func buildStagedChangelogRow(cfg stagedConfig, intent connector.DeliveryIntent, transaction connector.SourceTransaction, fragment connector.TransactionFragment, keyColumns []string, recordOrdinal uint64, record connector.Record) (stagedChangelogRow, int64, error) {
	schema := fragment.Batch.Schema
	table := record.Table
	if table == "" {
		table = schema.Name
	}
	if schema.Namespace != cfg.sourceSchema || schema.Name != cfg.sourceTable || table != cfg.sourceTable {
		return stagedChangelogRow{}, 0, fmt.Errorf("source relation %s.%s/%s is outside admitted relation %s.%s", schema.Namespace, schema.Name, table, cfg.sourceSchema, cfg.sourceTable)
	}
	if record.Operation == connector.OpDDL {
		return stagedChangelogRow{}, 0, fmt.Errorf("%w: managed staged Snowflake append rejects all DDL until live crash recovery evidence exists", errManagedSnowflakeSchemaNotReconciled)
	}
	if record.Operation != connector.OpInsert && record.Operation != connector.OpUpdate && record.Operation != connector.OpDelete {
		return stagedChangelogRow{}, 0, fmt.Errorf("unsupported managed staged Snowflake operation %q", record.Operation)
	}
	key, err := managedRecordKey(schema, keyColumns, record.Key)
	if err != nil {
		return stagedChangelogRow{}, 0, err
	}
	canonicalKey := make(map[string]any, len(key))
	for name, value := range key {
		canonical, err := canonicalStagedValue(value)
		if err != nil {
			return stagedChangelogRow{}, 0, fmt.Errorf("canonicalize staged key column %q: %w", name, err)
		}
		canonicalKey[name] = canonical
	}
	beforeImage, err := stagedValidatedImage(cfg, schema, record.Before)
	if err != nil {
		return stagedChangelogRow{}, 0, fmt.Errorf("staged before image: %w", err)
	}
	afterImage, err := stagedValidatedImage(cfg, schema, record.After)
	if err != nil {
		return stagedChangelogRow{}, 0, fmt.Errorf("staged after image: %w", err)
	}
	if record.Operation == connector.OpInsert {
		if len(afterImage) != len(schema.Columns) {
			return stagedChangelogRow{}, 0, errors.New("managed staged Snowflake insert requires one value for every admitted source column")
		}
	}
	if record.Operation == connector.OpDelete && afterImage != nil {
		return stagedChangelogRow{}, 0, errors.New("managed staged Snowflake delete must not carry an after image")
	}
	eventTime := transaction.Checkpoint.Timestamp.UTC()
	if !record.Timestamp.IsZero() {
		eventTime = record.Timestamp.UTC()
	}
	row := stagedChangelogRow{
		FlowID: intent.FlowID, FlowIncarnationID: intent.FlowIncarnationID, SourceLineageID: intent.SourceLineageID,
		DestinationRevisionID: intent.DestinationRevisionID, LogicalBatchID: intent.LogicalBatchID, ContentHash: intent.ContentHash,
		SourcePosition: intent.PositionID, TransactionID: uint64(transaction.TransactionID), BeginLSN: transaction.BeginLSN,
		CommitLSN: transaction.CommitLSN, EndLSN: transaction.EndLSN, FragmentOrdinal: fragment.Ordinal, RecordOrdinal: recordOrdinal,
		SourceNamespace: schema.Namespace, SourceTable: schema.Name, SchemaContractHash: cfg.schemaContractHash,
		Operation: string(record.Operation), Tombstone: record.Operation == connector.OpDelete,
		KeyJSON: canonicalKey, BeforeImage: beforeImage, AfterImage: afterImage,
		EventTime: eventTime.Truncate(time.Microsecond).Format(time.RFC3339Nano),
	}
	hash, err := stagedRecordHash(row)
	if err != nil {
		return stagedChangelogRow{}, 0, err
	}
	row.RecordHash = hash
	encoded, err := json.Marshal(row)
	if err != nil {
		return stagedChangelogRow{}, 0, fmt.Errorf("size staged changelog row: %w", err)
	}
	return row, int64(len(encoded)) + 1, nil
}

// stagedValidatedImage normalizes a source row image into stable JSON while
// proving every column is inside the admitted schema and carries an admitted
// primitive type. It returns nil when the image itself is nil.
func stagedValidatedImage(cfg stagedConfig, schema connector.Schema, image map[string]any) (map[string]any, error) {
	if image == nil {
		return nil, nil //nolint:nilnil // a nil image maps to a JSON null column, not an error.
	}
	result := make(map[string]any, len(image))
	for _, column := range schema.Columns {
		value, present := image[column.Name]
		if !present {
			continue
		}
		if !stagedSourceColumnSupported(cfg.typeMappings, column) {
			return nil, fmt.Errorf("column %q type %q is outside the admitted staged cell", column.Name, column.Type)
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
		return nil, errors.New("managed staged Snowflake image contains a column outside the admitted schema")
	}
	return result, nil
}

func managedSnowflakeStagedQualified(cfg stagedConfig, object string) string {
	return strings.Join([]string{quoteIdent(cfg.database, '"'), quoteIdent(cfg.schema, '"'), quoteIdent(object, '"')}, ".")
}
