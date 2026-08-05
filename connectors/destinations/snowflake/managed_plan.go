package snowflake

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

var errManagedSnowflakeSchemaNotReconciled = errors.New("managed Snowflake schema is not reconciled")

type managedConfig struct {
	profile                  string
	flowID                   string
	account                  string
	database                 string
	schema                   string
	table                    string
	receiptsTable            string
	ownerRole                string
	executionRole            string
	warehouse                string
	snowflakeVersion         string
	targetCreatedOn          string
	receiptsCreatedOn        string
	sourceSchema             string
	sourceTable              string
	schemaContract           connector.Schema
	schemaContractHash       string
	destinationRevision      string
	maxTransactionRows       int
	maxTransactionBytes      int64
	maxFragments             int
	maxOpenConnections       int
	statementTimeoutSeconds  int
	hybridLockTimeoutSeconds int
	validateEveryConnection  bool
	typeMappings             map[string]string
}

type managedSnowflakePlan struct {
	operations         []managedSnowflakeOperation
	receipt            managedSnowflakeReceipt
	catalogFingerprint string
	recordCount        int
	encodedBytes       int64
}

type managedOperationIdentity struct {
	fragmentOrdinal uint64
	recordOrdinal   uint64
	operation       connector.Operation
}

type managedSnowflakeOperation struct {
	identity managedOperationIdentity
	query    string
	args     []any
	bytes    int64
}

type managedSnowflakeReceipt struct {
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
}

type managedSchemaContract struct {
	Namespace string                `json:"namespace"`
	Name      string                `json:"name"`
	Columns   []managedSchemaColumn `json:"columns"`
}

type managedSchemaColumn struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	Nullable          bool   `json:"nullable"`
	Generated         bool   `json:"generated"`
	Expression        string `json:"expression,omitempty"`
	PrimaryKey        bool   `json:"primary_key"`
	PrimaryKeyOrdinal int    `json:"primary_key_ordinal,omitempty"`
	ReplicaIdentity   bool   `json:"replica_identity"`
}

// ManagedSchemaContractHash returns the immutable source-schema identity used
// by the constrained Snowflake SQL profile. Decoder-local schema versions and
// PostgreSQL catalog OIDs deliberately do not participate.
func ManagedSchemaContractHash(schema connector.Schema) (string, error) {
	contract, err := canonicalManagedSchemaContract(schema)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(contract)
	if err != nil {
		return "", fmt.Errorf("encode managed Snowflake schema contract: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalManagedSchemaContract(schema connector.Schema) (managedSchemaContract, error) {
	contract := managedSchemaContract{
		Namespace: schema.Namespace,
		Name:      schema.Name,
		Columns:   make([]managedSchemaColumn, 0, len(schema.Columns)),
	}
	if contract.Namespace == "" || contract.Name == "" || len(schema.Columns) == 0 {
		return managedSchemaContract{}, errors.New("managed Snowflake schema contract requires namespace, table, and columns")
	}
	seen := make(map[string]struct{}, len(schema.Columns))
	primaryCount := 0
	for _, column := range schema.Columns {
		if column.TypeMetadata["primary_key"] == "true" {
			primaryCount++
		}
	}
	seenPrimaryOrdinals := make(map[int]struct{}, primaryCount)
	for _, column := range schema.Columns {
		name := column.Name
		if name == "" {
			return managedSchemaContract{}, errors.New("managed Snowflake schema contract contains an unnamed column")
		}
		targetName := strings.ToUpper(name)
		if err := validateManagedSnowflakeUnquotedIdentifier("source column target", targetName); err != nil {
			return managedSchemaContract{}, err
		}
		if _, duplicate := seen[targetName]; duplicate {
			return managedSchemaContract{}, fmt.Errorf("managed Snowflake schema contract repeats unquoted target column %q", targetName)
		}
		seen[targetName] = struct{}{}
		primaryKey := column.TypeMetadata["primary_key"] == "true"
		primaryKeyOrdinal := 0
		if primaryKey && primaryCount > 1 {
			primaryKeyOrdinal, _ = strconv.Atoi(strings.TrimSpace(column.TypeMetadata["primary_key_ordinal"]))
			if primaryKeyOrdinal < 1 || primaryKeyOrdinal > primaryCount {
				return managedSchemaContract{}, fmt.Errorf("managed Snowflake composite primary-key column %q requires primary_key_ordinal between 1 and %d", name, primaryCount)
			}
			if _, duplicate := seenPrimaryOrdinals[primaryKeyOrdinal]; duplicate {
				return managedSchemaContract{}, fmt.Errorf("managed Snowflake composite primary key repeats ordinal %d", primaryKeyOrdinal)
			}
			seenPrimaryOrdinals[primaryKeyOrdinal] = struct{}{}
		}
		contract.Columns = append(contract.Columns, managedSchemaColumn{
			Name: name, Type: normalizeManagedSourceType(column.Type), Nullable: column.Nullable,
			Generated: column.Generated, Expression: strings.TrimSpace(column.Expression),
			PrimaryKey: primaryKey, PrimaryKeyOrdinal: primaryKeyOrdinal,
			ReplicaIdentity: primaryKey || column.TypeMetadata["replica_identity"] == "true",
		})
	}
	return contract, nil
}

func planManagedSnowflakeTransaction(cfg managedConfig, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (managedSnowflakePlan, error) {
	if err := intent.Validate(); err != nil {
		return managedSnowflakePlan{}, err
	}
	if err := validateManagedSnowflakeIntentBounds(intent); err != nil {
		return managedSnowflakePlan{}, err
	}
	if intent.FlowID != cfg.flowID {
		return managedSnowflakePlan{}, fmt.Errorf("%w: delivery flow %q differs from admitted Snowflake flow %q", connector.ErrDeliveryConflict, intent.FlowID, cfg.flowID)
	}
	if intent.DestinationRevisionID != cfg.destinationRevision {
		return managedSnowflakePlan{}, fmt.Errorf("%w: delivery destination revision %q differs from admitted Snowflake revision %q", connector.ErrDeliveryConflict, intent.DestinationRevisionID, cfg.destinationRevision)
	}
	if err := transaction.Validate(); err != nil {
		return managedSnowflakePlan{}, err
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		return managedSnowflakePlan{}, fmt.Errorf("identify managed Snowflake checkpoint: %w", err)
	}
	if positionID != intent.PositionID {
		return managedSnowflakePlan{}, fmt.Errorf("%w: managed Snowflake transaction checkpoint position %q differs from delivery intent %q", connector.ErrDeliveryConflict, positionID, intent.PositionID)
	}
	if cfg.maxTransactionRows <= 0 || cfg.maxTransactionBytes <= 0 || cfg.maxFragments <= 0 {
		return managedSnowflakePlan{}, errors.New("managed Snowflake transaction bounds must be positive")
	}
	if len(transaction.Fragments) > cfg.maxFragments {
		return managedSnowflakePlan{}, fmt.Errorf("managed Snowflake transaction has %d fragments, maximum is %d", len(transaction.Fragments), cfg.maxFragments)
	}
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return managedSnowflakePlan{}, fmt.Errorf("identify managed Snowflake transaction: %w", err)
	}
	if contentHash != intent.ContentHash || logicalBatchID != intent.LogicalBatchID || transaction.SourceLineageID != intent.SourceLineageID {
		return managedSnowflakePlan{}, fmt.Errorf("%w: managed Snowflake transaction identity differs from delivery intent", connector.ErrDeliveryConflict)
	}

	contractHash, err := ManagedSchemaContractHash(cfg.schemaContract)
	if err != nil {
		return managedSnowflakePlan{}, err
	}
	if contractHash != cfg.schemaContractHash {
		return managedSnowflakePlan{}, fmt.Errorf("%w: managed Snowflake configured schema contract hash differs", connector.ErrDeliveryConflict)
	}
	keyColumns, err := managedIdentityColumns(cfg.schemaContract)
	if err != nil {
		return managedSnowflakePlan{}, err
	}
	target := quoteIdent(cfg.database, '"') + "." + quoteIdent(cfg.schema, '"') + "." + quoteIdent(cfg.table, '"')
	plan := managedSnowflakePlan{operations: make([]managedSnowflakeOperation, 0)}
	for _, fragment := range transaction.Fragments {
		controlFragment := len(fragment.Batch.Records) > 0 && fragment.Batch.Records[0].Operation == connector.OpDDL
		if !controlFragment {
			if err := validateManagedRuntimeSchema(cfg.schemaContract, fragment.Batch.Schema); err != nil {
				return managedSnowflakePlan{}, fmt.Errorf("validate managed Snowflake fragment %d schema: %w", fragment.Ordinal, err)
			}
		}
		for recordIndex, record := range fragment.Batch.Records {
			identity := managedOperationIdentity{fragmentOrdinal: fragment.Ordinal, recordOrdinal: uint64(recordIndex), operation: record.Operation} // #nosec G115 -- transaction rows are explicitly bounded.
			operation, err := buildManagedSnowflakeOperation(cfg, target, fragment.Batch.Schema, keyColumns, identity, record)
			if err != nil {
				return managedSnowflakePlan{}, fmt.Errorf("plan managed Snowflake fragment %d record %d: %w", fragment.Ordinal, recordIndex, err)
			}
			plan.recordCount++
			if plan.recordCount > cfg.maxTransactionRows {
				return managedSnowflakePlan{}, fmt.Errorf("managed Snowflake transaction has more than %d records", cfg.maxTransactionRows)
			}
			plan.encodedBytes += operation.bytes
			if plan.encodedBytes > cfg.maxTransactionBytes {
				return managedSnowflakePlan{}, fmt.Errorf("managed Snowflake transaction exceeds %d encoded bytes", cfg.maxTransactionBytes)
			}
			plan.operations = append(plan.operations, operation)
		}
	}
	manifestHash := managedDestinationManifestHash(cfg, intent)
	plan.receipt = managedSnowflakeReceipt{
		profileVersion: cfg.profile, flowID: intent.FlowID, flowIncarnationID: intent.FlowIncarnationID,
		sourceLineageID: intent.SourceLineageID, destinationRevisionID: intent.DestinationRevisionID,
		logicalBatchID: intent.LogicalBatchID, positionID: intent.PositionID, contentHash: intent.ContentHash,
		schemaContractHash: cfg.schemaContractHash, manifestHash: manifestHash, externalID: "sf-marker:v1:" + manifestHash,
		generation: intent.Generation, acquisitionID: intent.AcquisitionID, leaseEpoch: intent.LeaseEpoch,
		transactionID: transaction.TransactionID, fragmentCount: len(transaction.Fragments), recordCount: plan.recordCount,
	}
	return plan, nil
}

func validateManagedSnowflakeIntentBounds(intent connector.DeliveryIntent) error {
	for name, value := range map[string]string{
		"flow_id": intent.FlowID, "flow_incarnation_id": intent.FlowIncarnationID,
		"source_lineage_id": intent.SourceLineageID, "acquisition_id": intent.AcquisitionID,
		"logical_batch_id": intent.LogicalBatchID, "position_id": intent.PositionID, "content_hash": intent.ContentHash,
	} {
		if strings.TrimSpace(value) == "" || len(value) > 1024 {
			return fmt.Errorf("managed Snowflake delivery %s must contain 1-1024 bytes", name)
		}
	}
	return nil
}

func buildManagedSnowflakeOperation(cfg managedConfig, target string, schema connector.Schema, keyColumns []string, identity managedOperationIdentity, record connector.Record) (managedSnowflakeOperation, error) {
	table := record.Table
	if table == "" || strings.ContainsRune(table, '\x00') || schema.Namespace == "" || strings.ContainsRune(schema.Namespace, '\x00') || schema.Name == "" || strings.ContainsRune(schema.Name, '\x00') {
		return managedSnowflakeOperation{}, errors.New("managed Snowflake operation requires exact nonempty NUL-free projected schema and table identifiers")
	}
	if schema.Namespace != cfg.schemaContract.Namespace || schema.Name != cfg.schemaContract.Name || table != cfg.schemaContract.Name {
		return managedSnowflakeOperation{}, fmt.Errorf("projected relation %s.%s/%s is outside admitted destination contract %s.%s", schema.Namespace, schema.Name, table, cfg.schemaContract.Namespace, cfg.schemaContract.Name)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return managedSnowflakeOperation{}, fmt.Errorf("size managed Snowflake record: %w", err)
	}
	operation := managedSnowflakeOperation{identity: identity, bytes: int64(len(encoded))}
	if record.Operation == connector.OpDDL {
		return managedSnowflakeOperation{}, fmt.Errorf("%w: managed Snowflake SQL v1 rejects all DDL until live crash recovery evidence exists", errManagedSnowflakeSchemaNotReconciled)
	}
	if record.Operation != connector.OpInsert && record.Operation != connector.OpUpdate && record.Operation != connector.OpDelete {
		return managedSnowflakeOperation{}, fmt.Errorf("unsupported managed Snowflake operation %q", record.Operation)
	}
	key, err := managedRecordKey(schema, keyColumns, record.Key)
	if err != nil {
		return managedSnowflakeOperation{}, err
	}
	where, whereArgs := managedWhereFromIdentity(keyColumns, key)
	switch record.Operation {
	case connector.OpInsert:
		columns, values, expressions, err := managedRecordColumns(cfg, schema, record)
		if err != nil {
			return managedSnowflakeOperation{}, err
		}
		if len(columns) != len(schema.Columns) {
			return managedSnowflakeOperation{}, errors.New("managed Snowflake insert requires one value for every admitted source column")
		}
		for _, keyColumn := range keyColumns {
			value, present := record.After[keyColumn]
			normalized, normalizeErr := normalizeManagedSnowflakeColumnValue(managedSchemaColumnByName(schema, keyColumn), value, false)
			if !present || normalizeErr != nil || !reflect.DeepEqual(normalized, key[keyColumn]) {
				return managedSnowflakeOperation{}, fmt.Errorf("managed Snowflake insert identity column %q differs between key and after image", keyColumn)
			}
		}
		operation.query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", target, quoteColumns(columns), strings.Join(expressions, ", "))
		operation.args = values
	case connector.OpUpdate:
		columns, values, expressions, err := managedRecordColumns(cfg, schema, record)
		if err != nil {
			return managedSnowflakeOperation{}, err
		}
		if len(columns) == 0 {
			return managedSnowflakeOperation{}, errors.New("managed Snowflake update requires an after image")
		}
		for _, keyColumn := range keyColumns {
			if value, present := record.After[keyColumn]; present {
				normalized, normalizeErr := normalizeManagedSnowflakeColumnValue(managedSchemaColumnByName(schema, keyColumn), value, false)
				if normalizeErr != nil || !reflect.DeepEqual(normalized, key[keyColumn]) {
					return managedSnowflakeOperation{}, fmt.Errorf("managed Snowflake update changes immutable identity column %q", keyColumn)
				}
			}
		}
		assignments := make([]string, 0, len(columns))
		for index, column := range columns {
			assignments = append(assignments, fmt.Sprintf("%s = %s", quoteIdent(column, '"'), expressions[index]))
		}
		operation.query = fmt.Sprintf("UPDATE %s SET %s WHERE %s", target, strings.Join(assignments, ", "), where)
		operation.args = append(operation.args, values...)
		operation.args = append(operation.args, whereArgs...)
	case connector.OpDelete:
		operation.query = fmt.Sprintf("DELETE FROM %s WHERE %s", target, where)
		operation.args = whereArgs
	}
	return operation, nil
}

func managedRecordColumns(cfg managedConfig, schema connector.Schema, record connector.Record) ([]string, []any, []string, error) {
	if record.After == nil {
		return nil, nil, nil, nil
	}
	columns := make([]string, 0, len(record.After))
	values := make([]any, 0, len(record.After))
	expressions := make([]string, 0, len(record.After))
	for _, column := range schema.Columns {
		value, present := record.After[column.Name]
		if !present {
			continue
		}
		mappedType := managedSnowflakeColumnType(cfg, column)
		if mappedType == "" {
			return nil, nil, nil, fmt.Errorf("managed Snowflake has no type mapping for source column %s type %q", column.Name, column.Type)
		}
		normalized, err := normalizeManagedSnowflakeColumnValue(column, value, false)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("normalize managed Snowflake column %s: %w", column.Name, err)
		}
		expression := "?"
		if isSnowflakeJSONType(mappedType) {
			expression = "PARSE_JSON(?)"
		}
		columns = append(columns, strings.ToUpper(column.Name))
		values = append(values, immutableManagedSnowflakeValue(normalized))
		expressions = append(expressions, expression)
	}
	if len(columns) != len(record.After) {
		return nil, nil, nil, errors.New("managed Snowflake after image contains a column outside the admitted schema")
	}
	return columns, values, expressions, nil
}

func validateManagedRuntimeSchema(contract, runtime connector.Schema) error {
	if contract.Namespace != runtime.Namespace || contract.Name != runtime.Name || len(contract.Columns) != len(runtime.Columns) {
		return fmt.Errorf("%w: runtime relation shape differs from the configured contract", errManagedSnowflakeSchemaNotReconciled)
	}
	contractIdentity := make([]string, 0)
	runtimeIdentity := make([]string, 0)
	for index := range contract.Columns {
		expected := contract.Columns[index]
		actual := runtime.Columns[index]
		if expected.Name != actual.Name || normalizeManagedSourceType(expected.Type) != normalizeManagedSourceType(actual.Type) {
			return fmt.Errorf("%w: runtime column %d name or type differs from the configured contract", errManagedSnowflakeSchemaNotReconciled, index)
		}
		if actual.TypeMetadata["nullability_known"] == "true" && expected.Nullable != actual.Nullable {
			return fmt.Errorf("%w: runtime column %q nullability differs from the configured contract", errManagedSnowflakeSchemaNotReconciled, actual.Name)
		}
		if actual.TypeMetadata["generated_known"] == "true" && (expected.Generated != actual.Generated || expected.Expression != actual.Expression) {
			return fmt.Errorf("%w: runtime column %q generated contract differs", errManagedSnowflakeSchemaNotReconciled, actual.Name)
		}
		if expected.TypeMetadata["primary_key"] == "true" || expected.TypeMetadata["replica_identity"] == "true" {
			contractIdentity = append(contractIdentity, expected.Name)
		}
		if actual.TypeMetadata["primary_key"] == "true" || actual.TypeMetadata["replica_identity"] == "true" {
			runtimeIdentity = append(runtimeIdentity, actual.Name)
		}
	}
	if !reflect.DeepEqual(contractIdentity, runtimeIdentity) {
		return fmt.Errorf("%w: runtime replica identity %v differs from configured identity %v", errManagedSnowflakeSchemaNotReconciled, runtimeIdentity, contractIdentity)
	}
	return nil
}

func managedSchemaColumnByName(schema connector.Schema, name string) connector.Column {
	for _, column := range schema.Columns {
		if column.Name == name {
			return column
		}
	}
	return connector.Column{Name: name}
}

func normalizeManagedSnowflakeColumnValue(column connector.Column, value any, serializedKey bool) (any, error) {
	if value == nil {
		if !column.Nullable {
			return nil, errors.New("NULL is not allowed by the admitted source column")
		}
		return nil, nil //nolint:nilnil // nil maps to SQL NULL
	}
	base := normalizeManagedSourceType(column.Type)
	if index := strings.IndexByte(base, '('); index >= 0 {
		base = strings.TrimSpace(base[:index])
	}
	switch base {
	case "bigint":
		switch typed := value.(type) {
		case int:
			return int64(typed), nil
		case int8:
			return int64(typed), nil
		case int16:
			return int64(typed), nil
		case int32:
			return int64(typed), nil
		case int64:
			return typed, nil
		case uint:
			return normalizeManagedSnowflakeUint64(uint64(typed))
		case uint8:
			return int64(typed), nil
		case uint16:
			return int64(typed), nil
		case uint32:
			return int64(typed), nil
		case uint64:
			return normalizeManagedSnowflakeUint64(typed)
		case json.Number:
			parsed, err := typed.Int64()
			if err != nil {
				return nil, fmt.Errorf("invalid bigint %q: %w", typed, err)
			}
			return parsed, nil
		case string:
			if !serializedKey {
				return nil, fmt.Errorf("bigint value has type %T", value)
			}
			parsed, err := strconv.ParseInt(typed, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid bigint %q: %w", typed, err)
			}
			return parsed, nil
		default:
			return nil, fmt.Errorf("bigint value has type %T", value)
		}
	case "boolean":
		if typed, ok := value.(bool); ok {
			return typed, nil
		}
		return nil, fmt.Errorf("boolean value has type %T", value)
	case "bytea":
		if typed, ok := value.([]byte); ok {
			return append([]byte(nil), typed...), nil
		}
		if serializedKey {
			if typed, ok := value.(string); ok {
				decoded, err := base64.StdEncoding.DecodeString(typed)
				if err != nil {
					return nil, fmt.Errorf("invalid base64 bytea key: %w", err)
				}
				return decoded, nil
			}
		}
		return nil, fmt.Errorf("bytea value has type %T", value)
	case "numeric":
		var raw string
		switch typed := value.(type) {
		case json.Number:
			raw = typed.String()
		case string:
			raw = typed
		case int:
			raw = strconv.FormatInt(int64(typed), 10)
		case int8:
			raw = strconv.FormatInt(int64(typed), 10)
		case int16:
			raw = strconv.FormatInt(int64(typed), 10)
		case int32:
			raw = strconv.FormatInt(int64(typed), 10)
		case int64:
			raw = strconv.FormatInt(typed, 10)
		default:
			return nil, fmt.Errorf("numeric value has type %T", value)
		}
		if err := validateManagedSnowflakeNumericValue(column.Type, raw); err != nil {
			return nil, err
		}
		return raw, nil
	case "text":
		typed, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("text value has type %T", value)
		}
		if !utf8.ValidString(typed) {
			return nil, errors.New("text value is not valid UTF-8")
		}
		return typed, nil
	case "timestamp with time zone":
		var instant time.Time
		switch typed := value.(type) {
		case time.Time:
			instant = typed
		case string:
			if !serializedKey {
				return nil, fmt.Errorf("timestamp with time zone value has type %T", value)
			}
			parsed, err := time.Parse(time.RFC3339Nano, typed)
			if err != nil {
				return nil, fmt.Errorf("invalid timestamp with time zone %q: %w", typed, err)
			}
			instant = parsed
		default:
			return nil, fmt.Errorf("timestamp with time zone value has type %T", value)
		}
		if instant.Year() < 1 || instant.Year() > 9999 {
			return nil, fmt.Errorf("timestamp year %d is outside admitted Snowflake range 1-9999", instant.Year())
		}
		return instant.Round(0).UTC().Truncate(time.Microsecond), nil
	default:
		return nil, fmt.Errorf("source type %q is outside the managed Snowflake value cell", column.Type)
	}
}

func normalizeManagedSnowflakeUint64(value uint64) (int64, error) {
	const maxInt64Uint = ^uint64(0) >> 1
	if value > maxInt64Uint {
		return 0, fmt.Errorf("bigint value %d exceeds int64", value)
	}
	return int64(value), nil // #nosec G115 -- value is bounded above.
}

func validateManagedSnowflakeNumericValue(sourceType, raw string) error {
	normalizedType := normalizeManagedSourceType(sourceType)
	start := strings.IndexByte(normalizedType, '(')
	if start < 0 || !strings.HasSuffix(normalizedType, ")") {
		return errors.New("managed Snowflake numeric requires explicit precision and scale")
	}
	parts := strings.Split(strings.TrimSuffix(normalizedType[start+1:], ")"), ",")
	if len(parts) != 2 {
		return errors.New("managed Snowflake numeric requires precision and scale")
	}
	precision, precisionErr := strconv.Atoi(strings.TrimSpace(parts[0]))
	scale, scaleErr := strconv.Atoi(strings.TrimSpace(parts[1]))
	if precisionErr != nil || scaleErr != nil || precision < 1 || precision > 38 || scale < 0 || scale > precision {
		return errors.New("managed Snowflake numeric precision or scale is outside the admitted range")
	}
	raw = strings.TrimPrefix(strings.TrimSpace(raw), "-")
	decimal := strings.Split(raw, ".")
	if len(decimal) > 2 || len(decimal[0]) == 0 {
		return fmt.Errorf("numeric value %q is not a finite plain decimal", raw)
	}
	for _, component := range decimal {
		for _, character := range component {
			if character < '0' || character > '9' {
				return fmt.Errorf("numeric value %q is not a finite plain decimal", raw)
			}
		}
	}
	integerDigits := len(strings.TrimLeft(decimal[0], "0"))
	fractionDigits := 0
	if len(decimal) == 2 {
		fractionDigits = len(decimal[1])
	}
	if integerDigits > precision-scale || fractionDigits > scale {
		return fmt.Errorf("numeric value exceeds admitted numeric(%d,%d)", precision, scale)
	}
	return nil
}

func managedRecordKey(schema connector.Schema, identityColumns []string, raw []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var key map[string]any
	if err := decoder.Decode(&key); err != nil {
		return nil, fmt.Errorf("decode managed Snowflake record key: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode managed Snowflake record key: trailing JSON value")
	}
	if len(key) != len(identityColumns) {
		return nil, fmt.Errorf("managed Snowflake record key has %d columns, admitted identity has %d", len(key), len(identityColumns))
	}
	for _, name := range identityColumns {
		value, present := key[name]
		if !present {
			return nil, fmt.Errorf("managed Snowflake record key lacks identity column %q", name)
		}
		normalized, err := normalizeManagedSnowflakeColumnValue(managedSchemaColumnByName(schema, name), value, true)
		if err != nil {
			return nil, fmt.Errorf("normalize managed Snowflake record key column %q: %w", name, err)
		}
		if normalized == nil {
			return nil, fmt.Errorf("managed Snowflake record key identity column %q is NULL", name)
		}
		key[name] = normalized
	}
	return key, nil
}

func managedWhereFromIdentity(identityColumns []string, key map[string]any) (string, []any) {
	predicates := make([]string, 0, len(identityColumns))
	values := make([]any, 0, len(identityColumns))
	for _, column := range identityColumns {
		predicates = append(predicates, quoteIdent(strings.ToUpper(column), '"')+" = ?")
		values = append(values, immutableManagedSnowflakeValue(key[column]))
	}
	return strings.Join(predicates, " AND "), values
}

func immutableManagedSnowflakeValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...)
	default:
		return value
	}
}

func managedIdentityColumns(schema connector.Schema) ([]string, error) {
	type identityColumn struct {
		name    string
		ordinal int
	}
	identities := make([]identityColumn, 0)
	for _, column := range schema.Columns {
		if column.TypeMetadata["primary_key"] != "true" {
			continue
		}
		if column.Nullable {
			return nil, fmt.Errorf("%w: managed source primary-key column %q must be NOT NULL", errManagedSnowflakeSchemaNotReconciled, column.Name)
		}
		ordinal, _ := strconv.Atoi(strings.TrimSpace(column.TypeMetadata["primary_key_ordinal"]))
		identities = append(identities, identityColumn{name: column.Name, ordinal: ordinal})
	}
	if len(identities) == 0 {
		return nil, fmt.Errorf("%w: managed source schema has no primary-key columns", errManagedSnowflakeSchemaNotReconciled)
	}
	if len(identities) == 1 {
		return []string{identities[0].name}, nil
	}
	seen := make(map[int]struct{}, len(identities))
	for _, identity := range identities {
		if identity.ordinal < 1 || identity.ordinal > len(identities) {
			return nil, fmt.Errorf("%w: composite primary-key column %q has invalid primary_key_ordinal", errManagedSnowflakeSchemaNotReconciled, identity.name)
		}
		if _, duplicate := seen[identity.ordinal]; duplicate {
			return nil, fmt.Errorf("%w: composite primary key repeats ordinal %d", errManagedSnowflakeSchemaNotReconciled, identity.ordinal)
		}
		seen[identity.ordinal] = struct{}{}
	}
	sort.Slice(identities, func(i, j int) bool { return identities[i].ordinal < identities[j].ordinal })
	columns := make([]string, len(identities))
	for index, identity := range identities {
		columns[index] = identity.name
	}
	return columns, nil
}

func managedSnowflakeColumnType(cfg managedConfig, column connector.Column) string {
	normalized := normalizeManagedSourceType(column.Type)
	compact := strings.ReplaceAll(normalized, " ", "")
	if strings.HasSuffix(compact, "[]") {
		return ""
	}
	baseType := normalized
	if index := strings.LastIndex(baseType, "."); index >= 0 {
		baseType = baseType[index+1:]
	}
	if index := strings.IndexByte(baseType, '('); index >= 0 {
		baseType = strings.TrimSpace(baseType[:index])
	}
	extension := strings.ToLower(strings.TrimSpace(column.TypeMetadata["extension"]))
	if extension != "" {
		return ""
	}
	switch baseType {
	case "bigint", "boolean", "bytea", "numeric", "text", "timestamp with time zone":
		// This is the complete SQL-v1 type cell. Expanding it requires the same
		// real-service round-trip and recovery evidence as any other admission
		// change; generic Snowflake mappings do not widen this profile.
	default:
		return ""
	}
	if mapped := managedSnowflakeType(cfg, column.Type); mapped != "" {
		if isSnowflakeJSONType(mapped) {
			return ""
		}
		return mapped
	}
	base := normalized
	isArray := strings.HasSuffix(base, "[]")
	if isArray {
		base = strings.TrimSpace(strings.TrimSuffix(base, "[]"))
	}
	if index := strings.LastIndex(base, "."); index >= 0 {
		base = base[index+1:]
	}
	rejectedBase := base
	if index := strings.IndexByte(rejectedBase, '('); index >= 0 {
		rejectedBase = strings.TrimSpace(rejectedBase[:index])
	}
	if rejectedBase == "numeric" || rejectedBase == "money" || rejectedBase == "time with time zone" {
		return ""
	}
	candidates := []string{base}
	if isArray {
		candidates = append([]string{base + "[]"}, candidates...)
	}
	if extension != "" {
		candidates = append([]string{"ext:" + extension + "." + base, "ext:" + extension}, candidates...)
	}
	for _, candidate := range candidates {
		if mapped := strings.TrimSpace(cfg.typeMappings[normalizeManagedSourceType(candidate)]); mapped != "" {
			mapped = strings.ToUpper(mapped)
			if isSnowflakeJSONType(mapped) {
				return ""
			}
			return mapped
		}
	}
	return ""
}

func managedSnowflakeType(cfg managedConfig, sourceType string) string {
	normalized := normalizeManagedSourceType(sourceType)
	compact := strings.ReplaceAll(normalized, " ", "")
	if strings.HasSuffix(compact, "[]") {
		return "ARRAY"
	}
	base, modifier := normalized, ""
	if index := strings.IndexByte(normalized, '('); index >= 0 && strings.HasSuffix(normalized, ")") {
		base = strings.TrimSpace(normalized[:index])
		modifier = strings.ReplaceAll(normalized[index:], " ", "")
	}
	switch base {
	case "timetz", "time with time zone":
		return ""
	case "numeric", "decimal":
		if modifier == "" || !validManagedNumericModifier(modifier) {
			return ""
		}
		return "NUMBER" + modifier
	case "money":
		return ""
	case "varchar", "character varying", "character", "char", "bpchar":
		if modifier != "" {
			return "VARCHAR" + modifier
		}
	}
	if mapped := strings.TrimSpace(cfg.typeMappings[normalized]); mapped != "" {
		return strings.ToUpper(mapped)
	}
	mappings := defaultSnowflakeTypeMappings()
	if mapped := strings.TrimSpace(mappings[normalized]); mapped != "" {
		return strings.ToUpper(mapped)
	}
	return ""
}

func validManagedNumericModifier(modifier string) bool {
	if len(modifier) < 5 || modifier[0] != '(' || modifier[len(modifier)-1] != ')' {
		return false
	}
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(modifier, "("), ")"), ",")
	if len(parts) != 2 {
		return false
	}
	precision, precisionErr := strconv.Atoi(parts[0])
	scale, scaleErr := strconv.Atoi(parts[1])
	return precisionErr == nil && scaleErr == nil && precision >= 1 && precision <= 38 && scale >= 0 && scale <= precision
}

func normalizeManagedSourceType(value string) string {
	normalized := strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
	if strings.HasSuffix(normalized, "[]") {
		return normalizeManagedSourceType(strings.TrimSpace(strings.TrimSuffix(normalized, "[]"))) + "[]"
	}
	for alias, canonical := range map[string]string{
		"int2": "smallint", "int4": "integer", "int8": "bigint", "bool": "boolean",
		"float4": "real", "float8": "double precision", "decimal": "numeric",
		"varchar": "character varying", "bpchar": "character", "timestamp": "timestamp without time zone",
		"timestamptz": "timestamp with time zone", "time without time zone": "time", "timetz": "time with time zone",
	} {
		if normalized == alias {
			return canonical
		}
		if (alias == "varchar" || alias == "bpchar" || alias == "decimal") && strings.HasPrefix(normalized, alias+"(") {
			return canonical + strings.TrimPrefix(normalized, alias)
		}
	}
	return normalized
}

func managedDestinationManifestHash(cfg managedConfig, intent connector.DeliveryIntent) string {
	encoded, _ := json.Marshal(struct {
		Profile                  string            `json:"profile"`
		FlowID                   string            `json:"flow_id"`
		Account                  string            `json:"account"`
		Database                 string            `json:"database"`
		Schema                   string            `json:"schema"`
		Table                    string            `json:"table"`
		ReceiptsTable            string            `json:"receipts_table"`
		OwnerRole                string            `json:"owner_role"`
		ExecutionRole            string            `json:"execution_role"`
		Warehouse                string            `json:"warehouse"`
		SnowflakeVersion         string            `json:"snowflake_version"`
		TargetCreatedOn          string            `json:"target_created_on"`
		ReceiptsCreatedOn        string            `json:"receipts_created_on"`
		SourceSchema             string            `json:"source_schema"`
		SourceTable              string            `json:"source_table"`
		SchemaContractHash       string            `json:"schema_contract_hash"`
		TypeMappings             map[string]string `json:"type_mappings"`
		MaxTransactionRows       int               `json:"max_transaction_rows"`
		MaxTransactionBytes      int64             `json:"max_transaction_bytes"`
		MaxFragments             int               `json:"max_fragments"`
		MaxOpenConnections       int               `json:"max_open_connections"`
		StatementTimeoutSeconds  int               `json:"statement_timeout_seconds"`
		HybridLockTimeoutSeconds int               `json:"hybrid_lock_timeout_seconds"`
		DestinationRevision      string            `json:"destination_revision"`
		FlowIncarnationID        string            `json:"flow_incarnation_id"`
		SourceLineageID          string            `json:"source_lineage_id"`
		LogicalBatchID           string            `json:"logical_batch_id"`
		PositionID               string            `json:"position_id"`
		ContentHash              string            `json:"content_hash"`
	}{
		Profile: cfg.profile, FlowID: cfg.flowID, Account: cfg.account, Database: cfg.database, Schema: cfg.schema,
		Table: cfg.table, ReceiptsTable: cfg.receiptsTable, OwnerRole: cfg.ownerRole, ExecutionRole: cfg.executionRole, Warehouse: cfg.warehouse,
		SnowflakeVersion: cfg.snowflakeVersion, TargetCreatedOn: cfg.targetCreatedOn, ReceiptsCreatedOn: cfg.receiptsCreatedOn,
		SourceSchema: cfg.sourceSchema, SourceTable: cfg.sourceTable,
		SchemaContractHash: cfg.schemaContractHash, TypeMappings: cfg.typeMappings,
		MaxTransactionRows: cfg.maxTransactionRows, MaxTransactionBytes: cfg.maxTransactionBytes,
		MaxFragments: cfg.maxFragments, MaxOpenConnections: cfg.maxOpenConnections,
		StatementTimeoutSeconds: cfg.statementTimeoutSeconds, HybridLockTimeoutSeconds: cfg.hybridLockTimeoutSeconds,
		DestinationRevision: intent.DestinationRevisionID, FlowIncarnationID: intent.FlowIncarnationID,
		SourceLineageID: intent.SourceLineageID, LogicalBatchID: intent.LogicalBatchID,
		PositionID: intent.PositionID, ContentHash: intent.ContentHash,
	})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
