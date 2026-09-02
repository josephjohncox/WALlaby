package snowflake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type managedCatalogSnapshot struct {
	target    managedTableSnapshot
	receipts  managedTableSnapshot
	taskCount int
}

type managedTableSnapshot struct {
	kind                 string
	ownerRole            string
	createdOn            string
	comment              string
	definition           string
	columns              map[string]managedColumnSnapshot
	constraints          []managedConstraintSnapshot
	otherConstraintCount int
	grants               map[string][]string
}

type managedColumnSnapshot struct {
	dataType               string
	characterMaximumLength int64
	numericPrecision       int64
	numericScale           int64
	datetimePrecision      int64
	nullable               bool
	hasDefault             bool
	generated              bool
}

type managedConstraintSnapshot struct {
	name           string
	constraintType string
	enforced       bool
	columns        []string
}

func validateManagedSnowflakeCatalog(cfg managedConfig, catalog managedCatalogSnapshot) error {
	if catalog.taskCount != 0 {
		return fmt.Errorf("managed Snowflake schema contains %d tasks; dedicated managed schema requires none", catalog.taskCount)
	}
	if normalizeManagedSnowflakeKind(catalog.target.kind) != "HYBRID TABLE" {
		return fmt.Errorf("managed Snowflake target must be HYBRID TABLE, got %q", catalog.target.kind)
	}
	if catalog.target.ownerRole != cfg.ownerRole {
		return fmt.Errorf("managed Snowflake target must be owned by %s, got %s", cfg.ownerRole, catalog.target.ownerRole)
	}
	if catalog.target.createdOn != cfg.targetCreatedOn {
		return fmt.Errorf("managed Snowflake target creation identity %q differs from configured %q", catalog.target.createdOn, cfg.targetCreatedOn)
	}
	if catalog.target.comment != managedTableOwnershipComment(cfg, false) {
		return fmt.Errorf("managed Snowflake target ownership comment differs from destination revision and schema contract")
	}
	if err := validateManagedSnowflakeExecutionGrants(cfg, catalog.target, []string{"DELETE", "INSERT", "SELECT", "UPDATE"}); err != nil {
		return fmt.Errorf("managed Snowflake target grants: %w", err)
	}
	if catalog.target.otherConstraintCount != 0 {
		return fmt.Errorf("managed Snowflake target has %d non-primary/unique constraints, want none", catalog.target.otherConstraintCount)
	}
	if err := validateManagedSnowflakeTargetSchema(cfg, catalog.target); err != nil {
		return err
	}

	if normalizeManagedSnowflakeKind(catalog.receipts.kind) != "HYBRID TABLE" {
		return fmt.Errorf("managed Snowflake receipt table must be HYBRID TABLE, got %q", catalog.receipts.kind)
	}
	if catalog.receipts.ownerRole != cfg.ownerRole {
		return fmt.Errorf("managed Snowflake receipt table must be owned by %s, got %s", cfg.ownerRole, catalog.receipts.ownerRole)
	}
	if catalog.receipts.createdOn != cfg.receiptsCreatedOn {
		return fmt.Errorf("managed Snowflake receipt creation identity %q differs from configured %q", catalog.receipts.createdOn, cfg.receiptsCreatedOn)
	}
	if catalog.receipts.comment != managedTableOwnershipComment(cfg, true) {
		return errors.New("managed Snowflake receipt ownership comment differs from destination revision and schema contract")
	}
	if err := validateManagedSnowflakeExecutionGrants(cfg, catalog.receipts, []string{"INSERT", "SELECT"}); err != nil {
		return fmt.Errorf("managed Snowflake receipt grants: %w", err)
	}
	if catalog.receipts.otherConstraintCount != 0 {
		return fmt.Errorf("managed Snowflake receipt table has %d non-primary/unique constraints, want none", catalog.receipts.otherConstraintCount)
	}
	if err := validateManagedSnowflakeReceiptSchema(catalog.receipts); err != nil {
		return err
	}
	return nil
}

func validateManagedSnowflakeExecutionGrants(cfg managedConfig, table managedTableSnapshot, expected []string) error {
	actual := append([]string(nil), table.grants[cfg.executionRole]...)
	sort.Strings(actual)
	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("execution role %s privileges=%v, want %v", cfg.executionRole, actual, expected)
	}
	owner := append([]string(nil), table.grants[cfg.ownerRole]...)
	sort.Strings(owner)
	if !reflect.DeepEqual(owner, []string{"OWNERSHIP"}) {
		return fmt.Errorf("owner role %s privileges=%v, want [OWNERSHIP]", cfg.ownerRole, owner)
	}
	for role, privileges := range table.grants {
		if role == cfg.executionRole || role == cfg.ownerRole {
			continue
		}
		for _, privilege := range privileges {
			switch privilege {
			case "DELETE", "INSERT", "OWNERSHIP", "TRUNCATE", "UPDATE":
				return fmt.Errorf("additional writer role %s has %s", role, privilege)
			}
		}
	}
	return nil
}

func managedSnowflakeCatalogFingerprint(catalog managedCatalogSnapshot) (string, error) {
	type fingerprintColumn struct {
		DataType               string `json:"data_type"`
		CharacterMaximumLength int64  `json:"character_maximum_length"`
		DatetimePrecision      int64  `json:"datetime_precision"`
		Nullable               bool   `json:"nullable"`
		HasDefault             bool   `json:"has_default"`
		Generated              bool   `json:"generated"`
	}
	type fingerprintConstraint struct {
		Name           string   `json:"name"`
		ConstraintType string   `json:"constraint_type"`
		Enforced       bool     `json:"enforced"`
		Columns        []string `json:"columns"`
	}
	type fingerprintTable struct {
		Kind                 string                       `json:"kind"`
		OwnerRole            string                       `json:"owner_role"`
		CreatedOn            string                       `json:"created_on"`
		Comment              string                       `json:"comment"`
		Columns              map[string]fingerprintColumn `json:"columns"`
		Constraints          []fingerprintConstraint      `json:"constraints"`
		OtherConstraintCount int                          `json:"other_constraint_count"`
		Grants               map[string][]string          `json:"grants"`
	}
	canonicalize := func(table managedTableSnapshot) fingerprintTable {
		result := fingerprintTable{
			Kind: table.kind, OwnerRole: table.ownerRole, CreatedOn: table.createdOn, Comment: table.comment,
			OtherConstraintCount: table.otherConstraintCount,
			Columns:              make(map[string]fingerprintColumn, len(table.columns)), Grants: make(map[string][]string, len(table.grants)),
		}
		for name, column := range table.columns {
			result.Columns[name] = fingerprintColumn{
				DataType: column.dataType, CharacterMaximumLength: column.characterMaximumLength,
				DatetimePrecision: column.datetimePrecision, Nullable: column.nullable,
				HasDefault: column.hasDefault, Generated: column.generated,
			}
		}
		for _, constraint := range table.constraints {
			result.Constraints = append(result.Constraints, fingerprintConstraint{
				Name: constraint.name, ConstraintType: constraint.constraintType, Enforced: constraint.enforced,
				Columns: append([]string(nil), constraint.columns...),
			})
		}
		for role, privileges := range table.grants {
			result.Grants[role] = append([]string(nil), privileges...)
			sort.Strings(result.Grants[role])
		}
		return result
	}
	encoded, err := json.Marshal(struct {
		Target    fingerprintTable `json:"target"`
		Receipts  fingerprintTable `json:"receipts"`
		TaskCount int              `json:"task_count"`
	}{Target: canonicalize(catalog.target), Receipts: canonicalize(catalog.receipts), TaskCount: catalog.taskCount})
	if err != nil {
		return "", fmt.Errorf("encode managed Snowflake catalog fingerprint: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func validateManagedSnowflakeTargetSchema(cfg managedConfig, target managedTableSnapshot) error {
	identityColumns, err := managedIdentityColumns(cfg.schemaContract)
	if err != nil {
		return err
	}
	upperIdentity := make([]string, len(identityColumns))
	for index, column := range identityColumns {
		upperIdentity[index] = strings.ToUpper(column)
	}
	for _, source := range cfg.schemaContract.Columns {
		if source.Generated {
			return fmt.Errorf("managed Snowflake source generated column %q is not admitted", source.Name)
		}
		if source.TypeMetadata["nullability_known"] != "true" || source.TypeMetadata["generated_known"] != "true" {
			return fmt.Errorf("managed Snowflake source column %q has unknown nullability or generation status", source.Name)
		}
		targetColumn, present := target.columns[strings.ToUpper(source.Name)]
		if !present {
			return fmt.Errorf("managed Snowflake target is missing source column %q", source.Name)
		}
		expectedType := managedSnowflakeColumnType(cfg, source)
		if expectedType == "" || !managedSnowflakeTypesEquivalent(targetColumn.dataType, expectedType) {
			return fmt.Errorf("managed Snowflake target column %q has incompatible type %q for source type %q", source.Name, targetColumn.dataType, source.Type)
		}
		normalizedTargetType := normalizeManagedSnowflakeType(expectedType)
		if normalizedTargetType == "VARCHAR" {
			requiredWidth := cfg.maxTransactionBytes
			if boundedWidth, bounded := managedSourceCharacterWidth(source.Type); bounded && boundedWidth < requiredWidth {
				requiredWidth = boundedWidth
			}
			if targetColumn.characterMaximumLength < requiredWidth {
				return fmt.Errorf("managed Snowflake target column %q VARCHAR width %d is below admitted source width %d", source.Name, targetColumn.characterMaximumLength, requiredWidth)
			}
		}
		if normalizedTargetType == "BINARY" && targetColumn.characterMaximumLength < cfg.maxTransactionBytes {
			return fmt.Errorf("managed Snowflake target column %q BINARY width %d is below admitted transaction bytes %d", source.Name, targetColumn.characterMaximumLength, cfg.maxTransactionBytes)
		}
		if (strings.HasPrefix(normalizedTargetType, "TIMESTAMP_") || normalizedTargetType == "TIME") && targetColumn.datetimePrecision < 6 {
			return fmt.Errorf("managed Snowflake target column %q datetime precision %d is below PostgreSQL microsecond precision", source.Name, targetColumn.datetimePrecision)
		}
		if targetColumn.nullable != source.Nullable {
			return fmt.Errorf("managed Snowflake target column %q nullability differs from source", source.Name)
		}
		if targetColumn.generated || targetColumn.hasDefault {
			return fmt.Errorf("managed Snowflake target column %q must be an ordinary stored column without a target default", source.Name)
		}
	}
	if len(target.columns) != len(cfg.schemaContract.Columns) {
		return fmt.Errorf("managed Snowflake target has %d columns, admitted source contract has %d", len(target.columns), len(cfg.schemaContract.Columns))
	}
	if len(target.constraints) != 1 || !hasManagedEnforcedConstraintType(target.constraints, "PRIMARY KEY", upperIdentity) {
		return fmt.Errorf("managed Snowflake target requires exactly one enforced primary key on source identity columns %v", upperIdentity)
	}
	return nil
}

func validateManagedSnowflakeReceiptSchema(receipts managedTableSnapshot) error {
	expected := managedExpectedReceiptColumns()
	for name, want := range expected {
		got, present := receipts.columns[name]
		if !present {
			return fmt.Errorf("managed Snowflake receipt column %s is missing", name)
		}
		if !managedSnowflakeTypesEquivalent(got.dataType, want.dataType) || got.nullable != want.nullable || got.generated || got.hasDefault != want.hasDefault {
			return fmt.Errorf("managed Snowflake receipt column %s contract differs", name)
		}
		if normalizeManagedSnowflakeType(want.dataType) == "VARCHAR" && got.characterMaximumLength < 1024 {
			return fmt.Errorf("managed Snowflake receipt column %s VARCHAR width %d is below identity bound 1024", name, got.characterMaximumLength)
		}
	}
	if len(receipts.columns) != len(expected) {
		return fmt.Errorf("managed Snowflake receipt column count=%d, want %d", len(receipts.columns), len(expected))
	}
	required := []struct {
		kind    string
		columns []string
	}{
		{kind: "PRIMARY KEY", columns: []string{"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "SOURCE_LINEAGE_ID", "POSITION_ID"}},
		{kind: "UNIQUE", columns: []string{"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID"}},
		{kind: "UNIQUE", columns: []string{"EXTERNAL_ID"}},
	}
	if len(receipts.constraints) != len(required) {
		return fmt.Errorf("managed Snowflake receipt table has %d primary/unique constraints, want exactly %d", len(receipts.constraints), len(required))
	}
	for _, constraint := range required {
		if !hasManagedEnforcedConstraintType(receipts.constraints, constraint.kind, constraint.columns) {
			return fmt.Errorf("managed Snowflake receipt table requires enforced %s constraint on %v", constraint.kind, constraint.columns)
		}
	}
	return nil
}

func managedExpectedReceiptColumns() map[string]managedColumnSnapshot {
	notNullText := managedColumnSnapshot{dataType: "VARCHAR", characterMaximumLength: 1024, nullable: false}
	notNullNumber := managedColumnSnapshot{dataType: "NUMBER(38,0)", nullable: false}
	columns := map[string]managedColumnSnapshot{
		"PROFILE_VERSION":         notNullText,
		"FLOW_ID":                 notNullText,
		"FLOW_INCARNATION_ID":     notNullText,
		"SOURCE_LINEAGE_ID":       notNullText,
		"DESTINATION_REVISION_ID": notNullText,
		"LOGICAL_BATCH_ID":        notNullText,
		"POSITION_ID":             notNullText,
		"CONTENT_HASH":            notNullText,
		"SCHEMA_CONTRACT_HASH":    notNullText,
		"CATALOG_FINGERPRINT":     notNullText,
		"MANIFEST_HASH":           notNullText,
		"EXTERNAL_ID":             notNullText,
		"GENERATION":              notNullNumber,
		"ACQUISITION_ID":          notNullText,
		"LEASE_EPOCH":             notNullNumber,
		"TRANSACTION_ID":          notNullNumber,
		"FRAGMENT_COUNT":          notNullNumber,
		"RECORD_COUNT":            notNullNumber,
		"COMMITTED_AT":            {dataType: "TIMESTAMP_TZ", nullable: false},
	}
	return columns
}

func hasManagedEnforcedConstraintType(constraints []managedConstraintSnapshot, expectedType string, columns []string) bool {
	for _, constraint := range constraints {
		kind := strings.ToUpper(strings.TrimSpace(constraint.constraintType))
		if !constraint.enforced || kind != expectedType {
			continue
		}
		if reflect.DeepEqual(constraint.columns, columns) {
			return true
		}
	}
	return false
}

func managedTableOwnershipComment(cfg managedConfig, receipts bool) string {
	objectKind := "target"
	if receipts {
		objectKind = "receipts"
	}
	flowDigest := sha256.Sum256([]byte(cfg.flowID))
	return strings.Join([]string{"wallaby", cfg.profile, objectKind, cfg.destinationRevision, cfg.schemaContractHash, hex.EncodeToString(flowDigest[:])}, ":")
}

func managedSourceCharacterWidth(sourceType string) (int64, bool) {
	normalized := normalizeManagedSourceType(sourceType)
	open := strings.IndexByte(normalized, '(')
	if open < 0 || !strings.HasSuffix(normalized, ")") {
		return 0, false
	}
	base := strings.TrimSpace(normalized[:open])
	if base != "varchar" && base != "character varying" && base != "character" && base != "char" && base != "bpchar" {
		return 0, false
	}
	width, err := strconv.ParseInt(strings.TrimSpace(normalized[open+1:len(normalized)-1]), 10, 64)
	return width, err == nil && width > 0
}

func managedSnowflakeTypesEquivalent(actual, expected string) bool {
	return normalizeManagedSnowflakeType(actual) == normalizeManagedSnowflakeType(expected)
}

func normalizeManagedSnowflakeType(value string) string {
	value = strings.ToUpper(strings.ReplaceAll(strings.Join(strings.Fields(value), ""), " ", ""))
	switch {
	case value == "BIGINT", value == "INTEGER", value == "INT", value == "SMALLINT", value == "NUMBER", value == "NUMBER(38,0)", value == "DECIMAL(38,0)", value == "NUMERIC(38,0)":
		return "NUMBER(38,0)"
	case strings.HasPrefix(value, "VARCHAR"), strings.HasPrefix(value, "CHARACTER VARYING"), value == "TEXT", value == "STRING":
		return "VARCHAR"
	case value == "DOUBLE", value == "DOUBLEPRECISION", value == "REAL", value == "FLOAT", strings.HasPrefix(value, "FLOAT("):
		return "FLOAT"
	case strings.HasPrefix(value, "BINARY"), strings.HasPrefix(value, "VARBINARY"):
		return "BINARY"
	case strings.HasPrefix(value, "TIMESTAMP_NTZ"):
		return "TIMESTAMP_NTZ"
	case strings.HasPrefix(value, "TIMESTAMP_TZ"):
		return "TIMESTAMP_TZ"
	case strings.HasPrefix(value, "TIMESTAMP_LTZ"):
		return "TIMESTAMP_LTZ"
	case strings.HasPrefix(value, "TIME"):
		return "TIME"
	case strings.HasPrefix(value, "NUMBER(") || strings.HasPrefix(value, "DECIMAL(") || strings.HasPrefix(value, "NUMERIC("):
		return strings.NewReplacer("DECIMAL", "NUMBER", "NUMERIC", "NUMBER").Replace(value)
	default:
		return value
	}
}

func normalizeManagedSnowflakeKind(value string) string {
	words := strings.Fields(strings.ToUpper(value))
	sort.Strings(words)
	if strings.Join(words, " ") == "HYBRID TABLE" {
		return "HYBRID TABLE"
	}
	return strings.Join(strings.Fields(strings.ToUpper(value)), " ")
}
