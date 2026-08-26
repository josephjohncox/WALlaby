package snowflake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type managedStagedCatalogSnapshot struct {
	stage               managedStageSnapshot
	fileFormat          managedFileFormatSnapshot
	target              managedTableSnapshot
	receipts            managedTableSnapshot
	landing             managedTableSnapshot
	authority           managedTableSnapshot
	targetManifest      managedTableSnapshot
	pipe                managedPipeSnapshot
	taskCount           int
	unexpectedPipeCount int
}

type managedStageSnapshot struct {
	kind      string
	ownerRole string
	createdOn string
	comment   string
	grants    map[string][]string
}

type managedFileFormatPropertySnapshot struct {
	propertyType    string
	propertyValue   string
	propertyDefault string
}

type managedFileFormatSnapshot struct {
	formatType string
	ownerRole  string
	createdOn  string
	comment    string
	definition string
	properties map[string]managedFileFormatPropertySnapshot
	grants     map[string][]string
}

func managedStagedJSONFileFormatProperties() map[string]managedFileFormatPropertySnapshot {
	values := map[string]struct{ propertyType, propertyValue string }{
		"TYPE": {"String", "JSON"}, "FILE_EXTENSION": {"String", ""},
		"DATE_FORMAT": {"String", "AUTO"}, "TIME_FORMAT": {"String", "AUTO"}, "TIMESTAMP_FORMAT": {"String", "AUTO"},
		"BINARY_FORMAT": {"String", "HEX"}, "TRIM_SPACE": {"Boolean", "FALSE"}, "NULL_IF": {"List", "[]"},
		"COMPRESSION": {"String", "AUTO"}, "ENABLE_OCTAL": {"Boolean", "FALSE"}, "ALLOW_DUPLICATE": {"Boolean", "FALSE"},
		"STRIP_OUTER_ARRAY": {"Boolean", "FALSE"}, "STRIP_NULL_VALUES": {"Boolean", "FALSE"},
		"IGNORE_UTF8_ERRORS": {"Boolean", "FALSE"}, "REPLACE_INVALID_CHARACTERS": {"Boolean", "FALSE"},
		"SKIP_BYTE_ORDER_MARK": {"Boolean", "TRUE"},
	}
	properties := make(map[string]managedFileFormatPropertySnapshot, len(values))
	for name, value := range values {
		properties[name] = managedFileFormatPropertySnapshot{propertyType: value.propertyType, propertyValue: value.propertyValue}
	}
	return properties
}

type managedPipeSnapshot struct {
	present           bool
	definition        string
	ownerRole         string
	createdOn         string
	autoIngest        bool
	onError           string
	force             string
	purge             string
	matchByColumnName string
	comment           string
	grants            map[string][]string
}

func validateManagedStagedCatalog(cfg stagedConfig, catalog managedStagedCatalogSnapshot) error {
	if catalog.taskCount != 0 {
		return fmt.Errorf("managed staged Snowflake schema contains %d tasks; the dedicated schema requires none", catalog.taskCount)
	}
	if err := validateManagedStagedStage(cfg, catalog.stage); err != nil {
		return err
	}
	if err := validateManagedStagedFileFormat(cfg, catalog.fileFormat); err != nil {
		return err
	}
	if err := validateManagedStagedTarget(cfg, catalog.target); err != nil {
		return err
	}
	if err := validateManagedStagedReceipts(cfg, catalog.receipts); err != nil {
		return err
	}
	if err := validateManagedStagedAuxiliaryTables(cfg, catalog); err != nil {
		return err
	}
	if err := validateManagedStagedPipe(cfg, catalog.pipe); err != nil {
		return err
	}
	if !cfg.autoIngest && catalog.unexpectedPipeCount != 0 {
		return fmt.Errorf("managed staged Snowflake synchronous profile observed %d pipes in the dedicated schema", catalog.unexpectedPipeCount)
	}
	if cfg.autoIngest && catalog.unexpectedPipeCount != 1 {
		return fmt.Errorf("managed staged Snowflake auto-ingest profile requires exactly one pipe in the dedicated schema, got %d", catalog.unexpectedPipeCount)
	}
	return nil
}

func validateManagedStagedStage(cfg stagedConfig, stage managedStageSnapshot) error {
	if strings.ToUpper(strings.TrimSpace(stage.kind)) != "INTERNAL" {
		return fmt.Errorf("managed staged Snowflake stage must be INTERNAL, got %q", stage.kind)
	}
	if stage.ownerRole != cfg.ownerRole {
		return fmt.Errorf("managed staged Snowflake stage must be owned by %s, got %s", cfg.ownerRole, stage.ownerRole)
	}
	if stage.createdOn != cfg.stageCreatedOn {
		return fmt.Errorf("managed staged Snowflake stage creation identity %q differs from configured %q", stage.createdOn, cfg.stageCreatedOn)
	}
	if stage.comment != managedStagedOwnershipComment(cfg, "stage") {
		return errors.New("managed staged Snowflake stage ownership comment differs from destination revision and schema contract")
	}
	return validateManagedStagedGrants(cfg, stage.grants, []string{"READ", "WRITE"}, []string{"OWNERSHIP", "READ", "WRITE"})
}

func validateManagedStagedFileFormat(cfg stagedConfig, format managedFileFormatSnapshot) error {
	if strings.ToUpper(strings.TrimSpace(format.formatType)) != "JSON" {
		return fmt.Errorf("managed staged Snowflake file format must be JSON, got %q", format.formatType)
	}
	if format.ownerRole != cfg.ownerRole {
		return fmt.Errorf("managed staged Snowflake file format must be owned by %s, got %s", cfg.ownerRole, format.ownerRole)
	}
	if format.createdOn != cfg.fileFormatCreatedOn {
		return fmt.Errorf("managed staged Snowflake file format creation identity %q differs from configured %q", format.createdOn, cfg.fileFormatCreatedOn)
	}
	if format.comment != managedStagedOwnershipComment(cfg, "file_format") {
		return errors.New("managed staged Snowflake file format ownership comment differs from destination revision and schema contract")
	}
	if len(format.definition) == 0 || len(format.definition) > 64<<10 {
		return fmt.Errorf("managed staged Snowflake file format definition has invalid length %d", len(format.definition))
	}
	if !stagedFileFormatDefinitionOption(format.definition, "MULTI_LINE", "FALSE") {
		return errors.New("managed staged Snowflake JSON file format requires an explicit MULTI_LINE=FALSE definition")
	}
	expected := managedStagedJSONFileFormatProperties()
	if len(format.properties) < len(expected) || len(format.properties) > len(expected)+1 {
		return fmt.Errorf("managed staged Snowflake JSON file format exposes %d properties, want %d required properties and at most documented MULTI_LINE", len(format.properties), len(expected))
	}
	for name := range format.properties {
		if _, required := expected[name]; !required && name != "MULTI_LINE" {
			return fmt.Errorf("managed staged Snowflake JSON file format exposed unadmitted property %s", name)
		}
	}
	for name, expectedProperty := range expected {
		actual, ok := format.properties[name]
		if !ok {
			return fmt.Errorf("managed staged Snowflake JSON file format omitted property %s", name)
		}
		if actual.propertyType != expectedProperty.propertyType || actual.propertyValue != expectedProperty.propertyValue {
			return fmt.Errorf("managed staged Snowflake JSON file format property %s=(type:%q value:%q), want (type:%q value:%q)", name, actual.propertyType, actual.propertyValue, expectedProperty.propertyType, expectedProperty.propertyValue)
		}
	}
	if multiLine, present := format.properties["MULTI_LINE"]; present && (multiLine.propertyType != "Boolean" || multiLine.propertyValue != "FALSE") {
		return fmt.Errorf("managed staged Snowflake JSON file format property MULTI_LINE=(type:%q value:%q), want (type:%q value:%q)", multiLine.propertyType, multiLine.propertyValue, "Boolean", "FALSE")
	}
	return validateManagedStagedGrants(cfg, format.grants, []string{"USAGE"}, []string{"OWNERSHIP", "USAGE"})
}

func stagedFileFormatDefinitionOption(definition, option, expected string) bool {
	upper := strings.ToUpper(stripStagedSQLStringLiterals(definition))
	option = strings.ToUpper(strings.TrimSpace(option))
	expected = strings.ToUpper(strings.TrimSpace(expected))
	identifierByte := func(value byte) bool { return value == '_' || value >= 'A' && value <= 'Z' }
	for offset := 0; offset < len(upper); {
		position := strings.Index(upper[offset:], option)
		if position < 0 {
			return false
		}
		position += offset
		beforeOK := position == 0 || !identifierByte(upper[position-1])
		after := position + len(option)
		afterOK := after == len(upper) || !identifierByte(upper[after])
		if beforeOK && afterOK {
			rest := strings.TrimSpace(upper[after:])
			if strings.HasPrefix(rest, "=") {
				value := strings.TrimSpace(rest[1:])
				if len(value) >= len(expected) && value[:len(expected)] == expected && (len(value) == len(expected) || strings.ContainsRune(" \t\r\n,);", rune(value[len(expected)]))) {
					return true
				}
			}
		}
		offset = after
	}
	return false
}

// stagedFileFormatDefinitionOptionPresent reports whether an option name appears
// at all outside string literals, regardless of its value.
func stagedFileFormatDefinitionOptionPresent(definition, option string) bool {
	upper := strings.ToUpper(stripStagedSQLStringLiterals(definition))
	option = strings.ToUpper(strings.TrimSpace(option))
	identifierByte := func(value byte) bool { return value == '_' || value >= 'A' && value <= 'Z' }
	for offset := 0; offset < len(upper); {
		position := strings.Index(upper[offset:], option)
		if position < 0 {
			return false
		}
		position += offset
		before := position == 0 || !identifierByte(upper[position-1])
		after := position + len(option)
		if before && (after == len(upper) || !identifierByte(upper[after])) {
			return true
		}
		offset = after
	}
	return false
}

// stripStagedSQLStringLiterals blanks string literals and SQL comments so option
// scanning can never be satisfied by text a provisioner placed inside a quoted
// comment value, a line comment, or a block comment.
func stripStagedSQLStringLiterals(statement string) string {
	result := []byte(statement)
	for position := 0; position < len(result); {
		if position+1 < len(result) && result[position] == '/' && result[position+1] == '*' {
			for position < len(result) {
				closing := position+1 < len(result) && result[position] == '*' && result[position+1] == '/'
				result[position] = ' '
				position++
				if closing {
					result[position] = ' '
					position++
					break
				}
			}
			continue
		}
		if position+1 < len(result) && result[position] == '-' && result[position+1] == '-' {
			for position < len(result) && result[position] != '\n' {
				result[position] = ' '
				position++
			}
			continue
		}
		if result[position] != '\'' {
			position++
			continue
		}
		result[position] = ' '
		position++
		for position < len(result) {
			if result[position] != '\'' {
				result[position] = ' '
				position++
				continue
			}
			result[position] = ' '
			if position+1 < len(result) && result[position+1] == '\'' {
				result[position+1] = ' '
				position += 2
				continue
			}
			position++
			break
		}
	}
	return string(result)
}

func validateManagedStagedTarget(cfg stagedConfig, target managedTableSnapshot) error {
	if normalizeManagedSnowflakeKind(target.kind) == "HYBRID TABLE" {
		return errors.New("managed staged Snowflake target must be a standard append table, not a hybrid table")
	}
	if target.ownerRole != cfg.ownerRole {
		return fmt.Errorf("managed staged Snowflake target must be owned by %s, got %s", cfg.ownerRole, target.ownerRole)
	}
	if target.createdOn != cfg.targetCreatedOn {
		return fmt.Errorf("managed staged Snowflake target creation identity %q differs from configured %q", target.createdOn, cfg.targetCreatedOn)
	}
	if target.comment != managedStagedOwnershipComment(cfg, "target") {
		return errors.New("managed staged Snowflake target ownership comment differs from destination revision and schema contract")
	}
	if err := validateManagedStagedGrants(cfg, target.grants, []string{"INSERT", "SELECT"}, []string{"OWNERSHIP"}); err != nil {
		return fmt.Errorf("managed staged Snowflake target grants: %w", err)
	}
	if target.otherConstraintCount != 0 {
		return fmt.Errorf("managed staged Snowflake target has %d unexpected constraints, want none", target.otherConstraintCount)
	}
	return validateManagedStagedColumns("target", target.columns, stagedExpectedTargetColumns())
}

func validateManagedStagedReceipts(cfg stagedConfig, receipts managedTableSnapshot) error {
	if normalizeManagedSnowflakeKind(receipts.kind) != "HYBRID TABLE" {
		return fmt.Errorf("managed staged Snowflake receipt table must be HYBRID TABLE, got %q", receipts.kind)
	}
	if receipts.ownerRole != cfg.ownerRole {
		return fmt.Errorf("managed staged Snowflake receipt table must be owned by %s, got %s", cfg.ownerRole, receipts.ownerRole)
	}
	if receipts.createdOn != cfg.receiptsCreatedOn {
		return fmt.Errorf("managed staged Snowflake receipt creation identity %q differs from configured %q", receipts.createdOn, cfg.receiptsCreatedOn)
	}
	if receipts.comment != managedStagedOwnershipComment(cfg, "receipts") {
		return errors.New("managed staged Snowflake receipt ownership comment differs from destination revision and schema contract")
	}
	if err := validateManagedStagedGrants(cfg, receipts.grants, []string{"INSERT", "SELECT"}, []string{"OWNERSHIP"}); err != nil {
		return fmt.Errorf("managed staged Snowflake receipt grants: %w", err)
	}
	if err := validateManagedStagedColumns("receipts", receipts.columns, stagedExpectedReceiptColumns()); err != nil {
		return err
	}
	required := []struct {
		kind    string
		columns []string
	}{
		{kind: "PRIMARY KEY", columns: []string{"RECEIPT_KIND", "FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID"}},
		{kind: "UNIQUE", columns: []string{"EXTERNAL_ID"}},
	}
	if len(receipts.constraints) != len(required) {
		return fmt.Errorf("managed staged Snowflake receipt table has %d primary/unique constraints, want %d", len(receipts.constraints), len(required))
	}
	for _, constraint := range required {
		if !hasManagedEnforcedConstraintType(receipts.constraints, constraint.kind, constraint.columns) {
			return fmt.Errorf("managed staged Snowflake receipt table requires enforced %s on %v", constraint.kind, constraint.columns)
		}
	}
	return nil
}

func validateManagedStagedAuxiliaryTables(cfg stagedConfig, catalog managedStagedCatalogSnapshot) error {
	for _, item := range []struct {
		name, object, createdOn, commentKind, kind string
		table                                      managedTableSnapshot
		columns                                    map[string]managedColumnSnapshot
		execution                                  []string
	}{
		{name: "landing", object: cfg.landingTable, createdOn: cfg.landingCreatedOn, commentKind: "landing", kind: "TABLE", table: catalog.landing, columns: stagedExpectedTargetColumns(), execution: []string{"DELETE", "INSERT", "SELECT"}},
		{name: "authority", object: cfg.authorityTable, createdOn: cfg.authorityCreatedOn, commentKind: "authority", kind: "HYBRID TABLE", table: catalog.authority, columns: stagedExpectedAuthorityColumns(), execution: []string{"DELETE", "INSERT", "SELECT", "UPDATE"}},
		{name: "target manifest", object: cfg.targetManifestTable, createdOn: cfg.targetManifestCreatedOn, commentKind: "target_manifest", kind: "HYBRID TABLE", table: catalog.targetManifest, columns: stagedExpectedTargetManifestColumns(), execution: []string{"INSERT", "SELECT"}},
	} {
		if normalizeManagedSnowflakeKind(item.table.kind) != item.kind {
			return fmt.Errorf("managed staged Snowflake %s table kind=%q, want %s", item.name, item.table.kind, item.kind)
		}
		if item.table.ownerRole != cfg.ownerRole || item.table.createdOn != item.createdOn || item.table.comment != managedStagedOwnershipComment(cfg, item.commentKind) {
			return fmt.Errorf("managed staged Snowflake %s ownership or creation identity differs", item.name)
		}
		if err := validateManagedStagedGrants(cfg, item.table.grants, item.execution, []string{"OWNERSHIP"}); err != nil {
			return fmt.Errorf("managed staged Snowflake %s grants: %w", item.name, err)
		}
		if err := validateManagedStagedColumns(item.name, item.table.columns, item.columns); err != nil {
			return err
		}
		if item.table.otherConstraintCount != 0 {
			return fmt.Errorf("managed staged Snowflake %s has unsupported constraints", item.name)
		}
		switch item.name {
		case "landing":
			if len(item.table.constraints) != 0 {
				return errors.New("managed staged Snowflake landing table must not have key constraints")
			}
		case "authority":
			if len(item.table.constraints) != 1 || !hasManagedEnforcedConstraintType(item.table.constraints, "PRIMARY KEY", []string{"AUTHORITY_KIND", "DESTINATION_REVISION_ID", "AUTHORITY_ID"}) {
				return errors.New("managed staged Snowflake authority table requires its exact enforced primary key")
			}
		case "target manifest":
			if len(item.table.constraints) != 2 || !hasManagedEnforcedConstraintType(item.table.constraints, "PRIMARY KEY", []string{"DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID"}) || !hasManagedEnforcedConstraintType(item.table.constraints, "UNIQUE", []string{"MANIFEST_HASH"}) {
				return errors.New("managed staged Snowflake target manifest table requires exact enforced primary and manifest keys")
			}
		}
	}
	return nil
}

func normalizeStagedPipeSQL(value string) string {
	return strings.Map(func(character rune) rune {
		if character == '"' || character == '`' || character == ' ' || character == '\t' || character == '\r' || character == '\n' {
			return -1
		}
		if character >= 'A' && character <= 'Z' {
			return character + ('a' - 'A')
		}
		return character
	}, value)
}

func stagedExpectedAuthorityColumns() map[string]managedColumnSnapshot {
	text := managedColumnSnapshot{dataType: "VARCHAR", nullable: false}
	number := managedColumnSnapshot{dataType: "NUMBER(38,0)", nullable: false}
	timestamp := managedColumnSnapshot{dataType: "TIMESTAMP_LTZ(9)", nullable: false}
	optionalText := managedColumnSnapshot{dataType: "VARCHAR", nullable: true}
	optionalNumber := managedColumnSnapshot{dataType: "NUMBER(38,0)", nullable: true}
	return map[string]managedColumnSnapshot{
		"AUTHORITY_KIND": text, "DESTINATION_REVISION_ID": text, "AUTHORITY_ID": text, "OWNER_ID": text,
		"FLOW_INCARNATION_ID": optionalText, "GENERATION": optionalNumber, "ACQUISITION_ID": optionalText,
		"LEASE_EPOCH": optionalNumber, "PROVISION_EPOCH": number, "PROVISION_ATTEMPT_ID": optionalText, "CATALOG_FINGERPRINT": text,
		"LOGICAL_BATCH_ID": optionalText, "MANIFEST_HASH": optionalText, "CONTENT_HASH": optionalText,
		"FILE_CONTENT_HASH": optionalText, "PLAN_HASH": optionalText, "EXPECTED_ROW_COUNT": optionalNumber,
		"STATE": text, "EXPIRES_AT": timestamp, "UPDATED_AT": timestamp,
	}
}

func stagedExpectedTargetManifestColumns() map[string]managedColumnSnapshot {
	text := managedColumnSnapshot{dataType: "VARCHAR", nullable: false}
	number := managedColumnSnapshot{dataType: "NUMBER(38,0)", nullable: false}
	return map[string]managedColumnSnapshot{
		"DESTINATION_REVISION_ID": text, "LOGICAL_BATCH_ID": text, "MANIFEST_HASH": text, "CONTENT_HASH": text,
		"FILE_CONTENT_HASH": text, "PLAN_HASH": text, "EXPECTED_ROW_COUNT": number, "ROW_HASHES_JSON": text,
		"PROVISION_EPOCH": number, "CATALOG_FINGERPRINT": text,
		"COMMITTED_AT": {dataType: "TIMESTAMP_LTZ(9)", nullable: false},
	}
}

func validateManagedStagedPipe(cfg stagedConfig, pipe managedPipeSnapshot) error {
	if !cfg.autoIngest {
		if pipe.present {
			return errors.New("managed staged Snowflake profile observed a pipe but auto-ingest is disabled")
		}
		return nil
	}
	if !pipe.present {
		return errors.New("managed staged Snowflake auto-ingest requires the configured pipe to exist")
	}
	if !pipe.autoIngest {
		return errors.New("managed staged Snowflake auto-ingest pipe must have AUTO_INGEST=TRUE")
	}
	// The fail-closed COPY invariants enforced on the synchronous path
	// (newStagedCopyPlan) must also hold on an operator-provisioned pipe, because
	// Snowpipe's default ON_ERROR is SKIP_FILE. Reject any pipe whose COPY
	// DEFINITION would allow partial or skipped loads that wallaby never inspects.
	plan, err := newStagedCopyPlan(cfg)
	if err != nil {
		return err
	}
	if stripped := stripStagedSQLStringLiterals(pipe.definition); stripped != pipe.definition {
		return errors.New("managed staged Snowflake pipe definition must not contain comments or string literals")
	}
	normalizedDefinition := normalizeStagedPipeSQL(pipe.definition)
	normalizedTarget := normalizeStagedPipeSQL(managedSnowflakeStagedQualifiedTable(cfg, cfg.landingTable))
	normalizedStage := normalizeStagedPipeSQL(managedSnowflakeStagedQualified(cfg, cfg.stage))
	expectedSource := "copyinto" + normalizedTarget + "from@" + normalizedStage + "/wallaby_staged_append_v1/"
	if strings.Count(normalizedDefinition, "copyinto") != 1 || !strings.HasPrefix(normalizedDefinition, expectedSource) || strings.Count(normalizedDefinition, "from@"+normalizedStage+"/wallaby_staged_append_v1/") != 1 {
		return errors.New("managed staged Snowflake pipe must COPY from the exact @stage/wallaby_staged_append_v1/ root into the exact landing table")
	}
	if strings.Contains(normalizedDefinition, ";") {
		return errors.New("managed staged Snowflake pipe definition contains an extra statement")
	}
	// A pipe that references a named file format reopens exactly the mutation
	// window the synchronous COPY closed by inlining its parsing options, so the
	// pipe definition must inline the same options instead.
	if stagedFileFormatDefinitionOptionPresent(pipe.definition, "FORMAT_NAME") {
		return errors.New("managed staged Snowflake auto-ingest pipe must inline its JSON format options instead of referencing FORMAT_NAME")
	}
	for name, value := range plan.formatOptions {
		if !stagedFileFormatDefinitionOption(pipe.definition, name, value) {
			return fmt.Errorf("managed staged Snowflake auto-ingest pipe definition must inline %s = %s", name, value)
		}
	}
	if pipe.onError != plan.loadOptions["ON_ERROR"] {
		return fmt.Errorf("managed staged Snowflake auto-ingest pipe COPY must set ON_ERROR = %s (fail-closed), got %q", plan.loadOptions["ON_ERROR"], pipe.onError)
	}
	if pipe.force != "FALSE" {
		return fmt.Errorf("managed staged Snowflake auto-ingest pipe COPY must set FORCE = FALSE, got %q", pipe.force)
	}
	if pipe.purge != "FALSE" {
		return fmt.Errorf("managed staged Snowflake auto-ingest pipe COPY must set PURGE = FALSE, got %q", pipe.purge)
	}
	if pipe.matchByColumnName != plan.loadOptions["MATCH_BY_COLUMN_NAME"] {
		return fmt.Errorf("managed staged Snowflake auto-ingest pipe COPY must set MATCH_BY_COLUMN_NAME = %s, got %q", plan.loadOptions["MATCH_BY_COLUMN_NAME"], pipe.matchByColumnName)
	}
	if pipe.ownerRole != cfg.ownerRole {
		return fmt.Errorf("managed staged Snowflake pipe must be owned by %s, got %s", cfg.ownerRole, pipe.ownerRole)
	}
	if pipe.createdOn != cfg.pipeCreatedOn {
		return fmt.Errorf("managed staged Snowflake pipe creation identity %q differs from configured %q", pipe.createdOn, cfg.pipeCreatedOn)
	}
	if pipe.comment != managedStagedOwnershipComment(cfg, "pipe") {
		return errors.New("managed staged Snowflake pipe ownership comment differs from destination revision and schema contract")
	}
	return validateManagedStagedGrants(cfg, pipe.grants, []string{"MONITOR", "OPERATE"}, []string{"OWNERSHIP"})
}

// validateManagedStagedGrants enforces that the execution role holds exactly the
// admitted privileges, the owner role holds exactly ownership (plus any inherent
// object privileges), and no other role holds a mutating privilege.
func validateManagedStagedGrants(cfg stagedConfig, grants map[string][]string, executionExpected, ownerExpected []string) error {
	execution := append([]string(nil), grants[cfg.executionRole]...)
	sort.Strings(execution)
	expected := append([]string(nil), executionExpected...)
	sort.Strings(expected)
	if !reflect.DeepEqual(execution, expected) {
		return fmt.Errorf("execution role %s privileges=%v, want %v", cfg.executionRole, execution, expected)
	}
	owner := append([]string(nil), grants[cfg.ownerRole]...)
	sort.Strings(owner)
	wantOwner := append([]string(nil), ownerExpected...)
	sort.Strings(wantOwner)
	if !reflect.DeepEqual(owner, wantOwner) {
		return fmt.Errorf("owner role %s privileges=%v, want %v", cfg.ownerRole, owner, wantOwner)
	}
	for role, privileges := range grants {
		if role == cfg.executionRole || role == cfg.ownerRole {
			continue
		}
		for _, privilege := range privileges {
			switch privilege {
			case "DELETE", "INSERT", "OWNERSHIP", "TRUNCATE", "UPDATE", "WRITE", "OPERATE":
				return fmt.Errorf("additional privileged role %s holds %s", role, privilege)
			}
		}
	}
	return nil
}

func validateManagedStagedColumns(object string, actual, expected map[string]managedColumnSnapshot) error {
	for name, want := range expected {
		got, present := actual[name]
		if !present {
			return fmt.Errorf("managed staged Snowflake %s column %s is missing", object, name)
		}
		if !managedSnowflakeTypesEquivalent(got.dataType, want.dataType) || got.nullable != want.nullable || got.generated || got.hasDefault {
			return fmt.Errorf("managed staged Snowflake %s column %s contract differs (type=%q nullable=%t generated=%t default=%t)", object, name, got.dataType, got.nullable, got.generated, got.hasDefault)
		}
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("managed staged Snowflake %s column count=%d, want %d", object, len(actual), len(expected))
	}
	return nil
}

func stagedExpectedTargetColumns() map[string]managedColumnSnapshot {
	text := managedColumnSnapshot{dataType: "VARCHAR", nullable: false}
	number := managedColumnSnapshot{dataType: "NUMBER(38,0)", nullable: false}
	variant := managedColumnSnapshot{dataType: "VARIANT", nullable: true}
	return map[string]managedColumnSnapshot{
		"FLOW_ID": text, "FLOW_INCARNATION_ID": text, "SOURCE_LINEAGE_ID": text, "DESTINATION_REVISION_ID": text,
		"LOGICAL_BATCH_ID": text, "CONTENT_HASH": text, "SOURCE_POSITION": text, "TRANSACTION_ID": number,
		"BEGIN_LSN": text, "COMMIT_LSN": text, "END_LSN": text, "FRAGMENT_ORDINAL": number, "RECORD_ORDINAL": number,
		"SOURCE_NAMESPACE": text, "SOURCE_TABLE": text, "SCHEMA_CONTRACT_HASH": text, "OPERATION": text,
		"TOMBSTONE": {dataType: "BOOLEAN", nullable: false},
		"KEY_JSON":  variant, "BEFORE_IMAGE": variant, "AFTER_IMAGE": variant,
		"EVENT_TIME":  {dataType: "TIMESTAMP_TZ", nullable: false},
		"RECORD_HASH": text,
	}
}

func stagedExpectedReceiptColumns() map[string]managedColumnSnapshot {
	text := managedColumnSnapshot{dataType: "VARCHAR", nullable: false}
	number := managedColumnSnapshot{dataType: "NUMBER(38,0)", nullable: false}
	columns := map[string]managedColumnSnapshot{
		"RECEIPT_KIND": text, "PROFILE_VERSION": text, "FLOW_ID": text, "FLOW_INCARNATION_ID": text, "SOURCE_LINEAGE_ID": text,
		"DESTINATION_REVISION_ID": text, "LOGICAL_BATCH_ID": text, "POSITION_ID": text, "CONTENT_HASH": text, "SCHEMA_CONTRACT_HASH": text,
		"CATALOG_FINGERPRINT": text, "PROVISION_EPOCH": number, "MANIFEST_HASH": text, "PLAN_HASH": text, "EXTERNAL_ID": text, "GENERATION": number, "ACQUISITION_ID": text, "LEASE_EPOCH": number,
		"TRANSACTION_ID": number, "FRAGMENT_COUNT": number, "RECORD_COUNT": number, "STAGE_NAME": text, "STAGE_PATH": text, "FILE_CONTENT_HASH": text,
		"FILE_MD5": text, "LOAD_ROW_COUNT": number, "LOAD_STATUS": text,
		"COMMITTED_AT": {dataType: "TIMESTAMP_TZ", nullable: false},
	}
	return columns
}

func managedStagedOwnershipComment(cfg stagedConfig, objectKind string) string {
	flowDigest := sha256.Sum256([]byte(cfg.flowID))
	return strings.Join([]string{"wallaby", cfg.profile, objectKind, cfg.destinationRevision, cfg.schemaContractHash, hex.EncodeToString(flowDigest[:])}, ":")
}

func managedStagedCatalogFingerprint(catalog managedStagedCatalogSnapshot) (string, error) {
	type fingerprintColumn struct {
		DataType   string `json:"data_type"`
		Nullable   bool   `json:"nullable"`
		HasDefault bool   `json:"has_default"`
		Generated  bool   `json:"generated"`
	}
	type fingerprintConstraint struct {
		Name           string   `json:"name"`
		ConstraintType string   `json:"constraint_type"`
		Enforced       bool     `json:"enforced"`
		Columns        []string `json:"columns"`
	}
	type fingerprintFileFormatProperty struct {
		PropertyType    string `json:"property_type"`
		PropertyValue   string `json:"property_value"`
		PropertyDefault string `json:"property_default"`
	}
	canonicalGrants := func(grants map[string][]string) map[string][]string {
		result := make(map[string][]string, len(grants))
		for role, privileges := range grants {
			sorted := append([]string(nil), privileges...)
			sort.Strings(sorted)
			result[role] = sorted
		}
		return result
	}
	canonicalTable := func(table managedTableSnapshot) any {
		columns := make(map[string]fingerprintColumn, len(table.columns))
		for name, column := range table.columns {
			columns[name] = fingerprintColumn{DataType: column.dataType, Nullable: column.nullable, HasDefault: column.hasDefault, Generated: column.generated}
		}
		constraints := make([]fingerprintConstraint, 0, len(table.constraints))
		for _, constraint := range table.constraints {
			constraints = append(constraints, fingerprintConstraint{Name: constraint.name, ConstraintType: constraint.constraintType, Enforced: constraint.enforced, Columns: append([]string(nil), constraint.columns...)})
		}
		return struct {
			Kind        string                       `json:"kind"`
			OwnerRole   string                       `json:"owner_role"`
			CreatedOn   string                       `json:"created_on"`
			Comment     string                       `json:"comment"`
			Definition  string                       `json:"definition"`
			Columns     map[string]fingerprintColumn `json:"columns"`
			Constraints []fingerprintConstraint      `json:"constraints"`
			Grants      map[string][]string          `json:"grants"`
		}{Kind: table.kind, OwnerRole: table.ownerRole, CreatedOn: table.createdOn, Comment: table.comment, Definition: table.definition, Columns: columns, Constraints: constraints, Grants: canonicalGrants(table.grants)}
	}
	fileFormatProperties := make(map[string]fingerprintFileFormatProperty, len(catalog.fileFormat.properties))
	for name, property := range catalog.fileFormat.properties {
		fileFormatProperties[name] = fingerprintFileFormatProperty{
			PropertyType: property.propertyType, PropertyValue: property.propertyValue, PropertyDefault: property.propertyDefault,
		}
	}
	encoded, err := json.Marshal(struct {
		Stage struct {
			Kind      string              `json:"kind"`
			OwnerRole string              `json:"owner_role"`
			CreatedOn string              `json:"created_on"`
			Comment   string              `json:"comment"`
			Grants    map[string][]string `json:"grants"`
		} `json:"stage"`
		FileFormat struct {
			FormatType string                                   `json:"format_type"`
			OwnerRole  string                                   `json:"owner_role"`
			CreatedOn  string                                   `json:"created_on"`
			Comment    string                                   `json:"comment"`
			Definition string                                   `json:"definition"`
			Properties map[string]fingerprintFileFormatProperty `json:"properties"`
			Grants     map[string][]string                      `json:"grants"`
		} `json:"file_format"`
		Target         any `json:"target"`
		Receipts       any `json:"receipts"`
		Landing        any `json:"landing"`
		Authority      any `json:"authority"`
		TargetManifest any `json:"target_manifest"`
		Pipe           struct {
			Present           bool                `json:"present"`
			Definition        string              `json:"definition"`
			OwnerRole         string              `json:"owner_role"`
			CreatedOn         string              `json:"created_on"`
			AutoIngest        bool                `json:"auto_ingest"`
			OnError           string              `json:"on_error"`
			Force             string              `json:"force"`
			Purge             string              `json:"purge"`
			MatchByColumnName string              `json:"match_by_column_name"`
			Comment           string              `json:"comment"`
			Grants            map[string][]string `json:"grants"`
		} `json:"pipe"`
		TaskCount           int `json:"task_count"`
		UnexpectedPipeCount int `json:"unexpected_pipe_count"`
	}{
		Stage: struct {
			Kind      string              `json:"kind"`
			OwnerRole string              `json:"owner_role"`
			CreatedOn string              `json:"created_on"`
			Comment   string              `json:"comment"`
			Grants    map[string][]string `json:"grants"`
		}{Kind: catalog.stage.kind, OwnerRole: catalog.stage.ownerRole, CreatedOn: catalog.stage.createdOn, Comment: catalog.stage.comment, Grants: canonicalGrants(catalog.stage.grants)},
		FileFormat: struct {
			FormatType string                                   `json:"format_type"`
			OwnerRole  string                                   `json:"owner_role"`
			CreatedOn  string                                   `json:"created_on"`
			Comment    string                                   `json:"comment"`
			Definition string                                   `json:"definition"`
			Properties map[string]fingerprintFileFormatProperty `json:"properties"`
			Grants     map[string][]string                      `json:"grants"`
		}{FormatType: catalog.fileFormat.formatType, OwnerRole: catalog.fileFormat.ownerRole, CreatedOn: catalog.fileFormat.createdOn, Comment: catalog.fileFormat.comment, Definition: catalog.fileFormat.definition, Properties: fileFormatProperties, Grants: canonicalGrants(catalog.fileFormat.grants)},
		Target:         canonicalTable(catalog.target),
		Receipts:       canonicalTable(catalog.receipts),
		Landing:        canonicalTable(catalog.landing),
		Authority:      canonicalTable(catalog.authority),
		TargetManifest: canonicalTable(catalog.targetManifest),
		Pipe: struct {
			Present           bool                `json:"present"`
			Definition        string              `json:"definition"`
			OwnerRole         string              `json:"owner_role"`
			CreatedOn         string              `json:"created_on"`
			AutoIngest        bool                `json:"auto_ingest"`
			OnError           string              `json:"on_error"`
			Force             string              `json:"force"`
			Purge             string              `json:"purge"`
			MatchByColumnName string              `json:"match_by_column_name"`
			Comment           string              `json:"comment"`
			Grants            map[string][]string `json:"grants"`
		}{Present: catalog.pipe.present, Definition: catalog.pipe.definition, OwnerRole: catalog.pipe.ownerRole, CreatedOn: catalog.pipe.createdOn, AutoIngest: catalog.pipe.autoIngest, OnError: catalog.pipe.onError, Force: catalog.pipe.force, Purge: catalog.pipe.purge, MatchByColumnName: catalog.pipe.matchByColumnName, Comment: catalog.pipe.comment, Grants: canonicalGrants(catalog.pipe.grants)},
		TaskCount:           catalog.taskCount,
		UnexpectedPipeCount: catalog.unexpectedPipeCount,
	})
	if err != nil {
		return "", fmt.Errorf("encode managed staged Snowflake catalog fingerprint: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// loadManagedStagedCatalog reads the live staged catalog. It is exercised only by
// the credential-gated live matrix; the validation and fingerprint above are
// covered by deterministic unit tests.
func (d *Destination) loadManagedStagedCatalog(ctx context.Context, queryer managedSnowflakeCatalogQueryer) (managedStagedCatalogSnapshot, error) {
	cfg := d.stagedConfig
	informationSchema := quoteIdent(cfg.database, '"') + ".INFORMATION_SCHEMA."
	snapshot := managedStagedCatalogSnapshot{}
	var err error
	if snapshot.stage, err = loadStagedStage(ctx, queryer, cfg); err != nil {
		return managedStagedCatalogSnapshot{}, err
	}
	if snapshot.fileFormat, err = loadStagedFileFormat(ctx, queryer, cfg, informationSchema); err != nil {
		return managedStagedCatalogSnapshot{}, err
	}
	if snapshot.target, err = loadStagedTable(ctx, queryer, cfg, informationSchema, cfg.table); err != nil {
		return managedStagedCatalogSnapshot{}, fmt.Errorf("inspect managed staged Snowflake target: %w", err)
	}
	if snapshot.receipts, err = loadStagedTable(ctx, queryer, cfg, informationSchema, cfg.receiptsTable); err != nil {
		return managedStagedCatalogSnapshot{}, fmt.Errorf("inspect managed staged Snowflake receipts: %w", err)
	}
	if snapshot.landing, err = loadStagedTable(ctx, queryer, cfg, informationSchema, cfg.landingTable); err != nil {
		return managedStagedCatalogSnapshot{}, fmt.Errorf("inspect managed staged Snowflake landing table: %w", err)
	}
	if snapshot.authority, err = loadStagedTable(ctx, queryer, cfg, informationSchema, cfg.authorityTable); err != nil {
		return managedStagedCatalogSnapshot{}, fmt.Errorf("inspect managed staged Snowflake authority table: %w", err)
	}
	if snapshot.targetManifest, err = loadStagedTable(ctx, queryer, cfg, informationSchema, cfg.targetManifestTable); err != nil {
		return managedStagedCatalogSnapshot{}, fmt.Errorf("inspect managed staged Snowflake target manifest table: %w", err)
	}
	if cfg.autoIngest {
		if snapshot.pipe, err = loadStagedPipe(ctx, queryer, cfg, informationSchema); err != nil {
			return managedStagedCatalogSnapshot{}, err
		}
	}
	if err := queryer.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+informationSchema+"PIPES WHERE PIPE_SCHEMA = ?", cfg.schema).Scan(&snapshot.unexpectedPipeCount); err != nil {
		return managedStagedCatalogSnapshot{}, fmt.Errorf("count managed staged Snowflake schema pipes: %w", err)
	}
	// #nosec G202 -- the database identifier is one validated unquoted uppercase identifier.
	if err := queryer.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM TABLE("+quoteIdent(cfg.database, '"')+".INFORMATION_SCHEMA.TASKS()) WHERE SCHEMA_NAME = ?",
		cfg.schema,
	).Scan(&snapshot.taskCount); err != nil {
		return managedStagedCatalogSnapshot{}, fmt.Errorf("inspect managed staged Snowflake schema tasks: %w", err)
	}
	return snapshot, nil
}

func loadStagedStage(ctx context.Context, queryer managedSnowflakeCatalogQueryer, cfg stagedConfig) (managedStageSnapshot, error) {
	informationSchema := quoteIdent(cfg.database, '"') + ".INFORMATION_SCHEMA."
	snapshot := managedStageSnapshot{grants: make(map[string][]string)}
	// #nosec G202 -- the database identifier is one validated unquoted uppercase identifier.
	if err := queryer.QueryRowContext(ctx,
		"SELECT STAGE_TYPE, COALESCE(COMMENT, ''), TO_VARCHAR(CREATED, '"+managedSnowflakeCatalogTimestampFormat+"') FROM "+informationSchema+"STAGES WHERE STAGE_SCHEMA = ? AND STAGE_NAME = ?",
		cfg.schema, cfg.stage,
	).Scan(&snapshot.kind, &snapshot.comment, &snapshot.createdOn); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return managedStageSnapshot{}, fmt.Errorf("stage %s.%s.%s does not exist", cfg.database, cfg.schema, cfg.stage)
		}
		return managedStageSnapshot{}, fmt.Errorf("inspect managed staged Snowflake stage: %w", err)
	}
	owner, grants, err := loadStagedGrants(ctx, queryer, "STAGE", managedSnowflakeStagedQualified(cfg, cfg.stage))
	if err != nil {
		return managedStageSnapshot{}, err
	}
	snapshot.ownerRole = owner
	snapshot.grants = grants
	return snapshot, nil
}

func loadStagedFileFormat(ctx context.Context, queryer managedSnowflakeCatalogQueryer, cfg stagedConfig, informationSchema string) (managedFileFormatSnapshot, error) {
	snapshot := managedFileFormatSnapshot{grants: make(map[string][]string)}
	// #nosec G202 -- the database identifier is one validated unquoted uppercase identifier.
	if err := queryer.QueryRowContext(ctx,
		"SELECT FILE_FORMAT_TYPE, COALESCE(COMMENT, ''), TO_VARCHAR(CREATED, '"+managedSnowflakeCatalogTimestampFormat+"') FROM "+informationSchema+"FILE_FORMATS WHERE FILE_FORMAT_SCHEMA = ? AND FILE_FORMAT_NAME = ?",
		cfg.schema, cfg.fileFormat,
	).Scan(&snapshot.formatType, &snapshot.comment, &snapshot.createdOn); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return managedFileFormatSnapshot{}, fmt.Errorf("file format %s.%s.%s does not exist", cfg.database, cfg.schema, cfg.fileFormat)
		}
		return managedFileFormatSnapshot{}, fmt.Errorf("inspect managed staged Snowflake file format: %w", err)
	}
	qualified := managedSnowflakeStagedQualified(cfg, cfg.fileFormat)
	if err := queryer.QueryRowContext(ctx, "SELECT LEFT(GET_DDL('FILE_FORMAT', ?), 65537)", qualified).Scan(&snapshot.definition); err != nil {
		return managedFileFormatSnapshot{}, fmt.Errorf("read managed staged Snowflake file format definition: %w", err)
	}
	if len(snapshot.definition) > 64<<10 {
		return managedFileFormatSnapshot{}, errors.New("managed staged Snowflake file format definition exceeds 64 KiB")
	}
	properties, err := loadStagedFileFormatProperties(ctx, queryer, qualified)
	if err != nil {
		return managedFileFormatSnapshot{}, err
	}
	snapshot.properties = properties
	owner, grants, err := loadStagedGrants(ctx, queryer, "FILE FORMAT", qualified)
	if err != nil {
		return managedFileFormatSnapshot{}, err
	}
	snapshot.ownerRole = owner
	snapshot.grants = grants
	return snapshot, nil
}

func loadStagedFileFormatProperties(ctx context.Context, queryer managedSnowflakeCatalogQueryer, qualified string) (map[string]managedFileFormatPropertySnapshot, error) {
	rows, err := queryer.QueryContext(ctx, "DESCRIBE FILE FORMAT "+qualified)
	if err != nil {
		return nil, fmt.Errorf("describe managed staged Snowflake file format: %w", err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read file format property columns: %w", err)
	}
	if len(columns) != 4 {
		return nil, fmt.Errorf("snowflake DESCRIBE FILE FORMAT returned %d columns, want exactly 4", len(columns))
	}
	for _, column := range columns {
		if len(column) == 0 || len(column) > 64 {
			return nil, errors.New("snowflake DESCRIBE FILE FORMAT returned an invalid column label")
		}
	}
	index := make(map[string]int, len(columns))
	for position, column := range columns {
		index[strings.ToLower(strings.TrimSpace(column))] = position
	}
	propertyIndex, hasProperty := index["property"]
	typeIndex, hasType := index["property_type"]
	valueIndex, hasValue := index["property_value"]
	defaultIndex, hasDefault := index["property_default"]
	if !hasProperty || !hasType || !hasValue || !hasDefault {
		return nil, errors.New("snowflake DESCRIBE FILE FORMAT omitted required property columns")
	}
	properties := make(map[string]managedFileFormatPropertySnapshot)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for position := range values {
			pointers[position] = &values[position]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan managed staged Snowflake file format property: %w", err)
		}
		name := strings.ToUpper(strings.TrimSpace(sqlValueString(values[propertyIndex])))
		property := managedFileFormatPropertySnapshot{
			propertyType: strings.TrimSpace(sqlValueString(values[typeIndex])), propertyValue: strings.TrimSpace(sqlValueString(values[valueIndex])),
			propertyDefault: strings.TrimSpace(sqlValueString(values[defaultIndex])),
		}
		if name == "" {
			return nil, errors.New("snowflake DESCRIBE FILE FORMAT returned an empty property name")
		}
		if len(name) > 128 || len(property.propertyType) > 128 || len(property.propertyValue) > 4096 || len(property.propertyDefault) > 4096 {
			return nil, fmt.Errorf("snowflake file format property %s exceeds bounded catalog limits", name)
		}
		if _, duplicate := properties[name]; duplicate {
			return nil, fmt.Errorf("snowflake DESCRIBE FILE FORMAT repeated property %s", name)
		}
		properties[name] = property
		if len(properties) > 64 {
			return nil, errors.New("snowflake DESCRIBE FILE FORMAT exceeded 64 properties")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate managed staged Snowflake file format properties: %w", err)
	}
	return properties, nil
}

func loadStagedPipe(ctx context.Context, queryer managedSnowflakeCatalogQueryer, cfg stagedConfig, informationSchema string) (managedPipeSnapshot, error) {
	snapshot := managedPipeSnapshot{grants: make(map[string][]string)}
	var definition, comment, createdOn string
	var autoIngest bool
	// #nosec G202 -- the database identifier is one validated unquoted uppercase identifier.
	if err := queryer.QueryRowContext(ctx,
		"SELECT DEFINITION, COALESCE(COMMENT, ''), TO_VARCHAR(CREATED, '"+managedSnowflakeCatalogTimestampFormat+"'), AUTO_INGEST FROM "+informationSchema+"PIPES WHERE PIPE_SCHEMA = ? AND PIPE_NAME = ?",
		cfg.schema, cfg.pipe,
	).Scan(&definition, &comment, &createdOn, &autoIngest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return managedPipeSnapshot{}, nil
		}
		return managedPipeSnapshot{}, fmt.Errorf("inspect managed staged Snowflake pipe: %w", err)
	}
	if len(definition) > 64<<10 {
		return managedPipeSnapshot{}, errors.New("managed staged Snowflake pipe definition exceeds 64 KiB")
	}
	snapshot.present = true
	snapshot.definition = definition
	snapshot.comment = comment
	snapshot.createdOn = createdOn
	snapshot.autoIngest = autoIngest
	snapshot.onError = stagedPipeCopyOption(definition, "ON_ERROR")
	snapshot.force = stagedPipeCopyOption(definition, "FORCE")
	snapshot.purge = stagedPipeCopyOption(definition, "PURGE")
	snapshot.matchByColumnName = stagedPipeCopyOption(definition, "MATCH_BY_COLUMN_NAME")
	owner, grants, err := loadStagedGrants(ctx, queryer, "PIPE", managedSnowflakeStagedQualified(cfg, cfg.pipe))
	if err != nil {
		return managedPipeSnapshot{}, err
	}
	snapshot.ownerRole = owner
	snapshot.grants = grants
	return snapshot, nil
}

// stagedPipeCopyOption extracts one COPY option token from a pipe DEFINITION so
// admission can enforce the same fail-closed load semantics on the auto-ingest
// pipe that newStagedCopyPlan enforces on the synchronous COPY. It returns the
// uppercased token (any surrounding quote stripped) or "" when the option is
// absent.
func stagedPipeCopyOption(definition, option string) string {
	const spaces = " \t\r\n"
	upper := strings.ToUpper(definition)
	optionUpper := strings.ToUpper(option)
	identifierByte := func(character byte) bool {
		return character == '_' || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9')
	}
	for searchFrom := 0; searchFrom <= len(upper); {
		offset := strings.Index(upper[searchFrom:], optionUpper)
		if offset < 0 {
			return ""
		}
		start := searchFrom + offset
		searchFrom = start + len(optionUpper)
		// Reject a match embedded in a larger identifier (e.g. ENFORCE vs FORCE).
		if start > 0 && identifierByte(upper[start-1]) {
			continue
		}
		rest := strings.TrimLeft(upper[start+len(optionUpper):], spaces)
		if !strings.HasPrefix(rest, "=") {
			continue
		}
		rest = strings.TrimPrefix(strings.TrimLeft(strings.TrimPrefix(rest, "="), spaces), "'")
		end := 0
		for end < len(rest) && identifierByte(rest[end]) {
			end++
		}
		return rest[:end]
	}
	return ""
}

func loadStagedTable(ctx context.Context, queryer managedSnowflakeCatalogQueryer, cfg stagedConfig, informationSchema, table string) (managedTableSnapshot, error) {
	snapshot := managedTableSnapshot{columns: make(map[string]managedColumnSnapshot), grants: make(map[string][]string)}
	var isHybrid string
	// #nosec G202 -- the database identifier is one validated unquoted uppercase identifier.
	if err := queryer.QueryRowContext(ctx,
		"SELECT IS_HYBRID, COALESCE(COMMENT, ''), TO_VARCHAR(CREATED, '"+managedSnowflakeCatalogTimestampFormat+"') FROM "+informationSchema+"TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		cfg.schema, table,
	).Scan(&isHybrid, &snapshot.comment, &snapshot.createdOn); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return managedTableSnapshot{}, fmt.Errorf("table %s.%s.%s does not exist", cfg.database, cfg.schema, table)
		}
		return managedTableSnapshot{}, err
	}
	if strings.EqualFold(isHybrid, "YES") {
		snapshot.kind = "HYBRID TABLE"
	} else {
		snapshot.kind = "TABLE"
	}
	qualified := managedSnowflakeStagedQualified(cfg, table)
	if err := queryer.QueryRowContext(ctx, "SELECT LEFT(GET_DDL('TABLE', ?), 65537)", qualified).Scan(&snapshot.definition); err != nil {
		return managedTableSnapshot{}, fmt.Errorf("read managed staged Snowflake table definition: %w", err)
	}
	if len(snapshot.definition) == 0 || len(snapshot.definition) > 64<<10 {
		return managedTableSnapshot{}, fmt.Errorf("managed staged Snowflake table definition has invalid length %d", len(snapshot.definition))
	}
	owner, grants, err := loadStagedGrants(ctx, queryer, "TABLE", managedSnowflakeStagedQualified(cfg, table))
	if err != nil {
		return managedTableSnapshot{}, err
	}
	snapshot.ownerRole = owner
	snapshot.grants = grants
	// #nosec G202 -- the database identifier is one validated unquoted uppercase identifier.
	rows, err := queryer.QueryContext(ctx, `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, IS_IDENTITY,
       NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_MAXIMUM_LENGTH
FROM `+informationSchema+`COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`, cfg.schema, table)
	if err != nil {
		return managedTableSnapshot{}, fmt.Errorf("query staged columns: %w", err)
	}
	for rows.Next() {
		var name, dataType, nullable, identity string
		var defaultValue sql.NullString
		var precision, scale, datetimePrecision, characterMaximumLength sql.NullInt64
		if err := rows.Scan(&name, &dataType, &nullable, &defaultValue, &identity, &precision, &scale, &datetimePrecision, &characterMaximumLength); err != nil {
			_ = rows.Close()
			return managedTableSnapshot{}, fmt.Errorf("scan staged column: %w", err)
		}
		if strings.EqualFold(dataType, "NUMBER") && precision.Valid && scale.Valid {
			dataType = fmt.Sprintf("NUMBER(%d,%d)", precision.Int64, scale.Int64)
		} else if strings.HasPrefix(strings.ToUpper(dataType), "TIMESTAMP_") && datetimePrecision.Valid {
			dataType = fmt.Sprintf("%s(%d)", dataType, datetimePrecision.Int64)
		}
		canonicalName, err := canonicalManagedSnowflakeCatalogIdentifier(name)
		if err != nil {
			_ = rows.Close()
			return managedTableSnapshot{}, fmt.Errorf("staged catalog column: %w", err)
		}
		snapshot.columns[canonicalName] = managedColumnSnapshot{
			dataType: dataType, characterMaximumLength: characterMaximumLength.Int64, datetimePrecision: datetimePrecision.Int64,
			nullable: strings.EqualFold(nullable, "YES"), hasDefault: defaultValue.Valid, generated: strings.EqualFold(identity, "YES"),
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return managedTableSnapshot{}, fmt.Errorf("iterate staged columns: %w", err)
	}
	if err := rows.Close(); err != nil {
		return managedTableSnapshot{}, fmt.Errorf("close staged columns: %w", err)
	}
	constraints, err := loadStagedConstraints(ctx, queryer, cfg, informationSchema, table)
	if err != nil {
		return managedTableSnapshot{}, err
	}
	snapshot.constraints = constraints
	// #nosec G202 -- the database identifier is one validated unquoted uppercase identifier.
	if err := queryer.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+informationSchema+"TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE NOT IN ('PRIMARY KEY','UNIQUE')", cfg.schema, table).Scan(&snapshot.otherConstraintCount); err != nil {
		return managedTableSnapshot{}, fmt.Errorf("query staged unsupported constraints: %w", err)
	}
	return snapshot, nil
}

func loadStagedGrants(ctx context.Context, queryer managedSnowflakeCatalogQueryer, objectKind, qualified string) (string, map[string][]string, error) {
	rows, err := queryer.QueryContext(ctx, "SHOW GRANTS ON "+objectKind+" "+qualified)
	if err != nil {
		return "", nil, fmt.Errorf("show %s grants: %w", strings.ToLower(objectKind), err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return "", nil, fmt.Errorf("read grant columns: %w", err)
	}
	index := make(map[string]int, len(columns))
	for position, column := range columns {
		index[strings.ToLower(column)] = position
	}
	privilegeIndex, hasPrivilege := index["privilege"]
	granteeIndex, hasGrantee := index["grantee_name"]
	if !hasPrivilege || !hasGrantee {
		return "", nil, errors.New("snowflake SHOW GRANTS omitted privilege or grantee_name")
	}
	owners := make(map[string]struct{})
	grants := make(map[string][]string)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for position := range values {
			pointers[position] = &values[position]
		}
		if err := rows.Scan(pointers...); err != nil {
			return "", nil, fmt.Errorf("scan grant: %w", err)
		}
		role, err := canonicalManagedSnowflakeCatalogIdentifier(sqlValueString(values[granteeIndex]))
		if err != nil {
			return "", nil, fmt.Errorf("catalog grantee role: %w", err)
		}
		privilege := strings.ToUpper(strings.TrimSpace(sqlValueString(values[privilegeIndex])))
		if privilege == "" {
			return "", nil, errors.New("snowflake SHOW GRANTS returned an empty privilege")
		}
		for _, existing := range grants[role] {
			if existing == privilege {
				return "", nil, fmt.Errorf("snowflake SHOW GRANTS repeated %s for role %s", privilege, role)
			}
		}
		grants[role] = append(grants[role], privilege)
		if privilege == "OWNERSHIP" {
			owners[role] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return "", nil, fmt.Errorf("iterate grants: %w", err)
	}
	if len(owners) != 1 {
		return "", nil, fmt.Errorf("object has %d ownership grants, want exactly one", len(owners))
	}
	for role := range grants {
		sort.Strings(grants[role])
	}
	for owner := range owners {
		return owner, grants, nil
	}
	return "", nil, errors.New("object ownership grant is absent")
}

func loadStagedConstraints(ctx context.Context, queryer managedSnowflakeCatalogQueryer, cfg stagedConfig, informationSchema, table string) ([]managedConstraintSnapshot, error) {
	// #nosec G202 -- the database identifier is one validated unquoted uppercase identifier.
	rows, err := queryer.QueryContext(ctx, `SELECT TC.CONSTRAINT_NAME, TC.CONSTRAINT_TYPE, TC.ENFORCED, KCU.COLUMN_NAME, KCU.ORDINAL_POSITION
FROM `+informationSchema+`TABLE_CONSTRAINTS AS TC
JOIN `+informationSchema+`KEY_COLUMN_USAGE AS KCU
  ON KCU.CONSTRAINT_CATALOG = TC.CONSTRAINT_CATALOG AND KCU.CONSTRAINT_SCHEMA = TC.CONSTRAINT_SCHEMA AND KCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
WHERE TC.TABLE_SCHEMA = ? AND TC.TABLE_NAME = ? AND TC.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
ORDER BY TC.CONSTRAINT_NAME, KCU.ORDINAL_POSITION`, cfg.schema, table)
	if err != nil {
		return nil, fmt.Errorf("query staged enforced constraints: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type namedConstraint struct {
		name     string
		kind     string
		enforced bool
		columns  map[int]string
	}
	byName := make(map[string]*namedConstraint)
	for rows.Next() {
		var name, kind string
		var enforced any
		var column string
		var ordinal int
		if err := rows.Scan(&name, &kind, &enforced, &column, &ordinal); err != nil {
			return nil, fmt.Errorf("scan staged constraint: %w", err)
		}
		canonicalName, err := canonicalManagedSnowflakeCatalogIdentifier(name)
		if err != nil {
			return nil, fmt.Errorf("staged constraint name: %w", err)
		}
		constraint := byName[canonicalName]
		if constraint == nil {
			constraint = &namedConstraint{name: canonicalName, kind: kind, enforced: sqlValueBool(enforced), columns: make(map[int]string)}
			byName[canonicalName] = constraint
		}
		canonicalColumn, err := canonicalManagedSnowflakeCatalogIdentifier(column)
		if err != nil {
			return nil, fmt.Errorf("staged constraint %s column: %w", name, err)
		}
		constraint.columns[ordinal] = canonicalColumn
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate staged constraints: %w", err)
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]managedConstraintSnapshot, 0, len(names))
	for _, name := range names {
		constraint := byName[name]
		ordinals := make([]int, 0, len(constraint.columns))
		for ordinal := range constraint.columns {
			ordinals = append(ordinals, ordinal)
		}
		sort.Ints(ordinals)
		orderedColumns := make([]string, 0, len(ordinals))
		for _, ordinal := range ordinals {
			orderedColumns = append(orderedColumns, constraint.columns[ordinal])
		}
		result = append(result, managedConstraintSnapshot{name: constraint.name, constraintType: constraint.kind, enforced: constraint.enforced, columns: orderedColumns})
	}
	return result, nil
}
