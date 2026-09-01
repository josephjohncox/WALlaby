package snowflake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type managedStreamCatalogSnapshot struct {
	pipe      managedPipeSnapshot
	target    managedTableSnapshot
	receipts  managedTableSnapshot
	channel   managedTableSnapshot
	requests  managedTableSnapshot
	pipeCount int
	taskCount int
}

func streamCatalogStagedConfig(cfg streamConfig) stagedConfig {
	return stagedConfig{
		profile: cfg.profile, flowID: cfg.flowID, account: cfg.account, database: cfg.database, schema: cfg.schema,
		pipe: cfg.pipe, table: cfg.table, receiptsTable: cfg.receiptsTable, ownerRole: cfg.ownerRole,
		executionRole: cfg.executionRole, warehouse: cfg.warehouse, destinationRevision: cfg.destinationRevision,
		schemaContractHash: cfg.schemaContractHash, autoIngest: true, pipeCreatedOn: cfg.pipeCreatedOn,
		targetCreatedOn: cfg.targetCreatedOn, receiptsCreatedOn: cfg.receiptsCreatedOn,
	}
}

func streamCatalogManagedConfig(cfg streamConfig) managedConfig {
	return managedConfig{profile: cfg.profile, flowID: cfg.flowID, database: cfg.database, schema: cfg.schema, ownerRole: cfg.ownerRole, executionRole: cfg.executionRole}
}

func loadManagedStreamCatalog(ctx context.Context, queryer managedSnowflakeCatalogQueryer, cfg streamConfig) (managedStreamCatalogSnapshot, error) {
	shim := streamCatalogStagedConfig(cfg)
	informationSchema := quoteIdent(cfg.database, '"') + ".INFORMATION_SCHEMA."
	var result managedStreamCatalogSnapshot
	var err error
	if result.pipe, err = loadStagedPipe(ctx, queryer, shim, informationSchema); err != nil {
		return result, err
	}
	for name, target := range map[string]*managedTableSnapshot{
		cfg.table: &result.target, cfg.receiptsTable: &result.receipts,
		cfg.channelStateTable: &result.channel, cfg.channelStateTable + "_REQUESTS": &result.requests,
	} {
		*target, err = loadStagedTable(ctx, queryer, shim, informationSchema, name)
		if err != nil {
			return result, fmt.Errorf("inspect managed streaming Snowflake table %s: %w", name, err)
		}
	}
	if err := queryer.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+informationSchema+"PIPES WHERE PIPE_SCHEMA = ?", cfg.schema).Scan(&result.pipeCount); err != nil {
		return result, fmt.Errorf("count managed streaming Snowflake pipes: %w", err)
	}
	if err := queryer.QueryRowContext(ctx, "SELECT COUNT(*) FROM TABLE("+quoteIdent(cfg.database, '"')+".INFORMATION_SCHEMA.TASKS()) WHERE SCHEMA_NAME = ?", cfg.schema).Scan(&result.taskCount); err != nil {
		return result, fmt.Errorf("count managed streaming Snowflake tasks: %w", err)
	}
	return result, nil
}

func validateManagedStreamCatalog(ctx context.Context, queryer managedSnowflakeCatalogQueryer, cfg streamConfig) (string, error) {
	catalog, err := loadManagedStreamCatalog(ctx, queryer, cfg)
	if err != nil {
		return "", err
	}
	if err := validateManagedStreamCatalogSnapshot(cfg, catalog); err != nil {
		return "", err
	}
	return managedStreamCatalogFingerprint(catalog)
}

func validateManagedStreamCatalogSnapshot(cfg streamConfig, catalog managedStreamCatalogSnapshot) error {
	if catalog.taskCount != 0 || catalog.pipeCount != 1 {
		return fmt.Errorf("managed streaming Snowflake dedicated schema requires zero tasks and one pipe, got tasks=%d pipes=%d", catalog.taskCount, catalog.pipeCount)
	}
	if !catalog.pipe.present || catalog.pipe.ownerRole != cfg.ownerRole || catalog.pipe.createdOn != cfg.pipeCreatedOn {
		return errors.New("managed streaming Snowflake pipe identity differs from the admitted catalog")
	}
	if catalog.pipe.comment != managedStreamOwnershipComment(cfg, "pipe") {
		return errors.New("managed streaming Snowflake pipe ownership comment differs")
	}
	if err := validateManagedStagedGrants(streamCatalogStagedConfig(cfg), catalog.pipe.grants, []string{"MONITOR", "OPERATE"}, []string{"OWNERSHIP"}); err != nil {
		return fmt.Errorf("managed streaming Snowflake pipe grants: %w", err)
	}
	pipeWithoutAllowedLiteral := strings.ReplaceAll(strings.ReplaceAll(catalog.pipe.definition, "'STREAMING'", "STREAMING"), "'streaming'", "STREAMING")
	if stripStagedSQLStringLiterals(pipeWithoutAllowedLiteral) != pipeWithoutAllowedLiteral || strings.Contains(catalog.pipe.definition, ";") {
		return errors.New("managed streaming Snowflake pipe definition must not contain comments, unexpected string literals, or extra statements")
	}
	normalizedPipe := normalizeStagedPipeSQL(catalog.pipe.definition)
	normalizedTarget := normalizeStagedPipeSQL(managedSnowflakeStreamQualifiedTable(cfg, cfg.table))
	exactTarget := "copyinto" + normalizedTarget + "from"
	if strings.Count(normalizedPipe, "copyinto") != 1 || strings.Count(normalizedPipe, exactTarget) != 1 || !strings.Contains(normalizedPipe, "datasource(type=>\"streaming\")") && !strings.Contains(normalizedPipe, "datasource(type=>'streaming')") {
		return errors.New("managed streaming Snowflake pipe must COPY into the exact target from DATA_SOURCE(TYPE=>'STREAMING')")
	}

	managedCfg := streamCatalogManagedConfig(cfg)
	if err := validateStreamTable(cfg, managedCfg, "target", catalog.target, cfg.targetCreatedOn, false, streamChangelogColumns(), nil, []string{"SELECT"}); err != nil {
		return err
	}
	if err := validateStreamTable(cfg, managedCfg, "receipts", catalog.receipts, cfg.receiptsCreatedOn, true, append(streamReceiptColumns(), "COMMITTED_AT"), []managedConstraintSnapshot{{name: "WALLABY_STREAM_RECEIPT_PK", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"EXTERNAL_ID"}}}, []string{"DELETE", "INSERT", "SELECT", "UPDATE"}); err != nil {
		return err
	}
	channelColumns := []string{"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "CHANNEL_NAME", "PIPE_NAME", "PIPE_REVISION", "CHANNEL_REVISION", "CONTINUATION_TOKEN", "COMMITTED_OFFSET_TOKEN", "LOGICAL_BATCH_ID", "ROWS_CONTENT_HASH", "REQUEST_ID", "STATE_VERSION", "UPDATED_AT"}
	if err := validateStreamTable(cfg, managedCfg, "channel_state", catalog.channel, cfg.channelStateCreatedOn, true, channelColumns, []managedConstraintSnapshot{{name: "WALLABY_STREAM_CHANNEL_PK", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "CHANNEL_NAME"}}}, []string{"DELETE", "INSERT", "SELECT", "UPDATE"}); err != nil {
		return err
	}
	requestColumns := []string{"REQUEST_ID", "FLOW_ID", "FLOW_INCARNATION_ID", "SOURCE_LINEAGE_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID", "POSITION_ID", "CONTENT_HASH", "MANIFEST_HASH", "ROWS_CONTENT_HASH", "ROW_COUNT", "CHANNEL_NAME", "PIPE_NAME", "CHANNEL_REVISION", "PIPE_REVISION", "INPUT_CONTINUATION_TOKEN", "EXPECTED_PREVIOUS_COMMITTED_OFFSET_TOKEN", "REQUESTED_OFFSET_TOKEN", "RESPONSE_CONTINUATION_TOKEN", "COMMITTED_OFFSET_TOKEN", "GENERATION", "ACQUISITION_ID", "LEASE_EPOCH", "ATTEMPT", "PHASE", "PHASE_VERSION", "RESPONSE_KIND", "RESPONSE_EVIDENCE", "CREATED_AT", "UPDATED_AT"}
	requestConstraints := []managedConstraintSnapshot{
		{name: "WALLABY_STREAM_REQUEST_ATTEMPT", constraintType: "UNIQUE", enforced: true, columns: []string{"FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID", "ATTEMPT"}},
		{name: "WALLABY_STREAM_REQUEST_PK", constraintType: "PRIMARY KEY", enforced: true, columns: []string{"REQUEST_ID"}},
	}
	return validateStreamTable(cfg, managedCfg, "request_journal", catalog.requests, cfg.requestJournalCreatedOn, true, requestColumns, requestConstraints, []string{"DELETE", "INSERT", "SELECT", "UPDATE"})
}

func validateStreamTable(cfg streamConfig, managedCfg managedConfig, kind string, table managedTableSnapshot, createdOn string, hybrid bool, columns []string, constraints []managedConstraintSnapshot, grants []string) error {
	actualHybrid := normalizeManagedSnowflakeKind(table.kind) == "HYBRID TABLE"
	if actualHybrid != hybrid || table.ownerRole != cfg.ownerRole || table.createdOn != createdOn || table.comment != managedStreamOwnershipComment(cfg, kind) {
		return fmt.Errorf("managed streaming Snowflake %s table identity differs", kind)
	}
	if err := validateManagedSnowflakeExecutionGrants(managedCfg, table, grants); err != nil {
		return fmt.Errorf("managed streaming Snowflake %s grants: %w", kind, err)
	}
	if table.otherConstraintCount != 0 || len(table.columns) != len(columns) {
		return fmt.Errorf("managed streaming Snowflake %s schema cardinality differs", kind)
	}
	contracts := managedStreamColumnContracts(kind, columns)
	for _, column := range columns {
		value, ok := table.columns[column]
		contract := contracts[column]
		if !ok || value.nullable || value.hasDefault || value.generated || value.dataType != contract.dataType || contract.characterMaximumLength >= 0 && value.characterMaximumLength != contract.characterMaximumLength || contract.datetimePrecision >= 0 && value.datetimePrecision != contract.datetimePrecision {
			return fmt.Errorf("managed streaming Snowflake %s column %s differs", kind, column)
		}
	}
	actualConstraints := append([]managedConstraintSnapshot(nil), table.constraints...)
	sort.Slice(actualConstraints, func(i, j int) bool { return actualConstraints[i].name < actualConstraints[j].name })
	expectedConstraints := append([]managedConstraintSnapshot(nil), constraints...)
	sort.Slice(expectedConstraints, func(i, j int) bool { return expectedConstraints[i].name < expectedConstraints[j].name })
	if len(actualConstraints) != len(expectedConstraints) {
		return fmt.Errorf("managed streaming Snowflake %s constraint count differs", kind)
	}
	for index := range expectedConstraints {
		if actualConstraints[index].name != expectedConstraints[index].name || actualConstraints[index].constraintType != expectedConstraints[index].constraintType || actualConstraints[index].enforced != expectedConstraints[index].enforced || strings.Join(actualConstraints[index].columns, "\x00") != strings.Join(expectedConstraints[index].columns, "\x00") {
			return fmt.Errorf("managed streaming Snowflake %s constraint %d differs", kind, index)
		}
	}
	return nil
}

type managedStreamColumnContract struct {
	dataType               string
	characterMaximumLength int64
	datetimePrecision      int64
}

func managedStreamColumnContracts(kind string, columns []string) map[string]managedStreamColumnContract {
	contracts := make(map[string]managedStreamColumnContract, len(columns))
	for _, column := range columns {
		contracts[column] = managedStreamColumnContract{dataType: "VARCHAR", characterMaximumLength: 16 << 20, datetimePrecision: -1}
	}
	set := func(dataType string, characterLength, datetimePrecision int64, names ...string) {
		for _, name := range names {
			contracts[name] = managedStreamColumnContract{dataType: dataType, characterMaximumLength: characterLength, datetimePrecision: datetimePrecision}
		}
	}
	set("VARCHAR", 64, -1, "CONTENT_HASH", "SCHEMA_CONTRACT_HASH", "CATALOG_FINGERPRINT", "MANIFEST_HASH", "ROWS_CONTENT_HASH", "ROW_HASH")
	set("NUMBER(38,0)", -1, -1, "APPEND_ORDINAL", "TRANSACTION_ID", "FRAGMENT_ORDINAL", "RECORD_ORDINAL", "GENERATION", "LEASE_EPOCH", "FRAGMENT_COUNT", "RECORD_COUNT", "CHANNEL_REVISION", "STATE_VERSION", "ROW_COUNT", "ATTEMPT", "PHASE_VERSION")
	set("BOOLEAN", -1, -1, "TOMBSTONE")
	set("VARIANT", -1, -1, "KEY_JSON", "BEFORE_IMAGE", "AFTER_IMAGE")
	set("ARRAY", -1, -1, "UNCHANGED_TOAST")
	set("TIMESTAMP_TZ(9)", -1, 9, "EVENT_TIME", "COMMITTED_AT", "UPDATED_AT", "CREATED_AT")
	_ = kind
	return contracts
}

func managedStreamOwnershipComment(cfg streamConfig, kind string) string {
	payload := struct {
		Profile  string `json:"profile"`
		Flow     string `json:"flow"`
		Revision string `json:"revision"`
		Schema   string `json:"schema"`
		Kind     string `json:"kind"`
	}{cfg.profile, cfg.flowID, cfg.destinationRevision, cfg.schemaContractHash, kind}
	encoded, _ := json.Marshal(payload)
	return string(encoded)
}

func managedStreamCatalogFingerprint(catalog managedStreamCatalogSnapshot) (string, error) {
	value := struct {
		Pipe     string `json:"pipe"`
		Target   string `json:"target"`
		Receipts string `json:"receipts"`
		Channel  string `json:"channel"`
		Requests string `json:"requests"`
		Counts   string `json:"counts"`
	}{canonicalManagedStreamPipe(catalog.pipe), canonicalManagedStreamTable(catalog.target), canonicalManagedStreamTable(catalog.receipts), canonicalManagedStreamTable(catalog.channel), canonicalManagedStreamTable(catalog.requests), fmt.Sprintf("pipes=%d;tasks=%d", catalog.pipeCount, catalog.taskCount)}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalManagedStreamPipe(pipe managedPipeSnapshot) string {
	values := make([]string, 0, 10+len(pipe.grants))
	values = append(values, strconv.FormatBool(pipe.present), pipe.definition, pipe.ownerRole, pipe.createdOn, strconv.FormatBool(pipe.autoIngest), pipe.onError, pipe.force, pipe.purge, pipe.matchByColumnName, pipe.comment)
	values = append(values, canonicalManagedStreamGrants(pipe.grants)...)
	return strings.Join(values, "\x00")
}

func canonicalManagedStreamTable(table managedTableSnapshot) string {
	values := make([]string, 0, 6+len(table.columns)+len(table.constraints)+len(table.grants))
	values = append(values, table.kind, table.ownerRole, table.createdOn, table.comment, table.definition, strconv.Itoa(table.otherConstraintCount))
	columns := make([]string, 0, len(table.columns))
	for name, column := range table.columns {
		columns = append(columns, fmt.Sprintf("%s:%s:%d:%d:%t:%t:%t", name, column.dataType, column.characterMaximumLength, column.datetimePrecision, column.nullable, column.hasDefault, column.generated))
	}
	sort.Strings(columns)
	values = append(values, columns...)
	constraints := make([]string, 0, len(table.constraints))
	for _, constraint := range table.constraints {
		constraints = append(constraints, constraint.name+":"+constraint.constraintType+":"+strconv.FormatBool(constraint.enforced)+":"+strings.Join(constraint.columns, ","))
	}
	sort.Strings(constraints)
	values = append(values, constraints...)
	values = append(values, canonicalManagedStreamGrants(table.grants)...)
	return strings.Join(values, "\x00")
}

func canonicalManagedStreamGrants(grants map[string][]string) []string {
	values := make([]string, 0, len(grants))
	for role, privileges := range grants {
		copyPrivileges := append([]string(nil), privileges...)
		sort.Strings(copyPrivileges)
		values = append(values, role+":"+strings.Join(copyPrivileges, ","))
	}
	sort.Strings(values)
	return values
}
