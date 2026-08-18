package snowflake

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

// stagedConfig is the immutable admitted configuration for the staged COPY
// append profile. It is derived only from the connector spec; nothing observed
// from the live service can widen it.
type stagedConfig struct {
	profile                 string
	flowID                  string
	account                 string
	database                string
	schema                  string
	stage                   string
	table                   string
	receiptsTable           string
	fileFormat              string
	pipe                    string
	autoIngest              bool
	ownerRole               string
	executionRole           string
	warehouse               string
	snowflakeVersion        string
	stageCreatedOn          string
	targetCreatedOn         string
	receiptsCreatedOn       string
	fileFormatCreatedOn     string
	pipeCreatedOn           string
	sourceSchema            string
	sourceTable             string
	schemaContract          connector.Schema
	schemaContractHash      string
	destinationRevision     string
	maxTransactionRows      int
	maxTransactionBytes     int64
	maxFragments            int
	maxOpenConnections      int
	statementTimeoutSeconds int
	loadVerifyAttempts      int
	loadVerifyInterval      time.Duration
	cleanupMaxObjects       int
	cleanupRetention        time.Duration
	validateEveryConnection bool
	typeMappings            map[string]string
}

// stagedProfileAllowedOptions is the exact admitted option set for the staged
// profile. Any option outside the set is rejected before connector side effects.
func stagedProfileAllowedOptions() map[string]struct{} {
	return map[string]struct{}{
		"dsn": {}, "flow_id": {}, "managed_profile": {}, "destination_revision_id": {},
		"batch_mode": {}, "batch_resolution": {}, "meta_table_enabled": {},
		"disable_transactions": {}, "session_keep_alive": {},
		"managed_account": {}, "managed_database": {}, "managed_schema": {}, "managed_stage": {},
		"managed_table": {}, "managed_receipts_table": {}, "managed_file_format": {},
		"managed_pipe": {}, "managed_auto_ingest": {},
		"managed_owner_role": {}, "managed_execution_role": {}, "managed_warehouse": {},
		"managed_snowflake_version": {}, "managed_stage_created_on": {}, "managed_target_created_on": {},
		"managed_receipts_created_on": {}, "managed_file_format_created_on": {}, "managed_pipe_created_on": {},
		"managed_source_schema": {}, "managed_source_table": {},
		"managed_schema_contract": {}, "managed_schema_contract_hash": {},
		"managed_max_transaction_rows": {}, "managed_max_transaction_bytes": {},
		"managed_max_transaction_fragments": {}, "managed_max_open_conns": {},
		"managed_statement_timeout_seconds": {}, "managed_load_verify_attempts": {},
		"managed_load_verify_interval_ms": {}, "managed_cleanup_max_objects": {},
		"managed_cleanup_retention_seconds": {},
	}
}

// ValidateManagedStagedProfileOptions rejects options outside the constrained
// staged COPY profile before connector side effects occur.
func ValidateManagedStagedProfileOptions(options map[string]string) error {
	if _, exists := options["write_mode"]; exists {
		return errors.New("managed Snowflake write_mode is obsolete; managed_profile and the mandatory table mapping select the protocol")
	}
	allowed := stagedProfileAllowedOptions()
	for option := range options {
		if _, ok := allowed[option]; !ok {
			return fmt.Errorf("managed staged Snowflake profile does not allow option %s", option)
		}
	}
	return nil
}

// ValidateManagedStagedProfileSpec performs the complete side-effect-free
// portion of staged COPY admission.
func ValidateManagedStagedProfileSpec(spec connector.RuntimeSpec) error {
	_, err := stagedConfigFromSpec(strings.TrimSpace(spec.Options["dsn"]), spec)
	return err
}

func stagedConfigFromSpec(dsn string, spec connector.RuntimeSpec) (stagedConfig, error) {
	const profileName = connector.ManagedProfilePostgresToSnowflakeStagedAppendV1
	options := spec.Options
	if strings.TrimSpace(options["managed_profile"]) != profileName {
		return stagedConfig{}, fmt.Errorf("managed staged Snowflake profile must be %s", profileName)
	}
	if err := ValidateManagedStagedProfileOptions(options); err != nil {
		return stagedConfig{}, err
	}
	if err := connector.ValidateSnowflakeDSN(dsn); err != nil {
		return stagedConfig{}, err
	}
	dsnConfig, err := gosnowflake.ParseDSN(dsn)
	if err != nil {
		return stagedConfig{}, connector.ErrMalformedSnowflakeDSN
	}
	if !strings.EqualFold(dsnConfig.Protocol, "https") || managedSnowflakeDSNDisablesOCSP(dsn) || dsnConfig.DisableOCSPChecks || dsnConfig.OCSPFailOpen != gosnowflake.OCSPFailOpenFalse {
		return stagedConfig{}, errors.New("managed staged Snowflake profile requires verified HTTPS with OCSP fail-closed")
	}
	if dsnConfig.Authenticator != gosnowflake.AuthTypeJwt {
		return stagedConfig{}, errors.New("managed staged Snowflake profile requires key-pair JWT authentication")
	}
	if !managedSnowflakeSessionParameterEnabled(dsnConfig.Params, "READ_LATEST_WRITES") {
		return stagedConfig{}, errors.New("managed staged Snowflake profile requires DSN session parameter READ_LATEST_WRITES=true for cross-session hybrid receipt reconciliation")
	}
	if timezone, present := managedSnowflakeSessionParameter(dsnConfig.Params, "TIMEZONE"); !present || !strings.EqualFold(timezone, "UTC") {
		return stagedConfig{}, errors.New("managed staged Snowflake profile requires DSN session parameter TIMEZONE=UTC")
	}
	if managedSnowflakeSessionParameterEnabled(dsnConfig.Params, "CLIENT_SESSION_KEEP_ALIVE") {
		return stagedConfig{}, errors.New("managed staged Snowflake profile rejects DSN session parameter CLIENT_SESSION_KEEP_ALIVE=true")
	}
	cfg := stagedConfig{
		profile:  profileName,
		flowID:   strings.TrimSpace(options["flow_id"]),
		account:  strings.ToUpper(strings.TrimSpace(options["managed_account"])),
		database: strings.TrimSpace(options["managed_database"]), schema: strings.TrimSpace(options["managed_schema"]),
		stage: strings.TrimSpace(options["managed_stage"]), table: strings.TrimSpace(options["managed_table"]),
		receiptsTable: strings.TrimSpace(options["managed_receipts_table"]), fileFormat: strings.TrimSpace(options["managed_file_format"]),
		pipe:      strings.TrimSpace(options["managed_pipe"]),
		ownerRole: strings.TrimSpace(options["managed_owner_role"]), executionRole: strings.TrimSpace(options["managed_execution_role"]),
		warehouse: strings.TrimSpace(options["managed_warehouse"]), snowflakeVersion: strings.TrimSpace(options["managed_snowflake_version"]),
		stageCreatedOn: strings.TrimSpace(options["managed_stage_created_on"]), targetCreatedOn: strings.TrimSpace(options["managed_target_created_on"]),
		receiptsCreatedOn: strings.TrimSpace(options["managed_receipts_created_on"]), fileFormatCreatedOn: strings.TrimSpace(options["managed_file_format_created_on"]),
		pipeCreatedOn: strings.TrimSpace(options["managed_pipe_created_on"]),
		sourceSchema:  options["managed_source_schema"], sourceTable: options["managed_source_table"],
		schemaContractHash:  strings.TrimSpace(options["managed_schema_contract_hash"]),
		destinationRevision: strings.TrimSpace(options["destination_revision_id"]),
	}
	if err := stagedSnowflakeDSNRedactsSecrets(dsn); err != nil {
		return stagedConfig{}, err
	}
	cfg.autoIngest, err = parseManagedSnowflakeBoolOption(options, "managed_auto_ingest", false)
	if err != nil {
		return stagedConfig{}, err
	}
	if cfg.flowID == "" || cfg.account == "" || cfg.snowflakeVersion == "" || cfg.sourceSchema == "" || cfg.sourceTable == "" ||
		strings.ContainsRune(cfg.sourceSchema, '\x00') || strings.ContainsRune(cfg.sourceTable, '\x00') ||
		cfg.destinationRevision == "" || cfg.stageCreatedOn == "" || cfg.targetCreatedOn == "" || cfg.receiptsCreatedOn == "" || cfg.fileFormatCreatedOn == "" {
		return stagedConfig{}, errors.New("managed staged Snowflake flow, account, version, object creation identities, exact nonempty NUL-free source relation, and destination revision are required")
	}
	if len(cfg.flowID) > 1024 || strings.TrimSpace(cfg.flowID) != cfg.flowID || strings.ContainsAny(cfg.flowID, "\r\n\x00") {
		return stagedConfig{}, errors.New("managed staged Snowflake flow_id must be a bounded single-line exact value")
	}
	if len(cfg.snowflakeVersion) > 128 || strings.ContainsAny(cfg.snowflakeVersion, "\r\n\x00") {
		return stagedConfig{}, errors.New("managed staged Snowflake version must be a bounded single-line exact value")
	}
	createdIdentities := map[string]string{
		"managed_stage_created_on": cfg.stageCreatedOn, "managed_target_created_on": cfg.targetCreatedOn,
		"managed_receipts_created_on": cfg.receiptsCreatedOn, "managed_file_format_created_on": cfg.fileFormatCreatedOn,
	}
	if cfg.autoIngest {
		if cfg.pipe == "" || cfg.pipeCreatedOn == "" {
			return stagedConfig{}, errors.New("managed staged Snowflake auto-ingest requires managed_pipe and managed_pipe_created_on")
		}
		createdIdentities["managed_pipe_created_on"] = cfg.pipeCreatedOn
	} else if cfg.pipe != "" || cfg.pipeCreatedOn != "" {
		return stagedConfig{}, errors.New("managed staged Snowflake profile rejects a pipe unless managed_auto_ingest=true")
	}
	for name, value := range createdIdentities {
		if _, err := time.Parse("2006-01-02T15:04:05.000000000Z07:00", value); err != nil {
			return stagedConfig{}, fmt.Errorf("managed staged Snowflake %s must use YYYY-MM-DDTHH:MM:SS.FF9+00:00 form: %w", name, err)
		}
	}
	identifiers := map[string]string{
		"managed_database": cfg.database, "managed_schema": cfg.schema, "managed_stage": cfg.stage, "managed_table": cfg.table,
		"managed_receipts_table": cfg.receiptsTable, "managed_file_format": cfg.fileFormat, "managed_owner_role": cfg.ownerRole,
		"managed_execution_role": cfg.executionRole, "managed_warehouse": cfg.warehouse,
	}
	if cfg.autoIngest {
		identifiers["managed_pipe"] = cfg.pipe
	}
	for name, value := range identifiers {
		if err := validateManagedSnowflakeUnquotedIdentifier(name, value); err != nil {
			return stagedConfig{}, err
		}
	}
	for _, pair := range []struct{ name, configured, dsn string }{
		{name: "account", configured: cfg.account, dsn: dsnConfig.Account},
		{name: "database", configured: cfg.database, dsn: dsnConfig.Database},
		{name: "schema", configured: cfg.schema, dsn: dsnConfig.Schema},
		{name: "role", configured: cfg.executionRole, dsn: dsnConfig.Role},
		{name: "warehouse", configured: cfg.warehouse, dsn: dsnConfig.Warehouse},
	} {
		if !strings.EqualFold(pair.configured, pair.dsn) {
			return stagedConfig{}, fmt.Errorf("managed staged Snowflake %s %q does not match DSN value %q", pair.name, pair.configured, pair.dsn)
		}
	}
	if cfg.ownerRole == cfg.executionRole {
		return stagedConfig{}, errors.New("managed staged Snowflake execution role must not own target objects")
	}
	if err := validateManagedSnowflakeRevision(cfg.destinationRevision); err != nil {
		return stagedConfig{}, err
	}
	if err := json.Unmarshal([]byte(options["managed_schema_contract"]), &cfg.schemaContract); err != nil {
		return stagedConfig{}, fmt.Errorf("decode managed staged Snowflake schema contract: %w", err)
	}
	if cfg.schemaContract.Namespace != cfg.sourceSchema || cfg.schemaContract.Name != cfg.sourceTable {
		return stagedConfig{}, errors.New("managed staged Snowflake schema contract does not identify the configured source relation")
	}
	contractHash, err := ManagedSchemaContractHash(cfg.schemaContract)
	if err != nil {
		return stagedConfig{}, err
	}
	if contractHash != cfg.schemaContractHash {
		return stagedConfig{}, fmt.Errorf("%w: managed staged Snowflake schema contract hash=%s, configured=%s", connector.ErrDeliveryConflict, contractHash, cfg.schemaContractHash)
	}
	if _, err := managedIdentityColumns(cfg.schemaContract); err != nil {
		return stagedConfig{}, err
	}
	for _, column := range cfg.schemaContract.Columns {
		if column.TypeMetadata["nullability_known"] != "true" || column.TypeMetadata["generated_known"] != "true" {
			return stagedConfig{}, fmt.Errorf("managed staged Snowflake schema contract column %q requires nullability_known=true and generated_known=true", column.Name)
		}
		if column.Generated {
			return stagedConfig{}, fmt.Errorf("managed staged Snowflake schema contract rejects generated column %q", column.Name)
		}
	}
	if strings.TrimSpace(options["type_mappings"]) != "" {
		return stagedConfig{}, errors.New("managed staged Snowflake profile rejects type mapping overrides until each mapping has real-service recovery evidence")
	}
	cfg.typeMappings = defaultSnowflakeTypeMappings()
	for _, column := range cfg.schemaContract.Columns {
		if !stagedSourceColumnSupported(cfg.typeMappings, column) {
			return stagedConfig{}, fmt.Errorf("managed staged Snowflake has no lossless serialization for source column %q type %q", column.Name, column.Type)
		}
	}
	if cfg.maxTransactionRows, err = parseManagedSnowflakeInt(options, "managed_max_transaction_rows", 100_000); err != nil {
		return stagedConfig{}, err
	}
	if cfg.maxTransactionBytes, err = parseManagedSnowflakeInt64(options, "managed_max_transaction_bytes", 256<<20); err != nil {
		return stagedConfig{}, err
	}
	if cfg.maxFragments, err = parseManagedSnowflakeInt(options, "managed_max_transaction_fragments", 4096); err != nil {
		return stagedConfig{}, err
	}
	if cfg.maxOpenConnections, err = parseManagedSnowflakeInt(options, "managed_max_open_conns", 8); err != nil {
		return stagedConfig{}, err
	}
	if cfg.statementTimeoutSeconds, err = parseManagedSnowflakeInt(options, "managed_statement_timeout_seconds", 3600); err != nil {
		return stagedConfig{}, err
	}
	if cfg.loadVerifyAttempts, err = parseManagedSnowflakeInt(options, "managed_load_verify_attempts", 240); err != nil {
		return stagedConfig{}, err
	}
	intervalMillis, err := parseManagedSnowflakeInt(options, "managed_load_verify_interval_ms", 60_000)
	if err != nil {
		return stagedConfig{}, err
	}
	cfg.loadVerifyInterval = time.Duration(intervalMillis) * time.Millisecond
	if cfg.cleanupMaxObjects, err = parseManagedSnowflakeInt(options, "managed_cleanup_max_objects", 10_000); err != nil {
		return stagedConfig{}, err
	}
	retentionSeconds, err := parseManagedSnowflakeInt(options, "managed_cleanup_retention_seconds", 90*24*3600)
	if err != nil {
		return stagedConfig{}, err
	}
	cfg.cleanupRetention = time.Duration(retentionSeconds) * time.Second
	cfg.validateEveryConnection = true
	if strings.ToLower(strings.TrimSpace(options["batch_mode"])) != "target" || strings.ToLower(strings.TrimSpace(options["batch_resolution"])) != "none" {
		return stagedConfig{}, errors.New("managed staged Snowflake profile requires batch_mode=target and batch_resolution=none")
	}
	metaEnabled, err := parseManagedSnowflakeBoolOption(options, "meta_table_enabled", true)
	if err != nil {
		return stagedConfig{}, err
	}
	disableTransactions, err := parseManagedSnowflakeBoolOption(options, "disable_transactions", false)
	if err != nil {
		return stagedConfig{}, err
	}
	keepAlive, err := parseManagedSnowflakeBoolOption(options, "session_keep_alive", false)
	if err != nil {
		return stagedConfig{}, err
	}
	if metaEnabled || disableTransactions || keepAlive {
		return stagedConfig{}, errors.New("managed staged Snowflake profile requires owned receipts, single-statement COPY, and non-persistent sessions")
	}
	for _, option := range []string{
		"schema", "table", "staging_schema", "staging_table", "staging_suffix", "warehouse", "warehouse_size",
		"warehouse_auto_suspend", "warehouse_auto_resume", "meta_schema", "meta_table", "meta_pk_prefix",
	} {
		if strings.TrimSpace(options[option]) != "" {
			return stagedConfig{}, fmt.Errorf("managed staged Snowflake profile rejects generic option %s", option)
		}
	}
	return cfg, nil
}

func stagedSourceColumnSupported(typeMappings map[string]string, column connector.Column) bool {
	return managedSnowflakeColumnType(managedConfig{typeMappings: typeMappings}, column) != ""
}

// stagedSnowflakeDSNRedactsSecrets delegates to the one persistence-safe DSN
// validator shared by generic and every managed Snowflake-backed connector.
func stagedSnowflakeDSNRedactsSecrets(dsn string) error {
	return connector.ValidateSnowflakeDSN(dsn)
}
