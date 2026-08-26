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

// streamingTransportLinked reports whether this build links a reviewed,
// live-proven Snowpipe Streaming high-performance append transport.
//
// It is deliberately false. There is no officially supported Go SDK or
// high-performance REST client for Snowpipe Streaming that the gosnowflake
// database/sql driver can execute: gosnowflake speaks the query API, not the
// channel append protocol. Rather than fabricate delivery from local
// continuation/offset tokens ("local-token theater"), the profile fails closed
// at admission until a reviewed transport is linked here AND its same-SHA live
// recovery matrix passes on one commercial Snowflake deployment cell.
//
// Flipping this constant is a promotion action, not a configuration one; it must
// be accompanied by a concrete streamAppendTransport implementation and the live
// evidence gates named by the profile contract.
const streamingTransportLinked = false

var (
	// ErrManagedStreamingTransportUnavailable is the fail-closed admission error
	// returned whenever the streaming REST append profile is requested without a
	// linked, reviewed high-performance append transport. It is exported so the
	// runner admission and live entrypoints can assert the exact refusal.
	ErrManagedStreamingTransportUnavailable = errors.New("managed Snowpipe Streaming REST append profile has no reviewed high-performance append transport linked; admission fails closed rather than proving delivery from local tokens")

	// errStreamingTransportUnavailable is the internal transport-construction
	// sentinel; the SQL-backed protocol returns it from every append operation.
	errStreamingTransportUnavailable = errors.New("snowpipe streaming append transport unavailable")
)

// ManagedStreamingTransportAvailable reports whether a reviewed high-performance
// Snowpipe Streaming append transport is linked into this build. It is a pure
// function so runner admission can fail closed without any side effect and
// without a live service.
func ManagedStreamingTransportAvailable() bool { return streamingTransportLinked }

// streamConfig is the immutable admitted configuration for the Snowpipe
// Streaming REST append profile. It is derived only from the connector spec;
// nothing observed from the live service can widen it.
type streamConfig struct {
	profile               string
	flowID                string
	account               string
	database              string
	schema                string
	pipe                  string
	table                 string
	receiptsTable         string
	channelStateTable     string
	channelNamePrefix     string
	ownerRole             string
	executionRole         string
	warehouse             string
	snowflakeVersion      string
	pipeCreatedOn         string
	targetCreatedOn       string
	receiptsCreatedOn     string
	channelStateCreatedOn string
	sourceSchema          string
	sourceTable           string
	schemaContract        connector.Schema
	schemaContractHash    string
	destinationRevision   string

	maxTransactionRows      int
	maxTransactionBytes     int64
	maxFragments            int
	maxRowBytes             int64
	maxOpenConnections      int
	statementTimeoutSeconds int
	observeAttempts         int
	observeInterval         time.Duration
	appendAttempts          int
	appendBackoff           time.Duration
	cleanupMaxObjects       int
	cleanupRetention        time.Duration
	validateEveryConnection bool
	typeMappings            map[string]string
}

// streamProfileAllowedOptions is the exact admitted option set for the streaming
// profile. Any option outside the set is rejected before connector side effects.
func streamProfileAllowedOptions() map[string]struct{} {
	return map[string]struct{}{
		"dsn": {}, "flow_id": {}, "managed_profile": {}, "destination_revision_id": {},
		"batch_mode": {}, "batch_resolution": {}, "meta_table_enabled": {},
		"disable_transactions": {}, "session_keep_alive": {},
		"managed_account": {}, "managed_database": {}, "managed_schema": {}, "managed_pipe": {},
		"managed_table": {}, "managed_receipts_table": {}, "managed_channel_state_table": {},
		"managed_channel_name_prefix": {},
		"managed_owner_role":          {}, "managed_execution_role": {}, "managed_warehouse": {},
		"managed_snowflake_version": {}, "managed_pipe_created_on": {}, "managed_target_created_on": {},
		"managed_receipts_created_on": {}, "managed_channel_state_created_on": {},
		"managed_source_schema": {}, "managed_source_table": {},
		"managed_schema_contract": {}, "managed_schema_contract_hash": {},
		"managed_max_transaction_rows": {}, "managed_max_transaction_bytes": {},
		"managed_max_transaction_fragments": {}, "managed_max_row_bytes": {}, "managed_max_open_conns": {},
		"managed_statement_timeout_seconds": {}, "managed_observe_attempts": {},
		"managed_observe_interval_ms": {}, "managed_append_attempts": {}, "managed_append_backoff_ms": {},
		"managed_cleanup_max_objects": {}, "managed_cleanup_retention_seconds": {},
		"managed_streaming_transport": {},
	}
}

// streamRequiredTransport is the only admitted value for managed_streaming_transport.
// It names the intended reviewed transport; the presence of the option is not
// sufficient for admission — ManagedStreamingTransportAvailable() must also be
// true, which requires the transport to be linked and live-proven.
const streamRequiredTransport = "snowpipe-streaming-highperf-rest"

// ValidateManagedStreamingProfileOptions rejects options outside the constrained
// streaming append profile before connector side effects occur.
func ValidateManagedStreamingProfileOptions(options map[string]string) error {
	if _, exists := options["write_mode"]; exists {
		return errors.New("managed Snowflake write_mode is obsolete; managed_profile and the mandatory table mapping select the protocol")
	}
	allowed := streamProfileAllowedOptions()
	for option := range options {
		if _, ok := allowed[option]; !ok {
			return fmt.Errorf("managed streaming Snowflake profile does not allow option %s", option)
		}
	}
	return nil
}

// ValidateManagedStreamingProfileSpec performs the complete side-effect-free
// portion of streaming append admission.
func ValidateManagedStreamingProfileSpec(spec connector.RuntimeSpec) error {
	_, err := streamConfigFromSpec(strings.TrimSpace(spec.Options["dsn"]), spec)
	return err
}

func streamConfigFromSpec(dsn string, spec connector.RuntimeSpec) (streamConfig, error) {
	const profileName = connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
	options := spec.Options
	if strings.TrimSpace(options["managed_profile"]) != profileName {
		return streamConfig{}, fmt.Errorf("managed streaming Snowflake profile must be %s", profileName)
	}
	if err := ValidateManagedStreamingProfileOptions(options); err != nil {
		return streamConfig{}, err
	}
	if got := strings.TrimSpace(options["managed_streaming_transport"]); got != streamRequiredTransport {
		return streamConfig{}, fmt.Errorf("managed streaming Snowflake profile requires managed_streaming_transport=%s; got %q", streamRequiredTransport, got)
	}
	if err := connector.ValidateSnowflakeDSN(dsn); err != nil {
		return streamConfig{}, err
	}
	dsnConfig, err := gosnowflake.ParseDSN(dsn)
	if err != nil {
		return streamConfig{}, connector.ErrMalformedSnowflakeDSN
	}
	if !strings.EqualFold(dsnConfig.Protocol, "https") || managedSnowflakeDSNDisablesOCSP(dsn) || dsnConfig.DisableOCSPChecks || dsnConfig.OCSPFailOpen != gosnowflake.OCSPFailOpenFalse {
		return streamConfig{}, errors.New("managed streaming Snowflake profile requires verified HTTPS with OCSP fail-closed")
	}
	if dsnConfig.Authenticator != gosnowflake.AuthTypeJwt {
		return streamConfig{}, errors.New("managed streaming Snowflake profile requires key-pair JWT authentication")
	}
	// READ_LATEST_WRITES is load-bearing, not cosmetic: recovery proves
	// completeness by re-observing committed ROW_HASH identities after an append,
	// so the observation read must reflect the just-committed writes. Without
	// consistent read-after-append, a lost/throttled response combined with a
	// lagging observation drives a re-append whose duplicate identity fails closed
	// (errStreamObservationInconsistent) rather than acknowledging a silent
	// duplicate — safe, but stuck. The live recovery matrix must exercise this.
	if !managedSnowflakeSessionParameterEnabled(dsnConfig.Params, "READ_LATEST_WRITES") {
		return streamConfig{}, errors.New("managed streaming Snowflake profile requires DSN session parameter READ_LATEST_WRITES=true for cross-session observed-row and receipt reconciliation")
	}
	if timezone, present := managedSnowflakeSessionParameter(dsnConfig.Params, "TIMEZONE"); !present || !strings.EqualFold(timezone, "UTC") {
		return streamConfig{}, errors.New("managed streaming Snowflake profile requires DSN session parameter TIMEZONE=UTC")
	}
	if managedSnowflakeSessionParameterEnabled(dsnConfig.Params, "CLIENT_SESSION_KEEP_ALIVE") {
		return streamConfig{}, errors.New("managed streaming Snowflake profile rejects DSN session parameter CLIENT_SESSION_KEEP_ALIVE=true")
	}
	cfg := streamConfig{
		profile:  profileName,
		flowID:   strings.TrimSpace(options["flow_id"]),
		account:  strings.ToUpper(strings.TrimSpace(options["managed_account"])),
		database: strings.TrimSpace(options["managed_database"]), schema: strings.TrimSpace(options["managed_schema"]),
		pipe: strings.TrimSpace(options["managed_pipe"]), table: strings.TrimSpace(options["managed_table"]),
		receiptsTable:     strings.TrimSpace(options["managed_receipts_table"]),
		channelStateTable: strings.TrimSpace(options["managed_channel_state_table"]),
		channelNamePrefix: strings.TrimSpace(options["managed_channel_name_prefix"]),
		ownerRole:         strings.TrimSpace(options["managed_owner_role"]), executionRole: strings.TrimSpace(options["managed_execution_role"]),
		warehouse: strings.TrimSpace(options["managed_warehouse"]), snowflakeVersion: strings.TrimSpace(options["managed_snowflake_version"]),
		pipeCreatedOn: strings.TrimSpace(options["managed_pipe_created_on"]), targetCreatedOn: strings.TrimSpace(options["managed_target_created_on"]),
		receiptsCreatedOn: strings.TrimSpace(options["managed_receipts_created_on"]), channelStateCreatedOn: strings.TrimSpace(options["managed_channel_state_created_on"]),
		sourceSchema: options["managed_source_schema"], sourceTable: options["managed_source_table"],
		schemaContractHash:  strings.TrimSpace(options["managed_schema_contract_hash"]),
		destinationRevision: strings.TrimSpace(options["destination_revision_id"]),
	}
	if err := streamSnowflakeDSNRedactsSecrets(dsn); err != nil {
		return streamConfig{}, err
	}
	if cfg.flowID == "" || cfg.account == "" || cfg.snowflakeVersion == "" || cfg.sourceSchema == "" || cfg.sourceTable == "" ||
		strings.ContainsRune(cfg.sourceSchema, '\x00') || strings.ContainsRune(cfg.sourceTable, '\x00') ||
		cfg.destinationRevision == "" || cfg.pipeCreatedOn == "" || cfg.targetCreatedOn == "" || cfg.receiptsCreatedOn == "" || cfg.channelStateCreatedOn == "" {
		return streamConfig{}, errors.New("managed streaming Snowflake flow, account, version, object creation identities, exact nonempty NUL-free source relation, and destination revision are required")
	}
	if len(cfg.flowID) > 1024 || strings.TrimSpace(cfg.flowID) != cfg.flowID || strings.ContainsAny(cfg.flowID, "\r\n\x00") {
		return streamConfig{}, errors.New("managed streaming Snowflake flow_id must be a bounded single-line exact value")
	}
	if len(cfg.snowflakeVersion) > 128 || strings.ContainsAny(cfg.snowflakeVersion, "\r\n\x00") {
		return streamConfig{}, errors.New("managed streaming Snowflake version must be a bounded single-line exact value")
	}
	createdIdentities := map[string]string{
		"managed_pipe_created_on": cfg.pipeCreatedOn, "managed_target_created_on": cfg.targetCreatedOn,
		"managed_receipts_created_on": cfg.receiptsCreatedOn, "managed_channel_state_created_on": cfg.channelStateCreatedOn,
	}
	for name, value := range createdIdentities {
		if _, err := time.Parse("2006-01-02T15:04:05.000000000Z07:00", value); err != nil {
			return streamConfig{}, fmt.Errorf("managed streaming Snowflake %s must use YYYY-MM-DDTHH:MM:SS.FF9+00:00 form: %w", name, err)
		}
	}
	identifiers := map[string]string{
		"managed_database": cfg.database, "managed_schema": cfg.schema, "managed_pipe": cfg.pipe, "managed_table": cfg.table,
		"managed_receipts_table": cfg.receiptsTable, "managed_channel_state_table": cfg.channelStateTable,
		"managed_request_journal_table": cfg.channelStateTable + "_REQUESTS",
		"managed_owner_role":            cfg.ownerRole, "managed_execution_role": cfg.executionRole, "managed_warehouse": cfg.warehouse,
	}
	for name, value := range identifiers {
		if err := validateManagedSnowflakeUnquotedIdentifier(name, value); err != nil {
			return streamConfig{}, err
		}
	}
	if ddl := managedStreamCurrentSchemaDDL(cfg); len(ddl) != 2 || ddl[0] == "" || ddl[1] == "" {
		return streamConfig{}, errors.New("managed streaming Snowflake current control schema is incomplete")
	}
	if err := validateManagedStreamingChannelPrefix(cfg.channelNamePrefix); err != nil {
		return streamConfig{}, err
	}
	for _, pair := range []struct{ name, configured, dsn string }{
		{name: "account", configured: cfg.account, dsn: dsnConfig.Account},
		{name: "database", configured: cfg.database, dsn: dsnConfig.Database},
		{name: "schema", configured: cfg.schema, dsn: dsnConfig.Schema},
		{name: "role", configured: cfg.executionRole, dsn: dsnConfig.Role},
		{name: "warehouse", configured: cfg.warehouse, dsn: dsnConfig.Warehouse},
	} {
		if !strings.EqualFold(pair.configured, pair.dsn) {
			return streamConfig{}, fmt.Errorf("managed streaming Snowflake %s %q does not match DSN value %q", pair.name, pair.configured, pair.dsn)
		}
	}
	if cfg.ownerRole == cfg.executionRole {
		return streamConfig{}, errors.New("managed streaming Snowflake execution role must not own target objects")
	}
	if err := validateManagedSnowflakeRevision(cfg.destinationRevision); err != nil {
		return streamConfig{}, err
	}
	if err := json.Unmarshal([]byte(options["managed_schema_contract"]), &cfg.schemaContract); err != nil {
		return streamConfig{}, fmt.Errorf("decode managed streaming Snowflake schema contract: %w", err)
	}
	if cfg.schemaContract.Namespace != cfg.sourceSchema || cfg.schemaContract.Name != cfg.sourceTable {
		return streamConfig{}, errors.New("managed streaming Snowflake schema contract does not identify the configured source relation")
	}
	contractHash, err := ManagedSchemaContractHash(cfg.schemaContract)
	if err != nil {
		return streamConfig{}, err
	}
	if contractHash != cfg.schemaContractHash {
		return streamConfig{}, fmt.Errorf("%w: managed streaming Snowflake schema contract hash=%s, configured=%s", connector.ErrDeliveryConflict, contractHash, cfg.schemaContractHash)
	}
	if _, err := managedIdentityColumns(cfg.schemaContract); err != nil {
		return streamConfig{}, err
	}
	for _, column := range cfg.schemaContract.Columns {
		if column.TypeMetadata["nullability_known"] != "true" || column.TypeMetadata["generated_known"] != "true" {
			return streamConfig{}, fmt.Errorf("managed streaming Snowflake schema contract column %q requires nullability_known=true and generated_known=true", column.Name)
		}
		if column.Generated {
			return streamConfig{}, fmt.Errorf("managed streaming Snowflake schema contract rejects generated column %q", column.Name)
		}
	}
	if strings.TrimSpace(options["type_mappings"]) != "" {
		return streamConfig{}, errors.New("managed streaming Snowflake profile rejects type mapping overrides until each mapping has real-service recovery evidence")
	}
	cfg.typeMappings = defaultSnowflakeTypeMappings()
	for _, column := range cfg.schemaContract.Columns {
		if !streamSourceColumnSupported(cfg.typeMappings, column) {
			return streamConfig{}, fmt.Errorf("managed streaming Snowflake has no lossless serialization for source column %q type %q", column.Name, column.Type)
		}
	}
	if cfg.maxTransactionRows, err = parseManagedSnowflakeInt(options, "managed_max_transaction_rows", 100_000); err != nil {
		return streamConfig{}, err
	}
	if cfg.maxTransactionBytes, err = parseManagedSnowflakeInt64(options, "managed_max_transaction_bytes", 256<<20); err != nil {
		return streamConfig{}, err
	}
	if cfg.maxFragments, err = parseManagedSnowflakeInt(options, "managed_max_transaction_fragments", 4096); err != nil {
		return streamConfig{}, err
	}
	if cfg.maxRowBytes, err = parseManagedSnowflakeInt64(options, "managed_max_row_bytes", 16<<20); err != nil {
		return streamConfig{}, err
	}
	if cfg.maxOpenConnections, err = parseManagedSnowflakeInt(options, "managed_max_open_conns", 8); err != nil {
		return streamConfig{}, err
	}
	if cfg.statementTimeoutSeconds, err = parseManagedSnowflakeInt(options, "managed_statement_timeout_seconds", 3600); err != nil {
		return streamConfig{}, err
	}
	if cfg.observeAttempts, err = parseManagedSnowflakeInt(options, "managed_observe_attempts", 240); err != nil {
		return streamConfig{}, err
	}
	observeIntervalMillis, err := parseManagedSnowflakeInt(options, "managed_observe_interval_ms", 1_000)
	if err != nil {
		return streamConfig{}, err
	}
	cfg.observeInterval = time.Duration(observeIntervalMillis) * time.Millisecond
	if cfg.appendAttempts, err = parseManagedSnowflakeInt(options, "managed_append_attempts", 16); err != nil {
		return streamConfig{}, err
	}
	appendBackoffMillis, err := parseManagedSnowflakeInt(options, "managed_append_backoff_ms", 250)
	if err != nil {
		return streamConfig{}, err
	}
	cfg.appendBackoff = time.Duration(appendBackoffMillis) * time.Millisecond
	if cfg.cleanupMaxObjects, err = parseManagedSnowflakeInt(options, "managed_cleanup_max_objects", 10_000); err != nil {
		return streamConfig{}, err
	}
	retentionSeconds, err := parseManagedSnowflakeInt(options, "managed_cleanup_retention_seconds", 90*24*3600)
	if err != nil {
		return streamConfig{}, err
	}
	cfg.cleanupRetention = time.Duration(retentionSeconds) * time.Second
	cfg.validateEveryConnection = true
	if strings.ToLower(strings.TrimSpace(options["batch_mode"])) != "target" || strings.ToLower(strings.TrimSpace(options["batch_resolution"])) != "none" {
		return streamConfig{}, errors.New("managed streaming Snowflake profile requires batch_mode=target and batch_resolution=none")
	}
	metaEnabled, err := parseManagedSnowflakeBoolOption(options, "meta_table_enabled", true)
	if err != nil {
		return streamConfig{}, err
	}
	disableTransactions, err := parseManagedSnowflakeBoolOption(options, "disable_transactions", false)
	if err != nil {
		return streamConfig{}, err
	}
	keepAlive, err := parseManagedSnowflakeBoolOption(options, "session_keep_alive", false)
	if err != nil {
		return streamConfig{}, err
	}
	if metaEnabled || disableTransactions || keepAlive {
		return streamConfig{}, errors.New("managed streaming Snowflake profile requires owned receipts, no generic metadata table, and non-persistent sessions")
	}
	for _, option := range []string{
		"schema", "table", "staging_schema", "staging_table", "staging_suffix", "warehouse", "warehouse_size",
		"warehouse_auto_suspend", "warehouse_auto_resume", "meta_schema", "meta_table", "meta_pk_prefix",
	} {
		if strings.TrimSpace(options[option]) != "" {
			return streamConfig{}, fmt.Errorf("managed streaming Snowflake profile rejects generic option %s", option)
		}
	}
	return cfg, nil
}

// validateManagedStreamingChannelPrefix bounds the operator-supplied channel
// name prefix. The full channel name is derived deterministically from this
// prefix and the flow incarnation so two writers never collide, and an empty
// prefix falls back to a fixed constant.
func validateManagedStreamingChannelPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	if len(prefix) > 96 {
		return errors.New("managed streaming Snowflake channel name prefix must be at most 96 characters")
	}
	for _, character := range prefix {
		if (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') && character != '_' && character != '-' {
			return errors.New("managed streaming Snowflake channel name prefix may contain only letters, digits, underscore, and dash")
		}
	}
	return nil
}

func streamSourceColumnSupported(typeMappings map[string]string, column connector.Column) bool {
	return managedSnowflakeColumnType(managedConfig{typeMappings: typeMappings}, column) != ""
}

// streamSnowflakeDSNRedactsSecrets delegates to the one persistence-safe DSN
// validator shared by generic and every managed Snowflake-backed connector.
func streamSnowflakeDSNRedactsSecrets(dsn string) error {
	return connector.ValidateSnowflakeDSN(dsn)
}

func managedSnowflakeStreamQualified(cfg streamConfig, object string) string {
	return strings.Join([]string{quoteIdent(cfg.database, '"'), quoteIdent(cfg.schema, '"'), quoteIdent(object, '"')}, ".")
}
