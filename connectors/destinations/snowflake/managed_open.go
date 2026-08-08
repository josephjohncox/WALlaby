package snowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

func (d *Destination) openManaged(ctx context.Context, dsn string, spec connector.RuntimeSpec) (resultErr error) {
	ctx, endAdmission := telemetry.StartSnowflakeManagedSpan(ctx, "admission", "", "", 0, 0)
	defer func() { endAdmission(resultErr) }()
	cfg, err := managedConfigFromSpec(dsn, spec)
	if err != nil {
		return err
	}
	d.managedScopeMu.Lock()
	d.managedFlowIncarnation = ""
	d.managedScopeMu.Unlock()
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("open managed Snowflake: %w", err)
	}
	db.SetMaxOpenConns(cfg.maxOpenConnections)
	db.SetMaxIdleConns(cfg.maxOpenConnections)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	opened := false
	defer func() {
		if !opened {
			_ = db.Close()
			d.db = nil
		}
	}()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping managed Snowflake: %w", err)
	}
	d.db = db
	d.managedConfig = cfg
	conn, err := d.acquireManagedSnowflakeConn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err := d.validateManagedSnowflakeTransactions(ctx, conn); err != nil {
		return err
	}
	catalog, err := d.loadManagedSnowflakeCatalogWith(ctx, conn)
	if err != nil {
		return err
	}
	if err := validateManagedSnowflakeCatalog(cfg, catalog); err != nil {
		return err
	}
	var foreignReceipts int
	// #nosec G202 -- all three object identifiers passed through strict unquoted-uppercase validation.
	if err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+managedSnowflakeQualifiedTable(cfg, cfg.receiptsTable)+" WHERE \"PROFILE_VERSION\" <> ? OR \"FLOW_ID\" <> ? OR \"DESTINATION_REVISION_ID\" <> ? OR \"SCHEMA_CONTRACT_HASH\" <> ?",
		cfg.profile, cfg.flowID, cfg.destinationRevision, cfg.schemaContractHash,
	).Scan(&foreignReceipts); err != nil {
		return fmt.Errorf("validate managed Snowflake receipt ownership rows: %w", err)
	}
	if foreignReceipts != 0 {
		return fmt.Errorf("managed Snowflake receipt table contains %d rows owned by another profile, flow, destination revision, or schema contract", foreignReceipts)
	}
	var receiptRows int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+managedSnowflakeQualifiedTable(cfg, cfg.receiptsTable)).Scan(&receiptRows); err != nil {
		return fmt.Errorf("count managed Snowflake receipts: %w", err)
	}
	var targetHasRows bool
	if err := conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+managedSnowflakeQualifiedTable(cfg, cfg.table)+" LIMIT 1)").Scan(&targetHasRows); err != nil {
		return fmt.Errorf("inspect managed Snowflake clean-start target: %w", err)
	}
	if err := validateManagedSnowflakeCleanStartState(receiptRows, targetHasRows); err != nil {
		return err
	}
	opened = true
	return nil
}

func validateManagedSnowflakeCleanStartState(receiptRows int, targetHasRows bool) error {
	if receiptRows < 0 {
		return errors.New("managed Snowflake receipt count cannot be negative")
	}
	if receiptRows == 0 && targetHasRows {
		return errors.New("managed Snowflake clean start requires an empty target when no managed receipts exist")
	}
	return nil
}

// ValidateManagedProfileSpec performs the complete side-effect-free portion of
// managed Snowflake admission.
func ValidateManagedProfileSpec(spec connector.RuntimeSpec) error {
	_, err := managedConfigFromSpec(strings.TrimSpace(spec.Options["dsn"]), spec)
	return err
}

func managedConfigFromSpec(dsn string, spec connector.RuntimeSpec) (managedConfig, error) {
	const profileName = connector.ManagedProfilePostgresToSnowflakeSQLV1
	options := spec.Options
	if strings.TrimSpace(options["managed_profile"]) != profileName {
		return managedConfig{}, fmt.Errorf("managed Snowflake profile must be %s", profileName)
	}
	if err := ValidateManagedProfileOptions(options); err != nil {
		return managedConfig{}, err
	}
	dsnConfig, err := gosnowflake.ParseDSN(dsn)
	if err != nil {
		return managedConfig{}, fmt.Errorf("parse managed Snowflake DSN: %w", err)
	}
	if !strings.EqualFold(dsnConfig.Protocol, "https") || managedSnowflakeDSNDisablesOCSP(dsn) || dsnConfig.DisableOCSPChecks || dsnConfig.OCSPFailOpen != gosnowflake.OCSPFailOpenFalse {
		return managedConfig{}, errors.New("managed Snowflake profile requires verified HTTPS with OCSP fail-closed")
	}
	if dsnConfig.Authenticator != gosnowflake.AuthTypeJwt || dsnConfig.PrivateKey == nil {
		return managedConfig{}, errors.New("managed Snowflake profile requires key-pair JWT authentication")
	}
	if !managedSnowflakeSessionParameterEnabled(dsnConfig.Params, "READ_LATEST_WRITES") {
		return managedConfig{}, errors.New("managed Snowflake profile requires DSN session parameter READ_LATEST_WRITES=true")
	}
	if timezone, present := managedSnowflakeSessionParameter(dsnConfig.Params, "TIMEZONE"); !present || !strings.EqualFold(timezone, "UTC") {
		return managedConfig{}, errors.New("managed Snowflake profile requires DSN session parameter TIMEZONE=UTC")
	}
	if managedSnowflakeSessionParameterEnabled(dsnConfig.Params, "CLIENT_SESSION_KEEP_ALIVE") {
		return managedConfig{}, errors.New("managed Snowflake profile rejects DSN session parameter CLIENT_SESSION_KEEP_ALIVE=true")
	}
	cfg := managedConfig{
		profile:  profileName,
		flowID:   strings.TrimSpace(options["flow_id"]),
		account:  strings.ToUpper(strings.TrimSpace(options["managed_account"])),
		database: strings.TrimSpace(options["managed_database"]), schema: strings.TrimSpace(options["managed_schema"]),
		table: strings.TrimSpace(options["managed_table"]), receiptsTable: strings.TrimSpace(options["managed_receipts_table"]),
		ownerRole: strings.TrimSpace(options["managed_owner_role"]), executionRole: strings.TrimSpace(options["managed_execution_role"]),
		warehouse: strings.TrimSpace(options["managed_warehouse"]), snowflakeVersion: strings.TrimSpace(options["managed_snowflake_version"]),
		targetCreatedOn: strings.TrimSpace(options["managed_target_created_on"]), receiptsCreatedOn: strings.TrimSpace(options["managed_receipts_created_on"]),
		sourceSchema: options["managed_source_schema"], sourceTable: options["managed_source_table"],
		schemaContractHash:  strings.TrimSpace(options["managed_schema_contract_hash"]),
		destinationRevision: strings.TrimSpace(options["destination_revision_id"]),
	}
	if cfg.flowID == "" || cfg.account == "" || cfg.snowflakeVersion == "" || cfg.sourceSchema == "" || cfg.sourceTable == "" || strings.ContainsRune(cfg.sourceSchema, '\x00') || strings.ContainsRune(cfg.sourceTable, '\x00') || cfg.destinationRevision == "" || cfg.targetCreatedOn == "" || cfg.receiptsCreatedOn == "" {
		return managedConfig{}, errors.New("managed Snowflake flow, account, version, object creation identities, exact nonempty NUL-free source relation, and destination revision are required")
	}
	if len(cfg.flowID) > 1024 || strings.TrimSpace(cfg.flowID) != cfg.flowID || strings.ContainsAny(cfg.flowID, "\r\n\x00") {
		return managedConfig{}, errors.New("managed Snowflake flow_id must be a bounded single-line exact value")
	}
	if len(cfg.snowflakeVersion) > 128 || strings.TrimSpace(cfg.snowflakeVersion) != cfg.snowflakeVersion || strings.ContainsAny(cfg.snowflakeVersion, "\r\n\x00") {
		return managedConfig{}, errors.New("managed Snowflake version must be a bounded single-line exact value")
	}
	for name, value := range map[string]string{"managed_target_created_on": cfg.targetCreatedOn, "managed_receipts_created_on": cfg.receiptsCreatedOn} {
		if _, err := time.Parse("2006-01-02T15:04:05.000000000Z07:00", value); err != nil {
			return managedConfig{}, fmt.Errorf("managed Snowflake %s must use YYYY-MM-DDTHH:MM:SS.FF9+00:00 form: %w", name, err)
		}
	}
	for name, value := range map[string]string{
		"managed_database": cfg.database, "managed_schema": cfg.schema, "managed_table": cfg.table,
		"managed_receipts_table": cfg.receiptsTable, "managed_owner_role": cfg.ownerRole,
		"managed_execution_role": cfg.executionRole, "managed_warehouse": cfg.warehouse,
	} {
		if err := validateManagedSnowflakeUnquotedIdentifier(name, value); err != nil {
			return managedConfig{}, err
		}
	}
	for _, pair := range []struct {
		name       string
		configured string
		dsn        string
	}{
		{name: "account", configured: cfg.account, dsn: dsnConfig.Account},
		{name: "database", configured: cfg.database, dsn: dsnConfig.Database},
		{name: "schema", configured: cfg.schema, dsn: dsnConfig.Schema},
		{name: "role", configured: cfg.executionRole, dsn: dsnConfig.Role},
		{name: "warehouse", configured: cfg.warehouse, dsn: dsnConfig.Warehouse},
	} {
		if !strings.EqualFold(pair.configured, pair.dsn) {
			return managedConfig{}, fmt.Errorf("managed Snowflake %s %q does not match DSN value %q", pair.name, pair.configured, pair.dsn)
		}
	}
	if cfg.ownerRole == cfg.executionRole {
		return managedConfig{}, errors.New("managed Snowflake execution role must not own target objects")
	}
	if err := validateManagedSnowflakeRevision(cfg.destinationRevision); err != nil {
		return managedConfig{}, err
	}
	if err := json.Unmarshal([]byte(options["managed_schema_contract"]), &cfg.schemaContract); err != nil {
		return managedConfig{}, fmt.Errorf("decode managed Snowflake schema contract: %w", err)
	}
	if cfg.schemaContract.Namespace != cfg.schema || cfg.schemaContract.Name != cfg.table {
		return managedConfig{}, errors.New("managed Snowflake projected schema contract does not identify the provisioned target relation")
	}
	// The schema contract is destination-shaped. Keep the independently
	// persisted PostgreSQL source identity byte-exact for publication admission.
	contractHash, err := ManagedSchemaContractHash(cfg.schemaContract)
	if err != nil {
		return managedConfig{}, err
	}
	if contractHash != cfg.schemaContractHash {
		return managedConfig{}, fmt.Errorf("%w: managed Snowflake schema contract hash=%s, configured=%s", connector.ErrDeliveryConflict, contractHash, cfg.schemaContractHash)
	}
	if _, err := managedIdentityColumns(cfg.schemaContract); err != nil {
		return managedConfig{}, err
	}
	for _, column := range cfg.schemaContract.Columns {
		if column.TypeMetadata["nullability_known"] != "true" || column.TypeMetadata["generated_known"] != "true" {
			return managedConfig{}, fmt.Errorf("managed Snowflake schema contract column %q requires nullability_known=true and generated_known=true", column.Name)
		}
		if column.Generated {
			return managedConfig{}, fmt.Errorf("managed Snowflake schema contract rejects generated column %q", column.Name)
		}
	}
	if strings.TrimSpace(options["type_mappings"]) != "" {
		return managedConfig{}, errors.New("managed Snowflake profile rejects type mapping overrides until each mapping has real-service recovery evidence")
	}
	cfg.typeMappings = defaultSnowflakeTypeMappings()
	for _, column := range cfg.schemaContract.Columns {
		if managedSnowflakeColumnType(cfg, column) == "" {
			return managedConfig{}, fmt.Errorf("managed Snowflake has no lossless type mapping for source column %q type %q", column.Name, column.Type)
		}
	}
	if cfg.maxTransactionRows, err = parseManagedSnowflakeInt(options, "managed_max_transaction_rows", 1_000); err != nil {
		return managedConfig{}, err
	}
	if cfg.maxTransactionBytes, err = parseManagedSnowflakeInt64(options, "managed_max_transaction_bytes", 8<<20); err != nil {
		return managedConfig{}, err
	}
	if cfg.maxFragments, err = parseManagedSnowflakeInt(options, "managed_max_transaction_fragments", 128); err != nil {
		return managedConfig{}, err
	}
	if cfg.maxOpenConnections, err = parseManagedSnowflakeInt(options, "managed_max_open_conns", 8); err != nil {
		return managedConfig{}, err
	}
	if cfg.statementTimeoutSeconds, err = parseManagedSnowflakeInt(options, "managed_statement_timeout_seconds", 600); err != nil {
		return managedConfig{}, err
	}
	if cfg.hybridLockTimeoutSeconds, err = parseManagedSnowflakeInt(options, "managed_hybrid_table_lock_timeout_seconds", 600); err != nil {
		return managedConfig{}, err
	}
	cfg.validateEveryConnection = true
	if strings.ToLower(strings.TrimSpace(options["batch_mode"])) != "target" || strings.ToLower(strings.TrimSpace(options["batch_resolution"])) != "none" {
		return managedConfig{}, errors.New("managed Snowflake profile requires target batch mode and batch_resolution=none")
	}
	metaEnabled, err := parseManagedSnowflakeBoolOption(options, "meta_table_enabled", true)
	if err != nil {
		return managedConfig{}, err
	}
	disableTransactions, err := parseManagedSnowflakeBoolOption(options, "disable_transactions", false)
	if err != nil {
		return managedConfig{}, err
	}
	keepAlive, err := parseManagedSnowflakeBoolOption(options, "session_keep_alive", false)
	if err != nil {
		return managedConfig{}, err
	}
	if metaEnabled || disableTransactions || keepAlive {
		return managedConfig{}, errors.New("managed Snowflake profile requires owned receipts, transactions, and non-persistent sessions")
	}
	for _, option := range []string{
		"schema", "table", "staging_schema", "staging_table", "staging_suffix", "warehouse", "warehouse_size",
		"warehouse_auto_suspend", "warehouse_auto_resume", "meta_schema", "meta_table", "meta_pk_prefix",
	} {
		if strings.TrimSpace(options[option]) != "" {
			return managedConfig{}, fmt.Errorf("managed Snowflake profile rejects generic option %s", option)
		}
	}
	return cfg, nil
}

// ValidateManagedProfileOptions rejects options outside the constrained SQL
// profile before connector side effects occur.
func ValidateManagedProfileOptions(options map[string]string) error {
	allowed := map[string]struct{}{
		"dsn": {}, "flow_id": {}, "managed_profile": {}, "destination_revision_id": {},
		"batch_mode": {}, "batch_resolution": {}, "meta_table_enabled": {},
		"disable_transactions": {}, "session_keep_alive": {},
		"managed_account": {}, "managed_database": {}, "managed_schema": {}, "managed_table": {},
		"managed_receipts_table": {}, "managed_owner_role": {}, "managed_execution_role": {}, "managed_warehouse": {},
		"managed_snowflake_version": {}, "managed_target_created_on": {}, "managed_receipts_created_on": {},
		"managed_source_schema": {}, "managed_source_table": {},
		"managed_schema_contract": {}, "managed_schema_contract_hash": {},
		"managed_max_transaction_rows": {}, "managed_max_transaction_bytes": {},
		"managed_max_transaction_fragments": {}, "managed_max_open_conns": {},
		"managed_statement_timeout_seconds": {}, "managed_hybrid_table_lock_timeout_seconds": {},
	}
	for option := range options {
		if _, ok := allowed[option]; !ok {
			return fmt.Errorf("managed Snowflake profile does not allow option %s", option)
		}
	}
	return nil
}

func parseManagedSnowflakeBoolOption(options map[string]string, name string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(options[name])
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("managed Snowflake %s must be true or false", name)
	}
	return value, nil
}

func managedSnowflakeSessionParameter(parameters map[string]*string, name string) (string, bool) {
	for key, value := range parameters {
		if strings.EqualFold(strings.TrimSpace(key), name) && value != nil {
			return strings.TrimSpace(*value), true
		}
	}
	return "", false
}

func managedSnowflakeDSNDisablesOCSP(dsn string) bool {
	queryOffset := strings.IndexByte(dsn, '?')
	if queryOffset < 0 {
		return false
	}
	values, err := url.ParseQuery(dsn[queryOffset+1:])
	if err != nil {
		return true
	}
	for key, entries := range values {
		if !strings.EqualFold(strings.TrimSpace(key), "insecureMode") && !strings.EqualFold(strings.TrimSpace(key), "disableOCSPChecks") {
			continue
		}
		for _, entry := range entries {
			disabled, err := strconv.ParseBool(strings.TrimSpace(entry))
			if err != nil || disabled {
				return true
			}
		}
	}
	return false
}

func managedSnowflakeSessionParameterEnabled(parameters map[string]*string, name string) bool {
	raw, present := managedSnowflakeSessionParameter(parameters, name)
	if !present {
		return false
	}
	enabled, err := strconv.ParseBool(raw)
	return err == nil && enabled
}

func parseManagedSnowflakeInt(options map[string]string, name string, maximum int) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(options[name]))
	if err != nil || value < 1 || value > maximum {
		return 0, fmt.Errorf("managed Snowflake %s must be between 1 and %d", name, maximum)
	}
	return value, nil
}

func parseManagedSnowflakeInt64(options map[string]string, name string, maximum int64) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(options[name]), 10, 64)
	if err != nil || value < 1 || value > maximum {
		return 0, fmt.Errorf("managed Snowflake %s must be between 1 and %d", name, maximum)
	}
	return value, nil
}

func validateManagedSnowflakeRevision(value string) error {
	if value == "" || len(value) > 128 {
		return errors.New("managed Snowflake destination_revision_id must be a 1-128 character identifier")
	}
	for _, character := range value {
		if (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') && character != '-' && character != '_' && character != '.' {
			return errors.New("managed Snowflake destination_revision_id may contain only letters, digits, dash, underscore, and dot")
		}
	}
	return nil
}

func validateManagedSnowflakeUnquotedIdentifier(name, value string) error {
	if value == "" || len(value) > 255 || value != strings.ToUpper(value) || ((value[0] < 'A' || value[0] > 'Z') && value[0] != '_') {
		return fmt.Errorf("managed Snowflake %s must be one unquoted uppercase identifier", name)
	}
	for _, character := range value[1:] {
		if (character < 'A' || character > 'Z') && (character < '0' || character > '9') && character != '_' && character != '$' {
			return fmt.Errorf("managed Snowflake %s must be one unquoted uppercase identifier", name)
		}
	}
	return nil
}

func (d *Destination) acquireManagedSnowflakeConn(ctx context.Context) (*sql.Conn, error) {
	if d.db == nil {
		return nil, errors.New("managed Snowflake database is not open")
	}
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire managed Snowflake session: %w", err)
	}
	if d.managedConfig.validateEveryConnection {
		if err := d.configureAndValidateManagedSnowflakeSession(ctx, conn); err != nil {
			discardManagedSnowflakeConn(conn)
			return nil, err
		}
	}
	return conn, nil
}

func discardManagedSnowflakeConn(conn *sql.Conn) {
	if conn == nil {
		return
	}
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
}

func (d *Destination) configureAndValidateManagedSnowflakeSession(ctx context.Context, conn *sql.Conn) error {
	if d.managedConfig.statementTimeoutSeconds <= 0 || d.managedConfig.hybridLockTimeoutSeconds <= 0 {
		return errors.New("managed Snowflake session timeouts must be positive")
	}
	if _, err := conn.ExecContext(ctx, "USE SECONDARY ROLES NONE"); err != nil {
		return fmt.Errorf("disable managed Snowflake secondary roles: %w", err)
	}
	statement := fmt.Sprintf(`ALTER SESSION SET
  AUTOCOMMIT=TRUE,
  TRANSACTION_ABORT_ON_ERROR=TRUE,
  ABORT_DETACHED_QUERY=TRUE,
  ERROR_ON_NONDETERMINISTIC_MERGE=TRUE,
  ERROR_ON_NONDETERMINISTIC_UPDATE=TRUE,
  READ_LATEST_WRITES=TRUE,
  STATEMENT_TIMEOUT_IN_SECONDS=%d,
  HYBRID_TABLE_LOCK_TIMEOUT=%d,
  CLIENT_SESSION_KEEP_ALIVE=FALSE`, d.managedConfig.statementTimeoutSeconds, d.managedConfig.hybridLockTimeoutSeconds)
	if _, err := conn.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("configure managed Snowflake session: %w", err)
	}
	if err := d.validateManagedSnowflakeSessionIdentity(ctx, conn); err != nil {
		return err
	}
	parameters, err := loadManagedSnowflakeSessionParameters(ctx, conn)
	if err != nil {
		return err
	}
	expected := map[string]string{
		"AUTOCOMMIT": "true", "TRANSACTION_ABORT_ON_ERROR": "true", "ABORT_DETACHED_QUERY": "true",
		"ERROR_ON_NONDETERMINISTIC_MERGE": "true", "ERROR_ON_NONDETERMINISTIC_UPDATE": "true",
		"READ_LATEST_WRITES": "true", "CLIENT_SESSION_KEEP_ALIVE": "false",
		"STATEMENT_TIMEOUT_IN_SECONDS": strconv.Itoa(d.managedConfig.statementTimeoutSeconds),
		"HYBRID_TABLE_LOCK_TIMEOUT":    strconv.Itoa(d.managedConfig.hybridLockTimeoutSeconds),
	}
	for name, want := range expected {
		got, present := parameters[name]
		if !present || !strings.EqualFold(strings.TrimSpace(got), want) {
			return fmt.Errorf("managed Snowflake session parameter %s=%q, want %s", name, got, want)
		}
	}
	return nil
}

func (d *Destination) validateManagedSnowflakeSessionIdentity(ctx context.Context, queryer managedSnowflakeCatalogQueryer) error {
	var account, database, schema, role, warehouse, version string
	if err := queryer.QueryRowContext(ctx, `SELECT CURRENT_ACCOUNT_NAME(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_VERSION()`).Scan(
		&account, &database, &schema, &role, &warehouse, &version,
	); err != nil {
		return fmt.Errorf("read managed Snowflake session identity: %w", err)
	}
	checks := []struct{ name, actual, expected string }{
		{name: "account", actual: account, expected: d.managedConfig.account},
		{name: "database", actual: database, expected: d.managedConfig.database},
		{name: "schema", actual: schema, expected: d.managedConfig.schema},
		{name: "role", actual: role, expected: d.managedConfig.executionRole},
		{name: "warehouse", actual: warehouse, expected: d.managedConfig.warehouse},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.actual) != check.expected {
			return fmt.Errorf("managed Snowflake live %s=%q, configured=%q", check.name, check.actual, check.expected)
		}
	}
	if strings.TrimSpace(version) != d.managedConfig.snowflakeVersion {
		return fmt.Errorf("managed Snowflake CURRENT_VERSION()=%q, exact runtime pin=%q", version, d.managedConfig.snowflakeVersion)
	}
	var ownerRoleInSession sql.NullBool
	if err := queryer.QueryRowContext(ctx, `SELECT IS_ROLE_IN_SESSION(?)`, d.managedConfig.ownerRole).Scan(&ownerRoleInSession); err != nil {
		return fmt.Errorf("check managed Snowflake owner-role inheritance: %w", err)
	}
	if !ownerRoleInSession.Valid {
		return errors.New("managed Snowflake could not prove owner-role isolation from the execution session")
	}
	if ownerRoleInSession.Bool {
		return errors.New("managed Snowflake execution role inherits the object-owner role")
	}
	return nil
}

func loadManagedSnowflakeSessionParameters(ctx context.Context, queryer managedSnowflakeCatalogQueryer) (map[string]string, error) {
	rows, err := queryer.QueryContext(ctx, "SHOW PARAMETERS IN SESSION")
	if err != nil {
		return nil, fmt.Errorf("show managed Snowflake session parameters: %w", err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read managed Snowflake session parameter columns: %w", err)
	}
	indexes := make(map[string]int, len(columns))
	for index, column := range columns {
		indexes[strings.ToLower(strings.TrimSpace(column))] = index
	}
	keyIndex, hasKey := indexes["key"]
	valueIndex, hasValue := indexes["value"]
	if !hasKey || !hasValue {
		return nil, errors.New("snowflake SHOW PARAMETERS omitted key or value")
	}
	parameters := make(map[string]string)
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, fmt.Errorf("scan managed Snowflake session parameter: %w", err)
		}
		key := strings.ToUpper(strings.TrimSpace(sqlValueString(values[keyIndex])))
		if key == "" {
			return nil, errors.New("snowflake SHOW PARAMETERS returned an empty key")
		}
		if _, duplicate := parameters[key]; duplicate {
			return nil, fmt.Errorf("snowflake SHOW PARAMETERS repeated %s", key)
		}
		parameters[key] = strings.TrimSpace(sqlValueString(values[valueIndex]))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate managed Snowflake session parameters: %w", err)
	}
	return parameters, nil
}

func (d *Destination) validateManagedSnowflakeTransactions(ctx context.Context, conn *sql.Conn) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("managed Snowflake profile requires transaction support: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var transactionID any
	if err := tx.QueryRowContext(ctx, "SELECT CURRENT_TRANSACTION()").Scan(&transactionID); err != nil {
		return fmt.Errorf("managed Snowflake profile cannot observe an active transaction: %w", err)
	}
	if transactionID == nil {
		return errors.New("managed Snowflake CURRENT_TRANSACTION() is NULL inside BeginTx")
	}
	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("rollback managed Snowflake transaction probe: %w", err)
	}
	return nil
}

// ManagedSnowflakePoolStats exposes bounded database/sql pool accounting for
// operational diagnostics and the real-service resource-safety gate.
func (d *Destination) ManagedSnowflakePoolStats() sql.DBStats {
	if d.db == nil || d.managedProfile == "" {
		return sql.DBStats{}
	}
	return d.db.Stats()
}

// ManagedSnowflakeVersion reports the exact live service version admitted by Open.
func (d *Destination) ManagedSnowflakeVersion() string {
	if d.managedProfile == "" {
		return ""
	}
	return d.managedConfig.snowflakeVersion
}
