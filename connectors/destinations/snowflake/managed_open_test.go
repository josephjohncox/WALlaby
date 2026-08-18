package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

func TestManagedSnowflakeApplyDDLRejectsBeforeExecutor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := &Destination{db: db, managedProfile: connector.ManagedProfilePostgresToSnowflakeSQLV1}
	err = destination.ApplyDDL(context.Background(), connector.Schema{Namespace: "mapped", Name: "events"}, connector.Record{Operation: connector.OpDDL, DDL: "ALTER TABLE mapped.events ADD COLUMN status text"})
	if err == nil || !strings.Contains(err.Error(), "managed Snowflake SQL profile rejects DDL") {
		t.Fatalf("ApplyDDL error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("managed DDL invoked executor: %v", err)
	}
}

func TestManagedSnowflakeCapabilitiesAreScopedToTheNamedProfile(t *testing.T) {
	t.Parallel()
	destination := &Destination{}
	generic, err := destination.CapabilitiesFor(connector.RuntimeSpec{Type: connector.EndpointSnowflake})
	if err != nil {
		t.Fatal(err)
	}
	if generic.Delivery.TransactionalBatch || generic.Delivery.IdempotentReplay || generic.Delivery.ReplaySafe {
		t.Fatalf("generic Snowflake mode inherited managed guarantees: %+v", generic.Delivery)
	}
	managed, err := destination.CapabilitiesFor(connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: map[string]string{
		"managed_profile": connector.ManagedProfilePostgresToSnowflakeSQLV1,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !managed.Delivery.TransactionalBatch || !managed.Delivery.IdempotentReplay || !managed.Delivery.ReplaySafe || managed.Delivery.ExecutesDDL {
		t.Fatalf("named Snowflake profile capabilities=%+v", managed.Delivery)
	}
	if !managed.TableWrites.Upsert || !managed.TableWrites.ExplicitKey || managed.TableWrites.Append || managed.TableWrites.WatermarkGuard {
		t.Fatalf("named Snowflake profile table writes=%+v", managed.TableWrites)
	}
	if managed.Support != connector.SupportExperimental {
		t.Fatalf("unproven managed Snowflake support=%s", managed.Support)
	}
	for _, profile := range []string{
		connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
		connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
	} {
		capabilities, err := destination.CapabilitiesFor(connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: map[string]string{"managed_profile": profile}})
		if err != nil {
			t.Fatal(err)
		}
		if !capabilities.TableWrites.Append || capabilities.TableWrites.Upsert || capabilities.TableWrites.ExplicitKey || capabilities.TableWrites.WatermarkGuard {
			t.Fatalf("managed append profile %s table writes=%+v", profile, capabilities.TableWrites)
		}
		if capabilities.Delivery.TransactionalBatch || !capabilities.Delivery.IdempotentReplay || !capabilities.Delivery.ReplaySafe || capabilities.Delivery.ExecutesDDL {
			t.Fatalf("managed append profile %s delivery=%+v", profile, capabilities.Delivery)
		}
	}
	wantProfiles := []connector.CapabilityProfileID{
		CapabilityProfileBase,
		CapabilityProfileManagedSQL,
		CapabilityProfileManagedStaged,
		CapabilityProfileManagedStreaming,
	}
	if got := destination.CapabilityProfileIDs(); !reflect.DeepEqual(got, wantProfiles) {
		t.Fatalf("Snowflake capability profiles=%v, want closed set %v", got, wantProfiles)
	}
	if _, err := destination.ClassifyCapabilityProfile(connector.RuntimeSpec{Type: connector.EndpointSnowflake, Options: map[string]string{"managed_profile": "unknown"}}); err == nil {
		t.Fatal("unknown Snowflake profile was admitted outside the closed registry")
	}
}

func TestManagedSnowflakeOpenStateInitializesEveryProfileAuthority(t *testing.T) {
	t.Parallel()
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, test := range []struct {
		name        string
		destination *Destination
		wantErr     error
	}{
		{name: "SQL", destination: &Destination{
			db: db, managedProfile: connector.ManagedProfilePostgresToSnowflakeSQLV1,
			managedConfig: managedConfig{destinationRevision: "sql-v1", receiptsTable: "RECEIPTS", schemaContractHash: strings.Repeat("a", 64)},
		}},
		{name: "staged COPY", destination: &Destination{
			db: db, managedProfile: connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
			stagedConfig:             stagedConfig{destinationRevision: "staged-v1", receiptsTable: "RECEIPTS", schemaContractHash: strings.Repeat("b", 64)},
			stagedCatalogFingerprint: strings.Repeat("c", 64),
		}},
		{name: "Streaming REST", destination: &Destination{
			db: db, managedProfile: connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
			streamConfig:             streamConfig{destinationRevision: "stream-v1", receiptsTable: "RECEIPTS", channelStateTable: "CHANNELS", schemaContractHash: strings.Repeat("d", 64)},
			streamCatalogFingerprint: strings.Repeat("e", 64),
		}, wantErr: ErrManagedStreamingTransportUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.destination.InitializeManagedDelivery(context.Background())
			if test.wantErr == nil && err != nil {
				t.Fatalf("Open-established %s authority rejected: %v", test.name, err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("Open-established %s authority error=%v, want %v", test.name, err, test.wantErr)
			}
		})
	}
}

func TestManagedSnowflakeValidatesEveryPinnedSessionContractAndVersion(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg, _ := managedCatalogFixture(t)
	cfg.statementTimeoutSeconds = 120
	cfg.hybridLockTimeoutSeconds = 60
	cfg.validateEveryConnection = true
	destination := &Destination{db: db, managedProfile: cfg.profile, managedConfig: cfg}
	alterSession := regexp.QuoteMeta("ALTER SESSION SET") + ".*"
	expectSession := func(version string, ownerRoleInSession, includeParameters bool) {
		mock.ExpectExec(regexp.QuoteMeta("USE SECONDARY ROLES NONE")).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(alterSession).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT CURRENT_ACCOUNT_NAME(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_VERSION()`)).
			WillReturnRows(sqlmock.NewRows([]string{"account", "database", "schema", "role", "warehouse", "version"}).
				AddRow(cfg.account, cfg.database, cfg.schema, cfg.executionRole, cfg.warehouse, version))
		if version != cfg.snowflakeVersion {
			return
		}
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT IS_ROLE_IN_SESSION(?)`)).WithArgs(cfg.ownerRole).
			WillReturnRows(sqlmock.NewRows([]string{"is_role_in_session"}).AddRow(ownerRoleInSession))
		if !includeParameters {
			return
		}
		rows := sqlmock.NewRows([]string{"key", "value"})
		for key, value := range map[string]string{
			"AUTOCOMMIT": "true", "TRANSACTION_ABORT_ON_ERROR": "true", "ABORT_DETACHED_QUERY": "true",
			"ERROR_ON_NONDETERMINISTIC_MERGE": "true", "ERROR_ON_NONDETERMINISTIC_UPDATE": "true",
			"READ_LATEST_WRITES": "true", "CLIENT_SESSION_KEEP_ALIVE": "false",
			"STATEMENT_TIMEOUT_IN_SECONDS": "120", "HYBRID_TABLE_LOCK_TIMEOUT": "60",
		} {
			rows.AddRow(key, value)
		}
		mock.ExpectQuery(regexp.QuoteMeta("SHOW PARAMETERS IN SESSION")).WillReturnRows(rows)
	}

	expectSession(cfg.snowflakeVersion, false, true)
	conn, err := destination.acquireManagedSnowflakeConn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	expectSession("different-version", false, false)
	if _, err := destination.acquireManagedSnowflakeConn(context.Background()); err == nil || !strings.Contains(err.Error(), "exact runtime pin") {
		t.Fatalf("rotated session version error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSnowflakeRejectsExecutionSessionThatInheritsOwnerRole(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg, _ := managedCatalogFixture(t)
	cfg.statementTimeoutSeconds = 120
	cfg.hybridLockTimeoutSeconds = 60
	cfg.validateEveryConnection = true
	destination := &Destination{db: db, managedProfile: cfg.profile, managedConfig: cfg}
	mock.ExpectExec(regexp.QuoteMeta("USE SECONDARY ROLES NONE")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("ALTER SESSION SET") + ".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT CURRENT_ACCOUNT_NAME(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_VERSION()`)).
		WillReturnRows(sqlmock.NewRows([]string{"account", "database", "schema", "role", "warehouse", "version"}).
			AddRow(cfg.account, cfg.database, cfg.schema, cfg.executionRole, cfg.warehouse, cfg.snowflakeVersion))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT IS_ROLE_IN_SESSION(?)`)).WithArgs(cfg.ownerRole).
		WillReturnRows(sqlmock.NewRows([]string{"is_role_in_session"}).AddRow(true))
	if _, err := destination.acquireManagedSnowflakeConn(context.Background()); err == nil || !strings.Contains(err.Error(), "inherits the object-owner role") {
		t.Fatalf("owner-role inheritance error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSnowflakeCleanStartRejectsUnreceiptedTargetRows(t *testing.T) {
	t.Parallel()
	if err := validateManagedSnowflakeCleanStartState(0, true); err == nil || !strings.Contains(err.Error(), "empty target") {
		t.Fatalf("unreceipted target error=%v", err)
	}
	for _, state := range []struct {
		receipts int
		target   bool
	}{{receipts: 0, target: false}, {receipts: 1, target: true}, {receipts: 1, target: false}} {
		if err := validateManagedSnowflakeCleanStartState(state.receipts, state.target); err != nil {
			t.Fatalf("valid restart state %+v: %v", state, err)
		}
	}
}

func TestManagedSnowflakeConfigRequiresExactSecureRevisionAndSchemaContract(t *testing.T) {
	t.Parallel()
	schema := managedTestSchema()
	schema.Namespace, schema.Name = "PUBLIC", "WIDGETS"
	encoded, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	hash := mustManagedSchemaHash(t, schema)
	spec := connector.RuntimeSpec{Name: "snowflake", Type: connector.EndpointSnowflake, Options: map[string]string{
		"dsn":                                       managedSnowflakeTestDSN(t, nil),
		"flow_id":                                   "flow-1",
		"managed_profile":                           connector.ManagedProfilePostgresToSnowflakeSQLV1,
		"destination_revision_id":                   "snowflake-v1",
		"batch_mode":                                "target",
		"batch_resolution":                          "none",
		"meta_table_enabled":                        "false",
		"disable_transactions":                      "false",
		"session_keep_alive":                        "false",
		"managed_account":                           "ACCOUNT",
		"managed_database":                          "DB",
		"managed_schema":                            "PUBLIC",
		"managed_table":                             "WIDGETS",
		"managed_receipts_table":                    "WALLABY_RECEIPTS",
		"managed_owner_role":                        "OWNER_ROLE",
		"managed_execution_role":                    "ROLE",
		"managed_warehouse":                         "WH",
		"managed_snowflake_version":                 "9.99.0",
		"managed_target_created_on":                 "2026-01-01T00:00:00.000000000+00:00",
		"managed_receipts_created_on":               "2026-01-01T00:00:01.000000000+00:00",
		"managed_source_schema":                     "public",
		"managed_source_table":                      "widgets",
		"managed_schema_contract":                   string(encoded),
		"managed_schema_contract_hash":              hash,
		"managed_max_transaction_rows":              "1000",
		"managed_max_transaction_bytes":             "8388608",
		"managed_max_transaction_fragments":         "64",
		"managed_max_open_conns":                    "4",
		"managed_statement_timeout_seconds":         "120",
		"managed_hybrid_table_lock_timeout_seconds": "60",
	}}
	cfg, err := managedConfigFromSpec(spec.Options["dsn"], spec)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.schemaContractHash != hash || cfg.destinationRevision != "snowflake-v1" || cfg.maxOpenConnections != 4 {
		t.Fatalf("config=%+v", cfg)
	}
	for _, exact := range []string{" ", " leading", "trailing ", " both "} {
		exactSpec := spec
		exactSpec.Options = make(map[string]string, len(spec.Options))
		for key, value := range spec.Options {
			exactSpec.Options[key] = value
		}
		exactSpec.Options["managed_source_schema"] = exact
		exactSpec.Options["managed_source_table"] = exact
		exactConfig, err := managedConfigFromSpec(exactSpec.Options["dsn"], exactSpec)
		if err != nil {
			t.Fatalf("exact source identifier %q rejected: %v", exact, err)
		}
		if exactConfig.sourceSchema != exact || exactConfig.sourceTable != exact {
			t.Fatalf("exact source identifier changed: schema/table=%q/%q", exactConfig.sourceSchema, exactConfig.sourceTable)
		}
	}

	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "missing flow binding", key: "flow_id", value: "", want: "flow, account"},
		{name: "empty source schema", key: "managed_source_schema", value: "", want: "NUL-free source relation"},
		{name: "NUL source table", key: "managed_source_table", value: "bad\x00table", want: "NUL-free source relation"},
		{name: "fakesnow HTTP", key: "dsn", value: managedSnowflakeTestDSN(t, func(cfg *gosnowflake.Config) { cfg.Protocol = "http" }), want: "prohibited credential or connection control"},
		{name: "OCSP fail-open", key: "dsn", value: managedSnowflakeTestDSN(t, func(cfg *gosnowflake.Config) { cfg.OCSPFailOpen = gosnowflake.OCSPFailOpenTrue }), want: "prohibited credential or connection control"},
		{name: "deprecated insecure mode", key: "dsn", value: spec.Options["dsn"] + "&insecureMode=true", want: "prohibited credential or connection control"},
		{name: "disabled OCSP checks", key: "dsn", value: spec.Options["dsn"] + "&disableOCSPChecks=true", want: "prohibited credential or connection control"},
		{name: "password authentication", key: "dsn", value: "user:pass@account/DB/PUBLIC?warehouse=WH&role=ROLE&ocspFailOpen=false&READ_LATEST_WRITES=true&TIMEZONE=UTC", want: "prohibited credential"},
		{name: "stale cross-session reads", key: "dsn", value: managedSnowflakeTestDSN(t, func(cfg *gosnowflake.Config) { delete(cfg.Params, "READ_LATEST_WRITES") }), want: "READ_LATEST_WRITES=true"},
		{name: "non-UTC session", key: "dsn", value: managedSnowflakeTestDSN(t, func(cfg *gosnowflake.Config) { value := "local"; cfg.Params["TIMEZONE"] = &value }), want: "TIMEZONE=UTC"},
		{name: "persistent sessions", key: "dsn", value: managedSnowflakeTestDSN(t, func(cfg *gosnowflake.Config) { value := "true"; cfg.Params["CLIENT_SESSION_KEEP_ALIVE"] = &value }), want: "CLIENT_SESSION_KEEP_ALIVE=true"},
		{name: "schema content changed", key: "managed_schema_contract_hash", value: strings.Repeat("0", 64), want: "schema contract hash"},
		{name: "revision delimiter", key: "destination_revision_id", value: "snowflake:v2", want: "letters, digits"},
		{name: "unbounded runtime version", key: "managed_snowflake_version", value: "9.99.0\nother", want: "single-line"},
		{name: "invalid target creation identity", key: "managed_target_created_on", value: "pending", want: "YYYY-MM-DD"},
		{name: "transaction overflow", key: "managed_max_transaction_bytes", value: "8388609", want: "between 1 and 8388608"},
		{name: "pool overflow", key: "managed_max_open_conns", value: "9", want: "between 1 and 8"},
		{name: "invalid transaction boolean", key: "disable_transactions", value: "sometimes", want: "must be true or false"},
		{name: "generic warehouse mutation", key: "warehouse_size", value: "XSMALL", want: "does not allow option warehouse_size"},
		{name: "unknown managed option", key: "managed_typo", value: "true", want: "does not allow option managed_typo"},
		{name: "inline type mapping override", key: "type_mappings", value: `{"text":"VARIANT"}`, want: "does not allow option type_mappings"},
		{name: "removed type mapping file", key: "type_mappings_file", value: "mappings.json", want: "does not allow option type_mappings_file"},
		{name: "unknown nullability", key: "managed_schema_contract", value: `{"Name":"WIDGETS","Namespace":"PUBLIC","Columns":[{"Name":"id","Type":"int8","TypeMetadata":{"primary_key":"true"}}]}`, want: "schema contract hash"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copySpec := spec
			copySpec.Options = make(map[string]string, len(spec.Options))
			for key, value := range spec.Options {
				copySpec.Options[key] = value
			}
			copySpec.Options[tt.key] = tt.value
			_, err := managedConfigFromSpec(copySpec.Options["dsn"], copySpec)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want substring %q", err, tt.want)
			}
		})
	}

	unknownGeneration := schema
	unknownGeneration.Columns = append([]connector.Column(nil), schema.Columns...)
	unknownGeneration.Columns[0].TypeMetadata = map[string]string{"primary_key": "true", "nullability_known": "true"}
	unknownJSON, err := json.Marshal(unknownGeneration)
	if err != nil {
		t.Fatal(err)
	}
	unknownSpec := spec
	unknownSpec.Options = make(map[string]string, len(spec.Options))
	for key, value := range spec.Options {
		unknownSpec.Options[key] = value
	}
	unknownSpec.Options["managed_schema_contract"] = string(unknownJSON)
	if _, err := managedConfigFromSpec(unknownSpec.Options["dsn"], unknownSpec); err == nil || !strings.Contains(err.Error(), "generated_known=true") {
		t.Fatalf("unknown generated status error=%v", err)
	}

	lossy := schema
	lossy.Columns = append([]connector.Column(nil), schema.Columns...)
	lossy.Columns[1].Type = "numeric"
	lossyJSON, err := json.Marshal(lossy)
	if err != nil {
		t.Fatal(err)
	}
	lossyHash := mustManagedSchemaHash(t, lossy)
	lossySpec := spec
	lossySpec.Options = make(map[string]string, len(spec.Options))
	for key, value := range spec.Options {
		lossySpec.Options[key] = value
	}
	lossySpec.Options["managed_schema_contract"] = string(lossyJSON)
	lossySpec.Options["managed_schema_contract_hash"] = lossyHash
	if _, err := managedConfigFromSpec(lossySpec.Options["dsn"], lossySpec); err == nil || !strings.Contains(err.Error(), "no lossless type mapping") {
		t.Fatalf("lossy type mapping error=%v", err)
	}

	for _, sourceType := range []string{"json", "jsonb", "text[]", "numeric[]"} {
		lossy := schema
		lossy.Columns = append([]connector.Column(nil), schema.Columns...)
		lossy.Columns[1].Type = sourceType
		lossyJSON, err := json.Marshal(lossy)
		if err != nil {
			t.Fatal(err)
		}
		lossyHash := mustManagedSchemaHash(t, lossy)
		lossySpec := spec
		lossySpec.Options = cloneStringMap(spec.Options)
		lossySpec.Options["managed_schema_contract"] = string(lossyJSON)
		lossySpec.Options["managed_schema_contract_hash"] = lossyHash
		if _, err := managedConfigFromSpec(lossySpec.Options["dsn"], lossySpec); err == nil || !strings.Contains(err.Error(), "no lossless type mapping") {
			t.Errorf("lossy source type %q error=%v", sourceType, err)
		}
	}
}

func managedSnowflakeTestDSN(t *testing.T, mutate func(*gosnowflake.Config)) string {
	t.Helper()
	readLatestWrites := "true"
	timezone := "UTC"
	cfg := &gosnowflake.Config{
		Account: "account", User: "user", Database: "DB", Schema: "PUBLIC", Warehouse: "WH", Role: "ROLE",
		Protocol: "https", Authenticator: gosnowflake.AuthTypeJwt,
		OCSPFailOpen: gosnowflake.OCSPFailOpenFalse,
		Params:       map[string]*string{"READ_LATEST_WRITES": &readLatestWrites, "TIMEZONE": &timezone},
	}
	if mutate != nil {
		mutate(cfg)
	}
	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return dsn
}

func cloneStringMap(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
