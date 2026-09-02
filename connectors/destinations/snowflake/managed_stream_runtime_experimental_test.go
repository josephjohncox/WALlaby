//go:build snowpipe_streaming_rest_experimental

package snowflake

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func experimentalStreamPolicy(t *testing.T, enabled bool) connector.SnowflakeDeploymentPolicy {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key, enabled)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	return policy
}

func experimentalStreamSpec(t *testing.T) connector.RuntimeSpec {
	t.Helper()
	_, options := streamValidOptions(t)
	return connector.RuntimeSpec{Name: "stream", Type: connector.EndpointSnowflake, Options: options}
}

func TestExperimentalStreamingRuntimeAssemblyRequiresDeploymentCapability(t *testing.T) {
	if !ManagedStreamingTransportAvailable() {
		t.Fatal("experimental build did not link the Streaming REST adapter")
	}
	spec := experimentalStreamSpec(t)
	for name, policy := range map[string]connector.SnowflakeDeploymentPolicy{
		"disabled base":      {},
		"streaming disabled": experimentalStreamPolicy(t, false),
	} {
		t.Run(name, func(t *testing.T) {
			openCalls := 0
			destination := NewDestination(policy)
			err := destination.open(context.Background(), spec, destinationFactories{
				openDB: func(string, string) (*sql.DB, error) { openCalls++; return nil, errors.New("must not open") },
			})
			if err == nil || openCalls != 0 {
				t.Fatalf("deployment-disabled assembly error/calls=%v/%d", err, openCalls)
			}
		})
	}
}

func TestExperimentalStreamingRuntimeCloseWaitsForOperationSnapshot(t *testing.T) {
	policy := experimentalStreamPolicy(t, true)
	streamingPolicy, err := policy.StreamingRESTPolicy()
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectClose()
	destination := NewDestination(policy)
	destination.spec = experimentalStreamSpec(t)
	destination.db = db
	destination.managedProfile = connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
	destination.streamRuntimeProtocol = newFakeStreamProtocol()
	destination.streamConfig = streamTestConfig(t)
	destination.streamCatalogFingerprint = strings.Repeat("a", 64)
	destination.streamingPolicy = streamingPolicy
	_, unlock, err := destination.lockStreamRuntime()
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- destination.Close(context.Background()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned while an operation snapshot was active: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	unlock()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExperimentalStreamingRuntimeRejectsMissingComposedProtocol(t *testing.T) {
	policy := experimentalStreamPolicy(t, true)
	destination := NewDestination(policy)
	destination.spec = experimentalStreamSpec(t)
	destination.db = &sql.DB{}
	destination.managedProfile = connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
	streamingPolicy, err := policy.StreamingRESTPolicy()
	if err != nil {
		t.Fatal(err)
	}
	destination.streamingPolicy = streamingPolicy
	if _, unlock, err := destination.lockStreamRuntime(); err == nil {
		unlock()
		t.Fatal("missing composed protocol unexpectedly admitted")
	}
	if err := destination.InitializeManagedDelivery(context.Background()); err == nil {
		t.Fatal("InitializeManagedDelivery accepted a missing composed protocol")
	}
}

func TestExperimentalPreparedStreamingRejectsRuntimeDriftBeforeSideEffects(t *testing.T) {
	policy := experimentalStreamPolicy(t, true)
	streamingPolicy, err := policy.StreamingRESTPolicy()
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeStreamProtocol()
	destination := NewDestination(policy)
	destination.spec = experimentalStreamSpec(t)
	destination.db = &sql.DB{}
	destination.managedProfile = connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
	destination.streamRuntimeProtocol = fake
	destination.streamConfig = streamTestConfig(t)
	destination.streamConfig.configDigest = strings.Repeat("c", 64)
	destination.streamCatalogFingerprint = strings.Repeat("b", 64)
	destination.streamingPolicy = streamingPolicy
	prepared := &preparedManagedStreamTransaction{
		destination: destination,
		intent:      connector.DeliveryIntent{LogicalBatchID: "prepared-drift"},
		plan: managedStreamPlan{
			catalogFingerprint: strings.Repeat("a", 64),
			receipt:            managedStreamReceipt{catalogFingerprint: strings.Repeat("a", 64)},
		},
		runtimeFingerprint: strings.Repeat("a", 64),
		configDigest:       strings.Repeat("c", 64),
	}
	if _, err := prepared.Apply(context.Background()); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("prepared runtime drift error=%v, want conflict", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.appendedPayloads) != 0 || len(fake.requests) != 0 || len(fake.receipts) != 0 {
		t.Fatalf("prepared drift produced side effects: appends=%d requests=%d receipts=%d", len(fake.appendedPayloads), len(fake.requests), len(fake.receipts))
	}
}

func TestExperimentalPreparedStreamingRejectsConfigDriftBeforeSideEffects(t *testing.T) {
	policy := experimentalStreamPolicy(t, true)
	streamingPolicy, err := policy.StreamingRESTPolicy()
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeStreamProtocol()
	destination := NewDestination(policy)
	destination.spec = experimentalStreamSpec(t)
	destination.db = &sql.DB{}
	destination.managedProfile = connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
	destination.streamRuntimeProtocol = fake
	destination.streamConfig = streamTestConfig(t)
	destination.streamConfig.configDigest = strings.Repeat("d", 64)
	destination.streamCatalogFingerprint = strings.Repeat("a", 64)
	destination.streamingPolicy = streamingPolicy
	prepared := &preparedManagedStreamTransaction{
		destination: destination,
		intent:      connector.DeliveryIntent{LogicalBatchID: "prepared-config-drift"},
		plan: managedStreamPlan{
			catalogFingerprint: strings.Repeat("a", 64),
			receipt:            managedStreamReceipt{catalogFingerprint: strings.Repeat("a", 64)},
		},
		runtimeFingerprint: strings.Repeat("a", 64),
		configDigest:       strings.Repeat("c", 64),
	}
	if _, err := prepared.Apply(context.Background()); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("prepared config drift error=%v, want conflict", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.appendedPayloads) != 0 || len(fake.requests) != 0 || len(fake.receipts) != 0 {
		t.Fatalf("prepared config drift produced side effects: appends=%d requests=%d receipts=%d", len(fake.appendedPayloads), len(fake.requests), len(fake.receipts))
	}
}

func expectExperimentalStreamSession(mock sqlmock.Sqlmock, cfg streamConfig) {
	mock.ExpectExec(regexp.QuoteMeta("USE SECONDARY ROLES NONE")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("ALTER SESSION SET") + ".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT CURRENT_ACCOUNT_NAME(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_VERSION()`)).
		WillReturnRows(sqlmock.NewRows([]string{"account", "database", "schema", "role", "warehouse", "version"}).AddRow(cfg.account, cfg.database, cfg.schema, cfg.executionRole, cfg.warehouse, cfg.snowflakeVersion))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT IS_ROLE_IN_SESSION(?)`)).WithArgs(cfg.ownerRole).
		WillReturnRows(sqlmock.NewRows([]string{"is_role_in_session"}).AddRow(false))
	parameters := sqlmock.NewRows([]string{"key", "value"})
	for key, value := range map[string]string{
		"AUTOCOMMIT": "true", "TRANSACTION_ABORT_ON_ERROR": "true", "ABORT_DETACHED_QUERY": "true",
		"ERROR_ON_NONDETERMINISTIC_MERGE": "true", "ERROR_ON_NONDETERMINISTIC_UPDATE": "true",
		"READ_LATEST_WRITES": "true", "CLIENT_SESSION_KEEP_ALIVE": "false",
		"STATEMENT_TIMEOUT_IN_SECONDS": fmt.Sprint(cfg.statementTimeoutSeconds),
		"HYBRID_TABLE_LOCK_TIMEOUT":    fmt.Sprint(cfg.statementTimeoutSeconds),
	} {
		parameters.AddRow(key, value)
	}
	mock.ExpectQuery(regexp.QuoteMeta("SHOW PARAMETERS IN SESSION")).WillReturnRows(parameters)
}

func experimentalGrantRows(grants map[string][]string) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"privilege", "grantee_name"})
	roles := make([]string, 0, len(grants))
	for role := range grants {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	for _, role := range roles {
		privileges := append([]string(nil), grants[role]...)
		sort.Strings(privileges)
		for _, privilege := range privileges {
			rows.AddRow(privilege, role)
		}
	}
	return rows
}

func experimentalColumnRows(columns map[string]managedColumnSnapshot) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT", "IS_IDENTITY", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_MAXIMUM_LENGTH"})
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		column := columns[name]
		dataType := column.dataType
		var precision, scale, datetimePrecision, length any
		if strings.HasPrefix(dataType, "NUMBER") {
			dataType, precision, scale = "NUMBER", column.numericPrecision, column.numericScale
		}
		if strings.HasPrefix(dataType, "TIMESTAMP_") {
			dataType, datetimePrecision = "TIMESTAMP_TZ", column.datetimePrecision
		}
		if column.characterMaximumLength >= 0 {
			length = column.characterMaximumLength
		}
		rows.AddRow(name, dataType, "NO", nil, "NO", precision, scale, datetimePrecision, length)
	}
	return rows
}

func experimentalConstraintRows(constraints []managedConstraintSnapshot) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"CONSTRAINT_NAME", "CONSTRAINT_TYPE", "ENFORCED", "COLUMN_NAME", "ORDINAL_POSITION"})
	for _, constraint := range constraints {
		for index, column := range constraint.columns {
			rows.AddRow(constraint.name, constraint.constraintType, constraint.enforced, column, index+1)
		}
	}
	return rows
}

func expectExperimentalStreamCatalog(mock sqlmock.Sqlmock, cfg streamConfig) {
	catalog := validManagedStreamCatalog(cfg)
	mock.ExpectQuery("SELECT DEFINITION.*PIPES").WithArgs(cfg.schema, cfg.pipe).
		WillReturnRows(sqlmock.NewRows([]string{"definition", "comment", "created", "auto_ingest"}).AddRow(catalog.pipe.definition, catalog.pipe.comment, catalog.pipe.createdOn, true))
	mock.ExpectQuery("SHOW GRANTS ON PIPE").WillReturnRows(experimentalGrantRows(catalog.pipe.grants))
	for _, table := range []managedTableSnapshot{catalog.target, catalog.receipts, catalog.channel, catalog.requests} {
		hybrid := "NO"
		if table.kind == "HYBRID TABLE" {
			hybrid = "YES"
		}
		mock.ExpectQuery("SELECT IS_HYBRID.*TABLES").WillReturnRows(sqlmock.NewRows([]string{"is_hybrid", "comment", "created"}).AddRow(hybrid, table.comment, table.createdOn))
		mock.ExpectQuery("SELECT LEFT\\(GET_DDL").WillReturnRows(sqlmock.NewRows([]string{"ddl"}).AddRow("CREATE " + table.kind + " VALIDATED"))
		mock.ExpectQuery("SHOW GRANTS ON TABLE").WillReturnRows(experimentalGrantRows(table.grants))
		mock.ExpectQuery("SELECT COLUMN_NAME, DATA_TYPE.*COLUMNS").WillReturnRows(experimentalColumnRows(table.columns))
		mock.ExpectQuery("SELECT TC.CONSTRAINT_NAME.*TABLE_CONSTRAINTS").WillReturnRows(experimentalConstraintRows(table.constraints))
		mock.ExpectQuery("SELECT COUNT\\(\\*\\).*TABLE_CONSTRAINTS").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	}
	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*PIPES").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\).*TASKS").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
}

func TestExperimentalStreamingRuntimeFullAssemblyUsesValidatedSQLAndREST(t *testing.T) {
	cfg := streamTestConfig(t)
	policy := experimentalStreamPolicy(t, true)
	streamingPolicy, err := policy.StreamingRESTPolicy()
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := NewDestination(policy)
	destination.db = db
	destination.streamConfig = cfg
	destination.managedConfig = destination.streamSessionShim(cfg)
	expectExperimentalStreamSession(mock, cfg)
	expectExperimentalStreamCatalog(mock, cfg)
	protocol, fingerprint, err := destination.openManagedStreamRuntime(context.Background(), db, cfg, streamingPolicy)
	if err != nil {
		t.Fatal(err)
	}
	composed, ok := protocol.(*composedStreamProtocol)
	if !ok || composed.streamTransport == nil || composed.streamStateStore == nil || fingerprint == "" {
		t.Fatalf("full runtime assembly=%T/%v/%v/%q", protocol, composed.streamTransport, composed.streamStateStore, fingerprint)
	}
	if _, ok := composed.streamTransport.(*streamRESTTransport); !ok {
		t.Fatalf("transport=%T, want real REST transport", composed.streamTransport)
	}
	if _, ok := composed.streamStateStore.(*sqlStreamProtocol); !ok {
		t.Fatalf("state store=%T, want validated SQL protocol", composed.streamStateStore)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExperimentalStreamingRuntimeAssemblyComposesRESTAndSQLAndRollsBack(t *testing.T) {
	spec := experimentalStreamSpec(t)
	policy := experimentalStreamPolicy(t, true)
	for _, test := range []struct {
		name        string
		openRuntime func(context.Context, *sql.DB, streamConfig, connector.SnowflakeStreamingRESTPolicy) (streamProtocol, string, error)
		wantErr     string
	}{
		{name: "success", openRuntime: func(_ context.Context, db *sql.DB, _ streamConfig, capability connector.SnowflakeStreamingRESTPolicy) (streamProtocol, string, error) {
			if !capability.Enabled() || db == nil {
				return nil, "", errors.New("missing runtime authority")
			}
			return newFakeStreamProtocol(), strings.Repeat("a", 64), nil
		}},
		{name: "catalog failure", openRuntime: func(context.Context, *sql.DB, streamConfig, connector.SnowflakeStreamingRESTPolicy) (streamProtocol, string, error) {
			return nil, "", errors.New("catalog rejected")
		}, wantErr: "catalog rejected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectPing()
			mock.ExpectClose()
			destination := NewDestination(policy)
			err = destination.open(context.Background(), spec, destinationFactories{openDB: func(string, string) (*sql.DB, error) { return db, nil }, openStreamRuntime: test.openRuntime})
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) || destination.db != nil || destination.streamRuntimeProtocol != nil {
					t.Fatalf("rollback error/state=%v/%v/%v", err, destination.db, destination.streamRuntimeProtocol)
				}
			} else {
				if err != nil || destination.db == nil || destination.streamRuntimeProtocol == nil || destination.streamCatalogFingerprint == "" {
					t.Fatalf("assembled state error/db/protocol/fingerprint=%v/%v/%v/%q", err, destination.db, destination.streamRuntimeProtocol, destination.streamCatalogFingerprint)
				}
				_, unlock, err := destination.lockStreamRuntime()
				if err != nil {
					t.Fatal(err)
				}
				unlock()
				if err := destination.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
