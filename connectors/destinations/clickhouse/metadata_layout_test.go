package clickhouse

import (
	"context"
	"errors"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func mustCompileMetaLayout(t *testing.T, engine, orderBy string) metaTableLayout {
	t.Helper()
	layout, err := compileMetaTableLayout(engine, orderBy)
	if err != nil {
		t.Fatal(err)
	}
	return layout
}

func TestCompileMetaTableLayoutAdmitsFixedEngineAndMetadataColumns(t *testing.T) {
	for _, tc := range []struct {
		name, engine, orderBy, wantOrderBy string
	}{
		{"defaults", "", "", "`flow_id`, `source_schema`, `source_table`, `key_json`"},
		{"explicit engine case-insensitive", " mergetree ", "", "`flow_id`, `source_schema`, `source_table`, `key_json`"},
		{"single column", "MergeTree", "flow_id", "`flow_id`"},
		{"whitespace tolerant", "MERGETREE", " lsn ,\tsynced_at\n, key_json ", "`lsn`, `synced_at`, `key_json`"},
		{"all eight columns reordered", "", "key_json,operation,lsn,is_deleted,synced_at,source_table,source_schema,flow_id", "`key_json`, `operation`, `lsn`, `is_deleted`, `synced_at`, `source_table`, `source_schema`, `flow_id`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			layout, err := compileMetaTableLayout(tc.engine, tc.orderBy)
			if err != nil {
				t.Fatal(err)
			}
			if layout.engine != "MergeTree" || layout.orderBy != tc.wantOrderBy {
				t.Fatalf("layout=%+v, want engine MergeTree orderBy %q", layout, tc.wantOrderBy)
			}
		})
	}
}

func TestCompileMetaTableLayoutRejectsHostileInput(t *testing.T) {
	huge := strings.Repeat("flow_id,", 1<<16)
	for _, tc := range []struct {
		name, engine, orderBy string
	}{
		{"other engine", "ReplacingMergeTree", ""},
		{"engine with arguments", "MergeTree()", ""},
		{"engine fragment", "MergeTree() SETTINGS x=1", ""},
		{"engine statement injection", "MergeTree(); DROP TABLE victim; --", ""},
		{"engine function", "Distributed('c','d','t')", ""},
		{"engine comment", "MergeTree -- x", ""},
		{"engine huge", strings.Repeat("M", 1<<20), ""},
		{"order by function", "", "cityHash64(flow_id)"},
		{"order by expression", "", "flow_id + 1"},
		{"order by semicolon", "", "flow_id); DROP TABLE victim; --"},
		{"order by line comment", "", "flow_id -- x"},
		{"order by block comment", "", "flow_id /* x */"},
		{"order by backtick", "", "`flow_id`"},
		{"order by escaped backtick", "", "flow_id`, x"},
		{"order by backslash", "", `flow_id\`},
		{"order by quote", "", "'flow_id'"},
		{"order by sort direction", "", "flow_id DESC"},
		{"order by unknown column", "", "flow_id, secret"},
		{"order by case mismatch", "", "FLOW_ID"},
		{"order by qualified", "", "t.flow_id"},
		{"order by duplicate", "", "flow_id, source_table, flow_id"},
		{"order by empty element", "", "flow_id,,lsn"},
		{"order by trailing comma", "", "flow_id,"},
		{"order by only comma", "", ","},
		{"order by nul", "", "flow_id\x00"},
		{"order by unicode lookalike", "", "flow_id\u200b"},
		{"order by oversize", "", huge},
		{"order by just over limit", "", strings.Repeat("a", maxMetaOrderByBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			layout, err := compileMetaTableLayout(tc.engine, tc.orderBy)
			if err == nil {
				t.Fatalf("compileMetaTableLayout accepted %q/%.40q: %+v", tc.engine, tc.orderBy, layout)
			}
			if len(err.Error()) > 512 {
				t.Fatalf("error echoes oversized input: %d bytes", len(err.Error()))
			}
		})
	}
}

func TestEnsureMetaTableExecutesCanonicalDDL(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	want := "CREATE TABLE IF NOT EXISTS `wallaby_meta`.`__metadata` (\n  flow_id String,\n  source_schema String,\n  source_table String,\n  synced_at DateTime64(9),\n  is_deleted UInt8,\n  lsn String,\n  operation String,\n  key_json String\n) ENGINE = MergeTree ORDER BY (`lsn`, `key_json`)"
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `wallaby_meta`").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(want).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT name FROM system.columns WHERE database = ? AND table = ?").WithArgs("wallaby_meta", "__metadata").WillReturnRows(sqlmock.NewRows([]string{"name"}))
	destination := &Destination{db: db, metaSchema: "wallaby_meta", metaTable: "__metadata", metaLayout: mustCompileMetaLayout(t, "mergetree", "lsn, key_json")}
	if err := destination.ensureMetaTable(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureMetaTableRejectsUncompiledLayoutBeforeSQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := &Destination{db: db, metaSchema: "wallaby_meta", metaTable: "__metadata"}
	if err := destination.ensureMetaTable(context.Background()); err == nil {
		t.Fatal("ensureMetaTable accepted a zero layout")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRejectsHostileMetadataLayoutBeforeAnyIO(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dsn := "clickhouse://" + listener.Addr().String() + "/default"
	for _, tc := range []struct {
		name    string
		options map[string]string
	}{
		{"engine", map[string]string{optMetaEngine: "MergeTree(); DROP TABLE victim; --"}},
		{"order by", map[string]string{optMetaOrderBy: "flow_id); DROP TABLE victim; --"}},
		{"order by with metadata disabled", map[string]string{optMetaEnabled: "false", optMetaOrderBy: "cityHash64(flow_id)"}},
		{"engine with metadata disabled", map[string]string{optMetaEnabled: "false", optMetaEngine: "Log"}},
		{"oversize", map[string]string{optMetaOrderBy: strings.Repeat("flow_id,", 1<<16)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := map[string]string{optDSN: dsn}
			for key, value := range tc.options {
				options[key] = value
			}
			destination := &Destination{}
			err := destination.Open(context.Background(), connector.RuntimeSpec{Type: connector.EndpointClickHouse, Options: options})
			if err == nil || !strings.Contains(err.Error(), "clickhouse meta_") {
				t.Fatalf("Open() error = %v, want metadata layout rejection", err)
			}
			if destination.db != nil {
				t.Fatal("Open() retained a database handle after rejection")
			}
			_ = listener.(*net.TCPListener).SetDeadline(time.Now().Add(100 * time.Millisecond))
			if conn, err := listener.Accept(); err == nil {
				_ = conn.Close()
				t.Fatal("Open() dialed the server before rejecting the metadata layout")
			}
		})
	}
}

func TestManagedProfileStillRejectsMetadataLayoutOptions(t *testing.T) {
	for _, option := range []string{optMetaEngine, optMetaOrderBy} {
		err := ValidateManagedProfileOptions(map[string]string{"managed_profile": connector.ManagedProfilePostgresToClickHouseAppendV1, option: "x"})
		if err == nil || !strings.Contains(err.Error(), option) {
			t.Fatalf("ValidateManagedProfileOptions(%s) error = %v", option, err)
		}
	}
}

func TestEnsureMetaTableFailurePropagatesWithoutLeakingLayout(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	execErr := errors.New("create table failed")
	mock.ExpectExec(regexp.QuoteMeta("CREATE DATABASE")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE").WillReturnError(execErr)
	destination := &Destination{db: db, metaSchema: "s", metaTable: "t", metaLayout: mustCompileMetaLayout(t, "", "")}
	if err := destination.ensureMetaTable(context.Background()); !errors.Is(err, execErr) {
		t.Fatalf("ensureMetaTable() error = %v", err)
	}
}

func TestConfigureLegacyClosesDatabaseWhenMetadataTableFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	execErr := errors.New("create database failed")
	mock.ExpectExec("CREATE DATABASE").WillReturnError(execErr)
	mock.ExpectClose()
	destination := &Destination{}
	err = destination.configureLegacy(context.Background(), db, connector.RuntimeSpec{Options: map[string]string{}}, mustCompileMetaLayout(t, "", ""))
	if !errors.Is(err, execErr) {
		t.Fatalf("configureLegacy() error = %v", err)
	}
	if destination.db != nil {
		t.Fatal("configureLegacy() retained the closed database handle")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if err := destination.Close(context.Background()); err != nil {
		t.Fatalf("Close() after failed open = %v", err)
	}
}

func TestConfigureLegacyKeepsDatabaseOpenAndCreatesCanonicalMetadataTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec("CREATE DATABASE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ENGINE = MergeTree ORDER BY \\(`flow_id`, `key_json`\\)").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT name FROM system.columns").WillReturnRows(sqlmock.NewRows([]string{"name"}))
	destination := &Destination{}
	options := map[string]string{optMetaEngine: "MERGETREE", optMetaOrderBy: "flow_id,key_json"}
	layout := mustCompileMetaLayout(t, options[optMetaEngine], options[optMetaOrderBy])
	if err := destination.configureLegacy(context.Background(), db, connector.RuntimeSpec{Options: options}, layout); err != nil {
		t.Fatal(err)
	}
	if destination.db != db {
		t.Fatal("configureLegacy() dropped the database handle on success")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
