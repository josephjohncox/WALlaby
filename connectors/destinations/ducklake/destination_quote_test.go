package ducklake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// quoteForTest builds a DuckDB quoted identifier independently of the
// production quoteIdent so fixtures do not depend on the code under test.
func quoteForTest(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

var duckDBAdversarialNames = struct {
	schema, table string
	columns       []string
}{
	schema: `s"; DROP TABLE victim; --`,
	table:  `t"); DROP TABLE victim; --`,
	columns: []string{
		`c"; DROP TABLE victim; --`,
		`trailing\`,
		`a"b`,
		"?",
		"$1",
		`'quoted'`,
		"new\nline",
		"-- comment",
		"/* block */",
		"semi;colon",
		"名前",
	},
}

func newAdversarialDuckDB(t *testing.T) (*sql.DB, string, string) {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	// An in-memory DuckDB database is per connection; keep one connection so
	// every statement sees the same catalog.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	names := duckDBAdversarialNames
	defs := make([]string, 0, len(names.columns))
	for _, column := range names.columns {
		defs = append(defs, quoteForTest(column)+" VARCHAR")
	}
	target := quoteForTest(names.schema) + "." + quoteForTest(names.table)
	staging := quoteForTest(names.schema) + "." + quoteForTest(names.table+"_staging")
	for _, statement := range []string{
		`CREATE TABLE victim (id INTEGER)`,
		`INSERT INTO victim VALUES (1)`,
		`CREATE SCHEMA ` + quoteForTest(names.schema),
		fmt.Sprintf(`CREATE TABLE %s (%s)`, target, strings.Join(defs, ", ")),
		fmt.Sprintf(`CREATE TABLE %s (%s)`, staging, strings.Join(defs, ", ")),
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	return db, target, staging
}

func insertAdversarialStagingRow(t *testing.T, db *sql.DB, table string, value string) {
	t.Helper()
	names := duckDBAdversarialNames
	quoted := make([]string, 0, len(names.columns))
	for _, column := range names.columns {
		quoted = append(quoted, quoteForTest(column))
	}
	statement := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, table, strings.Join(quoted, ", "), strings.TrimRight(strings.Repeat("?,", len(quoted)), ","))
	args := make([]any, len(quoted))
	for i := range args {
		args[i] = value
	}
	if _, err := db.Exec(statement, args...); err != nil {
		t.Fatal(err)
	}
}

func requireVictimAndRows(t *testing.T, db *sql.DB, target string, wantRows int) {
	t.Helper()
	var victims int
	if err := db.QueryRow(`SELECT count(*) FROM victim`).Scan(&victims); err != nil || victims != 1 {
		t.Fatalf("victim rows=%d err=%v", victims, err)
	}
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM ` + target).Scan(&rows); err != nil || rows != wantRows {
		t.Fatalf("target rows=%d want=%d err=%v", rows, wantRows, err)
	}
}

func TestResolveStagingTableQuotesAdversarialIdentifiers(t *testing.T) {
	names := duckDBAdversarialNames
	for _, tc := range []struct {
		name          string
		resolve       string
		catalogLoaded bool
		wantRows      int
	}{
		{"append with schema columns", batchResolveAppend, false, 2},
		{"replace with schema columns", batchResolveReplace, false, 1},
		{"append with catalog-loaded columns", batchResolveAppend, true, 2},
		{"replace with catalog-loaded columns", batchResolveReplace, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, target, staging := newAdversarialDuckDB(t)
			insertAdversarialStagingRow(t, db, target, "old")
			insertAdversarialStagingRow(t, db, staging, `new'); DROP TABLE victim; --`)
			schema := connector.Schema{Namespace: names.schema}
			if !tc.catalogLoaded {
				for _, column := range names.columns {
					schema.Columns = append(schema.Columns, connector.Column{Name: column, Type: "text"})
				}
			}
			destination := &Destination{db: db, batchResolve: tc.resolve, stagingSuffix: "_staging"}
			if err := destination.resolveStagingTable(context.Background(), tableInfo{schema: schema, table: names.table}); err != nil {
				t.Fatal(err)
			}
			requireVictimAndRows(t, db, target, tc.wantRows)
			var copied string
			if err := db.QueryRow(`SELECT ` + quoteForTest(names.columns[0]) + ` FROM ` + target + ` WHERE ` + quoteForTest(names.columns[0]) + ` LIKE 'new%'`).Scan(&copied); err != nil || copied != `new'); DROP TABLE victim; --` {
				t.Fatalf("copied value=%q err=%v", copied, err)
			}
		})
	}
}

func TestResolveStagingTableQuotesAdversarialStagingOption(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	for _, statement := range []string{
		`CREATE TABLE victim (id INTEGER)`,
		`CREATE TABLE events (id INTEGER)`,
		`CREATE SCHEMA ` + quoteForTest(`stg"; DROP TABLE victim; --`),
		`CREATE TABLE ` + quoteForTest(`stg"; DROP TABLE victim; --`) + `.` + quoteForTest(`rows"; DROP TABLE victim; --`) + ` (id INTEGER)`,
		`INSERT INTO ` + quoteForTest(`stg"; DROP TABLE victim; --`) + `.` + quoteForTest(`rows"; DROP TABLE victim; --`) + ` VALUES (7)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	// A dotted staging table is split into schema and table; each part is quoted separately.
	destination := &Destination{db: db, batchResolve: batchResolveAppend, stagingTableName: `stg"; DROP TABLE victim; --.rows"; DROP TABLE victim; --`}
	schema := connector.Schema{Columns: []connector.Column{{Name: "id", Type: "int4"}}}
	if err := destination.resolveStagingTable(context.Background(), tableInfo{schema: schema, table: "events"}); err != nil {
		t.Fatal(err)
	}
	var id int
	if err := db.QueryRow(`SELECT id FROM events`).Scan(&id); err != nil || id != 7 {
		t.Fatalf("events id=%d err=%v", id, err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM victim`).Scan(&id); err != nil {
		t.Fatalf("victim table was dropped: %v", err)
	}
}

func TestResolveStagingTableReportsExecFailure(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	destination := &Destination{db: db, batchResolve: batchResolveAppend, stagingSuffix: "_staging"}
	schema := connector.Schema{Columns: []connector.Column{{Name: "id", Type: "int4"}}}
	err = destination.resolveStagingTable(context.Background(), tableInfo{schema: schema, table: "missing"})
	if err == nil || !strings.Contains(err.Error(), "resolve staging") {
		t.Fatalf("resolveStagingTable() error = %v", err)
	}
}

func TestResolveStagingTableHonorsCancellationBeforeCopyingRows(t *testing.T) {
	for _, resolve := range []string{batchResolveAppend, batchResolveReplace} {
		t.Run(resolve, func(t *testing.T) {
			db, err := sql.Open("duckdb", ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			db.SetMaxOpenConns(1)
			defer db.Close()
			for _, statement := range []string{
				`CREATE TABLE events (id INTEGER)`,
				`CREATE TABLE events_staging (id INTEGER)`,
				`INSERT INTO events VALUES (1)`,
				`INSERT INTO events_staging VALUES (2), (3)`,
			} {
				if _, err := db.Exec(statement); err != nil {
					t.Fatalf("%s: %v", statement, err)
				}
			}
			destination := &Destination{db: db, batchResolve: resolve, stagingSuffix: "_staging"}
			schema := connector.Schema{Columns: []connector.Column{{Name: "id", Type: "int4"}}}
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			err = destination.resolveStagingTable(canceled, tableInfo{schema: schema, table: "events"})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled resolveStagingTable() error = %v, want context.Canceled", err)
			}
			var ids []int
			rows, err := db.Query(`SELECT id FROM events ORDER BY id`)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(ids, []int{1}) {
				t.Fatalf("target rows after canceled resolve = %v, want [1] (nothing truncated or copied)", ids)
			}
		})
	}
}
