package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/josephjohncox/wallaby/connectors/destinations/duckdb"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDuckDBDestination(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_DUCKDB_DSN")
	if dsn == "" {
		t.Skip("WALLABY_TEST_DUCKDB_DSN not set")
	}

	ctx := context.Background()
	setupDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer setupDB.Close()

	table := fmt.Sprintf("wallaby_duckdb_%d", time.Now().UnixNano())
	createSQL := fmt.Sprintf("CREATE TABLE %s (id INTEGER, name TEXT)", table)
	if _, err := setupDB.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create table: %v", err)
	}

	dest := &duckdb.Destination{}
	spec := connector.Spec{
		Name: "duckdb-test",
		Type: connector.EndpointDuckDB,
		Options: map[string]string{
			"dsn":                dsn,
			"table":              table,
			"meta_table_enabled": "false",
			"write_mode":         "target",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schema := connector.Schema{
		Name: table,
		Columns: []connector.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	insert := connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 1}),
		After: map[string]any{
			"id":   1,
			"name": "alpha",
		},
	}
	batch := connector.Batch{Records: []connector.Record{insert}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "1"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert write: %v", err)
	}

	var name string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT name FROM %s WHERE id = 1", table)).Scan(&name); err != nil {
		t.Fatalf("select after insert: %v", err)
	}
	if name != "alpha" {
		t.Fatalf("unexpected name after insert: %s", name)
	}

	update := connector.Record{
		Table:     table,
		Operation: connector.OpUpdate,
		Key:       recordKey(t, map[string]any{"id": 1}),
		After: map[string]any{
			"id":   1,
			"name": "beta",
		},
	}
	batch = connector.Batch{Records: []connector.Record{update}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "2"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("update write: %v", err)
	}
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT name FROM %s WHERE id = 1", table)).Scan(&name); err != nil {
		t.Fatalf("select after update: %v", err)
	}
	if name != "beta" {
		t.Fatalf("unexpected name after update: %s", name)
	}

	del := connector.Record{
		Table:     table,
		Operation: connector.OpDelete,
		Key:       recordKey(t, map[string]any{"id": 1}),
	}
	batch = connector.Batch{Records: []connector.Record{del}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "3"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("delete write: %v", err)
	}

	var count int
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT count() FROM %s", table)).Scan(&count); err != nil {
		t.Fatalf("count after delete: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after delete, got %d", count)
	}
}

func TestDuckDBDestinationDDLAndTableLifecycle(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_DUCKDB_DSN")
	if dsn == "" {
		dsn = filepath.Join(t.TempDir(), "wallaby-ddl.duckdb")
	}

	ctx := context.Background()
	setupDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		t.Skipf("duckdb not available: %v", err)
	}
	defer setupDB.Close()
	if err := setupDB.PingContext(ctx); err != nil {
		t.Skipf("duckdb ping failed: %v", err)
	}

	table := fmt.Sprintf("wallaby_duckdb_ddl_%d", time.Now().UnixNano())
	renamed := table + "_renamed"
	createSQL := fmt.Sprintf("CREATE TABLE %s (id INTEGER, name TEXT)", table)
	if _, err := setupDB.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create table: %v", err)
	}

	dest := &duckdb.Destination{}
	spec := connector.Spec{
		Name: "duckdb-ddl",
		Type: connector.EndpointDuckDB,
		Options: map[string]string{
			"dsn":                dsn,
			"table":              table,
			"meta_table_enabled": "false",
			"write_mode":         "target",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}

	schema := connector.Schema{
		Name: table,
		Columns: []connector.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	addDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra TEXT", table),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, addDDL); err != nil {
		t.Fatalf("apply add ddl: %v", err)
	}

	alterDefaults := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN extra SET DEFAULT 'seed', ALTER COLUMN extra SET NOT NULL", table),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, alterDefaults); err != nil {
		t.Fatalf("apply default/not-null ddl: %v", err)
	}

	schema.Columns = append(schema.Columns, connector.Column{Name: "extra", Type: "TEXT"})
	insertWithDefault := connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 1}),
		After: map[string]any{
			"id":   1,
			"name": "alpha",
		},
	}
	batch := connector.Batch{
		Records:    []connector.Record{insertWithDefault},
		Schema:     schema,
		Checkpoint: connector.Checkpoint{LSN: "1"},
	}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write with default: %v", err)
	}

	var extra string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT extra FROM %s WHERE id = 1", table)).Scan(&extra); err != nil {
		t.Fatalf("select default value: %v", err)
	}
	if extra != "seed" {
		t.Fatalf("unexpected default value: %s", extra)
	}

	relaxDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN extra DROP DEFAULT, ALTER COLUMN extra DROP NOT NULL", table),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, relaxDDL); err != nil {
		t.Fatalf("apply drop default/not-null ddl: %v", err)
	}

	insertNull := connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 2}),
		After: map[string]any{
			"id":   2,
			"name": "beta",
		},
	}
	batch = connector.Batch{
		Records:    []connector.Record{insertNull},
		Schema:     schema,
		Checkpoint: connector.Checkpoint{LSN: "2"},
	}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write with nullable extra: %v", err)
	}
	var nullableExtra sql.NullString
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT extra FROM %s WHERE id = 2", table)).Scan(&nullableExtra); err != nil {
		t.Fatalf("select nullable extra: %v", err)
	}
	if nullableExtra.Valid {
		t.Fatalf("expected nullable extra to be NULL, got %q", nullableExtra.String)
	}

	renameDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s RENAME TO %s", table, renamed),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, renameDDL); err != nil {
		t.Fatalf("apply rename table ddl: %v", err)
	}

	if err := dest.Close(ctx); err != nil {
		t.Fatalf("close destination: %v", err)
	}

	renamedDest := &duckdb.Destination{}
	renamedSpec := connector.Spec{
		Name: "duckdb-ddl-renamed",
		Type: connector.EndpointDuckDB,
		Options: map[string]string{
			"dsn":                dsn,
			"table":              renamed,
			"meta_table_enabled": "false",
			"write_mode":         "target",
		},
	}
	if err := renamedDest.Open(ctx, renamedSpec); err != nil {
		t.Fatalf("open renamed destination: %v", err)
	}
	defer renamedDest.Close(ctx)

	renamedSchema := connector.Schema{
		Name: renamed,
		Columns: []connector.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
			{Name: "extra", Type: "TEXT"},
		},
	}
	insertRenamed := connector.Record{
		Table:     renamed,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 3}),
		After: map[string]any{
			"id":    3,
			"name":  "gamma",
			"extra": "manual",
		},
	}
	if err := renamedDest.Write(ctx, connector.Batch{
		Records:    []connector.Record{insertRenamed},
		Schema:     renamedSchema,
		Checkpoint: connector.Checkpoint{LSN: "3"},
	}); err != nil {
		t.Fatalf("write after rename table: %v", err)
	}

	var renamedValue string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT extra FROM %s WHERE id = 3", renamed)).Scan(&renamedValue); err != nil {
		t.Fatalf("select renamed table row: %v", err)
	}
	if renamedValue != "manual" {
		t.Fatalf("unexpected value after rename table: %s", renamedValue)
	}

	dropDDL := connector.Record{
		Table:     renamed,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("DROP TABLE IF EXISTS %s", renamed),
		Timestamp: time.Now().UTC(),
	}
	if err := renamedDest.ApplyDDL(ctx, renamedSchema, dropDDL); err != nil {
		t.Fatalf("apply drop table ddl: %v", err)
	}

	var tableCount int
	if err := setupDB.QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_name = ?`,
		renamed,
	).Scan(&tableCount); err != nil {
		t.Fatalf("check dropped table: %v", err)
	}
	if tableCount != 0 {
		t.Fatalf("expected dropped table to be absent, found %d entries", tableCount)
	}
}

func TestDuckDBDestinationWithTempFile(t *testing.T) {
	if os.Getenv("WALLABY_TEST_DUCKDB_DSN") != "" {
		return
	}
	path := filepath.Join(t.TempDir(), "wallaby.duckdb")
	ctx := context.Background()
	dest := &duckdb.Destination{}
	spec := connector.Spec{
		Name: "duckdb-temp",
		Type: connector.EndpointDuckDB,
		Options: map[string]string{
			"dsn":                path,
			"table":              "wallaby_temp",
			"meta_table_enabled": "false",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Skipf("duckdb not available: %v", err)
	}
	_ = dest.Close(ctx)
}
