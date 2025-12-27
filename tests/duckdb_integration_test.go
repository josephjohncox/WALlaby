package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/connectors/destinations/duckdb"
	"github.com/josephjohncox/wallaby/pkg/connector"
	_ "github.com/marcboeker/go-duckdb"
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
