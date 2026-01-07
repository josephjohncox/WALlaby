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
	_ "github.com/duckdb/duckdb-go/v2"
)

func BenchmarkDuckDBUpdate(b *testing.B) {
	dsn := os.Getenv("WALLABY_TEST_DUCKDB_DSN")
	if dsn == "" {
		dsn = filepath.Join(os.TempDir(), fmt.Sprintf("wallaby_bench_duckdb_%d.duckdb", time.Now().UnixNano()))
	}

	ctx := context.Background()
	setupDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		b.Fatalf("open duckdb: %v", err)
	}
	defer setupDB.Close()

	table := fmt.Sprintf("wallaby_bench_duckdb_%d", time.Now().UnixNano())
	if _, err := setupDB.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER, name TEXT)", table)); err != nil {
		b.Fatalf("create table: %v", err)
	}

	for i := 0; i < b.N; i++ {
		if _, err := setupDB.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", table), i, "seed"); err != nil {
			b.Fatalf("seed row: %v", err)
		}
	}

	dest := &duckdb.Destination{}
	spec := connector.Spec{
		Name: "duckdb-bench",
		Type: connector.EndpointDuckDB,
		Options: map[string]string{
			"dsn":                dsn,
			"table":              table,
			"meta_table_enabled": "false",
			"write_mode":         "target",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		b.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schema := connector.Schema{
		Name: table,
		Columns: []connector.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := connector.Record{
			Table:     table,
			Operation: connector.OpUpdate,
			Key:       recordKey(b, map[string]any{"id": i}),
			After: map[string]any{
				"id":   i,
				"name": "updated",
			},
		}
		batch := connector.Batch{Records: []connector.Record{record}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "bench"}}
		if err := dest.Write(ctx, batch); err != nil {
			b.Fatalf("write update: %v", err)
		}
	}
}

func BenchmarkDuckDBAppend(b *testing.B) {
	dsn := os.Getenv("WALLABY_TEST_DUCKDB_DSN")
	if dsn == "" {
		dsn = filepath.Join(os.TempDir(), fmt.Sprintf("wallaby_bench_duckdb_append_%d.duckdb", time.Now().UnixNano()))
	}

	ctx := context.Background()
	setupDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		b.Fatalf("open duckdb: %v", err)
	}
	defer setupDB.Close()

	table := fmt.Sprintf("wallaby_bench_duckdb_append_%d", time.Now().UnixNano())
	if _, err := setupDB.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER, name TEXT)", table)); err != nil {
		b.Fatalf("create table: %v", err)
	}

	dest := &duckdb.Destination{}
	spec := connector.Spec{
		Name: "duckdb-bench-append",
		Type: connector.EndpointDuckDB,
		Options: map[string]string{
			"dsn":                dsn,
			"table":              table,
			"meta_table_enabled": "false",
			"write_mode":         "append",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		b.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schema := connector.Schema{
		Name: table,
		Columns: []connector.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := connector.Record{
			Table:     table,
			Operation: connector.OpUpdate,
			Key:       recordKey(b, map[string]any{"id": i}),
			After: map[string]any{
				"id":   i,
				"name": "append",
			},
		}
		batch := connector.Batch{Records: []connector.Record{record}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "bench"}}
		if err := dest.Write(ctx, batch); err != nil {
			b.Fatalf("write append: %v", err)
		}
	}
}
