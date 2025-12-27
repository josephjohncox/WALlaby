package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/ductstream/connectors/destinations/snowflake"
	"github.com/josephjohncox/ductstream/pkg/connector"
	_ "github.com/snowflakedb/gosnowflake"
)

func BenchmarkSnowflakeUpdate(b *testing.B) {
	dsn := os.Getenv("DUCTSTREAM_TEST_SNOWFLAKE_DSN")
	if dsn == "" {
		b.Skip("DUCTSTREAM_TEST_SNOWFLAKE_DSN not set")
	}
	schema := os.Getenv("DUCTSTREAM_TEST_SNOWFLAKE_SCHEMA")
	if schema == "" {
		schema = "PUBLIC"
	}

	ctx := context.Background()
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		b.Fatalf("open snowflake: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteSnowflakeIdentBench(schema))); err != nil {
		b.Fatalf("create schema: %v", err)
	}

	table := fmt.Sprintf("ductstream_bench_sf_%d", time.Now().UnixNano())
	fullTable := quoteSnowflakeIdentBench(schema) + "." + quoteSnowflakeIdentBench(table)
	createSQL := fmt.Sprintf("CREATE TABLE %s (id NUMBER, name STRING)", fullTable)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		b.Fatalf("create table: %v", err)
	}

	for i := 0; i < b.N; i++ {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", fullTable), i, "seed"); err != nil {
			b.Fatalf("seed row: %v", err)
		}
	}

	dest := &snowflake.Destination{}
	spec := connector.Spec{
		Name: "snowflake-bench",
		Type: connector.EndpointSnowflake,
		Options: map[string]string{
			"dsn":                dsn,
			"schema":             schema,
			"table":              table,
			"meta_table_enabled": "false",
			"write_mode":         "target",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		b.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schemaDef := connector.Schema{
		Name:      table,
		Namespace: schema,
		Columns: []connector.Column{
			{Name: "id", Type: "NUMBER"},
			{Name: "name", Type: "STRING"},
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
		batch := connector.Batch{Records: []connector.Record{record}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "bench"}}
		if err := dest.Write(ctx, batch); err != nil {
			b.Fatalf("write update: %v", err)
		}
	}
}

func BenchmarkSnowflakeAppend(b *testing.B) {
	dsn := os.Getenv("DUCTSTREAM_TEST_SNOWFLAKE_DSN")
	if dsn == "" {
		b.Skip("DUCTSTREAM_TEST_SNOWFLAKE_DSN not set")
	}
	schema := os.Getenv("DUCTSTREAM_TEST_SNOWFLAKE_SCHEMA")
	if schema == "" {
		schema = "PUBLIC"
	}

	ctx := context.Background()
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		b.Fatalf("open snowflake: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteSnowflakeIdentBench(schema))); err != nil {
		b.Fatalf("create schema: %v", err)
	}

	table := fmt.Sprintf("ductstream_bench_sf_append_%d", time.Now().UnixNano())
	fullTable := quoteSnowflakeIdentBench(schema) + "." + quoteSnowflakeIdentBench(table)
	createSQL := fmt.Sprintf("CREATE TABLE %s (id NUMBER, name STRING)", fullTable)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		b.Fatalf("create table: %v", err)
	}

	dest := &snowflake.Destination{}
	spec := connector.Spec{
		Name: "snowflake-bench-append",
		Type: connector.EndpointSnowflake,
		Options: map[string]string{
			"dsn":                dsn,
			"schema":             schema,
			"table":              table,
			"meta_table_enabled": "false",
			"write_mode":         "append",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		b.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schemaDef := connector.Schema{
		Name:      table,
		Namespace: schema,
		Columns: []connector.Column{
			{Name: "id", Type: "NUMBER"},
			{Name: "name", Type: "STRING"},
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
		batch := connector.Batch{Records: []connector.Record{record}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "bench"}}
		if err := dest.Write(ctx, batch); err != nil {
			b.Fatalf("write append: %v", err)
		}
	}
}

func quoteSnowflakeIdentBench(value string) string {
	if value == "" {
		return value
	}
	escaped := strings.ReplaceAll(value, "\"", "\"\"")
	return "\"" + escaped + "\""
}
