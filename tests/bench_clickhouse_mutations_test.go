package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func BenchmarkClickHouseMutationUpdate(b *testing.B) {
	dsn := os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		b.Skip("WALLABY_TEST_CLICKHOUSE_DSN not set")
	}
	database := os.Getenv("WALLABY_TEST_CLICKHOUSE_DB")
	if database == "" {
		database = "default"
	}

	ctx := context.Background()
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		b.Fatalf("open clickhouse: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)); err != nil {
		b.Fatalf("create database: %v", err)
	}

	table := fmt.Sprintf("wallaby_bench_mut_%d", time.Now().UnixNano())
	fullTable := fmt.Sprintf("%s.%s", database, table)
	createSQL := fmt.Sprintf(`CREATE TABLE %s (
  id UInt64,
  name String
) ENGINE = MergeTree ORDER BY id`, fullTable)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		b.Fatalf("create table: %v", err)
	}

	// Preload rows for updates.
	for i := 0; i < b.N; i++ {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", fullTable), uint64(i), "seed"); err != nil {
			b.Fatalf("seed row: %v", err)
		}
	}

	dest := &clickhouse.Destination{}
	spec := connector.Spec{
		Name: "clickhouse-bench",
		Type: connector.EndpointClickHouse,
		Options: map[string]string{
			"dsn":                dsn,
			"database":           database,
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
		Name:      table,
		Namespace: database,
		Columns: []connector.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := connector.Record{
			Table:     table,
			Operation: connector.OpUpdate,
			Key:       recordKey(b, map[string]any{"id": uint64(i)}),
			After: map[string]any{
				"id":   uint64(i),
				"name": "updated",
			},
		}
		batch := connector.Batch{Records: []connector.Record{record}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "bench"}}
		if err := dest.Write(ctx, batch); err != nil {
			b.Fatalf("write update: %v", err)
		}
	}
}

func BenchmarkClickHouseAppendInsert(b *testing.B) {
	dsn := os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		b.Skip("WALLABY_TEST_CLICKHOUSE_DSN not set")
	}
	database := os.Getenv("WALLABY_TEST_CLICKHOUSE_DB")
	if database == "" {
		database = "default"
	}

	ctx := context.Background()
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		b.Fatalf("open clickhouse: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)); err != nil {
		b.Fatalf("create database: %v", err)
	}

	table := fmt.Sprintf("wallaby_bench_append_%d", time.Now().UnixNano())
	fullTable := fmt.Sprintf("%s.%s", database, table)
	createSQL := fmt.Sprintf(`CREATE TABLE %s (
  id UInt64,
  name String
) ENGINE = MergeTree ORDER BY id`, fullTable)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		b.Fatalf("create table: %v", err)
	}

	dest := &clickhouse.Destination{}
	spec := connector.Spec{
		Name: "clickhouse-bench-append",
		Type: connector.EndpointClickHouse,
		Options: map[string]string{
			"dsn":                dsn,
			"database":           database,
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
		Name:      table,
		Namespace: database,
		Columns: []connector.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := connector.Record{
			Table:     table,
			Operation: connector.OpInsert,
			Key:       recordKey(b, map[string]any{"id": uint64(i)}),
			After: map[string]any{
				"id":   uint64(i),
				"name": "append",
			},
		}
		batch := connector.Batch{Records: []connector.Record{record}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "bench"}}
		if err := dest.Write(ctx, batch); err != nil {
			b.Fatalf("write insert: %v", err)
		}
	}
}
