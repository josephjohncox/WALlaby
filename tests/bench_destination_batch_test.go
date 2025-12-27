package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/josephjohncox/ductstream/connectors/destinations/clickhouse"
	"github.com/josephjohncox/ductstream/connectors/destinations/duckdb"
	"github.com/josephjohncox/ductstream/connectors/destinations/snowflake"
	"github.com/josephjohncox/ductstream/pkg/connector"
	_ "github.com/marcboeker/go-duckdb"
	_ "github.com/snowflakedb/gosnowflake"
)

func BenchmarkClickHouseBatchSizes(b *testing.B) {
	dsn := os.Getenv("DUCTSTREAM_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		b.Skip("DUCTSTREAM_TEST_CLICKHOUSE_DSN not set")
	}
	database := os.Getenv("DUCTSTREAM_TEST_CLICKHOUSE_DB")
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

	benchBatchSizes(b, "clickhouse", []int{1, 10, 100, 500}, func(size int) benchTarget {
		table := fmt.Sprintf("ductstream_batch_ch_%d_%d", time.Now().UnixNano(), size)
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
			Name: "clickhouse-batch",
			Type: connector.EndpointClickHouse,
			Options: map[string]string{
				"dsn":                dsn,
				"database":           database,
				"table":              table,
				"write_mode":         "append",
				"meta_table_enabled": "false",
			},
		}
		if err := dest.Open(ctx, spec); err != nil {
			b.Fatalf("open destination: %v", err)
		}

		return benchTarget{
			dest: dest,
			schema: connector.Schema{
				Name:      table,
				Namespace: database,
				Columns: []connector.Column{
					{Name: "id", Type: "UInt64"},
					{Name: "name", Type: "String"},
				},
			},
			closeFn: func() { _ = dest.Close(ctx) },
		}
	})
}

func BenchmarkDuckDBBatchSizes(b *testing.B) {
	dsn := os.Getenv("DUCTSTREAM_TEST_DUCKDB_DSN")
	if dsn == "" {
		b.Skip("DUCTSTREAM_TEST_DUCKDB_DSN not set")
	}

	ctx := context.Background()
	setupDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		b.Fatalf("open duckdb: %v", err)
	}
	defer setupDB.Close()

	benchBatchSizes(b, "duckdb", []int{1, 10, 100, 500}, func(size int) benchTarget {
		table := fmt.Sprintf("ductstream_batch_duck_%d_%d", time.Now().UnixNano(), size)
		if _, err := setupDB.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER, name TEXT)", table)); err != nil {
			b.Fatalf("create table: %v", err)
		}

		dest := &duckdb.Destination{}
		spec := connector.Spec{
			Name: "duckdb-batch",
			Type: connector.EndpointDuckDB,
			Options: map[string]string{
				"dsn":                dsn,
				"table":              table,
				"write_mode":         "append",
				"meta_table_enabled": "false",
			},
		}
		if err := dest.Open(ctx, spec); err != nil {
			b.Fatalf("open destination: %v", err)
		}

		return benchTarget{
			dest: dest,
			schema: connector.Schema{
				Name: table,
				Columns: []connector.Column{
					{Name: "id", Type: "INTEGER"},
					{Name: "name", Type: "TEXT"},
				},
			},
			closeFn: func() { _ = dest.Close(ctx) },
		}
	})
}

func BenchmarkSnowflakeBatchSizes(b *testing.B) {
	dsn := os.Getenv("DUCTSTREAM_TEST_SNOWFLAKE_DSN")
	if dsn == "" {
		b.Skip("DUCTSTREAM_TEST_SNOWFLAKE_DSN not set")
	}
	schema := os.Getenv("DUCTSTREAM_TEST_SNOWFLAKE_SCHEMA")
	if schema == "" {
		schema = "PUBLIC"
	}

	ctx := context.Background()
	setupDB, err := sql.Open("snowflake", dsn)
	if err != nil {
		b.Fatalf("open snowflake: %v", err)
	}
	defer setupDB.Close()

	if _, err := setupDB.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteSnowflakeIdentBench(schema))); err != nil {
		b.Fatalf("create schema: %v", err)
	}

	benchBatchSizes(b, "snowflake", []int{1, 10, 100, 500}, func(size int) benchTarget {
		table := fmt.Sprintf("ductstream_batch_sf_%d_%d", time.Now().UnixNano(), size)
		fullTable := quoteSnowflakeIdentBench(schema) + "." + quoteSnowflakeIdentBench(table)
		createSQL := fmt.Sprintf("CREATE TABLE %s (id NUMBER, name STRING)", fullTable)
		if _, err := setupDB.ExecContext(ctx, createSQL); err != nil {
			b.Fatalf("create table: %v", err)
		}

		dest := &snowflake.Destination{}
		spec := connector.Spec{
			Name: "snowflake-batch",
			Type: connector.EndpointSnowflake,
			Options: map[string]string{
				"dsn":                dsn,
				"schema":             schema,
				"table":              table,
				"write_mode":         "append",
				"meta_table_enabled": "false",
			},
		}
		if err := dest.Open(ctx, spec); err != nil {
			b.Fatalf("open destination: %v", err)
		}

		return benchTarget{
			dest: dest,
			schema: connector.Schema{
				Name:      table,
				Namespace: schema,
				Columns: []connector.Column{
					{Name: "id", Type: "NUMBER"},
					{Name: "name", Type: "STRING"},
				},
			},
			closeFn: func() { _ = dest.Close(ctx) },
		}
	})
}

type benchTarget struct {
	dest    connector.Destination
	schema  connector.Schema
	closeFn func()
}

func benchBatchSizes(b *testing.B, name string, sizes []int, setup func(int) benchTarget) {
	ctx := context.Background()
	for _, size := range sizes {
		size := size
		b.Run(fmt.Sprintf("%s/batch_%d", name, size), func(b *testing.B) {
			target := setup(size)
			defer target.closeFn()
			batch := buildBatch(target.schema, size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := target.dest.Write(ctx, batch); err != nil {
					b.Fatalf("write batch: %v", err)
				}
			}
		})
	}
}

func buildBatch(schema connector.Schema, size int) connector.Batch {
	records := make([]connector.Record, 0, size)
	for i := 0; i < size; i++ {
		key, _ := encodeKey(map[string]any{"id": i})
		records = append(records, connector.Record{
			Table:     schema.Name,
			Operation: connector.OpInsert,
			Key:       key,
			After: map[string]any{
				"id":   i,
				"name": fmt.Sprintf("name-%d", i),
			},
			Timestamp: time.Now().UTC(),
		})
	}
	return connector.Batch{Records: records, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "bench"}}
}

func encodeKey(key map[string]any) ([]byte, error) {
	if key == nil {
		return nil, nil
	}
	return json.Marshal(key)
}
