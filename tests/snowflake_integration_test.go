package tests

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	"github.com/josephjohncox/wallaby/pkg/connector"
	_ "github.com/snowflakedb/gosnowflake"
)

func TestSnowflakeDestination(t *testing.T) {
	dsn, schema, ok := snowflakeTestDSN(t)
	if !ok {
		t.Skip("snowflake DSN not configured; set WALLABY_TEST_SNOWFLAKE_DSN or WALLABY_TEST_FAKESNOW_HOST/PORT")
	}
	if usingFakesnow() && !allowFakesnowSnowflake() {
		t.Skip("fakesnow enabled; set WALLABY_TEST_RUN_FAKESNOW=1 to run Snowflake integration")
	}

	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	setupDB, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("open snowflake: %v", err)
	}
	defer setupDB.Close()
	if err := setupDB.PingContext(ctx); err != nil {
		if usingFakesnow() {
			t.Skipf("fakesnow ping failed: %v", err)
		}
		t.Fatalf("ping snowflake: %v", err)
	}

	if schema != "" {
		if _, err := setupDB.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteSnowflakeIdent(schema))); err != nil {
			t.Fatalf("create schema: %v", err)
		}
	}

	table := fmt.Sprintf("wallaby_sf_%d", time.Now().UnixNano())
	fullTable := quoteSnowflakeIdent(table)
	if schema != "" {
		fullTable = quoteSnowflakeIdent(schema) + "." + quoteSnowflakeIdent(table)
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (id NUMBER, name STRING)", fullTable)
	if _, err := setupDB.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create table: %v", err)
	}

	dest := &snowflake.Destination{}
	spec := connector.Spec{
		Name: "snowflake-test",
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
		t.Fatalf("open destination: %v", err)
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

	insert := connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 1}),
		After: map[string]any{
			"id":   1,
			"name": "alpha",
		},
	}
	batch := connector.Batch{Records: []connector.Record{insert}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "1"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert write: %v", err)
	}

	var name string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT name FROM %s WHERE id = 1", fullTable)).Scan(&name); err != nil {
		t.Fatalf("select after insert: %v", err)
	}
	if strings.TrimSpace(name) != "alpha" {
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
	batch = connector.Batch{Records: []connector.Record{update}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "2"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("update write: %v", err)
	}
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT name FROM %s WHERE id = 1", fullTable)).Scan(&name); err != nil {
		t.Fatalf("select after update: %v", err)
	}
	if strings.TrimSpace(name) != "beta" {
		t.Fatalf("unexpected name after update: %s", name)
	}

	del := connector.Record{
		Table:     table,
		Operation: connector.OpDelete,
		Key:       recordKey(t, map[string]any{"id": 1}),
	}
	batch = connector.Batch{Records: []connector.Record{del}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "3"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("delete write: %v", err)
	}

	var count int
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT count() FROM %s", fullTable)).Scan(&count); err != nil {
		t.Fatalf("count after delete: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 rows after delete, got %d", count)
	}
}

func quoteSnowflakeIdent(value string) string {
	if value == "" {
		return value
	}
	escaped := strings.ReplaceAll(value, "\"", "\"\"")
	return "\"" + escaped + "\""
}
