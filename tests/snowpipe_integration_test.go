package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/connectors/destinations/snowpipe"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestSnowpipeAutoIngestUpload(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_SNOWPIPE_DSN")
	stage := os.Getenv("WALLABY_TEST_SNOWPIPE_STAGE")
	var schema string
	if dsn == "" {
		if usingFakesnow() {
			derived, derivedSchema, ok := snowflakeTestDSN(t)
			if !ok {
				t.Skip("snowpipe DSN not configured")
			}
			dsn = derived
			schema = derivedSchema
		}
	}
	if schema == "" {
		schema = os.Getenv("WALLABY_TEST_SNOWPIPE_SCHEMA")
	}
	if stage == "" && usingFakesnow() {
		stage = "@~"
	}
	if dsn == "" || stage == "" {
		t.Skip("WALLABY_TEST_SNOWPIPE_DSN or WALLABY_TEST_SNOWPIPE_STAGE not set")
	}
	if usingFakesnow() && !allowFakesnowSnowflake() {
		t.Skip("fakesnow enabled; set WALLABY_TEST_RUN_FAKESNOW=1 to run Snowpipe integration")
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

	dest := &snowpipe.Destination{}
	table := fmt.Sprintf("wallaby_snowpipe_%d", time.Now().UnixNano())
	fullTable := quoteSnowflakeIdent(table)
	if schema != "" {
		fullTable = quoteSnowflakeIdent(schema) + "." + quoteSnowflakeIdent(table)
	}
	spec := connector.Spec{
		Name: "snowpipe-test",
		Type: connector.EndpointSnowpipe,
		Options: map[string]string{
			"dsn":                       dsn,
			"stage":                     stage,
			"format":                    "json",
			"auto_ingest":               "false",
			"copy_on_write":             "true",
			"copy_pattern":              ".*",
			"copy_on_error":             "continue",
			"copy_match_by_column_name": "case_insensitive",
			"copy_purge":                "false",
			"meta_table_enabled":        "false",
			"schema":                    schema,
			"table":                     table,
		},
	}
	if usingFakesnow() {
		spec.Options["compat_mode"] = "fakesnow"
	}

	if err := dest.Open(ctx, spec); err != nil {
		if usingFakesnow() {
			t.Skipf("fakesnow open failed: %v", err)
		}
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schemaDef := connector.Schema{
		Name:      table,
		Namespace: schema,
		Columns: []connector.Column{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "text"},
		},
	}
	ddlRecord := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("CREATE TABLE %s (id int, name text)", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, ddlRecord); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}
	record := connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		After:     map[string]any{"id": 1, "name": "alpha"},
	}
	batch := connector.Batch{Records: []connector.Record{record}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "1"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	var name string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT name FROM %s WHERE id = 1", fullTable)).Scan(&name); err != nil {
		t.Fatalf("select after write: %v", err)
	}
	if name != "alpha" {
		t.Fatalf("unexpected name after write: %s", name)
	}
}
