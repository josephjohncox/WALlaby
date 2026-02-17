package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/connectors/destinations/snowpipe"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestSnowpipeAutoIngestUpload(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_SNOWPIPE_DSN")
	stage := os.Getenv("WALLABY_TEST_SNOWPIPE_STAGE")
	var schema string
	var stagePath string
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
	metaSchema := fmt.Sprintf("WALLABY_META_%d", time.Now().UnixNano())
	metaTable := "__METADATA"
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
			"meta_table_enabled":        "true",
			"meta_schema":               metaSchema,
			"meta_table":                metaTable,
			"schema_registry":           "local",
			"flow_id":                   "snowpipe-test",
			"schema":                    schema,
			"table":                     table,
		},
	}
	if usingFakesnow() {
		spec.Options["compat_mode"] = "fakesnow"
	} else {
		stagePath = fmt.Sprintf("wallaby_snowpipe_%d", time.Now().UnixNano())
		spec.Options["stage_path"] = stagePath
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
		Key:       recordKey(t, map[string]any{"id": 1}),
		After:     map[string]any{"id": 1, "name": "alpha"},
	}
	batch := connector.Batch{Records: []connector.Record{record}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "1"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	metaTableIdent := quoteSnowflakeIdent(metaSchema) + "." + quoteSnowflakeIdent(metaTable)
	registryVersionForID := func(id int) int {
		var raw string
		query := fmt.Sprintf(`SELECT "REGISTRY_VERSION" FROM %s WHERE "SOURCE_TABLE" = ? AND "pk_id" = ?`, metaTableIdent)
		if err := setupDB.QueryRowContext(ctx, query, table, id).Scan(&raw); err != nil {
			t.Fatalf("select registry version for id=%d: %v", id, err)
		}
		version, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			t.Fatalf("parse registry version for id=%d (%q): %v", id, raw, err)
		}
		return version
	}
	registryV1 := registryVersionForID(1)

	var name string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT name FROM %s WHERE id = 1", fullTable)).Scan(&name); err != nil {
		t.Fatalf("select after write: %v", err)
	}
	if name != "alpha" {
		t.Fatalf("unexpected name after write: %s", name)
	}

	if !usingFakesnow() {
		evolveDDL := connector.Record{
			Table:     table,
			Operation: connector.OpDDL,
			DDL:       fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra text, ADD COLUMN note text", fullTable),
			Timestamp: time.Now().UTC(),
		}
		if err := dest.ApplyDDL(ctx, schemaDef, evolveDDL); err != nil {
			t.Fatalf("apply evolve ddl: %v", err)
		}
		schemaDef.Columns = append(schemaDef.Columns,
			connector.Column{Name: "extra", Type: "text"},
			connector.Column{Name: "note", Type: "text"},
		)
		record = connector.Record{
			Table:     table,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 2}),
			After: map[string]any{
				"id":    2,
				"name":  "beta",
				"extra": "v2",
				"note":  "n2",
			},
		}
		batch = connector.Batch{Records: []connector.Record{record}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "2"}}
		if err := dest.Write(ctx, batch); err != nil {
			t.Fatalf("write after evolve ddl: %v", err)
		}
		var extra string
		var note string
		if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT extra, note FROM %s WHERE id = 2", fullTable)).Scan(&extra, &note); err != nil {
			t.Fatalf("select extra/note: %v", err)
		}
		if extra != "v2" || note != "n2" {
			t.Fatalf("unexpected extra/note values: extra=%s note=%s", extra, note)
		}
		registryV2 := registryVersionForID(2)
		if registryV1 == registryV2 {
			t.Fatalf("expected registry version to change across schema evolution (v1=%d v2=%d)", registryV1, registryV2)
		}

		renameDDL := connector.Record{
			Table:     table,
			Operation: connector.OpDDL,
			DDL:       fmt.Sprintf("ALTER TABLE %s RENAME COLUMN name TO display_name", fullTable),
			Timestamp: time.Now().UTC(),
		}
		if err := dest.ApplyDDL(ctx, schemaDef, renameDDL); err != nil {
			t.Fatalf("apply rename column ddl: %v", err)
		}
		schemaDef.Columns = []connector.Column{
			{Name: "id", Type: "int"},
			{Name: "display_name", Type: "text"},
			{Name: "extra", Type: "text"},
			{Name: "note", Type: "text"},
		}
		record = connector.Record{
			Table:     table,
			Operation: connector.OpInsert,
			After: map[string]any{
				"id":           3,
				"display_name": "gamma",
				"extra":        "v3",
				"note":         "n3",
			},
		}
		batch = connector.Batch{Records: []connector.Record{record}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "3"}}
		if err := dest.Write(ctx, batch); err != nil {
			t.Fatalf("write after rename ddl: %v", err)
		}
		var displayName string
		if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT display_name FROM %s WHERE id = 3", fullTable)).Scan(&displayName); err != nil {
			t.Fatalf("select display_name: %v", err)
		}
		if displayName != "gamma" {
			t.Fatalf("unexpected display_name after rename ddl: %s", displayName)
		}

		typeDDL := connector.Record{
			Table:     table,
			Operation: connector.OpDDL,
			DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN extra TYPE VARCHAR(32)", fullTable),
			Timestamp: time.Now().UTC(),
		}
		if err := dest.ApplyDDL(ctx, schemaDef, typeDDL); err != nil {
			t.Fatalf("apply type ddl: %v", err)
		}

		defaultDDL := connector.Record{
			Table:     table,
			Operation: connector.OpDDL,
			DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN note SET DEFAULT 'seed', ALTER COLUMN note SET NOT NULL", fullTable),
			Timestamp: time.Now().UTC(),
		}
		if err := dest.ApplyDDL(ctx, schemaDef, defaultDDL); err != nil {
			t.Fatalf("apply set default/not null ddl: %v", err)
		}
		record = connector.Record{
			Table:     table,
			Operation: connector.OpInsert,
			After: map[string]any{
				"id":           4,
				"display_name": "delta",
				"extra":        "v4",
			},
		}
		batch = connector.Batch{Records: []connector.Record{record}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "4"}}
		if err := dest.Write(ctx, batch); err != nil {
			t.Fatalf("write after set default/not null ddl: %v", err)
		}
		var seeded string
		if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT note FROM %s WHERE id = 4", fullTable)).Scan(&seeded); err != nil {
			t.Fatalf("select note default: %v", err)
		}
		if seeded != "seed" {
			t.Fatalf("unexpected note default value: %s", seeded)
		}
	}

	if !usingFakesnow() {
		stageLocation := joinStageForTest(stage, stagePath)
		rows, err := setupDB.QueryContext(ctx, fmt.Sprintf("LIST %s", stageLocation))
		if err != nil {
			t.Fatalf("list stage: %v", err)
		}
		defer rows.Close()
		var staged int
		for rows.Next() {
			staged++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate stage list: %v", err)
		}
		if staged == 0 {
			t.Fatalf("expected staged files in %s", stageLocation)
		}

		copyTable := table
		if schema != "" {
			copyTable = schema + "." + table
		}
		var copyCount int
		if err := setupDB.QueryRowContext(ctx,
			fmt.Sprintf("SELECT count(*) FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'%s'))", strings.ToUpper(copyTable)),
		).Scan(&copyCount); err != nil {
			t.Fatalf("copy history: %v", err)
		}
		if copyCount == 0 {
			t.Fatalf("expected COPY history entries for %s", copyTable)
		}
	}
}

func joinStageForTest(stage, path string) string {
	stage = strings.TrimSpace(stage)
	if stage == "" || path == "" {
		return stage
	}
	stage = strings.TrimSuffix(stage, "/")
	path = strings.TrimPrefix(path, "/")
	return stage + "/" + path
}
