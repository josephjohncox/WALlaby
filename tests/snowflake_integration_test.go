package tests

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
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
	metaSchema := fmt.Sprintf("WALLABY_META_%d", time.Now().UnixNano())
	metaTable := "__METADATA"
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
			"meta_table_enabled": "true",
			"meta_schema":        metaSchema,
			"meta_table":         metaTable,
			"write_mode":         "target",
			"schema_registry":    "local",
			"flow_id":            "snowflake-test",
		},
	}
	if usingFakesnow() {
		spec.Options["disable_transactions"] = "true"
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = dest.Close(ctx)
		}
	}()

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

	ddlRecord := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra text", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, ddlRecord); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}

	schemaDef.Columns = append(schemaDef.Columns, connector.Column{Name: "extra", Type: "STRING"})
	insert = connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 2}),
		After: map[string]any{
			"id":    2,
			"name":  "gamma",
			"extra": "v2",
		},
	}
	batch = connector.Batch{Records: []connector.Record{insert}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "4"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert after ddl: %v", err)
	}

	var extra string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT extra FROM %s WHERE id = 2", fullTable)).Scan(&extra); err != nil {
		t.Fatalf("select extra: %v", err)
	}
	if strings.TrimSpace(extra) != "v2" {
		t.Fatalf("unexpected extra after ddl: %s", extra)
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
		t.Fatalf("apply rename ddl: %v", err)
	}
	schemaDef.Columns = []connector.Column{
		{Name: "id", Type: "NUMBER"},
		{Name: "display_name", Type: "STRING"},
		{Name: "extra", Type: "STRING"},
	}
	insert = connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 3}),
		After: map[string]any{
			"id":           3,
			"display_name": "delta",
			"extra":        "v3",
		},
	}
	batch = connector.Batch{Records: []connector.Record{insert}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "5"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert after rename ddl: %v", err)
	}
	var renamed string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT display_name FROM %s WHERE id = 3", fullTable)).Scan(&renamed); err != nil {
		t.Fatalf("select renamed column: %v", err)
	}
	if strings.TrimSpace(renamed) != "delta" {
		t.Fatalf("unexpected display_name after rename ddl: %s", renamed)
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
	schemaDef.Columns = []connector.Column{
		{Name: "id", Type: "NUMBER"},
		{Name: "display_name", Type: "STRING"},
		{Name: "extra", Type: "VARCHAR"},
	}
	insert = connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 4}),
		After: map[string]any{
			"id":           4,
			"display_name": "epsilon",
			"extra":        "v4",
		},
	}
	batch = connector.Batch{Records: []connector.Record{insert}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "6"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert after type ddl: %v", err)
	}
	var typed string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT extra FROM %s WHERE id = 4", fullTable)).Scan(&typed); err != nil {
		t.Fatalf("select extra after type ddl: %v", err)
	}
	if strings.TrimSpace(typed) != "v4" {
		t.Fatalf("unexpected extra after type ddl: %s", typed)
	}

	multiDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ADD COLUMN flag boolean, ADD COLUMN note text", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, multiDDL); err != nil {
		t.Fatalf("apply multi-column ddl: %v", err)
	}
	schemaDef.Columns = append(schemaDef.Columns,
		connector.Column{Name: "flag", Type: "BOOLEAN"},
		connector.Column{Name: "note", Type: "STRING"},
	)
	insert = connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 5}),
		After: map[string]any{
			"id":           5,
			"display_name": "zeta",
			"extra":        "v5",
			"flag":         true,
			"note":         "n5",
		},
	}
	batch = connector.Batch{Records: []connector.Record{insert}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "7"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert after multi-column ddl: %v", err)
	}
	var flag bool
	var note string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT flag, note FROM %s WHERE id = 5", fullTable)).Scan(&flag, &note); err != nil {
		t.Fatalf("select flag/note after multi ddl: %v", err)
	}
	if !flag || strings.TrimSpace(note) != "n5" {
		t.Fatalf("unexpected values after multi ddl: flag=%v note=%s", flag, note)
	}

	if !usingFakesnow() {
		defaultDDL := connector.Record{
			Table:     table,
			Operation: connector.OpDDL,
			DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN note SET DEFAULT 'seed', ALTER COLUMN note SET NOT NULL", fullTable),
			Timestamp: time.Now().UTC(),
		}
		if err := dest.ApplyDDL(ctx, schemaDef, defaultDDL); err != nil {
			t.Fatalf("apply set default/not null ddl: %v", err)
		}

		insert = connector.Record{
			Table:     table,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 6}),
			After: map[string]any{
				"id":           6,
				"display_name": "eta",
				"extra":        "v6",
				"flag":         false,
			},
		}
		batch = connector.Batch{Records: []connector.Record{insert}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "8"}}
		if err := dest.Write(ctx, batch); err != nil {
			t.Fatalf("insert after set default/not null ddl: %v", err)
		}
		var seeded string
		if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT note FROM %s WHERE id = 6", fullTable)).Scan(&seeded); err != nil {
			t.Fatalf("select note default: %v", err)
		}
		if strings.TrimSpace(seeded) != "seed" {
			t.Fatalf("unexpected note default after ddl: %s", seeded)
		}

		dropDefaultDDL := connector.Record{
			Table:     table,
			Operation: connector.OpDDL,
			DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN note DROP DEFAULT, ALTER COLUMN note DROP NOT NULL", fullTable),
			Timestamp: time.Now().UTC(),
		}
		if err := dest.ApplyDDL(ctx, schemaDef, dropDefaultDDL); err != nil {
			t.Fatalf("apply drop default/not null ddl: %v", err)
		}
		insert = connector.Record{
			Table:     table,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 7}),
			After: map[string]any{
				"id":           7,
				"display_name": "theta",
				"extra":        "v7",
				"flag":         true,
			},
		}
		batch = connector.Batch{Records: []connector.Record{insert}, Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "9"}}
		if err := dest.Write(ctx, batch); err != nil {
			t.Fatalf("insert after drop default/not null ddl: %v", err)
		}
		var nullableNote sql.NullString
		if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT note FROM %s WHERE id = 7", fullTable)).Scan(&nullableNote); err != nil {
			t.Fatalf("select nullable note: %v", err)
		}
		if nullableNote.Valid {
			t.Fatalf("expected note to be NULL after dropping default/not null, got %q", nullableNote.String)
		}
	}

	renamedTable := table + "_renamed"
	renamedFullTable := quoteSnowflakeIdent(renamedTable)
	if schema != "" {
		renamedFullTable = quoteSnowflakeIdent(schema) + "." + quoteSnowflakeIdent(renamedTable)
	}
	renameTableDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s RENAME TO %s", fullTable, quoteSnowflakeIdent(renamedTable)),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, renameTableDDL); err != nil {
		t.Fatalf("apply rename table ddl: %v", err)
	}
	if err := dest.Close(ctx); err != nil {
		t.Fatalf("close destination before reopen: %v", err)
	}
	closed = true

	renamedDest := &snowflake.Destination{}
	renamedSpec := connector.Spec{
		Name: "snowflake-test-renamed",
		Type: connector.EndpointSnowflake,
		Options: map[string]string{
			"dsn":                dsn,
			"schema":             schema,
			"table":              renamedTable,
			"meta_table_enabled": "false",
			"write_mode":         "target",
		},
	}
	if usingFakesnow() {
		renamedSpec.Options["disable_transactions"] = "true"
	}
	if err := renamedDest.Open(ctx, renamedSpec); err != nil {
		t.Fatalf("open renamed destination: %v", err)
	}
	defer renamedDest.Close(ctx)

	renamedSchemaDef := schemaDef
	renamedSchemaDef.Name = renamedTable
	insert = connector.Record{
		Table:     renamedTable,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 8}),
		After: map[string]any{
			"id":           8,
			"display_name": "iota",
			"extra":        "v8",
			"flag":         true,
			"note":         "n8",
		},
	}
	batch = connector.Batch{Records: []connector.Record{insert}, Schema: renamedSchemaDef, Checkpoint: connector.Checkpoint{LSN: "10"}}
	if err := renamedDest.Write(ctx, batch); err != nil {
		t.Fatalf("insert after rename table ddl: %v", err)
	}
	var renamedVal string
	if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT display_name FROM %s WHERE id = 8", renamedFullTable)).Scan(&renamedVal); err != nil {
		t.Fatalf("select after rename table ddl: %v", err)
	}
	if strings.TrimSpace(renamedVal) != "iota" {
		t.Fatalf("unexpected value after rename table ddl: %s", renamedVal)
	}

	dropTableDDL := connector.Record{
		Table:     renamedTable,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("DROP TABLE IF EXISTS %s", renamedFullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := renamedDest.ApplyDDL(ctx, renamedSchemaDef, dropTableDDL); err != nil {
		t.Fatalf("apply drop table ddl: %v", err)
	}
	if _, err := setupDB.ExecContext(ctx, fmt.Sprintf("SELECT count() FROM %s", renamedFullTable)); err == nil {
		t.Fatalf("expected renamed table to be dropped")
	}
}

func quoteSnowflakeIdent(value string) string {
	if value == "" {
		return value
	}
	escaped := strings.ReplaceAll(value, "\"", "\"\"")
	return "\"" + escaped + "\""
}
