package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/connectors/destinations/snowpipe"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestSnowpipeAutoIngestUpload(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_SNOWPIPE_DSN")
	stage := os.Getenv("WALLABY_TEST_SNOWPIPE_STAGE")
	schema := os.Getenv("WALLABY_TEST_SNOWPIPE_SCHEMA")
	if dsn == "" || stage == "" {
		t.Skip("WALLABY_TEST_SNOWPIPE_DSN and WALLABY_TEST_SNOWPIPE_STAGE are required for the real-service Snowpipe integration test")
	}
	stagePath := fmt.Sprintf("wallaby_snowpipe_%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), snowflakeTestTimeout())
	defer cancel()
	setupDB, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("open snowflake: %v", err)
	}
	t.Cleanup(func() {
		if err := setupDB.Close(); err != nil {
			t.Errorf("close Snowflake setup connection: %v", err)
		}
	})
	if err := setupDB.PingContext(ctx); err != nil {
		t.Fatalf("ping Snowflake: %v", err)
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
	renamedTable := table + "_renamed"
	renamedFullTable := quoteSnowflakeIdent(renamedTable)
	if schema != "" {
		renamedFullTable = quoteSnowflakeIdent(schema) + "." + renamedFullTable
	}
	stageLocation := joinStageForTest(stage, stagePath)
	metaSchemaIdent := quoteSnowflakeIdent(metaSchema)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cleanupCancel()
		if err := cleanupSnowpipeIntegrationResources(cleanupCtx, setupDB, stageLocation, fullTable, renamedFullTable, metaSchemaIdent); err != nil {
			t.Errorf("Snowpipe integration cleanup: %v", err)
		}
	})
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
	spec.Options["stage_path"] = stagePath

	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		if err := dest.Close(closeCtx); err != nil {
			t.Errorf("close Snowpipe destination: %v", err)
		}
	})
	assertSnowpipeIntegrationPreflight(t, dest, spec)

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
	writeSnowpipeAppendBatch(t, ctx, dest, schemaDef, record, "1", "write batch")

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

	{
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
		writeSnowpipeAppendBatch(t, ctx, dest, schemaDef, record, "2", "write after evolve ddl")
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
		writeSnowpipeAppendBatch(t, ctx, dest, schemaDef, record, "3", "write after rename ddl")
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
		writeSnowpipeAppendBatch(t, ctx, dest, schemaDef, record, "4", "write after set default/not null ddl")
		var seeded string
		if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT note FROM %s WHERE id = 4", fullTable)).Scan(&seeded); err != nil {
			t.Fatalf("select note default: %v", err)
		}
		if seeded != "seed" {
			t.Fatalf("unexpected note default value: %s", seeded)
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
		record = connector.Record{
			Table:     table,
			Operation: connector.OpInsert,
			After: map[string]any{
				"id":           5,
				"display_name": "epsilon",
				"extra":        "v5",
			},
		}
		writeSnowpipeAppendBatch(t, ctx, dest, schemaDef, record, "5", "write after drop default/not null ddl")
		var nullableNote sql.NullString
		if err := setupDB.QueryRowContext(ctx, fmt.Sprintf("SELECT note FROM %s WHERE id = 5", fullTable)).Scan(&nullableNote); err != nil {
			t.Fatalf("select dropped default note: %v", err)
		}
		if nullableNote.Valid {
			t.Fatalf("expected nullable note to be NULL after dropping default/not-null: %s", nullableNote.String)
		}
	}

	{
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

		renameTableDDL := connector.Record{
			Table:     table,
			Operation: connector.OpDDL,
			DDL:       fmt.Sprintf("ALTER TABLE %s RENAME TO %s", fullTable, renamedFullTable),
			Timestamp: time.Now().UTC(),
		}
		if err := dest.ApplyDDL(ctx, schemaDef, renameTableDDL); err != nil {
			t.Fatalf("apply rename table ddl: %v", err)
		}

		var renamedCount int
		if err := setupDB.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.tables WHERE lower(table_name) = ?", strings.ToLower(renamedTable)).Scan(&renamedCount); err != nil {
			t.Fatalf("check renamed table: %v", err)
		}
		if renamedCount != 1 {
			t.Fatalf("expected renamed table %q to exist", renamedTable)
		}

		dropTableDDL := connector.Record{
			Table:     renamedTable,
			Operation: connector.OpDDL,
			DDL:       fmt.Sprintf("DROP TABLE IF EXISTS %s", renamedFullTable),
			Timestamp: time.Now().UTC(),
		}
		if err := dest.ApplyDDL(ctx, schemaDef, dropTableDDL); err != nil {
			t.Fatalf("apply drop table ddl: %v", err)
		}

		var droppedCount int
		if err := setupDB.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.tables WHERE lower(table_name) = ?", strings.ToLower(renamedTable)).Scan(&droppedCount); err != nil {
			t.Fatalf("check dropped table: %v", err)
		}
		if droppedCount != 0 {
			t.Fatalf("expected renamed table %q to be dropped, found %d", renamedTable, droppedCount)
		}
	}
}

func assertSnowpipeIntegrationPreflight(t *testing.T, destination *snowpipe.Destination, spec connector.Spec) {
	t.Helper()
	appendPolicy := connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}
	if err := destination.Capabilities().SupportsTablePolicy(appendPolicy); err != nil {
		t.Fatalf("Snowpipe append policy preflight: %v", err)
	}
	if err := destination.Capabilities().SupportsTablePolicy(connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}}); err == nil {
		t.Fatal("Snowpipe preflight unexpectedly admits upsert")
	}
	for key, want := range map[string]string{
		"auto_ingest":        "false",
		"copy_on_write":      "true",
		"meta_table_enabled": "true",
	} {
		if got := spec.Options[key]; got != want {
			t.Fatalf("Snowpipe %s preflight=%q want=%q", key, got, want)
		}
	}
	if strings.TrimSpace(spec.Options["stage"]) == "" || strings.TrimSpace(spec.Options["stage_path"]) == "" {
		t.Fatal("Snowpipe PUT preflight requires a stage and isolated stage path")
	}
}

func writeSnowpipeAppendBatch(
	t *testing.T,
	ctx context.Context,
	destination *snowpipe.Destination,
	schema connector.Schema,
	record connector.Record,
	lsn string,
	operation string,
) {
	t.Helper()
	batch := connector.Batch{
		Records:     []connector.Record{record},
		Schema:      schema,
		Checkpoint:  connector.Checkpoint{LSN: lsn},
		WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
	}
	if batch.WritePolicy.Mode != connector.ResolvedWriteAppend {
		t.Fatalf("%s preflight write policy=%q", operation, batch.WritePolicy.Mode)
	}
	if err := destination.Write(ctx, batch); err != nil {
		t.Fatalf("%s: %v", operation, err)
	}
}

func cleanupSnowpipeIntegrationResources(
	ctx context.Context,
	db *sql.DB,
	stageLocation string,
	originalTable string,
	renamedTable string,
	metaSchema string,
) error {
	statements := []string{
		fmt.Sprintf("REMOVE %s PATTERN = '.*'", stageLocation),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", renamedTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", originalTable),
		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", metaSchema),
	}
	var cleanupErrors []error
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("%q: %w", statement, err))
		}
	}
	return errors.Join(cleanupErrors...)
}

func TestSnowpipeIntegrationCleanupContinuesAfterFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectExec("REMOVE @stage/isolated").WillReturnError(errors.New("remove failed"))
	mock.ExpectExec("DROP TABLE IF EXISTS renamed").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DROP TABLE IF EXISTS original").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DROP SCHEMA IF EXISTS metadata CASCADE").WillReturnResult(sqlmock.NewResult(0, 0))

	err = cleanupSnowpipeIntegrationResources(context.Background(), db, "@stage/isolated", "original", "renamed", "metadata")
	if err == nil || !strings.Contains(err.Error(), "remove failed") {
		t.Fatalf("cleanup error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
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
