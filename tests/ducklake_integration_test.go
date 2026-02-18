package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/josephjohncox/wallaby/connectors/destinations/ducklake"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDuckLakeDestination(t *testing.T) {
	if os.Getenv("WALLABY_TEST_DUCKLAKE") == "0" {
		t.Skip("ducklake test disabled")
	}

	ctx := context.Background()
	baseDir := t.TempDir()
	dsn := filepath.Join(baseDir, "ducklake.db")
	catalog := filepath.Join(baseDir, "metadata.ducklake")
	dataPath := filepath.Join(baseDir, "data")
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}

	setupDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		t.Skipf("duckdb not available: %v", err)
	}
	if _, err := setupDB.ExecContext(ctx, "INSTALL ducklake"); err != nil {
		_ = setupDB.Close()
		t.Skipf("ducklake extension not available: %v", err)
	}
	if _, err := setupDB.ExecContext(ctx, "LOAD ducklake"); err != nil {
		_ = setupDB.Close()
		t.Skipf("ducklake load failed: %v", err)
	}
	_ = setupDB.Close()

	dest := &ducklake.Destination{}
	spec := connector.Spec{
		Name: "ducklake-test",
		Type: connector.EndpointDuckLake,
		Options: map[string]string{
			"dsn":                dsn,
			"catalog":            catalog,
			"catalog_name":       "lake",
			"data_path":          dataPath,
			"override_data_path": "true",
			"meta_table_enabled": "false",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			t.Skipf("ducklake open failed: %v", err)
		}
		t.Skipf("ducklake destination not available: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = dest.Close(ctx)
		}
	}()

	schema := connector.Schema{
		Name: "widgets",
		Columns: []connector.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	createDDL := connector.Record{
		Table:     "widgets",
		Operation: connector.OpDDL,
		DDL:       "CREATE TABLE widgets (id INTEGER, name VARCHAR)",
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, createDDL); err != nil {
		t.Fatalf("apply create ddl: %v", err)
	}

	record := connector.Record{
		Table:     "widgets",
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 1}),
		After: map[string]any{
			"id":   1,
			"name": "alpha",
		},
		Timestamp: time.Now().UTC(),
	}

	batch := connector.Batch{Records: []connector.Record{record}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "1"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	ddlRecord := connector.Record{
		Table:     "widgets",
		Operation: connector.OpDDL,
		DDL:       "ALTER TABLE widgets ADD COLUMN extra TEXT",
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, ddlRecord); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}

	schema.Columns = append(schema.Columns, connector.Column{Name: "extra", Type: "TEXT"})
	withExtra := connector.Record{
		Table:     "widgets",
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 2}),
		After: map[string]any{
			"id":    2,
			"name":  "beta",
			"extra": "v2",
		},
		Timestamp: time.Now().UTC(),
	}
	batch = connector.Batch{Records: []connector.Record{withExtra}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "2"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write with extra: %v", err)
	}

	renameDDL := connector.Record{
		Table:     "widgets",
		Operation: connector.OpDDL,
		DDL:       "ALTER TABLE widgets RENAME COLUMN name TO display_name",
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, renameDDL); err != nil {
		t.Fatalf("apply rename ddl: %v", err)
	}

	typeDDL := connector.Record{
		Table:     "widgets",
		Operation: connector.OpDDL,
		DDL:       "ALTER TABLE widgets ALTER COLUMN extra SET DATA TYPE VARCHAR",
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, typeDDL); err != nil {
		t.Fatalf("apply type ddl: %v", err)
	}

	schema.Columns = []connector.Column{
		{Name: "id", Type: "INTEGER"},
		{Name: "display_name", Type: "VARCHAR"},
		{Name: "extra", Type: "VARCHAR"},
	}
	renameInsert := connector.Record{
		Table:     "widgets",
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 3}),
		After: map[string]any{
			"id":           3,
			"display_name": "delta",
			"extra":        "v4",
		},
		Timestamp: time.Now().UTC(),
	}
	batch = connector.Batch{Records: []connector.Record{renameInsert}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "2b"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write after rename/type ddl: %v", err)
	}

	update := connector.Record{
		Table:     "widgets",
		Operation: connector.OpUpdate,
		Key:       recordKey(t, map[string]any{"id": 2}),
		After: map[string]any{
			"id":           2,
			"display_name": "gamma",
			"extra":        "v3",
		},
		Timestamp: time.Now().UTC(),
	}
	batch = connector.Batch{Records: []connector.Record{update}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "3"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("update write: %v", err)
	}

	dropDDL := connector.Record{
		Table:     "widgets",
		Operation: connector.OpDDL,
		DDL:       "ALTER TABLE widgets DROP COLUMN extra",
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, dropDDL); err != nil {
		t.Fatalf("apply drop ddl: %v", err)
	}

	schema.Columns = []connector.Column{
		{Name: "id", Type: "INTEGER"},
		{Name: "display_name", Type: "VARCHAR"},
	}
	dropInsert := connector.Record{
		Table:     "widgets",
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 4}),
		After: map[string]any{
			"id":           4,
			"display_name": "omega",
		},
		Timestamp: time.Now().UTC(),
	}
	batch = connector.Batch{Records: []connector.Record{dropInsert}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "3b"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write after drop ddl: %v", err)
	}

	del := connector.Record{
		Table:     "widgets",
		Operation: connector.OpDelete,
		Key:       recordKey(t, map[string]any{"id": 1}),
		Timestamp: time.Now().UTC(),
	}
	batch = connector.Batch{Records: []connector.Record{del}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "4"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("delete write: %v", err)
	}

	if err := dest.Close(ctx); err != nil {
		t.Fatalf("close destination: %v", err)
	}
	closed = true

	verifyDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer verifyDB.Close()
	if _, err := verifyDB.ExecContext(ctx, "INSTALL ducklake"); err != nil {
		t.Fatalf("install ducklake: %v", err)
	}
	if _, err := verifyDB.ExecContext(ctx, "LOAD ducklake"); err != nil {
		t.Fatalf("load ducklake: %v", err)
	}
	attachStmt := fmt.Sprintf("ATTACH 'ducklake:%s' AS lake (DATA_PATH '%s')", catalog, dataPath)
	if _, err := verifyDB.ExecContext(ctx, attachStmt); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	if _, err := verifyDB.ExecContext(ctx, "USE lake"); err != nil {
		t.Fatalf("use ducklake catalog: %v", err)
	}

	var count int
	if err := verifyDB.QueryRowContext(ctx, "SELECT count(*) FROM widgets").Scan(&count); err != nil {
		t.Fatalf("count after delete: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows after delete, got %d", count)
	}

	var finalName string
	if err := verifyDB.QueryRowContext(ctx, "SELECT display_name FROM widgets WHERE id = 2").Scan(&finalName); err != nil {
		t.Fatalf("select final row: %v", err)
	}
	if finalName != "gamma" {
		t.Fatalf("unexpected final values: name=%s", finalName)
	}

	var renamedName string
	if err := verifyDB.QueryRowContext(ctx, "SELECT display_name FROM widgets WHERE id = 3").Scan(&renamedName); err != nil {
		t.Fatalf("select renamed row: %v", err)
	}
	if renamedName != "delta" {
		t.Fatalf("unexpected renamed values: name=%s", renamedName)
	}

	var droppedName string
	if err := verifyDB.QueryRowContext(ctx, "SELECT display_name FROM widgets WHERE id = 4").Scan(&droppedName); err != nil {
		t.Fatalf("select dropped row: %v", err)
	}
	if droppedName != "omega" {
		t.Fatalf("unexpected dropped values: name=%s", droppedName)
	}

	colRows, err := verifyDB.QueryContext(ctx, "PRAGMA table_info('widgets')")
	if err != nil {
		t.Fatalf("table info: %v", err)
	}
	defer colRows.Close()
	var columns []string
	for colRows.Next() {
		var cid int
		var name string
		var colType string
		var notNull any
		var defaultVal any
		var pk any
		if err := colRows.Scan(&cid, &name, &colType, &notNull, &defaultVal, &pk); err != nil {
			t.Fatalf("scan table info: %v", err)
		}
		columns = append(columns, name)
	}
	if err := colRows.Err(); err != nil {
		t.Fatalf("iterate table info: %v", err)
	}
	for _, col := range columns {
		if col == "extra" {
			t.Fatalf("expected extra column to be dropped, still present")
		}
	}

	renameDest := &ducklake.Destination{}
	if err := renameDest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination for rename/drop: %v", err)
	}
	defer renameDest.Close(ctx)

	renameTableDDL := connector.Record{
		Table:     "widgets",
		Operation: connector.OpDDL,
		DDL:       "ALTER TABLE widgets RENAME TO widgets_renamed",
		Timestamp: time.Now().UTC(),
	}
	if err := renameDest.ApplyDDL(ctx, schema, renameTableDDL); err != nil {
		t.Fatalf("apply rename table ddl: %v", err)
	}

	renamedSchema := connector.Schema{
		Name: "widgets_renamed",
		Columns: []connector.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "display_name", Type: "VARCHAR"},
		},
	}
	renamedInsert := connector.Record{
		Table:     "widgets_renamed",
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 5}),
		After: map[string]any{
			"id":           5,
			"display_name": "lambda",
		},
		Timestamp: time.Now().UTC(),
	}
	if err := renameDest.Write(ctx, connector.Batch{
		Records:    []connector.Record{renamedInsert},
		Schema:     renamedSchema,
		Checkpoint: connector.Checkpoint{LSN: "5"},
	}); err != nil {
		t.Fatalf("write after rename table ddl: %v", err)
	}
	var renamedRow string
	if err := verifyDB.QueryRowContext(ctx, "SELECT display_name FROM widgets_renamed WHERE id = 5").Scan(&renamedRow); err != nil {
		t.Fatalf("select renamed table row: %v", err)
	}
	if renamedRow != "lambda" {
		t.Fatalf("unexpected value after rename table ddl: %s", renamedRow)
	}

	dropTableDDL := connector.Record{
		Table:     "widgets_renamed",
		Operation: connector.OpDDL,
		DDL:       "DROP TABLE IF EXISTS widgets_renamed",
		Timestamp: time.Now().UTC(),
	}
	if err := renameDest.ApplyDDL(ctx, renamedSchema, dropTableDDL); err != nil {
		t.Fatalf("apply drop table ddl: %v", err)
	}

	var tableCount int
	if err := verifyDB.QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE lower(table_name) = 'widgets_renamed'`,
	).Scan(&tableCount); err != nil {
		t.Fatalf("count renamed tables: %v", err)
	}
	if tableCount != 0 {
		t.Fatalf("expected widgets_renamed to be dropped, found %d entries", tableCount)
	}
}

func TestDuckLakeDestinationGeneratedColumns(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	dsn := filepath.Join(baseDir, "wallaby_generated.ducklake")
	catalog := filepath.Join(baseDir, "metadata.ducklake")
	dataPath := filepath.Join(baseDir, "data")
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}

	setupDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		t.Skipf("duckdb not available: %v", err)
	}
	if _, err := setupDB.ExecContext(ctx, "INSTALL ducklake"); err != nil {
		_ = setupDB.Close()
		t.Skipf("ducklake extension not available: %v", err)
	}
	if _, err := setupDB.ExecContext(ctx, "LOAD ducklake"); err != nil {
		_ = setupDB.Close()
		t.Skipf("ducklake load failed: %v", err)
	}
	_ = setupDB.Close()

	dest := &ducklake.Destination{}
	table := fmt.Sprintf("wallaby_ducklake_generated_%d", time.Now().UnixNano())
	spec := connector.Spec{
		Name: "ducklake-generated",
		Type: connector.EndpointDuckLake,
		Options: map[string]string{
			"dsn":                dsn,
			"catalog":            catalog,
			"catalog_name":       "lake",
			"data_path":          dataPath,
			"override_data_path": "true",
			"meta_table_enabled": "false",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Skipf("ducklake destination not available: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = dest.Close(ctx)
		}
	}()

	schema := connector.Schema{
		Name: table,
		Columns: []connector.Column{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "VARCHAR"},
		},
	}

	created := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("CREATE TABLE %s (id INTEGER, name VARCHAR)", table),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, created); err != nil {
		t.Fatalf("apply create ddl: %v", err)
	}

	insert := connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		After: map[string]any{
			"id":   1,
			"name": "alpha",
		},
	}
	if err := dest.Write(ctx, connector.Batch{
		Records:    []connector.Record{insert},
		Schema:     schema,
		Checkpoint: connector.Checkpoint{LSN: "1"},
	}); err != nil {
		t.Fatalf("write base row: %v", err)
	}

	addGenerated := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL: fmt.Sprintf(`ALTER TABLE %s ADD COLUMN display_name TEXT GENERATED ALWAYS AS (name || '::' || CAST(id AS VARCHAR)) STORED,
			ADD COLUMN row_id INT GENERATED BY DEFAULT AS IDENTITY`, table),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, addGenerated); err != nil {
		t.Fatalf("apply add generated ddl: %v", err)
	}

	schema.Columns = append(schema.Columns,
		connector.Column{
			Name:       "display_name",
			Type:       "TEXT",
			Generated:  true,
			Expression: "COALESCE(name || '::' || CAST(id AS VARCHAR), '')",
		},
		connector.Column{
			Name:       "row_id",
			Type:       "INT",
			Generated:  true,
			Expression: "gen_random_id()",
		},
	)

	insertGenerated := connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		After: map[string]any{
			"id":   2,
			"name": "beta",
		},
	}
	if err := dest.Write(ctx, connector.Batch{
		Records:    []connector.Record{insertGenerated},
		Schema:     schema,
		Checkpoint: connector.Checkpoint{LSN: "2"},
	}); err != nil {
		t.Fatalf("write after generated ddl: %v", err)
	}
	if err := dest.Close(ctx); err != nil {
		t.Fatalf("close destination before verify: %v", err)
	}
	closed = true

	verifyDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer verifyDB.Close()
	if _, err := verifyDB.ExecContext(ctx, "INSTALL ducklake"); err != nil {
		t.Fatalf("install ducklake: %v", err)
	}
	if _, err := verifyDB.ExecContext(ctx, "LOAD ducklake"); err != nil {
		t.Fatalf("load ducklake: %v", err)
	}
	attachStmt := fmt.Sprintf("ATTACH 'ducklake:%s' AS lake (DATA_PATH '%s')", catalog, dataPath)
	if _, err := verifyDB.ExecContext(ctx, attachStmt); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	if _, err := verifyDB.ExecContext(ctx, "USE lake"); err != nil {
		t.Fatalf("use ducklake catalog: %v", err)
	}

	var displayName sql.NullString
	var rowID sql.NullInt64
	if err := verifyDB.QueryRowContext(ctx, fmt.Sprintf("SELECT display_name, row_id FROM %s WHERE id = 2", table)).Scan(&displayName, &rowID); err != nil {
		t.Fatalf("select generated columns: %v", err)
	}
	if displayName.Valid {
		t.Fatalf("expected display_name to be absent from generated write path, got %s", displayName.String)
	}
	if rowID.Valid {
		t.Fatalf("expected row_id to be absent from generated write path, got %d", rowID.Int64)
	}
}

func TestDuckLakeDestinationDefaults(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	dsn := filepath.Join(baseDir, "wallaby_defaults.ducklake")
	catalog := filepath.Join(baseDir, "metadata.ducklake")
	dataPath := filepath.Join(baseDir, "data")
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatalf("mkdir data: %v", err)
	}

	setupDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		t.Skipf("duckdb not available: %v", err)
	}
	if _, err := setupDB.ExecContext(ctx, "INSTALL ducklake"); err != nil {
		_ = setupDB.Close()
		t.Skipf("ducklake extension not available: %v", err)
	}
	if _, err := setupDB.ExecContext(ctx, "LOAD ducklake"); err != nil {
		_ = setupDB.Close()
		t.Skipf("ducklake load failed: %v", err)
	}
	_ = setupDB.Close()

	dest := &ducklake.Destination{}
	table := fmt.Sprintf("wallaby_ducklake_defaults_%d", time.Now().UnixNano())
	spec := connector.Spec{
		Name: "ducklake-defaults",
		Type: connector.EndpointDuckLake,
		Options: map[string]string{
			"dsn":                dsn,
			"catalog":            catalog,
			"catalog_name":       "lake",
			"data_path":          dataPath,
			"override_data_path": "true",
			"meta_table_enabled": "false",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Skipf("ducklake destination not available: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = dest.Close(ctx)
		}
	}()

	schema := connector.Schema{
		Name: table,
		Columns: []connector.Column{
			{Name: "id", Type: "INTEGER"},
		},
	}

	createDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("CREATE TABLE %s (id INTEGER)", table),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, createDDL); err != nil {
		t.Fatalf("apply create ddl: %v", err)
	}

	addDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ADD COLUMN status TEXT", table),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, addDDL); err != nil {
		t.Fatalf("apply add ddl: %v", err)
	}
	schema.Columns = append(schema.Columns, connector.Column{Name: "status", Type: "TEXT"})

	setDefaultDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN status SET DEFAULT 'seed'", table),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, setDefaultDDL); err != nil {
		t.Fatalf("apply set default ddl: %v", err)
	}

	if err := dest.Write(ctx, connector.Batch{
		Records: []connector.Record{{
			Table:     table,
			Operation: connector.OpInsert,
			After: map[string]any{
				"id": 1,
			},
		}},
		Schema:     schema,
		Checkpoint: connector.Checkpoint{LSN: "1"},
	}); err != nil {
		t.Fatalf("write with default: %v", err)
	}

	dropDefaultDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN status DROP DEFAULT", table),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schema, dropDefaultDDL); err != nil {
		t.Fatalf("apply drop default ddl: %v", err)
	}

	if err := dest.Write(ctx, connector.Batch{
		Records: []connector.Record{{
			Table:     table,
			Operation: connector.OpInsert,
			After: map[string]any{
				"id": 2,
			},
		}},
		Schema:     schema,
		Checkpoint: connector.Checkpoint{LSN: "2"},
	}); err != nil {
		t.Fatalf("write with nullable status: %v", err)
	}
	if err := dest.Close(ctx); err != nil {
		t.Fatalf("close destination before verify: %v", err)
	}
	closed = true

	verifyDB, err := sql.Open("duckdb", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer verifyDB.Close()
	if _, err := verifyDB.ExecContext(ctx, "INSTALL ducklake"); err != nil {
		t.Fatalf("install ducklake: %v", err)
	}
	if _, err := verifyDB.ExecContext(ctx, "LOAD ducklake"); err != nil {
		t.Fatalf("load ducklake: %v", err)
	}
	attachStmt := fmt.Sprintf("ATTACH 'ducklake:%s' AS lake (DATA_PATH '%s')", catalog, dataPath)
	if _, err := verifyDB.ExecContext(ctx, attachStmt); err != nil {
		t.Fatalf("attach ducklake: %v", err)
	}
	if _, err := verifyDB.ExecContext(ctx, "USE lake"); err != nil {
		t.Fatalf("use ducklake catalog: %v", err)
	}

	var status string
	if err := verifyDB.QueryRowContext(ctx, fmt.Sprintf("SELECT status FROM %s WHERE id = 1", table)).Scan(&status); err != nil {
		t.Fatalf("select seeded status: %v", err)
	}
	if status != "seed" {
		t.Fatalf("expected seeded status 'seed', got %q", status)
	}

	var nullableStatus sql.NullString
	if err := verifyDB.QueryRowContext(ctx, fmt.Sprintf("SELECT status FROM %s WHERE id = 2", table)).Scan(&nullableStatus); err != nil {
		t.Fatalf("select nullable status: %v", err)
	}
	if nullableStatus.Valid {
		t.Fatalf("expected nullable status to be NULL, got %q", nullableStatus.String)
	}
}
