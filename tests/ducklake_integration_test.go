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

	update := connector.Record{
		Table:     "widgets",
		Operation: connector.OpUpdate,
		Key:       recordKey(t, map[string]any{"id": 2}),
		After: map[string]any{
			"id":    2,
			"name":  "gamma",
			"extra": "v3",
		},
		Timestamp: time.Now().UTC(),
	}
	batch = connector.Batch{Records: []connector.Record{update}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "3"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("update write: %v", err)
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
	if count != 1 {
		t.Fatalf("expected 1 row after delete, got %d", count)
	}

	var finalName, finalExtra string
	if err := verifyDB.QueryRowContext(ctx, "SELECT name, extra FROM widgets WHERE id = 2").Scan(&finalName, &finalExtra); err != nil {
		t.Fatalf("select final row: %v", err)
	}
	if finalName != "gamma" || finalExtra != "v3" {
		t.Fatalf("unexpected final values: name=%s extra=%s", finalName, finalExtra)
	}
}
