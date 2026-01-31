package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresDestinationDDLAndMutations(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	tableName := fmt.Sprintf("wallaby_dest_%d", time.Now().UnixNano())
	fullTable := fmt.Sprintf(`public."%s"`, tableName)

	dest := &pgdest.Destination{}
	spec := connector.Spec{
		Name: "dest",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                dsn,
			"schema":             "public",
			"write_mode":         "target",
			"meta_table_enabled": "false",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schemaDef := connector.Schema{
		Name:      tableName,
		Namespace: "public",
		Columns: []connector.Column{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "text"},
		},
	}
	createDDL := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("CREATE TABLE %s (id int primary key, name text)", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, createDDL); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTable))
	}()

	insert := connector.Record{
		Table:     tableName,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 1}),
		After:     map[string]any{"id": 1, "name": "alpha"},
	}
	update := connector.Record{
		Table:     tableName,
		Operation: connector.OpUpdate,
		Key:       recordKey(t, map[string]any{"id": 1}),
		After:     map[string]any{"id": 1, "name": "beta"},
	}
	if err := dest.Write(ctx, connector.Batch{Schema: schemaDef, Records: []connector.Record{insert, update}}); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	alterDDL := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra text", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, alterDDL); err != nil {
		t.Fatalf("apply alter ddl: %v", err)
	}

	renameDDL := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s RENAME COLUMN name TO display_name", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, renameDDL); err != nil {
		t.Fatalf("apply rename ddl: %v", err)
	}

	typeDDL := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN extra TYPE varchar(32)", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, typeDDL); err != nil {
		t.Fatalf("apply type ddl: %v", err)
	}

	schemaDef.Columns = []connector.Column{
		{Name: "id", Type: "int"},
		{Name: "display_name", Type: "text"},
		{Name: "extra", Type: "varchar"},
	}

	insertAfter := connector.Record{
		Table:     tableName,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 2}),
		After: map[string]any{
			"id":           2,
			"display_name": "gamma",
			"extra":        "v2",
		},
	}
	if err := dest.Write(ctx, connector.Batch{Schema: schemaDef, Records: []connector.Record{insertAfter}}); err != nil {
		t.Fatalf("write insert after ddl: %v", err)
	}

	var extra string
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT extra FROM %s WHERE id = 2", fullTable)).Scan(&extra); err != nil {
		t.Fatalf("select extra: %v", err)
	}
	if extra != "v2" {
		t.Fatalf("unexpected extra after ddl: %s", extra)
	}

	deleteRec1 := connector.Record{
		Table:     tableName,
		Operation: connector.OpDelete,
		Key:       recordKey(t, map[string]any{"id": 1}),
	}
	deleteRec2 := connector.Record{
		Table:     tableName,
		Operation: connector.OpDelete,
		Key:       recordKey(t, map[string]any{"id": 2}),
	}
	if err := dest.Write(ctx, connector.Batch{Schema: schemaDef, Records: []connector.Record{deleteRec1, deleteRec2}}); err != nil {
		t.Fatalf("write delete: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", fullTable)).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected rows to be deleted, count=%d", count)
	}
}
