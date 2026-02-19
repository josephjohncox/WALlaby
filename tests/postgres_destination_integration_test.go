package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
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

func TestPostgresDestinationPlanDDL(t *testing.T) {
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

	tableName := fmt.Sprintf("wallaby_plan_%d", time.Now().UnixNano())
	fullTable := fmt.Sprintf(`public."%s"`, tableName)

	dest := &pgdest.Destination{}
	spec := connector.Spec{
		Name: "plan-dest",
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
		},
	}
	createDDL := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("CREATE TABLE %s (id int primary key)", fullTable),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, createDDL); err != nil {
		t.Fatalf("apply create ddl: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTable))
	}()

	if err := dest.Write(ctx, connector.Batch{
		Schema:     schemaDef,
		Checkpoint: connector.Checkpoint{LSN: "1"},
		Records: []connector.Record{{
			Table:     tableName,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 1}),
			After: map[string]any{
				"id": 1,
			},
		}},
	}); err != nil {
		t.Fatalf("write base row: %v", err)
	}

	planAdd := internalschema.Plan{
		Changes: []internalschema.Change{
			{
				Type:      internalschema.ChangeAddColumn,
				Namespace: "public",
				Table:     tableName,
				Column:    "display_name",
				ToType:    "text",
				Nullable:  true,
			},
		},
	}
	planAddBytes, err := json.Marshal(planAdd)
	if err != nil {
		t.Fatalf("marshal plan add: %v", err)
	}
	if err := dest.ApplyDDL(ctx, schemaDef, connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDLPlan:   planAddBytes,
	}); err != nil {
		t.Fatalf("apply plan add ddl: %v", err)
	}

	schemaDef.Columns = append(schemaDef.Columns, connector.Column{
		Name: "display_name",
		Type: "text",
	})
	if err := dest.Write(ctx, connector.Batch{
		Schema:     schemaDef,
		Checkpoint: connector.Checkpoint{LSN: "2"},
		Records: []connector.Record{{
			Table:     tableName,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 2}),
			After: map[string]any{
				"id":           2,
				"display_name": "second",
			},
		}},
	}); err != nil {
		t.Fatalf("write after plan add: %v", err)
	}

	var fromPlanAdd string
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT display_name FROM %s WHERE id = $1", fullTable), 2).Scan(&fromPlanAdd); err != nil {
		t.Fatalf("query plan add: %v", err)
	}
	if fromPlanAdd != "second" {
		t.Fatalf("expected plan-added column write to persist, got %q", fromPlanAdd)
	}

	planAlterRename := internalschema.Plan{
		Changes: []internalschema.Change{
			{
				Type:      internalschema.ChangeAlterColumn,
				Namespace: "public",
				Table:     tableName,
				Column:    "display_name",
				FromType:  "text",
				ToType:    "varchar(64)",
			},
			{
				Type:      internalschema.ChangeRenameColumn,
				Namespace: "public",
				Table:     tableName,
				Column:    "display_name",
				ToColumn:  "title",
			},
		},
	}
	planAlterRenameBytes, err := json.Marshal(planAlterRename)
	if err != nil {
		t.Fatalf("marshal plan alter/rename: %v", err)
	}
	if err := dest.ApplyDDL(ctx, schemaDef, connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDLPlan:   planAlterRenameBytes,
	}); err != nil {
		t.Fatalf("apply plan alter/rename ddl: %v", err)
	}

	schemaDef.Columns = []connector.Column{
		{Name: "id", Type: "int"},
		{Name: "title", Type: "varchar(64)"},
	}
	if err := dest.Write(ctx, connector.Batch{
		Schema:     schemaDef,
		Checkpoint: connector.Checkpoint{LSN: "3"},
		Records: []connector.Record{{
			Table:     tableName,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 3}),
			After: map[string]any{
				"id":    3,
				"title": "third",
			},
		}},
	}); err != nil {
		t.Fatalf("write after plan alter/rename: %v", err)
	}

	var fromPlanRename string
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT title FROM %s WHERE id = $1", fullTable), 3).Scan(&fromPlanRename); err != nil {
		t.Fatalf("query plan rename: %v", err)
	}
	if fromPlanRename != "third" {
		t.Fatalf("expected renamed title write to persist, got %q", fromPlanRename)
	}

	planDrop := internalschema.Plan{
		Changes: []internalschema.Change{
			{
				Type:      internalschema.ChangeDropColumn,
				Namespace: "public",
				Table:     tableName,
				Column:    "title",
			},
		},
	}
	planDropBytes, err := json.Marshal(planDrop)
	if err != nil {
		t.Fatalf("marshal plan drop: %v", err)
	}
	if err := dest.ApplyDDL(ctx, schemaDef, connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDLPlan:   planDropBytes,
	}); err != nil {
		t.Fatalf("apply plan drop ddl: %v", err)
	}

	var columnCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`,
		"public", tableName, "title",
	).Scan(&columnCount); err != nil {
		t.Fatalf("query dropped column metadata: %v", err)
	}
	if columnCount != 0 {
		t.Fatalf("expected title column dropped from plan change, got %d", columnCount)
	}
}
