package tests

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestClickHouseAppendChangelog(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("WALLABY_TEST_CLICKHOUSE_DSN not set")
	}
	database := os.Getenv("WALLABY_TEST_CLICKHOUSE_DB")
	if database == "" {
		database = "default"
	}

	ctx := context.Background()
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)); err != nil {
		t.Fatalf("create database: %v", err)
	}

	table := fmt.Sprintf("wallaby_append_%d", time.Now().UnixNano())
	fullTable := fmt.Sprintf("%s.%s", database, table)
	createSQL := fmt.Sprintf(`CREATE TABLE %s (
  id UInt64,
  name String,
  __wallaby_operation String,
  __wallaby_deleted Bool,
  __wallaby_source_position String
) ENGINE = MergeTree ORDER BY (id, __wallaby_source_position)`, fullTable)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create table: %v", err)
	}

	dest := &clickhouse.Destination{}
	spec := connector.Spec{
		Name: "clickhouse-test",
		Type: connector.EndpointClickHouse,
		Options: map[string]string{
			"dsn":                dsn,
			"meta_table_enabled": "false",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schema := connector.Schema{
		Name:      table,
		Namespace: database,
		Columns: []connector.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
			{Name: connector.AppendOperationColumn, Type: "String"},
			{Name: connector.AppendDeletedColumn, Type: "Bool"},
			{Name: connector.AppendSourcePositionColumn, Type: "String"},
		},
	}
	appendPolicy := connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}

	insert := connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": uint64(1)}),
		After: map[string]any{
			"id": uint64(1), "name": "alpha",
			connector.AppendOperationColumn: "insert", connector.AppendDeletedColumn: false, connector.AppendSourcePositionColumn: "1",
		},
	}
	batch := connector.Batch{Records: []connector.Record{insert}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "1"}, WritePolicy: appendPolicy}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert write: %v", err)
	}

	var name string
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT name FROM %s WHERE id = 1 ORDER BY __wallaby_source_position DESC LIMIT 1", fullTable)).Scan(&name); err != nil {
		t.Fatalf("select after insert: %v", err)
	}
	if name != "alpha" {
		t.Fatalf("unexpected name after insert: %s", name)
	}

	update := connector.Record{
		Table:     table,
		Operation: connector.OpUpdate,
		Key:       recordKey(t, map[string]any{"id": uint64(1)}),
		After: map[string]any{
			"id": uint64(1), "name": "beta",
			connector.AppendOperationColumn: "update", connector.AppendDeletedColumn: false, connector.AppendSourcePositionColumn: "2",
		},
	}
	batch = connector.Batch{Records: []connector.Record{update}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "2"}, WritePolicy: appendPolicy}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("update write: %v", err)
	}
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT name FROM %s WHERE id = 1 ORDER BY __wallaby_source_position DESC LIMIT 1", fullTable)).Scan(&name); err != nil {
		t.Fatalf("select after update: %v", err)
	}
	if name != "beta" {
		t.Fatalf("unexpected name after update: %s", name)
	}

	del := connector.Record{
		Table:     table,
		Operation: connector.OpDelete,
		Key:       recordKey(t, map[string]any{"id": uint64(1)}),
		After: map[string]any{
			"id": uint64(1), "name": "beta",
			connector.AppendOperationColumn: "delete", connector.AppendDeletedColumn: true, connector.AppendSourcePositionColumn: "3",
		},
	}
	batch = connector.Batch{Records: []connector.Record{del}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "3"}, WritePolicy: appendPolicy}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("delete write: %v", err)
	}
	var count, tombstones int
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(), countIf(__wallaby_deleted) FROM %s", fullTable)).Scan(&count, &tombstones); err != nil {
		t.Fatalf("count append changelog: %v", err)
	}
	if count != 3 || tombstones != 1 {
		t.Fatalf("append changelog rows/tombstones=%d/%d, want 3/1", count, tombstones)
	}
}

func TestClickHouseStagingAndDDL(t *testing.T) {
	dsn := os.Getenv("WALLABY_TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Skip("WALLABY_TEST_CLICKHOUSE_DSN not set")
	}
	database := os.Getenv("WALLABY_TEST_CLICKHOUSE_DB")
	if database == "" {
		database = "default"
	}

	ctx := context.Background()
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)); err != nil {
		t.Fatalf("create database: %v", err)
	}

	table := fmt.Sprintf("wallaby_staging_%d", time.Now().UnixNano())
	fullTable := fmt.Sprintf("%s.%s", database, table)
	createSQL := fmt.Sprintf(`CREATE TABLE %s (
  id UInt64,
  name String
) ENGINE = MergeTree ORDER BY id`, fullTable)
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create table: %v", err)
	}

	stagingTable := fullTable + "_staging"
	createStaging := fmt.Sprintf(`CREATE TABLE %s (
  id UInt64,
  name String
) ENGINE = MergeTree ORDER BY id`, stagingTable)
	if _, err := db.ExecContext(ctx, createStaging); err != nil {
		t.Fatalf("create staging table: %v", err)
	}

	dest := &clickhouse.Destination{}
	spec := connector.Spec{
		Name: "clickhouse-staging",
		Type: connector.EndpointClickHouse,
		Options: map[string]string{
			"dsn":                dsn,
			"batch_mode":         "staging",
			"batch_resolution":   "replace",
			"meta_table_enabled": "false",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schema := connector.Schema{
		Name:      table,
		Namespace: database,
		Columns: []connector.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "name", Type: "String"},
		},
	}
	appendPolicy := connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}

	load := connector.Record{
		Table:     table,
		Operation: connector.OpLoad,
		Key:       recordKey(t, map[string]any{"id": uint64(10)}),
		After: map[string]any{
			"id":   uint64(10),
			"name": "staged",
		},
	}
	batch := connector.Batch{Records: []connector.Record{load}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "1"}, WritePolicy: appendPolicy}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write staging batch: %v", err)
	}

	if err := dest.ResolveStaging(ctx); err != nil {
		t.Fatalf("resolve staging: %v", err)
	}

	var count int
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count() FROM %s WHERE id = 10", fullTable)).Scan(&count); err != nil {
		t.Fatalf("count after resolve: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected staged row in target, got %d", count)
	}

	ddlRecord := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra text", fullTable),
	}
	if err := dest.ApplyDDL(ctx, schema, ddlRecord); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}

	schema.Columns = append(schema.Columns, connector.Column{Name: "extra", Type: "String"})
	insert := connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": uint64(11)}),
		After: map[string]any{
			"id":    uint64(11),
			"name":  "alpha",
			"extra": "ok",
		},
	}
	batch = connector.Batch{Records: []connector.Record{insert}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "2"}, WritePolicy: appendPolicy}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert after ddl: %v", err)
	}

	var extra string
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT extra FROM %s WHERE id = 11", fullTable)).Scan(&extra); err != nil {
		t.Fatalf("select extra: %v", err)
	}
	if extra != "ok" {
		t.Fatalf("unexpected extra value: %s", extra)
	}

	renameDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s RENAME COLUMN name TO display_name", fullTable),
	}
	if err := dest.ApplyDDL(ctx, schema, renameDDL); err != nil {
		t.Fatalf("apply rename ddl: %v", err)
	}
	schema.Columns = []connector.Column{
		{Name: "id", Type: "UInt64"},
		{Name: "display_name", Type: "String"},
		{Name: "extra", Type: "String"},
	}
	insert = connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": uint64(12)}),
		After: map[string]any{
			"id":           uint64(12),
			"display_name": "renamed",
			"extra":        "v2",
		},
	}
	batch = connector.Batch{Records: []connector.Record{insert}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "3"}, WritePolicy: appendPolicy}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert after rename ddl: %v", err)
	}
	var renamed string
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT display_name FROM %s WHERE id = 12", fullTable)).Scan(&renamed); err != nil {
		t.Fatalf("select display_name: %v", err)
	}
	if renamed != "renamed" {
		t.Fatalf("unexpected display_name after rename ddl: %s", renamed)
	}

	typeDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN extra TYPE text", fullTable),
	}
	if err := dest.ApplyDDL(ctx, schema, typeDDL); err != nil {
		t.Fatalf("apply type ddl: %v", err)
	}
	schema.Columns = []connector.Column{
		{Name: "id", Type: "UInt64"},
		{Name: "display_name", Type: "String"},
		{Name: "extra", Type: "String"},
	}
	insert = connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": uint64(13)}),
		After: map[string]any{
			"id":           uint64(13),
			"display_name": "typed",
			"extra":        "v3",
		},
	}
	batch = connector.Batch{Records: []connector.Record{insert}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "4"}, WritePolicy: appendPolicy}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert after type ddl: %v", err)
	}
	var typed string
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT extra FROM %s WHERE id = 13", fullTable)).Scan(&typed); err != nil {
		t.Fatalf("select extra after type change: %v", err)
	}
	if typed != "v3" {
		t.Fatalf("unexpected extra after type ddl: %s", typed)
	}

	dropDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s DROP COLUMN extra", fullTable),
	}
	if err := dest.ApplyDDL(ctx, schema, dropDDL); err != nil {
		t.Fatalf("apply drop ddl: %v", err)
	}
	schema.Columns = []connector.Column{
		{Name: "id", Type: "UInt64"},
		{Name: "display_name", Type: "String"},
	}
	insert = connector.Record{
		Table:     table,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": uint64(14)}),
		After: map[string]any{
			"id":           uint64(14),
			"display_name": "dropped",
		},
	}
	batch = connector.Batch{Records: []connector.Record{insert}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "5"}, WritePolicy: appendPolicy}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert after drop ddl: %v", err)
	}
	var dropped string
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT display_name FROM %s WHERE id = 14", fullTable)).Scan(&dropped); err != nil {
		t.Fatalf("select after drop ddl: %v", err)
	}
	if dropped != "dropped" {
		t.Fatalf("unexpected display_name after drop ddl: %s", dropped)
	}

	renamedTable := fmt.Sprintf("%s_renamed", table)
	renamedFull := fmt.Sprintf("%s.%s", database, renamedTable)
	renameTableDDL := connector.Record{
		Table:     table,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("RENAME TABLE %s TO %s", fullTable, renamedFull),
	}
	if err := dest.ApplyDDL(ctx, schema, renameTableDDL); err != nil {
		t.Fatalf("apply rename table ddl: %v", err)
	}
	if err := dest.Close(ctx); err != nil {
		t.Fatalf("close destination after rename: %v", err)
	}
	dest = &clickhouse.Destination{}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("reopen destination after rename: %v", err)
	}
	defer dest.Close(ctx)
	fullTable = renamedFull
	schema.Name = renamedTable
	insert = connector.Record{
		Table:     renamedTable,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": uint64(15)}),
		After: map[string]any{
			"id":           uint64(15),
			"display_name": "renamed_table",
		},
	}
	batch = connector.Batch{Records: []connector.Record{insert}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "6"}, WritePolicy: appendPolicy}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("insert after rename table ddl: %v", err)
	}
	var renamedTableVal string
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT display_name FROM %s WHERE id = 15", fullTable)).Scan(&renamedTableVal); err != nil {
		t.Fatalf("select after rename table ddl: %v", err)
	}
	if renamedTableVal != "renamed_table" {
		t.Fatalf("unexpected value after rename table ddl: %s", renamedTableVal)
	}
}
