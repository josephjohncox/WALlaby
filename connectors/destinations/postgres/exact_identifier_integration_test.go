package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/flow"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/internal/tablemap"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestPostgresDestinationCarriesDottedQuotedIdentifiersAndRecoversCopyFallback(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schemaName, tableName := `exact.schema"quoted`, `events.table"quoted`
	qualified := pgx.Identifier{schemaName, tableName}.Sanitize()
	copyTriggerFunction := pgx.Identifier{schemaName, "reject_copy_statement"}.Sanitize()
	rejectInsertFunction := pgx.Identifier{schemaName, "reject_fallback_insert"}.Sanitize()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
DROP SCHEMA IF EXISTS %s CASCADE;
CREATE SCHEMA %s;
CREATE TABLE %s (id bigint PRIMARY KEY,payload text);
CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  IF current_setting('wallaby.copy_from_active', true) = 'on' THEN
    RAISE EXCEPTION 'copy statement rejected for fallback test';
  END IF;
  RETURN NEW;
END
$function$;
CREATE TRIGGER a_reject_copy BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s()`,
		pgx.Identifier{schemaName}.Sanitize(), pgx.Identifier{schemaName}.Sanitize(), qualified, copyTriggerFunction, qualified, copyTriggerFunction)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, pgx.Identifier{schemaName}.Sanitize()))
	}()
	destination := &Destination{}
	if err := destination.Open(ctx, connector.RuntimeSpec{Name: "exact-postgres", Type: connector.EndpointPostgres, Options: map[string]string{"dsn": dsn, "batch_mode": "target", "meta_table_enabled": "false"}}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(context.Background())
	destinationSchema := connector.Schema{Namespace: schemaName, Name: tableName, Columns: []connector.Column{{Name: "id", Type: "bigint"}, {Name: "payload", Type: "text"}}}
	rawTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rawTx.Exec(ctx, "SET LOCAL wallaby.copy_from_active = 'on'"); err != nil {
		t.Fatal(err)
	}
	if _, err := rawTx.CopyFrom(ctx, pgx.Identifier{schemaName, tableName}, []string{"id", "payload"}, pgx.CopyFromRows([][]any{{int64(0), "raw-copy"}})); err == nil || !strings.Contains(err.Error(), "copy statement rejected") {
		_ = rawTx.Rollback(ctx)
		t.Fatalf("COPY trigger did not produce a real transaction-aborting COPY failure: %v", err)
	}
	if err := rawTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// Destination.Write must roll the real server-side COPY error back to its
	// savepoint before issuing the ordinary INSERT fallback.
	if err := destination.Write(ctx, connector.Batch{Schema: destinationSchema, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}, Records: []connector.Record{{Table: tableName, Operation: connector.OpInsert, After: map[string]any{"id": "1", "payload": "fallback"}}}}); err != nil {
		t.Fatalf("copy fallback on exact target: %v", err)
	}
	var fallbackRows int
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE id=1 AND payload='fallback'`, qualified)).Scan(&fallbackRows); err != nil || fallbackRows != 1 {
		t.Fatalf("savepoint fallback row count=%d err=%v", fallbackRows, err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP TRIGGER a_reject_copy ON %s`, qualified)); err != nil {
		t.Fatal(err)
	}
	if err := destination.Write(ctx, connector.Batch{Schema: destinationSchema, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}, Records: []connector.Record{{Table: tableName, Operation: connector.OpInsert, After: map[string]any{"id": int64(2), "payload": "copy"}}}}); err != nil {
		t.Fatalf("CopyFrom on exact target: %v", err)
	}
	upsertPolicy := connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}}
	if err := destination.Write(ctx, connector.Batch{Schema: destinationSchema, WritePolicy: upsertPolicy, Records: []connector.Record{{Table: tableName, Operation: connector.OpUpdate, Key: []byte(`{"id":2}`), After: map[string]any{"id": int64(2), "payload": "upsert"}}}}); err != nil {
		t.Fatalf("upsert on exact target: %v", err)
	}
	var first, second string
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT min(payload),max(payload) FROM %s`, qualified)).Scan(&first, &second); err != nil {
		t.Fatal(err)
	}
	if first != "fallback" || second != "upsert" {
		t.Fatalf("exact target rows=%q/%q", first, second)
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(`
CREATE TRIGGER a_reject_copy BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s();
CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $function$
BEGIN
  IF NEW.payload = 'reject-all' THEN RAISE EXCEPTION 'ordinary insert rejected'; END IF;
  RETURN NEW;
END
$function$;
CREATE TRIGGER b_reject_insert BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s()`, qualified, copyTriggerFunction, rejectInsertFunction, qualified, rejectInsertFunction)); err != nil {
		t.Fatal(err)
	}
	fallbackFailure := destination.Write(ctx, connector.Batch{Schema: destinationSchema, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}, Records: []connector.Record{{Table: tableName, Operation: connector.OpInsert, After: map[string]any{"id": int64(3), "payload": "reject-all"}}}})
	if fallbackFailure == nil || !strings.Contains(fallbackFailure.Error(), "ordinary insert rejected") {
		t.Fatalf("fallback INSERT failure=%v, want trigger rejection", fallbackFailure)
	}
	var rejectedRows, totalRows int
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FILTER (WHERE id=3),count(*) FROM %s`, qualified)).Scan(&rejectedRows, &totalRows); err != nil {
		t.Fatal(err)
	}
	if rejectedRows != 0 || totalRows != 2 {
		t.Fatalf("failed fallback transaction persisted rows: rejected/total=%d/%d", rejectedRows, totalRows)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP TRIGGER a_reject_copy ON %s; DROP TRIGGER b_reject_insert ON %s`, qualified, qualified)); err != nil {
		t.Fatal(err)
	}

	mappings := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{
		Destination: "exact-postgres", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude},
		Tables: []flow.TableMapping{{SourceSchema: "source.schema", SourceTable: "source.table", Action: flow.MappingActionInclude, TargetSchema: schemaName, TargetTable: tableName,
			FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionExclude},
			Columns:       []flow.ColumnMapping{{SourceColumn: "id", Action: flow.MappingActionInclude, TargetColumn: "id"}, {SourceColumn: "payload", Action: flow.MappingActionInclude, TargetColumn: "payload"}, {SourceColumn: "renamed", Action: flow.MappingActionInclude, TargetColumn: " "}},
			Write:         flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}}}},
	}}}
	projector, err := tablemap.New(mappings, "exact-postgres")
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := json.Marshal(internalschema.Plan{Changes: []internalschema.Change{{Type: internalschema.ChangeRenameColumn, Namespace: "source.schema", Table: "source.table", Column: "payload", ToColumn: "renamed"}}})
	projected, decision, err := projector.ProjectBatch(connector.Batch{Schema: connector.Schema{Namespace: "source.schema", Name: "source.table", Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"replica_identity": "true"}}, {Name: "payload", Type: "text"}}}, Records: []connector.Record{{Table: "source.table", Operation: connector.OpDDL, DDLPlan: plan, SourcePosition: "0/10"}}, Checkpoint: connector.Checkpoint{LSN: "0/10"}})
	if err != nil || decision != stream.ProjectionIncluded {
		t.Fatalf("project exact rename: decision=%v err=%v", decision, err)
	}
	if err := destination.ApplyDDL(ctx, projected.Schema, projected.Records[0]); err != nil {
		t.Fatalf("apply projected exact rename: %v", err)
	}
	var renamed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 AND column_name=' '`, schemaName, tableName).Scan(&renamed); err != nil {
		t.Fatal(err)
	}
	if renamed != 1 {
		t.Fatalf("projector-to-PostgreSQL whitespace rename columns=%d", renamed)
	}
}

func TestPostgresDestinationAlwaysQuotesWhitespaceAndCaseDistinctIdentifiers(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	blankSchema := pgx.Identifier{"  "}.Sanitize()
	blankTable := pgx.Identifier{" "}.Sanitize()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
DROP SCHEMA IF EXISTS %s CASCADE;
CREATE SCHEMA %s;
CREATE TABLE %s.%s ("ID" bigint,"id" text," id " boolean)`, blankSchema, blankSchema, blankSchema, blankTable)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, blankSchema))
	}()

	destination := &Destination{}
	if err := destination.Open(ctx, connector.RuntimeSpec{Name: "exact-postgres", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "batch_mode": "target", "meta_table_enabled": "false",
	}}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(context.Background())
	schema := connector.Schema{Namespace: "  ", Name: " ", Columns: []connector.Column{
		{Name: "ID", Type: "bigint"},
		{Name: "id", Type: "text"},
		{Name: " id ", Type: "boolean"},
	}}
	batch := connector.Batch{Schema: schema, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}, Records: []connector.Record{{
		Table: " ", Operation: connector.OpInsert,
		After: map[string]any{"ID": int64(7), "id": "lower", " id ": true},
	}}}
	if err := destination.Write(ctx, batch); err != nil {
		t.Fatal(err)
	}
	var upper int64
	var lower string
	var spaced bool
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT "ID","id"," id " FROM %s.%s`, blankSchema, blankTable)).Scan(&upper, &lower, &spaced); err != nil {
		t.Fatal(err)
	}
	if upper != 7 || lower != "lower" || !spaced {
		t.Fatalf("exact identifier row=%d/%q/%t", upper, lower, spaced)
	}

	plan := internalschema.Plan{Changes: []internalschema.Change{
		{Type: internalschema.ChangeAddColumn, Namespace: "  ", Table: " ", Column: " added ", ToType: "text", Nullable: true},
		{Type: internalschema.ChangeRenameColumn, Namespace: "  ", Table: " ", Column: "ID", ToColumn: " ID "},
	}}
	encodedPlan, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.ApplyDDL(ctx, schema, connector.Record{Table: " ", Operation: connector.OpDDL, DDLPlan: encodedPlan}); err != nil {
		t.Fatal(err)
	}
	var exactColumns int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM information_schema.columns
WHERE table_schema='  ' AND table_name=' ' AND column_name=ANY($1::text[])`, []string{" ID ", "id", " id ", " added "}).Scan(&exactColumns); err != nil {
		t.Fatal(err)
	}
	if exactColumns != 4 {
		t.Fatalf("quoted exact columns after DDL=%d, want 4", exactColumns)
	}
}
