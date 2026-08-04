package postgres

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestInspectCatalogRequiresExplicitScope(t *testing.T) {
	if _, err := InspectCatalog(context.Background(), "postgres://unused", nil, CatalogScope{}); err == nil || !strings.Contains(err.Error(), "explicit table, schema, or publication scope") {
		t.Fatalf("error=%v", err)
	}
}
func TestPublicationTablesQueryCompatibility(t *testing.T) {
	pg14 := publicationTablesQuery(140000)
	if strings.Contains(pg14, "attrs") || strings.Contains(pg14, "pg_publication_namespace") || !strings.Contains(pg14, "FROM pg_catalog.pg_get_publication_tables") {
		t.Fatalf("PG14 query is not compatible: %s", pg14)
	}
	pg15 := publicationTablesQuery(150000)
	if !strings.Contains(pg15, "attrs") || strings.Contains(pg15, "pg_publication_namespace") {
		t.Fatalf("PG15 query lacks effective column metadata: %s", pg15)
	}
}

func TestParseCatalogTableNameQuotedIdentifiers(t *testing.T) {
	got, err := ParseCatalogTableName(`"Odd Schema"."Table""Name"`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Schema != "Odd Schema" || got.Table != `Table"Name` {
		t.Fatalf("got=%+v", got)
	}
	plain, err := ParseCatalogTableName("PUBLIC.Events")
	if err != nil {
		t.Fatal(err)
	}
	if plain.Schema != "public" || plain.Table != "events" {
		t.Fatalf("plain=%+v", plain)
	}
	spaced, err := ParseCatalogTableName(`"  Odd Schema  "." Table Name "`)
	if err != nil {
		t.Fatal(err)
	}
	if spaced.Schema != "  Odd Schema  " || spaced.Table != " Table Name " {
		t.Fatalf("quoted whitespace lost: %+v", spaced)
	}
	for _, invalid := range []string{"public.bad name", "public.events trailing", "public.events;drop", "public..events", "public.events.extra"} {
		if _, err := ParseCatalogTableName(invalid); err == nil {
			t.Fatalf("invalid selector %q accepted", invalid)
		}
	}
}
func TestInspectCatalogLivePG14PublicationQuery(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := newPool(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var version int
	if err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("server_version_num=%d", version)
	if version < 140000 || version >= 150000 {
		t.Skipf("requires PostgreSQL 14, server_version_num=%d", version)
	}
	_, _ = pool.Exec(ctx, `DROP PUBLICATION IF EXISTS mapping_pg14_query; DROP SCHEMA IF EXISTS mapping_pg14_query CASCADE`)
	if _, err := pool.Exec(ctx, `CREATE SCHEMA mapping_pg14_query; CREATE TABLE mapping_pg14_query.events(id bigint PRIMARY KEY,payload text); CREATE PUBLICATION mapping_pg14_query FOR TABLE mapping_pg14_query.events`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP PUBLICATION IF EXISTS mapping_pg14_query; DROP SCHEMA IF EXISTS mapping_pg14_query CASCADE`)
	}()
	tables, err := InspectCatalog(ctx, dsn, nil, CatalogScope{Publication: "mapping_pg14_query"})
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 || tables[0].Schema != "mapping_pg14_query" || tables[0].Table != "events" || len(tables[0].Columns) != 2 {
		t.Fatalf("PG14 effective publication=%+v", tables)
	}
}

func TestInspectCatalogLiveEffectivePublicationMembership(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := newPool(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var version int
	if err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("server_version_num=%d", version)
	if version < 150000 {
		t.Skip("schema publications require PostgreSQL 15+")
	}
	_, _ = pool.Exec(ctx, `DROP PUBLICATION IF EXISTS mapping_effective_root; DROP PUBLICATION IF EXISTS mapping_effective_schema; DROP PUBLICATION IF EXISTS mapping_effective_all; DROP SCHEMA IF EXISTS mapping_effective CASCADE`)
	if _, err := pool.Exec(ctx, `CREATE SCHEMA mapping_effective; CREATE TABLE mapping_effective.parent(id int) PARTITION BY RANGE(id); CREATE TABLE mapping_effective.child PARTITION OF mapping_effective.parent FOR VALUES FROM (0) TO (100); CREATE TABLE mapping_effective.plain(id int); CREATE PUBLICATION mapping_effective_root FOR TABLE mapping_effective.parent WITH (publish_via_partition_root=true); CREATE PUBLICATION mapping_effective_schema FOR TABLES IN SCHEMA mapping_effective; CREATE PUBLICATION mapping_effective_all FOR ALL TABLES`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP PUBLICATION IF EXISTS mapping_effective_root; DROP PUBLICATION IF EXISTS mapping_effective_schema; DROP PUBLICATION IF EXISTS mapping_effective_all; DROP SCHEMA IF EXISTS mapping_effective CASCADE`)
	}()
	root, err := InspectCatalog(ctx, dsn, nil, CatalogScope{Publication: "mapping_effective_root"})
	if err != nil {
		t.Fatal(err)
	}
	if len(root) != 1 || root[0].Table != "parent" {
		t.Fatalf("publish_via_partition_root result=%+v", root)
	}
	schemaTables, err := InspectCatalog(ctx, dsn, nil, CatalogScope{Publication: "mapping_effective_schema"})
	if err != nil {
		t.Fatal(err)
	}
	if !catalogContains(schemaTables, "mapping_effective", "plain") {
		t.Fatalf("schema publication omitted plain table: %+v", schemaTables)
	}
	allTables, err := InspectCatalog(ctx, dsn, nil, CatalogScope{Publication: "mapping_effective_all"})
	if err != nil {
		t.Fatal(err)
	}
	if !catalogContains(allTables, "mapping_effective", "plain") {
		t.Fatalf("FOR ALL TABLES omitted publishable table")
	}
}
func catalogContains(tables []CatalogTable, schema, table string) bool {
	for _, candidate := range tables {
		if candidate.Schema == schema && candidate.Table == table {
			return true
		}
	}
	return false
}

func TestInspectCatalogLiveExactMetadata(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := newPool(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, _ = pool.Exec(ctx, `DROP PUBLICATION IF EXISTS "Mapping Pub"; DROP PUBLICATION IF EXISTS " Mapping Columns "; DROP SCHEMA IF EXISTS "Mapping Gen" CASCADE`)
	if _, err := pool.Exec(ctx, `CREATE SCHEMA "Mapping Gen"`); err != nil {
		t.Fatal(err)
	}
	var hadHstore bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='hstore')`).Scan(&hadHstore); err != nil {
		t.Fatal(err)
	}
	if !hadHstore {
		if _, err := pool.Exec(ctx, `CREATE EXTENSION hstore WITH SCHEMA "Mapping Gen"`); err != nil {
			t.Fatal(err)
		}
	}
	var hstoreSchema string
	if err := pool.QueryRow(ctx, `SELECT n.nspname FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE t.typname='hstore'`).Scan(&hstoreSchema); err != nil {
		t.Fatal(err)
	}
	var serverVersion int
	if err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&serverVersion); err != nil {
		t.Fatal(err)
	}
	hstoreType := pgx.Identifier{hstoreSchema, "hstore"}.Sanitize()
	ddl := fmt.Sprintf(`CREATE DOMAIN "Mapping Gen".inner_not_null AS text NOT NULL; CREATE DOMAIN "Mapping Gen".outer_not_null AS "Mapping Gen".inner_not_null; CREATE DOMAIN "Mapping Gen".extension_domain AS %s[]; CREATE TABLE "Mapping Gen"."Odd Table"("Second Key" text NOT NULL,"First Key" integer DEFAULT 7,"generated value" text GENERATED ALWAYS AS ("Second Key" || "First Key"::text) STORED,"identity value" bigint GENERATED BY DEFAULT AS IDENTITY,"domain value" "Mapping Gen".outer_not_null,"extension value" "Mapping Gen".extension_domain,PRIMARY KEY("Second Key","First Key")); CREATE PUBLICATION "Mapping Pub" FOR TABLE "Mapping Gen"."Odd Table"`, hstoreType)
	if serverVersion >= 150000 {
		ddl += `; CREATE PUBLICATION " Mapping Columns " FOR TABLE "Mapping Gen"."Odd Table" ("Second Key","First Key")`
	}
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP PUBLICATION IF EXISTS "Mapping Pub"; DROP PUBLICATION IF EXISTS " Mapping Columns "`)
		if !hadHstore {
			_, _ = pool.Exec(context.Background(), `DROP EXTENSION IF EXISTS hstore CASCADE`)
		}
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS "Mapping Gen" CASCADE`)
	}()
	tables, err := InspectCatalog(ctx, dsn, nil, CatalogScope{TableSelectors: []string{` "Mapping Gen" . "Odd Table" `}})
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("tables=%+v", tables)
	}
	bySchema, err := InspectCatalog(ctx, dsn, nil, CatalogScope{SchemaSelectors: []string{`"Mapping Gen"`}})
	if err != nil {
		t.Fatal(err)
	}
	if len(bySchema) != 1 || bySchema[0].Table != "Odd Table" {
		t.Fatalf("schema selector=%+v", bySchema)
	}
	if _, err := InspectCatalog(ctx, dsn, nil, CatalogScope{TableSelectors: []string{`"Mapping Gen"."Odd Table" trailing`}}); err == nil {
		t.Fatal("server parser accepted trailing selector tokens")
	}
	table := tables[0]
	if table.RelationOID == 0 || table.ReplicaIdentity != "d" || !reflect.DeepEqual(table.PrimaryKeyColumns, []string{"Second Key", "First Key"}) || !reflect.DeepEqual(table.ReplicaIdentityColumns, table.PrimaryKeyColumns) {
		t.Fatalf("table=%+v", table)
	}
	if len(table.Columns) != 6 || table.Columns[0].Attnum != 1 || table.Columns[1].Attnum != 2 || table.Columns[2].GeneratedKind != "s" || table.Columns[2].GenerationExpression == "" || table.Columns[2].HasDefault || table.Columns[2].DefaultExpression != "" || table.Columns[3].IdentityKind != "d" || !table.Columns[1].HasDefault || table.Columns[1].DefaultExpression == "" || table.Columns[0].FormattedType != "text" || table.Columns[0].Nullable || table.Columns[4].Nullable || table.Columns[5].Extension != "hstore" {
		t.Fatalf("columns=%+v", table.Columns)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE "Mapping Gen"."Odd Table" ADD COLUMN "future value" text`); err != nil {
		t.Fatal(err)
	}
	published, err := InspectCatalog(ctx, dsn, nil, CatalogScope{Publication: "Mapping Pub"})
	if err != nil {
		t.Fatal(err)
	}
	wantPublishedColumns := len(table.Columns) + 1
	if serverVersion >= 150000 {
		wantPublishedColumns = len(table.Columns)
	}
	if len(published) != 1 || published[0].RelationOID != table.RelationOID || len(published[0].Columns) != wantPublishedColumns {
		t.Fatalf("publication tables=%+v", published)
	}
	hasGenerated := false
	for _, column := range published[0].Columns {
		hasGenerated = hasGenerated || column.Name == "generated value"
	}
	if hasGenerated != (serverVersion < 150000) || published[0].Columns[len(published[0].Columns)-1].Name != "future value" {
		t.Fatalf("effective publishability=%+v", published[0].Columns)
	}
	if serverVersion >= 150000 {
		columnsOnly, err := InspectCatalog(ctx, dsn, nil, CatalogScope{Publication: " Mapping Columns "})
		if err != nil {
			t.Fatal(err)
		}
		if len(columnsOnly) != 1 || len(columnsOnly[0].Columns) != 2 || columnsOnly[0].Columns[0].Name != "Second Key" || columnsOnly[0].Columns[1].Name != "First Key" {
			t.Fatalf("publication column list=%+v", columnsOnly)
		}
	}
}
