package postgres

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestSnowflakeManagedProfilePostgresSourceCatalog(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required for live managed Snowflake source-catalog evidence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := newPool(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var version int
	if err := pool.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version/10000 != 16 {
		t.Fatalf("managed Snowflake source-catalog gate requires PostgreSQL 16, got %d", version)
	}

	suffix := strings.ToLower(strconv.FormatInt(time.Now().UnixNano(), 36))
	schemaName := "wallaby_sf_" + suffix
	tableName := "widgets"
	publication := "wallaby_sf_pub_" + suffix
	qualified := pgx.Identifier{schemaName, tableName}.Sanitize()
	publicationID := pgx.Identifier{publication}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	publicationCreated := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if publicationCreated {
			_, _ = pool.Exec(cleanupCtx, "DROP PUBLICATION "+publicationID)
		}
		_, _ = pool.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schemaName}.Sanitize()+" CASCADE")
	})
	if _, err := pool.Exec(ctx, "CREATE TABLE "+qualified+" (id bigint PRIMARY KEY, value text, payload bytea, event_at timestamptz)"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')", publicationID, qualified)); err != nil {
		t.Fatal(err)
	}
	publicationCreated = true

	tables, schemas, err := loadManagedSnowflakePublicationContract(ctx, pool, publication)
	if err != nil {
		t.Fatal(err)
	}
	wantRelation := qualified
	if len(tables) != 1 || tables[0] != wantRelation || len(schemas) != 1 {
		t.Fatalf("publication tables/schemas=%v/%d, want [%s]/1", tables, len(schemas), wantRelation)
	}
	schema := schemas[0]
	if schema.Namespace != schemaName || schema.Name != tableName || len(schema.Columns) != 4 {
		t.Fatalf("live source schema=%+v", schema)
	}
	for _, column := range schema.Columns {
		if column.TypeMetadata["nullability_known"] != "true" || column.TypeMetadata["generated_known"] != "true" {
			t.Fatalf("column %s lacks exact catalog metadata: %v", column.Name, column.TypeMetadata)
		}
	}
	if schema.Columns[0].Type != "bigint" || schema.Columns[0].TypeMetadata["primary_key"] != "true" || schema.Columns[0].TypeMetadata["primary_key_ordinal"] != "1" || schema.Columns[0].TypeMetadata["replica_identity"] != "true" {
		t.Fatalf("live source primary key=%+v", schema.Columns[0])
	}
	if _, err := pool.Exec(ctx, "ALTER PUBLICATION "+publicationID+" SET (publish = 'insert, update, delete, truncate')"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadManagedSnowflakePublicationContract(ctx, pool, publication); err == nil || !strings.Contains(err.Error(), "rejects truncate") {
		t.Fatalf("truncate publication error=%v", err)
	}
	if _, err := pool.Exec(ctx, "ALTER PUBLICATION "+publicationID+" SET (publish = 'insert, update, delete')"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ALTER PUBLICATION "+publicationID+" SET TABLE "+qualified+" WHERE (id > 0)"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadManagedSnowflakePublicationContract(ctx, pool, publication); err == nil || !strings.Contains(err.Error(), "row filters") {
		t.Fatalf("filtered publication error=%v", err)
	}
	if _, err := pool.Exec(ctx, "ALTER PUBLICATION "+publicationID+" SET TABLE "+qualified+" (id, value)"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadManagedSnowflakePublicationContract(ctx, pool, publication); err == nil || !strings.Contains(err.Error(), "column lists") {
		t.Fatalf("column-list publication error=%v", err)
	}
	if _, err := pool.Exec(ctx, "ALTER PUBLICATION "+publicationID+" SET TABLE "+qualified); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE "+qualified+" REPLICA IDENTITY FULL"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadManagedSnowflakePublicationContract(ctx, pool, publication); err == nil || !strings.Contains(err.Error(), "default primary-key replica identity") {
		t.Fatalf("full replica-identity error=%v", err)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE "+qualified+" REPLICA IDENTITY DEFAULT"); err != nil {
		t.Fatal(err)
	}
	deferrableTable := pgx.Identifier{schemaName, "deferrable_widgets"}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE TABLE "+deferrableTable+" (id bigint PRIMARY KEY DEFERRABLE, value text)"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ALTER PUBLICATION "+publicationID+" SET TABLE "+deferrableTable); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadManagedSnowflakePublicationContract(ctx, pool, publication); err == nil || !strings.Contains(err.Error(), "immediate, valid, ready primary key") {
		t.Fatalf("deferrable primary-key error=%v", err)
	}
	partitionedTable := pgx.Identifier{schemaName, "partitioned_widgets"}.Sanitize()
	if _, err := pool.Exec(ctx, "CREATE TABLE "+partitionedTable+" (id bigint PRIMARY KEY, value text) PARTITION BY RANGE (id)"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ALTER PUBLICATION "+publicationID+" SET TABLE "+partitionedTable); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadManagedSnowflakePublicationContract(ctx, pool, publication); err == nil || !strings.Contains(err.Error(), "partitioned") {
		t.Fatalf("partitioned relation error=%v", err)
	}
	t.Logf("validated PostgreSQL server_version_num=%d publication=%s schema=%s", version, publication, wantRelation)
}
