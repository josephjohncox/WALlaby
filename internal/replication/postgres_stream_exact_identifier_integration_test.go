package replication

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresStreamExactIdentifierBaselinesEmitDDLOnlyForChangedRelation(t *testing.T) {
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

	const (
		sourceSchema = "wallaby_exact_stream"
		publication  = "wallaby_exact_stream_publication"
		slot         = "wallaby_exact_stream_slot"
	)
	quotedSchema := pgx.Identifier{sourceSchema}.Sanitize()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
DROP PUBLICATION IF EXISTS %s;
DROP SCHEMA IF EXISTS %s CASCADE;
CREATE SCHEMA %s;
CREATE TABLE %s."Events" (id bigint PRIMARY KEY);
CREATE TABLE %s.events (id bigint PRIMARY KEY);
CREATE TABLE %s." " (id bigint PRIMARY KEY);
CREATE PUBLICATION %s FOR TABLE %s."Events",%s.events,%s." "`,
		pgx.Identifier{publication}.Sanitize(), quotedSchema, quotedSchema, quotedSchema, quotedSchema, quotedSchema,
		pgx.Identifier{publication}.Sanitize(), quotedSchema, quotedSchema, quotedSchema)); err != nil {
		t.Fatal(err)
	}
	baselines := []connector.Schema{
		{Namespace: sourceSchema, Name: "Events", Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: true}}},
		{Namespace: sourceSchema, Name: "events", Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: true}}},
		{Namespace: sourceSchema, Name: " ", Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: true}}},
	}
	stream := NewPostgresStream(dsn, WithSchemaBaselines(baselines), WithStatusInterval(20*time.Millisecond), WithEmitPlanDDL(true))
	changes, err := stream.Start(ctx, slot, publication)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = stream.Stop(context.Background())
		_, _ = pool.Exec(context.Background(), `SELECT pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name=$1`, slot)
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP PUBLICATION IF EXISTS %s; DROP SCHEMA IF EXISTS %s CASCADE`, pgx.Identifier{publication}.Sanitize(), quotedSchema))
	}()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
ALTER TABLE %s."Events" ADD COLUMN note text;
BEGIN;
INSERT INTO %s."Events" (id,note) VALUES (1,'changed');
INSERT INTO %s.events (id) VALUES (2);
INSERT INTO %s." " (id) VALUES (3);
COMMIT`, quotedSchema, quotedSchema, quotedSchema, quotedSchema)); err != nil {
		t.Fatal(err)
	}

	ddlTables := make([]string, 0, 1)
	dataTables := make(map[string]bool, 3)
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for len(dataTables) < 3 {
		select {
		case change, ok := <-changes:
			if !ok {
				t.Fatal("logical replication stream closed before exact-identifier transaction")
			}
			if change.Operation == string(connector.OpDDL) || change.DDL != "" {
				ddlTables = append(ddlTables, change.Table)
			}
			if change.Record != nil && change.Record.Operation != connector.OpDDL {
				dataTables[change.Table] = true
			}
		case <-deadline.C:
			t.Fatalf("exact-identifier stream timed out: DDL=%v data=%v", ddlTables, dataTables)
		}
	}
	if len(ddlTables) != 1 || ddlTables[0] != "Events" {
		t.Fatalf("exact-identifier DDL tables=%v, want Events only", ddlTables)
	}
	for _, name := range []string{"Events", "events", " "} {
		if !dataTables[name] {
			t.Fatalf("exact data relation %q missing from stream: %v", name, dataTables)
		}
	}
}
