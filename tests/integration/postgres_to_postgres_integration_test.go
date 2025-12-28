package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresToPostgresE2E(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	var walLevel string
	if err := pool.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		t.Fatalf("read wal_level: %v", err)
	}
	if walLevel != "logical" {
		t.Skipf("wal_level must be logical (got %s)", walLevel)
	}

	suffix := fmt.Sprintf("%d", rand.New(rand.NewSource(time.Now().UnixNano())).Int63())
	srcSchema := "e2e_src_" + suffix
	dstSchema := "e2e_dst_" + suffix
	table := "events"
	pub := "wallaby_e2e_" + suffix
	slot := "wallaby_e2e_" + suffix

	cleanup := func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pub))
		_, _ = pool.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", slot)
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", srcSchema))
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", dstSchema))
	}
	defer cleanup()

	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", srcSchema)); err != nil {
		t.Fatalf("create source schema: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", dstSchema)); err != nil {
		t.Fatalf("create dest schema: %v", err)
	}

	createTable := fmt.Sprintf(`CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  payload JSONB,
  tags JSONB,
  updated_at TIMESTAMPTZ
)`, srcSchema, table)
	if _, err := pool.Exec(ctx, createTable); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	createDest := fmt.Sprintf(`CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  payload JSONB,
  tags JSONB,
  updated_at TIMESTAMPTZ
)`, dstSchema, table)
	if _, err := pool.Exec(ctx, createDest); err != nil {
		t.Fatalf("create dest table: %v", err)
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s.%s", pub, srcSchema, table)); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	var startLSN string
	if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&startLSN); err != nil {
		t.Fatalf("read start LSN: %v", err)
	}

	sourceSpec := connector.Spec{
		Name: "e2e-source",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                 dsn,
			"slot":                slot,
			"publication":         pub,
			"publication_tables":  fmt.Sprintf("%s.%s", srcSchema, table),
			"ensure_publication":  "true",
			"sync_publication":    "true",
			"batch_size":          "500",
			"batch_timeout":       "200ms",
			"emit_empty":          "true",
			"resolve_types":       "true",
			"start_lsn":           startLSN,
		},
	}

	destSpec := connector.Spec{
		Name: "e2e-dest",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                dsn,
			"schema":             dstSchema,
			"meta_table_enabled": "false",
			"synchronous_commit": "off",
		},
	}

	runner := &stream.Runner{
		Source:      &pgsource.Source{},
		SourceSpec:  sourceSpec,
		Destinations: []stream.DestinationConfig{{Spec: destSpec, Dest: &pgdest.Destination{}}},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	time.Sleep(1 * time.Second)

	now := time.Now().UTC()
	insertSQL := fmt.Sprintf(`INSERT INTO %s.%s (id, payload, tags, updated_at) VALUES ($1, $2::jsonb, $3::jsonb, $4)`, srcSchema, table)
	if _, err := pool.Exec(ctx, insertSQL, 1, `{"status":"new"}`, `["a","b"]`, now); err != nil {
		t.Fatalf("insert row1: %v", err)
	}
	if _, err := pool.Exec(ctx, insertSQL, 2, `{"status":"old"}`, `["x","y"]`, now); err != nil {
		t.Fatalf("insert row2: %v", err)
	}

	updateSQL := fmt.Sprintf(`UPDATE %s.%s SET payload = $2::jsonb, tags = $3::jsonb, updated_at = $4 WHERE id = $1`, srcSchema, table)
	if _, err := pool.Exec(ctx, updateSQL, 1, `{"status":"updated"}`, `["c"]`, now.Add(1*time.Second)); err != nil {
		t.Fatalf("update row1: %v", err)
	}

	deleteSQL := fmt.Sprintf(`DELETE FROM %s.%s WHERE id = $1`, srcSchema, table)
	if _, err := pool.Exec(ctx, deleteSQL, 2); err != nil {
		t.Fatalf("delete row2: %v", err)
	}

	waitFor(t, 30*time.Second, 200*time.Millisecond, func() (bool, error) {
		var count int
		query := fmt.Sprintf("SELECT count(*) FROM %s.%s", dstSchema, table)
		if err := pool.QueryRow(ctx, query).Scan(&count); err != nil {
			return false, err
		}
		return count == 1, nil
	})

	var payloadRaw, tagsRaw []byte
	rowQuery := fmt.Sprintf("SELECT payload::text, tags::text FROM %s.%s WHERE id = $1", dstSchema, table)
	if err := pool.QueryRow(ctx, rowQuery, 1).Scan(&payloadRaw, &tagsRaw); err != nil {
		t.Fatalf("read dest row: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["status"] != "updated" {
		t.Fatalf("expected updated payload, got %v", payload)
	}

	var tags []any
	if err := json.Unmarshal(tagsRaw, &tags); err != nil {
		t.Fatalf("decode tags: %v", err)
	}
	if len(tags) != 1 || tags[0] != "c" {
		t.Fatalf("expected tags [c], got %v", tags)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("runner error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("runner did not stop after cancel")
	}
}

func waitFor(t *testing.T, timeout, interval time.Duration, fn func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, err := fn()
		if err != nil {
			t.Fatalf("wait check failed: %v", err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for condition")
		}
		time.Sleep(interval)
	}
}
