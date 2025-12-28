package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestPostgresToPostgresE2E(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	var walLevel string
	if err := adminPool.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		t.Fatalf("read wal_level: %v", err)
	}
	if walLevel != "logical" {
		t.Skipf("wal_level must be logical (got %s)", walLevel)
	}

	suffix := fmt.Sprintf("%d", rand.New(rand.NewSource(time.Now().UnixNano())).Int63())
	srcDB := "wallaby_src_" + suffix
	dstDB := "wallaby_dst_" + suffix
	schemaName := "e2e_" + suffix
	table := "events"
	pub := "wallaby_e2e_" + suffix
	slot := "wallaby_e2e_" + suffix

	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", srcDB)); err != nil {
		t.Fatalf("create source database: %v", err)
	}
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dstDB)); err != nil {
		t.Fatalf("create dest database: %v", err)
	}

	srcDSN, err := dsnWithDatabase(baseDSN, srcDB)
	if err != nil {
		t.Fatalf("build source dsn: %v", err)
	}
	dstDSN, err := dsnWithDatabase(baseDSN, dstDB)
	if err != nil {
		t.Fatalf("build dest dsn: %v", err)
	}

	srcPool, err := pgxpool.New(ctx, srcDSN)
	if err != nil {
		t.Fatalf("connect source db: %v", err)
	}
	defer srcPool.Close()

	dstPool, err := pgxpool.New(ctx, dstDSN)
	if err != nil {
		t.Fatalf("connect dest db: %v", err)
	}
	defer dstPool.Close()

	cleanup := func() {
		cancel()
		_, _ = srcPool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pub))
		_, _ = srcPool.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", slot)
		srcPool.Close()
		dstPool.Close()
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", srcDB))
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dstDB))
	}
	defer cleanup()

	if _, err := srcPool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schemaName)); err != nil {
		t.Fatalf("create source schema: %v", err)
	}
	if _, err := dstPool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schemaName)); err != nil {
		t.Fatalf("create dest schema: %v", err)
	}

	createTable := fmt.Sprintf(`CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  payload JSONB,
  tags JSONB,
  amount NUMERIC(10,2),
  uid UUID,
  updated_at TIMESTAMPTZ
)`, schemaName, table)
	if _, err := srcPool.Exec(ctx, createTable); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	createDest := fmt.Sprintf(`CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  payload JSONB,
  tags JSONB,
  amount NUMERIC(10,2),
  uid UUID,
  updated_at TIMESTAMPTZ
)`, schemaName, table)
	if _, err := dstPool.Exec(ctx, createDest); err != nil {
		t.Fatalf("create dest table: %v", err)
	}

	var startLSN string
	if err := srcPool.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&startLSN); err != nil {
		t.Fatalf("read start LSN: %v", err)
	}

	sourceSpec := connector.Spec{
		Name: "e2e-source",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                srcDSN,
			"slot":               slot,
			"publication":        pub,
			"publication_tables": fmt.Sprintf("%s.%s", schemaName, table),
			"ensure_publication": "true",
			"sync_publication":   "true",
			"batch_size":         "500",
			"batch_timeout":      "200ms",
			"emit_empty":         "true",
			"resolve_types":      "true",
			"start_lsn":          startLSN,
			"capture_ddl":        "true",
		},
	}

	destSpec := connector.Spec{
		Name: "e2e-dest",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                dstDSN,
			"schema":             schemaName,
			"meta_table_enabled": "true",
			"flow_id":            "e2e-flow",
			"synchronous_commit": "off",
		},
	}

	runner := &stream.Runner{
		Source:       &pgsource.Source{},
		SourceSpec:   sourceSpec,
		Destinations: []stream.DestinationConfig{{Spec: destSpec, Dest: &pgdest.Destination{}}},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	time.Sleep(1 * time.Second)

	ddlSQL := fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN extra TEXT`, schemaName, table)
	if _, err := srcPool.Exec(ctx, ddlSQL); err != nil {
		t.Fatalf("alter table: %v", err)
	}

	waitFor(t, 30*time.Second, 200*time.Millisecond, func() (bool, error) {
		var exists bool
		err := dstPool.QueryRow(ctx,
			`SELECT EXISTS (
			   SELECT 1
			   FROM information_schema.columns
			   WHERE table_schema = $1 AND table_name = $2 AND column_name = 'extra'
			 )`, schemaName, table,
		).Scan(&exists)
		if err != nil {
			return false, err
		}
		return exists, nil
	})

	now := time.Now().UTC()
	uid1 := "11111111-1111-1111-1111-111111111111"
	uid2 := "22222222-2222-2222-2222-222222222222"
	uid3 := "33333333-3333-3333-3333-333333333333"
	insertSQL := fmt.Sprintf(`INSERT INTO %s.%s (id, payload, tags, amount, uid, updated_at, extra) VALUES ($1, $2::jsonb, $3::jsonb, $4, $5::uuid, $6, $7)`, schemaName, table)
	if _, err := srcPool.Exec(ctx, insertSQL, 1, `{"status":"new"}`, `["a","b"]`, "10.50", uid1, now, "extra-1"); err != nil {
		t.Fatalf("insert row1: %v", err)
	}
	if _, err := srcPool.Exec(ctx, insertSQL, 2, `{"status":"old"}`, `["x","y"]`, "20.00", uid2, now, "extra-2"); err != nil {
		t.Fatalf("insert row2: %v", err)
	}
	if _, err := srcPool.Exec(ctx, insertSQL, 3, `{"status":"move"}`, `["m"]`, "42.00", uid3, now, "extra-3"); err != nil {
		t.Fatalf("insert row3: %v", err)
	}

	updateSQL := fmt.Sprintf(`UPDATE %s.%s SET payload = $2::jsonb, tags = $3::jsonb, amount = $4, updated_at = $5, extra = $6 WHERE id = $1`, schemaName, table)
	if _, err := srcPool.Exec(ctx, updateSQL, 1, `{"status":"updated"}`, `["c"]`, "11.75", now.Add(1*time.Second), "extra-1b"); err != nil {
		t.Fatalf("update row1: %v", err)
	}
	updatePKSQL := fmt.Sprintf(`UPDATE %s.%s SET id = $2, amount = $3, updated_at = $4 WHERE id = $1`, schemaName, table)
	if _, err := srcPool.Exec(ctx, updatePKSQL, 3, 4, "43.00", now.Add(2*time.Second)); err != nil {
		t.Fatalf("update row3 -> row4: %v", err)
	}

	deleteSQL := fmt.Sprintf(`DELETE FROM %s.%s WHERE id = $1`, schemaName, table)
	if _, err := srcPool.Exec(ctx, deleteSQL, 2); err != nil {
		t.Fatalf("delete row2: %v", err)
	}

	waitFor(t, 30*time.Second, 200*time.Millisecond, func() (bool, error) {
		var count int
		query := fmt.Sprintf("SELECT count(*) FROM %s.%s", schemaName, table)
		if err := dstPool.QueryRow(ctx, query).Scan(&count); err != nil {
			return false, err
		}
		return count == 2, nil
	})

	var payloadRaw, tagsRaw []byte
	var extra, amountText, uidText string
	rowQuery := fmt.Sprintf("SELECT payload::text, tags::text, extra, amount::text, uid::text FROM %s.%s WHERE id = $1", schemaName, table)
	if err := dstPool.QueryRow(ctx, rowQuery, 1).Scan(&payloadRaw, &tagsRaw, &extra, &amountText, &uidText); err != nil {
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
	if extra != "extra-1b" {
		t.Fatalf("expected extra column to match, got %s", extra)
	}
	if amountText != "11.75" {
		t.Fatalf("expected amount 11.75, got %s", amountText)
	}
	if uidText != uid1 {
		t.Fatalf("expected uid %s, got %s", uid1, uidText)
	}

	var movedCount int
	movedQuery := fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE id = $1", schemaName, table)
	if err := dstPool.QueryRow(ctx, movedQuery, 3).Scan(&movedCount); err != nil {
		t.Fatalf("check id 3: %v", err)
	}
	if movedCount != 0 {
		t.Fatalf("expected id 3 to be moved, still present")
	}
	var movedAmount string
	if err := dstPool.QueryRow(ctx, movedQuery, 4).Scan(&movedCount); err != nil {
		t.Fatalf("check id 4: %v", err)
	}
	if movedCount != 1 {
		t.Fatalf("expected id 4 to exist, count=%d", movedCount)
	}
	if err := dstPool.QueryRow(ctx, fmt.Sprintf("SELECT amount::text FROM %s.%s WHERE id = $1", schemaName, table), 4).Scan(&movedAmount); err != nil {
		t.Fatalf("read id 4 amount: %v", err)
	}
	if movedAmount != "43.00" {
		t.Fatalf("expected id 4 amount 43.00, got %s", movedAmount)
	}

	var metaCount int
	if err := dstPool.QueryRow(ctx, `SELECT count(*) FROM wallaby_meta.__metadata WHERE pk_id = $1 AND is_deleted = true AND operation = 'delete'`, "2").Scan(&metaCount); err != nil {
		t.Fatalf("read meta delete: %v", err)
	}
	if metaCount == 0 {
		t.Fatalf("expected meta delete for id 2, got none")
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

func dsnWithDatabase(baseDSN, database string) (string, error) {
	if strings.TrimSpace(baseDSN) == "" {
		return "", errors.New("base DSN is empty")
	}
	if u, err := url.Parse(baseDSN); err == nil && u.Scheme != "" {
		u.Path = "/" + database
		return u.String(), nil
	}

	parts := strings.Fields(baseDSN)
	replaced := false
	for i, part := range parts {
		if strings.HasPrefix(part, "dbname=") || strings.HasPrefix(part, "database=") {
			parts[i] = "dbname=" + database
			replaced = true
		}
	}
	if !replaced {
		parts = append(parts, "dbname="+database)
	}
	return strings.Join(parts, " "), nil
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
