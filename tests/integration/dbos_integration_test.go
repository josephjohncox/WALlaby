package integration_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/orchestrator"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDBOSIntegrationBackfill(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_DBOS_DSN"))
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	}
	if dsn == "" {
		t.Skip("WALLABY_TEST_DBOS_DSN or TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	suffix := time.Now().UnixNano()
	schema := fmt.Sprintf("dbos_%d", suffix)
	table := "events"
	streamName := fmt.Sprintf("wallaby_stream_%d", suffix)
	flowID := fmt.Sprintf("flow-%d", suffix)

	cleanup := func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	}
	defer cleanup()

	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	createTable := fmt.Sprintf(`CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  payload JSONB,
  updated_at TIMESTAMPTZ
)`, schema, table)
	if _, err := pool.Exec(ctx, createTable); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if _, err := pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s.%s (id, payload, updated_at) VALUES ($1, $2::jsonb, $3), ($4, $5::jsonb, $6)`, schema, table),
		1, `{"status":"dbos"}`, time.Now().UTC(),
		2, `{"status":"dbos"}`, time.Now().UTC(),
	); err != nil {
		t.Fatalf("seed source rows: %v", err)
	}

	sourceSpec := connector.Spec{
		Name: "dbos-source",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                 dsn,
			"mode":                "backfill",
			"tables":              fmt.Sprintf("%s.%s", schema, table),
			"batch_size":          "200",
			"snapshot_workers":    "1",
			"snapshot_consistent": "false",
			"resolve_types":       "true",
		},
	}

	destSpec := connector.Spec{
		Name: "dbos-stream",
		Type: connector.EndpointPGStream,
		Options: map[string]string{
			"dsn":    dsn,
			"stream": streamName,
			"format": "json",
		},
	}

	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	defer engine.Close()
	defer func() {
		_ = engine.Delete(context.Background(), flowID)
	}()

	created, err := engine.Create(ctx, flow.Flow{
		ID:           flowID,
		Name:         "dbos-test",
		Source:       sourceSpec,
		Destinations: []connector.Spec{destSpec},
		State:        flow.StateCreated,
		Parallelism:  1,
	})
	if err != nil {
		t.Fatalf("create flow: %v", err)
	}
	if _, err := engine.Start(ctx, created.ID); err != nil {
		t.Fatalf("start flow: %v", err)
	}

	orch, err := orchestrator.NewDBOSOrchestrator(ctx, orchestrator.Config{
		AppName:       "wallaby-test",
		DatabaseURL:   dsn,
		Queue:         "wallaby",
		MaxEmptyReads: 1,
		DefaultWire:   connector.WireFormatJSON,
	}, engine, nil, runner.Factory{})
	if err != nil {
		t.Fatalf("create dbos orchestrator: %v", err)
	}
	defer orch.Shutdown(5 * time.Second)

	if err := orch.EnqueueFlow(ctx, flowID); err != nil {
		t.Fatalf("enqueue flow: %v", err)
	}

	waitFor(t, 30*time.Second, 200*time.Millisecond, func() (bool, error) {
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM stream_events WHERE stream = $1", streamName).Scan(&count); err != nil {
			return false, err
		}
		return count >= 2, nil
	})
}

func TestDBOSIntegrationStreaming(t *testing.T) {
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_DBOS_STREAM")) != "1" {
		t.Skip("set WALLABY_TEST_DBOS_STREAM=1 to run streaming DBOS test")
	}
	dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_DBOS_DSN"))
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	}
	if dsn == "" {
		t.Skip("WALLABY_TEST_DBOS_DSN or TEST_PG_DSN not set")
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

	suffix := time.Now().UnixNano()
	schema := fmt.Sprintf("dbos_stream_%d", suffix)
	table := "events"
	pub := fmt.Sprintf("wallaby_dbos_%d", suffix)
	slot := fmt.Sprintf("wallaby_dbos_%d", suffix)
	streamName := fmt.Sprintf("wallaby_stream_%d", suffix)
	flowID := fmt.Sprintf("flow-%d", suffix)

	cleanup := func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pub))
		_, _ = pool.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", slot)
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	}
	defer cleanup()

	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	createTable := fmt.Sprintf(`CREATE TABLE %s.%s (
  id BIGINT PRIMARY KEY,
  payload JSONB,
  updated_at TIMESTAMPTZ
)`, schema, table)
	if _, err := pool.Exec(ctx, createTable); err != nil {
		t.Fatalf("create table: %v", err)
	}

	sourceSpec := connector.Spec{
		Name: "dbos-source",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                dsn,
			"slot":               slot,
			"publication":        pub,
			"publication_tables": fmt.Sprintf("%s.%s", schema, table),
			"ensure_publication": "true",
			"sync_publication":   "true",
			"batch_size":         "200",
			"batch_timeout":      "200ms",
			"emit_empty":         "false",
			"resolve_types":      "true",
		},
	}

	destSpec := connector.Spec{
		Name: "dbos-stream",
		Type: connector.EndpointPGStream,
		Options: map[string]string{
			"dsn":    dsn,
			"stream": streamName,
			"format": "json",
		},
	}

	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	defer engine.Close()
	defer func() {
		_ = engine.Delete(context.Background(), flowID)
	}()

	created, err := engine.Create(ctx, flow.Flow{
		ID:           flowID,
		Name:         "dbos-test-stream",
		Source:       sourceSpec,
		Destinations: []connector.Spec{destSpec},
		State:        flow.StateCreated,
		Parallelism:  1,
	})
	if err != nil {
		t.Fatalf("create flow: %v", err)
	}
	if _, err := engine.Start(ctx, created.ID); err != nil {
		t.Fatalf("start flow: %v", err)
	}

	orch, err := orchestrator.NewDBOSOrchestrator(ctx, orchestrator.Config{
		AppName:       "wallaby-test",
		DatabaseURL:   dsn,
		Queue:         "wallaby",
		MaxEmptyReads: 5,
		DefaultWire:   connector.WireFormatJSON,
	}, engine, nil, runner.Factory{})
	if err != nil {
		t.Fatalf("create dbos orchestrator: %v", err)
	}
	defer orch.Shutdown(5 * time.Second)

	if err := orch.EnqueueFlow(ctx, flowID); err != nil {
		t.Fatalf("enqueue flow: %v", err)
	}

	waitFor(t, 30*time.Second, 200*time.Millisecond, func() (bool, error) {
		var active bool
		if err := pool.QueryRow(ctx, "SELECT COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name = $1), false)", slot).Scan(&active); err != nil {
			return false, err
		}
		return active, nil
	})

	if _, err := pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s.%s (id, payload, updated_at) VALUES ($1, $2::jsonb, $3)`, schema, table),
		1,
		`{"status":"dbos"}`,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("insert source row: %v", err)
	}

	waitFor(t, 30*time.Second, 200*time.Millisecond, func() (bool, error) {
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM stream_events WHERE stream = $1", streamName).Scan(&count); err != nil {
			return false, err
		}
		return count > 0, nil
	})
}
