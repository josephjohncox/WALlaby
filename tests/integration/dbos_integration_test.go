package integration_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/orchestrator"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
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

	store, err := pgstream.NewStore(ctx, dsn)
	if err != nil {
		t.Fatalf("open stream store: %v", err)
	}
	store.Close()

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
			"flow_id":             flowID,
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

	queueName := "wallaby"
	orch, err := orchestrator.NewDBOSOrchestrator(ctx, orchestrator.Config{
		AppName:       "wallaby-test",
		DatabaseURL:   dsn,
		Queue:         queueName,
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

	t.Logf("dbos backfill flow=%s stream=%s queue=%s", flowID, streamName, queueName)
	inputPayload := ""
	if payload, err := json.Marshal(orchestrator.FlowRunInput{
		FlowID:        flowID,
		MaxEmptyReads: 1,
	}); err == nil {
		inputPayload = base64.StdEncoding.EncodeToString(payload)
	}
	waitForStreamEvents(t, ctx, pool, flowID, streamName, inputPayload, 2)
}

func TestDBOSIntegrationStreaming(t *testing.T) {
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

func TestDBOSIntegrationRetries(t *testing.T) {
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
	flowID := fmt.Sprintf("flow-retry-%d", suffix)

	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatalf("create engine: %v", err)
	}
	defer engine.Close()
	defer func() {
		_ = engine.Delete(context.Background(), flowID)
	}()

	badSource := connector.Spec{
		Name: "bad-source",
		Type: connector.EndpointType("bogus"),
	}
	destSpec := connector.Spec{
		Name: "stream",
		Type: connector.EndpointPGStream,
		Options: map[string]string{
			"dsn":    dsn,
			"stream": fmt.Sprintf("wallaby_retry_%d", suffix),
			"format": "json",
		},
	}

	created, err := engine.Create(ctx, flow.Flow{
		ID:           flowID,
		Name:         "dbos-retry",
		Source:       badSource,
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
		MaxRetries:    1,
		MaxRetriesSet: true,
		DefaultWire:   connector.WireFormatJSON,
	}, engine, nil, runner.Factory{})
	if err != nil {
		t.Fatalf("create dbos orchestrator: %v", err)
	}
	defer orch.Shutdown(5 * time.Second)

	if err := orch.EnqueueFlow(ctx, flowID); err != nil {
		t.Fatalf("enqueue flow: %v", err)
	}

	startedAt := time.Now().Add(-1 * time.Second).UnixMilli()

	waitFor(t, 30*time.Second, 500*time.Millisecond, func() (bool, error) {
		var status string
		var attempts int64
		var errMsg sql.NullString
		err := pool.QueryRow(ctx, `SELECT status, recovery_attempts, error
FROM dbos.workflow_status
WHERE created_at >= $1 AND error ILIKE '%unsupported source type%'
ORDER BY created_at DESC
LIMIT 1`, startedAt).Scan(&status, &attempts, &errMsg)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		if status == "ERROR" || status == "MAX_RECOVERY_ATTEMPTS_EXCEEDED" {
			if attempts >= 1 {
				if errMsg.Valid && !strings.Contains(errMsg.String, "unsupported source type") {
					t.Fatalf("unexpected error: %s", errMsg.String)
				}
				return true, nil
			}
		}
		return false, nil
	})
}

func waitForStreamEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, flowID, streamName, inputPayload string, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	logEvery := 5 * time.Second
	nextLog := time.Now().Add(logEvery)

	for {
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM stream_events WHERE stream = $1", streamName).Scan(&count); err != nil {
			logDBOSDiagnostics(t, ctx, pool, flowID, streamName)
			t.Fatalf("query stream_events: %v", err)
		}
		if count >= want {
			return
		}
		now := time.Now()
		if inputPayload != "" {
			var status string
			var errMsg sql.NullString
			err := pool.QueryRow(ctx, `SELECT status, error
FROM dbos.workflow_status
WHERE inputs = $1
ORDER BY created_at DESC
LIMIT 1`, inputPayload).Scan(&status, &errMsg)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				t.Logf("diagnostics dbos workflow status query failed: %v", err)
			} else if err == nil {
				if status == "ERROR" {
					t.Fatalf("dbos workflow error status=%s error=%s", status, errMsg.String)
				}
				if status == "SUCCESS" && count < want {
					t.Fatalf("dbos workflow completed without stream events status=%s stream=%s count=%d want=%d", status, streamName, count, want)
				}
			}
		}

		if now.After(nextLog) {
			t.Logf("waiting for stream_events stream=%s count=%d want=%d", streamName, count, want)
			logDBOSDiagnostics(t, ctx, pool, flowID, streamName)
			nextLog = now.Add(logEvery)
		}
		if now.After(deadline) {
			logDBOSDiagnostics(t, ctx, pool, flowID, streamName)
			t.Fatalf("timed out waiting for stream events stream=%s count=%d want=%d", streamName, count, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func logDBOSDiagnostics(t *testing.T, ctx context.Context, pool *pgxpool.Pool, flowID, streamName string) {
	t.Helper()

	var flowState string
	if err := pool.QueryRow(ctx, "SELECT state FROM flows WHERE id = $1", flowID).Scan(&flowState); err != nil {
		t.Logf("diagnostics flow state query failed: %v", err)
	} else {
		t.Logf("diagnostics flow state=%s", flowState)
	}

	rows, err := pool.Query(ctx, "SELECT from_state, to_state, COALESCE(reason, ''), created_at FROM flow_state_events WHERE flow_id = $1 ORDER BY id DESC LIMIT 5", flowID)
	if err != nil {
		t.Logf("diagnostics flow state events query failed: %v", err)
	} else {
		for rows.Next() {
			var fromState sql.NullString
			var toState, reason string
			var createdAt time.Time
			if scanErr := rows.Scan(&fromState, &toState, &reason, &createdAt); scanErr != nil {
				t.Logf("diagnostics flow state event scan failed: %v", scanErr)
				continue
			}
			fromValue := ""
			if fromState.Valid {
				fromValue = fromState.String
			}
			t.Logf("diagnostics flow event from=%s to=%s at=%s reason=%s", fromValue, toState, createdAt.UTC().Format(time.RFC3339), reason)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Logf("diagnostics flow state events rows error: %v", rowsErr)
		}
		rows.Close()
	}

	var streamCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM stream_events WHERE stream = $1", streamName).Scan(&streamCount); err != nil {
		t.Logf("diagnostics stream_events query failed: %v", err)
	} else {
		t.Logf("diagnostics stream_events count=%d stream=%s", streamCount, streamName)
	}

	rows, err = pool.Query(ctx, `SELECT workflow_uuid, status, name,
  COALESCE(queue_name, ''), COALESCE(deduplication_id, ''), COALESCE(error, ''),
  created_at, updated_at
FROM dbos.workflow_status
WHERE deduplication_id = $1
ORDER BY created_at DESC`, flowID)
	if err != nil {
		t.Logf("diagnostics dbos workflow status query failed: %v", err)
	} else {
		found := false
		for rows.Next() {
			var workflowID, status, name, queueName, dedupID, errMsg string
			var createdAt, updatedAt int64
			if scanErr := rows.Scan(&workflowID, &status, &name, &queueName, &dedupID, &errMsg, &createdAt, &updatedAt); scanErr != nil {
				t.Logf("diagnostics dbos workflow status scan failed: %v", scanErr)
				continue
			}
			found = true
			t.Logf("diagnostics dbos workflow id=%s status=%s name=%s queue=%s dedup=%s created=%s updated=%s error=%s",
				workflowID,
				status,
				name,
				queueName,
				dedupID,
				time.UnixMilli(createdAt).UTC().Format(time.RFC3339),
				time.UnixMilli(updatedAt).UTC().Format(time.RFC3339),
				errMsg,
			)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Logf("diagnostics dbos workflow status rows error: %v", rowsErr)
		}
		rows.Close()
		if !found {
			t.Logf("diagnostics dbos workflow status: no rows for deduplication_id=%s", flowID)
		}
	}

	rows, err = pool.Query(ctx, `SELECT workflow_uuid, status, name,
  COALESCE(queue_name, ''), COALESCE(deduplication_id, ''), COALESCE(error, ''),
  COALESCE(inputs, ''), created_at, updated_at
FROM dbos.workflow_status
ORDER BY created_at DESC
LIMIT 5`)
	if err != nil {
		t.Logf("diagnostics dbos recent workflows query failed: %v", err)
	} else {
		for rows.Next() {
			var workflowID, status, name, queueName, dedupID, errMsg, inputs string
			var createdAt, updatedAt int64
			if scanErr := rows.Scan(&workflowID, &status, &name, &queueName, &dedupID, &errMsg, &inputs, &createdAt, &updatedAt); scanErr != nil {
				t.Logf("diagnostics dbos recent workflows scan failed: %v", scanErr)
				continue
			}
			inputPreview := inputs
			if len(inputPreview) > 200 {
				inputPreview = inputPreview[:200] + "..."
			}
			t.Logf("diagnostics dbos recent id=%s status=%s name=%s queue=%s dedup=%s created=%s updated=%s error=%s",
				workflowID,
				status,
				name,
				queueName,
				dedupID,
				time.UnixMilli(createdAt).UTC().Format(time.RFC3339),
				time.UnixMilli(updatedAt).UTC().Format(time.RFC3339),
				errMsg,
			)
			if inputPreview != "" {
				t.Logf("diagnostics dbos recent inputs=%s", inputPreview)
			}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Logf("diagnostics dbos recent workflows rows error: %v", rowsErr)
		}
		rows.Close()
	}

	rows, err = pool.Query(ctx, "SELECT status, count(*) FROM dbos.workflow_status GROUP BY status ORDER BY status")
	if err != nil {
		t.Logf("diagnostics dbos status counts query failed: %v", err)
	} else {
		for rows.Next() {
			var status string
			var count int
			if scanErr := rows.Scan(&status, &count); scanErr != nil {
				t.Logf("diagnostics dbos status counts scan failed: %v", scanErr)
				continue
			}
			t.Logf("diagnostics dbos status count status=%s count=%d", status, count)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Logf("diagnostics dbos status counts rows error: %v", rowsErr)
		}
		rows.Close()
	}
}
