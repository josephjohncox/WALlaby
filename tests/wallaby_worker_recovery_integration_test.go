package tests

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestWallabyWorkerProcessKillRecovery(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	workerBinary := os.Getenv("WALLABY_WORKER_BINARY")
	if workerBinary == "" {
		t.Fatal("WALLABY_WORKER_BINARY is required for process recovery evidence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	flowID := "wallaby-worker-kill-" + uuid.NewString()
	targetTable := "wallaby_worker_kill_target"
	publication := "wallaby_worker_kill_publication"
	if _, err := pool.Exec(ctx, `
DROP PUBLICATION IF EXISTS wallaby_worker_kill_publication;
DROP TABLE IF EXISTS public.wallaby_worker_kill_source;
DROP TABLE IF EXISTS public.wallaby_worker_kill_target;
CREATE TABLE public.wallaby_worker_kill_source (id bigint PRIMARY KEY, value text);
CREATE TABLE public.wallaby_worker_kill_target (id bigint PRIMARY KEY, value text);
INSERT INTO public.wallaby_worker_kill_source VALUES (0,'snapshot-before-worker');
CREATE PUBLICATION wallaby_worker_kill_publication FOR TABLE public.wallaby_worker_kill_source`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `
DROP PUBLICATION IF EXISTS wallaby_worker_kill_publication;
DROP TABLE IF EXISTS public.wallaby_worker_kill_source;
DROP TABLE IF EXISTS public.wallaby_worker_kill_target`)
	}()

	var sourceSystemID string
	if err := pool.QueryRow(ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&sourceSystemID); err != nil {
		t.Fatal(err)
	}
	publicationRevision, err := pgsource.PublicationFingerprint(ctx, pool, publication)
	if err != nil {
		t.Fatal(err)
	}
	var slotName string
	defer func() {
		if slotName != "" {
			_, _ = pool.Exec(context.Background(), "SELECT pg_catalog.pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slotName)
		}
	}()
	destinationRevisionID := "wallaby-worker-kill-" + uuid.NewString()

	definition := flow.Flow{
		ID: flowID,
		Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{
			"dsn": dsn, "publication": publication, "tables": "public.wallaby_worker_kill_source",
			"ensure_publication": "false", "managed": "true", "bootstrap": "required",
			"managed_profile": connector.ManagedProfilePostgresToPostgresV1, "streaming_transactions": "true",
			"status_interval": "10ms", "batch_timeout": "10ms", "ensure_state": "false",
			"source_system_identifier": sourceSystemID,
			"source_lineage_id":        sourceSystemID + ":" + publication + ":v1",
			"publication_revision":     publicationRevision,
		}},
		Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{
			"dsn": dsn, "schema": "public", "table": targetTable,
			"batch_mode": "target", "meta_table_enabled": "false",
			"managed_profile":    connector.ManagedProfilePostgresToPostgresV1,
			"synchronous_commit": "on", "destination_revision_id": destinationRevisionID,
		}}},
		Config: flow.Config{AckPolicy: stream.AckPolicyAll},
	}
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	defer cleanupBootstrapSlotsForFlow(t, pool, flowID)
	if _, err := engine.Create(ctx, definition); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}

	first := startWorkerProcess(t, workerBinary, dsn, flowID, control.Generation, "process-kill-first")
	defer first.stopAbruptly()
	waitForWorkerProcessCondition(t, ctx, first, "first worker managed bootstrap handoff", func() (bool, error) {
		var ready bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('source_bootstraps') IS NOT NULL`).Scan(&ready); err != nil || !ready {
			return false, err
		}
		var phase string
		if err := pool.QueryRow(ctx, `SELECT phase,slot_name FROM source_bootstraps WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1) ORDER BY bootstrap_generation DESC LIMIT 1`, flowID).Scan(&phase, &slotName); err != nil {
			return false, nil
		}
		var active bool
		err := pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&active)
		return phase == "streaming" && active, err
	})
	waitForWorkerProcessCondition(t, ctx, first, "first worker snapshot publication", func() (bool, error) {
		var value string
		err := pool.QueryRow(ctx, "SELECT value FROM public.wallaby_worker_kill_target WHERE id=0").Scan(&value)
		return value == "snapshot-before-worker", err
	})
	if _, err := pool.Exec(ctx, "INSERT INTO public.wallaby_worker_kill_source VALUES (1,'before-kill')"); err != nil {
		t.Fatal(err)
	}
	waitForWorkerProcessCondition(t, ctx, first, "first worker target commit", func() (bool, error) {
		var value string
		err := pool.QueryRow(ctx, "SELECT value FROM public.wallaby_worker_kill_target WHERE id=1").Scan(&value)
		return value == "before-kill", err
	})
	waitForWorkerProcessCondition(t, ctx, first, "first worker durable ACK receipt", func() (bool, error) {
		var count int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID).Scan(&count)
		return count >= 1, err
	})
	first.stopAbruptly()
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=(SELECT incarnation_id FROM flows WHERE id=$1)`, flowID); err != nil {
		t.Fatal(err)
	}

	second := startWorkerProcess(t, workerBinary, dsn, flowID, control.Generation, "process-kill-second")
	defer second.stopAbruptly()
	waitForWorkerProcessCondition(t, ctx, second, "replacement worker slot activation", func() (bool, error) {
		var active bool
		err := pool.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, slotName).Scan(&active)
		return active, err
	})
	if _, err := pool.Exec(ctx, "INSERT INTO public.wallaby_worker_kill_source VALUES (2,'after-kill')"); err != nil {
		t.Fatal(err)
	}
	waitForWorkerProcessCondition(t, ctx, second, "replacement worker target commit", func() (bool, error) {
		var value string
		err := pool.QueryRow(ctx, "SELECT value FROM public.wallaby_worker_kill_target WHERE id=2").Scan(&value)
		return value == "after-kill", err
	})
	second.stopAbruptly()
}

type workerProcess struct {
	command *exec.Cmd
	output  bytes.Buffer
	done    chan error
	exited  bool
	err     error
}

func startWorkerProcess(t *testing.T, binary, dsn, flowID string, generation int64, executionID string) *workerProcess {
	t.Helper()
	process := &workerProcess{done: make(chan error, 1)}
	process.command = exec.Command(binary,
		"--flow-id", flowID,
		"--generation", fmt.Sprintf("%d", generation),
		"--execution-backend", "integration",
		"--execution-id", executionID,
	)
	process.command.Env = append(os.Environ(), "WALLABY_POSTGRES_DSN="+dsn)
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { process.done <- process.command.Wait() }()
	return process
}

func (p *workerProcess) stopAbruptly() {
	if p == nil || p.exited {
		return
	}
	_ = p.command.Process.Kill()
	p.err = <-p.done
	p.exited = true
}

func waitForWorkerProcessCondition(t *testing.T, ctx context.Context, process *workerProcess, description string, check func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := check()
		if ok {
			return
		}
		if err != nil && !strings.Contains(err.Error(), "no rows") {
			t.Fatalf("%s: %v", description, err)
		}
		select {
		case process.err = <-process.done:
			process.exited = true
			if process.err == nil {
				process.err = errors.New("worker exited without an error")
			}
			t.Fatalf("%s: worker stopped early: %v\n%s", description, process.err, process.output.String())
		case <-ctx.Done():
			t.Fatalf("%s: %v\n%s", description, ctx.Err(), process.output.String())
		case <-ticker.C:
		}
	}
}
