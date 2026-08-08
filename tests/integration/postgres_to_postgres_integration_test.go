package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/replication"
	runnerpkg "github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/spec"
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
	flowID := "e2e-flow-" + suffix
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

	sourceSpec := connector.RuntimeSpec{
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
		},
	}

	destSpec := connector.RuntimeSpec{
		Name: "e2e-dest",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                dstDSN,
			"meta_table_enabled": "true",
			"synchronous_commit": "off",
		},
	}

	if err := controlplane.ApplyMigrations(ctx, srcPool); err != nil {
		t.Fatalf("apply centralized control migrations: %v", err)
	}
	checkpointStore, err := checkpoint.NewPostgresStore(ctx, srcDSN)
	if err != nil {
		t.Fatalf("create checkpoint store: %v", err)
	}
	defer checkpointStore.Close()
	registryStore, err := registry.NewPostgresStore(ctx, srcDSN)
	if err != nil {
		t.Fatalf("create registry store: %v", err)
	}
	defer registryStore.Close()

	mappings := flow.TableMappings{
		Version: flow.TableMappingsVersion,
		Destinations: []flow.DestinationTableMappings{{
			Destination: destSpec.Name,
			FutureTables: flow.FutureTableMapping{
				Action: flow.MappingActionExclude,
			},
			Tables: []flow.TableMapping{{
				SourceSchema: schemaName,
				SourceTable:  table,
				Action:       flow.MappingActionInclude,
				TargetSchema: schemaName,
				TargetTable:  table,
				FutureColumns: flow.FutureColumnMapping{
					Action:       flow.MappingActionInclude,
					TargetColumn: "{{ .Column }}",
				},
				Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}},
			}},
		}},
	}
	if err := mappings.Validate([]connector.RuntimeSpec{destSpec}); err != nil {
		t.Fatalf("validate E2E mappings: %v", err)
	}
	gate, autoApprove, autoApply := true, false, true
	definition := flow.Flow{
		ID: flowID, Source: testFlowSource(sourceSpec), Destinations: testFlowDestinations(destSpec),
		Config: flow.Config{TableMappings: mappings, DDL: flow.DDLPolicy{Gate: &gate, AutoApprove: &autoApprove, AutoApply: &autoApply}},
	}
	ddlDefaults := flow.ShippedDDLPolicyDefaults()
	factory := runnerpkg.Factory{SchemaHookForFlow: func(f flow.Flow) replication.SchemaHook {
		policy := flow.ResolveDDLPolicy(f.Config.DDL, &ddlDefaults)
		return &registry.Hook{Store: registryStore, FlowID: f.ID, AutoApprove: policy.AutoApprove, GateApproval: policy.Gate, AutoApply: policy.AutoApply}
	}}
	traceSink := &stream.MemoryTraceSink{}
	newRunner := func() stream.Runner {
		source, err := factory.SourceForFlow(definition)
		if err != nil {
			t.Fatalf("construct production source: %v", err)
		}
		destinations, err := factory.DestinationsForFlow(definition)
		if err != nil {
			t.Fatalf("construct production destinations: %v", err)
		}
		streamRunner, err := runnerpkg.NewStreamRunner(definition, source, destinations, runnerpkg.StreamRunnerConfig{
			Checkpoints: checkpointStore, DDLExecutions: registryStore, DDLPolicyDefaults: &ddlDefaults, TraceSink: traceSink,
		})
		if err != nil {
			t.Fatalf("construct production stream runner: %v", err)
		}
		if !streamRunner.RequireDDLExecution {
			t.Fatal("production construction did not resolve auto_apply=true")
		}
		return streamRunner
	}

	streamRunner := newRunner()
	errCh := make(chan error, 1)
	go func() {
		errCh <- streamRunner.Run(ctx)
	}()

	time.Sleep(1 * time.Second)

	waitFor(t, 10*time.Second, 200*time.Millisecond, func() (bool, error) {
		select {
		case runnerErr := <-errCh:
			if runnerErr == nil {
				return false, errors.New("runner exited before source-state registration")
			}
			return false, fmt.Errorf("runner exited before source-state registration: %w", runnerErr)
		default:
		}
		var count int
		err := srcPool.QueryRow(ctx,
			"SELECT COUNT(*) FROM wallaby.source_state WHERE slot_name = $1 AND publication_name = $2",
			slot, pub,
		).Scan(&count)
		if err != nil {
			return false, err
		}
		return count > 0, nil
	})

	positionBeforeBaseline := ""
	if initialCheckpoint, err := checkpointStore.Get(ctx, definition.ID); err == nil {
		positionBeforeBaseline, err = connector.CheckpointPositionID(initialCheckpoint)
		if err != nil {
			t.Fatalf("identify initial checkpoint: %v", err)
		}
	} else if !errors.Is(err, connector.ErrCheckpointNotFound) {
		t.Fatalf("load initial checkpoint: %v", err)
	}

	baselineTime := time.Now().UTC()
	baselineInsert := fmt.Sprintf(`INSERT INTO %s.%s (id,payload,tags,amount,uid,updated_at) VALUES (0,'{"baseline":true}'::jsonb,'[]'::jsonb,0,'00000000-0000-0000-0000-000000000000'::uuid,$1)`, schemaName, table)
	if _, err := srcPool.Exec(ctx, baselineInsert, baselineTime); err != nil {
		t.Fatalf("seed pre-DDL relation baseline: %v", err)
	}
	positionBeforeDDL := ""
	waitFor(t, 15*time.Second, 100*time.Millisecond, func() (bool, error) {
		select {
		case runnerErr := <-errCh:
			return false, fmt.Errorf("runner exited before establishing the pre-DDL schema baseline: %w", runnerErr)
		default:
		}
		var count int
		if err := dstPool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE id=0", schemaName, table)).Scan(&count); err != nil || count != 1 {
			return false, err
		}
		checkpointBeforeDDL, err := checkpointStore.Get(ctx, definition.ID)
		if errors.Is(err, connector.ErrCheckpointNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		position, err := connector.CheckpointPositionID(checkpointBeforeDDL)
		if err != nil {
			return false, err
		}
		if position == positionBeforeBaseline {
			return false, nil
		}
		positionBeforeDDL = position
		return true, nil
	})

	ddlSQL := fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN extra TEXT`, schemaName, table)
	if _, err := srcPool.Exec(ctx, ddlSQL); err != nil {
		t.Fatalf("alter source table: %v", err)
	}

	now := time.Now().UTC()
	uid1 := "11111111-1111-1111-1111-111111111111"
	uid2 := "22222222-2222-2222-2222-222222222222"
	uid3 := "33333333-3333-3333-3333-333333333333"
	insertSQL := fmt.Sprintf(`INSERT INTO %s.%s (id, payload, tags, amount, uid, updated_at, extra) VALUES ($1, $2::jsonb, $3::jsonb, $4, $5::uuid, $6, $7)`, schemaName, table)
	if _, err := srcPool.Exec(ctx, insertSQL, 1, `{"status":"new"}`, `["a","b"]`, "10.50", uid1, now, "extra-1"); err != nil {
		t.Fatalf("insert row1: %v", err)
	}
	var gateErr error
	select {
	case gateErr = <-errCh:
	case <-time.After(15 * time.Second):
		t.Fatal("production runner did not pause on pending DDL gate")
	}
	if !errors.Is(gateErr, connector.ErrDDLApprovalRequired) {
		t.Fatalf("first production runner error=%v, want DDL approval gate", gateErr)
	}
	gateDetails, ok := connector.AsDDLGate(gateErr)
	if !ok || gateDetails.EventID == 0 {
		t.Fatalf("DDL gate lacks durable event identity: %v", gateErr)
	}
	ddlEventID := gateDetails.EventID
	var ddlStatus string
	if err := srcPool.QueryRow(ctx, `SELECT status FROM ddl_events WHERE id=$1 AND flow_id=$2`, ddlEventID, definition.ID).Scan(&ddlStatus); err != nil {
		t.Fatalf("read pending DDL: %v", err)
	}
	if ddlStatus != registry.StatusPending {
		t.Fatalf("captured DDL status=%s, want pending before administrative approval", ddlStatus)
	}
	checkpointAtGate, err := checkpointStore.Get(ctx, definition.ID)
	if err != nil {
		t.Fatalf("load checkpoint at pending DDL gate: %v", err)
	}
	positionBeforeDDL, err = connector.CheckpointPositionID(checkpointAtGate)
	if err != nil {
		t.Fatalf("identify checkpoint at pending DDL gate: %v", err)
	}
	if err := registryStore.SetDDLStatus(ctx, ddlEventID, registry.StatusApproved); err != nil {
		t.Fatalf("approve DDL through current registry administration API: %v", err)
	}

	// Simulate a process failure after the source hook has registered the new
	// schema but before destination DDL or its execution receipt can commit. On
	// the next restart the observed relation diff is empty, so recovery depends
	// on replaying the exact approved durable plan by flow/fence/LSN.
	injectedDDLFailure := errors.New("injected failure before destination DDL")
	failedRunner := newRunner()
	failedRunner.Destinations[0].Dest = &failingDDLDestination{
		Destination: failedRunner.Destinations[0].Dest,
		err:         injectedDDLFailure,
	}
	failedErrCh := make(chan error, 1)
	go func() {
		failedErrCh <- failedRunner.Run(ctx)
	}()
	select {
	case failedErr := <-failedErrCh:
		if !errors.Is(failedErr, injectedDDLFailure) {
			t.Fatalf("failure-boundary runner error=%v, want injected DDL failure", failedErr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("failure-boundary runner did not reach destination DDL")
	}
	registeredSchema, foundRegisteredSchema, err := registryStore.LatestSchemaForFlow(ctx, definition.ID, schemaName, table)
	if err != nil {
		t.Fatalf("load schema registered before injected destination failure: %v", err)
	}
	if !foundRegisteredSchema {
		t.Fatal("new schema was not durably registered before injected destination failure")
	}
	registeredExtra := false
	for _, column := range registeredSchema.Columns {
		registeredExtra = registeredExtra || column.Name == "extra"
	}
	if !registeredExtra {
		t.Fatalf("registered schema after injected failure=%+v, want already-new baseline", registeredSchema)
	}
	var destinationExtra bool
	if err := dstPool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 AND column_name='extra')`, schemaName, table).Scan(&destinationExtra); err != nil {
		t.Fatalf("inspect destination after injected DDL failure: %v", err)
	}
	if destinationExtra {
		t.Fatal("destination DDL committed despite injected pre-ApplyDDL failure")
	}
	if err := srcPool.QueryRow(ctx, `SELECT status FROM ddl_events WHERE id=$1`, ddlEventID).Scan(&ddlStatus); err != nil {
		t.Fatalf("read DDL status after injected failure: %v", err)
	}
	if ddlStatus != registry.StatusApproved {
		t.Fatalf("DDL status after injected failure=%s, want approved for restart replay", ddlStatus)
	}
	checkpointAfterFailure, err := checkpointStore.Get(ctx, definition.ID)
	if err != nil {
		t.Fatalf("load checkpoint after injected DDL failure: %v", err)
	}
	positionAfterFailure, err := connector.CheckpointPositionID(checkpointAfterFailure)
	if err != nil {
		t.Fatalf("identify checkpoint after injected DDL failure: %v", err)
	}
	if positionAfterFailure != positionBeforeDDL {
		t.Fatalf("checkpoint advanced across unreceipted DDL: before=%s after=%s", positionBeforeDDL, positionAfterFailure)
	}

	streamRunner = newRunner()
	errCh = make(chan error, 1)
	go func() {
		errCh <- streamRunner.Run(ctx)
	}()
	waitFor(t, 15*time.Second, 100*time.Millisecond, func() (bool, error) {
		select {
		case runnerErr := <-errCh:
			var columnExists bool
			_ = dstPool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 AND column_name='extra')`, schemaName, table).Scan(&columnExists)
			var attempts, receipts int
			_ = srcPool.QueryRow(ctx, `SELECT count(*) FROM ddl_execution_attempts WHERE event_id=$1`, ddlEventID).Scan(&attempts)
			_ = srcPool.QueryRow(ctx, `SELECT count(*) FROM ddl_execution_receipts WHERE event_id=$1`, ddlEventID).Scan(&receipts)
			return false, fmt.Errorf("resumed production runner exited before structured DDL orchestration completed (column=%t attempts=%d receipts=%d): %w", columnExists, attempts, receipts, runnerErr)
		default:
		}
		var columnExists bool
		if err := dstPool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 AND column_name='extra')`, schemaName, table).Scan(&columnExists); err != nil || !columnExists {
			return false, err
		}
		var applied int
		err := srcPool.QueryRow(ctx, `SELECT count(*) FROM ddl_events event JOIN ddl_execution_manifests manifest ON manifest.event_id=event.id JOIN ddl_execution_receipts receipt ON receipt.event_id=event.id WHERE event.id=$1 AND event.flow_id=$2 AND event.status='applied' AND manifest.destinations=ARRAY[$3]::text[] AND manifest.manifest_hash<>'' AND receipt.destination=$3`, ddlEventID, definition.ID, destSpec.Name).Scan(&applied)
		return applied == 1, err
	})
	var positionAfterApply string
	waitFor(t, 5*time.Second, 50*time.Millisecond, func() (bool, error) {
		checkpointAfterApply, err := checkpointStore.Get(ctx, definition.ID)
		if err != nil {
			return false, err
		}
		positionAfterApply, err = connector.CheckpointPositionID(checkpointAfterApply)
		if err != nil {
			return false, err
		}
		return positionAfterApply != positionBeforeDDL, nil
	})
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

	deleteSQL := fmt.Sprintf(`DELETE FROM %s.%s WHERE id = ANY($1)`, schemaName, table)
	if _, err := srcPool.Exec(ctx, deleteSQL, []int64{0, 2}); err != nil {
		t.Fatalf("delete baseline and row2: %v", err)
	}

	waitFor(t, 30*time.Second, 200*time.Millisecond, func() (bool, error) {
		select {
		case runnerErr := <-errCh:
			if runnerErr == nil {
				return false, errors.New("runner exited before delivering table-scoped batches")
			}
			return false, fmt.Errorf("runner exited before delivering table-scoped batches: %w", runnerErr)
		default:
		}
		var count int
		var rowOneStatus string
		query := fmt.Sprintf(`SELECT count(*),COALESCE((SELECT payload->>'status' FROM %s.%s WHERE id=1),'') FROM %s.%s`, schemaName, table, schemaName, table)
		if err := dstPool.QueryRow(ctx, query).Scan(&count, &rowOneStatus); err != nil {
			return false, err
		}
		return count == 2 && rowOneStatus == "updated", nil
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

	manifest := loadCDCFlowManifest(t)
	if _, err := stream.EvaluateTrace(traceSink.Events(), stream.TraceValidationOptions{}, &manifest); err != nil {
		t.Fatalf("trace validation failed: %v", err)
	}
}

func TestManagedBootstrapNeverRejectsTamperedPostgresAuthorityBeforeSourceIO(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	database := fmt.Sprintf("wallaby_tampered_destination_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+database); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+database+" WITH (FORCE)") }()
	destinationDSN, err := dsnWithDatabase(baseDSN, database)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, destinationDSN)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE SCHEMA wallaby_meta; CREATE TABLE wallaby_meta.__delivery_receipts (broken text)`); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	events := []string{}
	source := &managedStartupProbeSource{events: &events}
	fence := authority.RunFence{FlowID: "tampered-bootstrap-never", FlowIncarnationID: uuid.New(), Generation: 1, AcquisitionID: uuid.New(), ExecutionID: "test", LeaseEpoch: 1}
	runner := stream.Runner{
		Source: source,
		SourceSpec: connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{
			"managed": "true", "bootstrap": "never", "source_lineage_id": "lineage",
		}},
		Destinations: []stream.DestinationConfig{{Spec: connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{
			"dsn": destinationDSN, "batch_mode": "target", "destination_revision_id": "revision",
		}}, Dest: &pgdest.Destination{}}},
		Checkpoints: managedStartupCheckpointStore{}, FlowID: fence.FlowID, AckPolicy: stream.AckPolicyAll,
		RunFence: &fence, DeliveryCoordinator: managedStartupCoordinator{}, SchemaBaselines: managedStartupSchemaBaselines{},
	}
	err = runner.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "exact columns/NOT NULL contract mismatch") {
		t.Fatalf("tampered destination startup error=%v, want exact authority rejection", err)
	}
	if len(events) != 0 {
		t.Fatalf("tampered destination initialization invoked source methods: %v", events)
	}
}

type managedStartupSchemaBaselines struct{}

func (managedStartupSchemaBaselines) Load(context.Context, connector.RunFence, string) ([]connector.Schema, error) {
	return nil, nil
}
func (managedStartupSchemaBaselines) Persist(context.Context, connector.RunFence, string, []connector.Schema) error {
	return nil
}

type managedStartupProbeSource struct{ events *[]string }

func (s *managedStartupProbeSource) Open(context.Context, connector.RuntimeSpec) error {
	*s.events = append(*s.events, "open")
	return nil
}
func (s *managedStartupProbeSource) Read(context.Context) (connector.Batch, error) {
	*s.events = append(*s.events, "read")
	return connector.Batch{}, errors.New("unexpected source read")
}
func (s *managedStartupProbeSource) ReadTransaction(context.Context) (connector.SourceTransaction, error) {
	*s.events = append(*s.events, "read-transaction")
	return connector.SourceTransaction{}, errors.New("unexpected transaction read")
}
func (s *managedStartupProbeSource) Ack(context.Context, connector.Checkpoint) error {
	*s.events = append(*s.events, "ack")
	return nil
}
func (s *managedStartupProbeSource) AckWithEvidence(context.Context, connector.Checkpoint) (connector.SourceFlushEvidence, error) {
	*s.events = append(*s.events, "ack-with-evidence")
	return connector.SourceFlushEvidence{}, nil
}
func (s *managedStartupProbeSource) Close(context.Context) error {
	*s.events = append(*s.events, "close")
	return nil
}
func (*managedStartupProbeSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, SupportsStreaming: true}
}

type managedStartupCheckpointStore struct{}

func (managedStartupCheckpointStore) Get(context.Context, string) (connector.Checkpoint, error) {
	return connector.Checkpoint{}, connector.ErrCheckpointNotFound
}
func (managedStartupCheckpointStore) Put(context.Context, string, connector.Checkpoint) error {
	return nil
}
func (managedStartupCheckpointStore) List(context.Context) ([]connector.FlowCheckpoint, error) {
	return nil, nil
}
func (managedStartupCheckpointStore) GetFenced(context.Context, authority.RunFence) (connector.Checkpoint, error) {
	return connector.Checkpoint{}, connector.ErrCheckpointNotFound
}
func (managedStartupCheckpointStore) PutFenced(context.Context, authority.RunFence, connector.Checkpoint) error {
	return nil
}
func (managedStartupCheckpointStore) PersistCheckpointAndOutboxFenced(context.Context, authority.RunFence, connector.Checkpoint, []connector.OutboxEntry) error {
	return nil
}
func (managedStartupCheckpointStore) ListOutboxFenced(context.Context, authority.RunFence) ([]connector.OutboxEntry, error) {
	return nil, nil
}
func (managedStartupCheckpointStore) CompleteOutboxFenced(context.Context, authority.RunFence, string, string) error {
	return nil
}

type managedStartupCoordinator struct{}

func (managedStartupCoordinator) AuthorizeAck(context.Context, authority.RunFence, connector.Checkpoint, connector.ManagedSchemaBaselinePayload) (connector.AckGrant, error) {
	return connector.AckGrant{}, errors.New("unexpected checkpoint authorization")
}
func (managedStartupCoordinator) DeliverTransaction(context.Context, authority.RunFence, connector.DeliveryIntent, connector.SourceTransaction, connector.ManagedSchemaBaselinePayload, connector.ManagedTransactionDestination) (connector.AckGrant, error) {
	return connector.AckGrant{}, errors.New("unexpected transaction delivery")
}
func (managedStartupCoordinator) ValidateAckGrant(context.Context, authority.RunFence, connector.AckGrant) error {
	return errors.New("unexpected ACK validation")
}
func (managedStartupCoordinator) RecordAckReceipt(context.Context, authority.RunFence, connector.AckGrant, string) error {
	return errors.New("unexpected ACK receipt")
}
func (managedStartupCoordinator) CommitSourceFeedback(context.Context, authority.RunFence, connector.AckGrant, connector.FlushEvidenceSource) error {
	return errors.New("unexpected source feedback")
}

func loadCDCFlowManifest(t *testing.T) spec.Manifest {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			path := spec.ManifestPath(filepath.Join(dir, "specs"), spec.SpecCDCFlow)
			manifest, err := spec.LoadManifest(path)
			if err != nil {
				t.Fatalf("load manifest: %v", err)
			}
			return manifest
		}
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
		dir = next
	}
	t.Fatalf("go.mod not found while resolving manifest path")
	return spec.Manifest{}
}

type failingDDLDestination struct {
	connector.Destination
	err error
}

func (d *failingDDLDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return d.err
}

func (d *failingDDLDestination) ReconcileDDL(ctx context.Context, schema connector.Schema, record connector.Record) (connector.DDLReconcileResult, error) {
	reconciler, ok := d.Destination.(connector.DDLReconciler)
	if !ok {
		return 0, errors.New("wrapped destination does not implement DDL reconciliation")
	}
	return reconciler.ReconcileDDL(ctx, schema, record)
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
