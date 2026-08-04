package tests

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

func TestSnowflakeManagedProfileFakesnowFailsClosed(t *testing.T) {
	if !usingFakesnow() || !allowFakesnowSnowflake() {
		t.Skip("fakesnow compatibility gate is opt-in")
	}
	dsn, _, ok := snowflakeTestDSN(t)
	if !ok {
		t.Skip("fakesnow DSN is not configured")
	}
	destination := &snowflake.Destination{}
	err := destination.Open(context.Background(), connector.Spec{Type: connector.EndpointSnowflake, Options: map[string]string{
		"dsn": dsn, "flow_id": "snowflake-flow", "managed_profile": connector.ManagedProfilePostgresToSnowflakeSQLV1,
	}})
	if err == nil || !strings.Contains(err.Error(), "verified HTTPS") {
		t.Fatalf("fakesnow managed admission error=%v, want verified HTTPS failure", err)
	}
}

func TestSnowflakeManagedProfileLiveAdmission(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	if got := fixture.destination.ManagedSnowflakeVersion(); got != fixture.version {
		t.Fatalf("managed Snowflake version=%q, want %q", got, fixture.version)
	}

	bad := fixture.spec
	bad.Options = cloneTestOptions(fixture.spec.Options)
	bad.Options["managed_snowflake_version"] = fixture.version + "-unproven"
	candidate := &snowflake.Destination{}
	if err := candidate.Open(context.Background(), bad); err == nil || !strings.Contains(err.Error(), "exact runtime pin") {
		t.Fatalf("unproven Snowflake version admission error=%v", err)
	}
}

func TestSnowflakeManagedProfileAmbiguousCommit(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 1, "committed")
	intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acquisition-1")
	fixture.destination.SetManagedHooks(snowflake.ManagedHooks{AfterCommit: func() error {
		return errors.New("synthetic response loss after COMMIT")
	}})
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("ambiguous commit error=%v", err)
	}
	fixture.destination.SetManagedHooks(snowflake.ManagedHooks{})
	disposition, evidence, err := fixture.destination.Reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryApplied || evidence.ContentHash != intent.ContentHash {
		t.Fatalf("reconcile disposition/evidence/error=%v/%+v/%v", disposition, evidence, err)
	}
	var rows int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+fixture.targetQualified+` WHERE "ID"=1`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("target rows=%d, want one committed row", rows)
	}
}

func TestSnowflakeManagedProfileOrderedFragmentsAndTypes(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	when := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	transaction := snowflakeManagedTransaction(fixture.schema, 51, []connector.TransactionFragment{
		{Ordinal: 0, Batch: connector.Batch{Schema: fixture.schema, Records: []connector.Record{
			{Table: "widgets", Operation: connector.OpInsert, Key: recordKey(t, map[string]any{"id": int64(1)}), After: map[string]any{"id": int64(1), "value": "first", "payload": []byte{0x01, 0x02}, "amount": "1.25", "active": true, "event_at": when, "extra": nil}},
		}}},
		{Ordinal: 1, Batch: connector.Batch{Schema: fixture.schema, Records: []connector.Record{
			{Table: "widgets", Operation: connector.OpUpdate, Key: recordKey(t, map[string]any{"id": int64(1)}), After: map[string]any{"value": "second", "amount": "2.50"}},
		}}},
		{Ordinal: 2, Batch: connector.Batch{Schema: fixture.schema, Records: []connector.Record{
			{Table: "widgets", Operation: connector.OpDelete, Key: recordKey(t, map[string]any{"id": int64(1)})},
			{Table: "widgets", Operation: connector.OpInsert, Key: recordKey(t, map[string]any{"id": int64(1)}), After: map[string]any{"id": int64(1), "value": "final", "payload": []byte{0x03}, "amount": "3.75", "active": false, "event_at": when.Add(time.Second), "extra": nil}},
		}}},
	})
	intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acquisition-1")
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatal(err)
	}
	var value, payloadHex string
	var amount float64
	var active bool
	var eventAt time.Time
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT \"VALUE\", HEX_ENCODE(\"PAYLOAD\"), \"AMOUNT\", \"ACTIVE\", \"EVENT_AT\" FROM "+fixture.targetQualified+` WHERE "ID"=1`).Scan(&value, &payloadHex, &amount, &active, &eventAt); err != nil {
		t.Fatal(err)
	}
	if value != "final" || payloadHex != "03" || amount != 3.75 || active || !eventAt.Equal(when.Add(time.Second)) {
		t.Fatalf("ordered typed row=%q/%s/%f/%t/%s", value, payloadHex, amount, active, eventAt)
	}
}

func TestSnowflakeManagedProfileSchemaReconciliation(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	planJSON, err := json.Marshal(map[string]any{"Changes": []map[string]any{{
		"Type": "add_column", "Namespace": "public", "Table": "widgets", "Column": "extra", "ToType": "text", "Nullable": true,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	for name, record := range map[string]connector.Record{
		"raw":        {Table: "widgets", Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN extra text", DDLPlan: planJSON},
		"structured": {Table: "widgets", Operation: connector.OpDDL, DDLPlan: planJSON},
	} {
		t.Run(name, func(t *testing.T) {
			transaction := snowflakeManagedTransaction(fixture.schema, 51, []connector.TransactionFragment{{
				Ordinal: 0, Batch: connector.Batch{Schema: fixture.schema, Records: []connector.Record{record}},
			}})
			intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acquisition-1")
			if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err == nil || !strings.Contains(err.Error(), "rejects all DDL") {
				t.Fatalf("managed Snowflake %s DDL error=%v", name, err)
			}
		})
	}

	// Replacement under the admitted name must fail even when the logical
	// columns and ownership comment are recreated.
	replaceTarget := fmt.Sprintf(`CREATE OR REPLACE HYBRID TABLE %s (
  "ID" NUMBER(38,0) NOT NULL,
  "VALUE" VARCHAR,
  "PAYLOAD" BINARY(8388608),
  "AMOUNT" NUMBER(12,2),
  "ACTIVE" BOOLEAN,
  "EVENT_AT" TIMESTAMP_TZ,
  "EXTRA" VARCHAR,
  CONSTRAINT "PK_REPLACEMENT" PRIMARY KEY ("ID")
) COMMENT = '%s'`, fixture.targetQualified, snowflakeManagedOwnershipComment("target", fixture.spec.Options["destination_revision_id"], fixture.spec.Options["managed_schema_contract_hash"], fixture.spec.Options["flow_id"]))
	if _, err := fixture.provisionDB.ExecContext(context.Background(), replaceTarget); err != nil {
		t.Fatal(err)
	}
	transaction := snowflakeManagedInsertTransaction(fixture.schema, 3, "replacement-must-fail")
	intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acquisition-1")
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err == nil || (!strings.Contains(err.Error(), "creation identity") && !strings.Contains(err.Error(), "privileges")) {
		t.Fatalf("same-name target replacement error=%v", err)
	}
}

func TestSnowflakeManagedProfileCancellationAndPoolSafety(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	seed := snowflakeManagedInsertTransaction(fixture.schema, 4, "before-cancel")
	seedIntent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], seed, 1, "acquisition-1")
	if _, err := fixture.destination.ApplyTransaction(context.Background(), seedIntent, seed); err != nil {
		t.Fatal(err)
	}

	lock, err := fixture.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.ExecContext(context.Background(), "UPDATE "+fixture.targetQualified+` SET "VALUE"='locked' WHERE "ID"=4`); err != nil {
		_ = lock.Rollback()
		t.Fatal(err)
	}
	transaction := snowflakeManagedTransaction(fixture.schema, 504, []connector.TransactionFragment{{
		Ordinal: 0, Batch: connector.Batch{Schema: fixture.schema, Records: []connector.Record{{
			Table: "widgets", Operation: connector.OpUpdate, Key: recordKey(t, map[string]any{"id": int64(4)}), After: map[string]any{"value": "after-cancel"},
		}}},
	}})
	intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "acquisition-1")
	cancelCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := fixture.destination.ApplyTransaction(cancelCtx, intent, transaction); !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		_ = lock.Rollback()
		t.Fatalf("canceled in-flight Snowflake DML error=%v", err)
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	disposition, _, err := fixture.destination.Reconcile(context.Background(), intent)
	if err != nil || disposition != connector.DeliveryNotApplied {
		t.Fatalf("canceled transaction reconciliation=%v/%v", disposition, err)
	}
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("destination did not recover after cancellation: %v", err)
	}
	stats := fixture.destination.ManagedSnowflakePoolStats()
	if stats.MaxOpenConnections != 4 || stats.OpenConnections > 4 || stats.InUse != 0 {
		t.Fatalf("managed Snowflake pool stats=%+v, want max=4 with no leaked in-use connection", stats)
	}
}

func TestSnowflakeManagedProfileProcessKillRecovery(t *testing.T) {
	fixture := newSnowflakeManagedFixture(t)
	signal := filepath.Join(t.TempDir(), "snowflake-open-transaction")
	encodedSpec, err := json.Marshal(fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestSnowflakeManagedProfileProcessKillHelper$", "-test.v")
	command.Env = append(os.Environ(),
		"WALLABY_SNOWFLAKE_PROCESS_HELPER=1",
		"WALLABY_SNOWFLAKE_PROCESS_SPEC="+string(encodedSpec),
		"WALLABY_SNOWFLAKE_PROCESS_SIGNAL="+signal,
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if _, err := os.Stat(signal); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatal("helper did not reach the pre-COMMIT open-transaction boundary")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()

	transaction := snowflakeManagedInsertTransaction(fixture.schema, 99, "process-kill")
	intent := snowflakeManagedIntent(t, fixture.spec.Options["destination_revision_id"], transaction, 1, "process-helper")
	deadline = time.Now().Add(2 * time.Minute)
	for {
		disposition, _, reconcileErr := fixture.destination.Reconcile(context.Background(), intent)
		if reconcileErr == nil && disposition == connector.DeliveryNotApplied {
			break
		}
		if reconcileErr == nil && disposition == connector.DeliveryApplied {
			t.Fatal("pre-COMMIT process kill unexpectedly left a committed receipt")
		}
		if time.Now().After(deadline) {
			t.Fatalf("pre-COMMIT process-kill reconciliation=%v/%v", disposition, reconcileErr)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if _, err := fixture.destination.ApplyTransaction(context.Background(), intent, transaction); err != nil {
		t.Fatalf("replay after killed open transaction: %v", err)
	}
	var rows int
	if err := fixture.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+fixture.targetQualified+` WHERE "ID"=99`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows after killed-transaction replay=%d, want 1", rows)
	}
}

func TestSnowflakeManagedProfileProcessKillHelper(t *testing.T) {
	if os.Getenv("WALLABY_SNOWFLAKE_PROCESS_HELPER") != "1" {
		t.Skip("process-kill helper")
	}
	var spec connector.Spec
	if err := json.Unmarshal([]byte(os.Getenv("WALLABY_SNOWFLAKE_PROCESS_SPEC")), &spec); err != nil {
		t.Fatal(err)
	}
	var schema connector.Schema
	if err := json.Unmarshal([]byte(spec.Options["managed_schema_contract"]), &schema); err != nil {
		t.Fatal(err)
	}
	destination := &snowflake.Destination{}
	if err := destination.Open(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(context.Background())
	destination.SetManagedHooks(snowflake.ManagedHooks{BeforeCommit: func() error {
		if err := os.WriteFile(os.Getenv("WALLABY_SNOWFLAKE_PROCESS_SIGNAL"), []byte("committed"), 0o600); err != nil {
			return err
		}
		select {}
	}})
	transaction := snowflakeManagedInsertTransaction(schema, 99, "process-kill")
	intent := snowflakeManagedIntent(t, spec.Options["destination_revision_id"], transaction, 1, "process-helper")
	_, _ = destination.ApplyTransaction(context.Background(), intent, transaction)
}

func TestPostgresToSnowflakeManagedProfileRecoveryContract(t *testing.T) {
	runID := time.Now().UnixNano()
	resourceSuffix := strings.ToLower(strconv.FormatInt(runID, 36))
	flowID := "snowflake-managed-recovery-" + strconv.FormatInt(runID, 10)
	sourceSchema := "wallaby_sf_gate_" + resourceSuffix
	sourceTable := "widgets"
	fixture := newSnowflakeManagedFixtureForFlowSource(t, flowID, sourceSchema, sourceTable)
	dsn := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required for PostgreSQL-authoritative Snowflake recovery evidence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	engine, err := workflow.NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	var postgresVersion int
	var sourceSystem string
	if err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer,system_identifier::text FROM pg_catalog.pg_control_system()`).Scan(&postgresVersion, &sourceSystem); err != nil {
		t.Fatal(err)
	}
	if postgresVersion/10000 != 16 {
		t.Fatalf("managed Snowflake recovery gate requires PostgreSQL 16, got server_version_num=%d", postgresVersion)
	}
	t.Logf("live recovery pair: PostgreSQL server_version_num=%d Snowflake CURRENT_VERSION()=%s", postgresVersion, fixture.version)

	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	if _, err := engine.Create(ctx, currentTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	oldFence, err := authorityStore.AcquireProducer(ctx, flowID, "snowflake-old", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	slot := bootstrap.GenerationSlotName(flowID, oldFence.FlowIncarnationID, 1)
	publication := "wallaby_sf_gate_" + resourceSuffix
	sourceSchemaIdent := `"` + sourceSchema + `"`
	sourceQualified := sourceSchemaIdent + `."` + sourceTable + `"`
	publicationIdent := `"` + publication + `"`
	schemaCreated := false
	publicationCreated := false
	slotOwned := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		var cleanupErrors []error
		if slotOwned {
			if _, err := pool.Exec(cleanupCtx, "SELECT pg_catalog.pg_drop_replication_slot($1)", slot); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("drop source slot: %w", err))
			}
		}
		if publicationCreated {
			if _, err := pool.Exec(cleanupCtx, "DROP PUBLICATION "+publicationIdent); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("drop source publication: %w", err))
			}
		}
		if schemaCreated {
			if _, err := pool.Exec(cleanupCtx, "DROP SCHEMA "+sourceSchemaIdent+" CASCADE"); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("drop source schema: %w", err))
			}
		}
		if len(cleanupErrors) > 0 {
			t.Errorf("managed PostgreSQL cleanup: %v", errors.Join(cleanupErrors...))
		}
	})
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+sourceSchemaIdent); err != nil {
		t.Fatalf("create isolated source schema without replacing existing resources: %v", err)
	}
	schemaCreated = true
	if _, err := pool.Exec(ctx, "CREATE TABLE "+sourceQualified+" (id bigint PRIMARY KEY, value text, payload bytea, amount numeric(12,2), active boolean, event_at timestamptz, extra text)"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "CREATE PUBLICATION "+publicationIdent+" FOR TABLE "+sourceQualified+" WITH (publish = 'insert, update, delete')"); err != nil {
		t.Fatal(err)
	}
	publicationCreated = true
	publicationRevision, err := pgsource.PublicationFingerprint(ctx, pool, publication)
	if err != nil {
		t.Fatal(err)
	}
	sourceSpec := connector.Spec{Name: "postgres-managed-snowflake", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "slot": "managed", "publication": publication,
		"managed_profile": connector.ManagedProfilePostgresToSnowflakeSQLV1,
		"create_slot":     "true", "ensure_state": "false", "ensure_publication": "false", "sync_publication": "false",
		"publication_tables": sourceSchema + "." + sourceTable, "publication_revision": publicationRevision,
		"source_system_identifier": sourceSystem, "source_lineage_id": "snowflake-source-lineage",
		"streaming_transactions": "true", "toast_fetch": "off", "max_transaction_records": "1000",
		"max_transaction_bytes": "8388608", "max_transaction_fragments": "64",
	}}
	oldSource := &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}
	if err := oldSource.BindRunFence(oldFence); err != nil {
		t.Fatal(err)
	}
	if err := oldSource.Open(ctx, sourceSpec); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := oldSource.Close(context.Background()); err != nil {
			t.Errorf("close initial PostgreSQL source: %v", err)
		}
	})
	slotOwned = true
	if tables := oldSource.ManagedPostgresPublicationTables(); len(tables) != 1 || tables[0] != sourceSchema+"."+sourceTable {
		t.Fatalf("live managed publication tables=%v", tables)
	}
	liveSchemas := oldSource.ManagedPostgresPublicationSchemas()
	if len(liveSchemas) != 1 {
		t.Fatalf("live managed publication schemas=%d", len(liveSchemas))
	}
	projectedLive := liveSchemas[0]
	projectedLive.Namespace, projectedLive.Name = fixture.schema.Namespace, fixture.schema.Name
	if err := fixture.destination.ValidateManagedSourceSchema(projectedLive); err != nil {
		t.Fatal(err)
	}
	initial, ok := oldSource.InitialCheckpoint()
	if !ok {
		t.Fatal("fenced Snowflake source cut did not expose an initial checkpoint")
	}
	var rootedLSN string
	var sourceResources int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT lsn FROM authoritative_checkpoints WHERE flow_incarnation_id=$1),
  (SELECT count(*) FROM source_resources WHERE flow_incarnation_id=$1 AND resource_kind='slot' AND state='ready')`, oldFence.FlowIncarnationID).Scan(&rootedLSN, &sourceResources); err != nil {
		t.Fatal(err)
	}
	if rootedLSN != initial.LSN || sourceResources != 1 {
		t.Fatalf("fenced source cut checkpoint/resources=%s/%d, want %s/1", rootedLSN, sourceResources, initial.LSN)
	}
	fingerprint, err := connector.DeliveryConfigFingerprint(fixture.spec, "integration-mapping-v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.RegisterDestinationRevision(ctx, oldFence, fixture.spec.Options["destination_revision_id"], fixture.spec.Name, fingerprint); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DELETE FROM destination_revisions WHERE destination_revision_id=$1", fixture.spec.Options["destination_revision_id"]); err != nil {
			t.Errorf("delete destination revision: %v", err)
		}
	})
	if _, err := pool.Exec(ctx, `INSERT INTO `+sourceQualified+`(id,value,payload,amount,active,event_at,extra) VALUES(7,'fenced',decode('0102','hex'),12.34,true,'2026-02-03T04:05:06Z','ready')`); err != nil {
		t.Fatal(err)
	}
	readCtx, readCancel := context.WithTimeout(ctx, time.Minute)
	transaction, err := oldSource.ReadTransaction(readCtx)
	readCancel()
	if err != nil {
		t.Fatal(err)
	}
	oldIntent := snowflakeManagedIntentForFence(t, oldFence, fixture.spec.Options["destination_revision_id"], transaction)
	fixture.destination.SetManagedHooks(snowflake.ManagedHooks{AfterCommit: func() error { return errors.New("lost response after confirmed COMMIT") }})
	if _, err := coordinator.DeliverTransaction(ctx, oldFence, oldIntent, transaction, fixture.destination); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("first fenced delivery error=%v", err)
	}
	fixture.destination.SetManagedHooks(snowflake.ManagedHooks{})
	if err := oldSource.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, oldFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	newFence, err := authorityStore.AcquireProducer(ctx, flowID, "snowflake-new", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	restartSpec := sourceSpec
	restartSpec.Options = cloneTestOptions(sourceSpec.Options)
	restartSpec.Options["create_slot"] = "false"
	restartSpec.Options["start_lsn"] = initial.LSN
	newSource := &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}
	if err := newSource.BindRunFence(newFence); err != nil {
		t.Fatal(err)
	}
	if err := newSource.Open(ctx, restartSpec); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := newSource.Close(context.Background()); err != nil {
			t.Errorf("close restarted PostgreSQL source: %v", err)
		}
	})
	replayCtx, replayCancel := context.WithTimeout(ctx, time.Minute)
	replayed, err := newSource.ReadTransaction(replayCtx)
	replayCancel()
	if err != nil {
		t.Fatal(err)
	}
	originalHash, originalBatch, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	replayHash, replayBatch, err := connector.SourceTransactionIdentity(replayed)
	if err != nil {
		t.Fatal(err)
	}
	if originalHash != replayHash || originalBatch != replayBatch {
		t.Fatalf("WAL replay identity changed: %s/%s != %s/%s", originalHash, originalBatch, replayHash, replayBatch)
	}
	newIntent := snowflakeManagedIntentForFence(t, newFence, fixture.spec.Options["destination_revision_id"], replayed)
	grant, err := coordinator.DeliverTransaction(ctx, newFence, newIntent, replayed, fixture.destination)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.CommitSourceFeedback(ctx, newFence, grant, newSource); err != nil {
		t.Fatal(err)
	}
	var confirmedFlush string
	if err := pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name=$1`, slot).Scan(&confirmedFlush); err != nil {
		t.Fatal(err)
	}
	if confirmedFlush != grant.Checkpoint.LSN {
		t.Fatalf("real source confirmed_flush_lsn=%s, want exact authorized %s", confirmedFlush, grant.Checkpoint.LSN)
	}
	if err := newSource.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.DeliverTransaction(ctx, oldFence, oldIntent, transaction, fixture.destination); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale destination owner error=%v", err)
	}
	if err := coordinator.RecordAckReceipt(ctx, oldFence, grant, grant.Checkpoint.LSN); !errors.Is(err, authority.ErrFenceRejected) {
		t.Fatalf("stale Snowflake ACK receipt error=%v", err)
	}
	var receipts, checkpoints, sourceReceipts int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM delivery_receipts WHERE flow_incarnation_id=$1),
  (SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=$1 AND acquisition_id=$2),
  (SELECT count(*) FROM source_ack_receipts WHERE flow_incarnation_id=$1)`, newFence.FlowIncarnationID, newFence.AcquisitionID).Scan(&receipts, &checkpoints, &sourceReceipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || checkpoints != 1 || sourceReceipts != 1 {
		t.Fatalf("PostgreSQL authority delivery/checkpoint/source receipts=%d/%d/%d, want 1/1/1", receipts, checkpoints, sourceReceipts)
	}
}

type snowflakeManagedFixture struct {
	db               *sql.DB
	provisionDB      *sql.DB
	destination      *snowflake.Destination
	spec             connector.Spec
	schema           connector.Schema
	version          string
	targetQualified  string
	receiptQualified string
}

func newSnowflakeManagedFixture(t *testing.T) *snowflakeManagedFixture {
	t.Helper()
	return newSnowflakeManagedFixtureForFlow(t, "snowflake-flow")
}

func newSnowflakeManagedFixtureForFlow(t *testing.T, flowID string) *snowflakeManagedFixture {
	t.Helper()
	return newSnowflakeManagedFixtureForFlowSource(t, flowID, "public", "widgets")
}

func newSnowflakeManagedFixtureForFlowSource(t *testing.T, flowID, sourceSchema, sourceTable string) *snowflakeManagedFixture {
	t.Helper()
	if os.Getenv("WALLABY_TEST_SNOWFLAKE_MANAGED") != "1" {
		t.Skip("set WALLABY_TEST_SNOWFLAKE_MANAGED=1 with a real Snowflake account; fakesnow is not promotion evidence")
	}
	if usingFakesnow() {
		t.Skip("managed Snowflake profile requires real hybrid-table transaction and recovery evidence")
	}
	dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_DSN"))
	provisionDSN := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_PROVISION_DSN"))
	expectedVersion := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_VERSION"))
	expectedRegion := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_REGION"))
	expectedOwnerRole := strings.ToUpper(strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_OWNER_ROLE")))
	for name, value := range map[string]string{
		"WALLABY_TEST_SNOWFLAKE_DSN":           dsn,
		"WALLABY_TEST_SNOWFLAKE_PROVISION_DSN": provisionDSN,
		"WALLABY_TEST_SNOWFLAKE_VERSION":       expectedVersion,
		"WALLABY_TEST_SNOWFLAKE_REGION":        expectedRegion,
		"WALLABY_TEST_SNOWFLAKE_OWNER_ROLE":    expectedOwnerRole,
	} {
		if value == "" {
			t.Fatalf("%s is required when WALLABY_TEST_SNOWFLAKE_MANAGED=1", name)
		}
	}
	parsed, err := gosnowflake.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	provisionParsed, err := gosnowflake.ParseDSN(provisionDSN)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Authenticator != gosnowflake.AuthTypeJwt || parsed.PrivateKey == nil || provisionParsed.Authenticator != gosnowflake.AuthTypeJwt || provisionParsed.PrivateKey == nil {
		t.Fatal("managed Snowflake live gate requires key-pair JWT for execution and provisioning DSNs")
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	provisionDB, err := sql.Open("snowflake", provisionDSN)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = provisionDB.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	if err := provisionDB.PingContext(ctx); err != nil {
		_ = provisionDB.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	var account, database, schemaName, role, warehouse, version, region string
	if err := db.QueryRowContext(ctx, `SELECT CURRENT_ACCOUNT_NAME(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_VERSION(), CURRENT_REGION()`).Scan(&account, &database, &schemaName, &role, &warehouse, &version, &region); err != nil {
		_ = provisionDB.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	var provisionAccount, provisionDatabase, provisionSchema, ownerRole string
	if err := provisionDB.QueryRowContext(ctx, `SELECT CURRENT_ACCOUNT_NAME(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE()`).Scan(&provisionAccount, &provisionDatabase, &provisionSchema, &ownerRole); err != nil {
		_ = provisionDB.Close()
		_ = db.Close()
		t.Fatal(err)
	}
	if version != expectedVersion || region != expectedRegion || !strings.HasPrefix(strings.ToUpper(region), "AWS_") {
		t.Fatalf("Snowflake reviewed runtime mismatch: version=%q/%q region=%q/%q (commercial AWS required)", version, expectedVersion, region, expectedRegion)
	}
	if strings.ToUpper(ownerRole) != expectedOwnerRole || strings.EqualFold(ownerRole, role) {
		t.Fatalf("Snowflake owner/execution roles=%q/%q, want distinct owner %q", ownerRole, role, expectedOwnerRole)
	}
	for name, values := range map[string][2]string{
		"account": {parsed.Account, account}, "database": {parsed.Database, database}, "schema": {parsed.Schema, schemaName},
		"role": {parsed.Role, role}, "warehouse": {parsed.Warehouse, warehouse},
		"provision account": {provisionAccount, account}, "provision database": {provisionDatabase, database},
		"provision schema": {provisionSchema, schemaName},
	} {
		if !strings.EqualFold(values[0], values[1]) {
			_ = provisionDB.Close()
			_ = db.Close()
			t.Fatalf("managed Snowflake %s=%q must exactly select live %q", name, values[0], values[1])
		}
	}
	t.Logf("real Snowflake deployment: region=%s CURRENT_VERSION()=%s gosnowflake=%s auth=key-pair-jwt", region, version, gosnowflake.SnowflakeGoDriverVersion)

	suffix := strings.ToUpper(strconv.FormatInt(time.Now().UnixNano(), 36))
	target := "WALLABY_SF_MANAGED_" + suffix
	receipts := "WALLABY_SF_RECEIPTS_" + suffix
	revision := "snowflake-managed-" + strings.ToLower(suffix)
	schema := snowflakeManagedSchema()
	schema.Namespace = strings.ToUpper(schemaName)
	schema.Name = target
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	schemaHash, err := snowflake.ManagedSchemaContractHash(schema)
	if err != nil {
		t.Fatal(err)
	}
	targetQualified := quoteSnowflakeIdent(database) + "." + quoteSnowflakeIdent(schemaName) + "." + quoteSnowflakeIdent(target)
	receiptQualified := quoteSnowflakeIdent(database) + "." + quoteSnowflakeIdent(schemaName) + "." + quoteSnowflakeIdent(receipts)
	destination := &snowflake.Destination{}
	targetCreated := false
	receiptCreated := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		var cleanupErrors []error
		if err := destination.Close(cleanupCtx); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("close managed destination: %w", err))
		}
		if receiptCreated {
			if _, err := provisionDB.ExecContext(cleanupCtx, "DROP TABLE "+receiptQualified); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("drop receipt table: %w", err))
			}
		}
		if targetCreated {
			if _, err := provisionDB.ExecContext(cleanupCtx, "DROP TABLE "+targetQualified); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("drop target table: %w", err))
			}
		}
		if err := provisionDB.Close(); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("close provisioning connection: %w", err))
		}
		if err := db.Close(); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("close execution connection: %w", err))
		}
		if len(cleanupErrors) > 0 {
			t.Errorf("managed Snowflake cleanup: %v", errors.Join(cleanupErrors...))
		}
	})
	targetComment := snowflakeManagedOwnershipComment("target", revision, schemaHash, flowID)
	receiptComment := snowflakeManagedOwnershipComment("receipts", revision, schemaHash, flowID)
	createTarget := fmt.Sprintf(`CREATE HYBRID TABLE %s (
  "ID" NUMBER(38,0) NOT NULL,
  "VALUE" VARCHAR,
  "PAYLOAD" BINARY(8388608),
  "AMOUNT" NUMBER(12,2),
  "ACTIVE" BOOLEAN,
  "EVENT_AT" TIMESTAMP_TZ,
  "EXTRA" VARCHAR,
  CONSTRAINT "PK_%s" PRIMARY KEY ("ID")
) COMMENT = '%s'`, targetQualified, suffix, targetComment)
	createReceipts := fmt.Sprintf(`CREATE HYBRID TABLE %s (
  "PROFILE_VERSION" VARCHAR NOT NULL,
  "FLOW_ID" VARCHAR NOT NULL,
  "FLOW_INCARNATION_ID" VARCHAR NOT NULL,
  "SOURCE_LINEAGE_ID" VARCHAR NOT NULL,
  "DESTINATION_REVISION_ID" VARCHAR NOT NULL,
  "LOGICAL_BATCH_ID" VARCHAR NOT NULL,
  "POSITION_ID" VARCHAR NOT NULL,
  "CONTENT_HASH" VARCHAR NOT NULL,
  "SCHEMA_CONTRACT_HASH" VARCHAR NOT NULL,
  "CATALOG_FINGERPRINT" VARCHAR NOT NULL,
  "MANIFEST_HASH" VARCHAR NOT NULL,
  "EXTERNAL_ID" VARCHAR NOT NULL,
  "GENERATION" NUMBER(38,0) NOT NULL,
  "ACQUISITION_ID" VARCHAR NOT NULL,
  "LEASE_EPOCH" NUMBER(38,0) NOT NULL,
  "TRANSACTION_ID" NUMBER(38,0) NOT NULL,
  "FRAGMENT_COUNT" NUMBER(38,0) NOT NULL,
  "RECORD_COUNT" NUMBER(38,0) NOT NULL,
  "COMMITTED_AT" TIMESTAMP_TZ NOT NULL,
  CONSTRAINT "PK_RECEIPT_%s" PRIMARY KEY ("FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "SOURCE_LINEAGE_ID", "POSITION_ID"),
  CONSTRAINT "UQ_LOGICAL_%s" UNIQUE ("FLOW_INCARNATION_ID", "DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID"),
  CONSTRAINT "UQ_EXTERNAL_%s" UNIQUE ("EXTERNAL_ID")
) COMMENT = '%s'`, receiptQualified, suffix, suffix, suffix, receiptComment)
	for index, statement := range []string{createTarget, createReceipts} {
		if _, err := provisionDB.ExecContext(ctx, statement); err != nil {
			t.Fatalf("provision managed Snowflake hybrid table without replacing existing resources: %v\n%s", err, statement)
		}
		if index == 0 {
			targetCreated = true
		} else {
			receiptCreated = true
		}
	}
	for _, statement := range []string{
		"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE " + targetQualified + " TO ROLE " + quoteSnowflakeIdent(role),
		"GRANT SELECT, INSERT ON TABLE " + receiptQualified + " TO ROLE " + quoteSnowflakeIdent(role),
	} {
		if _, err := provisionDB.ExecContext(ctx, statement); err != nil {
			t.Fatalf("grant managed Snowflake execution role: %v\n%s", err, statement)
		}
	}
	const catalogTimestampFormat = `YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM`
	var targetCreatedOn, receiptsCreatedOn string
	createdQuery := "SELECT TO_VARCHAR(CREATED, '" + catalogTimestampFormat + "') FROM " + quoteSnowflakeIdent(database) + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?"
	if err := provisionDB.QueryRowContext(ctx, createdQuery, strings.ToUpper(schemaName), target).Scan(&targetCreatedOn); err != nil {
		t.Fatal(err)
	}
	if err := provisionDB.QueryRowContext(ctx, createdQuery, strings.ToUpper(schemaName), receipts).Scan(&receiptsCreatedOn); err != nil {
		t.Fatal(err)
	}
	spec := connector.Spec{Name: "snowflake-managed", Type: connector.EndpointSnowflake, Options: map[string]string{
		"dsn": dsn, "flow_id": flowID, "managed_profile": connector.ManagedProfilePostgresToSnowflakeSQLV1,
		"destination_revision_id": revision, "batch_mode": "target", "batch_resolution": "none",
		"meta_table_enabled": "false", "disable_transactions": "false", "session_keep_alive": "false",
		"managed_account": strings.ToUpper(account), "managed_database": strings.ToUpper(database), "managed_schema": strings.ToUpper(schemaName),
		"managed_table": target, "managed_receipts_table": receipts, "managed_owner_role": strings.ToUpper(ownerRole),
		"managed_execution_role": strings.ToUpper(role), "managed_warehouse": strings.ToUpper(warehouse), "managed_snowflake_version": expectedVersion,
		"managed_target_created_on": targetCreatedOn, "managed_receipts_created_on": receiptsCreatedOn,
		"managed_source_schema": sourceSchema, "managed_source_table": sourceTable,
		"managed_schema_contract": string(schemaJSON), "managed_schema_contract_hash": schemaHash,
		"managed_max_transaction_rows": "1000", "managed_max_transaction_bytes": "8388608",
		"managed_max_transaction_fragments": "64", "managed_max_open_conns": "4",
		"managed_statement_timeout_seconds": "120", "managed_hybrid_table_lock_timeout_seconds": "60",
	}}
	if err := destination.Open(ctx, spec); err != nil {
		t.Fatalf("open managed Snowflake destination: %v", err)
	}
	return &snowflakeManagedFixture{db: db, provisionDB: provisionDB, destination: destination, spec: spec, schema: schema, version: version, targetQualified: targetQualified, receiptQualified: receiptQualified}
}

func snowflakeManagedOwnershipComment(kind, revision, schemaHash, flowID string) string {
	flowDigest := sha256.Sum256([]byte(flowID))
	return fmt.Sprintf("wallaby:%s:%s:%s:%s:%s", connector.ManagedProfilePostgresToSnowflakeSQLV1, kind, revision, schemaHash, hex.EncodeToString(flowDigest[:]))
}

func snowflakeManagedSchema() connector.Schema {
	known := map[string]string{"nullability_known": "true", "generated_known": "true"}
	return connector.Schema{Name: "widgets", Namespace: "public", Version: 1, Columns: []connector.Column{
		{Name: "id", Type: "int8", TypeMetadata: map[string]string{"primary_key": "true", "nullability_known": "true", "generated_known": "true"}},
		{Name: "value", Type: "text", Nullable: true, TypeMetadata: cloneTestOptions(known)},
		{Name: "payload", Type: "bytea", Nullable: true, TypeMetadata: cloneTestOptions(known)},
		{Name: "amount", Type: "numeric(12,2)", Nullable: true, TypeMetadata: cloneTestOptions(known)},
		{Name: "active", Type: "boolean", Nullable: true, TypeMetadata: cloneTestOptions(known)},
		{Name: "event_at", Type: "timestamptz", Nullable: true, TypeMetadata: cloneTestOptions(known)},
		{Name: "extra", Type: "text", Nullable: true, TypeMetadata: cloneTestOptions(known)},
	}}
}

func snowflakeManagedInsertTransaction(schema connector.Schema, id int64, value string) connector.SourceTransaction {
	after := make(map[string]any, len(schema.Columns))
	for _, column := range schema.Columns {
		after[column.Name] = nil
	}
	after["id"] = id
	after["value"] = value
	return snowflakeManagedTransaction(schema, uint32(id+100), []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{Schema: schema, Records: []connector.Record{{
		Table: "widgets", Operation: connector.OpInsert, Key: mustRecordKey(map[string]any{"id": id}), After: after,
	}}}}})
}

func snowflakeManagedTransaction(schema connector.Schema, xid uint32, fragments []connector.TransactionFragment) connector.SourceTransaction {
	lsn := fmt.Sprintf("0/%X", 0x100+uint64(xid)*0x10)
	return connector.SourceTransaction{SourceLineageID: "snowflake-source-lineage", TransactionID: xid,
		BeginLSN: lsn, CommitLSN: lsn, EndLSN: lsn, Fragments: fragments,
		Checkpoint: connector.Checkpoint{LSN: lsn, Timestamp: time.Unix(int64(xid), 0).UTC()},
	}
}

func snowflakeManagedIntent(t *testing.T, revision string, transaction connector.SourceTransaction, generation int64, acquisition string) connector.DeliveryIntent {
	t.Helper()
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	position, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{FlowID: "snowflake-flow", FlowIncarnationID: "11111111-1111-1111-1111-111111111111",
		SourceLineageID: transaction.SourceLineageID, Generation: generation, AcquisitionID: acquisition, LeaseEpoch: generation,
		DestinationRevisionID: revision, LogicalBatchID: logicalBatchID, PositionID: position, ContentHash: contentHash}
}

func snowflakeManagedIntentForFence(t *testing.T, fence authority.RunFence, revision string, transaction connector.SourceTransaction) connector.DeliveryIntent {
	intent := snowflakeManagedIntent(t, revision, transaction, fence.Generation, fence.AcquisitionID.String())
	intent.FlowID = fence.FlowID
	intent.FlowIncarnationID = fence.FlowIncarnationID.String()
	intent.LeaseEpoch = fence.LeaseEpoch
	return intent
}

func cloneTestOptions(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func mustRecordKey(key map[string]any) []byte {
	encoded, _ := json.Marshal(key)
	return encoded
}
