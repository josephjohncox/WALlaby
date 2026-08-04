package tests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	postgrescodec "github.com/josephjohncox/wallaby/internal/postgres"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestManagedBootstrapLiveAdmissionMatrix(t *testing.T) {
	ctx, dsn, engine, pool, authorityStore := setupBootstrapControl(t)
	defer engine.Close()
	defer pool.Close()
	_ = newLiveBootstrapper(t, ctx, pool, dsn)

	var sourceSystem string
	if err := pool.QueryRow(ctx, `SELECT system_identifier::text FROM pg_catalog.pg_control_system()`).Scan(&sourceSystem); err != nil {
		t.Fatal(err)
	}

	t.Run("pool_max_conns_rejected_before_connector_side_effects", func(t *testing.T) {
		prefix := managedLivePrefix(t)
		sourceTable := prefix + "_source"
		targetSchema := prefix + "_target"
		publication := prefix + "_publication"
		flowID := prefix + "-flow"
		createAdmissionTables(t, ctx, pool, sourceTable, targetSchema, false, "")
		defer cleanupAdmissionObjects(context.Background(), pool, publication, sourceTable, targetSchema)
		defer cleanupAuthorityTest(context.Background(), pool, flowID)

		before := admissionSideEffects(t, ctx, pool, flowID, publication, targetSchema, sourceTable)
		err := runManagedAdmissionFlow(ctx, dsn, engine, pool, authorityStore, flowID, sourceSystem, publication, sourceTable, targetSchema, map[string]string{"pool_max_conns": "1"})
		if err == nil || !strings.Contains(err.Error(), "pool_max_conns>=2 before connector side effects") {
			t.Fatalf("managed runner error=%v, want pre-connector pool admission rejection", err)
		}
		after := admissionSideEffects(t, ctx, pool, flowID, publication, targetSchema, sourceTable)
		if after != before {
			t.Fatalf("pool admission changed source/destination evidence: before=%+v after=%+v", before, after)
		}
	})

	for _, relationKind := range []string{"partitioned_root", "partition_leaf"} {
		relationKind := relationKind
		t.Run(relationKind+"_rejected_before_source_bootstrap_side_effects", func(t *testing.T) {
			prefix := managedLivePrefix(t)
			rootTable := prefix + "_root"
			leafTable := prefix + "_leaf"
			targetSchema := prefix + "_target"
			publication := prefix + "_publication"
			flowID := prefix + "-flow"
			selectedTable := rootTable
			if relationKind == "partition_leaf" {
				selectedTable = leafTable
			}
			createPartitionAdmissionTables(t, ctx, pool, rootTable, leafTable, targetSchema, selectedTable)
			defer cleanupAdmissionObjects(context.Background(), pool, publication, rootTable, targetSchema)
			defer cleanupAuthorityTest(context.Background(), pool, flowID)

			before := admissionSideEffects(t, ctx, pool, flowID, publication, targetSchema, selectedTable)
			err := runManagedAdmissionFlow(ctx, dsn, engine, pool, authorityStore, flowID, sourceSystem, publication, selectedTable, targetSchema, nil)
			if err == nil || !strings.Contains(err.Error(), "does not support partitioned or partition relations") {
				t.Fatalf("managed runner error=%v, want partition admission rejection", err)
			}
			after := admissionSideEffects(t, ctx, pool, flowID, publication, targetSchema, selectedTable)
			if after.Publications != before.Publications || after.Slots != before.Slots || after.Bootstraps != before.Bootstraps || after.Resources != before.Resources {
				t.Fatalf("partition admission crossed source bootstrap boundary: before=%+v after=%+v", before, after)
			}
			if after.TargetRows != before.TargetRows || after.StageTables != before.StageTables || after.StageManifests != before.StageManifests {
				t.Fatalf("partition admission changed destination target/staging: before=%+v after=%+v", before, after)
			}
		})
	}

	for _, direction := range []string{"outbound", "inbound"} {
		direction := direction
		t.Run(direction+"_foreign_key_rejected_before_committed_staging", func(t *testing.T) {
			prefix := managedLivePrefix(t)
			sourceTable := prefix + "_events"
			targetSchema := prefix + "_target"
			publication := prefix + "_publication"
			flowID := prefix + "-flow"
			createAdmissionTables(t, ctx, pool, sourceTable, targetSchema, true, direction)
			defer cleanupAdmissionObjects(context.Background(), pool, publication, sourceTable, targetSchema)
			defer cleanupAuthorityTest(context.Background(), pool, flowID)

			before := admissionSideEffects(t, ctx, pool, flowID, publication, targetSchema, sourceTable)
			err := runManagedAdmissionFlow(ctx, dsn, engine, pool, authorityStore, flowID, sourceSystem, publication, sourceTable, targetSchema, nil)
			if err == nil || !strings.Contains(err.Error(), "does not support FK-connected destination target table") {
				t.Fatalf("managed runner error=%v, want %s FK rejection", err, direction)
			}
			after := admissionSideEffects(t, ctx, pool, flowID, publication, targetSchema, sourceTable)
			if after.TargetRows != before.TargetRows || after.StageTables != before.StageTables || after.StageManifests != before.StageManifests {
				t.Fatalf("%s FK rejection committed destination staging: before=%+v after=%+v", direction, before, after)
			}
		})
	}
}

func TestCanonicalPublicationFingerprintLive(t *testing.T) {
	ctx, dsn, engine, pool, authorityStore := setupBootstrapControl(t)
	defer engine.Close()
	defer pool.Close()

	var serverVersion int
	if err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&serverVersion); err != nil {
		t.Fatal(err)
	}
	var sourceSystem, databaseName string
	if err := pool.QueryRow(ctx, `SELECT system_identifier::text,current_database() FROM pg_catalog.pg_control_system()`).Scan(&sourceSystem, &databaseName); err != nil {
		t.Fatal(err)
	}

	type publicationCase struct {
		name       string
		minVersion int
		mode       string
		payload    string
		desired    int
	}
	cases := []publicationCase{
		{name: "publish_via_partition_root", minVersion: 130000, mode: "via_partition_root", desired: 1},
		{name: "publish_insert", mode: "publish_flags", payload: "update, delete, truncate", desired: 1},
		{name: "publish_update", mode: "publish_flags", payload: "insert, delete, truncate", desired: 1},
		{name: "publish_delete", mode: "publish_flags", payload: "insert, update, truncate", desired: 1},
		{name: "publish_truncate", mode: "publish_flags", payload: "insert, update, delete", desired: 1},
		{name: "row_filter", minVersion: 150000, mode: "row_filter", desired: 1},
		{name: "column_list", minVersion: 150000, mode: "column_list", desired: 1},
		{name: "extra_relation", mode: "extra_relation", desired: 1},
		{name: "missing_relation", mode: "exact", desired: 2},
	}

	for _, exact := range []struct {
		name      string
		precreate bool
		owned     bool
	}{{name: "exact_created", owned: true}, {name: "exact_adopted", precreate: true, owned: false}} {
		exact := exact
		t.Run(exact.name, func(t *testing.T) {
			prefix := managedLivePrefix(t)
			publication := prefix + "_publication"
			table := prefix + "_table"
			flowID := prefix + "-flow"
			createPublicationTables(t, ctx, pool, table, "")
			defer cleanupPublicationObjects(context.Background(), pool, publication, table, "")
			defer cleanupAuthorityTest(context.Background(), pool, flowID)
			if exact.precreate {
				execLiveDDL(t, ctx, pool, "create_publication", publication, table, "", "exact", "")
			}
			fence := createManagedResourceFence(t, ctx, engine, authorityStore, flowID, dsn)
			defer func() { _ = authorityStore.FinishProducer(context.Background(), fence, "test_complete") }()
			relations := livePublicationRelations(t, ctx, pool, table)
			expected := bootstrap.ExpectedPublicationRevision(publication, relations)
			coordinator := newLiveBootstrapper(t, ctx, pool, dsn)
			resource, err := coordinator.EnsurePublication(ctx, fence, bootstrap.ExportedSnapshot{SourceSystem: sourceSystem, DatabaseName: databaseName}, publication, expected, relations, true)
			if err != nil {
				t.Fatal(err)
			}
			if resource.Owned != exact.owned {
				t.Fatalf("publication ownership=%t, want %t", resource.Owned, exact.owned)
			}
			actual, err := postgrescodec.LivePublicationFingerprint(ctx, pool, publication)
			if err != nil {
				t.Fatal(err)
			}
			if actual != expected || resource.Revision != expected {
				t.Fatalf("publication revisions actual=%s resource=%s expected=%s", actual, resource.Revision, expected)
			}
		})
	}

	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name+"_sensitive_and_rejected", func(t *testing.T) {
			if serverVersion < testCase.minVersion {
				t.Skipf("PostgreSQL server_version_num=%d lacks %s publication syntax (requires %d)", serverVersion, testCase.name, testCase.minVersion)
			}
			prefix := managedLivePrefix(t)
			publication := prefix + "_publication"
			firstTable := prefix + "_first"
			secondTable := prefix + "_second"
			flowID := prefix + "-flow"
			createPublicationTables(t, ctx, pool, firstTable, secondTable)
			defer cleanupPublicationObjects(context.Background(), pool, publication, firstTable, secondTable)
			defer cleanupAuthorityTest(context.Background(), pool, flowID)
			execLiveDDL(t, ctx, pool, "create_publication", publication, firstTable, secondTable, testCase.mode, testCase.payload)

			fence := createManagedResourceFence(t, ctx, engine, authorityStore, flowID, dsn)
			defer func() { _ = authorityStore.FinishProducer(context.Background(), fence, "test_complete") }()
			desiredRelations := livePublicationRelations(t, ctx, pool, firstTable)
			if testCase.desired == 2 {
				desiredRelations = append(desiredRelations, livePublicationRelations(t, ctx, pool, secondTable)...)
			}
			expected := bootstrap.ExpectedPublicationRevision(publication, desiredRelations)
			actual, err := postgrescodec.LivePublicationFingerprint(ctx, pool, publication)
			if err != nil {
				t.Fatal(err)
			}
			if actual == expected {
				t.Fatalf("%s did not change canonical fingerprint %s", testCase.name, actual)
			}
			coordinator := newLiveBootstrapper(t, ctx, pool, dsn)
			_, err = coordinator.EnsurePublication(ctx, fence, bootstrap.ExportedSnapshot{SourceSystem: sourceSystem, DatabaseName: databaseName}, publication, expected, desiredRelations, false)
			if err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
				t.Fatalf("EnsurePublication error=%v, want publication semantics conflict", err)
			}
		})
	}
}

func TestManagedTerminalStopOwnershipLive(t *testing.T) {
	ctx, dsn, engine, pool, authorityStore := setupBootstrapControl(t)
	defer engine.Close()
	defer pool.Close()
	prefix := managedLivePrefix(t)
	table := prefix + "_table"
	adoptedPublication := prefix + "_adopted_pub"
	ownedPublication := prefix + "_owned_pub"
	adoptedSlot := prefix + "_adopted_slot"
	ownedFlowID := prefix + "-owned-slot"
	adoptedFlowID := prefix + "-adopted-slot"
	var ownedSlot string
	_, _ = pool.Exec(ctx, `SELECT pg_catalog.pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)`, adoptedSlot)
	execLiveDDLNoFail(ctx, pool, "drop_publication", adoptedPublication, "", "", "", "")
	execLiveDDLNoFail(ctx, pool, "drop_publication", ownedPublication, "", "", "", "")
	createPublicationTables(t, ctx, pool, table, "")
	execLiveDDL(t, ctx, pool, "create_publication", adoptedPublication, table, "", "exact", "")
	if _, err := pool.Exec(ctx, `SELECT * FROM pg_catalog.pg_create_logical_replication_slot($1,'pgoutput')`, adoptedSlot); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name=ANY($1::text[])`, []string{adoptedSlot, ownedSlot})
		cleanupPublicationObjects(context.Background(), pool, adoptedPublication, table, "")
		execLiveDDLNoFail(context.Background(), pool, "drop_publication", ownedPublication, "", "", "", "")
		cleanupAuthorityTest(context.Background(), pool, ownedFlowID)
		cleanupAuthorityTest(context.Background(), pool, adoptedFlowID)
	}()

	var sourceSystem, databaseName string
	if err := pool.QueryRow(ctx, `SELECT system_identifier::text,current_database() FROM pg_catalog.pg_control_system()`).Scan(&sourceSystem, &databaseName); err != nil {
		t.Fatal(err)
	}
	relations := livePublicationRelations(t, ctx, pool, table)
	coordinator := newLiveBootstrapper(t, ctx, pool, dsn)

	// Flow A owns its generated slot and adopts the pre-existing publication.
	ownedSlotFence := createManagedResourceFence(t, ctx, engine, authorityStore, ownedFlowID, dsn)
	adoptedRevision := bootstrap.ExpectedPublicationRevision(adoptedPublication, relations)
	adoptedPubResource, err := coordinator.EnsurePublication(ctx, ownedSlotFence, bootstrap.ExportedSnapshot{SourceSystem: sourceSystem, DatabaseName: databaseName}, adoptedPublication, adoptedRevision, relations, false)
	if err != nil {
		t.Fatal(err)
	}
	if adoptedPubResource.Owned {
		t.Fatal("pre-existing publication was not recorded as adopted")
	}
	ownedSlotSession, err := coordinator.Start(ctx, ownedSlotFence, adoptedPublication, "terminal-ownership-manifest")
	if err != nil {
		t.Fatal(err)
	}
	ownedSlot = ownedSlotSession.Snapshot.SlotName
	if err := ownedSlotSession.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := authorityStore.FinishProducer(ctx, ownedSlotFence, "test_setup_complete"); err != nil {
		t.Fatal(err)
	}

	// Flow B owns the publication created through the coordinator and adopts a
	// pre-existing logical slot represented by an exact ready authority row.
	adoptedSlotFence := createManagedResourceFence(t, ctx, engine, authorityStore, adoptedFlowID, dsn)
	ownedRevision := bootstrap.ExpectedPublicationRevision(ownedPublication, relations)
	ownedPubResource, err := coordinator.EnsurePublication(ctx, adoptedSlotFence, bootstrap.ExportedSnapshot{SourceSystem: sourceSystem, DatabaseName: databaseName}, ownedPublication, ownedRevision, relations, true)
	if err != nil {
		t.Fatal(err)
	}
	if !ownedPubResource.Owned {
		t.Fatal("new publication was not recorded as owned")
	}
	seedAdoptedSlotResource(t, ctx, pool, adoptedSlotFence, sourceSystem, databaseName, adoptedSlot)
	if err := authorityStore.FinishProducer(ctx, adoptedSlotFence, "test_setup_complete"); err != nil {
		t.Fatal(err)
	}

	lifecycle := workflow.NewOrchestratedEngine(engine, workflow.PassiveDispatcher{}, nil, runner.ManagedSourceCleanup{
		Factory: runner.Factory{ManagedControl: pool, ManagedAuthority: authorityStore}, Authority: authorityStore,
	})
	for _, flowID := range []string{ownedFlowID, adoptedFlowID} {
		paused, err := lifecycle.Pause(ctx, flowID)
		if err != nil || paused.State != flow.StatePaused {
			t.Fatalf("pause %s=(%s,%v)", flowID, paused.State, err)
		}
	}
	assertLiveSourceResource(t, ctx, pool, "slot", ownedSlot, true)
	assertLiveSourceResource(t, ctx, pool, "publication", adoptedPublication, true)
	assertLiveSourceResource(t, ctx, pool, "slot", adoptedSlot, true)
	assertLiveSourceResource(t, ctx, pool, "publication", ownedPublication, true)

	for _, flowID := range []string{ownedFlowID, adoptedFlowID} {
		resumed, err := lifecycle.Resume(ctx, flowID)
		if err != nil || resumed.State != flow.StateRunning {
			t.Fatalf("resume %s=(%s,%v)", flowID, resumed.State, err)
		}
		stopped, err := lifecycle.Stop(ctx, flowID)
		if err != nil || stopped.State != flow.StateStopped {
			t.Fatalf("stop %s=(%s,%v)", flowID, stopped.State, err)
		}
	}

	assertLiveSourceResource(t, ctx, pool, "slot", ownedSlot, false)
	assertLiveSourceResource(t, ctx, pool, "publication", ownedPublication, false)
	assertLiveSourceResource(t, ctx, pool, "slot", adoptedSlot, true)
	assertLiveSourceResource(t, ctx, pool, "publication", adoptedPublication, true)
	var ownedRetired, adoptedReady int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resources WHERE flow_id IN ($1,$2) AND ownership='owned' AND state='retired'`, ownedFlowID, adoptedFlowID).Scan(&ownedRetired); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resources WHERE flow_id IN ($1,$2) AND ownership='adopted' AND state='ready'`, ownedFlowID, adoptedFlowID).Scan(&adoptedReady); err != nil {
		t.Fatal(err)
	}
	if ownedRetired != 2 || adoptedReady != 2 {
		t.Fatalf("terminal ownership rows owned_retired=%d adopted_ready=%d, want 2/2", ownedRetired, adoptedReady)
	}
}

type liveAdmissionEvidence struct {
	Publications   int
	Slots          int
	Bootstraps     int
	Resources      int
	TargetRows     int
	StageTables    int
	StageManifests int
}

func managedLivePrefix(t *testing.T) string {
	t.Helper()
	digest := sha256.Sum256([]byte(t.Name()))
	return "wb_" + hex.EncodeToString(digest[:6])
}

const liveDDLFunction = `
CREATE OR REPLACE FUNCTION pg_temp.wallaby_live_ddl(
  operation text,publication_name text,first_table text,second_table text,target_schema text,payload text
) RETURNS void LANGUAGE plpgsql AS $function$
BEGIN
  CASE operation
  WHEN 'create_admission' THEN
    EXECUTE format('CREATE TABLE public.%I(id bigint PRIMARY KEY,value text NOT NULL)',first_table);
    EXECUTE format('INSERT INTO public.%I VALUES(1,''source'')',first_table);
    EXECUTE format('CREATE SCHEMA %I',target_schema);
    EXECUTE format('CREATE TABLE %I.%I(id bigint PRIMARY KEY,value text NOT NULL)',target_schema,first_table);
    EXECUTE format('INSERT INTO %I.%I VALUES(99,''sentinel'')',target_schema,first_table);
  WHEN 'add_fk' THEN
    EXECUTE format('CREATE TABLE %I.%I(id bigint PRIMARY KEY,target_id bigint)',target_schema,first_table || '_peer');
    IF payload='outbound' THEN
      EXECUTE format('ALTER TABLE %I.%I ADD COLUMN peer_id bigint REFERENCES %I.%I(id)',target_schema,first_table,target_schema,first_table || '_peer');
    ELSIF payload='inbound' THEN
      EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I FOREIGN KEY(target_id) REFERENCES %I.%I(id)',target_schema,first_table || '_peer',first_table || '_fk',target_schema,first_table);
    ELSE
      RAISE EXCEPTION 'unsupported FK direction %',payload;
    END IF;
  WHEN 'create_partition_admission' THEN
    EXECUTE format('CREATE TABLE public.%I(id bigint PRIMARY KEY,value text NOT NULL) PARTITION BY RANGE(id)',first_table);
    EXECUTE format('CREATE TABLE public.%I PARTITION OF public.%I FOR VALUES FROM (0) TO (100)',second_table,first_table);
    EXECUTE format('INSERT INTO public.%I VALUES(1,''source'')',first_table);
    EXECUTE format('CREATE SCHEMA %I',target_schema);
    EXECUTE format('CREATE TABLE %I.%I(id bigint PRIMARY KEY,value text NOT NULL)',target_schema,payload);
    EXECUTE format('INSERT INTO %I.%I VALUES(99,''sentinel'')',target_schema,payload);
  WHEN 'create_publication_tables' THEN
    EXECUTE format('CREATE TABLE public.%I(id bigint PRIMARY KEY,value text NOT NULL)',first_table);
    IF second_table<>'' THEN
      EXECUTE format('CREATE TABLE public.%I(id bigint PRIMARY KEY,value text NOT NULL)',second_table);
    END IF;
  WHEN 'create_publication' THEN
    IF target_schema='exact' THEN
      EXECUTE format('CREATE PUBLICATION %I FOR TABLE public.%I',publication_name,first_table);
    ELSIF target_schema='via_partition_root' THEN
      EXECUTE format('CREATE PUBLICATION %I FOR TABLE public.%I WITH (publish_via_partition_root=true)',publication_name,first_table);
    ELSIF target_schema='row_filter' THEN
      EXECUTE format('CREATE PUBLICATION %I FOR TABLE public.%I WHERE (id > 0)',publication_name,first_table);
    ELSIF target_schema='column_list' THEN
      EXECUTE format('CREATE PUBLICATION %I FOR TABLE public.%I (id)',publication_name,first_table);
    ELSIF target_schema='extra_relation' THEN
      EXECUTE format('CREATE PUBLICATION %I FOR TABLE public.%I,public.%I',publication_name,first_table,second_table);
    ELSIF target_schema='publish_flags' AND payload='update, delete, truncate' THEN
      EXECUTE format('CREATE PUBLICATION %I FOR TABLE public.%I WITH (publish=''update, delete, truncate'')',publication_name,first_table);
    ELSIF target_schema='publish_flags' AND payload='insert, delete, truncate' THEN
      EXECUTE format('CREATE PUBLICATION %I FOR TABLE public.%I WITH (publish=''insert, delete, truncate'')',publication_name,first_table);
    ELSIF target_schema='publish_flags' AND payload='insert, update, truncate' THEN
      EXECUTE format('CREATE PUBLICATION %I FOR TABLE public.%I WITH (publish=''insert, update, truncate'')',publication_name,first_table);
    ELSIF target_schema='publish_flags' AND payload='insert, update, delete' THEN
      EXECUTE format('CREATE PUBLICATION %I FOR TABLE public.%I WITH (publish=''insert, update, delete'')',publication_name,first_table);
    ELSE
      RAISE EXCEPTION 'unsupported publication mode % / %',target_schema,payload;
    END IF;
  WHEN 'cleanup_admission' THEN
    IF publication_name<>'' THEN EXECUTE format('DROP PUBLICATION IF EXISTS %I',publication_name); END IF;
    IF target_schema<>'' THEN EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE',target_schema); END IF;
    IF first_table<>'' THEN EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE',first_table); END IF;
  WHEN 'cleanup_publication' THEN
    IF publication_name<>'' THEN EXECUTE format('DROP PUBLICATION IF EXISTS %I',publication_name); END IF;
    IF first_table<>'' THEN EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE',first_table); END IF;
    IF second_table<>'' THEN EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE',second_table); END IF;
  WHEN 'drop_publication' THEN
    EXECUTE format('DROP PUBLICATION IF EXISTS %I',publication_name);
  ELSE
    RAISE EXCEPTION 'unsupported live DDL operation %',operation;
  END CASE;
END
$function$`

func execLiveDDL(t *testing.T, ctx context.Context, pool *pgxpool.Pool, operation, publication, firstTable, secondTable, targetSchema, payload string) {
	t.Helper()
	if err := execLiveDDLError(ctx, pool, operation, publication, firstTable, secondTable, targetSchema, payload); err != nil {
		t.Fatal(err)
	}
}

func execLiveDDLNoFail(ctx context.Context, pool *pgxpool.Pool, operation, publication, firstTable, secondTable, targetSchema, payload string) {
	_ = execLiveDDLError(ctx, pool, operation, publication, firstTable, secondTable, targetSchema, payload)
}

func execLiveDDLError(ctx context.Context, pool *pgxpool.Pool, operation, publication, firstTable, secondTable, targetSchema, payload string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, liveDDLFunction); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `SELECT pg_temp.wallaby_live_ddl($1,$2,$3,$4,$5,$6)`, operation, publication, firstTable, secondTable, targetSchema, payload)
	return err
}

func liveTargetRowCount(ctx context.Context, pool *pgxpool.Pool, schema, table string) (int, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `
CREATE OR REPLACE FUNCTION pg_temp.wallaby_live_count(schema_name text,table_name text)
RETURNS bigint LANGUAGE plpgsql AS $function$
DECLARE result bigint;
BEGIN
  EXECUTE format('SELECT count(*) FROM %I.%I',schema_name,table_name) INTO result;
  RETURN result;
END
$function$`); err != nil {
		return 0, err
	}
	var count int
	err = conn.QueryRow(ctx, `SELECT pg_temp.wallaby_live_count($1,$2)`, schema, table).Scan(&count)
	return count, err
}

func createAdmissionTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceTable, targetSchema string, foreignKey bool, direction string) {
	t.Helper()
	cleanupAdmissionObjects(ctx, pool, "", sourceTable, targetSchema)
	execLiveDDL(t, ctx, pool, "create_admission", "", sourceTable, "", targetSchema, "")
	if foreignKey {
		if direction != "outbound" && direction != "inbound" {
			t.Fatalf("unknown FK direction %q", direction)
		}
		execLiveDDL(t, ctx, pool, "add_fk", "", sourceTable, "", targetSchema, direction)
	}
}

func createPartitionAdmissionTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rootTable, leafTable, targetSchema, selectedTable string) {
	t.Helper()
	cleanupAdmissionObjects(ctx, pool, "", rootTable, targetSchema)
	execLiveDDL(t, ctx, pool, "create_partition_admission", "", rootTable, leafTable, targetSchema, selectedTable)
}

func cleanupAdmissionObjects(ctx context.Context, pool *pgxpool.Pool, publication, sourceTable, targetSchema string) {
	execLiveDDLNoFail(ctx, pool, "cleanup_admission", publication, sourceTable, "", targetSchema, "")
}

func runManagedAdmissionFlow(ctx context.Context, dsn string, engine *workflow.PostgresEngine, pool *pgxpool.Pool, authorityStore *authority.PostgresStore, flowID, sourceSystem, publication, sourceTable, targetSchema string, sourceOverrides map[string]string) error {
	sourceOptions := map[string]string{
		"dsn": dsn, "managed": "true", "bootstrap": "required", "ensure_publication": "true", "ensure_state": "true",
		"publication": publication, "tables": "public." + sourceTable, "snapshot_workers": "1", "batch_size": "10",
		"source_system_identifier": sourceSystem, "source_lineage_id": "lineage-" + flowID, "publication_revision": "bootstrap-pending",
	}
	for key, value := range sourceOverrides {
		sourceOptions[key] = value
	}
	definition := flow.Flow{
		ID:           flowID,
		Source:       connector.Spec{Name: "source", Type: connector.EndpointPostgres, Options: sourceOptions},
		Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres, Options: map[string]string{"dsn": dsn, "batch_mode": "target", "destination_revision_id": "revision-" + flowID, "synchronous_commit": "on", "meta_table_enabled": "false"}}},
		Config:       flow.Config{AckPolicy: stream.AckPolicyAll, TableMappings: flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{Destination: "target", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude}, Tables: []flow.TableMapping{{SourceSchema: "public", SourceTable: sourceTable, Action: flow.MappingActionInclude, TargetSchema: targetSchema, TargetTable: sourceTable, FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}}}}}}}},
	}
	if _, err := engine.Create(ctx, definition); err != nil {
		return err
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		return err
	}
	checkpoints, err := checkpoint.NewPostgresStoreWithPool(ctx, pool)
	if err != nil {
		return err
	}
	coordinator, err := delivery.NewCoordinator(ctx, pool)
	if err != nil {
		return err
	}
	return (&runner.FlowRunner{
		Engine: engine, Checkpoints: checkpoints, ExpectedGeneration: control.Generation,
		ExecutionBackend: "integration", ExecutionID: "admission-" + flowID,
		Authority: authorityStore, Deliveries: coordinator,
	}).Run(ctx, definition, &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}, []stream.DestinationConfig{{Spec: definition.Destinations[0], Dest: &pgdest.Destination{}}})
}

func admissionSideEffects(t *testing.T, ctx context.Context, pool *pgxpool.Pool, flowID, publication, targetSchema, targetTable string) liveAdmissionEvidence {
	t.Helper()
	var evidence liveAdmissionEvidence
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_publication WHERE pubname=$1`, publication).Scan(&evidence.Publications); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name IN (SELECT physical_name FROM source_resources WHERE flow_id=$1 AND resource_kind='slot')`, flowID).Scan(&evidence.Slots); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_bootstraps WHERE flow_id=$1`, flowID).Scan(&evidence.Bootstraps); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_resources WHERE flow_id=$1`, flowID).Scan(&evidence.Resources); err != nil {
		t.Fatal(err)
	}
	var err error
	evidence.TargetRows, err = liveTargetRowCount(ctx, pool, targetSchema, targetTable)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname=$1 AND tablename LIKE $2`, targetSchema, targetTable+"_wb_%").Scan(&evidence.StageTables); err != nil {
		t.Fatal(err)
	}
	var manifestTable bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('wallaby.managed_bootstrap_tables') IS NOT NULL`).Scan(&manifestTable); err != nil {
		t.Fatal(err)
	}
	if manifestTable {
		targetName := pgx.Identifier{targetSchema, targetTable}.Sanitize()
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallaby.managed_bootstrap_tables WHERE target_table=$1`, targetName).Scan(&evidence.StageManifests); err != nil {
			t.Fatal(err)
		}
	}
	return evidence
}

func createPublicationTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool, firstTable, secondTable string) {
	t.Helper()
	cleanupPublicationObjects(ctx, pool, "", firstTable, secondTable)
	execLiveDDL(t, ctx, pool, "create_publication_tables", "", firstTable, secondTable, "", "")
}

func cleanupPublicationObjects(ctx context.Context, pool *pgxpool.Pool, publication, firstTable, secondTable string) {
	execLiveDDLNoFail(ctx, pool, "cleanup_publication", publication, firstTable, secondTable, "", "")
}

func livePublicationRelations(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tables ...string) []bootstrap.PublicationRelation {
	t.Helper()
	relations := make([]bootstrap.PublicationRelation, 0, len(tables))
	for _, table := range tables {
		var relation bootstrap.PublicationRelation
		if err := pool.QueryRow(ctx, `SELECT c.oid,n.nspname,c.relname,c.relkind::text,c.relispartition FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=$1::regclass`, "public."+table).Scan(&relation.OID, &relation.Namespace, &relation.Table, &relation.RelationKind, &relation.IsPartition); err != nil {
			t.Fatal(err)
		}
		relations = append(relations, relation)
	}
	return relations
}

func createManagedResourceFence(t *testing.T, ctx context.Context, engine *workflow.PostgresEngine, store *authority.PostgresStore, flowID, dsn string) authority.RunFence {
	t.Helper()
	definition := flow.Flow{ID: flowID, Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{"dsn": dsn, "managed": "true"}}, Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres}}, Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.Spec{{Name: "target", Type: connector.EndpointPostgres}})}}
	if _, err := engine.Create(ctx, definition); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := store.AcquireProducer(ctx, flowID, "resource-"+flowID, "integration", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return fence
}

func newLiveBootstrapper(t *testing.T, ctx context.Context, pool *pgxpool.Pool, dsn string) *bootstrap.Bootstrapper {
	t.Helper()
	coordinator, err := bootstrap.NewBootstrapper(ctx, pool, dsn, pool, bootstrap.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func seedAdoptedSlotResource(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fence authority.RunFence, sourceSystem, databaseName, slot string) {
	t.Helper()
	revisionDigest := sha256.Sum256([]byte(sourceSystem + "\x00" + databaseName + "\x00" + slot + "\x00pgoutput"))
	revision := hex.EncodeToString(revisionDigest[:])
	resourceID := uuid.New()
	if _, err := pool.Exec(ctx, `
INSERT INTO source_resources(
 flow_incarnation_id,resource_kind,resource_id,flow_id,generation,acquisition_id,lease_epoch,
 created_generation,created_acquisition_id,created_lease_epoch,
 source_system_id,database_name,physical_name,ownership,revision,state
) VALUES($1,'slot',$2,$3,$4,$5,$6,$4,$5,$6,$7,$8,$9,'adopted',$10,'ready')`, fence.FlowIncarnationID, resourceID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, sourceSystem, databaseName, slot, revision); err != nil {
		t.Fatal(err)
	}
}

func assertLiveSourceResource(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind, name string, want bool) {
	t.Helper()
	var exists bool
	var err error
	switch kind {
	case "slot":
		err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)`, name).Scan(&exists)
	case "publication":
		err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_publication WHERE pubname=$1)`, name).Scan(&exists)
	default:
		t.Fatalf("unknown resource kind %q", kind)
	}
	if err != nil {
		t.Fatal(err)
	}
	if exists != want {
		t.Fatalf("live %s %q exists=%t, want %t", kind, name, exists, want)
	}
}
