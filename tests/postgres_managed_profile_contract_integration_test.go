package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/internal/flow"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/internal/tablemap"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresManagedProfileVersionContract(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var raw string
	if err := pool.QueryRow(ctx, "SHOW server_version_num").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	versionNumber, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatal(err)
	}
	major := versionNumber / 10000
	profile := connector.PostgresToPostgresV1Profile()
	if err := profile.ValidatePromotion(); err != nil {
		t.Fatal(err)
	}
	if !profile.SupportsPostgresVersion(major) {
		t.Fatalf("live PostgreSQL %d is outside %s", major, profile.Name)
	}
	if !profile.SameMajorOnly {
		t.Fatal("maintained profile exposed untested mixed-major pairing")
	}
	destination := &pgdest.Destination{}
	if err := destination.Open(ctx, connector.Spec{Name: "version-admission", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "managed_profile": profile.Name, "synchronous_commit": "on", "meta_table_enabled": "false",
	}}); err != nil {
		t.Fatalf("open exact named profile on PostgreSQL %d: %v", major, err)
	}
	_ = destination.Close(ctx)
}

func TestPostgresManagedProfileTargetAdmission(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const table = "wallaby_profile_target_admission"
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_profile_target_admission; CREATE TABLE public.wallaby_profile_target_admission (id bigint NOT NULL,value text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.wallaby_profile_target_admission`)
	}()
	destination := openNamedProfileDestination(t, ctx, dsn, 2)
	defer destination.Close(ctx)
	schema := managedProfileSchema(table, false)
	tables := []connector.BootstrapTable{{Schema: schema, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}}}}
	intent := connector.BootstrapIntent{
		FlowID: "target-admission", FlowIncarnationID: uuid.NewString(), SourceLineageID: "target-admission-lineage",
		BootstrapID: uuid.NewString(), BootstrapGeneration: 1, Generation: 1,
		AcquisitionID: uuid.NewString(), LeaseEpoch: 1, DestinationRevisionID: "target-admission-revision", ManifestHash: "target-admission-manifest",
	}
	if err := destination.PrepareBootstrap(ctx, intent, tables); err == nil || !strings.Contains(err.Error(), "unique/primary-key constraint") {
		t.Fatalf("bootstrap target without unique identity error=%v, want unique-constraint rejection", err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE public.wallaby_profile_target_admission ADD CONSTRAINT wallaby_profile_target_admission_deferred UNIQUE (id) DEFERRABLE INITIALLY IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	if err := destination.PrepareBootstrap(ctx, intent, tables); err == nil || !strings.Contains(err.Error(), "unique/primary-key constraint") {
		t.Fatalf("bootstrap target with deferrable unique identity error=%v, want ON CONFLICT-ineligible rejection", err)
	}
	if _, err := pool.Exec(ctx, `
ALTER TABLE public.wallaby_profile_target_admission DROP CONSTRAINT wallaby_profile_target_admission_deferred;
ALTER TABLE public.wallaby_profile_target_admission ADD CONSTRAINT wallaby_profile_target_admission_pk PRIMARY KEY (id)`); err != nil {
		t.Fatal(err)
	}
	if err := destination.PrepareBootstrap(ctx, intent, tables); err != nil {
		t.Fatalf("admit compatible target: %v", err)
	}
	if err := destination.AbandonBootstrap(ctx, intent, tables); err != nil {
		t.Fatalf("abandon admission stage: %v", err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE public.wallaby_profile_target_admission ALTER COLUMN value TYPE bigint USING 0`); err != nil {
		t.Fatal(err)
	}
	transaction := connector.SourceTransaction{
		SourceLineageID: "target-admission-lineage", TransactionID: 999,
		BeginLSN: "0/A00", CommitLSN: "0/A08", EndLSN: "0/A10", Checkpoint: connector.Checkpoint{LSN: "0/A10"},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{Schema: schema, Records: []connector.Record{{
				Table: table, Operation: connector.OpInsert, SchemaVersion: schema.Version,
				After: map[string]any{"id": int64(1), "value": "incompatible"},
			}}},
		}},
	}
	bindTestUpsertPolicy(&transaction, "id")
	if err := destination.ValidateTransaction(ctx, transaction); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("target type mismatch error=%v, want incompatibility rejection", err)
	}
}

func TestPostgresManagedProfileDestinationSchemaEvolution(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const table = "wallaby_profile_schema_evolution"
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_profile_schema_evolution; CREATE TABLE public.wallaby_profile_schema_evolution (id bigint PRIMARY KEY,value text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.wallaby_profile_schema_evolution`)
	}()

	destination := openNamedProfileDestination(t, ctx, dsn, 2)
	defer destination.Close(ctx)
	before := managedProfileSchema(table, false)
	after := managedProfileSchema(table, true)
	plan := internalschema.Diff(before, after)
	planBytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	transaction := connector.SourceTransaction{
		SourceLineageID: "profile-schema-lineage", TransactionID: 1001,
		BeginLSN: "0/200", CommitLSN: "0/280", EndLSN: "0/288", Checkpoint: connector.Checkpoint{LSN: "0/288"},
		Fragments: []connector.TransactionFragment{
			{
				Ordinal: 0,
				Batch: connector.Batch{Schema: after, Records: []connector.Record{{
					Table: table, Operation: connector.OpDDL, SchemaVersion: 2, DDLPlan: planBytes,
				}}},
			},
			{
				Ordinal: 1,
				Batch: connector.Batch{Schema: after, Records: []connector.Record{{
					Table: table, Operation: connector.OpInsert, SchemaVersion: 2,
					Key:   recordKey(t, map[string]any{"id": 1}),
					After: map[string]any{"id": int64(1), "value": "evolved", "note": "ordered-after-ddl"},
				}}},
			},
		},
	}
	bindTestUpsertPolicy(&transaction, "id")
	intent := managedProfileTransactionIntent(t, transaction, "schema-evolution")
	if _, err := destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatal(err)
	}
	var note string
	if err := pool.QueryRow(ctx, `SELECT note FROM public.wallaby_profile_schema_evolution WHERE id=1`).Scan(&note); err != nil {
		t.Fatal(err)
	}
	if note != "ordered-after-ddl" {
		t.Fatalf("evolved row note=%q", note)
	}

	altered := after
	altered.Version = 3
	altered.Columns = append([]connector.Column(nil), after.Columns...)
	altered.Columns[2].Type = "varchar(64)"
	alterPlan, err := json.Marshal(internalschema.Diff(after, altered))
	if err != nil {
		t.Fatal(err)
	}
	alterTransaction := connector.SourceTransaction{
		SourceLineageID: "profile-schema-lineage", TransactionID: 1002,
		BeginLSN: "0/290", CommitLSN: "0/298", EndLSN: "0/2A0", Checkpoint: connector.Checkpoint{LSN: "0/2A0"},
		Fragments: []connector.TransactionFragment{
			{
				Ordinal: 0,
				Batch: connector.Batch{Schema: altered, Records: []connector.Record{{
					Table: table, Operation: connector.OpDDL, SchemaVersion: 3, DDLPlan: alterPlan,
				}}},
			},
			{
				Ordinal: 1,
				Batch: connector.Batch{Schema: altered, Records: []connector.Record{{
					Table: table, Operation: connector.OpUpdate, SchemaVersion: 3,
					Key:    recordKey(t, map[string]any{"id": 1}),
					Before: map[string]any{"id": int64(1), "value": "evolved", "note": "ordered-after-ddl"},
					After:  map[string]any{"id": int64(1), "value": "altered", "note": "varchar"},
				}}},
			},
		},
	}
	bindTestUpsertPolicy(&alterTransaction, "id")
	if _, err := destination.ApplyTransaction(ctx, managedProfileTransactionIntent(t, alterTransaction, "schema-alter"), alterTransaction); err != nil {
		t.Fatal(err)
	}

	dropped := altered
	dropped.Version = 4
	dropped.Columns = append([]connector.Column(nil), altered.Columns[:2]...)
	dropPlan, err := json.Marshal(internalschema.Diff(altered, dropped))
	if err != nil {
		t.Fatal(err)
	}
	dropTransaction := connector.SourceTransaction{
		SourceLineageID: "profile-schema-lineage", TransactionID: 1003,
		BeginLSN: "0/2A8", CommitLSN: "0/2B0", EndLSN: "0/2B8", Checkpoint: connector.Checkpoint{LSN: "0/2B8"},
		Fragments: []connector.TransactionFragment{
			{
				Ordinal: 0,
				Batch: connector.Batch{Schema: dropped, Records: []connector.Record{{
					Table: table, Operation: connector.OpDDL, SchemaVersion: 4, DDLPlan: dropPlan,
				}}},
			},
			{
				Ordinal: 1,
				Batch: connector.Batch{Schema: dropped, Records: []connector.Record{{
					Table: table, Operation: connector.OpUpdate, SchemaVersion: 4,
					Key:    recordKey(t, map[string]any{"id": 1}),
					Before: map[string]any{"id": int64(1), "value": "altered"},
					After:  map[string]any{"id": int64(1), "value": "dropped"},
				}}},
			},
		},
	}
	bindTestUpsertPolicy(&dropTransaction, "id")
	if _, err := destination.ApplyTransaction(ctx, managedProfileTransactionIntent(t, dropTransaction, "schema-drop"), dropTransaction); err != nil {
		t.Fatal(err)
	}
	var value string
	var noteColumnCount int
	if err := pool.QueryRow(ctx, `SELECT value FROM public.wallaby_profile_schema_evolution WHERE id=1`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND column_name='note'`, table).Scan(&noteColumnCount); err != nil {
		t.Fatal(err)
	}
	if value != "dropped" || noteColumnCount != 0 {
		t.Fatalf("schema evolution final value=%q note_columns=%d, want dropped/0", value, noteColumnCount)
	}
}

func TestPostgresManagedProfileDDLTargetMapping(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const (
		sourceTable = "wallaby_profile_source_mapping"
		targetTable = "wallaby_profile_target_mapping"
	)
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_profile_target_mapping; CREATE TABLE public.wallaby_profile_target_mapping (id bigint PRIMARY KEY,value text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.wallaby_profile_target_mapping`)
	}()
	destination := &pgdest.Destination{}
	if err := destination.Open(ctx, connector.Spec{Name: "mapped-profile", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1,
		"batch_mode": "target", "synchronous_commit": "on",
		"meta_table_enabled": "false", "schema": "public", "table": targetTable,
	}}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(ctx)

	before := managedProfileSchema(sourceTable, false)
	after := managedProfileSchema(sourceTable, true)
	planBytes, err := json.Marshal(internalschema.Diff(before, after))
	if err != nil {
		t.Fatal(err)
	}
	transaction := connector.SourceTransaction{
		SourceLineageID: "profile-mapped-lineage", TransactionID: 1004,
		BeginLSN: "0/2C0", CommitLSN: "0/2C8", EndLSN: "0/2D0", Checkpoint: connector.Checkpoint{LSN: "0/2D0"},
		Fragments: []connector.TransactionFragment{
			{Ordinal: 0, Batch: connector.Batch{Schema: after, Records: []connector.Record{{
				Table: sourceTable, Operation: connector.OpDDL, SchemaVersion: after.Version, DDLPlan: planBytes,
			}}}},
			{Ordinal: 1, Batch: connector.Batch{Schema: after, Records: []connector.Record{{
				Table: sourceTable, Operation: connector.OpInsert, SchemaVersion: after.Version,
				Key: recordKey(t, map[string]any{"id": 1}), After: map[string]any{"id": int64(1), "value": "mapped", "note": "target"},
			}}}},
		},
	}
	mappings := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{Destination: "mapped-profile", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude}, Tables: []flow.TableMapping{{SourceSchema: "public", SourceTable: sourceTable, Action: flow.MappingActionInclude, TargetSchema: "public", TargetTable: targetTable, FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}}}}}}}
	projector, err := tablemap.New(mappings, "mapped-profile")
	if err != nil {
		t.Fatal(err)
	}
	transaction, _, err = projector.ProjectTransaction(transaction)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := destination.ApplyTransaction(ctx, managedProfileTransactionIntent(t, transaction, "ddl-target-mapping"), transaction); err != nil {
		t.Fatal(err)
	}
	var value, note string
	if err := pool.QueryRow(ctx, `SELECT value,note FROM public.wallaby_profile_target_mapping WHERE id=1`).Scan(&value, &note); err != nil {
		t.Fatal(err)
	}
	if value != "mapped" || note != "target" {
		t.Fatalf("mapped target row=(%q,%q), want mapped/target", value, note)
	}
	var sourceTableCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=$1`, sourceTable).Scan(&sourceTableCount); err != nil {
		t.Fatal(err)
	}
	if sourceTableCount != 0 {
		t.Fatalf("managed DDL created or altered source-named target table")
	}
}

func TestPostgresManagedProfileDDLCommitReconciliation(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const table = "wallaby_profile_ddl_reconcile"
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_profile_ddl_reconcile; CREATE TABLE public.wallaby_profile_ddl_reconcile (id bigint PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.wallaby_profile_ddl_reconcile`)
	}()
	destination := openNamedProfileDestination(t, ctx, dsn, 2)
	defer destination.Close(ctx)
	before := connector.Schema{Namespace: "public", Name: table, Version: 1, Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: managedProfileKeyMetadata()}}}
	after := before
	after.Version = 2
	after.Columns = append(append([]connector.Column(nil), before.Columns...), connector.Column{Name: "note", Type: "text", Nullable: true})
	planBytes, err := json.Marshal(internalschema.Diff(before, after))
	if err != nil {
		t.Fatal(err)
	}
	transaction := connector.SourceTransaction{
		SourceLineageID: "profile-ddl-lineage", TransactionID: 1002,
		BeginLSN: "0/300", CommitLSN: "0/380", EndLSN: "0/388", Checkpoint: connector.Checkpoint{LSN: "0/388"},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{Schema: after, Records: []connector.Record{{
				Table: table, Operation: connector.OpDDL, SchemaVersion: 2, DDLPlan: planBytes,
			}}},
		}},
	}
	bindTestUpsertPolicy(&transaction, "id")
	intent := managedProfileTransactionIntent(t, transaction, "ddl-reconcile")
	driver := &commitBeforeReceiptTransactionDriver{ManagedTransactionDestination: destination}
	evidence, err := driver.ApplyTransaction(ctx, intent, transaction)
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("injected DDL commit outcome=%v, want indeterminate", err)
	}
	disposition, reconciled, err := destination.Reconcile(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if disposition != connector.DeliveryApplied || reconciled != evidence {
		t.Fatalf("DDL reconcile=(%v,%+v), want applied/%+v", disposition, reconciled, evidence)
	}
}

func TestPostgresLegacyDeliveryIdentityMigrationFailsClosed(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := newAuthorityTestPool(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	const database = "wallaby_profile_upgrade_contract"
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS wallaby_profile_upgrade_contract WITH (FORCE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE DATABASE wallaby_profile_upgrade_contract`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DROP DATABASE IF EXISTS wallaby_profile_upgrade_contract WITH (FORCE)`)
	}()
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.ConnConfig.Database = database
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate migration fixtures")
	}
	root := filepath.Dir(filepath.Dir(filename))
	for _, name := range []string{"001_attempts_receipts.sql", "002_authority_protocol.sql", "003_authority_protocol_v2.sql"} {
		contents, err := os.ReadFile(filepath.Join(root, "internal", "delivery", "migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(contents)); err != nil {
			t.Fatalf("apply legacy delivery migration %s: %v", name, err)
		}
	}
	incarnationID := uuid.New()
	attemptID := uuid.New()
	acquisitionID := uuid.New()
	legacyTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacyTx.Exec(ctx, `SELECT set_config('wallaby.authority_protocol','v2',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := legacyTx.Exec(ctx, `
INSERT INTO delivery_manifests (
  flow_incarnation_id,destination_revision_id,source_lineage_id,position_id,
  source_transaction_id,content_hash,checkpoint_lsn
) VALUES ($1,'upgrade-revision','upgrade-lineage','upgrade-position','upgrade-transaction','upgrade-hash','0/900')`, incarnationID); err != nil {
		t.Fatal(err)
	}
	if _, err := legacyTx.Exec(ctx, `
INSERT INTO delivery_attempts (
  attempt_id,flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,
  destination_revision_id,source_lineage_id,position_id,content_hash
) VALUES ($1,$2,'upgrade-flow',1,$3,1,'upgrade-revision','upgrade-lineage','upgrade-position','upgrade-hash')`, attemptID, incarnationID, acquisitionID); err != nil {
		t.Fatal(err)
	}
	if _, err := legacyTx.Exec(ctx, `
INSERT INTO delivery_receipts (
  flow_incarnation_id,destination_revision_id,source_lineage_id,position_id,content_hash,
  attempt_id,external_id,adopted_by_acquisition_id,adopted_by_lease_epoch
) VALUES ($1,'upgrade-revision','upgrade-lineage','upgrade-position','upgrade-hash',$2,'upgrade-external',$3,1)`, incarnationID, attemptID, acquisitionID); err != nil {
		t.Fatal(err)
	}
	if err := legacyTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	err = controlplane.ApplyMigrations(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "refuses noncanonical logical batch identities") {
		t.Fatalf("migration error=%v", err)
	}
}

func managedProfileTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	return dsn
}

func openNamedProfileDestination(t *testing.T, ctx context.Context, dsn string, poolSize int) *pgdest.Destination {
	t.Helper()
	destination := &pgdest.Destination{}
	if err := destination.Open(ctx, connector.Spec{Name: "named-profile", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1,
		"batch_mode": "target", "synchronous_commit": "on",
		"meta_table_enabled": "false", "pool_max_conns": strconv.Itoa(poolSize),
	}}); err != nil {
		t.Fatal(err)
	}
	return destination
}

func managedProfileSchema(table string, includeNote bool) connector.Schema {
	columns := []connector.Column{
		{Name: "id", Type: "bigint", TypeMetadata: managedProfileKeyMetadata()},
		{Name: "value", Type: "text"},
	}
	if includeNote {
		columns = append(columns, connector.Column{Name: "note", Type: "text", Nullable: true})
	}
	version := int64(1)
	if includeNote {
		version = 2
	}
	return connector.Schema{Namespace: "public", Name: table, Version: version, Columns: columns}
}

func managedProfileKeyMetadata() map[string]string {
	return map[string]string{"primary_key": "true", "replica_identity": "true"}
}

func managedProfileTransactionIntent(t *testing.T, transaction connector.SourceTransaction, suffix string) connector.DeliveryIntent {
	t.Helper()
	contentHash, err := connector.SourceTransactionContentHash(transaction)
	if err != nil {
		t.Fatal(err)
	}
	logicalBatchID, err := connector.SourceTransactionLogicalBatchID(transaction)
	if err != nil {
		t.Fatal(err)
	}
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{
		FlowID: "profile-" + suffix, FlowIncarnationID: fmt.Sprintf("profile-incarnation-%s-%d", suffix, time.Now().UnixNano()),
		SourceLineageID: transaction.SourceLineageID, Generation: 1, AcquisitionID: "profile-acquisition", LeaseEpoch: 1,
		DestinationRevisionID: "profile-destination-" + suffix, LogicalBatchID: logicalBatchID, PositionID: positionID, ContentHash: contentHash,
	}
}
