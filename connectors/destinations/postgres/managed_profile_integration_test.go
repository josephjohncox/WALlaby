package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresManagedTargetRejectsLegacyReceiptSchemaWithoutMutation(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `
DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts;
CREATE SCHEMA IF NOT EXISTS wallaby_meta;
CREATE TABLE wallaby_meta.__delivery_receipts (
  marker_id TEXT NOT NULL UNIQUE,
  flow_id TEXT NOT NULL,
  flow_incarnation_id TEXT NOT NULL,
  generation BIGINT NOT NULL,
  acquisition_id TEXT NOT NULL,
  lease_epoch BIGINT NOT NULL,
  destination_revision_id TEXT NOT NULL,
  source_lineage_id TEXT NOT NULL,
  position_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (flow_incarnation_id,destination_revision_id,position_id)
)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts`)
	}()
	destination := &Destination{}
	err = destination.Open(ctx, connector.RuntimeSpec{Name: "rolling-target", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1,
		"batch_mode": "target", "synchronous_commit": "on", "meta_table_enabled": "false",
	}})
	defer destination.Close(context.Background())
	if err == nil || !strings.Contains(err.Error(), "contract") {
		t.Fatalf("legacy receipt schema admission error=%v", err)
	}
	var logicalColumnExists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid='wallaby_meta.__delivery_receipts'::regclass AND attname='logical_batch_id' AND attnum>0 AND NOT attisdropped)`).Scan(&logicalColumnExists); err != nil {
		t.Fatal(err)
	}
	if logicalColumnExists {
		t.Fatal("managed open mutated legacy receipt schema")
	}
	if _, err := admin.Exec(ctx, `DROP TABLE wallaby_meta.__delivery_receipts`); err != nil {
		t.Fatal(err)
	}
	canonical := &Destination{}
	if err := canonical.Open(ctx, connector.RuntimeSpec{Name: "canonical-target", Type: connector.EndpointPostgres, Options: map[string]string{"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1, "batch_mode": "target", "synchronous_commit": "on", "meta_table_enabled": "false"}}); err != nil {
		t.Fatal(err)
	}
	_ = canonical.Close(ctx)
	if _, err := admin.Exec(ctx, `ALTER TABLE wallaby_meta.__delivery_receipts DROP CONSTRAINT wallaby_delivery_receipts_logical_batch_unique`); err != nil {
		t.Fatal(err)
	}
	mismatch := &Destination{}
	err = mismatch.Open(ctx, connector.RuntimeSpec{Name: "mismatch-target", Type: connector.EndpointPostgres, Options: map[string]string{"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1, "batch_mode": "target", "synchronous_commit": "on", "meta_table_enabled": "false"}})
	_ = mismatch.Close(ctx)
	if err == nil || !strings.Contains(err.Error(), "contract mismatch") {
		t.Fatalf("receipt unique-index mismatch error=%v", err)
	}
	var restored bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid='wallaby_meta.__delivery_receipts'::regclass AND conname='wallaby_delivery_receipts_logical_batch_unique')`).Scan(&restored); err != nil {
		t.Fatal(err)
	}
	if restored {
		t.Fatal("managed open recreated a missing receipt constraint")
	}
	for name, mutation := range map[string]string{
		"removed_default":                      `ALTER TABLE wallaby_meta.__delivery_receipts ALTER COLUMN committed_at DROP DEFAULT`,
		"extra_check":                          `ALTER TABLE wallaby_meta.__delivery_receipts ADD CONSTRAINT adversarial_check CHECK (generation>0)`,
		"identity_column":                      `ALTER TABLE wallaby_meta.__delivery_receipts ADD COLUMN adversarial_identity bigint GENERATED ALWAYS AS IDENTITY`,
		"wrong_index":                          `CREATE INDEX adversarial_hash_index ON wallaby_meta.__delivery_receipts USING hash(content_hash)`,
		"weak_identity_check":                  `ALTER TABLE wallaby_meta.__delivery_receipts DROP CONSTRAINT wallaby_delivery_receipts_logical_batch_current;ALTER TABLE wallaby_meta.__delivery_receipts ADD CONSTRAINT wallaby_delivery_receipts_logical_batch_current CHECK (logical_batch_id<>'')`,
		"inheritance_child_fabricated_receipt": `CREATE TABLE wallaby_meta.adversarial_receipt_child () INHERITS (wallaby_meta.__delivery_receipts); INSERT INTO wallaby_meta.adversarial_receipt_child(marker_id,flow_id,flow_incarnation_id,generation,acquisition_id,lease_epoch,destination_revision_id,source_lineage_id,logical_batch_id,position_id,content_hash) VALUES('fabricated','flow','incarnation',1,'acquisition',1,'revision','lineage','logical-batch:'||encode(sha256(convert_to('lineage','UTF8')||decode('00','hex')||convert_to('0/10','UTF8')||decode('00','hex')||convert_to('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','UTF8')),'hex'),'0/10','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`,
		"user_trigger":                         `CREATE FUNCTION wallaby_meta.adversarial_receipt_trigger() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$; CREATE TRIGGER adversarial_receipt_trigger BEFORE INSERT ON wallaby_meta.__delivery_receipts FOR EACH ROW EXECUTE FUNCTION wallaby_meta.adversarial_receipt_trigger()`,
		"rewrite_rule":                         `CREATE RULE adversarial_receipt_rule AS ON UPDATE TO wallaby_meta.__delivery_receipts DO INSTEAD NOTHING`,
		"row_security_policy":                  `ALTER TABLE wallaby_meta.__delivery_receipts ENABLE ROW LEVEL SECURITY; ALTER TABLE wallaby_meta.__delivery_receipts FORCE ROW LEVEL SECURITY; CREATE POLICY adversarial_receipt_policy ON wallaby_meta.__delivery_receipts USING (false)`,
	} {
		mutation := mutation
		t.Run(name, func(t *testing.T) {
			if _, err := admin.Exec(ctx, `DROP TABLE IF EXISTS wallaby_meta.adversarial_receipt_child; DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts; DROP FUNCTION IF EXISTS wallaby_meta.adversarial_receipt_trigger()`); err != nil {
				t.Fatal(err)
			}
			verifier := &Destination{pool: admin, batchMode: batchModeTarget}
			if err := verifier.InitializeManagedDelivery(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := admin.Exec(ctx, mutation); err != nil {
				t.Fatal(err)
			}
			if err := verifier.InitializeManagedDelivery(ctx); err == nil {
				t.Fatal("adversarial receipt catalog mutation admitted")
			}
		})
	}
	t.Run("partitioned_relation", func(t *testing.T) {
		if _, err := admin.Exec(ctx, `DROP TABLE IF EXISTS wallaby_meta.adversarial_receipt_child; DROP TABLE IF EXISTS wallaby_meta.__delivery_receipts; CREATE TABLE wallaby_meta.__delivery_receipts (
  marker_id text NOT NULL,flow_id text NOT NULL,flow_incarnation_id text NOT NULL,generation bigint NOT NULL,
  acquisition_id text NOT NULL,lease_epoch bigint NOT NULL,destination_revision_id text NOT NULL,source_lineage_id text NOT NULL,
  logical_batch_id text NOT NULL,position_id text NOT NULL,content_hash text NOT NULL,
  committed_at timestamptz NOT NULL DEFAULT clock_timestamp()
) PARTITION BY LIST (flow_id)`); err != nil {
			t.Fatal(err)
		}
		if err := (&Destination{pool: admin, batchMode: batchModeTarget}).InitializeManagedDelivery(ctx); err == nil {
			t.Fatal("partitioned receipt authority was admitted")
		}
	})
}

func TestManagedBootstrapMetadataFailsClosedOnCatalogMismatch(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureManagedBootstrapTables(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	target, stage := "wallaby_stage_target_"+strings.ReplaceAll(uuid.NewString(), "-", ""), "wallaby_stage_copy_"+strings.ReplaceAll(uuid.NewString(), "-", "")
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE public.%s(id bigint PRIMARY KEY,value text NOT NULL)`, quoteIdent(target))); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT wallaby.prepare_managed_bootstrap_stage('public',$1,'public',$2)`, target, stage); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := verifyManagedBootstrapStage(ctx, tx, "public", target, "public", stage); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE public.%s ADD COLUMN extra text`, quoteIdent(stage))); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := verifyManagedBootstrapStage(ctx, tx, "public", target, "public", stage); err == nil {
		_ = tx.Rollback(ctx)
		t.Fatal("mismatched bootstrap stage was trusted")
	}
	_ = tx.Rollback(ctx)
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ALTER TABLE wallaby.managed_bootstrap_publications ALTER COLUMN external_id DROP NOT NULL`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := ensureManagedBootstrapTables(ctx, tx); err == nil || !strings.Contains(err.Error(), "contract") {
		_ = tx.Rollback(ctx)
		t.Fatalf("bootstrap table mismatch error=%v", err)
	}
	_ = tx.Rollback(ctx)
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `CREATE OR REPLACE FUNCTION wallaby.drop_managed_bootstrap_stage(stage_schema text,stage_table text) RETURNS void LANGUAGE plpgsql AS $$BEGIN NULL; END$$`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := ensureManagedBootstrapTables(ctx, tx); err == nil || !strings.Contains(err.Error(), "function") {
		_ = tx.Rollback(ctx)
		t.Fatalf("bootstrap function mismatch error=%v", err)
	}
	_ = tx.Rollback(ctx)
}

func TestHiddenWatermarkAndBootstrapCatalogRejectAdversarialObjects(t *testing.T) {
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
	setup, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureWatermarkStateTable(ctx, setup); err != nil {
		_ = setup.Rollback(ctx)
		t.Fatal(err)
	}
	if err := ensureManagedBootstrapTables(ctx, setup); err != nil {
		_ = setup.Rollback(ctx)
		t.Fatal(err)
	}
	if err := setup.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]string{
		"watermark_removed_default": `ALTER TABLE wallaby.watermark_state ALTER COLUMN updated_at DROP DEFAULT`,
		"watermark_extra_check":     `ALTER TABLE wallaby.watermark_state ADD CONSTRAINT adversarial_watermark_check CHECK (watermark_value<>'')`,
		"watermark_identity":        `ALTER TABLE wallaby.watermark_state ADD COLUMN adversarial_identity bigint GENERATED ALWAYS AS IDENTITY`,
		"watermark_wrong_index":     `CREATE INDEX adversarial_watermark_hash ON wallaby.watermark_state USING hash(content_hash)`,
		"bootstrap_removed_default": `ALTER TABLE wallaby.managed_bootstrap_publications ALTER COLUMN published_at DROP DEFAULT`,
		"bootstrap_extra_check":     `ALTER TABLE wallaby.managed_bootstrap_tables ADD CONSTRAINT adversarial_bootstrap_check CHECK (manifest_hash<>'')`,
		"bootstrap_identity":        `ALTER TABLE wallaby.managed_bootstrap_publications ADD COLUMN adversarial_identity bigint GENERATED ALWAYS AS IDENTITY`,
		"bootstrap_wrong_index":     `CREATE INDEX adversarial_bootstrap_hash ON wallaby.managed_bootstrap_tables USING hash(manifest_hash)`,
	}
	for name, statement := range mutations {
		t.Run(name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			if _, err := tx.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
			var verifyErr error
			if strings.HasPrefix(name, "watermark_") {
				verifyErr = ensureWatermarkStateTable(ctx, tx)
			} else {
				verifyErr = ensureManagedBootstrapTables(ctx, tx)
			}
			if verifyErr == nil {
				t.Fatal("adversarial hidden catalog mutation admitted")
			}
		})
	}
}

func TestManagedTargetAdmissionUsesOrderedProjectedNaturalKeysUnderFullIdentity(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	table := "wallaby_natural_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	ordered := table + "_ordered"
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE public.%s (id bigint PRIMARY KEY,email text NOT NULL UNIQUE,note text); CREATE TABLE public.%s (a bigint NOT NULL,b bigint NOT NULL,UNIQUE(b,a))`, quoteIdent(table), quoteIdent(ordered))); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS public.%s,public.%s`, quoteIdent(table), quoteIdent(ordered)))
	}()
	full := func(name string, columns ...connector.Column) connector.Schema {
		for index := range columns {
			if columns[index].TypeMetadata == nil {
				columns[index].TypeMetadata = map[string]string{}
			}
			columns[index].TypeMetadata["replica_identity"] = "true"
		}
		return connector.Schema{Namespace: "public", Name: name, Columns: columns}
	}
	destination := &Destination{}
	schema := full(table, connector.Column{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"primary_key": "true"}}, connector.Column{Name: "email", Type: "text"}, connector.Column{Name: "note", Type: "text", Nullable: true})
	if err := destination.validateManagedTargetSchema(ctx, pool, schema, connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"email"}}); err != nil {
		t.Fatalf("explicit natural-key admission under full identity: %v", err)
	}
	orderedSchema := full(ordered, connector.Column{Name: "a", Type: "bigint"}, connector.Column{Name: "b", Type: "bigint"})
	if err := destination.validateManagedTargetSchema(ctx, pool, orderedSchema, connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"a", "b"}}); err == nil || !strings.Contains(err.Error(), "in order") {
		t.Fatalf("wrong-order constraint admission error=%v", err)
	}
}

func TestManagedAppendFullAdmissionAllowsRepeatedKeysAndRejectsMixedUniqueness(t *testing.T) {
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
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	table, mixed := "wallaby_append_"+suffix, "wallaby_append_mixed_"+suffix
	columns := []connector.Column{{Name: "event_id", Type: "bigint"}, {Name: "payload", Type: "text"}, {Name: connector.AppendOperationColumn, Type: "text"}, {Name: connector.AppendDeletedColumn, Type: "boolean"}, {Name: connector.AppendSourcePositionColumn, Type: "text"}}
	createColumns := `event_id bigint,payload text,__wallaby_operation text,__wallaby_deleted boolean,__wallaby_source_position text`
	if _, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE public.%s(%s);CREATE TABLE public.%s(%s,optional_default text DEFAULT '',UNIQUE(event_id,optional_default))`, quoteIdent(table), createColumns, quoteIdent(mixed), createColumns)); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS public.%s,public.%s`, quoteIdent(table), quoteIdent(mixed)))
	}()
	destination := &Destination{pool: pool, syncCommit: "on", batchMode: batchModeTarget, flowID: "append-live"}
	if err := destination.ensureManagedReceiptTable(ctx); err != nil {
		t.Fatal(err)
	}
	policy := connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: "append-projection"}
	schema := connector.Schema{Namespace: "public", Name: table, Columns: columns}
	record := func(position, payload string) connector.Record {
		return connector.Record{Table: table, Operation: connector.OpInsert, Key: []byte(`{"event_id":1}`), After: map[string]any{"event_id": int64(1), "payload": payload, connector.AppendOperationColumn: "update", connector.AppendDeletedColumn: false, connector.AppendSourcePositionColumn: position}, SourcePosition: position}
	}
	transaction := connector.SourceTransaction{SourceLineageID: "append-lineage", TransactionID: 1, BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/28", Checkpoint: connector.Checkpoint{LSN: "0/28"}, Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{Schema: schema, WritePolicy: policy, Records: []connector.Record{record("0/18", "first"), record("0/20", "second")}}}}}
	if err := destination.ValidateTransaction(ctx, transaction); err != nil {
		t.Fatal(err)
	}
	hash, logical, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	position, err := connector.CheckpointPositionID(transaction.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	intent := connector.DeliveryIntent{FlowID: "append-live", FlowIncarnationID: uuid.NewString(), SourceLineageID: transaction.SourceLineageID, Generation: 1, AcquisitionID: uuid.NewString(), LeaseEpoch: 1, DestinationRevisionID: "append-live-v1", LogicalBatchID: logical, PositionID: position, ContentHash: hash}
	if _, err := destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM public.%s WHERE event_id=1`, quoteIdent(table))).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("repeated append rows=%d", count)
	}
	mixedSchema := schema
	mixedSchema.Name = mixed
	if err := destination.validateManagedTargetSchema(ctx, pool, mixedSchema, policy); err == nil || !strings.Contains(err.Error(), "any unique") {
		t.Fatalf("mixed/default uniqueness admission error=%v", err)
	}
}

func TestPostgresManagedProfilePoolExhaustion(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	destination := &Destination{}
	if err := destination.Open(ctx, connector.RuntimeSpec{Name: "pool-exhaustion", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "managed_profile": connector.ManagedProfilePostgresToPostgresV1,
		"batch_mode": "target", "synchronous_commit": "on",
		"meta_table_enabled": "true", "pool_max_conns": "1", "flow_id": "pool-exhaustion",
	}}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(ctx)
	if _, err := destination.pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_profile_pool_exhaustion; CREATE TABLE public.wallaby_profile_pool_exhaustion (id bigint PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = destination.pool.Exec(context.Background(), `DROP TABLE IF EXISTS public.wallaby_profile_pool_exhaustion`)
	}()

	const recordCount = 512
	records := make([]connector.Record, 0, recordCount)
	for id := 1; id <= recordCount; id++ {
		key, err := json.Marshal(map[string]any{"id": id})
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, connector.Record{
			Table: "wallaby_profile_pool_exhaustion", Operation: connector.OpInsert, SchemaVersion: 1,
			Key: key, After: map[string]any{"id": int64(id)},
		})
	}
	transaction := connector.SourceTransaction{
		SourceLineageID: "pool-exhaustion-lineage", TransactionID: 2001,
		BeginLSN: "0/400", CommitLSN: "0/480", EndLSN: "0/488", Checkpoint: connector.Checkpoint{LSN: "0/488"},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{
				WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}, ProjectionFingerprint: "pool-exhaustion-projection"},
				Schema: connector.Schema{
					Namespace: "public", Name: "wallaby_profile_pool_exhaustion", Version: 1,
					Columns: []connector.Column{{
						Name: "id", Type: "bigint",
						TypeMetadata: map[string]string{"primary_key": "true", "replica_identity": "true"},
					}},
				},
				Records: records,
			},
		}},
	}
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
	intent := connector.DeliveryIntent{
		FlowID: "pool-exhaustion", FlowIncarnationID: uuid.NewString(),
		SourceLineageID: transaction.SourceLineageID, Generation: 1, AcquisitionID: "pool-exhaustion-acquisition", LeaseEpoch: 1,
		DestinationRevisionID: "pool-exhaustion-revision", LogicalBatchID: logicalBatchID, PositionID: positionID, ContentHash: contentHash,
	}
	if _, err := destination.ApplyTransaction(ctx, intent, transaction); err != nil {
		t.Fatalf("managed transaction deadlocked or exhausted locks with one connection and metadata enabled: %v", err)
	}
	var rows int
	if err := destination.pool.QueryRow(ctx, `SELECT count(*) FROM public.wallaby_profile_pool_exhaustion`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != recordCount {
		t.Fatalf("delivered rows=%d, want %d", rows, recordCount)
	}

	secondKey, err := json.Marshal(map[string]any{"id": recordCount + 1})
	if err != nil {
		t.Fatal(err)
	}
	second := transaction
	second.TransactionID = 2002
	second.BeginLSN, second.CommitLSN, second.EndLSN = "0/490", "0/498", "0/4A0"
	second.Checkpoint = connector.Checkpoint{LSN: second.EndLSN}
	second.Fragments = []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{
		Schema: transaction.Fragments[0].Batch.Schema, WritePolicy: transaction.Fragments[0].Batch.WritePolicy,
		Records: []connector.Record{{
			Table: "wallaby_profile_pool_exhaustion", Operation: connector.OpInsert, SchemaVersion: 1,
			Key: secondKey, After: map[string]any{"id": int64(recordCount + 1)},
		}},
	}}}
	secondHash, secondLogicalBatchID, err := connector.SourceTransactionIdentity(second)
	if err != nil {
		t.Fatal(err)
	}
	secondPositionID, err := connector.CheckpointPositionID(second.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	secondIntent := intent
	secondIntent.ContentHash = secondHash
	secondIntent.LogicalBatchID = secondLogicalBatchID
	secondIntent.PositionID = secondPositionID
	if _, err := destination.ApplyTransaction(ctx, secondIntent, second); err != nil {
		t.Fatalf("deliver second managed transaction: %v", err)
	}
	var targetReceipts int
	if err := destination.pool.QueryRow(ctx, `
SELECT count(*) FROM wallaby_meta.__delivery_receipts
WHERE flow_incarnation_id=$1 AND destination_revision_id=$2`, intent.FlowIncarnationID, intent.DestinationRevisionID).Scan(&targetReceipts); err != nil {
		t.Fatal(err)
	}
	if targetReceipts != 2 {
		t.Fatalf("immutable target delivery receipts=%d, want one receipt per committed transaction", targetReceipts)
	}
}
