package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresDestinationDDLAndMutations(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	tableName := fmt.Sprintf("wallaby_dest_%d", time.Now().UnixNano())
	fullTable := fmt.Sprintf(`public."%s"`, tableName)

	dest := &pgdest.Destination{}
	spec := connector.Spec{
		Name: "dest",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                dsn,
			"schema":             "public",
			"write_mode":         "target",
			"meta_table_enabled": "false",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schemaDef := connector.Schema{
		Name:      tableName,
		Namespace: "public",
		Columns: []connector.Column{
			{Name: "id", Type: "int"},
			{Name: "name", Type: "text"},
		},
	}
	createDDL := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("CREATE TABLE %s (id int primary key, name text)", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, createDDL); err != nil {
		t.Fatalf("apply ddl: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTable))
	}()

	insert := connector.Record{
		Table:     tableName,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 1}),
		After:     map[string]any{"id": 1, "name": "alpha"},
	}
	update := connector.Record{
		Table:     tableName,
		Operation: connector.OpUpdate,
		Key:       recordKey(t, map[string]any{"id": 1}),
		After:     map[string]any{"id": 1, "name": "beta"},
	}
	if err := dest.Write(ctx, connector.Batch{Schema: schemaDef, Records: []connector.Record{insert, update}, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}}}); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	alterDDL := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ADD COLUMN extra text", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, alterDDL); err != nil {
		t.Fatalf("apply alter ddl: %v", err)
	}

	renameDDL := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s RENAME COLUMN name TO display_name", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, renameDDL); err != nil {
		t.Fatalf("apply rename ddl: %v", err)
	}

	typeDDL := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("ALTER TABLE %s ALTER COLUMN extra TYPE varchar(32)", fullTable),
		Timestamp: time.Now().UTC(),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, typeDDL); err != nil {
		t.Fatalf("apply type ddl: %v", err)
	}

	schemaDef.Columns = []connector.Column{
		{Name: "id", Type: "int"},
		{Name: "display_name", Type: "text"},
		{Name: "extra", Type: "varchar"},
	}

	insertAfter := connector.Record{
		Table:     tableName,
		Operation: connector.OpInsert,
		Key:       recordKey(t, map[string]any{"id": 2}),
		After: map[string]any{
			"id":           2,
			"display_name": "gamma",
			"extra":        "v2",
		},
	}
	if err := dest.Write(ctx, connector.Batch{Schema: schemaDef, Records: []connector.Record{insertAfter}, WritePolicy: testUpsertPolicy("id")}); err != nil {
		t.Fatalf("write insert after ddl: %v", err)
	}

	var extra string
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT extra FROM %s WHERE id = 2", fullTable)).Scan(&extra); err != nil {
		t.Fatalf("select extra: %v", err)
	}
	if extra != "v2" {
		t.Fatalf("unexpected extra after ddl: %s", extra)
	}

	deleteRec1 := connector.Record{
		Table:     tableName,
		Operation: connector.OpDelete,
		Key:       recordKey(t, map[string]any{"id": 1}),
	}
	deleteRec2 := connector.Record{
		Table:     tableName,
		Operation: connector.OpDelete,
		Key:       recordKey(t, map[string]any{"id": 2}),
	}
	if err := dest.Write(ctx, connector.Batch{Schema: schemaDef, Records: []connector.Record{deleteRec1, deleteRec2}, WritePolicy: testUpsertPolicy("id")}); err != nil {
		t.Fatalf("write delete: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", fullTable)).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected rows to be deleted, count=%d", count)
	}
}

func TestPostgresDestinationPlanDDL(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	tableName := fmt.Sprintf("wallaby_plan_%d", time.Now().UnixNano())
	fullTable := fmt.Sprintf(`public."%s"`, tableName)

	dest := &pgdest.Destination{}
	spec := connector.Spec{
		Name: "plan-dest",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                dsn,
			"schema":             "public",
			"write_mode":         "target",
			"meta_table_enabled": "false",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schemaDef := connector.Schema{
		Name:      tableName,
		Namespace: "public",
		Columns: []connector.Column{
			{Name: "id", Type: "int"},
		},
	}
	createDDL := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDL:       fmt.Sprintf("CREATE TABLE %s (id int primary key)", fullTable),
	}
	if err := dest.ApplyDDL(ctx, schemaDef, createDDL); err != nil {
		t.Fatalf("apply create ddl: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTable))
	}()

	if err := dest.Write(ctx, connector.Batch{
		Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "1"}, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}},
		Records: []connector.Record{{
			Table:     tableName,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 1}),
			After: map[string]any{
				"id": 1,
			},
		}},
	}); err != nil {
		t.Fatalf("write base row: %v", err)
	}

	planAdd := internalschema.Plan{
		Changes: []internalschema.Change{
			{
				Type:      internalschema.ChangeAddColumn,
				Namespace: "public",
				Table:     tableName,
				Column:    "display_name",
				ToType:    "text",
				Nullable:  true,
			},
		},
	}
	planAddBytes, err := json.Marshal(planAdd)
	if err != nil {
		t.Fatalf("marshal plan add: %v", err)
	}
	addRecord := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDLPlan:   planAddBytes,
	}
	if result, err := dest.ReconcileDDL(ctx, schemaDef, addRecord); err != nil || result != connector.DDLReconcileNotApplied {
		t.Fatalf("reconcile unapplied add result=%v error=%v", result, err)
	}
	if err := dest.ApplyDDL(ctx, schemaDef, addRecord); err != nil {
		t.Fatalf("apply plan add ddl: %v", err)
	}
	if result, err := dest.ReconcileDDL(ctx, schemaDef, addRecord); err != nil || result != connector.DDLReconcileApplied {
		t.Fatalf("reconcile applied add result=%v error=%v", result, err)
	}

	schemaDef.Columns = append(schemaDef.Columns, connector.Column{
		Name: "display_name",
		Type: "text",
	})
	if err := dest.Write(ctx, connector.Batch{
		Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "2"}, WritePolicy: testUpsertPolicy("id"),
		Records: []connector.Record{{
			Table:     tableName,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 2}),
			After: map[string]any{
				"id":           2,
				"display_name": "second",
			},
		}},
	}); err != nil {
		t.Fatalf("write after plan add: %v", err)
	}

	var fromPlanAdd string
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT display_name FROM %s WHERE id = $1", fullTable), 2).Scan(&fromPlanAdd); err != nil {
		t.Fatalf("query plan add: %v", err)
	}
	if fromPlanAdd != "second" {
		t.Fatalf("expected plan-added column write to persist, got %q", fromPlanAdd)
	}

	planAlterRename := internalschema.Plan{
		Changes: []internalschema.Change{
			{
				Type:      internalschema.ChangeAlterColumn,
				Namespace: "public",
				Table:     tableName,
				Column:    "display_name",
				FromType:  "text",
				ToType:    "varchar(64)",
			},
			{
				Type:      internalschema.ChangeRenameColumn,
				Namespace: "public",
				Table:     tableName,
				Column:    "display_name",
				ToColumn:  "title",
			},
		},
	}
	planAlterRenameBytes, err := json.Marshal(planAlterRename)
	if err != nil {
		t.Fatalf("marshal plan alter/rename: %v", err)
	}
	alterRenameRecord := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDLPlan:   planAlterRenameBytes,
	}
	if result, err := dest.ReconcileDDL(ctx, schemaDef, alterRenameRecord); err != nil || result != connector.DDLReconcileNotApplied {
		t.Fatalf("reconcile unapplied alter/rename result=%v error=%v", result, err)
	}
	if err := dest.ApplyDDL(ctx, schemaDef, alterRenameRecord); err != nil {
		t.Fatalf("apply plan alter/rename ddl: %v", err)
	}
	if result, err := dest.ReconcileDDL(ctx, schemaDef, alterRenameRecord); err != nil || result != connector.DDLReconcileApplied {
		t.Fatalf("reconcile applied alter/rename result=%v error=%v", result, err)
	}

	schemaDef.Columns = []connector.Column{
		{Name: "id", Type: "int"},
		{Name: "title", Type: "varchar(64)"},
	}
	if err := dest.Write(ctx, connector.Batch{
		Schema: schemaDef, Checkpoint: connector.Checkpoint{LSN: "3"}, WritePolicy: testUpsertPolicy("id"),
		Records: []connector.Record{{
			Table:     tableName,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 3}),
			After: map[string]any{
				"id":    3,
				"title": "third",
			},
		}},
	}); err != nil {
		t.Fatalf("write after plan alter/rename: %v", err)
	}

	var fromPlanRename string
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT title FROM %s WHERE id = $1", fullTable), 3).Scan(&fromPlanRename); err != nil {
		t.Fatalf("query plan rename: %v", err)
	}
	if fromPlanRename != "third" {
		t.Fatalf("expected renamed title write to persist, got %q", fromPlanRename)
	}

	planDrop := internalschema.Plan{
		Changes: []internalschema.Change{
			{
				Type:      internalschema.ChangeDropColumn,
				Namespace: "public",
				Table:     tableName,
				Column:    "title",
			},
		},
	}
	planDropBytes, err := json.Marshal(planDrop)
	if err != nil {
		t.Fatalf("marshal plan drop: %v", err)
	}
	dropRecord := connector.Record{
		Table:     tableName,
		Operation: connector.OpDDL,
		DDLPlan:   planDropBytes,
	}
	if result, err := dest.ReconcileDDL(ctx, schemaDef, dropRecord); err != nil || result != connector.DDLReconcileNotApplied {
		t.Fatalf("reconcile unapplied drop result=%v error=%v", result, err)
	}
	if err := dest.ApplyDDL(ctx, schemaDef, dropRecord); err != nil {
		t.Fatalf("apply plan drop ddl: %v", err)
	}
	if result, err := dest.ReconcileDDL(ctx, schemaDef, dropRecord); err != nil || result != connector.DDLReconcileApplied {
		t.Fatalf("reconcile applied drop result=%v error=%v", result, err)
	}

	var columnCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`,
		"public", tableName, "title",
	).Scan(&columnCount); err != nil {
		t.Fatalf("query dropped column metadata: %v", err)
	}
	if columnCount != 0 {
		t.Fatalf("expected title column dropped from plan change, got %d", columnCount)
	}
}

func TestPostgresManagedDriverMarkerReconciles(t *testing.T) {
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

	const tableName = "wallaby_managed_receipt_test"
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_managed_receipt_test; CREATE TABLE public.wallaby_managed_receipt_test (id bigint PRIMARY KEY, value text)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS public.wallaby_managed_receipt_test")
	}()

	destination := &pgdest.Destination{}
	if err := destination.Open(ctx, connector.Spec{Name: "managed", Options: map[string]string{
		"dsn": dsn, "schema": "public", "write_mode": "target", "meta_table_enabled": "false",
	}}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(ctx)

	batch := connector.Batch{
		Schema: testManagedUpsertSchema(tableName), Records: []connector.Record{{Table: tableName, Operation: connector.OpInsert, Key: recordKey(t, map[string]any{"id": 1}), After: map[string]any{"id": 1, "value": "applied"}}}, Checkpoint: connector.Checkpoint{LSN: "0/80"}, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}},
	}
	intent := managedIntent(t, batch, fmt.Sprintf("commit-before-receipt-%d", time.Now().UnixNano()))
	evidence, err := destination.Apply(ctx, intent, batch)
	if err != nil {
		t.Fatal(err)
	}
	disposition, reconciled, err := destination.Reconcile(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if disposition != connector.DeliveryApplied || reconciled != evidence {
		t.Fatalf("reconcile=(%v,%+v), want applied/%+v", disposition, reconciled, evidence)
	}

	// A restart would construct a new destination instance. Reopen and prove
	// that the target marker makes the same delivery a no-op.
	if err := destination.Close(ctx); err != nil {
		t.Fatal(err)
	}
	destination = &pgdest.Destination{}
	if err := destination.Open(ctx, connector.Spec{Name: "managed", Options: map[string]string{
		"dsn": dsn, "schema": "public", "write_mode": "target", "meta_table_enabled": "false",
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := destination.Apply(ctx, intent, batch); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM public.wallaby_managed_receipt_test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("row count=%d, want one replay-convergent row", count)
	}
}

func TestPostgresTargetReplayConvergesIncludingMetadata(t *testing.T) {
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

	const tableName = "wallaby_meta_replay_test"
	flowID := fmt.Sprintf("managed-flow-%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_meta_replay_test; CREATE TABLE public.wallaby_meta_replay_test (id bigint PRIMARY KEY, value text)`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS public.wallaby_meta_replay_test") }()

	destination := &pgdest.Destination{}
	if err := destination.Open(ctx, connector.Spec{Name: "managed", Options: map[string]string{
		"dsn": dsn, "schema": "public", "write_mode": "target", "flow_id": flowID,
	}}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(ctx)

	batch := connector.Batch{
		Schema: testManagedUpsertSchema(tableName), Records: []connector.Record{{Table: tableName, Operation: connector.OpInsert, Key: recordKey(t, map[string]any{"id": 1}), After: map[string]any{"id": 1, "value": "once"}}}, Checkpoint: connector.Checkpoint{LSN: "0/90"}, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}},
	}
	intent := managedIntent(t, batch, fmt.Sprintf("metadata-replay-%d", time.Now().UnixNano()))
	if _, err := destination.Apply(ctx, intent, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := destination.Apply(ctx, intent, batch); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallaby_meta.__metadata WHERE flow_id = $1 AND lsn = $2`, flowID, batch.Checkpoint.LSN).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("metadata rows=%d, want one after managed replay", count)
	}
}

func TestPostgresTargetPreservesSameKeyOperationOrderIntegration(t *testing.T) {
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

	const tableName = "wallaby_operation_order_test"
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS public.wallaby_operation_order_test; CREATE TABLE public.wallaby_operation_order_test (id bigint PRIMARY KEY, value text); INSERT INTO public.wallaby_operation_order_test VALUES (1, 'old')`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS public.wallaby_operation_order_test")
	}()

	destination := &pgdest.Destination{}
	if err := destination.Open(ctx, connector.Spec{Name: "managed", Options: map[string]string{
		"dsn": dsn, "schema": "public", "write_mode": "target", "meta_table_enabled": "false",
	}}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(ctx)
	key := recordKey(t, map[string]any{"id": 1})
	batch := connector.Batch{
		Schema: testManagedUpsertSchema(tableName),
		Records: []connector.Record{
			{Table: tableName, Operation: connector.OpDelete, Key: key},
			{Table: tableName, Operation: connector.OpInsert, Key: key, After: map[string]any{"id": 1, "value": "new"}},
		},
		Checkpoint: connector.Checkpoint{LSN: "0/A0"}, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}},
	}
	if _, err := destination.Apply(ctx, managedIntent(t, batch, fmt.Sprintf("same-key-order-%d", time.Now().UnixNano())), batch); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := pool.QueryRow(ctx, "SELECT value FROM public.wallaby_operation_order_test WHERE id = 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "new" {
		t.Fatalf("value=%q, want new after delete then insert", value)
	}
}

func testManagedUpsertSchema(table string) connector.Schema {
	return connector.Schema{Namespace: "public", Name: table, Version: 1, Columns: []connector.Column{{Name: "id", Type: "bigint", TypeMetadata: map[string]string{"source_relation_id": "42", "source_column_id": "1", "primary_key": "true", "primary_key_ordinal": "1", "replica_identity": "true"}}, {Name: "value", Type: "text", TypeMetadata: map[string]string{"source_relation_id": "42", "source_column_id": "2"}}}}
}

func managedIntent(t *testing.T, batch connector.Batch, suffix string) connector.DeliveryIntent {
	t.Helper()
	hash, err := connector.BatchContentHash(batch)
	if err != nil {
		t.Fatal(err)
	}
	positionID := "position-" + suffix
	logicalBatchID, err := connector.DeliveryLogicalBatchID("source-lineage-1", positionID, hash)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{
		FlowID:                "managed-flow",
		FlowIncarnationID:     "incarnation-" + suffix,
		Generation:            1,
		AcquisitionID:         "acquisition-1",
		LeaseEpoch:            1,
		DestinationRevisionID: "postgres-target-1", SourceLineageID: "source-lineage-1", LogicalBatchID: logicalBatchID,
		PositionID:  positionID,
		ContentHash: hash,
	}
}
