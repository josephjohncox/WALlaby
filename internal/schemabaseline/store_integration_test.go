package schemabaseline_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/schemabaseline"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedSchemaBaselinesAreFencedIsolatedAndRestartable(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	controlstore.ConfigurePool(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	engine, err := workflow.NewPostgresEngineWithPool(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	store, err := schemabaseline.NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	firstFence := createBaselineTestFence(t, ctx, engine, authorityStore, "baseline-flow-a", "baseline-exec-a")
	secondFence := createBaselineTestFence(t, ctx, engine, authorityStore, "baseline-flow-b", "baseline-exec-b")
	firstSchema := connector.Schema{Namespace: "public", Name: "events", Version: 1, Columns: []connector.Column{{Name: "id", Type: "bigint"}}}
	secondSchema := connector.Schema{Namespace: "public", Name: "events", Version: 9, Columns: []connector.Column{{Name: "id", Type: "uuid"}}}
	persistBaselineTest(t, ctx, pool, firstFence, "lineage-a", []connector.Schema{firstSchema})
	persistBaselineTest(t, ctx, pool, secondFence, "lineage-b", []connector.Schema{secondSchema})
	first, err := store.Load(ctx, firstFence, "lineage-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Load(ctx, secondFence, "lineage-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Version != 0 || first[0].Columns[0].Type != "bigint" {
		t.Fatalf("first flow adopted wrong public.events baseline: %+v", first)
	}
	if len(second) != 1 || second[0].Version != 0 || second[0].Columns[0].Type != "uuid" {
		t.Fatalf("second flow adopted wrong public.events baseline: %+v", second)
	}
	if wrongLineage, err := store.Load(ctx, firstFence, "lineage-b"); err != nil || len(wrongLineage) != 0 {
		t.Fatalf("first flow adopted another lineage: baselines=%+v err=%v", wrongLineage, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO public.schema_versions(flow_id,namespace,name,version,schema_json,authority_origin) VALUES('','public','events',999,'{"Name":"events","Namespace":"public","Version":999,"Columns":[]}'::jsonb,'legacy_unfenced')`); err != nil {
		t.Fatal(err)
	}
	thirdFence := createBaselineTestFence(t, ctx, engine, authorityStore, "baseline-flow-c", "baseline-exec-c")
	if global, err := store.Load(ctx, thirdFence, "lineage-c"); err != nil || len(global) != 0 {
		t.Fatalf("managed flow adopted global/unfenced schema row: baselines=%+v err=%v", global, err)
	}

	if _, err := pool.Exec(ctx, `UPDATE public.producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, firstFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	restartedFence, err := authorityStore.AcquireProducer(ctx, firstFence.FlowID, "baseline-exec-a-restart", "test", firstFence.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	stalePayload, err := connector.NewManagedSchemaBaselinePayload("lineage-a", []connector.Schema{{Namespace: "public", Name: "events", Version: 2}})
	if err != nil {
		t.Fatal(err)
	}
	staleTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := schemabaseline.UpsertExactTx(ctx, staleTx, firstFence, stalePayload); !errors.Is(err, authority.ErrFenceRejected) {
		_ = staleTx.Rollback(ctx)
		t.Fatalf("stale baseline writer error=%v, want fence rejection", err)
	}
	_ = staleTx.Rollback(ctx)
	restarted, err := store.Load(ctx, restartedFence, "lineage-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted) != 1 || restarted[0].Version != 0 || restarted[0].Columns[0].Type != firstSchema.Columns[0].Type {
		t.Fatalf("same-generation restart baseline=%+v", restarted)
	}
	var adoptedAcquisition string
	if err := pool.QueryRow(ctx, `SELECT acquisition_id::text FROM ONLY public.managed_schema_baselines WHERE flow_incarnation_id=$1 AND source_lineage_id='lineage-a'`, restartedFence.FlowIncarnationID).Scan(&adoptedAcquisition); err != nil {
		t.Fatal(err)
	}
	if adoptedAcquisition != firstFence.AcquisitionID.String() {
		t.Fatalf("baseline writer acquisition=%s want immutable audit writer %s", adoptedAcquisition, firstFence.AcquisitionID)
	}
	if _, err := pool.Exec(ctx, `UPDATE ONLY public.managed_schema_baselines SET acquisition_id=$2,lease_epoch=$3 WHERE flow_incarnation_id=$1 AND source_lineage_id='lineage-a'`, restartedFence.FlowIncarnationID, secondFence.AcquisitionID, secondFence.LeaseEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(ctx, restartedFence, "lineage-a"); err == nil || !strings.Contains(err.Error(), "invalid or future writer provenance") {
		t.Fatalf("tampered managed baseline provenance error=%v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE ONLY public.managed_schema_baselines SET acquisition_id=$2,lease_epoch=$3 WHERE flow_incarnation_id=$1 AND source_lineage_id='lineage-a'`, restartedFence.FlowIncarnationID, restartedFence.AcquisitionID, restartedFence.LeaseEpoch); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `UPDATE ONLY public.managed_schema_baselines SET schema_json='{"Namespace":"public","Name":"events","Version":99,"Columns":[]}'::jsonb WHERE flow_incarnation_id=$1 AND source_lineage_id='lineage-a'`, restartedFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(ctx, restartedFence, "lineage-a"); err == nil || !strings.Contains(err.Error(), "fingerprint mismatch") {
		t.Fatalf("tampered managed baseline load error=%v", err)
	}
}

func persistBaselineTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fence connector.RunFence, lineage string, schemas []connector.Schema) {
	t.Helper()
	payload, err := connector.NewManagedSchemaBaselinePayload(lineage, schemas)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := schemabaseline.UpsertExactTx(ctx, tx, fence, payload); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func createBaselineTestFence(t *testing.T, ctx context.Context, engine *workflow.PostgresEngine, authorityStore *authority.PostgresStore, flowID, executionID string) connector.RunFence {
	t.Helper()
	destination := connector.Spec{Name: "target", Type: connector.EndpointPostgres}
	definition := flow.Flow{
		ID: flowID, Name: flowID, Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres},
		Destinations: []connector.Spec{destination}, State: flow.StateCreated,
		Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.Spec{destination})},
	}
	if _, err := engine.Create(ctx, definition); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, flowID); err != nil {
		t.Fatal(err)
	}
	control, err := engine.Control(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, executionID, "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return fence
}
