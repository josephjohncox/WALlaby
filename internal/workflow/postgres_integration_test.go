package workflow

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/protobuf/proto"
)

func TestPostgresCustomEndpointsCreateGetUpdateListAndRestartWithInjectedRegistry(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	registry := connector.NewRegistry()
	if err := registry.RegisterSource("postgres-store-test-source", func() connector.Source { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterDestination("postgres-store-test-destination", func() connector.Destination { return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := NewPostgresEngineWithRegistry(ctx, dsn, registry)
	if err != nil {
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("custom-registry-%d", time.Now().UnixNano())
	destinationSpec := connector.RuntimeSpec{Name: "custom-destination", Type: "postgres-store-test-destination", Options: map[string]string{"destination": " exact "}}
	destination := &wallabypb.Endpoint{Name: destinationSpec.Name, Config: &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{ConnectorType: string(destinationSpec.Type), Options: destinationSpec.Options}}}
	source := &wallabypb.Endpoint{Name: "custom-source", Config: &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{ConnectorType: "postgres-store-test-source", Options: map[string]string{"source": " exact "}}}}
	definition := flow.Flow{
		ID:           flowID,
		Name:         "custom-before",
		Source:       source,
		Destinations: []*wallabypb.Endpoint{destination},
		Config:       flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{destinationSpec})},
	}
	created, err := store.Create(ctx, definition)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if !proto.Equal(created.Source, definition.Source) {
		store.Close()
		t.Fatalf("created source=%v", created.Source)
	}
	loaded, err := store.Get(ctx, flowID)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	loaded.Name = "custom-after"
	if _, err := store.Update(ctx, loaded); err != nil {
		store.Close()
		t.Fatal(err)
	}
	listed, err := store.List(ctx)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	found := false
	for _, item := range listed {
		found = found || item.ID == flowID
	}
	if !found {
		store.Close()
		t.Fatal("custom flow missing from List")
	}
	store.Close()

	restarted, err := NewPostgresEngineWithRegistry(ctx, dsn, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	defer func() { _, _ = restarted.pool.Exec(context.Background(), "DELETE FROM flows WHERE id=$1", flowID) }()
	afterRestart, err := restarted.Get(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	if afterRestart.Name != "custom-after" || !proto.Equal(afterRestart.Source, definition.Source) || afterRestart.Destinations[0].GetCustom().GetConnectorType() != string(destinationSpec.Type) || afterRestart.Source.GetCustom().GetOptions()["source"] != " exact " || afterRestart.Destinations[0].GetCustom().GetOptions()["destination"] != " exact " {
		t.Fatalf("restarted custom flow=%#v", afterRestart)
	}
}

func TestPostgresTableMappingChangeRotatesIncarnation(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	flowID := fmt.Sprintf("mapping-incarnation-%d", time.Now().UnixNano())
	defer func() { _, _ = store.pool.Exec(context.Background(), "DELETE FROM flows WHERE id=$1", flowID) }()
	created, err := store.Create(ctx, mappedTestFlow(flow.Flow{ID: flowID}))
	if err != nil {
		t.Fatal(err)
	}
	var first string
	if err := store.pool.QueryRow(ctx, "SELECT incarnation_id::text FROM flows WHERE id=$1", flowID).Scan(&first); err != nil {
		t.Fatal(err)
	}
	created.Config.TableMappings.Destinations[0].Tables = []flow.TableMapping{}
	created.Config.TableMappings.Destinations[0].FutureTables.Write.KeyColumns = []string{}
	created, err = store.Update(ctx, created)
	if err != nil {
		t.Fatal(err)
	}
	var canonical string
	if err := store.pool.QueryRow(ctx, "SELECT incarnation_id::text FROM flows WHERE id=$1", flowID).Scan(&canonical); err != nil {
		t.Fatal(err)
	}
	if first != canonical {
		t.Fatal("nil/empty canonical mapping change rotated postgres flow incarnation")
	}
	created.Config.TableMappings.Destinations[0].FutureTables.TargetTable = "mapped_{{ .Table }}"
	if _, err := store.Update(ctx, created); err != nil {
		t.Fatal(err)
	}
	var second string
	if err := store.pool.QueryRow(ctx, "SELECT incarnation_id::text FROM flows WHERE id=$1", flowID).Scan(&second); err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("table mapping change did not rotate postgres flow incarnation")
	}
	created, err = store.Get(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	created.WireFormat = "json"
	if _, err := store.Update(ctx, created); err != nil {
		t.Fatal(err)
	}
	var third string
	if err := store.pool.QueryRow(ctx, "SELECT incarnation_id::text FROM flows WHERE id=$1", flowID).Scan(&third); err != nil {
		t.Fatal(err)
	}
	if third == second {
		t.Fatal("wire-format change did not rotate postgres flow incarnation")
	}
	if _, err := store.Start(ctx, flowID); err != nil {
		t.Fatal(err)
	}
	created, err = store.Get(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	created.WireFormat = "proto"
	if _, err := store.Update(ctx, created); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("running wire-format update error=%v, want ErrInvalidState", err)
	}
}

func TestPostgresUpdateRejectsLegacyMissingMappingRow(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	flowID := fmt.Sprintf("legacy-mapping-update-%d", time.Now().UnixNano())
	defer func() { _, _ = store.pool.Exec(context.Background(), "DELETE FROM flows WHERE id=$1", flowID) }()
	typed := mappedTestFlow(flow.Flow{ID: flowID})
	sourceJSON, err := marshalPersistedEndpoint(typed.Source)
	if err != nil {
		t.Fatal(err)
	}
	destinationsJSON, err := marshalPersistedEndpoints(typed.Destinations)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `INSERT INTO flows(id,name,source,destinations,state,parallelism,config,lifecycle_target)
VALUES($1,$1,$2::jsonb,$3::jsonb,'created',1,'{"TableMappings":{"Version":1}}'::jsonb,'created')`, flowID, sourceJSON, destinationsJSON); err != nil {
		t.Fatal(err)
	}
	_, err = store.Update(ctx, mappedTestFlow(flow.Flow{ID: flowID}))
	if err == nil || !strings.Contains(err.Error(), "incompatible or missing table mappings") {
		t.Fatalf("Update() legacy row error=%v", err)
	}
}

func TestPostgresLifecycleGenerationAndQuiescentCompletion(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	flowID := fmt.Sprintf("lifecycle-fencing-%d", time.Now().UnixNano())
	defer func() { _, _ = store.pool.Exec(context.Background(), "DELETE FROM flows WHERE id=$1", flowID) }()
	if _, err := store.Create(ctx, mappedTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	started, control, err := store.PlanStart(ctx, flowID, false)
	if err != nil || started.State != flow.StateRunning || control.Generation != 1 {
		t.Fatalf("PlanStart()=(%s,%+v,%v)", started.State, control, err)
	}
	if err := store.RegisterExecutionGeneration(ctx, flowID, "exec", "test", control.Generation, time.Minute); err != nil {
		t.Fatal(err)
	}
	_, pauseControl, err := store.RequestPause(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompletePause(ctx, flowID, pauseControl.Generation); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("CompletePause(active) error=%v", err)
	}
	if err := store.RegisterExecutionGeneration(ctx, flowID, "late", "test", control.Generation, time.Minute); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("late registration error=%v", err)
	}
	if err := store.FinishExecutionReason(ctx, flowID, "exec", "test_done"); err != nil {
		t.Fatal(err)
	}
	paused, err := store.CompletePause(ctx, flowID, pauseControl.Generation)
	if err != nil || paused.State != flow.StatePaused {
		t.Fatalf("CompletePause()=(%s,%v)", paused.State, err)
	}
	_, stopControl, err := store.RequestStop(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := store.PendingControls(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foundStopping := false
	for _, item := range pending {
		if item.FlowID == flowID && item.State == flow.StateStopping && item.Target == TargetStopped {
			foundStopping = true
		}
	}
	if !foundStopping {
		t.Fatalf("PendingControls() omitted interrupted stopping flow; controls=%+v", pending)
	}
	if _, err := store.CompleteStopGeneration(ctx, flowID, stopControl.Generation); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresExactTerminalReconciliationRequiresExpiredLease(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := NewPostgresEngine(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	flowID := fmt.Sprintf("exact-terminal-%d", time.Now().UnixNano())
	defer func() { _, _ = store.pool.Exec(context.Background(), "DELETE FROM flows WHERE id=$1", flowID) }()
	if _, err := store.Create(ctx, mappedTestFlow(flow.Flow{ID: flowID})); err != nil {
		t.Fatal(err)
	}
	_, control, err := store.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterExecutionGeneration(ctx, flowID, "kube-exact", "kubernetes", control.Generation, 40*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterExecutionGeneration(ctx, flowID, "manual", "worker", control.Generation, 40*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileTerminatedExecutions(ctx, flowID, control.Generation, "kubernetes", []string{"kube-exact"}, "job_deleted"); err != nil {
		t.Fatal(err)
	}
	if active, err := store.ActiveExecutionsThrough(ctx, flowID, control.Generation); err != nil || active != 2 {
		t.Fatalf("active before expiry=(%d,%v), want 2", active, err)
	}
	time.Sleep(60 * time.Millisecond)
	if err := store.ReconcileTerminatedExecutions(ctx, flowID, control.Generation, "kubernetes", []string{"wrong"}, "job_deleted"); err != nil {
		t.Fatal(err)
	}
	if active, _ := store.ActiveExecutionsThrough(ctx, flowID, control.Generation); active != 2 {
		t.Fatalf("wrong exact id removed row; active=%d", active)
	}
	if err := store.ReconcileTerminatedExecutions(ctx, flowID, control.Generation, "kubernetes", []string{"kube-exact"}, "job_deleted"); err != nil {
		t.Fatal(err)
	}
	if active, err := store.ActiveExecutionsThrough(ctx, flowID, control.Generation); err != nil || active != 1 {
		t.Fatalf("active after exact reconciliation=(%d,%v), want unmatched backend row", active, err)
	}
}

func TestPostgresFlowLocksUseDedicatedPoolWithSingleNormalConnection(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := NewPostgresEngine(ctx, withPoolMaxConns(dsn, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	flowIDs := []string{
		fmt.Sprintf("lock-pool-left-%d", time.Now().UnixNano()),
		fmt.Sprintf("lock-pool-right-%d", time.Now().UnixNano()),
	}
	defer func() {
		_, _ = store.pool.Exec(context.Background(), "DELETE FROM flows WHERE id = ANY($1)", flowIDs)
	}()
	for _, flowID := range flowIDs {
		if _, err := store.Create(ctx, mappedTestFlow(flow.Flow{ID: flowID})); err != nil {
			t.Fatal(err)
		}
	}

	entered := make(chan string, len(flowIDs))
	release := make(chan struct{})
	done := make(chan error, len(flowIDs))
	for _, flowID := range flowIDs {
		flowID := flowID
		go func() {
			_, lockErr := store.WithFlowLock(ctx, flowID, false, func() error {
				entered <- flowID
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				_, controlErr := store.Control(ctx, flowID)
				return controlErr
			})
			done <- lockErr
		}()
	}
	for range flowIDs {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatalf("distinct flow locks did not enter concurrently: %v", ctx.Err())
		}
	}
	close(release)
	for range flowIDs {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	acquired, err := store.WithFlowLock(ctx, flowIDs[0], true, func() error {
		_, err := store.Control(ctx, flowIDs[0])
		return err
	})
	if err != nil || !acquired {
		t.Fatalf("lock after verified unlock=(%v,%v)", acquired, err)
	}
}

func TestMigration005RejectsLegacyRunningRowsAndCurrentRuntimeRejectsStableLegacyRows(t *testing.T) {
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

	legacySchema := fmt.Sprintf("wallaby_migration_legacy_%d", time.Now().UnixNano())
	legacyPool := newMigrationFixturePool(t, ctx, admin, dsn, legacySchema)
	defer legacyPool.Close()
	applyMigrationsThrough(t, ctx, legacyPool, 4)
	if _, err := legacyPool.Exec(ctx, `INSERT INTO flows(id,name,source,destinations,state) VALUES('legacy','legacy','{}','[]','running')`); err != nil {
		t.Fatal(err)
	}
	migration005, err := migrationFS.ReadFile("migrations/005_lifecycle_fencing.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacyPool.Exec(ctx, string(migration005)); err == nil || !strings.Contains(err.Error(), "requires a quiesced upgrade") {
		t.Fatalf("legacy running migration error=%v, want targeted quiesced-upgrade diagnostic", err)
	}

	stableSchema := fmt.Sprintf("wallaby_migration_stable_%d", time.Now().UnixNano())
	stablePool := newMigrationFixturePool(t, ctx, admin, dsn, stableSchema)
	defer stablePool.Close()
	applyMigrationsThrough(t, ctx, stablePool, 4)
	if _, err := stablePool.Exec(ctx, `INSERT INTO flows(id,name,source,destinations,state) VALUES
		('created','created','{}','[]','created'),
		('paused','paused','{}','[]','paused'),
		('failed','failed','{}','[]','failed')`); err != nil {
		t.Fatal(err)
	}
	if _, err := stablePool.Exec(ctx, string(migration005)); err != nil {
		t.Fatal(err)
	}
	// Upgrade the fixture through the current authority protocol before using
	// current PostgresEngine methods. The assertions above remain specific to
	// migration 005; migrations 006-007 add the provenance those methods require.
	for _, name := range []string{"006_authority_fences.sql", "007_authority_protocol_v2.sql"} {
		contents, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stablePool.Exec(ctx, string(contents)); err != nil {
			t.Fatalf("apply fixture migration %s: %v", name, err)
		}
	}
	lockCfg := stablePool.Config()
	lockCfg.MaxConns = 4
	lockPool, err := pgxpool.NewWithConfig(ctx, lockCfg)
	if err != nil {
		t.Fatal(err)
	}
	store := &PostgresEngine{pool: stablePool, lockPool: lockPool}
	defer lockPool.Close()
	for _, legacyFlowID := range []string{"created", "paused", "failed"} {
		if _, err := store.Get(ctx, legacyFlowID); err == nil || !strings.Contains(err.Error(), "endpoint config branch is required") {
			t.Fatalf("current runtime legacy flow %s error=%v, want typed endpoint rejection", legacyFlowID, err)
		}
	}
}

func newMigrationFixturePool(t *testing.T, ctx context.Context, admin *pgxpool.Pool, dsn, schema string) *pgxpool.Pool {
	t.Helper()
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cleanupPool, err := pgxpool.New(cleanupCtx, dsn)
		if err == nil {
			_, _ = cleanupPool.Exec(cleanupCtx, "DROP SCHEMA IF EXISTS "+identifier+" CASCADE")
			cleanupPool.Close()
		}
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	controlstore.ConfigurePool(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func applyMigrationsThrough(t *testing.T, ctx context.Context, pool *pgxpool.Pool, through int) {
	t.Helper()
	for version := 1; version <= through; version++ {
		matches, err := migrationFS.ReadDir("migrations")
		if err != nil {
			t.Fatal(err)
		}
		prefix := fmt.Sprintf("%03d_", version)
		var name string
		for _, match := range matches {
			if strings.HasPrefix(match.Name(), prefix) {
				name = match.Name()
				break
			}
		}
		if name == "" {
			t.Fatalf("migration %d not found", version)
		}
		contents, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(contents)); err != nil {
			t.Fatalf("apply fixture migration %s: %v", name, err)
		}
	}
}

func withPoolMaxConns(dsn string, max int) string {
	if parsed, err := url.Parse(dsn); err == nil && parsed.Scheme != "" {
		query := parsed.Query()
		query.Set("pool_max_conns", fmt.Sprint(max))
		parsed.RawQuery = query.Encode()
		return parsed.String()
	}
	return fmt.Sprintf("%s pool_max_conns=%d", dsn, max)
}
