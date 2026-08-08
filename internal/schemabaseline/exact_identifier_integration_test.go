package schemabaseline_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/flow"
	internalschema "github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/internal/schemabaseline"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedSchemaBaselinesPreserveExactPostgresIdentifiersAcrossPauseResume(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
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
	baselineStore, err := schemabaseline.NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}

	flowID := fmt.Sprintf("exact-baseline-identifiers-%d", time.Now().UnixNano())
	sourceSchema := " "
	defer func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, pgx.Identifier{sourceSchema}.Sanitize()))
		_, _ = pool.Exec(context.Background(), `DELETE FROM public.flows WHERE id=$1`, flowID)
	}()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
CREATE SCHEMA %s;
CREATE TABLE %s."Events" (id bigint PRIMARY KEY);
CREATE TABLE %s.events (id bigint PRIMARY KEY);
CREATE TABLE %s." " (id bigint PRIMARY KEY)`, pgx.Identifier{sourceSchema}.Sanitize(), pgx.Identifier{sourceSchema}.Sanitize(), pgx.Identifier{sourceSchema}.Sanitize(), pgx.Identifier{sourceSchema}.Sanitize())); err != nil {
		t.Fatal(err)
	}
	var exactRelations int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM pg_catalog.pg_class AS relation
JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace
WHERE namespace.nspname=$1 AND relation.relname=ANY($2::text[])`, sourceSchema, []string{"Events", "events", " "}).Scan(&exactRelations); err != nil {
		t.Fatal(err)
	}
	if exactRelations != 3 {
		t.Fatalf("live PostgreSQL case/whitespace-distinct relations=%d, want 3", exactRelations)
	}

	destination := connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}
	definition := flow.Flow{
		ID: flowID, Name: flowID,
		Source:       schemaBaselineSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}),
		Destinations: []*wallabypb.Endpoint{schemaBaselineDestination(destination)}, State: flow.StateCreated,
		Config: flow.Config{TableMappings: flow.TableMappings{
			Version: flow.TableMappingsVersion,
			Destinations: []flow.DestinationTableMappings{{
				Destination: "target", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude},
				Tables: []flow.TableMapping{
					exactIdentifierMapping(sourceSchema, "Events", "upper_events"),
					exactIdentifierMapping(sourceSchema, "events", "lower_events"),
					exactIdentifierMapping(sourceSchema, " ", "blank_events"),
				},
			}},
		}},
	}
	if _, err := engine.Create(ctx, definition); err != nil {
		t.Fatal(err)
	}
	persistedFlow, err := engine.Get(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	persistedMappings := persistedFlow.Config.TableMappings.Destinations[0].Tables
	mappedSources := make(map[string]bool, len(persistedMappings))
	for _, mapping := range persistedMappings {
		if mapping.SourceSchema != sourceSchema {
			t.Fatalf("durable mapping changed exact source schema: %+v", mapping)
		}
		mappedSources[mapping.SourceTable] = true
	}
	if len(persistedMappings) != 3 || !mappedSources["Events"] || !mappedSources["events"] || !mappedSources[" "] {
		t.Fatalf("durable mappings changed exact source identities: %+v", persistedMappings)
	}
	if _, err := engine.Start(ctx, flowID); err != nil {
		t.Fatal(err)
	}
	control, err := engine.Control(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	firstFence, err := authorityStore.AcquireProducer(ctx, flowID, "exact-identifiers-first", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	initialSchemas := exactIdentifierSchemas(sourceSchema, false)
	persistBaselineTest(t, ctx, pool, firstFence, "exact-identifier-lineage", initialSchemas)
	loaded, err := baselineStore.Load(ctx, firstFence, "exact-identifier-lineage")
	if err != nil {
		t.Fatal(err)
	}
	assertExactBaselineNames(t, loaded)
	var baselineRows int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM ONLY public.managed_schema_baselines
WHERE flow_incarnation_id=$1 AND source_lineage_id=$2
  AND source_namespace=$3 AND source_relation=ANY($4::text[])`, firstFence.FlowIncarnationID, "exact-identifier-lineage", sourceSchema, []string{"Events", "events", " "}).Scan(&baselineRows); err != nil {
		t.Fatal(err)
	}
	if baselineRows != 3 {
		t.Fatalf("distinct exact baseline rows=%d, want 3", baselineRows)
	}

	_, pauseControl, err := engine.RequestPause(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	paused, err := engine.CompletePause(ctx, flowID, pauseControl.Generation)
	if err != nil || paused.State != flow.StatePaused {
		t.Fatalf("complete exact-identifier pause=(%s,%v)", paused.State, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE public.producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, firstFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s."Events" ADD COLUMN note text`, pgx.Identifier{sourceSchema}.Sanitize())); err != nil {
		t.Fatal(err)
	}
	_, resumeControl, err := engine.PlanStart(ctx, flowID, true)
	if err != nil {
		t.Fatal(err)
	}
	secondFence, err := authorityStore.AcquireProducer(ctx, flowID, "exact-identifiers-resumed", "test", resumeControl.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	resumedBaselines, err := baselineStore.Load(ctx, secondFence, "exact-identifier-lineage")
	if err != nil {
		t.Fatal(err)
	}
	assertExactBaselineNames(t, resumedBaselines)

	byIdentity := make(map[string]connector.Schema, len(resumedBaselines))
	for _, baseline := range resumedBaselines {
		byIdentity[connector.ManagedSchemaBaselineKey(baseline.Namespace, baseline.Name)] = baseline
	}
	currentSchemas := exactIdentifierSchemas(sourceSchema, true)
	var changed []connector.Schema
	for _, current := range currentSchemas {
		key := connector.ManagedSchemaBaselineKey(current.Namespace, current.Name)
		plan := internalschema.DiffPublishedShape(byIdentity[key], current)
		if plan.HasChanges() {
			changed = append(changed, current)
			if current.Name != "Events" || len(plan.Changes) != 1 || plan.Changes[0].Type != internalschema.ChangeAddColumn || plan.Changes[0].Column != "note" {
				t.Fatalf("unexpected exact-identifier DDL plan for %q: %+v", current.Name, plan)
			}
		}
	}
	if len(changed) != 1 || changed[0].Name != "Events" {
		t.Fatalf("pause/resume changed exact relations=%+v, want Events only", changed)
	}
	persistBaselineTest(t, ctx, pool, secondFence, "exact-identifier-lineage", changed)
	finalBaselines, err := baselineStore.Load(ctx, secondFence, "exact-identifier-lineage")
	if err != nil {
		t.Fatal(err)
	}
	assertExactBaselineNames(t, finalBaselines)
	generations := make(map[string]int64, 3)
	rows, err := pool.Query(ctx, `
SELECT source_relation,generation FROM ONLY public.managed_schema_baselines
WHERE flow_incarnation_id=$1 AND source_lineage_id=$2 AND source_namespace=$3`, secondFence.FlowIncarnationID, "exact-identifier-lineage", sourceSchema)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var generation int64
		if err := rows.Scan(&name, &generation); err != nil {
			t.Fatal(err)
		}
		generations[name] = generation
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if generations["Events"] != secondFence.Generation || generations["events"] != firstFence.Generation || generations[" "] != firstFence.Generation {
		t.Fatalf("exact baseline generations=%v, want only Events advanced to %d", generations, secondFence.Generation)
	}
}

func exactIdentifierMapping(sourceSchema, sourceTable, targetTable string) flow.TableMapping {
	return flow.TableMapping{
		SourceSchema: sourceSchema, SourceTable: sourceTable, Action: flow.MappingActionInclude,
		TargetSchema: "public", TargetTable: targetTable,
		FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{{ .Column }}"},
		Write:         flow.TableWritePolicy{Mode: flow.TableWriteModeAppend},
	}
}

func exactIdentifierSchemas(namespace string, changed bool) []connector.Schema {
	upperColumns := []connector.Column{{Name: "id", Type: "int8", Nullable: true}}
	if changed {
		upperColumns = append(upperColumns, connector.Column{Name: "note", Type: "text", Nullable: true})
	}
	return []connector.Schema{
		{Namespace: namespace, Name: "Events", Columns: upperColumns},
		{Namespace: namespace, Name: "events", Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: true}}},
		{Namespace: namespace, Name: " ", Columns: []connector.Column{{Name: "id", Type: "int8", Nullable: true}}},
	}
}

func assertExactBaselineNames(t *testing.T, schemas []connector.Schema) {
	t.Helper()
	seen := make(map[string]bool, len(schemas))
	for _, schema := range schemas {
		seen[schema.Name] = true
	}
	if len(schemas) != 3 || !seen["Events"] || !seen["events"] || !seen[" "] {
		t.Fatalf("exact baseline identities not preserved: %+v", schemas)
	}
}
