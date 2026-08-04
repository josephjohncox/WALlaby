package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestSnowflakeManagedProfileFencedCleanSourceCut(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required for the managed Snowflake source-cut gate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
	if _, err := delivery.NewCoordinator(ctx, pool); err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	var version int
	var sourceSystem string
	if err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer,system_identifier::text FROM pg_catalog.pg_control_system()`).Scan(&version, &sourceSystem); err != nil {
		t.Fatal(err)
	}
	if version/10000 != 16 {
		t.Fatalf("managed Snowflake source-cut gate requires PostgreSQL 16, got %d", version)
	}

	suffix := strings.ToLower(strconv.FormatInt(time.Now().UnixNano(), 36))
	flowID := "snowflake-source-cut-" + suffix
	schemaName := "wallaby_sf_cut_" + suffix
	tableName := "widgets"
	publication := "wallaby_sf_cut_pub_" + suffix
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: connector.Spec{Name: "source", Type: connector.EndpointPostgres}, Destinations: []connector.Spec{{Name: "target", Type: connector.EndpointPostgres}}, Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.Spec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	firstFence, err := authorityStore.AcquireProducer(ctx, flowID, "source-cut-first", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	slot := bootstrap.GenerationSlotName(flowID, firstFence.FlowIncarnationID, 1)
	qualified := `"` + schemaName + `"."` + tableName + `"`
	schemaCreated := false
	publicationCreated := false
	slotOwned := false
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		var cleanupErrors []error
		if slotOwned {
			if _, err := pool.Exec(cleanupCtx, `SELECT pg_catalog.pg_drop_replication_slot($1)`, slot); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("drop slot: %w", err))
			}
		}
		if publicationCreated {
			if _, err := pool.Exec(cleanupCtx, `DROP PUBLICATION "`+publication+`"`); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("drop publication: %w", err))
			}
		}
		if schemaCreated {
			if _, err := pool.Exec(cleanupCtx, `DROP SCHEMA "`+schemaName+`" CASCADE`); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("drop schema: %w", err))
			}
		}
		if len(cleanupErrors) != 0 {
			t.Errorf("source-cut cleanup: %v", errors.Join(cleanupErrors...))
		}
	})
	if _, err := pool.Exec(ctx, `CREATE SCHEMA "`+schemaName+`"`); err != nil {
		t.Fatalf("create isolated source-cut schema without replacing existing resources: %v", err)
	}
	schemaCreated = true
	if _, err := pool.Exec(ctx, `CREATE TABLE `+qualified+` (id bigint PRIMARY KEY, value text)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE PUBLICATION "`+publication+`" FOR TABLE `+qualified+` WITH (publish='insert, update, delete')`); err != nil {
		t.Fatal(err)
	}
	publicationCreated = true
	publicationRevision, err := pgsource.PublicationFingerprint(ctx, pool, publication)
	if err != nil {
		t.Fatal(err)
	}
	spec := connector.Spec{Name: "snowflake-source-cut", Type: connector.EndpointPostgres, Options: map[string]string{
		"dsn": dsn, "slot": "managed", "publication": publication,
		"managed_profile": connector.ManagedProfilePostgresToSnowflakeSQLV1,
		"create_slot":     "true", "ensure_state": "false", "ensure_publication": "false", "sync_publication": "false",
		"publication_tables": schemaName + "." + tableName, "publication_revision": publicationRevision,
		"source_system_identifier": sourceSystem, "source_lineage_id": "lineage-" + suffix,
		"streaming_transactions": "true", "toast_fetch": "off",
		"max_transaction_records": "1000", "max_transaction_bytes": "8388608", "max_transaction_fragments": "128",
	}}
	if err := pool.QueryRow(ctx, `SELECT slot_name FROM pg_catalog.pg_create_logical_replication_slot($1,'pgoutput')`, slot).Scan(new(string)); err != nil {
		t.Fatal(err)
	}
	slotOwned = true
	preexistingCut := &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}
	if err := preexistingCut.BindRunFence(firstFence); err != nil {
		t.Fatal(err)
	}
	if err := preexistingCut.Open(ctx, spec); err == nil || !strings.Contains(err.Error(), "already exists without an authoritative checkpoint") {
		t.Fatalf("pre-existing slot admission error=%v", err)
	}
	var preserved bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)`, slot).Scan(&preserved); err != nil {
		t.Fatal(err)
	}
	if !preserved {
		t.Fatal("source-cut admission deleted a pre-existing unowned slot")
	}
	if _, err := pool.Exec(ctx, `SELECT pg_catalog.pg_drop_replication_slot($1)`, slot); err != nil {
		t.Fatal(err)
	}
	slotOwned = false

	failedCut := &pgsource.Source{
		ManagedControl: pool, ManagedAuthority: authorityStore,
		BootstrapHooks: bootstrap.Hooks{AfterSlotCreated: func(context.Context, bootstrap.ExportedSnapshot) error {
			return errors.New("injected source-cut crash before PostgreSQL checkpoint")
		}},
	}
	if err := failedCut.BindRunFence(firstFence); err != nil {
		t.Fatal(err)
	}
	if err := failedCut.Open(ctx, spec); err == nil || !strings.Contains(err.Error(), "injected source-cut crash") {
		t.Fatalf("source-cut crash error=%v", err)
	}
	var orphanSlot, orphanCheckpoint bool
	if err := pool.QueryRow(ctx, `SELECT
  EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1),
  EXISTS(SELECT 1 FROM authoritative_checkpoints WHERE flow_incarnation_id=$2)`, slot, firstFence.FlowIncarnationID).Scan(&orphanSlot, &orphanCheckpoint); err != nil {
		t.Fatal(err)
	}
	if orphanSlot || orphanCheckpoint {
		t.Fatalf("failed source cut left slot/checkpoint=%t/%t", orphanSlot, orphanCheckpoint)
	}

	catalogDriftCut := &pgsource.Source{
		ManagedControl: pool, ManagedAuthority: authorityStore,
		BootstrapHooks: bootstrap.Hooks{AfterSlotCreated: func(ctx context.Context, _ bootstrap.ExportedSnapshot) error {
			_, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN drift text`, qualified))
			return err
		}},
	}
	if err := catalogDriftCut.BindRunFence(firstFence); err != nil {
		t.Fatal(err)
	}
	if err := catalogDriftCut.Open(ctx, spec); err == nil || !strings.Contains(err.Error(), "catalog changed across slot consistent-point") {
		t.Fatalf("source catalog drift error=%v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DROP COLUMN drift`, qualified)); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT
  EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1),
  EXISTS(SELECT 1 FROM authoritative_checkpoints WHERE flow_incarnation_id=$2)`, slot, firstFence.FlowIncarnationID).Scan(&orphanSlot, &orphanCheckpoint); err != nil {
		t.Fatal(err)
	}
	if orphanSlot || orphanCheckpoint {
		t.Fatalf("catalog-drift source cut left slot/checkpoint=%t/%t", orphanSlot, orphanCheckpoint)
	}

	if _, err := pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s(id,value) VALUES(1,'preexisting')`, qualified)); err != nil {
		t.Fatal(err)
	}
	nonemptyCut := &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}
	if err := nonemptyCut.BindRunFence(firstFence); err != nil {
		t.Fatal(err)
	}
	if err := nonemptyCut.Open(ctx, spec); err == nil || !strings.Contains(err.Error(), "requires an empty PostgreSQL source relation") {
		t.Fatalf("nonempty source-cut error=%v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s`, qualified)); err != nil {
		t.Fatal(err)
	}

	first := &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}
	if err := first.BindRunFence(firstFence); err != nil {
		t.Fatal(err)
	}
	if err := first.Open(ctx, spec); err != nil {
		t.Fatal(err)
	}
	slotOwned = true
	initial, ok := first.InitialCheckpoint()
	if !ok {
		t.Fatal("new managed Snowflake slot did not expose its consistent point")
	}
	var checkpointLSN, resourceName, resourceState string
	if err := pool.QueryRow(ctx, `SELECT checkpoint.lsn,resource.physical_name,resource.state
FROM authoritative_checkpoints checkpoint
JOIN source_resources resource ON resource.flow_incarnation_id=checkpoint.flow_incarnation_id AND resource.resource_kind='slot'
WHERE checkpoint.flow_incarnation_id=$1`, firstFence.FlowIncarnationID).Scan(&checkpointLSN, &resourceName, &resourceState); err != nil {
		t.Fatal(err)
	}
	if checkpointLSN != initial.LSN || resourceName != slot || resourceState != "ready" {
		t.Fatalf("rooted source cut=%s/%s/%s, want %s/%s/ready", checkpointLSN, resourceName, resourceState, initial.LSN, slot)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE producer_leases SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, firstFence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	secondFence, err := authorityStore.AcquireProducer(ctx, flowID, "source-cut-second", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	restart := spec
	restart.Options = cloneTestOptions(spec.Options)
	restart.Options["create_slot"] = "false"
	restart.Options["start_lsn"] = initial.LSN
	second := &pgsource.Source{ManagedControl: pool, ManagedAuthority: authorityStore}
	if err := second.BindRunFence(secondFence); err != nil {
		t.Fatal(err)
	}
	if err := second.Open(ctx, restart); err != nil {
		t.Fatal(err)
	}
	if got, ok := second.InitialCheckpoint(); !ok || got.LSN != initial.LSN {
		t.Fatalf("restarted source checkpoint=%+v/%t, want %s", got, ok, initial.LSN)
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
