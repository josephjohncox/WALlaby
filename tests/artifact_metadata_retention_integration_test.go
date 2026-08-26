package tests

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/tablemap"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestArtifactMetadataRetentionBoundsConvergesAndPreservesCurrentRecovery(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	defer pool.Close()
	if store, err := checkpoint.NewPostgresStore(ctx, dsn); err != nil {
		t.Fatal(err)
	} else {
		store.Close()
	}
	if _, err := delivery.NewCoordinator(ctx, pool); err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := "artifact-metadata-retention-" + uuid.NewString()
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	destination := connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(destination), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{destination})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "artifact-metadata-retention", "test", control.Generation, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	mappings := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{Destination: "ice", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude}, Tables: []flow.TableMapping{{SourceSchema: "public", SourceTable: "events", Action: flow.MappingActionExclude}}}}}
	projector, err := tablemap.New(mappings, "ice")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := artifactlog.NewRuntime(ctx, pool, memoryMappedArtifactStore{}, artifactlog.RuntimeConfig{
		Stream: artifactlog.StreamConfig{
			ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: projector.Fingerprint(),
			HardRetainedBytes: 128 << 20, BacklogCountHigh: 100,
			BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour,
		},
		Projector: projector, OrphanGrace: time.Hour, Retention: time.Hour,
		MetadataRetention: 7 * 24 * time.Hour, MetadataMaxPublications: 2,
		MetadataMaxRows: 3, GCInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	var currentGrant connector.AckGrant
	for index := 0; index < 6; index++ {
		base := 0x100 + index*0x20
		transaction := artifactTransactionAt(uint32(1000+index), lsnForTest(base), lsnForTest(base+8), lsnForTest(base+16), "filtered")
		currentGrant, err = runtime.Append(ctx, fence, transaction, managedBaselinePayload(t, transaction))
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE artifact_publications SET published_at=clock_timestamp()-interval '8 days' WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID); err != nil {
		t.Fatal(err)
	}
	crashPruner, err := artifactlog.NewMetadataPruner(pool, artifactlog.WithMetadataPrunerHooks(artifactlog.MetadataPrunerHooks{Boundary: func(_ context.Context, boundary string, _ uuid.UUID) error {
		if boundary == "after_metadata_claim" {
			return errors.New("injected crash after durable metadata claim")
		}
		return nil
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crashPruner.Prune(ctx, fence, 7*24*time.Hour, 2, 3); err == nil {
		t.Fatal("metadata claim crash was not injected")
	}
	var afterCrashPublications, afterCrashClaims int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1),(SELECT count(*) FROM artifact_metadata_prune_claims WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&afterCrashPublications, &afterCrashClaims); err != nil {
		t.Fatal(err)
	}
	if afterCrashPublications != 6 || afterCrashClaims != 1 {
		t.Fatalf("durable claim crash publications/claims=%d/%d, want 6/1", afterCrashPublications, afterCrashClaims)
	}
	pruner, err := artifactlog.NewMetadataPruner(pool)
	if err != nil {
		t.Fatal(err)
	}
	for sweep := 0; sweep < 12; sweep++ {
		stats, err := pruner.Prune(ctx, fence, 7*24*time.Hour, 2, 3)
		if err != nil {
			t.Fatal(err)
		}
		if stats.PublicationsScanned > 2 || stats.RowsDeleted > 3 {
			t.Fatalf("sweep %d exceeded bounds: %+v", sweep, stats)
		}
		var publications, claims int
		if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1),(SELECT count(*) FROM artifact_metadata_prune_claims WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&publications, &claims); err != nil {
			t.Fatal(err)
		}
		if publications == 1 && claims == 0 {
			break
		}
		if sweep == 11 {
			t.Fatalf("metadata did not converge: publications=%d claims=%d", publications, claims)
		}
	}
	if _, err := runtime.RestoreCheckpoint(ctx, fence, currentGrant.Checkpoint); err != nil {
		t.Fatalf("current checkpoint recovery after pruning: %v", err)
	}
}

func lsnForTest(value int) string {
	const hex = "0123456789ABCDEF"
	if value < 0x100 || value > 0xFFF {
		panic("test LSN out of range")
	}
	return "0/" + string([]byte{hex[(value>>8)&0xF], hex[(value>>4)&0xF], hex[value&0xF]})
}
