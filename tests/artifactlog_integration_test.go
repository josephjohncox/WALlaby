package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func TestMappedArtifactFilteredTransactionAdvancesWithoutObjectOrCatalogAttempt(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	checkpointStore, err := checkpoint.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	checkpointStore.Close()
	if _, err := delivery.NewCoordinator(ctx, pool); err != nil {
		t.Fatal(err)
	}
	flowID := "artifact-mapped-filtered-" + uuid.NewString()
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	authorityFlow := flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}
	if _, err := engine.Create(ctx, authorityFlow); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "artifact-worker", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	mappings := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{Destination: "ice", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude}, Tables: []flow.TableMapping{{SourceSchema: "public", SourceTable: "events", Action: flow.MappingActionExclude}}}}}
	projector, err := tablemap.New(mappings, "ice")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := artifactlog.NewRuntime(ctx, pool, memoryMappedArtifactStore{}, artifactlog.RuntimeConfig{Stream: artifactlog.StreamConfig{ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: projector.Fingerprint(), HardRetainedBytes: 128 << 20, BacklogCountHigh: 100, BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour}, Projector: projector, OrphanGrace: time.Hour, Retention: time.Hour, GCInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	transaction := artifactSourceTransaction()
	grant, err := runtime.Append(ctx, fence, transaction, managedBaselinePayload(t, transaction))
	if err != nil {
		t.Fatal(err)
	}
	if grant.Checkpoint.LSN != transaction.Checkpoint.LSN {
		t.Fatalf("filtered checkpoint=%s want %s", grant.Checkpoint.LSN, transaction.Checkpoint.LSN)
	}
	var publications, objects, deliveries int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1),(SELECT count(*) FROM artifact_objects WHERE flow_incarnation_id=$1),(SELECT count(*) FROM artifact_deliveries WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&publications, &objects, &deliveries); err != nil {
		t.Fatal(err)
	}
	if publications != 1 || objects != 0 || deliveries != 0 {
		t.Fatalf("filtered publication/object/delivery=%d/%d/%d", publications, objects, deliveries)
	}
	changed := mappings.Clone()
	changed.Destinations[0].Tables[0].SourceTable = "other_events"
	changedProjector, err := tablemap.New(changed, "ice")
	if err != nil {
		t.Fatal(err)
	}
	mismatched, err := artifactlog.NewRuntime(ctx, pool, memoryMappedArtifactStore{}, artifactlog.RuntimeConfig{Stream: artifactlog.StreamConfig{ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: changedProjector.Fingerprint(), HardRetainedBytes: 128 << 20, BacklogCountHigh: 100, BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour}, Projector: changedProjector, OrphanGrace: time.Hour, Retention: time.Hour, GCInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := mismatched.Recover(ctx, fence); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("recovery mapping fingerprint mismatch error=%v", err)
	}
}

func TestMappedArtifactFilteredBaselineCheckpointCrashIsAtomic(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
	if _, err := delivery.NewCoordinator(ctx, pool); err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	flowID := "artifact-filtered-baseline-crash-" + uuid.NewString()
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	destination := connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(destination), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{destination})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "artifact-filtered-crash", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	mappings := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{Destination: "ice", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionExclude}, Tables: []flow.TableMapping{{SourceSchema: "public", SourceTable: "events", Action: flow.MappingActionExclude}}}}}
	projector, err := tablemap.New(mappings, "ice")
	if err != nil {
		t.Fatal(err)
	}
	sourceTransaction := artifactSourceTransaction()
	projected, _, err := projector.ProjectTransaction(sourceTransaction)
	if err != nil {
		t.Fatal(err)
	}
	baseline := managedBaselinePayload(t, sourceTransaction)
	config := artifactlog.StreamConfig{ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: projector.Fingerprint(), HardRetainedBytes: 128 << 20, BacklogCountHigh: 100, BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour}
	failed := false
	publisher, err := artifactlog.NewPublisher(ctx, pool, memoryMappedArtifactStore{}, config, artifactlog.WithPublisherHooks(artifactlog.PublisherHooks{Boundary: func(_ context.Context, boundary string) error {
		if boundary == "before_publication_commit" && !failed {
			failed = true
			return errors.New("crash before filtered publication commit")
		}
		return nil
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(ctx, fence, projected, baseline); err == nil {
		t.Fatal("filtered publication crash was not injected")
	}
	var checkpoints, baselines int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=$1),(SELECT count(*) FROM managed_schema_baselines WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&checkpoints, &baselines); err != nil {
		t.Fatal(err)
	}
	if checkpoints != 0 || baselines != 0 {
		t.Fatalf("filtered crash checkpoint/baseline=%d/%d, want old/old", checkpoints, baselines)
	}
	if _, err := publisher.Publish(ctx, fence, projected, baseline); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=$1),(SELECT count(*) FROM managed_schema_baselines WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&checkpoints, &baselines); err != nil {
		t.Fatal(err)
	}
	if checkpoints != 1 || baselines != 1 {
		t.Fatalf("filtered retry checkpoint/baseline=%d/%d, want new/new", checkpoints, baselines)
	}
}

type metadataArtifactObject struct {
	body     []byte
	evidence artifactlog.ObjectEvidence
}
type metadataArtifactStore struct {
	mu      sync.Mutex
	objects map[string]metadataArtifactObject
	puts    int
}

func (s *metadataArtifactStore) Bucket() string { return "metadata-memory" }
func (s *metadataArtifactStore) PutImmutable(_ context.Context, key string, body []byte, checksum, projection, mapping string) (artifactlog.ObjectEvidence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !((projection == artifactlog.ProjectionIDV2 && len(mapping) == 64) || (projection == artifactlog.ProjectionID && mapping == "")) {
		return artifactlog.ObjectEvidence{}, errors.New("invalid upload metadata")
	}
	if existing, ok := s.objects[key]; ok {
		return existing.evidence, errors.New("immutable object exists")
	}
	s.puts++
	evidence := artifactlog.ObjectEvidence{Bucket: s.Bucket(), Key: key, VersionID: fmt.Sprintf("version-%d", s.puts), ChecksumSHA256: checksum, Length: int64(len(body)), ProjectionID: projection, MappingFingerprint: mapping}
	s.objects[key] = metadataArtifactObject{body: append([]byte(nil), body...), evidence: evidence}
	return evidence, nil
}
func (s *metadataArtifactStore) ReconcileVersion(_ context.Context, key, checksum string, length int64, projection, mapping string) (artifactlog.ObjectEvidence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.objects[key]
	if !ok {
		return artifactlog.ObjectEvidence{}, artifactlog.ErrObjectNotFound
	}
	if object.evidence.ChecksumSHA256 != checksum || object.evidence.Length != length || object.evidence.ProjectionID != projection || object.evidence.MappingFingerprint != mapping {
		return artifactlog.ObjectEvidence{}, connector.ErrDeliveryConflict
	}
	return object.evidence, nil
}
func (s *metadataArtifactStore) HeadVersion(_ context.Context, evidence artifactlog.ObjectEvidence) (artifactlog.ObjectEvidence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.objects[evidence.Key]
	if !ok {
		return artifactlog.ObjectEvidence{}, artifactlog.ErrObjectNotFound
	}
	if object.evidence != evidence {
		return artifactlog.ObjectEvidence{}, connector.ErrDeliveryConflict
	}
	return object.evidence, nil
}
func (*metadataArtifactStore) DeleteVersion(context.Context, artifactlog.ObjectEvidence) error {
	return nil
}

type countingArtifactCommitter struct{ commits, reconciles int }

func (c *countingArtifactCommitter) Commit(context.Context, artifactlog.CommitRequest) (artifactlog.CommitResult, error) {
	c.commits++
	return artifactlog.CommitResult{}, errors.New("unexpected catalog commit")
}
func (c *countingArtifactCommitter) Reconcile(context.Context, artifactlog.CommitRequest) (artifactlog.ReconcileResult, error) {
	c.reconciles++
	return artifactlog.ReconcileResult{}, errors.New("unexpected catalog reconcile")
}

func TestMappedArtifactCrashRetryPreservesMetadataAndPublicationIdentity(t *testing.T) {
	for _, boundary := range []string{"after_upload_evidence", "after_object_verified"} {
		t.Run(boundary, func(t *testing.T) {
			dsn := os.Getenv("TEST_PG_DSN")
			if dsn == "" {
				t.Skip("TEST_PG_DSN is required")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
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
			authorityStore, err := authority.NewPostgresStore(pool)
			if err != nil {
				t.Fatal(err)
			}
			checkpointStore, err := checkpoint.NewPostgresStore(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			checkpointStore.Close()
			if _, err := delivery.NewCoordinator(ctx, pool); err != nil {
				t.Fatal(err)
			}
			flowID := "artifact-v2-retry-" + uuid.NewString()
			defer cleanupAuthorityTest(context.Background(), pool, flowID)
			authorityFlow := flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}
			if _, err := engine.Create(ctx, authorityFlow); err != nil {
				t.Fatal(err)
			}
			_, control, err := engine.PlanStart(ctx, flowID, false)
			if err != nil {
				t.Fatal(err)
			}
			fence, err := authorityStore.AcquireProducer(ctx, flowID, "artifact-worker", "test", control.Generation, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			mappings := flow.NewTableMappings([]connector.RuntimeSpec{{Name: "ice", Type: connector.EndpointIceberg}})
			projector, err := tablemap.New(mappings, "ice")
			if err != nil {
				t.Fatal(err)
			}
			projected, _, err := projector.ProjectTransaction(artifactSourceTransaction())
			if err != nil {
				t.Fatal(err)
			}
			store := &metadataArtifactStore{objects: map[string]metadataArtifactObject{}}
			failed := false
			publisher, err := artifactlog.NewPublisher(ctx, pool, store, artifactlog.StreamConfig{ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: projector.Fingerprint(), HardRetainedBytes: 128 << 20, BacklogCountHigh: 100, BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour, Consumers: []string{"ice-v1"}}, artifactlog.WithPublisherHooks(artifactlog.PublisherHooks{Boundary: func(_ context.Context, observed string) error {
				if observed == boundary && !failed {
					failed = true
					return errors.New("injected crash")
				}
				return nil
			}}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := publisher.Publish(ctx, fence, projected, managedBaselinePayload(t, projected)); err == nil || !strings.Contains(err.Error(), "injected crash") {
				t.Fatalf("first publish error=%v", err)
			}
			var publicationCount int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM artifact_publications WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&publicationCount); err != nil {
				t.Fatal(err)
			}
			if publicationCount != 0 {
				t.Fatal("publication committed before retry")
			}
			first, err := publisher.Publish(ctx, fence, projected, managedBaselinePayload(t, projected))
			if err != nil {
				t.Fatal(err)
			}
			second, err := publisher.Publish(ctx, fence, projected, managedBaselinePayload(t, projected))
			if err != nil {
				t.Fatal(err)
			}
			if first.ID != second.ID {
				t.Fatalf("retry publication IDs differ: %s %s", first.ID, second.ID)
			}
			if store.puts != 1 {
				t.Fatalf("immutable puts=%d want 1", store.puts)
			}
			var storedProjection, storedMapping string
			if err := pool.QueryRow(ctx, `SELECT projection_id,mapping_fingerprint FROM artifact_objects WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&storedProjection, &storedMapping); err != nil {
				t.Fatal(err)
			}
			if storedProjection != artifactlog.ProjectionIDV2 || storedMapping != projector.Fingerprint() {
				t.Fatalf("stored object identity=%s/%s", storedProjection, storedMapping)
			}
			committer := &countingArtifactCommitter{}
			consumer, err := artifactlog.NewConsumer(pool, committer)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := consumer.ConsumeNext(ctx, fence, "ice-v1"); err == nil || !strings.Contains(err.Error(), "unexpected catalog commit") {
				t.Fatalf("prime indeterminate attempt error=%v", err)
			}
			if committer.commits != 1 || committer.reconciles != 0 {
				t.Fatalf("prime catalog calls=%d/%d", committer.commits, committer.reconciles)
			}
			if boundary == "after_upload_evidence" {
				if _, err := pool.Exec(ctx, `UPDATE artifact_objects SET projection_id=$2,mapping_fingerprint='' WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID, artifactlog.ProjectionID); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := pool.Exec(ctx, `UPDATE artifact_objects SET mapping_fingerprint=$2 WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID, strings.Repeat("b", 64)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := consumer.ConsumeNext(ctx, fence, "ice-v1"); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
				t.Fatalf("tampered object identity error=%v", err)
			}
			if committer.commits != 1 || committer.reconciles != 0 {
				t.Fatalf("tampered object reached catalog reconciliation; calls=%d/%d", committer.commits, committer.reconciles)
			}
		})
	}
}

func TestArtifactCatalogAttemptNotAppliedRetryAndConflictStaySingleIdentity(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	databasePool, databaseCleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "artifact_attempt_recovery")
	dsn = isolatedDatabaseDSN(t, ctx, databasePool, dsn)
	defer databaseCleanup()
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
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	checkpointStore, err := checkpoint.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	checkpointStore.Close()
	if _, err := delivery.NewCoordinator(ctx, pool); err != nil {
		t.Fatal(err)
	}
	flowID := "artifact-attempt-current-" + uuid.NewString()
	defer cleanupAuthorityTest(context.Background(), pool, flowID)
	flowDef := flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}
	if _, err := engine.Create(ctx, flowDef); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "artifact-worker", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	store := &metadataArtifactStore{objects: map[string]metadataArtifactObject{}}
	publisher, err := artifactlog.NewPublisher(ctx, pool, store, artifactlog.StreamConfig{ProjectionID: artifactlog.ProjectionID, HardRetainedBytes: 128 << 20, BacklogCountHigh: 100, BacklogBytesHigh: 128 << 20, BacklogAgeHigh: time.Hour, Consumers: []string{"ice-current"}})
	if err != nil {
		t.Fatal(err)
	}
	firstTransaction := artifactSourceTransaction()
	first, err := publisher.Publish(ctx, fence, firstTransaction, managedBaselinePayload(t, firstTransaction))
	if err != nil {
		t.Fatal(err)
	}
	retry := &catalogAttemptTestCommitter{failCommit: true}
	consumer, err := artifactlog.NewConsumer(pool, retry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.ConsumeNext(ctx, fence, "ice-current"); err == nil {
		t.Fatal("catalog attempt did not fail at injected commit boundary")
	}
	if consumed, err := consumer.ConsumeNext(ctx, fence, "ice-current"); err != nil || !consumed {
		t.Fatalf("not-applied recovery consumed/error=%t/%v", consumed, err)
	}
	var attempts, receipts int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_delivery_attempts WHERE publication_id=$1),(SELECT count(*) FROM artifact_delivery_receipts WHERE publication_id=$1)`, first.ID).Scan(&attempts, &receipts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || receipts != 1 || retry.reconciles != 1 || retry.commits != 2 {
		t.Fatalf("retry attempt/receipt/reconcile/commit=%d/%d/%d/%d", attempts, receipts, retry.reconciles, retry.commits)
	}
	secondTransaction := artifactTransactionAt(101, "0/E0", "0/E8", "0/F0", "conflict")
	second, err := publisher.Publish(ctx, fence, secondTransaction, managedBaselinePayload(t, secondTransaction))
	if err != nil {
		t.Fatal(err)
	}
	committed := &catalogAttemptTestCommitter{}
	crashing, err := artifactlog.NewConsumer(pool, committed, artifactlog.WithConsumerHooks(artifactlog.ConsumerHooks{Reach: func(_ context.Context, boundary string) error {
		if boundary == "after_catalog_commit" {
			return errors.New("injected crash after catalog commit")
		}
		return nil
	}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := crashing.ConsumeNext(ctx, fence, "ice-current"); err == nil {
		t.Fatal("catalog commit crash boundary did not fire")
	}
	conflicting := &catalogAttemptTestCommitter{conflict: true}
	recovery, err := artifactlog.NewConsumer(pool, conflicting)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.ConsumeNext(ctx, fence, "ice-current"); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("catalog reconciliation conflict error=%v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_delivery_attempts WHERE publication_id=$1),(SELECT count(*) FROM artifact_delivery_receipts WHERE publication_id=$1)`, second.ID).Scan(&attempts, &receipts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || receipts != 0 {
		t.Fatalf("conflicting attempt/receipt=%d/%d", attempts, receipts)
	}
}

type catalogAttemptTestCommitter struct {
	commits, reconciles  int
	failCommit, conflict bool
}

func (c *catalogAttemptTestCommitter) Commit(_ context.Context, request artifactlog.CommitRequest) (artifactlog.CommitResult, error) {
	c.commits++
	if c.failCommit {
		c.failCommit = false
		return artifactlog.CommitResult{}, errors.New("injected catalog commit failure")
	}
	return catalogAttemptResult(request), nil
}
func (c *catalogAttemptTestCommitter) Reconcile(_ context.Context, request artifactlog.CommitRequest) (artifactlog.ReconcileResult, error) {
	c.reconciles++
	if c.conflict {
		result := catalogAttemptResult(request)
		result.ManifestSHA256 = "conflicting-manifest"
		return artifactlog.ReconcileResult{Disposition: artifactlog.CommitApplied, Commit: result}, nil
	}
	return artifactlog.ReconcileResult{Disposition: artifactlog.CommitNotApplied}, nil
}
func catalogAttemptResult(request artifactlog.CommitRequest) artifactlog.CommitResult {
	return artifactlog.CommitResult{SnapshotID: "snapshot-" + request.PublicationID.String(), SnapshotIDs: map[string]string{"test": "snapshot-" + request.PublicationID.String()}, ManifestSHA256: request.ManifestSHA256, CommitID: request.CommitID, LogicalBatchID: request.LogicalBatchID}
}

type memoryMappedArtifactStore struct{}

func (memoryMappedArtifactStore) Bucket() string { return "memory" }
func (memoryMappedArtifactStore) PutImmutable(context.Context, string, []byte, string, string, string) (artifactlog.ObjectEvidence, error) {
	return artifactlog.ObjectEvidence{}, errors.New("unexpected object upload")
}
func (memoryMappedArtifactStore) ReconcileVersion(context.Context, string, string, int64, string, string) (artifactlog.ObjectEvidence, error) {
	return artifactlog.ObjectEvidence{}, artifactlog.ErrObjectNotFound
}
func (memoryMappedArtifactStore) HeadVersion(context.Context, artifactlog.ObjectEvidence) (artifactlog.ObjectEvidence, error) {
	return artifactlog.ObjectEvidence{}, errors.New("unexpected object head")
}
func (memoryMappedArtifactStore) DeleteVersion(context.Context, artifactlog.ObjectEvidence) error {
	return errors.New("unexpected object delete")
}

func TestCanonicalArtifactS3AdmissionRequiresEnabledVersioning(t *testing.T) {
	endpoint := os.Getenv("WALLABY_TEST_S3_ENDPOINT")
	accessKey := os.Getenv("WALLABY_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("WALLABY_TEST_S3_SECRET_KEY")
	region := os.Getenv("WALLABY_TEST_S3_REGION")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("S3 integration environment is required")
	}
	if region == "" {
		region = "us-east-1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := newS3Client(ctx, endpoint, region, accessKey, secretKey)
	if err != nil {
		t.Fatal(err)
	}
	bucket := "wallaby-v-" + strings.ToLower(uuid.NewString())
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
	config := artifactlog.S3Config{
		Bucket: bucket, Region: region, Endpoint: endpoint,
		AccessKey: accessKey, SecretKey: secretKey, ForcePathStyle: true,
	}
	if _, err := artifactlog.NewS3Store(ctx, config); err == nil {
		t.Fatal("unversioned bucket was admitted for immutable artifacts")
	}
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := artifactlog.NewS3Store(ctx, config); err != nil {
		t.Fatalf("enabled versioning was rejected: %v", err)
	}
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := artifactlog.NewS3Store(ctx, config); err == nil {
		t.Fatal("versioning-suspended bucket was admitted for immutable artifacts")
	}
}

func TestCanonicalArtifactPublicationRecovery(t *testing.T) {
	endpoint := os.Getenv("WALLABY_TEST_S3_ENDPOINT")
	bucket := os.Getenv("WALLABY_TEST_S3_BUCKET")
	accessKey := os.Getenv("WALLABY_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("WALLABY_TEST_S3_SECRET_KEY")
	region := os.Getenv("WALLABY_TEST_S3_REGION")
	dsn := os.Getenv("TEST_PG_DSN")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" || dsn == "" {
		t.Skip("PostgreSQL and S3 integration environment is required")
	}
	if region == "" {
		region = "us-east-1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := newS3Client(ctx, endpoint, region, accessKey, secretKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) {
			t.Fatal(err)
		}
	}
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatal(err)
	}
	objects, err := artifactlog.NewS3Store(ctx, artifactlog.S3Config{
		Bucket: bucket, Region: region, Endpoint: endpoint,
		AccessKey: accessKey, SecretKey: secretKey, ForcePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}

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
	authorityStore, err := authority.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	checkpointStore, err := checkpoint.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	checkpointStore.Close()
	if _, err := delivery.NewCoordinator(ctx, pool); err != nil {
		t.Fatal(err)
	}
	flowID := fmt.Sprintf("artifact-publication-%d", time.Now().UnixNano())
	defer cleanupAuthorityTest(ctx, pool, flowID)
	if _, err := engine.Create(ctx, flow.Flow{ID: flowID, Source: testFlowSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres}), Destinations: testFlowDestinations(connector.RuntimeSpec{Name: "target", Type: connector.EndpointPostgres}), Config: flow.Config{TableMappings: flow.NewTableMappings([]connector.RuntimeSpec{{Name: "target", Type: connector.EndpointPostgres}})}}); err != nil {
		t.Fatal(err)
	}
	_, control, err := engine.PlanStart(ctx, flowID, false)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := authorityStore.AcquireProducer(ctx, flowID, "artifact-worker", "test", control.Generation, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	transaction := artifactSourceTransaction()
	config := artifactlog.StreamConfig{
		HardRetainedBytes: 128 << 20,
		BacklogCountHigh:  100,
		BacklogBytesHigh:  128 << 20,
		Consumers:         []string{"iceberg-append-v1"},
	}
	failingObjects := &failBeforePutStore{ObjectStore: objects, fail: true}
	publisher, err := artifactlog.NewPublisher(ctx, pool, failingObjects, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(ctx, fence, transaction, managedBaselinePayload(t, transaction)); err == nil {
		t.Fatal("expected injected failure after durable quota reservation and before upload")
	}
	var reservedBefore int64
	if err := pool.QueryRow(ctx, `SELECT reserved_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&reservedBefore); err != nil {
		t.Fatal(err)
	}
	if reservedBefore <= 0 {
		t.Fatalf("reserved bytes=%d, want durable positive reservation after crash", reservedBefore)
	}

	publisher, err = artifactlog.NewPublisher(ctx, pool, objects, config)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := publisher.Publish(ctx, fence, transaction, managedBaselinePayload(t, transaction))
	if err != nil {
		t.Fatal(err)
	}
	if publication.AckGrant.Checkpoint.LSN != transaction.EndLSN || len(publication.Artifacts) != 1 {
		t.Fatalf("publication=(checkpoint:%s artifacts:%d), want %s/1", publication.AckGrant.Checkpoint.LSN, len(publication.Artifacts), transaction.EndLSN)
	}
	artifact := publication.Artifacts[0]
	var state, versionID string
	var encodedLength, rootedBytes, reservedBytes int64
	if err := pool.QueryRow(ctx, `SELECT state,version_id,encoded_length FROM artifact_objects WHERE artifact_id=$1`, artifact.ID).Scan(&state, &versionID, &encodedLength); err != nil {
		t.Fatal(err)
	}
	if versionID == "" || state != "rooted" {
		t.Fatalf("artifact state/version=(%s,%q), want rooted/exact version", state, versionID)
	}
	if err := pool.QueryRow(ctx, `SELECT rooted_bytes,reserved_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&rootedBytes, &reservedBytes); err != nil {
		t.Fatal(err)
	}
	if rootedBytes != encodedLength || reservedBytes != 0 {
		t.Fatalf("quota rooted/reserved=(%d,%d), want (%d,0)", rootedBytes, reservedBytes, encodedLength)
	}
	var deliveryCount, ackCount, checkpointCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM artifact_deliveries WHERE flow_incarnation_id=$1 AND consumer_revision_id='iceberg-append-v1'`, fence.FlowIncarnationID).Scan(&deliveryCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM source_ack_intents WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&ackCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM authoritative_checkpoints WHERE flow_incarnation_id=$1 AND lsn=$2`, fence.FlowIncarnationID, transaction.EndLSN).Scan(&checkpointCount); err != nil {
		t.Fatal(err)
	}
	if deliveryCount != 1 || ackCount != 1 || checkpointCount != 1 {
		t.Fatalf("publication roots delivery/ack/checkpoint=(%d,%d,%d), want 1/1/1", deliveryCount, ackCount, checkpointCount)
	}

	catalog := &recordingAppendCatalog{snapshotID: "snapshot-1"}
	consumer, err := artifactlog.NewConsumer(pool, catalog, artifactlog.WithConsumerHooks(artifactlog.ConsumerHooks{
		Reach: func(_ context.Context, boundary string) error {
			if boundary == "after_catalog_commit" {
				return errors.New("injected crash after Iceberg commit before PostgreSQL receipt")
			}
			return nil
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if consumed, err := consumer.ConsumeNext(ctx, fence, "iceberg-append-v1"); err == nil || consumed {
		t.Fatalf("commit-before-receipt injection consumed/error=%t/%v", consumed, err)
	}
	consumer, err = artifactlog.NewConsumer(pool, catalog)
	if err != nil {
		t.Fatal(err)
	}
	consumed, err := consumer.ConsumeNext(ctx, fence, "iceberg-append-v1")
	if err != nil {
		t.Fatal(err)
	}
	if !consumed || catalog.appendCalls != 1 {
		t.Fatalf("Iceberg reconciliation consumed=%t append calls=%d, want true/1", consumed, catalog.appendCalls)
	}
	consumed, err = consumer.ConsumeNext(ctx, fence, "iceberg-append-v1")
	if err != nil {
		t.Fatal(err)
	}
	if consumed {
		t.Fatal("artifact consumer replayed an already receipted publication")
	}
	var consumerReceipts, consumerAttempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM artifact_delivery_attempts WHERE flow_incarnation_id=$1 AND publication_id=$2`, fence.FlowIncarnationID, publication.ID).Scan(&consumerAttempts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM artifact_delivery_receipts WHERE flow_incarnation_id=$1 AND publication_id=$2`, fence.FlowIncarnationID, publication.ID).Scan(&consumerReceipts); err != nil {
		t.Fatal(err)
	}
	if consumerAttempts != 1 || consumerReceipts != 1 {
		t.Fatalf("artifact consumer attempts/receipts=%d/%d, want 1/1", consumerAttempts, consumerReceipts)
	}
	var consumerSequence int64
	var consumerPosition, consumerCommitID string
	if err := pool.QueryRow(ctx, `
SELECT publication_sequence,position_id,commit_id
FROM artifact_consumer_checkpoints
WHERE flow_incarnation_id=$1 AND consumer_revision_id=$2`, fence.FlowIncarnationID, "iceberg-append-v1").Scan(
		&consumerSequence, &consumerPosition, &consumerCommitID,
	); err != nil {
		t.Fatal(err)
	}
	if consumerSequence != publication.Sequence || consumerPosition != publication.AckGrant.PositionID || !strings.HasPrefix(consumerCommitID, "wallaby-iceberg-") {
		t.Fatalf("consumer checkpoint=%d/%s/%s", consumerSequence, consumerPosition, consumerCommitID)
	}

	evidence := artifactlog.ObjectEvidence{
		Bucket: bucket, Key: artifact.ObjectKey, VersionID: versionID,
		ChecksumSHA256: artifact.EncodedByteHash, Length: encodedLength, ProjectionID: artifactlog.ProjectionID,
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(artifact.ObjectKey)}); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.HeadVersion(ctx, evidence); err != nil {
		t.Fatalf("exact rooted version was hidden by a later delete marker: %v", err)
	}
	if err := publisher.RecomputeQuota(ctx, fence); err != nil {
		t.Fatal(err)
	}
	var recomputedRooted, recomputedReserved int64
	if err := pool.QueryRow(ctx, `SELECT rooted_bytes,reserved_bytes FROM artifact_quota_accounts WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&recomputedRooted, &recomputedReserved); err != nil {
		t.Fatal(err)
	}
	if recomputedRooted != rootedBytes || recomputedReserved != 0 {
		t.Fatalf("recomputed quota=(%d,%d), want (%d,0)", recomputedRooted, recomputedReserved, rootedBytes)
	}
	if err := objects.DeleteVersion(ctx, evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(ctx, fence, transaction, managedBaselinePayload(t, transaction)); err == nil {
		t.Fatal("retry unexpectedly authorized ACK after rooted exact object version was deleted")
	}
}

type recordingAppendCatalog struct {
	snapshotID   string
	appendCalls  int
	objectCount  int
	barrierCount int
}

func (c *recordingAppendCatalog) Commit(_ context.Context, request artifactlog.CommitRequest) (artifactlog.CommitResult, error) {
	if len(request.Objects) == 0 && len(request.Barriers) == 0 {
		return artifactlog.CommitResult{}, errors.New("commit received no rooted objects or barriers")
	}
	c.appendCalls++
	c.objectCount += len(request.Objects)
	c.barrierCount += len(request.Barriers)
	return c.result(request), nil
}

func (c *recordingAppendCatalog) Reconcile(_ context.Context, request artifactlog.CommitRequest) (artifactlog.ReconcileResult, error) {
	return artifactlog.ReconcileResult{Disposition: artifactlog.CommitApplied, Commit: c.result(request)}, nil
}

func (c *recordingAppendCatalog) result(request artifactlog.CommitRequest) artifactlog.CommitResult {
	snapshotID := c.snapshotID
	if snapshotID == "" {
		snapshotID = "snapshot-" + request.PublicationID.String()
	}
	return artifactlog.CommitResult{
		SnapshotID: snapshotID, SnapshotIDs: map[string]string{"test": snapshotID},
		ManifestSHA256: request.ManifestSHA256, CommitID: request.CommitID,
		LogicalBatchID: request.LogicalBatchID,
	}
}

type failBeforePutStore struct {
	artifactlog.ObjectStore
	fail bool
}

func (s *failBeforePutStore) PutImmutable(ctx context.Context, key string, body []byte, digest, projectionID, mappingFingerprint string) (artifactlog.ObjectEvidence, error) {
	if s.fail {
		return artifactlog.ObjectEvidence{}, errors.New("injected crash before immutable PUT")
	}
	return s.ObjectStore.PutImmutable(ctx, key, body, digest, projectionID, mappingFingerprint)
}

func artifactSourceTransaction() connector.SourceTransaction {
	return connector.SourceTransaction{
		SourceLineageID: "postgres-system-1/artifact-publication-v1",
		TransactionID:   100,
		BeginLSN:        "0/C0",
		CommitLSN:       "0/C8",
		EndLSN:          "0/D0",
		Checkpoint:      connector.Checkpoint{LSN: "0/D0", Timestamp: time.Unix(1000, 0).UTC()},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{
				Schema: connector.Schema{Namespace: "public", Name: "artifact_events", Version: 1, Columns: []connector.Column{
					{Name: "id", Type: "int8", TypeMetadata: map[string]string{"source_relation_id": "84", "source_column_id": "1"}},
					{Name: "value", Type: "text", TypeMetadata: map[string]string{"source_relation_id": "84", "source_column_id": "2"}},
				}},
				Records: []connector.Record{{Table: "artifact_events", Operation: connector.OpInsert, SchemaVersion: 1, Key: []byte(`{"id":1}`), After: map[string]any{"id": int64(1), "value": "canonical"}, Timestamp: time.Unix(999, 0).UTC()}},
			},
		}},
	}
}
