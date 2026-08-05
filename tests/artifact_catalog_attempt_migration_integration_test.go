package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestFreshArtifactCatalogAttemptIdentitySchema(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "artifact_attempt_fresh")
	defer cleanup()
	if _, err := pool.Exec(ctx, `DROP EXTENSION IF EXISTS pgcrypto`); err != nil {
		t.Fatal(err)
	}
	if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var history, constraints, pgcrypto int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM wallaby_control_migrations WHERE domain='artifactlog' AND version='007_current_catalog_attempt_identity.sql'`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_constraint WHERE conname IN ('artifact_delivery_attempts_current_identity','artifact_delivery_attempts_publication_unique','artifact_delivery_attempts_commit_unique','artifact_delivery_receipts_current_identity','artifact_delivery_receipts_attempt_unique') AND convalidated`).Scan(&constraints); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_extension WHERE extname='pgcrypto'`).Scan(&pgcrypto); err != nil {
		t.Fatal(err)
	}
	if history != 1 || constraints != 5 || pgcrypto != 0 {
		t.Fatalf("artifact current history/constraints/pgcrypto=%d/%d/%d", history, constraints, pgcrypto)
	}
}

func TestArtifactCatalogAttemptMigrationAcceptsCanonicalIdentity(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "artifact_attempt_current")
	defer cleanup()
	prepareArtifactAttemptMigrationFixture(t, ctx, pool)
	identity := insertArtifactAttemptFixture(t, ctx, pool, artifactAttemptFixture{})
	insertArtifactReceiptAndCheckpoint(t, ctx, pool, identity)
	if err := artifactlog.ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("apply canonical catalog-attempt migration: %v", err)
	}
	var attempts, receipts, checkpoints int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM artifact_delivery_attempts),(SELECT count(*) FROM artifact_delivery_receipts),(SELECT count(*) FROM artifact_consumer_checkpoints)`).Scan(&attempts, &receipts, &checkpoints); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || receipts != 1 || checkpoints != 1 {
		t.Fatalf("canonical attempt/receipt/checkpoint=%d/%d/%d", attempts, receipts, checkpoints)
	}
}

func TestArtifactCatalogAttemptMigrationRejectsNoncanonicalIdentity(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	canonicalManifest := strings.Repeat("b", 64)
	for _, test := range []struct {
		name      string
		commitID  *string
		manifest  *string
		logicalID *string
	}{
		{name: "null_commit", commitID: nil},
		{name: "empty_commit", commitID: stringPointer("")},
		{name: "legacy_commit", commitID: stringPointer("legacy:attempt")},
		{name: "malformed_commit", commitID: stringPointer("wallaby-iceberg-not-a-digest")},
		{name: "wrong_canonical_commit", commitID: stringPointer("wallaby-iceberg-" + strings.Repeat("a", 64))},
		{name: "empty_manifest", manifest: stringPointer("")},
		{name: "case_variant_manifest", manifest: stringPointer(strings.ToUpper(canonicalManifest))},
		{name: "null_logical", logicalID: nil},
		{name: "empty_logical", logicalID: stringPointer("")},
		{name: "legacy_logical", logicalID: stringPointer("legacy:position")},
		{name: "malformed_logical", logicalID: stringPointer("logical-batch:not-current")},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "artifact_attempt_"+test.name)
			defer cleanup()
			prepareArtifactAttemptMigrationFixture(t, ctx, pool)
			fixture := artifactAttemptFixture{commitID: test.commitID, manifest: test.manifest, logicalID: test.logicalID, nullCommit: test.name == "null_commit", nullLogical: test.name == "null_logical"}
			insertArtifactAttemptFixture(t, ctx, pool, fixture)
			err := artifactlog.ApplyMigrations(ctx, pool)
			if err == nil || !strings.Contains(err.Error(), "refuses noncanonical attempt identities") {
				t.Fatalf("noncanonical attempt migration error=%v", err)
			}
		})
	}
}

func TestArtifactCatalogAttemptMigrationRejectsAmbiguousAttempts(t *testing.T) {
	dsn := managedProfileTestDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, cleanup := newDeliveryMigrationDatabase(t, ctx, dsn, "artifact_attempt_ambiguous")
	defer cleanup()
	prepareArtifactAttemptMigrationFixture(t, ctx, pool)
	identity := insertArtifactAttemptFixture(t, ctx, pool, artifactAttemptFixture{})
	insertArtifactAttemptRow(t, ctx, pool, identity, uuid.New())
	err := artifactlog.ApplyMigrations(ctx, pool)
	if err == nil || !strings.Contains(err.Error(), "refuses ambiguous attempt identities") {
		t.Fatalf("ambiguous attempt migration error=%v", err)
	}
}

type artifactAttemptIdentity struct {
	incarnationID, publicationID, attemptID                                                 uuid.UUID
	consumerRevisionID, manifestSHA256, logicalBatchID, commitID, positionID, checkpointLSN string
}

type artifactAttemptFixture struct {
	commitID, manifest, logicalID *string
	nullCommit, nullLogical       bool
}

func prepareArtifactAttemptMigrationFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if err := controlplane.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM wallaby_control_migrations WHERE domain='artifactlog' AND version='007_current_catalog_attempt_identity.sql';
ALTER TABLE artifact_delivery_attempts DROP CONSTRAINT artifact_delivery_attempts_current_identity;
ALTER TABLE artifact_delivery_attempts DROP CONSTRAINT artifact_delivery_attempts_publication_unique;
ALTER TABLE artifact_delivery_attempts DROP CONSTRAINT artifact_delivery_attempts_commit_unique;
ALTER TABLE artifact_delivery_attempts ALTER COLUMN commit_id DROP NOT NULL;
ALTER TABLE artifact_delivery_attempts ALTER COLUMN manifest_sha256 DROP NOT NULL;
ALTER TABLE artifact_delivery_attempts ALTER COLUMN logical_batch_id DROP NOT NULL;
ALTER TABLE artifact_delivery_receipts DROP CONSTRAINT artifact_delivery_receipts_current_identity;
ALTER TABLE artifact_delivery_receipts DROP CONSTRAINT artifact_delivery_receipts_attempt_unique;
ALTER TABLE artifact_delivery_receipts ALTER COLUMN commit_id DROP NOT NULL;
ALTER TABLE artifact_delivery_receipts ALTER COLUMN logical_batch_id DROP NOT NULL`); err != nil {
		t.Fatal(err)
	}
}

func insertArtifactAttemptFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixture artifactAttemptFixture) artifactAttemptIdentity {
	t.Helper()
	publicationLogical, err := connector.DeliveryLogicalBatchID("lineage", "artifact-position", "publication-content")
	if err != nil {
		t.Fatal(err)
	}
	identity := artifactAttemptIdentity{incarnationID: uuid.New(), publicationID: uuid.New(), attemptID: uuid.New(), consumerRevisionID: "iceberg-current-v1", manifestSHA256: strings.Repeat("b", 64), logicalBatchID: publicationLogical, positionID: "artifact-position", checkpointLSN: "0/100"}
	if fixture.manifest != nil {
		identity.manifestSHA256 = *fixture.manifest
	}
	if fixture.logicalID != nil {
		identity.logicalBatchID = *fixture.logicalID
	}
	identity.commitID = artifactlog.DeterministicCommitID(identity.incarnationID, identity.consumerRevisionID, identity.publicationID, identity.manifestSHA256)
	var commit any = identity.commitID
	var logical any = identity.logicalBatchID
	if fixture.nullCommit {
		commit = nil
	} else if fixture.commitID != nil {
		commit = *fixture.commitID
	}
	if fixture.nullLogical {
		logical = nil
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('wallaby.authority_protocol','v2',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO flow_incarnations(incarnation_id,flow_id) VALUES($1,'artifact-attempt-flow')`, identity.incarnationID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO artifact_publications(publication_id,flow_incarnation_id,source_lineage_id,source_transaction_id,source_xid,begin_lsn,commit_lsn,source_position,checkpoint_lsn,position_id,content_hash,generation,acquisition_id,lease_epoch,rooted_bytes,logical_batch_id,sequence,projection_id,mapping_fingerprint) VALUES($1,$2,'lineage','transaction',1,'0/80','0/90','0/100',$3,$4,'publication-content',1,$5,1,1,$6,1,'canonical_cdc_parquet_v1','')`, identity.publicationID, identity.incarnationID, identity.checkpointLSN, identity.positionID, uuid.New(), publicationLogical); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO artifact_delivery_attempts(attempt_id,flow_incarnation_id,consumer_revision_id,publication_id,generation,acquisition_id,lease_epoch,commit_id,manifest_sha256,logical_batch_id) VALUES($1,$2,$3,$4,1,$5,1,$6,$7,$8)`, identity.attemptID, identity.incarnationID, identity.consumerRevisionID, identity.publicationID, uuid.New(), commit, identity.manifestSHA256, logical); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return identity
}

func insertArtifactAttemptRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, identity artifactAttemptIdentity, attemptID uuid.UUID) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('wallaby.authority_protocol','v2',true)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO artifact_delivery_attempts(attempt_id,flow_incarnation_id,consumer_revision_id,publication_id,generation,acquisition_id,lease_epoch,commit_id,manifest_sha256,logical_batch_id) VALUES($1,$2,$3,$4,1,$5,1,$6,$7,$8)`, attemptID, identity.incarnationID, identity.consumerRevisionID, identity.publicationID, uuid.New(), identity.commitID, identity.manifestSHA256, identity.logicalBatchID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func insertArtifactReceiptAndCheckpoint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, identity artifactAttemptIdentity) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('wallaby.authority_protocol','v2',true)`); err != nil {
		t.Fatal(err)
	}
	acquisitionID := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO artifact_delivery_receipts(flow_incarnation_id,consumer_revision_id,publication_id,attempt_id,snapshot_id,content_hash,acquisition_id,lease_epoch,commit_id,logical_batch_id,publication_sequence,position_id,checkpoint_lsn,snapshot_ids) VALUES($1,$2,$3,$4,'snapshot-current',$5,$6,1,$7,$8,1,$9,$10,'{"table":"snapshot-current"}'::jsonb)`, identity.incarnationID, identity.consumerRevisionID, identity.publicationID, identity.attemptID, identity.manifestSHA256, acquisitionID, identity.commitID, identity.logicalBatchID, identity.positionID, identity.checkpointLSN); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO artifact_consumer_checkpoints(flow_incarnation_id,consumer_revision_id,publication_sequence,publication_id,position_id,checkpoint_lsn,commit_id,snapshot_id) VALUES($1,$2,1,$3,$4,$5,$6,'snapshot-current')`, identity.incarnationID, identity.consumerRevisionID, identity.publicationID, identity.positionID, identity.checkpointLSN, identity.commitID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func stringPointer(value string) *string { return &value }
