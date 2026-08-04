package artifactlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CommitRequest is the immutable PostgreSQL-rooted input to one append-only
// changelog commit. The committer may rewrite canonical objects, but it may not
// derive delivery identity or progress from the catalog.
type CommitRequest struct {
	FlowID              string
	FlowIncarnationID   uuid.UUID
	ConsumerRevisionID  string
	PublicationID       uuid.UUID
	PublicationSequence int64
	PositionID          string
	CheckpointLSN       string
	LogicalBatchID      string
	ProjectionID        string
	MappingFingerprint  string
	ManifestSHA256      string
	CommitID            string
	AttemptedAt         time.Time
	Objects             []RootedArtifact
	Barriers            []Barrier
}

// ReconcileRequest carries the same immutable identity and target plan as the
// original commit. AttemptedAt lets a catalog prove absence only when retained
// snapshot history spans the attempt; otherwise reconciliation is indeterminate.
type ReconcileRequest = CommitRequest

// CommitResult is catalog evidence. SnapshotID remains the compact receipt
// value for rolling compatibility; SnapshotIDs records every table snapshot in
// a multi-table source transaction.
type CommitResult struct {
	SnapshotID     string
	SnapshotIDs    map[string]string
	ManifestSHA256 string
	CommitID       string
	LogicalBatchID string
}

// CommitDisposition is the only safe conclusion after an interrupted catalog
// call. An absent summary is not enough for CommitNotApplied unless retained
// history proves that the attempt could not have expired.
type CommitDisposition uint8

const (
	CommitIndeterminate CommitDisposition = iota
	CommitNotApplied
	CommitApplied
)

// ReconcileResult couples the disposition with exact evidence when applied.
type ReconcileResult struct {
	Disposition CommitDisposition
	Commit      CommitResult
}

// ChangelogCommitter is the sole external-effect seam through which ordinary
// Iceberg and S3 Tables consume the canonical artifact log.
type ChangelogCommitter interface {
	Commit(context.Context, CommitRequest) (CommitResult, error)
	Reconcile(context.Context, ReconcileRequest) (ReconcileResult, error)
}

// DeterministicCommitID is stable across worker generations and retry attempts.
func DeterministicCommitID(flowIncarnationID uuid.UUID, consumerRevisionID string, publicationID uuid.UUID, manifestSHA256 string) string {
	hash := sha256.New()
	for _, value := range []string{
		"wallaby.iceberg.commit.v1",
		flowIncarnationID.String(),
		strings.TrimSpace(consumerRevisionID),
		publicationID.String(),
		strings.TrimSpace(manifestSHA256),
	} {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return "wallaby-iceberg-" + hex.EncodeToString(hash.Sum(nil))
}

// Rolling aliases keep package callers compiling while the old append-only
// catalog scaffold transitions to the request-oriented committer seam.
type CatalogCommit = CommitResult
type CatalogDisposition = CommitDisposition

const (
	CatalogIndeterminate = CommitIndeterminate
	CatalogNotApplied    = CommitNotApplied
	CatalogApplied       = CommitApplied
)
