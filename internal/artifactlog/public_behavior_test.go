package artifactlog_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRuntimeRejectsIncompleteConsumerAuthorityBeforeExternalIO(t *testing.T) {
	t.Parallel()
	objects := &recordingObjectStore{}
	config := artifactlog.RuntimeConfig{
		Stream:      artifactlog.StreamConfig{HardRetainedBytes: 1, BacklogCountHigh: 1, BacklogBytesHigh: 1},
		OrphanGrace: time.Second,
		Retention:   time.Second,
		GCInterval:  time.Second,
		Consumers:   []artifactlog.CatalogConsumerConfig{{RevisionID: "catalog-v1"}},
	}
	if _, err := artifactlog.NewRuntime(context.Background(), nil, objects, config); err == nil || !strings.Contains(err.Error(), "require revision ID and committer") {
		t.Fatalf("NewRuntime() error=%v, want incomplete consumer authority rejection", err)
	}
	if objects.calls != 0 {
		t.Fatalf("external object-store calls=%d, want zero", objects.calls)
	}
}

func TestDeterministicCommitIDBindsEveryAuthorityDimension(t *testing.T) {
	t.Parallel()
	incarnation := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	publication := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	base := artifactlog.DeterministicCommitID(incarnation, "catalog-v1", publication, "manifest-a")
	if base == "" || base != artifactlog.DeterministicCommitID(incarnation, " catalog-v1 ", publication, "manifest-a") {
		t.Fatalf("commit ID is empty or not replay-stable: %q", base)
	}
	changed := []string{
		artifactlog.DeterministicCommitID(uuid.MustParse("33333333-3333-3333-3333-333333333333"), "catalog-v1", publication, "manifest-a"),
		artifactlog.DeterministicCommitID(incarnation, "catalog-v2", publication, "manifest-a"),
		artifactlog.DeterministicCommitID(incarnation, "catalog-v1", uuid.MustParse("44444444-4444-4444-4444-444444444444"), "manifest-a"),
		artifactlog.DeterministicCommitID(incarnation, "catalog-v1", publication, "manifest-b"),
	}
	for index, candidate := range changed {
		if candidate == base {
			t.Fatalf("authority dimension %d did not change commit identity", index)
		}
	}
}

func TestEncoderRejectsInvalidSourceTransactionThroughPublicAPI(t *testing.T) {
	t.Parallel()
	transaction := connector.SourceTransaction{
		SourceLineageID: "source/publication-v1",
		TransactionID:   1,
		BeginLSN:        "0/10",
		CommitLSN:       "0/18",
		EndLSN:          "0/20",
		Checkpoint:      connector.Checkpoint{LSN: "0/21"},
		Fragments: []connector.TransactionFragment{{
			Ordinal: 0,
			Batch: connector.Batch{
				Schema:  connector.Schema{Namespace: "public", Name: "events", Version: 1},
				Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1}},
			},
		}},
	}
	if _, err := artifactlog.NewEncoder().PlanTransaction(context.Background(), uuid.New(), transaction); err == nil || !strings.Contains(err.Error(), "must equal transaction end") {
		t.Fatalf("PlanTransaction() error=%v, want source-cut mismatch rejection", err)
	}
}

type recordingObjectStore struct{ calls int }

func (*recordingObjectStore) Bucket() string { return "bucket" }
func (s *recordingObjectStore) PutImmutable(context.Context, string, []byte, string) (artifactlog.ObjectEvidence, error) {
	s.calls++
	return artifactlog.ObjectEvidence{}, nil
}
func (s *recordingObjectStore) ReconcileVersion(context.Context, string, string, int64) (artifactlog.ObjectEvidence, error) {
	s.calls++
	return artifactlog.ObjectEvidence{}, nil
}
func (s *recordingObjectStore) HeadVersion(context.Context, artifactlog.ObjectEvidence) (artifactlog.ObjectEvidence, error) {
	s.calls++
	return artifactlog.ObjectEvidence{}, nil
}
func (s *recordingObjectStore) DeleteVersion(context.Context, artifactlog.ObjectEvidence) error {
	s.calls++
	return nil
}
