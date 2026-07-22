package artifactlog

import (
	"context"
	"errors"
	"testing"
)

func TestPublisherRetriesConditionalPutAfterConflictWithoutEvidence(t *testing.T) {
	t.Parallel()

	objects := &retryObjectStore{}
	publisher := &Publisher{objects: objects}
	artifact := Artifact{ObjectKey: "artifact.parquet", Encoded: []byte("body"), EncodedByteHash: "digest"}
	evidence, err := publisher.putAndReconcile(context.Background(), artifact)
	if err != nil {
		t.Fatal(err)
	}
	if objects.puts != 2 || objects.reconciliations != 1 || evidence.VersionID != "version-2" {
		t.Fatalf("puts/reconciliations/version=%d/%d/%q, want 2/1/version-2", objects.puts, objects.reconciliations, evidence.VersionID)
	}
}

type retryObjectStore struct {
	puts            int
	reconciliations int
}

func (*retryObjectStore) Bucket() string { return "bucket" }
func (s *retryObjectStore) PutImmutable(context.Context, string, []byte, string) (ObjectEvidence, error) {
	s.puts++
	if s.puts == 1 {
		return ObjectEvidence{}, errors.New("conditional request conflict")
	}
	return ObjectEvidence{Bucket: "bucket", Key: "artifact.parquet", VersionID: "version-2", ChecksumSHA256: "digest", Length: 4}, nil
}
func (s *retryObjectStore) ReconcileVersion(context.Context, string, string, int64) (ObjectEvidence, error) {
	s.reconciliations++
	return ObjectEvidence{}, ErrObjectNotFound
}
func (*retryObjectStore) HeadVersion(_ context.Context, evidence ObjectEvidence) (ObjectEvidence, error) {
	return evidence, nil
}
func (*retryObjectStore) DeleteVersion(context.Context, ObjectEvidence) error { return nil }
