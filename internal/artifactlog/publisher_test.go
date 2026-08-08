package artifactlog

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestPublicationIdentityV2IsDeterministicAndProjectionBound(t *testing.T) {
	t.Parallel()
	transaction := plannerTransaction(1)
	incarnation := uuid.MustParse("66666666-6666-6666-6666-666666666666")
	mapping := strings.Repeat("a", 64)
	plan, err := NewEncoder().PlanMappedTransaction(context.Background(), incarnation, mapping, transaction)
	if err != nil {
		t.Fatal(err)
	}
	identity := func(mapping string, plan Plan) uuid.UUID {
		id, err := publicationIdentityV2(incarnation, mapping, transaction, plan)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	first := identity(mapping, plan)
	second := identity(mapping, plan)
	if first != second {
		t.Fatalf("retry publication IDs differ: %s %s", first, second)
	}
	if first == identity(strings.Repeat("b", 64), plan) {
		t.Fatal("mapping fingerprint did not bind v2 publication ID")
	}
	changed := plan
	changed.ContentHash = strings.Repeat("c", 64)
	if first == identity(mapping, changed) {
		t.Fatal("content hash did not bind v2 publication ID")
	}
}

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
func (s *retryObjectStore) PutImmutable(context.Context, string, []byte, string, string, string) (ObjectEvidence, error) {
	s.puts++
	if s.puts == 1 {
		return ObjectEvidence{}, errors.New("conditional request conflict")
	}
	return ObjectEvidence{Bucket: "bucket", Key: "artifact.parquet", VersionID: "version-2", ChecksumSHA256: "digest", Length: 4}, nil
}
func (s *retryObjectStore) ReconcileVersion(context.Context, string, string, int64, string, string) (ObjectEvidence, error) {
	s.reconciliations++
	return ObjectEvidence{}, ErrObjectNotFound
}
func (*retryObjectStore) HeadVersion(_ context.Context, evidence ObjectEvidence) (ObjectEvidence, error) {
	return evidence, nil
}
func (*retryObjectStore) DeleteVersion(context.Context, ObjectEvidence) error { return nil }
