package iceberg

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberggo "github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"
	icerest "github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCanonicalSchemaChecksumSurvivesPostgresJSONBNormalization(t *testing.T) {
	t.Parallel()
	document := canonicalSchemaDocument{ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: strings.Repeat("a", 64), SourceLineageID: "lineage", Namespace: "wallaby", Table: "events", Fields: []artifactlog.CanonicalField{}}
	canonical, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	schemaID := hex.EncodeToString(digest[:])
	var normalizedValue any
	if err := json.Unmarshal(canonical, &normalizedValue); err != nil {
		t.Fatal(err)
	}
	normalized, err := json.Marshal(normalizedValue)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(canonical, normalized) {
		t.Fatal("fixture did not normalize JSON object key order")
	}
	_, err = projectObject(context.Background(), artifactlog.CommitRequest{ProjectionID: document.ProjectionID, MappingFingerprint: document.MappingFingerprint}, artifactlog.RootedArtifact{ArtifactID: "artifact", Namespace: document.Namespace, Table: document.Table, SchemaID: schemaID, SchemaJSON: normalized, Evidence: artifactlog.ObjectEvidence{Key: "missing", VersionID: "v1"}}, &memoryCanonicalObjects{data: map[string][]byte{}})
	if !errors.Is(err, artifactlog.ErrObjectNotFound) {
		t.Fatalf("normalized schema error=%v", err)
	}
}

func TestRESTCommitClassificationPreservesPermanentTransportFailures(t *testing.T) {
	t.Parallel()

	for _, permanent := range []error{
		&url.Error{Op: "POST", URL: "https://catalog.invalid", Err: x509.UnknownAuthorityError{}},
		&url.Error{Op: "POST", URL: "https://catalog.invalid", Err: tls.RecordHeaderError{Msg: "server spoke plaintext"}},
		&url.Error{Op: "POST", URL: "https://catalog.invalid", Err: tls.AlertError(40)},
		errors.Join(icerest.ErrCommitFailed, &url.Error{Op: "POST", URL: "https://catalog.invalid", Err: tls.AlertError(40)}),
		&url.Error{Op: "POST", URL: "catalog.invalid", Err: errors.New("unsupported protocol scheme")},
		&net.DNSError{Err: "no such host", IsNotFound: true},
		icerest.ErrUnauthorized,
		icecatalog.ErrNoSuchTable,
		icecatalog.ErrNoSuchNamespace,
	} {
		classified := classifyRESTCatalogCommitError(permanent)
		if !errors.Is(classified, permanent) || errors.Is(classified, ErrCatalogIndeterminate) {
			t.Fatalf("REST permanent classification=%v for %v", classified, permanent)
		}
		consumerErr := classifyConsumerRetryableError(context.Background(), classified)
		if errors.Is(consumerErr, artifactlog.ErrConsumerRetryable) {
			t.Fatalf("permanent REST error became consumer-retryable: %v", consumerErr)
		}
	}

	for _, temporary := range []error{
		icerest.ErrServerError,
		&url.Error{Op: "POST", URL: "https://catalog.example", Err: &net.OpError{Op: "write", Net: "tcp", Err: syscall.ECONNRESET}},
	} {
		classified := classifyRESTCatalogCommitError(temporary)
		if !errors.Is(classified, ErrCatalogIndeterminate) {
			t.Fatalf("REST temporary classification=%v for %v, want ErrCatalogIndeterminate", classified, temporary)
		}
		if consumerErr := classifyConsumerRetryableError(context.Background(), classified); !errors.Is(consumerErr, artifactlog.ErrConsumerRetryable) {
			t.Fatalf("temporary REST consumer classification=%v", consumerErr)
		}
	}
}

func TestClassifyConsumerRetryableError(t *testing.T) {
	t.Parallel()

	for _, retryable := range []error{
		ErrCatalogConflict,
		ErrCatalogIndeterminate,
		icerest.ErrAuthorizationExpired,
		icerest.ErrServiceUnavailable,
		icerest.ErrServerError,
		context.DeadlineExceeded,
		&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
		&net.DNSError{Err: "temporary resolver failure", IsTemporary: true},
	} {
		err := classifyConsumerRetryableError(context.Background(), retryable)
		if !errors.Is(err, artifactlog.ErrConsumerRetryable) || !errors.Is(err, retryable) {
			t.Fatalf("classified error=%v, want retryable preserving %v", err, retryable)
		}
	}

	for _, terminal := range []error{
		errors.New("schema identity conflict"),
		icerest.ErrUnauthorized,
		&url.Error{Op: "Get", URL: "https://catalog.invalid", Err: x509.UnknownAuthorityError{}},
		errors.Join(ErrCatalogIndeterminate, &url.Error{Op: "POST", URL: "https://catalog.invalid", Err: tls.RecordHeaderError{Msg: "server spoke plaintext"}}),
		&net.DNSError{Err: "no such host", IsNotFound: true},
		errors.Join(connector.ErrDeliveryConflict, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}),
	} {
		if err := classifyConsumerRetryableError(context.Background(), terminal); !errors.Is(err, terminal) || errors.Is(err, artifactlog.ErrConsumerRetryable) {
			t.Fatalf("terminal error classification=%v for %v", err, terminal)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := classifyConsumerRetryableError(canceled, ErrCatalogIndeterminate); !errors.Is(err, ErrCatalogIndeterminate) || errors.Is(err, artifactlog.ErrConsumerRetryable) {
		t.Fatalf("canceled context classification=%v", err)
	}
}

// parquetNativeFieldIDs writes one record batch to Parquet and reads back the
// native Parquet field IDs, proving a committed data file carries the
// catalog-assigned PARQUET:field_id values rather than the canonical
// hash-derived IDs.
func parquetNativeFieldIDs(record arrow.RecordBatch) (map[string]int, error) {
	tbl := array.NewTableFromRecords(record.Schema(), []arrow.RecordBatch{record})
	defer tbl.Release()
	buf := bytes.NewBuffer(nil)
	if err := pqarrow.WriteTable(tbl, buf, tbl.NumRows(), parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties()); err != nil {
		return nil, err
	}
	reader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		return nil, err
	}
	schema, err := arrowReader.Schema()
	if err != nil {
		return nil, err
	}
	ids := make(map[string]int, schema.NumFields())
	for index := 0; index < schema.NumFields(); index++ {
		field := schema.Field(index)
		if value, ok := field.Metadata.GetValue("PARQUET:field_id"); ok {
			id, convErr := strconv.Atoi(value)
			if convErr != nil {
				return nil, convErr
			}
			ids[field.Name] = id
		}
	}
	return ids, nil
}

func TestCommitterRewritesCanonicalProjectionWithCatalogOwnedFieldIDs(t *testing.T) {
	t.Parallel()
	request, objects, canonicalFields := testCommitRequest(t, false)
	backend := newFakeCatalogBackend()
	committer := testCommitter(t, objects, backend)

	result, err := committer.Commit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.CommitID != request.CommitID || result.ManifestSHA256 != request.ManifestSHA256 || len(result.SnapshotIDs) != 1 {
		t.Fatalf("commit result=%+v", result)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.appendOrder) != 1 {
		t.Fatalf("append order=%v", backend.appendOrder)
	}
	state := backend.tables[backend.appendOrder[0]]

	// The catalog owns table field IDs. Every canonical field resolves by
	// name, and at least one user field must carry a catalog ID that differs
	// from its hash-derived canonical ID, proving the committer does not rely
	// on caller-supplied IDs.
	sawReassignedID := false
	for _, expected := range canonicalFields {
		field, ok := state.Schema.FindFieldByName(expected.Name)
		if !ok {
			t.Fatalf("canonical field %q missing from target schema %s", expected.Name, state.Schema)
		}
		if field.ID != int(expected.ID) {
			sawReassignedID = true
		}
		// The data file committed to Iceberg must carry the catalog-assigned
		// PARQUET:field_id, not the canonical hash-derived ID.
		if got := backend.lastAppendFieldIDs[expected.Name]; got != field.ID {
			t.Fatalf("data file field %q id=%d, want catalog id %d", expected.Name, got, field.ID)
		}
	}
	if !sawReassignedID {
		t.Fatal("no canonical field received a fresh catalog field ID; the fixture is masking the defect")
	}
	if got := backend.operations; strings.Join(got, ",") != "insert,update,delete" {
		t.Fatalf("appended operations=%v, want append-only insert/update/delete events", got)
	}
	if backend.lastSummary[SummaryLogicalBatchID] != request.LogicalBatchID || backend.lastSummary[SummaryManifestSHA256] != request.ManifestSHA256 {
		t.Fatalf("snapshot summary=%v", backend.lastSummary)
	}
	if backend.lastSummary[SummaryFieldMapping] == "" {
		t.Fatal("snapshot summary is missing the field-id mapping fingerprint")
	}
}

func TestBuildProjectionKeepsAlreadyMappedRelationsDistinct(t *testing.T) {
	t.Parallel()

	transaction := connector.SourceTransaction{
		SourceLineageID: "postgres-system/test-iceberg-v1", TransactionID: 72,
		BeginLSN: "0/20", CommitLSN: "0/28", EndLSN: "0/30",
		Checkpoint: connector.Checkpoint{LSN: "0/30", Timestamp: time.Unix(101, 0).UTC()},
		Fragments: []connector.TransactionFragment{
			{Ordinal: 0, Batch: connector.Batch{Schema: connector.Schema{Namespace: "public", Name: "events", Version: 1, Columns: []connector.Column{column("id", "int8", 1)}}, Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1, SourcePosition: "0/30", After: map[string]any{"id": int64(1)}}}}},
			{Ordinal: 1, Batch: connector.Batch{Schema: connector.Schema{Namespace: "public", Name: "audit", Version: 1, Columns: []connector.Column{column("id", "int8", 1), column("note", "text", 2)}}, Records: []connector.Record{{Table: "audit", Operation: connector.OpInsert, SchemaVersion: 1, SourcePosition: "0/30", After: map[string]any{"id": int64(2), "note": "two"}}}}},
		},
	}
	request, objects, _ := assembleCommitRequest(t, uuid.MustParse("44444444-4444-4444-4444-444444444444"), uuid.MustParse("66666666-6666-6666-6666-666666666666"), 2, transaction)
	cfg := testIcebergConfig()
	plan, err := buildProjection(context.Background(), request, objects, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer plan.release()
	if len(plan.groups) != 2 {
		t.Fatalf("mapped relation groups=%d, want 2", len(plan.groups))
	}
}

func TestBuildProjectionRejectsControlTableCollision(t *testing.T) {
	t.Parallel()

	for _, withBarriers := range []bool{false, true} {
		request, objects, _ := testCommitRequest(t, withBarriers)
		cfg := testIcebergConfig()
		cfg.ControlTable = "events"
		for index := range request.Objects {
			request.Objects[index].Namespace = "wallaby"
			request.Objects[index].Table = "events"
			var document map[string]any
			if err := json.Unmarshal(request.Objects[index].SchemaJSON, &document); err != nil {
				t.Fatal(err)
			}
			document["namespace"] = "wallaby"
			document["table"] = "events"
			request.Objects[index].SchemaJSON, _ = json.Marshal(document)
		}
		for index := range request.Barriers {
			request.Barriers[index].Namespace = "wallaby"
			request.Barriers[index].Table = "events"
		}
		plan, err := buildProjection(context.Background(), request, objects, cfg)
		if plan != nil {
			plan.release()
		}
		if err == nil || !errors.Is(err, connector.ErrDeliveryConflict) || !strings.Contains(err.Error(), "multiple schema projections target") {
			t.Fatalf("withBarriers=%t error=%v, want control-table projection conflict", withBarriers, err)
		}
		if _, err := expectedProjectionGroups(request, cfg); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("withBarriers=%t expected projection groups error=%v, want control-table conflict", withBarriers, err)
		}
	}
}

func TestCommitAndReconcileRejectNonV2OrMalformedMappingBeforeCatalog(t *testing.T) {
	t.Parallel()
	for _, mutation := range []func(*artifactlog.CommitRequest){func(request *artifactlog.CommitRequest) { request.ProjectionID = artifactlog.ProjectionID }, func(request *artifactlog.CommitRequest) {
		request.MappingFingerprint = "ABCDEF" + strings.Repeat("0", 58)
	}, func(request *artifactlog.CommitRequest) { request.MappingFingerprint = strings.Repeat("z", 64) }} {
		for _, operation := range []string{"commit", "reconcile"} {
			request, objects, _ := testCommitRequest(t, false)
			mutation(&request)
			backend := newFakeCatalogBackend()
			committer := testCommitter(t, objects, backend)
			var err error
			if operation == "commit" {
				_, err = committer.Commit(context.Background(), request)
			} else {
				_, err = committer.Reconcile(context.Background(), request)
			}
			if err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
				t.Fatalf("%s invalid projection identity error=%v", operation, err)
			}
			if backend.catalogCalls != 0 {
				t.Fatalf("%s reached catalog %d times", operation, backend.catalogCalls)
			}
		}
	}
}

func TestBuildProjectionRejectsMappingFingerprintMismatch(t *testing.T) {
	t.Parallel()
	request, objects, _ := testCommitRequest(t, false)
	request.MappingFingerprint = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	plan, err := buildProjection(context.Background(), request, objects, testIcebergConfig())
	if plan != nil {
		plan.release()
	}
	if err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("mapping fingerprint mismatch error=%v", err)
	}
}

func TestCommitterProcessesBarrierProjectionBeforeData(t *testing.T) {
	t.Parallel()
	request, objects, _ := testCommitRequest(t, true)
	backend := newFakeCatalogBackend()
	committer := testCommitter(t, objects, backend)
	if _, err := committer.Commit(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.appendOrder) != 2 || !strings.HasSuffix(backend.appendOrder[0], ".__wallaby_control") {
		t.Fatalf("append order=%v, want control barrier before data", backend.appendOrder)
	}
}

func TestCommitterReconcilesCommitBeforeReceipt(t *testing.T) {
	t.Parallel()
	request, objects, _ := testCommitRequest(t, false)
	backend := newFakeCatalogBackend()
	committer, err := NewCommitter(objects, backend, testIcebergConfig())
	if err != nil {
		t.Fatal(err)
	}
	committer.hooks = committerHooks{Reach: func(_ context.Context, boundary string) error {
		if strings.HasPrefix(boundary, "after_catalog_commit:") {
			return errors.New("injected lost response after catalog commit")
		}
		return nil
	}}
	if _, err := committer.Commit(context.Background(), request); err == nil {
		t.Fatal("commit unexpectedly survived injected post-commit failure")
	}
	reconciled, err := committer.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if reconciled.Disposition != artifactlog.CommitApplied || reconciled.Commit.CommitID != request.CommitID {
		t.Fatalf("reconciliation=%+v", reconciled)
	}
}

func TestCommitterRetriesOptimisticConflictAndConcurrentWriterConverges(t *testing.T) {
	t.Parallel()
	request, objects, _ := testCommitRequest(t, false)
	backend := newFakeCatalogBackend()
	backend.conflicts = 1
	committer := testCommitter(t, objects, backend)
	if _, err := committer.Commit(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if backend.appendCalls != 2 {
		t.Fatalf("append calls=%d, want conflict plus retry", backend.appendCalls)
	}

	first := testCommitter(t, objects, backend)
	second := testCommitter(t, objects, backend)
	var wait sync.WaitGroup
	errorsFound := make(chan error, 2)
	for _, current := range []*Committer{first, second} {
		wait.Add(1)
		go func(committer *Committer) {
			defer wait.Done()
			_, err := committer.Commit(context.Background(), request)
			errorsFound <- err
		}(current)
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	if backend.committedSnapshots() != 1 {
		t.Fatalf("snapshots=%d, identical concurrent writers must converge", backend.committedSnapshots())
	}
}

func TestCommitterBoundsOptimisticConflictRewrites(t *testing.T) {
	t.Parallel()

	request, objects, _ := testCommitRequest(t, false)
	backend := newFakeCatalogBackend()
	backend.conflicts = 100
	cfg := testIcebergConfig()
	cfg.MaxCommitRetries = 3
	committer, err := NewCommitter(objects, backend, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = committer.Commit(context.Background(), request)
	if !errors.Is(err, ErrCatalogConflict) || !strings.Contains(err.Error(), "exceeded 3 retries") {
		t.Fatalf("error=%v, want bounded catalog conflict", err)
	}
	if backend.appendCalls != 3 {
		t.Fatalf("append calls=%d, want exactly configured retry bound", backend.appendCalls)
	}
	if backend.committedSnapshots() != 0 {
		t.Fatalf("snapshots=%d, exhausted conflicts must not report a commit", backend.committedSnapshots())
	}
}

func TestEvolutionPlanAddsRenamesAndRejectsDropsOrUnprovenFields(t *testing.T) {
	t.Parallel()
	const lineage = "postgres-system/evolution"
	identity := func(relation, column int) string { return fmt.Sprintf("src:%s:%d:%d", lineage, relation, column) }
	current := iceberggo.NewSchema(3, iceberggo.NestedField{ID: 7, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Doc: identityDoc(identity(42, 1))}, iceberggo.NestedField{ID: 8, Name: "value", Type: iceberggo.PrimitiveTypes.String, Doc: identityDoc(identity(42, 2))})
	t.Run("valid add", func(t *testing.T) {
		desired := iceberggo.NewSchema(0, iceberggo.NestedField{ID: 1, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Doc: identityDoc(identity(42, 1))}, iceberggo.NestedField{ID: 2, Name: "value", Type: iceberggo.PrimitiveTypes.String, Doc: identityDoc(identity(42, 2))}, iceberggo.NestedField{ID: 3, Name: "note", Type: iceberggo.PrimitiveTypes.String, Doc: identityDoc(identity(42, 3))})
		adds, renames, err := evolutionPlan(current, desired)
		if err != nil || len(renames) != 0 || len(adds) != 1 || adds[0].Name != "note" || adds[0].Required {
			t.Fatalf("adds=%+v renames=%+v err=%v", adds, renames, err)
		}
	})
	t.Run("valid rename", func(t *testing.T) {
		desired := iceberggo.NewSchema(0, iceberggo.NestedField{ID: 1, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Doc: identityDoc(identity(42, 1))}, iceberggo.NestedField{ID: 2, Name: "payload", Type: iceberggo.PrimitiveTypes.String, Doc: identityDoc(identity(42, 2))})
		adds, renames, err := evolutionPlan(current, desired)
		if err != nil || len(adds) != 0 || len(renames) != 1 || renames[0].from != "value" || renames[0].to != "payload" {
			t.Fatalf("adds=%+v renames=%+v err=%v", adds, renames, err)
		}
	})
	t.Run("mapped column drop", func(t *testing.T) {
		desired := iceberggo.NewSchema(0, iceberggo.NestedField{ID: 1, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Doc: identityDoc(identity(42, 1))})
		if _, _, err := evolutionPlan(current, desired); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) || !strings.Contains(err.Error(), "absent from the complete canonical schema") {
			t.Fatalf("drop error=%v", err)
		}
	})
	t.Run("manual extra column", func(t *testing.T) {
		extra := iceberggo.NewSchema(3, append(current.Fields(), iceberggo.NestedField{ID: 9, Name: "manual", Type: iceberggo.PrimitiveTypes.String, Doc: identityDoc(identity(99, 1))})...)
		if _, _, err := evolutionPlan(extra, current); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("manual extra error=%v", err)
		}
	})
	for _, test := range []struct{ name, doc string }{{"missing doc", ""}, {"malformed source doc", icebergIdentityDocPrefix + "src::bad"}, {"malformed synthetic doc", icebergIdentityDocPrefix + "synthetic::::"}} {
		t.Run(test.name, func(t *testing.T) {
			malformed := iceberggo.NewSchema(1, iceberggo.NestedField{ID: 1, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Doc: test.doc})
			if _, _, err := evolutionPlan(malformed, current); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
				t.Fatalf("identity doc error=%v", err)
			}
		})
	}
	t.Run("type change", func(t *testing.T) {
		desired := iceberggo.NewSchema(0, iceberggo.NestedField{ID: 1, Name: "id", Type: iceberggo.PrimitiveTypes.String, Doc: identityDoc(identity(42, 1))}, iceberggo.NestedField{ID: 2, Name: "value", Type: iceberggo.PrimitiveTypes.String, Doc: identityDoc(identity(42, 2))})
		if _, _, err := evolutionPlan(current, desired); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("type error=%v", err)
		}
	})
}

func TestIcebergFieldIdentityIncludesSourceLineage(t *testing.T) {
	t.Parallel()
	a := artifactlog.CanonicalField{Name: "id", SourceLineageID: "lineage-a", SourceRelationID: 42, SourceColumnID: 1}
	b := a
	b.SourceLineageID = "lineage-b"
	identityA, identityB := stableFieldIdentity(a), stableFieldIdentity(b)
	if identityA == identityB {
		t.Fatalf("different lineages aliased as %q", identityA)
	}
	current := iceberggo.NewSchema(0, iceberggo.NestedField{ID: 1, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Required: false, Doc: identityDoc(identityA)})
	desired := iceberggo.NewSchema(0, iceberggo.NestedField{ID: 99, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Required: false, Doc: identityDoc(identityB)})
	if _, err := buildFieldMapping(current, desired); err == nil || !strings.Contains(err.Error(), "stable identity") {
		t.Fatalf("cross-lineage evolution error=%v", err)
	}
}

func TestBuildFieldMappingRejectsMissingAndColliding(t *testing.T) {
	t.Parallel()
	const identity = "src:postgres-system/mapping:42:1"
	current := iceberggo.NewSchema(1, iceberggo.NestedField{ID: 5, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Doc: identityDoc(identity)})
	t.Run("maps to catalog ids", func(t *testing.T) {
		mapping, err := buildFieldMapping(current, iceberggo.NewSchema(0, iceberggo.NestedField{ID: 99, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Doc: identityDoc(identity)}))
		if err != nil || mapping["id"] != 5 {
			t.Fatalf("mapping=%v err=%v", mapping, err)
		}
	})
	t.Run("missing desired field", func(t *testing.T) {
		if _, err := buildFieldMapping(current, iceberggo.NewSchema(0, iceberggo.NestedField{ID: 1, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Doc: identityDoc(identity)}, iceberggo.NestedField{ID: 2, Name: "note", Type: iceberggo.PrimitiveTypes.String, Doc: identityDoc("src:postgres-system/mapping:42:2")})); err == nil || !strings.Contains(err.Error(), "missing from the catalog schema") {
			t.Fatalf("missing error=%v", err)
		}
	})
	t.Run("extra current field", func(t *testing.T) {
		extra := iceberggo.NewSchema(1, iceberggo.NestedField{ID: 5, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Doc: identityDoc(identity)}, iceberggo.NestedField{ID: 6, Name: "manual", Type: iceberggo.PrimitiveTypes.String, Doc: identityDoc("src:postgres-system/mapping:99:1")})
		if _, err := buildFieldMapping(extra, current); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("extra error=%v", err)
		}
	})
	t.Run("requiredness", func(t *testing.T) {
		required := iceberggo.NewSchema(1, iceberggo.NestedField{ID: 5, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Required: true, Doc: identityDoc(identity)})
		if _, err := buildFieldMapping(required, iceberggo.NewSchema(0, iceberggo.NestedField{ID: 1, Name: "id", Type: iceberggo.PrimitiveTypes.Int64, Doc: identityDoc(identity)})); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("requiredness error=%v", err)
		}
	})
}

func TestMappingFingerprintIsBoundedStableDigest(t *testing.T) {
	t.Parallel()

	first := mappingFingerprint(map[string]int{"nested.value": 17, "id": 3})
	second := mappingFingerprint(map[string]int{"id": 3, "nested.value": 17})
	if first != second {
		t.Fatalf("mapping digest changed with map order: %q != %q", first, second)
	}
	if len(first) != sha256.Size*2 {
		t.Fatalf("mapping digest length=%d, want %d", len(first), sha256.Size*2)
	}
	if _, err := hex.DecodeString(first); err != nil {
		t.Fatalf("mapping digest is not lowercase hexadecimal: %q: %v", first, err)
	}
	if changed := mappingFingerprint(map[string]int{"id": 3, "nested.value": 18}); changed == first {
		t.Fatal("field-ID change did not change mapping digest")
	}
	if ambiguous := mappingFingerprint(map[string]int{"id=3\x00nested.value": 17}); ambiguous == first {
		t.Fatal("length-delimited mapping digest collided with delimiter-like field name")
	}
}

func TestCommitterAppendsAdditiveSchemaEvolutionAcrossPublications(t *testing.T) {
	t.Parallel()
	backend := newFakeCatalogBackend()

	first := eventsTransaction(71, "0/20",
		[]connector.Column{column("id", "int8", 1), column("value", "text", 2)},
		map[string]any{"id": int64(1), "value": "one"})
	requestV1, objectsV1, _ := assembleCommitRequest(t, uuid.MustParse("44444444-4444-4444-4444-444444444444"), uuid.MustParse("55555555-5555-5555-5555-555555555551"), 1, first)
	if _, err := testCommitter(t, objectsV1, backend).Commit(context.Background(), requestV1); err != nil {
		t.Fatal(err)
	}

	second := eventsTransaction(72, "0/40",
		[]connector.Column{column("id", "int8", 1), column("value", "text", 2), column("note", "text", 3)},
		map[string]any{"id": int64(2), "value": "two", "note": "added"})
	requestV2, objectsV2, fieldsV2 := assembleCommitRequest(t, uuid.MustParse("44444444-4444-4444-4444-444444444444"), uuid.MustParse("55555555-5555-5555-5555-555555555552"), 2, second)
	if _, err := testCommitter(t, objectsV2, backend).Commit(context.Background(), requestV2); err != nil {
		t.Fatal(err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.evolveCalls != 1 {
		t.Fatalf("evolve calls=%d, want exactly one additive evolution", backend.evolveCalls)
	}
	target, _ := testIcebergConfig().target("public", "events")
	state := backend.tables[strings.Join(target, ".")]
	note, ok := state.Schema.FindFieldByName("note")
	if !ok || note.Required {
		t.Fatalf("additive column note=%+v ok=%t, want optional field", note, ok)
	}
	// Every canonical v2 field, including the additive one, must be written to
	// the data file with the catalog-assigned ID.
	for _, field := range fieldsV2 {
		catalog, ok := state.Schema.FindFieldByName(field.Name)
		if !ok {
			t.Fatalf("canonical field %q missing after evolution", field.Name)
		}
		if backend.lastParquetFieldIDs[field.Name] != catalog.ID {
			t.Fatalf("data file field %q id=%d, want catalog id %d", field.Name, backend.lastParquetFieldIDs[field.Name], catalog.ID)
		}
	}
}

func TestCommitterAppliesSupportedRenameByStableIdentity(t *testing.T) {
	t.Parallel()
	backend := newFakeCatalogBackend()

	first := eventsTransaction(71, "0/20",
		[]connector.Column{column("id", "int8", 1), column("value", "text", 2)},
		map[string]any{"id": int64(1), "value": "one"})
	requestV1, objectsV1, _ := assembleCommitRequest(t, uuid.MustParse("44444444-4444-4444-4444-444444444444"), uuid.MustParse("55555555-5555-5555-5555-555555555551"), 1, first)
	if _, err := testCommitter(t, objectsV1, backend).Commit(context.Background(), requestV1); err != nil {
		t.Fatal(err)
	}

	// The same source column (relation 42, column 2) is renamed value -> payload.
	renamed := eventsTransaction(72, "0/40",
		[]connector.Column{column("id", "int8", 1), column("payload", "text", 2)},
		map[string]any{"id": int64(2), "payload": "renamed"})
	requestV2, objectsV2, _ := assembleCommitRequest(t, uuid.MustParse("44444444-4444-4444-4444-444444444444"), uuid.MustParse("55555555-5555-5555-5555-555555555552"), 2, renamed)
	if _, err := testCommitter(t, objectsV2, backend).Commit(context.Background(), requestV2); err != nil {
		t.Fatal(err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()
	target, _ := testIcebergConfig().target("public", "events")
	state := backend.tables[strings.Join(target, ".")]
	if _, ok := state.Schema.FindFieldByName("value"); ok {
		t.Fatal("pre-rename column value still present")
	}
	payload, ok := state.Schema.FindFieldByName("payload")
	if !ok {
		t.Fatalf("renamed column payload missing from %s", state.Schema)
	}
	// The renamed field keeps its original catalog ID; only its name changed.
	if backend.lastParquetFieldIDs["payload"] != payload.ID {
		t.Fatalf("renamed data file field id=%d, want catalog id %d", backend.lastParquetFieldIDs["payload"], payload.ID)
	}
}

func TestCommitterRejectsMappedColumnDropWithoutCatalogAppend(t *testing.T) {
	t.Parallel()
	backend := newFakeCatalogBackend()
	first := eventsTransaction(81, "0/60", []connector.Column{column("id", "int8", 1), column("value", "text", 2)}, map[string]any{"id": int64(1), "value": "one"})
	requestV1, objectsV1, _ := assembleCommitRequest(t, uuid.MustParse("44444444-4444-4444-4444-444444444444"), uuid.MustParse("55555555-5555-5555-5555-555555555561"), 1, first)
	if _, err := testCommitter(t, objectsV1, backend).Commit(context.Background(), requestV1); err != nil {
		t.Fatal(err)
	}
	dropped := eventsTransaction(82, "0/80", []connector.Column{column("id", "int8", 1)}, map[string]any{"id": int64(2)})
	requestV2, objectsV2, _ := assembleCommitRequest(t, uuid.MustParse("44444444-4444-4444-4444-444444444444"), uuid.MustParse("55555555-5555-5555-5555-555555555562"), 2, dropped)
	before := backend.appendCalls
	if _, err := testCommitter(t, objectsV2, backend).Commit(context.Background(), requestV2); err == nil || !errors.Is(err, connector.ErrDeliveryConflict) || !strings.Contains(err.Error(), "absent from the complete canonical schema") {
		t.Fatalf("mapped drop error=%v", err)
	}
	if backend.appendCalls != before {
		t.Fatalf("mapped drop reached catalog append: before=%d after=%d", before, backend.appendCalls)
	}
}

func TestCommitterRetryDoesNotDuplicateDerivedFiles(t *testing.T) {
	t.Parallel()
	request, objects, _ := testCommitRequest(t, false)
	backend := newFakeCatalogBackend()
	committer := testCommitter(t, objects, backend)

	first, err := committer.Commit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	// A restart or lost receipt replays the identical publication. The committer
	// must adopt the existing snapshot rather than writing a second set of
	// derived Iceberg data files.
	second, err := committer.Commit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.SnapshotID != second.SnapshotID {
		t.Fatalf("retry snapshot=%s, want idempotent %s", second.SnapshotID, first.SnapshotID)
	}
	if backend.appendCalls != 1 {
		t.Fatalf("append calls=%d, want a single derived-file append across the retry", backend.appendCalls)
	}
	if backend.committedSnapshots() != 1 {
		t.Fatalf("snapshots=%d, retry must not duplicate derived files", backend.committedSnapshots())
	}
}

func TestCommitterConvergesUnderConcurrentEvolutionConflict(t *testing.T) {
	t.Parallel()
	backend := newFakeCatalogBackend()

	first := eventsTransaction(71, "0/20",
		[]connector.Column{column("id", "int8", 1), column("value", "text", 2)},
		map[string]any{"id": int64(1), "value": "one"})
	requestV1, objectsV1, _ := assembleCommitRequest(t, uuid.MustParse("44444444-4444-4444-4444-444444444444"), uuid.MustParse("55555555-5555-5555-5555-555555555551"), 1, first)
	if _, err := testCommitter(t, objectsV1, backend).Commit(context.Background(), requestV1); err != nil {
		t.Fatal(err)
	}

	// A competing writer wins the first evolution commit; our attempt observes a
	// catalog conflict and must retry against the already-evolved schema.
	backend.mu.Lock()
	backend.evolveConflicts = 1
	backend.mu.Unlock()

	second := eventsTransaction(72, "0/40",
		[]connector.Column{column("id", "int8", 1), column("value", "text", 2), column("note", "text", 3)},
		map[string]any{"id": int64(2), "value": "two", "note": "added"})
	requestV2, objectsV2, _ := assembleCommitRequest(t, uuid.MustParse("44444444-4444-4444-4444-444444444444"), uuid.MustParse("55555555-5555-5555-5555-555555555552"), 2, second)
	if _, err := testCommitter(t, objectsV2, backend).Commit(context.Background(), requestV2); err != nil {
		t.Fatal(err)
	}

	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.evolveCalls < 2 {
		t.Fatalf("evolve calls=%d, want a conflict plus a converging retry", backend.evolveCalls)
	}
	target, _ := testIcebergConfig().target("public", "events")
	state := backend.tables[strings.Join(target, ".")]
	if _, ok := state.Schema.FindFieldByName("note"); !ok {
		t.Fatal("note column missing after converging evolution retry")
	}
	if len(state.Snapshots) != 2 {
		t.Fatalf("snapshots=%d, want v1 and v2 appended exactly once", len(state.Snapshots))
	}
}

func TestCommitterFailsClosedOnCorruptionAndSchemaMismatch(t *testing.T) {
	t.Parallel()
	t.Run("corrupt canonical object", func(t *testing.T) {
		request, objects, _ := testCommitRequest(t, false)
		for key, value := range objects.data {
			value[len(value)-1] ^= 0xff
			objects.data[key] = value
		}
		backend := newFakeCatalogBackend()
		committer := testCommitter(t, objects, backend)
		if _, err := committer.Commit(context.Background(), request); !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("corruption error=%v, want delivery conflict", err)
		}
		if backend.appendCalls != 0 {
			t.Fatal("corrupt object reached catalog append")
		}
	})

	t.Run("stable identity type change", func(t *testing.T) {
		request, objects, canonicalFields := testCommitRequest(t, false)
		backend := newFakeCatalogBackend()
		// Pre-create the table with the user column "id" carrying the correct
		// stable identity but the wrong type. A real catalog would own the IDs;
		// the identity-keyed type check must still fail closed.
		var idIdentity string
		for _, field := range canonicalFields {
			if field.Name == "id" {
				idIdentity = stableFieldIdentity(field)
			}
		}
		if idIdentity == "" {
			t.Fatal("canonical schema is missing the id column")
		}
		wrong := iceberggo.NewSchema(0, iceberggo.NestedField{
			ID: 1, Name: "id", Type: iceberggo.StringType{}, Required: false, Doc: identityDoc(idIdentity),
		})
		target, _ := testIcebergConfig().target("public", "events")
		backend.tables[strings.Join(target, ".")] = catalogTable{
			Identifier: target, Schema: wrong, PartitionSpec: *iceberggo.UnpartitionedSpec,
		}
		committer := testCommitter(t, objects, backend)
		if _, err := committer.Commit(context.Background(), request); err == nil || !strings.Contains(err.Error(), "type changed") {
			t.Fatalf("identity type-change error=%v", err)
		}
	})

	t.Run("partition source field missing", func(t *testing.T) {
		request, objects, _ := testCommitRequest(t, false)
		plan, err := buildProjection(context.Background(), request, objects, testIcebergConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer plan.release()
		group := plan.groups[0]
		partitioned := iceberggo.NewPartitionSpec(iceberggo.PartitionField{
			SourceID: 999999, FieldID: 1000, Name: "missing", Transform: iceberggo.IdentityTransform{},
		})
		state := catalogTable{Identifier: group.target, Schema: group.schema, PartitionSpec: partitioned}
		if err := validatePartitionSpec(state); err == nil || !strings.Contains(err.Error(), "absent from the table schema") {
			t.Fatalf("partition compatibility error=%v", err)
		}
	})
}

func TestReconcileDistinguishesConflictAbsenceAndExpiredEvidence(t *testing.T) {
	t.Parallel()
	request, objects, _ := testCommitRequest(t, false)
	expected, err := expectedProjectionGroups(request, testIcebergConfig())
	if err != nil {
		t.Fatal(err)
	}
	group := expected[0]

	t.Run("conflicting batch summary", func(t *testing.T) {
		backend := newFakeCatalogBackend()
		summary := snapshotSummary(request, group.id, group.schemaFingerprint)
		summary[SummaryManifestSHA256] = strings.Repeat("0", 64)
		id := int64(1)
		backend.tables[strings.Join(group.target, ".")] = catalogTable{
			Identifier: group.target, Schema: iceberggo.NewSchema(0), PartitionSpec: *iceberggo.UnpartitionedSpec,
			CurrentSnapshotID: &id, Snapshots: []catalogSnapshot{{ID: id, Timestamp: request.AttemptedAt, Summary: summary}},
		}
		committer := testCommitter(t, objects, backend)
		if _, err := committer.Reconcile(context.Background(), request); !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("conflict error=%v", err)
		}
	})

	t.Run("conclusive absence", func(t *testing.T) {
		backend := newFakeCatalogBackend()
		id := int64(1)
		backend.tables[strings.Join(group.target, ".")] = catalogTable{
			Identifier: group.target, Schema: iceberggo.NewSchema(0), PartitionSpec: *iceberggo.UnpartitionedSpec,
			CurrentSnapshotID: &id, Snapshots: []catalogSnapshot{{ID: id, Timestamp: request.AttemptedAt.Add(-time.Minute), Summary: map[string]string{}}},
		}
		committer := testCommitter(t, objects, backend)
		result, err := committer.Reconcile(context.Background(), request)
		if err != nil || result.Disposition != artifactlog.CommitNotApplied {
			t.Fatalf("absence reconciliation=%+v err=%v", result, err)
		}
	})

	t.Run("expired history is indeterminate", func(t *testing.T) {
		backend := newFakeCatalogBackend()
		missingParent, id := int64(41), int64(42)
		backend.tables[strings.Join(group.target, ".")] = catalogTable{
			Identifier: group.target, Schema: iceberggo.NewSchema(0), PartitionSpec: *iceberggo.UnpartitionedSpec,
			CurrentSnapshotID: &id, Snapshots: []catalogSnapshot{{ID: id, ParentID: &missingParent, Timestamp: request.AttemptedAt.Add(time.Minute), Summary: map[string]string{}}},
		}
		committer := testCommitter(t, objects, backend)
		result, err := committer.Reconcile(context.Background(), request)
		if err != nil || result.Disposition != artifactlog.CommitIndeterminate {
			t.Fatalf("expired reconciliation=%+v err=%v", result, err)
		}
	})
}

type memoryCanonicalObjects struct {
	data map[string][]byte
}

func (objects *memoryCanonicalObjects) ReadVersion(_ context.Context, evidence artifactlog.ObjectEvidence) ([]byte, error) {
	value, ok := objects.data[evidence.Key+"@"+evidence.VersionID]
	if !ok {
		return nil, artifactlog.ErrObjectNotFound
	}
	return append([]byte(nil), value...), nil
}

type fakeCatalogBackend struct {
	mu                  sync.Mutex
	tables              map[string]catalogTable
	conflicts           int
	evolveConflicts     int
	appendCalls         int
	evolveCalls         int
	appendOrder         []string
	operations          []string
	lastSummary         map[string]string
	lastAppendFieldIDs  map[string]int
	lastParquetFieldIDs map[string]int
	nextSnapshot        int64
	catalogCalls        int
}

func newFakeCatalogBackend() *fakeCatalogBackend {
	return &fakeCatalogBackend{tables: map[string]catalogTable{}, nextSnapshot: 1}
}

func (backend *fakeCatalogBackend) Load(_ context.Context, identifier table.Identifier) (catalogTable, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.catalogCalls++
	state, ok := backend.tables[strings.Join(identifier, ".")]
	if !ok {
		return catalogTable{}, ErrTableNotFound
	}
	return state, nil
}

// Create mirrors a real Apache Iceberg REST catalog: it replaces the caller's
// field IDs with fresh sequential table field IDs and preserves field docs.
// Unit tests therefore cannot mask a committer that assumes its hash-derived
// caller IDs survive.
func (backend *fakeCatalogBackend) Create(_ context.Context, identifier table.Identifier, schema *iceberggo.Schema) (catalogTable, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.catalogCalls++
	key := strings.Join(identifier, ".")
	if _, exists := backend.tables[key]; exists {
		return catalogTable{}, ErrCatalogConflict
	}
	fresh, err := iceberggo.AssignFreshSchemaIDs(schema, nil)
	if err != nil {
		return catalogTable{}, err
	}
	state := catalogTable{Identifier: identifier, Schema: fresh, PartitionSpec: *iceberggo.UnpartitionedSpec}
	backend.tables[key] = state
	return state, nil
}

// Evolve preserves existing catalog field IDs and assigns fresh IDs beyond the
// current maximum to additive columns, exactly as a real Iceberg catalog does.
func (backend *fakeCatalogBackend) Evolve(_ context.Context, state catalogTable, adds []iceberggo.NestedField, renames []renameOp) (catalogTable, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.evolveCalls++
	if backend.evolveConflicts > 0 {
		backend.evolveConflicts--
		return catalogTable{}, ErrCatalogConflict
	}
	key := strings.Join(state.Identifier, ".")
	stored, ok := backend.tables[key]
	if !ok {
		return catalogTable{}, ErrTableNotFound
	}
	fields := stored.Schema.Fields()
	for _, rename := range renames {
		renamed := false
		for index := range fields {
			if fields[index].Name == rename.from {
				fields[index].Name = rename.to
				renamed = true
				break
			}
		}
		if !renamed {
			return catalogTable{}, errors.New("rename source " + rename.from + " not found")
		}
	}
	nextID := 0
	for _, field := range fields {
		if field.ID > nextID {
			nextID = field.ID
		}
	}
	for _, add := range adds {
		nextID++
		add.ID = nextID
		add.Required = false
		fields = append(fields, add)
	}
	stored.Schema = iceberggo.NewSchema(stored.Schema.ID+1, fields...)
	backend.tables[key] = stored
	return stored, nil
}

func (backend *fakeCatalogBackend) Append(_ context.Context, state catalogTable, _ *iceberggo.Schema, records []arrow.RecordBatch, summary map[string]string) (catalogSnapshot, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.appendCalls++
	if backend.conflicts > 0 {
		backend.conflicts--
		return catalogSnapshot{}, ErrCatalogConflict
	}
	key := strings.Join(state.Identifier, ".")
	stored := backend.tables[key]
	for _, record := range records {
		indices := record.Schema().FieldIndices("__op")
		if len(indices) == 1 {
			values := record.Column(indices[0]).(*array.String)
			for row := 0; row < values.Len(); row++ {
				backend.operations = append(backend.operations, values.Value(row))
			}
		}
	}
	if len(records) > 0 {
		schema := records[0].Schema()
		ids := make(map[string]int, schema.NumFields())
		for index := 0; index < schema.NumFields(); index++ {
			field := schema.Field(index)
			if value, ok := field.Metadata.GetValue("PARQUET:field_id"); ok {
				id, convErr := strconv.Atoi(value)
				if convErr != nil {
					return catalogSnapshot{}, convErr
				}
				ids[field.Name] = id
			}
		}
		backend.lastAppendFieldIDs = ids
		// Prove the committed data file itself carries the catalog field IDs by
		// serializing to Parquet and reading the native field IDs back.
		parquetIDs, parquetErr := parquetNativeFieldIDs(records[0])
		if parquetErr != nil {
			return catalogSnapshot{}, parquetErr
		}
		backend.lastParquetFieldIDs = parquetIDs
	}
	id := backend.nextSnapshot
	backend.nextSnapshot++
	snapshot := catalogSnapshot{ID: id, Timestamp: time.Now().UTC(), Summary: maps.Clone(summary)}
	if stored.CurrentSnapshotID != nil {
		parent := *stored.CurrentSnapshotID
		snapshot.ParentID = &parent
	}
	stored.Snapshots = append(stored.Snapshots, snapshot)
	stored.CurrentSnapshotID = &id
	stored.Schema = state.Schema
	backend.tables[key] = stored
	backend.appendOrder = append(backend.appendOrder, key)
	backend.lastSummary = maps.Clone(summary)
	return snapshot, nil
}

func (backend *fakeCatalogBackend) committedSnapshots() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	total := 0
	for _, state := range backend.tables {
		total += len(state.Snapshots)
	}
	return total
}

func testCommitter(t *testing.T, objects CanonicalObjectReader, backend catalogBackend) *Committer {
	t.Helper()
	committer, err := NewCommitter(objects, backend, testIcebergConfig())
	if err != nil {
		t.Fatal(err)
	}
	return committer
}

func testIcebergConfig() Config {
	return Config{
		Profile: CatalogProfileREST, URI: "http://catalog.invalid", Warehouse: "file:///tmp/wallaby-iceberg-test",
		ControlTable: "__wallaby_control", DestinationRevisionID: "iceberg-test-v1",
		MaxCommitRetries: 4, RequestTimeout: time.Second, ReconciliationHorizon: time.Hour, AllowHTTP: true,
	}
}

func testCommitRequest(t *testing.T, barriers bool) (artifactlog.CommitRequest, *memoryCanonicalObjects, []artifactlog.CanonicalField) {
	t.Helper()
	transaction := connector.SourceTransaction{
		SourceLineageID: "postgres-system/test-iceberg-v1", TransactionID: 71,
		BeginLSN: "0/10", CommitLSN: "0/18", EndLSN: "0/20",
		Checkpoint: connector.Checkpoint{LSN: "0/20", Timestamp: time.Unix(100, 0).UTC()},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{
			Schema: connector.Schema{Namespace: "public", Name: "events", Version: 1, Columns: []connector.Column{
				{Name: "id", Type: "int8", TypeMetadata: map[string]string{"source_relation_id": "42", "source_column_id": "1"}},
				{Name: "value", Type: "text", Nullable: true, TypeMetadata: map[string]string{"source_relation_id": "42", "source_column_id": "2"}},
			}},
			Records: []connector.Record{
				{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1, SourcePosition: "0/20", After: map[string]any{"id": int64(1), "value": "one"}},
				{Table: "events", Operation: connector.OpUpdate, SchemaVersion: 1, SourcePosition: "0/20", Before: map[string]any{"id": int64(1), "value": "one"}, After: map[string]any{"id": int64(1), "value": "two"}},
				{Table: "events", Operation: connector.OpDelete, SchemaVersion: 1, SourcePosition: "0/20", Before: map[string]any{"id": int64(1), "value": "two"}},
			},
		}}},
	}
	if barriers {
		transaction.Fragments = append([]connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{
			Schema:  connector.Schema{Namespace: "public", Name: "events", Version: 1},
			Records: []connector.Record{{Table: "events", Operation: connector.OpDDL, DDL: "ALTER TABLE events ADD COLUMN note text", SourcePosition: "0/20"}},
		}}}, transaction.Fragments...)
		transaction.Fragments[1].Ordinal = 1
	}
	return assembleCommitRequest(t, uuid.MustParse("44444444-4444-4444-4444-444444444444"), uuid.MustParse("55555555-5555-5555-5555-555555555555"), 1, transaction)
}

// assembleCommitRequest plans a source transaction into a rooted commit request
// and its immutable canonical objects, mirroring what the PostgreSQL-authoritative
// consumer assembles at runtime.
func assembleCommitRequest(t *testing.T, incarnationID, publicationID uuid.UUID, sequence int64, transaction connector.SourceTransaction) (artifactlog.CommitRequest, *memoryCanonicalObjects, []artifactlog.CanonicalField) {
	t.Helper()
	const mappingFingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	plan, err := artifactlog.NewEncoder().PlanMappedTransaction(context.Background(), incarnationID, mappingFingerprint, transaction)
	if err != nil {
		t.Fatal(err)
	}
	objects := &memoryCanonicalObjects{data: map[string][]byte{}}
	request := artifactlog.CommitRequest{
		FlowID: "flow-iceberg", FlowIncarnationID: incarnationID,
		ConsumerRevisionID: "iceberg-test-v1", PublicationID: publicationID,
		PublicationSequence: sequence, PositionID: transaction.EndLSN, CheckpointLSN: transaction.EndLSN,
		LogicalBatchID: plan.LogicalBatchID, ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: mappingFingerprint,
		AttemptedAt: time.Now().UTC(), Barriers: plan.Barriers,
	}
	manifest := sha256.New()
	var fields []artifactlog.CanonicalField
	for _, artifact := range plan.Artifacts {
		evidence := artifactlog.ObjectEvidence{
			Bucket: "canonical", Key: artifact.ObjectKey, VersionID: "version-" + artifact.ID,
			ChecksumSHA256: artifact.EncodedByteHash, Length: int64(len(artifact.Encoded)), ProjectionID: artifactlog.ProjectionIDV2, MappingFingerprint: mappingFingerprint,
		}
		objects.data[evidence.Key+"@"+evidence.VersionID] = append([]byte(nil), artifact.Encoded...)
		request.Objects = append(request.Objects, artifactlog.RootedArtifact{
			Evidence: evidence, ArtifactID: artifact.ID, LogicalBatchID: artifact.LogicalBatchID,
			Namespace: artifact.Namespace, Table: artifact.Table, SchemaID: artifact.SchemaID,
			SchemaJSON: artifact.SchemaJSON, EncodedByteHash: artifact.EncodedByteHash,
			FragmentOrdinal: artifact.FragmentOrdinal, FirstRecordOrdinal: artifact.FirstRecordOrdinal,
			RecordCount: artifact.RecordCount,
		})
		_, _ = manifest.Write([]byte(artifact.ID))
		_, _ = manifest.Write([]byte{0})
		_, _ = manifest.Write([]byte(artifact.EncodedByteHash))
		_, _ = manifest.Write([]byte{0})
		if fields == nil {
			var document canonicalSchemaDocument
			if err := json.Unmarshal(artifact.SchemaJSON, &document); err != nil {
				t.Fatal(err)
			}
			fields = document.Fields
		}
	}
	for _, barrier := range plan.Barriers {
		_, _ = manifest.Write([]byte("barrier"))
		_, _ = manifest.Write([]byte{0})
		_, _ = manifest.Write([]byte(barrier.ContentHash))
		_, _ = manifest.Write([]byte{0})
	}
	request.ManifestSHA256 = hex.EncodeToString(manifest.Sum(nil))
	request.CommitID = artifactlog.DeterministicCommitID(request.FlowIncarnationID, request.ConsumerRevisionID, request.PublicationID, request.ManifestSHA256)
	return request, objects, fields
}

// eventsTransaction builds a single-insert source transaction for the events
// table with the supplied columns. Distinct LSNs yield distinct logical
// batches so successive commits model schema evolution across publications.
func eventsTransaction(txID uint32, lsn string, columns []connector.Column, after map[string]any) connector.SourceTransaction {
	return connector.SourceTransaction{
		SourceLineageID: "postgres-system/test-iceberg-v1", TransactionID: txID,
		BeginLSN: lsn, CommitLSN: lsn, EndLSN: lsn,
		Checkpoint: connector.Checkpoint{LSN: lsn, Timestamp: time.Unix(int64(txID), 0).UTC()},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{
			Schema:  connector.Schema{Namespace: "public", Name: "events", Version: 1, Columns: columns},
			Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 1, SourcePosition: lsn, After: after}},
		}}},
	}
}

func column(name, pgType string, columnID int) connector.Column {
	return connector.Column{
		Name: name, Type: pgType, Nullable: true,
		TypeMetadata: map[string]string{"source_relation_id": "42", "source_column_id": strconv.Itoa(columnID)},
	}
}
