package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/wire"
	"pgregory.net/rapid"
)

func TestCapabilitiesDoNotOverstateRestartReplaySafety(t *testing.T) {
	t.Parallel()

	capabilities := (&Destination{}).Capabilities()
	if !capabilities.Delivery.Declared {
		t.Fatalf("S3 delivery semantics are undeclared: %+v", capabilities.Delivery)
	}
	if capabilities.Delivery.IdempotentReplay || capabilities.Delivery.ReplaySafe || capabilities.Delivery.TransactionalBatch {
		t.Fatalf("S3 overstates batch-boundary or transactional replay safety: %+v", capabilities.Delivery)
	}
}

func TestDestinationReplayConverges(t *testing.T) {
	t.Parallel()

	for _, format := range []string{"json", "parquet"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			client := newFakeObjectClient()
			destination := testDestination(t, client, format, "")
			batch := s3TestBatch()

			if err := destination.Write(context.Background(), batch); err != nil {
				t.Fatalf("first Write() error = %v", err)
			}
			if err := destination.Write(context.Background(), batch); err != nil {
				t.Fatalf("replay Write() error = %v", err)
			}
			if got := client.objectCount(); got != 1 {
				t.Fatalf("object count after replay = %d, want 1", got)
			}
			if got := client.putCount(); got != 2 {
				t.Fatalf("put count after replay = %d, want 2 conditional attempts", got)
			}
		})
	}
}

func TestDestinationRejectsConflictingContentAtSamePosition(t *testing.T) {
	t.Parallel()

	client := newFakeObjectClient()
	destination := testDestination(t, client, "json", "")
	original := s3TestBatch()
	if err := destination.Write(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	originalObject := client.onlyObject(t)

	conflicting := s3TestBatch()
	conflicting.Records[0].After = map[string]any{"id": int64(1), "status": "conflicting"}
	err := destination.Write(context.Background(), conflicting)
	if !errors.Is(err, ErrObjectConflict) {
		t.Fatalf("conflicting Write() error = %v, want object conflict", err)
	}
	var conflict *ObjectConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("conflicting Write() error type = %T, want *ObjectConflictError", err)
	}
	if conflict.ExpectedHash == conflict.ActualHash {
		t.Fatalf("conflict hashes unexpectedly match: %+v", conflict)
	}
	if got := client.objectCount(); got != 1 {
		t.Fatalf("object count after conflict = %d, want 1", got)
	}
	if got := client.onlyObject(t); !bytes.Equal(got.body, originalObject.body) {
		t.Fatal("conflicting replay overwrote the original object")
	}
}

func TestDestinationAmbiguousSuccessfulPutConverges(t *testing.T) {
	t.Parallel()

	client := newFakeObjectClient()
	client.ambiguousAfterStore = 1
	destination := testDestination(t, client, "json", "")

	if err := destination.Write(context.Background(), s3TestBatch()); err != nil {
		t.Fatalf("ambiguous Write() error = %v", err)
	}
	if got := client.objectCount(); got != 1 {
		t.Fatalf("object count after ambiguous success = %d, want 1", got)
	}
	if got := client.headCount(); got != 1 {
		t.Fatalf("head count after ambiguous success = %d, want 1", got)
	}
}

func TestDestinationPartialPartitionReplayConverges(t *testing.T) {
	t.Parallel()

	client := newFakeObjectClient()
	client.failBeforeStore = 2
	destination := testDestination(t, client, "json", "region")
	batch := s3TestBatch()
	batch.Records = append(batch.Records, connector.Record{
		Table:         "orders",
		Operation:     connector.OpInsert,
		SchemaVersion: 2,
		After:         map[string]any{"id": int64(2), "status": "new", "region": "us/west"},
		Timestamp:     time.Unix(20, 0).UTC(),
	})
	batch.Records[0].After["region"] = "eu_west"

	if err := destination.Write(context.Background(), batch); err == nil {
		t.Fatal("first partitioned Write() unexpectedly succeeded")
	}
	if got := client.objectCount(); got != 1 {
		t.Fatalf("object count after partial failure = %d, want 1", got)
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatalf("partition replay Write() error = %v", err)
	}
	if got := client.objectCount(); got != 3 {
		t.Fatalf("object count after partition replay = %d, want identity marker plus 2 partition objects", got)
	}
}

func TestDestinationRejectsPartitionSetConflictAtSamePosition(t *testing.T) {
	t.Parallel()

	client := newFakeObjectClient()
	destination := testDestination(t, client, "json", "region")
	original := s3TestBatch()
	original.Records[0].After["region"] = "us"
	if err := destination.Write(context.Background(), original); err != nil {
		t.Fatalf("original Write() error = %v", err)
	}

	conflicting := s3TestBatch()
	conflicting.Records[0].After["region"] = "eu"
	if err := destination.Write(context.Background(), conflicting); !errors.Is(err, ErrObjectConflict) {
		t.Fatalf("partition conflict Write() error = %v, want object conflict", err)
	}
	if got := client.objectCount(); got != 2 {
		t.Fatalf("object count after partition conflict = %d, want identity marker and original partition", got)
	}
}

func TestDestinationReconciliationRequiresStoredChecksum(t *testing.T) {
	t.Parallel()

	client := newFakeObjectClient()
	destination := testDestination(t, client, "json", "")
	batch := s3TestBatch()
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatalf("first Write() error = %v", err)
	}
	client.omitHeadChecksum = true
	if err := destination.Write(context.Background(), batch); !errors.Is(err, ErrObjectConflict) {
		t.Fatalf("replay without stored checksum error = %v, want object conflict", err)
	}
}

func TestDestinationConcurrentIdenticalWritersConverge(t *testing.T) {
	t.Parallel()

	client := newFakeObjectClient()
	destination := testDestination(t, client, "json", "")
	batch := s3TestBatch()
	const writers = 16
	results := make(chan error, writers)
	var wait sync.WaitGroup
	for range writers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- destination.Write(context.Background(), batch)
		}()
	}
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("concurrent Write() error = %v", err)
		}
	}
	if got := client.objectCount(); got != 1 {
		t.Fatalf("object count after concurrent writes = %d, want 1", got)
	}
}

func TestPartitionEncodingIsReversibleAndCollisionFreeRapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		value := rapid.String().Draw(t, "value")
		encoded := stablePathValue(value)
		if !strings.HasPrefix(encoded, "v1-") {
			t.Fatalf("stable path value %q lacks version prefix", encoded)
		}
		decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(encoded, "v1-"))
		if err != nil {
			t.Fatalf("decode stable path value %q: %v", encoded, err)
		}
		if string(decoded) != value {
			t.Fatalf("partition value round trip = %q, want %q", decoded, value)
		}

		other := value + "\x00"
		if stablePathValue(other) == encoded {
			t.Fatalf("distinct partition values %q and %q collided", value, other)
		}
	})
}

func TestPartitionEncodingDistinguishesPreviouslyLossyValues(t *testing.T) {
	t.Parallel()

	destination := testDestination(t, newFakeObjectClient(), "json", "region")
	values := []struct {
		label string
		value any
	}{
		{label: "nil", value: nil},
		{label: "string null", value: "null"},
		{label: "bytes null", value: []byte("null")},
		{label: "string one", value: "1"},
		{label: "int64 one", value: int64(1)},
		{label: "float64 one", value: float64(1)},
		{label: "slash", value: "a/b"},
		{label: "dot", value: "."},
		{label: "dot dot", value: ".."},
		{label: "empty", value: ""},
	}
	paths := make(map[string]string)
	for _, testValue := range values {
		partitionPath, err := destination.partitionPath(connector.Record{After: map[string]any{"region": testValue.value}})
		if err != nil {
			t.Fatal(err)
		}
		if previous, exists := paths[partitionPath]; exists {
			t.Fatalf("partition values %s and %s produced path %q", previous, testValue.label, partitionPath)
		}
		paths[partitionPath] = testValue.label
		if strings.Contains(partitionPath, "/./") || strings.Contains(partitionPath, "/../") {
			t.Fatalf("partition value %s produced path-normalized segment %q", testValue.label, partitionPath)
		}
	}
}

func testDestination(t testing.TB, client objectClient, format, partitionBy string) *Destination {
	t.Helper()
	codec, err := wire.NewCodec(format)
	if err != nil {
		t.Fatalf("NewCodec(%q) error = %v", format, err)
	}
	return &Destination{
		spec: connector.Spec{
			Name:    "archive",
			Options: map[string]string{"flow_id": "flow/orders"},
		},
		bucket:     "wallaby-test",
		prefix:     "cdc",
		format:     format,
		partitions: parsePartitionBy(partitionBy),
		codec:      codec,
		client:     client,
	}
}

func s3TestBatch() connector.Batch {
	return connector.Batch{
		Schema: connector.Schema{
			Name:      "orders",
			Namespace: "public",
			Version:   2,
			Columns: []connector.Column{
				{Name: "id", Type: "int8"},
				{Name: "status", Type: "text"},
				{Name: "region", Type: "text"},
			},
		},
		Records: []connector.Record{{
			Table:         "orders",
			Operation:     connector.OpInsert,
			SchemaVersion: 2,
			After:         map[string]any{"id": int64(1), "status": "new"},
			Timestamp:     time.Unix(10, 0).UTC(),
		}},
		Checkpoint: connector.Checkpoint{LSN: "0/10"},
		WireFormat: connector.WireFormatJSON,
	}
}

type fakeStoredObject struct {
	body           []byte
	metadata       map[string]string
	checksumSHA256 string
}

type fakeObjectClient struct {
	mu                  sync.Mutex
	objects             map[string]fakeStoredObject
	puts                int
	heads               int
	failBeforeStore     int
	ambiguousAfterStore int
	omitHeadChecksum    bool
}

func newFakeObjectClient() *fakeObjectClient {
	return &fakeObjectClient{objects: make(map[string]fakeStoredObject)}
}

func (c *fakeObjectClient) PutObject(ctx context.Context, input *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(body)
	wantChecksum := base64.StdEncoding.EncodeToString(digest[:])
	if input.IfNoneMatch == nil || *input.IfNoneMatch != "*" {
		return nil, errors.New("missing If-None-Match: *")
	}
	if input.ChecksumSHA256 == nil || *input.ChecksumSHA256 != wantChecksum {
		return nil, errors.New("invalid SHA-256 checksum")
	}
	if objectMetadata(input.Metadata, metadataBatchHash) == "" || objectMetadata(input.Metadata, metadataPosition) == "" {
		return nil, errors.New("missing logical identity metadata")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts++
	request := c.puts
	key := *input.Key
	if _, exists := c.objects[key]; exists {
		return nil, errors.New("precondition failed")
	}
	if c.failBeforeStore == request {
		return nil, errors.New("injected put failure")
	}
	c.objects[key] = fakeStoredObject{
		body:           append([]byte(nil), body...),
		metadata:       cloneMetadata(input.Metadata),
		checksumSHA256: *input.ChecksumSHA256,
	}
	if c.ambiguousAfterStore == request {
		return nil, errors.New("injected lost put response")
	}
	return &awss3.PutObjectOutput{}, nil
}

func (c *fakeObjectClient) HeadObject(ctx context.Context, input *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if input.ChecksumMode != types.ChecksumModeEnabled {
		return nil, errors.New("checksum mode is not enabled")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.heads++
	object, exists := c.objects[*input.Key]
	if !exists {
		return nil, errors.New("not found")
	}
	length := int64(len(object.body))
	var checksum *string
	if !c.omitHeadChecksum {
		stored := object.checksumSHA256
		checksum = &stored
	}
	return &awss3.HeadObjectOutput{
		ChecksumSHA256: checksum,
		ContentLength:  &length,
		Metadata:       cloneMetadata(object.metadata),
	}, nil
}

func (c *fakeObjectClient) objectCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.objects)
}

func (c *fakeObjectClient) putCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.puts
}

func (c *fakeObjectClient) headCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.heads
}

func (c *fakeObjectClient) onlyObject(t testing.TB) fakeStoredObject {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.objects) != 1 {
		t.Fatalf("onlyObject called with %d objects", len(c.objects))
	}
	for _, object := range c.objects {
		return fakeStoredObject{
			body:           append([]byte(nil), object.body...),
			metadata:       cloneMetadata(object.metadata),
			checksumSHA256: object.checksumSHA256,
		}
	}
	return fakeStoredObject{}
}

func cloneMetadata(metadata map[string]string) map[string]string {
	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[strings.ToLower(key)] = value
	}
	return cloned
}
