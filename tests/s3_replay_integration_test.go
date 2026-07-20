package tests

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	s3dest "github.com/josephjohncox/wallaby/connectors/destinations/s3"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestS3ReplayConvergenceAndConflict(t *testing.T) {
	endpoint := os.Getenv("WALLABY_TEST_S3_ENDPOINT")
	bucket := os.Getenv("WALLABY_TEST_S3_BUCKET")
	accessKey := os.Getenv("WALLABY_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("WALLABY_TEST_S3_SECRET_KEY")
	region := os.Getenv("WALLABY_TEST_S3_REGION")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("S3 test env not configured")
	}
	if region == "" {
		region = "us-east-1"
	}

	ctx := context.Background()
	client, err := newS3Client(ctx, endpoint, region, accessKey, secretKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) {
			t.Fatalf("create bucket: %v", err)
		}
	}

	prefix := fmt.Sprintf("wallaby-replay-%d", time.Now().UnixNano())
	destination := &s3dest.Destination{}
	spec := connector.Spec{
		Name: "s3-replay",
		Type: connector.EndpointS3,
		Options: map[string]string{
			"bucket":           bucket,
			"region":           region,
			"endpoint":         endpoint,
			"access_key":       accessKey,
			"secret_key":       secretKey,
			"force_path_style": "true",
			"format":           "json",
			"prefix":           prefix,
			"flow_id":          "s3-replay-integration",
		},
	}
	if err := destination.Open(ctx, spec); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(ctx)

	batch := connector.Batch{
		Schema: connector.Schema{Name: "orders", Namespace: "public", Version: 1},
		Records: []connector.Record{{
			Table:         "orders",
			Operation:     connector.OpInsert,
			SchemaVersion: 1,
			After:         map[string]any{"id": int64(1), "status": "new"},
		}},
		Checkpoint: connector.Checkpoint{LSN: "0/10"},
	}
	if err := destination.Write(ctx, batch); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := destination.Write(ctx, batch); err != nil {
		t.Fatalf("replay write: %v", err)
	}

	const writers = 8
	var wait sync.WaitGroup
	results := make(chan error, writers)
	for range writers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- destination.Write(ctx, batch)
		}()
	}
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent replay: %v", err)
		}
	}

	listed, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix + "/"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 1 {
		t.Fatalf("objects after repeated and concurrent replay = %d, want 1", len(listed.Contents))
	}
	key := aws.ToString(listed.Contents[0].Key)
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, metadataKey := range []string{"wallaby-batch-hash", "wallaby-position", "wallaby-codec-version", "wallaby-object-sha256"} {
		if metadataLookup(head.Metadata, metadataKey) == "" {
			t.Fatalf("object %s missing %s metadata: %v", key, metadataKey, head.Metadata)
		}
	}
	if head.ChecksumSHA256 == nil || aws.ToString(head.ChecksumSHA256) == "" {
		t.Fatalf("object %s did not return a stored SHA-256 checksum", key)
	}
	checksum, err := base64.StdEncoding.DecodeString(aws.ToString(head.ChecksumSHA256))
	if err != nil {
		t.Fatalf("decode object checksum: %v", err)
	}
	if got, want := hex.EncodeToString(checksum), metadataLookup(head.Metadata, "wallaby-object-sha256"); got != want {
		t.Fatalf("stored checksum = %s, metadata digest = %s", got, want)
	}
	object, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("get replay object: %v", err)
	}
	body, readErr := io.ReadAll(object.Body)
	closeErr := object.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read replay object: read=%v close=%v", readErr, closeErr)
	}
	digest := sha256.Sum256(body)
	if got, want := hex.EncodeToString(digest[:]), metadataLookup(head.Metadata, "wallaby-object-sha256"); got != want {
		t.Fatalf("object bytes digest = %s, metadata digest = %s", got, want)
	}

	conflicting := batch
	conflicting.Records = []connector.Record{{
		Table:         "orders",
		Operation:     connector.OpInsert,
		SchemaVersion: 1,
		After:         map[string]any{"id": int64(1), "status": "conflicting"},
	}}
	if err := destination.Write(ctx, conflicting); !errors.Is(err, s3dest.ErrObjectConflict) {
		t.Fatalf("conflicting write error = %v, want object conflict", err)
	}
	listed, err = client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix + "/"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 1 {
		t.Fatalf("objects after conflict = %d, want original object only", len(listed.Contents))
	}
}

func TestS3PartitionedReplayRepairsMissingObject(t *testing.T) {
	endpoint := os.Getenv("WALLABY_TEST_S3_ENDPOINT")
	bucket := os.Getenv("WALLABY_TEST_S3_BUCKET")
	accessKey := os.Getenv("WALLABY_TEST_S3_ACCESS_KEY")
	secretKey := os.Getenv("WALLABY_TEST_S3_SECRET_KEY")
	region := os.Getenv("WALLABY_TEST_S3_REGION")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("S3 test env not configured")
	}
	if region == "" {
		region = "us-east-1"
	}

	ctx := context.Background()
	client, err := newS3Client(ctx, endpoint, region, accessKey, secretKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) {
			t.Fatalf("create bucket: %v", err)
		}
	}

	prefix := fmt.Sprintf("wallaby-partition-replay-%d", time.Now().UnixNano())
	destination := &s3dest.Destination{}
	if err := destination.Open(ctx, connector.Spec{
		Name: "s3-partition-replay",
		Type: connector.EndpointS3,
		Options: map[string]string{
			"bucket":           bucket,
			"region":           region,
			"endpoint":         endpoint,
			"access_key":       accessKey,
			"secret_key":       secretKey,
			"force_path_style": "true",
			"format":           "json",
			"prefix":           prefix,
			"flow_id":          "s3-partition-replay-integration",
			"partition_by":     "region",
		},
	}); err != nil {
		t.Fatal(err)
	}
	defer destination.Close(ctx)

	batch := connector.Batch{
		Schema: connector.Schema{Name: "orders", Namespace: "public", Version: 1},
		Records: []connector.Record{
			{Table: "orders", Operation: connector.OpInsert, SchemaVersion: 1, After: map[string]any{"id": int64(1), "region": "us"}},
			{Table: "orders", Operation: connector.OpInsert, SchemaVersion: 1, After: map[string]any{"id": int64(2), "region": "eu"}},
		},
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}
	if err := destination.Write(ctx, batch); err != nil {
		t.Fatalf("first partitioned write: %v", err)
	}
	listed, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix + "/")})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 3 {
		t.Fatalf("partitioned objects = %d, want identity marker plus two partitions", len(listed.Contents))
	}
	var deletedKey string
	for _, object := range listed.Contents {
		key := aws.ToString(object.Key)
		if !strings.Contains(key, "/_wallaby_batches/") {
			deletedKey = key
			break
		}
	}
	if deletedKey == "" {
		t.Fatal("partitioned write did not produce a data object")
	}
	if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(deletedKey)}); err != nil {
		t.Fatalf("delete one partition object: %v", err)
	}
	if err := destination.Write(ctx, batch); err != nil {
		t.Fatalf("repair partitioned replay: %v", err)
	}
	listed, err = client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix + "/")})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Contents) != 3 {
		t.Fatalf("objects after partition repair = %d, want 3", len(listed.Contents))
	}
}
