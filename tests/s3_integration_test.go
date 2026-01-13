package tests

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	s3dest "github.com/josephjohncox/wallaby/connectors/destinations/s3"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestS3PartitionedParquet(t *testing.T) {
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
		t.Fatalf("create s3 client: %v", err)
	}

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if !errors.As(err, &owned) && !errors.As(err, &exists) {
			t.Fatalf("create bucket: %v", err)
		}
	}

	table := "orders"
	schema := connector.Schema{
		Namespace: "public",
		Name:      table,
		Columns: []connector.Column{
			{Name: "id", Type: "int8"},
			{Name: "region", Type: "text"},
			{Name: "created_at", Type: "timestamptz"},
		},
	}

	t1 := time.Date(2025, 1, 2, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 3, 15, 30, 0, 0, time.UTC)

	records := []connector.Record{
		{
			Table:     table,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 1}),
			After: map[string]any{
				"id":         1,
				"region":     "us-east",
				"created_at": t1,
			},
			Timestamp: t1,
		},
		{
			Table:     table,
			Operation: connector.OpInsert,
			Key:       recordKey(t, map[string]any{"id": 2}),
			After: map[string]any{
				"id":         2,
				"region":     "eu-west",
				"created_at": t2,
			},
			Timestamp: t2,
		},
	}

	dest := &s3dest.Destination{}
	spec := connector.Spec{
		Name: "s3-test",
		Type: connector.EndpointS3,
		Options: map[string]string{
			"bucket":           bucket,
			"region":           region,
			"endpoint":         endpoint,
			"access_key":       accessKey,
			"secret_key":       secretKey,
			"force_path_style": "true",
			"format":           "parquet",
			"prefix":           "wallaby-test",
			"partition_by":     "region,created_at:day",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open s3 destination: %v", err)
	}
	defer dest.Close(ctx)

	batch := connector.Batch{Records: records, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "1"}}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	prefix := "wallaby-test/public/orders/"
	resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}
	if len(resp.Contents) == 0 {
		t.Fatalf("expected objects under %s", prefix)
	}

	var foundUS, foundEU bool
	for _, obj := range resp.Contents {
		key := aws.ToString(obj.Key)
		if strings.Contains(key, "region=us-east") && strings.Contains(key, "created_at=2025-01-02") {
			foundUS = true
			if err := assertParquetObject(ctx, client, bucket, key); err != nil {
				t.Fatalf("parquet object %s: %v", key, err)
			}
		}
		if strings.Contains(key, "region=eu-west") && strings.Contains(key, "created_at=2025-01-03") {
			foundEU = true
			if err := assertParquetObject(ctx, client, bucket, key); err != nil {
				t.Fatalf("parquet object %s: %v", key, err)
			}
		}
	}
	if !foundUS || !foundEU {
		t.Fatalf("partitioned object paths not found (us=%v eu=%v)", foundUS, foundEU)
	}
}

func assertParquetObject(ctx context.Context, client *s3.Client, bucket, key string) error {
	obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer obj.Body.Close()

	var reader io.Reader = obj.Body
	if strings.HasSuffix(key, ".gz") {
		gz, err := gzip.NewReader(obj.Body)
		if err != nil {
			return fmt.Errorf("gzip reader: %w", err)
		}
		defer gz.Close()
		reader = gz
	}

	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return fmt.Errorf("read parquet header: %w", err)
	}
	if string(header) != "PAR1" {
		return fmt.Errorf("unexpected parquet header: %q", string(header))
	}
	return nil
}

func newS3Client(ctx context.Context, endpoint, region, accessKey, secretKey string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return client, nil
}
