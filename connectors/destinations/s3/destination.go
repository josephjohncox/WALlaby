package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/wire"
)

const (
	optBucket       = "bucket"
	optPrefix       = "prefix"
	optRegion       = "region"
	optFormat       = "format"
	optCompression  = "compression"
	optPartitionBy  = "partition_by"
	optEndpoint     = "endpoint"
	optAccessKey    = "access_key"
	optSecretKey    = "secret_key"
	optSessionToken = "session_token"
	optPathStyle    = "force_path_style"
	optUseFIPS      = "use_fips"
	optUseDualstack = "use_dualstack"
)

type partitionSpec struct {
	name   string
	bucket string
}

// Destination writes batches to S3.
type Destination struct {
	spec           connector.Spec
	bucket         string
	prefix         string
	format         string
	compression    string
	partitions     []partitionSpec
	endpoint       string
	accessKey      string
	secretKey      string
	sessionToken   string
	forcePathStyle bool
	useFIPS        bool
	useDualstack   bool
	codec          wire.Codec
	uploader       *manager.Uploader
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	d.spec = spec
	d.bucket = spec.Options[optBucket]
	if d.bucket == "" {
		return errors.New("s3 bucket is required")
	}
	d.prefix = strings.TrimPrefix(spec.Options[optPrefix], "/")
	d.format = spec.Options[optFormat]
	d.compression = strings.ToLower(spec.Options[optCompression])
	d.partitions = parsePartitionBy(spec.Options[optPartitionBy])
	d.endpoint = strings.TrimSpace(spec.Options[optEndpoint])
	d.accessKey = strings.TrimSpace(spec.Options[optAccessKey])
	d.secretKey = strings.TrimSpace(spec.Options[optSecretKey])
	d.sessionToken = strings.TrimSpace(spec.Options[optSessionToken])
	d.forcePathStyle = parseBool(spec.Options[optPathStyle])
	d.useFIPS = parseBool(spec.Options[optUseFIPS])
	d.useDualstack = parseBool(spec.Options[optUseDualstack])

	codec, err := wire.NewCodec(d.format)
	if err != nil {
		return err
	}
	d.codec = codec

	loadOpts := []func(*config.LoadOptions) error{}
	region := strings.TrimSpace(spec.Options[optRegion])
	if region == "" && d.endpoint != "" {
		region = "us-east-1"
	}
	if region != "" {
		loadOpts = append(loadOpts, config.WithRegion(region))
	}
	if d.accessKey != "" && d.secretKey != "" {
		creds := credentials.NewStaticCredentialsProvider(d.accessKey, d.secretKey, d.sessionToken)
		loadOpts = append(loadOpts, config.WithCredentialsProvider(creds))
	}
	if d.endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:               d.endpoint,
					SigningRegion:     region,
					HostnameImmutable: true,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		loadOpts = append(loadOpts, config.WithEndpointResolverWithOptions(resolver))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if d.forcePathStyle {
			o.UsePathStyle = true
		}
		if d.useFIPS {
			o.EndpointOptions.UseFIPSEndpoint = aws.FIPSEndpointStateEnabled
		}
		if d.useDualstack {
			o.EndpointOptions.UseDualStackEndpoint = aws.DualStackEndpointStateEnabled
		}
	})
	d.uploader = manager.NewUploader(client)

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.uploader == nil {
		return errors.New("s3 destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}
	if len(d.partitions) == 0 {
		return d.writeBatch(ctx, batch, connector.Record{})
	}

	grouped := map[string][]connector.Record{}
	representative := map[string]connector.Record{}
	for _, record := range batch.Records {
		partPath, err := d.partitionPath(batch.Schema, record)
		if err != nil {
			return err
		}
		grouped[partPath] = append(grouped[partPath], record)
		if _, ok := representative[partPath]; !ok {
			representative[partPath] = record
		}
	}

	for partPath, records := range grouped {
		subBatch := connector.Batch{
			Records:    records,
			Schema:     batch.Schema,
			Checkpoint: batch.Checkpoint,
			WireFormat: batch.WireFormat,
		}
		record := representative[partPath]
		if err := d.writeBatch(ctx, subBatch, record, partPath); err != nil {
			return err
		}
	}
	return nil
}

func (d *Destination) writeBatch(ctx context.Context, batch connector.Batch, record connector.Record, partitions ...string) error {
	if len(batch.Records) == 0 {
		return nil
	}
	payload, err := d.codec.Encode(batch)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}

	body, contentType, contentEncoding, err := d.prepareBody(payload)
	if err != nil {
		return err
	}

	partPath := ""
	if len(partitions) > 0 {
		partPath = partitions[0]
	}
	key := d.objectKey(batch.Schema, record, partPath)
	input := &s3.PutObjectInput{
		Bucket:      &d.bucket,
		Key:         &key,
		Body:        body,
		ContentType: &contentType,
	}
	if contentEncoding != "" {
		input.ContentEncoding = &contentEncoding
	}

	_, err = d.uploader.Upload(ctx, input)
	if err != nil {
		return fmt.Errorf("upload to s3: %w", err)
	}

	return nil
}

func (d *Destination) ApplyDDL(_ context.Context, _ connector.Schema, _ connector.Record) error {
	return nil
}

func (d *Destination) TypeMappings() map[string]string { return nil }

func (d *Destination) Close(_ context.Context) error {
	return nil
}

func (d *Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     false,
		SupportsBulkLoad:      true,
		SupportsTypeMapping:   true,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatArrow,
			connector.WireFormatParquet,
			connector.WireFormatAvro,
			connector.WireFormatProto,
			connector.WireFormatJSON,
		},
	}
}

func (d *Destination) objectKey(schema connector.Schema, record connector.Record, partitionPath string) string {
	stamp := time.Now().UTC().Format("20060102T150405Z")
	table := record.Table
	if table == "" {
		table = schema.Name
	}
	suffix := d.fileSuffix()
	ext := extensionForFormat(d.codec.Name())
	if d.compression == "gzip" {
		ext = ext + ".gz"
	}
	name := fmt.Sprintf("%s_%s_%d_%s.%s", table, stamp, schema.Version, suffix, ext)

	parts := make([]string, 0, 6)
	if d.prefix != "" {
		parts = append(parts, d.prefix)
	}
	if schema.Namespace != "" {
		parts = append(parts, schema.Namespace)
	}
	if table != "" {
		parts = append(parts, table)
	}
	if partitionPath != "" {
		parts = append(parts, partitionPath)
	}
	parts = append(parts, name)
	return path.Join(parts...)
}

func extensionForFormat(format connector.WireFormat) string {
	switch format {
	case connector.WireFormatArrow:
		return "arrow"
	case connector.WireFormatParquet:
		return "parquet"
	case connector.WireFormatAvro:
		return "avro"
	case connector.WireFormatProto:
		return "pb"
	case connector.WireFormatJSON:
		return "json"
	default:
		return "bin"
	}
}

func (d *Destination) prepareBody(payload []byte) (io.Reader, string, string, error) {
	contentType := d.codec.ContentType()
	if d.compression == "gzip" {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(payload); err != nil {
			_ = gz.Close()
			return nil, "", "", fmt.Errorf("gzip: %w", err)
		}
		if err := gz.Close(); err != nil {
			return nil, "", "", fmt.Errorf("gzip close: %w", err)
		}
		return bytes.NewReader(buf.Bytes()), contentType, "gzip", nil
	}

	return bytes.NewReader(payload), contentType, "", nil
}

func (d *Destination) fileSuffix() string {
	return uuid.NewString()
}

func parsePartitionBy(raw string) []partitionSpec {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]partitionSpec, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name := part
		bucket := ""
		if strings.Contains(part, ":") {
			pieces := strings.SplitN(part, ":", 2)
			name = strings.TrimSpace(pieces[0])
			bucket = strings.TrimSpace(pieces[1])
		}
		if name == "" {
			continue
		}
		out = append(out, partitionSpec{name: name, bucket: strings.ToLower(bucket)})
	}
	return out
}

func (d *Destination) partitionPath(schema connector.Schema, record connector.Record) (string, error) {
	if len(d.partitions) == 0 {
		return "", nil
	}
	values := record.After
	if values == nil {
		values = record.Before
	}
	parts := make([]string, 0, len(d.partitions))
	for _, spec := range d.partitions {
		val, ok := values[spec.name]
		if !ok || val == nil {
			if isIngestTimePartition(spec.name) {
				val = record.Timestamp
			}
		}
		if val == nil && spec.bucket != "" && !record.Timestamp.IsZero() {
			val = record.Timestamp
		}
		formatted, err := formatPartitionValue(val, spec.bucket)
		if err != nil {
			return "", fmt.Errorf("partition %s: %w", spec.name, err)
		}
		parts = append(parts, fmt.Sprintf("%s=%s", spec.name, sanitizePartitionValue(formatted)))
	}
	return path.Join(parts...), nil
}

func isIngestTimePartition(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "ingest_time", "_ingest_time", "ingest_timestamp":
		return true
	default:
		return false
	}
}

func formatPartitionValue(value any, bucket string) (string, error) {
	if value == nil {
		return "null", nil
	}
	if bucket != "" {
		t, ok := parsePartitionTime(value)
		if !ok {
			return "", fmt.Errorf("expected time value for bucket %s", bucket)
		}
		return formatTimeBucket(t, bucket), nil
	}
	switch v := value.(type) {
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano), nil
	case json.RawMessage:
		return string(v), nil
	case []byte:
		return string(v), nil
	default:
		return fmt.Sprint(v), nil
	}
}

func parsePartitionTime(value any) (time.Time, bool) {
	switch v := value.(type) {
	case time.Time:
		return v, true
	case *time.Time:
		if v == nil {
			return time.Time{}, false
		}
		return *v, true
	case string:
		return parseTimeString(v)
	case []byte:
		return parseTimeString(string(v))
	default:
		return time.Time{}, false
	}
}

func parseTimeString(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts, true
		}
	}
	return time.Time{}, false
}

func formatTimeBucket(value time.Time, bucket string) string {
	ts := value.UTC()
	switch bucket {
	case "year":
		return ts.Format("2006")
	case "month":
		return ts.Format("2006-01")
	case "day":
		return ts.Format("2006-01-02")
	case "hour":
		return ts.Format("2006-01-02-15")
	default:
		return ts.Format(time.RFC3339Nano)
	}
}

func sanitizePartitionValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "null"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "-")
	value = replacer.Replace(value)
	return value
}

func parseBool(raw string) bool {
	if raw == "" {
		return false
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false
	}
	return value
}
