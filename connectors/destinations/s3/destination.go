package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
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

const (
	directObjectVersion     = "wallaby_direct_s3_v2"
	metadataBatchHash       = "wallaby-batch-hash"
	metadataPosition        = "wallaby-position"
	metadataCodecVersion    = "wallaby-codec-version"
	metadataObjectSHA256    = "wallaby-object-sha256"
	metadataRegistrySubject = "wallaby-registry-subject"
	metadataRegistryID      = "wallaby-registry-id"
	metadataRegistryVersion = "wallaby-registry-version"
)

// ErrObjectConflict identifies an existing object whose stable logical identity
// names different content. The original object is never overwritten.
var ErrObjectConflict = errors.New("s3 object identity conflict")

// ObjectConflictError describes a fail-closed stable-key collision.
const maxConditionalPutBytes int64 = 5 << 30

type ObjectConflictError struct {
	Bucket       string
	Key          string
	ExpectedHash string
	ActualHash   string
	Reason       string
}

func (e *ObjectConflictError) Error() string {
	return fmt.Sprintf(
		"%v for s3://%s/%s: %s (expected hash %q, actual hash %q)",
		ErrObjectConflict,
		e.Bucket,
		e.Key,
		e.Reason,
		e.ExpectedHash,
		e.ActualHash,
	)
}

func (e *ObjectConflictError) Unwrap() error { return ErrObjectConflict }

type partitionSpec struct {
	name   string
	bucket string
}

type objectClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// Destination writes batches to S3.
type Destination struct {
	spec              connector.Spec
	bucket            string
	prefix            string
	format            string
	compression       string
	partitions        []partitionSpec
	endpoint          string
	accessKey         string
	secretKey         string
	sessionToken      string
	forcePathStyle    bool
	useFIPS           bool
	useDualstack      bool
	codec             wire.Codec
	client            objectClient
	registry          schemaregistry.Registry
	registrySubject   string
	protoTypesSubject string
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
	d.registrySubject = strings.TrimSpace(spec.Options[schemaregistry.OptRegistrySubject])
	d.protoTypesSubject = strings.TrimSpace(spec.Options[schemaregistry.OptRegistryProtoTypes])
	switch d.codec.Name() {
	case connector.WireFormatAvro, connector.WireFormatProto:
		registryCfg := schemaregistry.ConfigFromOptions(spec.Options)
		registry, err := schemaregistry.NewRegistry(ctx, registryCfg)
		if err != nil && !errors.Is(err, schemaregistry.ErrRegistryDisabled) {
			return err
		}
		if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
			registry = nil
		}
		d.registry = registry
	}

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

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if d.endpoint != "" {
			o.BaseEndpoint = aws.String(d.endpoint)
		}
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
	d.client = client

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.client == nil {
		return errors.New("s3 destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}
	meta, err := d.ensureSchema(ctx, batch.Schema)
	if err != nil {
		if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
			meta = nil
		} else {
			return err
		}
	}
	if len(d.partitions) == 0 {
		return d.writeBatch(ctx, batch, connector.Record{}, meta)
	}

	grouped := map[string][]connector.Record{}
	representative := map[string]connector.Record{}
	for _, record := range batch.Records {
		partPath, err := d.partitionPath(record)
		if err != nil {
			return err
		}
		grouped[partPath] = append(grouped[partPath], record)
		if _, ok := representative[partPath]; !ok {
			representative[partPath] = record
		}
	}

	partitionPaths := make([]string, 0, len(grouped))
	for partPath := range grouped {
		partitionPaths = append(partitionPaths, partPath)
	}
	sort.Strings(partitionPaths)
	if err := d.reservePartitionedBatch(ctx, batch); err != nil {
		return err
	}
	for _, partPath := range partitionPaths {
		records := grouped[partPath]
		subBatch := connector.Batch{
			Records:    records,
			Schema:     batch.Schema,
			Checkpoint: batch.Checkpoint,
			WireFormat: batch.WireFormat,
		}
		record := representative[partPath]
		if err := d.writeBatch(ctx, subBatch, record, meta, partPath); err != nil {
			return err
		}
	}
	return nil
}

func (d *Destination) reservePartitionedBatch(ctx context.Context, batch connector.Batch) error {
	batchHash, err := connector.BatchContentHash(batch)
	if err != nil {
		return fmt.Errorf("hash partitioned s3 batch: %w", err)
	}
	position, err := connector.CheckpointPositionID(batch.Checkpoint)
	if err != nil {
		return fmt.Errorf("identify partitioned s3 batch position: %w", err)
	}
	key, err := d.batchIdentityKey(batch)
	if err != nil {
		return err
	}
	body := []byte(batchHash + "\n")
	digest := sha256.Sum256(body)
	objectSHA256 := hex.EncodeToString(digest[:])
	checksumSHA256 := base64.StdEncoding.EncodeToString(digest[:])
	codecVersion := d.codecVersion()
	contentType := "text/plain"
	if _, err := d.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:         &d.bucket,
		Key:            &key,
		Body:           bytes.NewReader(body),
		ChecksumSHA256: &checksumSHA256,
		ContentLength:  aws.Int64(int64(len(body))),
		ContentType:    &contentType,
		IfNoneMatch:    aws.String("*"),
		Metadata: map[string]string{
			metadataBatchHash:    batchHash,
			metadataPosition:     position,
			metadataCodecVersion: codecVersion,
			metadataObjectSHA256: objectSHA256,
		},
	}); err != nil {
		reconcileErr := d.reconcileObject(ctx, key, position, batchHash, codecVersion)
		if reconcileErr == nil {
			return nil
		}
		if errors.Is(reconcileErr, ErrObjectConflict) {
			return reconcileErr
		}
		return fmt.Errorf("reserve partitioned s3 batch: %w (reconcile identity object: %w)", err, reconcileErr)
	}
	return nil
}

func (d *Destination) writeBatch(ctx context.Context, batch connector.Batch, record connector.Record, meta *schemaMeta, partitions ...string) error {
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
	if int64(len(body)) > maxConditionalPutBytes {
		return fmt.Errorf("S3 object size %d exceeds the 5 GiB conditional single-PUT limit; replay-safe multipart upload is not implemented", len(body))
	}
	batchHash, err := connector.BatchContentHash(batch)
	if err != nil {
		return fmt.Errorf("hash s3 batch: %w", err)
	}
	position, err := connector.CheckpointPositionID(batch.Checkpoint)
	if err != nil {
		return fmt.Errorf("identify s3 batch position: %w", err)
	}

	partPath := ""
	if len(partitions) > 0 {
		partPath = partitions[0]
	}
	key, err := d.objectKey(batch, record, partPath)
	if err != nil {
		return err
	}
	objectDigest := sha256.Sum256(body)
	objectSHA256 := hex.EncodeToString(objectDigest[:])
	checksumSHA256 := base64.StdEncoding.EncodeToString(objectDigest[:])
	codecVersion := d.codecVersion()
	metadata := map[string]string{
		metadataBatchHash:    batchHash,
		metadataPosition:     position,
		metadataCodecVersion: codecVersion,
		metadataObjectSHA256: objectSHA256,
	}
	if meta != nil {
		metadata[metadataRegistrySubject] = meta.Subject
		metadata[metadataRegistryID] = meta.ID
		if meta.Version > 0 {
			metadata[metadataRegistryVersion] = fmt.Sprintf("%d", meta.Version)
		}
	}

	input := &s3.PutObjectInput{
		Bucket:         &d.bucket,
		Key:            &key,
		Body:           bytes.NewReader(body),
		ChecksumSHA256: &checksumSHA256,
		ContentLength:  aws.Int64(int64(len(body))),
		ContentType:    &contentType,
		IfNoneMatch:    aws.String("*"),
		Metadata:       metadata,
	}
	if contentEncoding != "" {
		input.ContentEncoding = &contentEncoding
	}

	if _, err := d.client.PutObject(ctx, input); err != nil {
		reconcileErr := d.reconcileObject(ctx, key, position, batchHash, codecVersion)
		if reconcileErr == nil {
			return nil
		}
		if errors.Is(reconcileErr, ErrObjectConflict) {
			return reconcileErr
		}
		return fmt.Errorf("upload to s3: %w (reconcile stable object: %w)", err, reconcileErr)
	}

	return nil
}

func (d *Destination) reconcileObject(ctx context.Context, key, position, batchHash, codecVersion string) error {
	head, err := d.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       &d.bucket,
		Key:          &key,
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return fmt.Errorf("head s3://%s/%s: %w", d.bucket, key, err)
	}
	actualHash := objectMetadata(head.Metadata, metadataBatchHash)
	if actualHash != batchHash {
		return d.objectConflict(key, batchHash, actualHash, "logical batch hash differs")
	}
	if actualPosition := objectMetadata(head.Metadata, metadataPosition); actualPosition != position {
		return d.objectConflict(key, batchHash, actualHash, "checkpoint position differs")
	}
	if actualCodec := objectMetadata(head.Metadata, metadataCodecVersion); actualCodec != codecVersion {
		return d.objectConflict(key, batchHash, actualHash, "codec version differs")
	}
	storedSHA256 := objectMetadata(head.Metadata, metadataObjectSHA256)
	if storedSHA256 == "" {
		return d.objectConflict(key, batchHash, actualHash, "stored object checksum is missing")
	}
	if head.ChecksumSHA256 == nil || *head.ChecksumSHA256 == "" {
		return d.objectConflict(key, batchHash, actualHash, "S3 did not return the stored object checksum")
	}
	checksum, decodeErr := base64.StdEncoding.DecodeString(*head.ChecksumSHA256)
	if decodeErr != nil || hex.EncodeToString(checksum) != storedSHA256 {
		return d.objectConflict(key, batchHash, actualHash, "stored object checksum metadata differs from S3 checksum")
	}
	return nil
}

func (d *Destination) objectConflict(key, expectedHash, actualHash, reason string) error {
	return &ObjectConflictError{
		Bucket:       d.bucket,
		Key:          key,
		ExpectedHash: expectedHash,
		ActualHash:   actualHash,
		Reason:       reason,
	}
}

func objectMetadata(metadata map[string]string, name string) string {
	for key, value := range metadata {
		if strings.EqualFold(key, name) {
			return value
		}
	}
	return ""
}

func (d *Destination) ApplyDDL(_ context.Context, _ connector.Schema, _ connector.Record) error {
	return nil
}

func (d *Destination) TypeMappings() map[string]string { return nil }

func (d *Destination) Close(_ context.Context) error {
	if d.registry != nil {
		_ = d.registry.Close()
	}
	return nil
}

type schemaMeta struct {
	Subject string
	ID      string
	Version int
}

func (d *Destination) ensureSchema(ctx context.Context, schema connector.Schema) (*schemaMeta, error) {
	if d.registry == nil || d.codec == nil {
		return nil, schemaregistry.ErrRegistryDisabled
	}
	subject := d.registrySubjectFor(schema)
	switch d.codec.Name() {
	case connector.WireFormatAvro:
		return d.registerAvroSchema(ctx, subject, schema)
	case connector.WireFormatProto:
		return d.registerProtoSchema(ctx, subject)
	default:
		return nil, schemaregistry.ErrRegistryDisabled
	}
}

func (d *Destination) registerAvroSchema(ctx context.Context, subject string, schema connector.Schema) (*schemaMeta, error) {
	req := schemaregistry.RegisterRequest{
		Subject:    subject,
		Schema:     wire.AvroSchema(schema),
		SchemaType: schemaregistry.SchemaTypeAvro,
	}
	result, err := d.registry.Register(ctx, req)
	if err != nil {
		return nil, err
	}
	return &schemaMeta{Subject: subject, ID: result.ID, Version: result.Version}, nil
}

func (d *Destination) registerProtoSchema(ctx context.Context, subject string) (*schemaMeta, error) {
	def, err := wire.ProtoBatchSchema()
	if err != nil {
		return nil, err
	}
	refNames := make([]string, 0, len(def.Dependencies))
	for name := range def.Dependencies {
		refNames = append(refNames, name)
	}
	sort.Strings(refNames)

	refs := make([]schemaregistry.Reference, 0, len(refNames))
	for _, name := range refNames {
		depSubject := d.protoReferenceSubject(subject, name)
		refResult, err := d.registry.Register(ctx, schemaregistry.RegisterRequest{
			Subject:    depSubject,
			Schema:     def.Dependencies[name],
			SchemaType: schemaregistry.SchemaTypeProtobuf,
		})
		if err != nil {
			return nil, err
		}
		refs = append(refs, schemaregistry.Reference{
			Name:    name,
			Subject: depSubject,
			Version: refResult.Version,
		})
	}

	result, err := d.registry.Register(ctx, schemaregistry.RegisterRequest{
		Subject:    subject,
		Schema:     def.Schema,
		SchemaType: schemaregistry.SchemaTypeProtobuf,
		References: refs,
	})
	if err != nil {
		return nil, err
	}
	return &schemaMeta{Subject: subject, ID: result.ID, Version: result.Version}, nil
}

func (d *Destination) registrySubjectFor(schema connector.Schema) string {
	if d.registrySubject != "" {
		return d.registrySubject
	}
	if schema.Namespace != "" {
		return fmt.Sprintf("%s.%s", schema.Namespace, schema.Name)
	}
	return schema.Name
}

func (d *Destination) protoReferenceSubject(subject, ref string) string {
	if d.protoTypesSubject != "" {
		return d.protoTypesSubject
	}
	name := strings.TrimSuffix(filepath.Base(ref), ".proto")
	if name == "" {
		name = "types"
	}
	return fmt.Sprintf("%s.%s", subject, name)
}

func (d *Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		Support:               connector.SupportExperimental,
		TableWrites:           connector.TableWriteSemantics{Append: true},
		Delivery:              connector.DeliverySemantics{},
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

func (d *Destination) batchIdentityKey(batch connector.Batch) (string, error) {
	position, err := connector.CheckpointPositionID(batch.Checkpoint)
	if err != nil {
		return "", fmt.Errorf("identify s3 batch identity position: %w", err)
	}
	parts := d.objectKeyPrefix(batch, batch.Schema.Name)
	parts = append(parts, "_wallaby_batches", "position="+stablePathValue(position)+".identity")
	return path.Join(parts...), nil
}

func (d *Destination) objectKey(batch connector.Batch, record connector.Record, partitionPath string) (string, error) {
	position, err := connector.CheckpointPositionID(batch.Checkpoint)
	if err != nil {
		return "", fmt.Errorf("identify s3 object position: %w", err)
	}
	table := record.Table
	if table == "" {
		table = batch.Schema.Name
	}
	ext := extensionForFormat(d.codec.Name())
	if d.compression == "gzip" {
		ext += ".gz"
	}
	name := fmt.Sprintf("position=%s.%s", stablePathValue(position), ext)

	parts := d.objectKeyPrefix(batch, table)
	if partitionPath != "" {
		parts = append(parts, partitionPath)
	}
	parts = append(parts, name)
	return path.Join(parts...), nil
}

func (d *Destination) objectKeyPrefix(batch connector.Batch, table string) []string {
	parts := make([]string, 0, 12)
	if d.prefix != "" {
		parts = append(parts, d.prefix)
	}
	if batch.Schema.Namespace != "" {
		parts = append(parts, stablePathValue(batch.Schema.Namespace))
	}
	if table != "" {
		parts = append(parts, stablePathValue(table))
	}
	return append(parts,
		"schema_version="+strconv.FormatInt(batch.Schema.Version, 10),
		"flow="+stablePathValue(d.spec.Options["flow_id"]),
		"destination="+stablePathValue(d.spec.Name),
		"codec="+stablePathValue(d.codecVersion()),
	)
}

func (d *Destination) codecVersion() string {
	compression := d.compression
	if compression == "" {
		compression = "none"
	}
	partitionParts := make([]string, 0, len(d.partitions))
	for _, partition := range d.partitions {
		partitionParts = append(partitionParts, partition.name+":"+partition.bucket)
	}
	return fmt.Sprintf("%s:%s:%s:%s", directObjectVersion, d.codec.Name(), compression, strings.Join(partitionParts, ","))
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

func (d *Destination) prepareBody(payload []byte) ([]byte, string, string, error) {
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
		return buf.Bytes(), contentType, "gzip", nil
	}

	return payload, contentType, "", nil
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

func (d *Destination) partitionPath(record connector.Record) (string, error) {
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
		parts = append(parts, fmt.Sprintf("%s=%s", stablePathValue(spec.name), stablePathValue(formatted)))
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
		return typedPartitionValue("null", nil), nil
	}
	if bucket != "" {
		t, ok := parsePartitionTime(value)
		if !ok {
			return "", fmt.Errorf("expected time value for bucket %s", bucket)
		}
		return typedPartitionValue("time/"+bucket, []byte(formatTimeBucket(t, bucket))), nil
	}

	typeOf := reflect.TypeOf(value)
	typeID := typeOf.PkgPath() + ":" + typeOf.String()
	switch v := value.(type) {
	case time.Time:
		return typedPartitionValue(typeID, []byte(v.UTC().Format(time.RFC3339Nano))), nil
	case json.RawMessage:
		return typedPartitionValue(typeID, []byte(v)), nil
	case []byte:
		return typedPartitionValue(typeID, v), nil
	case string:
		return typedPartitionValue(typeID, []byte(v)), nil
	case bool:
		return typedPartitionValue(typeID, []byte(strconv.FormatBool(v))), nil
	case float32:
		return typedPartitionValue(typeID, []byte(strconv.FormatUint(uint64(math.Float32bits(v)), 16))), nil
	case float64:
		return typedPartitionValue(typeID, []byte(strconv.FormatUint(math.Float64bits(v), 16))), nil
	}

	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return typedPartitionValue(typeID, []byte(strconv.FormatInt(reflected.Int(), 10))), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return typedPartitionValue(typeID, []byte(strconv.FormatUint(reflected.Uint(), 10))), nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode partition value %s: %w", typeID, err)
	}
	return typedPartitionValue(typeID, encoded), nil
}

func typedPartitionValue(typeID string, value []byte) string {
	return strconv.Itoa(len(typeID)) + ":" + typeID + ":" + base64.RawURLEncoding.EncodeToString(value)
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

func stablePathValue(value string) string {
	return "v1-" + base64.RawURLEncoding.EncodeToString([]byte(value))
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
