package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/josephjohncox/ductstream/pkg/connector"
	"github.com/josephjohncox/ductstream/pkg/wire"
)

const (
	optBucket      = "bucket"
	optPrefix      = "prefix"
	optRegion      = "region"
	optFormat      = "format"
	optCompression = "compression"
)

// Destination writes batches to S3.
type Destination struct {
	spec        connector.Spec
	bucket      string
	prefix      string
	format      string
	compression string
	codec       wire.Codec
	uploader    *manager.Uploader
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

	codec, err := wire.NewCodec(d.format)
	if err != nil {
		return err
	}
	d.codec = codec

	loadOpts := []func(*config.LoadOptions) error{}
	if region := spec.Options[optRegion]; region != "" {
		loadOpts = append(loadOpts, config.WithRegion(region))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg)
	d.uploader = manager.NewUploader(client)

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.uploader == nil {
		return errors.New("s3 destination not initialized")
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

	key := d.objectKey(batch.Schema)
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

func (d *Destination) Close(_ context.Context) error {
	return nil
}

func (d *Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     false,
		SupportsBulkLoad:      true,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatArrow,
			connector.WireFormatJSON,
		},
	}
}

func (d *Destination) objectKey(schema connector.Schema) string {
	stamp := time.Now().UTC().Format("20060102T150405Z")
	name := fmt.Sprintf("%s_%s_%d_%s.%s", schema.Name, stamp, schema.Version, d.fileSuffix(), extensionForFormat(d.codec.Name()))
	if d.prefix == "" {
		return name
	}
	return path.Join(d.prefix, schema.Namespace, name)
}

func extensionForFormat(format connector.WireFormat) string {
	switch format {
	case connector.WireFormatArrow:
		return "arrow"
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
