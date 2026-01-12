package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	optBrokers     = "brokers"
	optTopic       = "topic"
	optFormat      = "format"
	optCompression = "compression"
	optAcks        = "acks"
	optMaxMessage  = "max_message_bytes"
	optMaxBatch    = "max_batch_bytes"
	optMaxRecord   = "max_record_bytes"
	optOversize    = "oversize_policy"
)

// Destination writes batches to Kafka.
type Destination struct {
	spec           connector.Spec
	client         *kgo.Client
	topic          string
	codec          wire.Codec
	maxMessageSize int
	maxBatchSize   int
	maxRecordSize  int
	oversizePolicy string
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	d.spec = spec
	brokers := splitCSV(spec.Options[optBrokers])
	if len(brokers) == 0 {
		return errors.New("kafka brokers are required")
	}
	if spec.Options[optTopic] == "" {
		return errors.New("kafka topic is required")
	}
	d.topic = spec.Options[optTopic]

	codec, err := wire.NewCodec(spec.Options[optFormat])
	if err != nil {
		return err
	}
	d.codec = codec

	maxMessage, err := parseSizeOption(spec.Options, optMaxMessage, 900000)
	if err != nil {
		return err
	}
	maxBatch, err := parseSizeOption(spec.Options, optMaxBatch, maxMessage)
	if err != nil {
		return err
	}
	maxRecord, err := parseSizeOption(spec.Options, optMaxRecord, maxMessage)
	if err != nil {
		return err
	}
	oversize := strings.ToLower(strings.TrimSpace(spec.Options[optOversize]))
	if oversize == "" {
		oversize = "error"
	}
	if oversize != "error" && oversize != "drop" {
		return fmt.Errorf("unsupported oversize_policy %q (use error or drop)", oversize)
	}
	d.maxMessageSize = maxMessage
	d.maxBatchSize = maxBatch
	d.maxRecordSize = maxRecord
	d.oversizePolicy = oversize

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(parseAcks(spec.Options[optAcks])),
	}

	if compression := strings.ToLower(spec.Options[optCompression]); compression != "" {
		opts = append(opts, kgo.ProducerBatchCompression(parseCompression(compression)))
	}
	if d.maxMessageSize > 0 {
		if d.maxMessageSize > math.MaxInt32 {
			return fmt.Errorf("max_message_size exceeds int32: %d", d.maxMessageSize)
		}
		// #nosec G115 -- size validated above.
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(d.maxMessageSize)))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	d.client = client

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.client == nil {
		return errors.New("kafka destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}
	limit := d.batchLimit()
	if limit <= 0 {
		return d.writeEncoded(ctx, batch)
	}
	return d.writeWithLimit(ctx, batch, limit)
}

func (d *Destination) ApplyDDL(_ context.Context, _ connector.Schema, _ connector.Record) error {
	return nil
}

func (d *Destination) TypeMappings() map[string]string { return nil }

func (d *Destination) Close(_ context.Context) error {
	if d.client != nil {
		d.client.Close()
	}
	return nil
}

func (d *Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsBulkLoad:      false,
		SupportsTypeMapping:   true,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatArrow,
			connector.WireFormatAvro,
			connector.WireFormatProto,
			connector.WireFormatJSON,
		},
	}
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trim := strings.TrimSpace(part)
		if trim != "" {
			out = append(out, trim)
		}
	}
	return out
}

func parseSizeOption(options map[string]string, key string, fallback int) (int, error) {
	if options == nil {
		return fallback, nil
	}
	raw := strings.TrimSpace(options[key])
	if raw == "" {
		return fallback, nil
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	if val < 0 {
		return 0, fmt.Errorf("%s must be >= 0", key)
	}
	return val, nil
}

func (d *Destination) batchLimit() int {
	limit := d.maxBatchSize
	if d.maxMessageSize > 0 && (limit <= 0 || d.maxMessageSize < limit) {
		limit = d.maxMessageSize
	}
	if limit <= 0 && d.maxRecordSize > 0 {
		limit = d.maxRecordSize
	}
	return limit
}

func (d *Destination) writeEncoded(ctx context.Context, batch connector.Batch) error {
	payload, err := d.codec.Encode(batch)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	if len(batch.Records) == 1 && d.maxRecordSize > 0 && len(payload) > d.maxRecordSize {
		return d.handleOversize(batch, len(payload), d.maxRecordSize, "record")
	}
	return d.producePayload(ctx, batch, payload)
}

func (d *Destination) writeWithLimit(ctx context.Context, batch connector.Batch, limit int) error {
	payload, err := d.codec.Encode(batch)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	if len(batch.Records) == 1 && d.maxRecordSize > 0 && len(payload) > d.maxRecordSize {
		return d.handleOversize(batch, len(payload), d.maxRecordSize, "record")
	}
	if len(payload) <= limit {
		return d.producePayload(ctx, batch, payload)
	}
	if len(batch.Records) == 1 {
		return d.handleOversize(batch, len(payload), limit, "message")
	}
	mid := len(batch.Records) / 2
	left := batch
	left.Records = batch.Records[:mid]
	right := batch
	right.Records = batch.Records[mid:]
	if err := d.writeWithLimit(ctx, left, limit); err != nil {
		return err
	}
	return d.writeWithLimit(ctx, right, limit)
}

func (d *Destination) producePayload(ctx context.Context, batch connector.Batch, payload []byte) error {
	record := &kgo.Record{
		Topic: d.topic,
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "wallaby-format", Value: []byte(d.codec.Name())},
			{Key: "wallaby-schema", Value: []byte(batch.Schema.Name)},
			{Key: "wallaby-namespace", Value: []byte(batch.Schema.Namespace)},
			{Key: "wallaby-schema-version", Value: []byte(fmt.Sprintf("%d", batch.Schema.Version))},
		},
	}
	results := d.client.ProduceSync(ctx, record)
	return results.FirstErr()
}

func (d *Destination) handleOversize(batch connector.Batch, size, limit int, limitType string) error {
	msg := fmt.Sprintf("kafka %s payload size %d exceeds limit %d (records=%d)", limitType, size, limit, len(batch.Records))
	switch d.oversizePolicy {
	case "drop":
		log.Printf("kafka oversize_policy=drop: %s", msg)
		return nil
	default:
		return errors.New(msg)
	}
}

func parseCompression(value string) kgo.CompressionCodec {
	switch value {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.NoCompression()
	}
}

func parseAcks(value string) kgo.Acks {
	switch strings.ToLower(value) {
	case "none", "0":
		return kgo.NoAck()
	case "leader", "1":
		return kgo.LeaderAck()
	default:
		return kgo.AllISRAcks()
	}
}
