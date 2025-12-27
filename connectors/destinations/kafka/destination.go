package kafka

import (
	"context"
	"errors"
	"fmt"
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
)

// Destination writes batches to Kafka.
type Destination struct {
	spec   connector.Spec
	client *kgo.Client
	topic  string
	codec  wire.Codec
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

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(parseAcks(spec.Options[optAcks])),
	}

	if compression := strings.ToLower(spec.Options[optCompression]); compression != "" {
		opts = append(opts, kgo.ProducerBatchCompression(parseCompression(compression)))
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
	payload, err := d.codec.Encode(batch)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}

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
