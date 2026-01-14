package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
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
	optMessageMode = "message_mode"
	optKeyMode     = "key_mode"
	optTxnID       = "transactional_id"
	optTxnTimeout  = "transaction_timeout"
	optTxnHeader   = "transaction_header"
)

// Destination writes batches to Kafka.
type Destination struct {
	spec              connector.Spec
	client            *kgo.Client
	topic             string
	codec             wire.Codec
	maxMessageSize    int
	maxBatchSize      int
	maxRecordSize     int
	oversizePolicy    string
	messageMode       string
	keyMode           string
	transactional     bool
	txnHeader         string
	registry          schemaregistry.Registry
	registryMode      string
	registrySubject   string
	protoTypesSubject string
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
	d.messageMode = strings.ToLower(strings.TrimSpace(spec.Options[optMessageMode]))
	if d.messageMode == "" {
		d.messageMode = "batch"
	}
	d.keyMode = strings.ToLower(strings.TrimSpace(spec.Options[optKeyMode]))
	if d.keyMode == "" {
		d.keyMode = "hash"
	}
	d.txnHeader = strings.TrimSpace(spec.Options[optTxnHeader])
	if d.txnHeader == "" {
		d.txnHeader = "wallaby-transaction-id"
	}
	d.registryMode = strings.ToLower(strings.TrimSpace(spec.Options[schemaregistry.OptRegistrySubjectMode]))
	if d.registryMode == "" {
		d.registryMode = "topic_table"
	}
	d.registrySubject = strings.TrimSpace(spec.Options[schemaregistry.OptRegistrySubject])
	d.protoTypesSubject = strings.TrimSpace(spec.Options[schemaregistry.OptRegistryProtoTypes])

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
	if txnID := strings.TrimSpace(spec.Options[optTxnID]); txnID != "" {
		opts = append(opts, kgo.TransactionalID(txnID))
		d.transactional = true
		if raw := strings.TrimSpace(spec.Options[optTxnTimeout]); raw != "" {
			timeout, err := time.ParseDuration(raw)
			if err != nil {
				return fmt.Errorf("parse transaction_timeout: %w", err)
			}
			opts = append(opts, kgo.TransactionTimeout(timeout))
		}
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	d.client = client

	if d.codec.Name() == connector.WireFormatAvro || d.codec.Name() == connector.WireFormatProto {
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

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.client == nil {
		return errors.New("kafka destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}
	if d.transactional {
		if err := d.client.BeginTransaction(); err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}
	}
	meta, err := d.ensureSchema(ctx, batch)
	if err != nil {
		if d.transactional {
			_ = d.client.EndTransaction(ctx, kgo.TryAbort)
		}
		if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
			meta = nil
		} else {
			return err
		}
	}
	if d.messageMode == "record" {
		err = d.writeRecords(ctx, batch, meta)
	} else {
		limit := d.batchLimit()
		if limit <= 0 {
			err = d.writeEncoded(ctx, batch, meta)
		} else {
			err = d.writeWithLimit(ctx, batch, limit, meta)
		}
	}
	if d.transactional {
		commit := kgo.TryCommit
		if err != nil {
			commit = kgo.TryAbort
		}
		if endErr := d.client.EndTransaction(ctx, commit); endErr != nil && err == nil {
			err = endErr
		}
	}
	return err
}

func (d *Destination) ApplyDDL(_ context.Context, _ connector.Schema, _ connector.Record) error {
	return nil
}

func (d *Destination) TypeMappings() map[string]string { return nil }

func (d *Destination) Close(_ context.Context) error {
	if d.client != nil {
		d.client.Close()
	}
	if d.registry != nil {
		_ = d.registry.Close()
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

func (d *Destination) writeEncoded(ctx context.Context, batch connector.Batch, meta *schemaMeta) error {
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
	return d.producePayload(ctx, batch, payload, nil, meta)
}

func (d *Destination) writeWithLimit(ctx context.Context, batch connector.Batch, limit int, meta *schemaMeta) error {
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
		return d.producePayload(ctx, batch, payload, nil, meta)
	}
	if len(batch.Records) == 1 {
		return d.handleOversize(batch, len(payload), limit, "message")
	}
	mid := len(batch.Records) / 2
	left := batch
	left.Records = batch.Records[:mid]
	right := batch
	right.Records = batch.Records[mid:]
	if err := d.writeWithLimit(ctx, left, limit, meta); err != nil {
		return err
	}
	return d.writeWithLimit(ctx, right, limit, meta)
}

func (d *Destination) writeRecords(ctx context.Context, batch connector.Batch, meta *schemaMeta) error {
	for _, record := range batch.Records {
		recordBatch := connector.Batch{
			Schema:     batch.Schema,
			Records:    []connector.Record{record},
			Checkpoint: batch.Checkpoint,
			WireFormat: batch.WireFormat,
		}
		payload, err := d.codec.Encode(recordBatch)
		if err != nil {
			return err
		}
		if len(payload) == 0 {
			continue
		}
		if d.maxRecordSize > 0 && len(payload) > d.maxRecordSize {
			if err := d.handleOversize(recordBatch, len(payload), d.maxRecordSize, "record"); err != nil {
				return err
			}
			continue
		}
		if limit := d.batchLimit(); limit > 0 && len(payload) > limit {
			if err := d.handleOversize(recordBatch, len(payload), limit, "message"); err != nil {
				return err
			}
			continue
		}
		headers := d.recordHeaders(batch, record)
		key := d.recordKey(batch, record, payload)
		if err := d.producePayload(ctx, recordBatch, payload, &kgo.Record{Key: key, Headers: headers}, meta); err != nil {
			return err
		}
	}
	return nil
}

func (d *Destination) producePayload(ctx context.Context, batch connector.Batch, payload []byte, override *kgo.Record, meta *schemaMeta) error {
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
	if meta != nil {
		record.Headers = append(record.Headers,
			kgo.RecordHeader{Key: "wallaby-registry-subject", Value: []byte(meta.Subject)},
			kgo.RecordHeader{Key: "wallaby-registry-id", Value: []byte(meta.ID)},
		)
		if meta.Version > 0 {
			record.Headers = append(record.Headers, kgo.RecordHeader{Key: "wallaby-registry-version", Value: []byte(fmt.Sprintf("%d", meta.Version))})
		}
	}
	if override != nil {
		record.Key = override.Key
		if len(override.Headers) > 0 {
			record.Headers = append(record.Headers, override.Headers...)
		}
	}
	results := d.client.ProduceSync(ctx, record)
	return results.FirstErr()
}

func (d *Destination) recordHeaders(batch connector.Batch, record connector.Record) []kgo.RecordHeader {
	headers := []kgo.RecordHeader{
		{Key: "wallaby-table", Value: []byte(record.Table)},
		{Key: "wallaby-op", Value: []byte(record.Operation)},
		{Key: "wallaby-lsn", Value: []byte(batch.Checkpoint.LSN)},
	}
	if d.txnHeader != "" {
		headers = append(headers, kgo.RecordHeader{Key: d.txnHeader, Value: []byte(d.transactionID(batch, record))})
	}
	headers = append(headers, kgo.RecordHeader{Key: "wallaby-key-hash", Value: []byte(d.keyHash(batch, record))})
	return headers
}

func (d *Destination) recordKey(batch connector.Batch, record connector.Record, payload []byte) []byte {
	switch d.keyMode {
	case "raw":
		if len(record.Key) > 0 {
			return record.Key
		}
		return payload
	default:
		return []byte(d.keyHash(batch, record))
	}
}

type schemaMeta struct {
	Subject string
	ID      string
	Version int
}

func (d *Destination) ensureSchema(ctx context.Context, batch connector.Batch) (*schemaMeta, error) {
	if d.registry == nil {
		return nil, schemaregistry.ErrRegistryDisabled
	}
	subject := d.registrySubjectFor(batch.Schema)
	switch d.codec.Name() {
	case connector.WireFormatAvro:
		return d.registerAvroSchema(ctx, subject, batch.Schema)
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
	switch d.registryMode {
	case "topic":
		return fmt.Sprintf("%s-value", d.topic)
	case "table":
		if schema.Namespace != "" {
			return fmt.Sprintf("%s.%s", schema.Namespace, schema.Name)
		}
		return schema.Name
	default:
		if schema.Namespace != "" {
			return fmt.Sprintf("%s.%s.%s", d.topic, schema.Namespace, schema.Name)
		}
		return fmt.Sprintf("%s.%s", d.topic, schema.Name)
	}
}

func (d *Destination) protoReferenceSubject(subject, ref string) string {
	if d.protoTypesSubject != "" {
		return d.protoTypesSubject
	}
	name := strings.TrimSuffix(path.Base(ref), ".proto")
	if name == "" {
		name = "types"
	}
	return fmt.Sprintf("%s.%s", subject, name)
}

func (d *Destination) keyHash(batch connector.Batch, record connector.Record) string {
	base := string(record.Key)
	if base == "" {
		base = fmt.Sprintf("%s|%s|%s", record.Table, batch.Checkpoint.LSN, record.Operation)
	}
	return hashString(base)
}

func (d *Destination) transactionID(batch connector.Batch, record connector.Record) string {
	if batch.Checkpoint.LSN != "" {
		return batch.Checkpoint.LSN
	}
	base := fmt.Sprintf("%s|%s", record.Table, d.keyHash(batch, record))
	return hashString(base)
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
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
