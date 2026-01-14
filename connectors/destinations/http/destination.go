package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	"github.com/josephjohncox/wallaby/pkg/wire"
)

const (
	optURL               = "url"
	optMethod            = "method"
	optFormat            = "format"
	optPayloadMode       = "payload_mode"
	optTimeout           = "timeout"
	optHeaders           = "headers"
	optMaxRetries        = "max_retries"
	optBackoffBase       = "backoff_base"
	optBackoffMax        = "backoff_max"
	optBackoffFactor     = "backoff_factor"
	optIdempotencyHeader = "idempotency_header"
	optDedupeWindow      = "dedupe_window"
	optTransactionHeader = "transaction_header"
)

const (
	payloadModeWire       = "wire"
	payloadModeRecordJSON = "record_json"
	payloadModeWAL        = "wal"
)

// Destination delivers records to an HTTP endpoint.
type Destination struct {
	spec              connector.Spec
	url               string
	method            string
	codec             wire.Codec
	payloadMode       string
	headers           map[string]string
	client            *http.Client
	maxRetries        int
	backoffBase       time.Duration
	backoffMax        time.Duration
	backoffFactor     float64
	idempotencyHeader string
	dedupeWindow      time.Duration
	transactionHeader string
	dedupe            map[string]time.Time
	dedupeMu          sync.Mutex
	registry          schemaregistry.Registry
	registrySubject   string
	protoTypesSubject string
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	d.spec = spec
	d.url = spec.Options[optURL]
	if d.url == "" {
		return errors.New("http url is required")
	}

	d.method = strings.ToUpper(spec.Options[optMethod])
	if d.method == "" {
		d.method = http.MethodPost
	}

	d.payloadMode = normalizePayloadMode(spec.Options[optPayloadMode])

	format := spec.Options[optFormat]
	if format == "" {
		format = string(connector.WireFormatJSON)
	}
	if d.payloadMode == payloadModeWire {
		codec, err := wire.NewCodec(format)
		if err != nil {
			return err
		}
		d.codec = codec
	}
	d.registrySubject = strings.TrimSpace(spec.Options[schemaregistry.OptRegistrySubject])
	d.protoTypesSubject = strings.TrimSpace(spec.Options[schemaregistry.OptRegistryProtoTypes])
	if d.payloadMode == payloadModeWire && d.codec != nil {
		switch d.codec.Name() {
		case connector.WireFormatAvro, connector.WireFormatProto:
			registryCfg := schemaregistry.ConfigFromOptions(spec.Options)
			registry, err := schemaregistry.NewRegistry(ctx, registryCfg)
			if err != nil {
				return err
			}
			d.registry = registry
		}
	}

	if spec.Options[optTimeout] == "" {
		d.client = &http.Client{Timeout: 10 * time.Second}
	} else {
		timeout, err := time.ParseDuration(spec.Options[optTimeout])
		if err != nil {
			return fmt.Errorf("parse timeout: %w", err)
		}
		d.client = &http.Client{Timeout: timeout}
	}

	d.headers = parseHeaders(spec.Options[optHeaders])
	d.maxRetries = parseInt(spec.Options[optMaxRetries], 3)
	d.backoffBase = parseDuration(spec.Options[optBackoffBase], 200*time.Millisecond)
	d.backoffMax = parseDuration(spec.Options[optBackoffMax], 5*time.Second)
	d.backoffFactor = parseFloat(spec.Options[optBackoffFactor], 2.0)
	d.idempotencyHeader = spec.Options[optIdempotencyHeader]
	if d.idempotencyHeader == "" {
		d.idempotencyHeader = "Idempotency-Key"
	}
	d.transactionHeader = strings.TrimSpace(spec.Options[optTransactionHeader])
	if d.transactionHeader == "" {
		d.transactionHeader = "X-Wallaby-Transaction-Id"
	}
	d.dedupeWindow = parseDuration(spec.Options[optDedupeWindow], 0)
	if d.dedupeWindow > 0 {
		d.dedupe = make(map[string]time.Time)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.client == nil {
		return errors.New("http destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}

	meta, err := d.ensureSchema(ctx, batch.Schema)
	if err != nil {
		return err
	}
	for _, record := range batch.Records {
		payloadBatch := connector.Batch{
			Records:    []connector.Record{record},
			Schema:     batch.Schema,
			Checkpoint: batch.Checkpoint,
			WireFormat: batch.WireFormat,
		}

		payload, contentType, err := d.encodePayload(payloadBatch, record)
		if err != nil {
			return err
		}
		if len(payload) == 0 {
			continue
		}

		idempotencyKey := d.buildIdempotencyKey(record, batch.Checkpoint.LSN, payload)
		if d.shouldSkip(idempotencyKey) {
			continue
		}
		txnID := d.transactionID(record, batch.Checkpoint.LSN)
		if err := d.sendWithRetry(ctx, payload, contentType, idempotencyKey, txnID, meta); err != nil {
			return err
		}
	}

	return nil
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

func (d *Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
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

func (d *Destination) encodePayload(batch connector.Batch, record connector.Record) ([]byte, string, error) {
	switch d.payloadMode {
	case payloadModeRecordJSON:
		payload, err := marshalRecordJSON(record)
		if err != nil {
			return nil, "", err
		}
		return payload, "application/json", nil
	case payloadModeWAL:
		if len(record.Payload) == 0 {
			return nil, "", errors.New("wal payload not available on record")
		}
		return record.Payload, "application/octet-stream", nil
	default:
		if d.codec == nil {
			return nil, "", errors.New("wire codec not initialized")
		}
		payload, err := d.codec.Encode(batch)
		if err != nil {
			return nil, "", err
		}
		return payload, d.codec.ContentType(), nil
	}
}

func normalizePayloadMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", payloadModeWire:
		return payloadModeWire
	case "record", "record_json", "raw":
		return payloadModeRecordJSON
	case "wal":
		return payloadModeWAL
	default:
		return payloadModeWire
	}
}

func marshalRecordJSON(record connector.Record) ([]byte, error) {
	type recordJSON struct {
		Table         string         `json:"table"`
		Operation     string         `json:"operation"`
		SchemaVersion int64          `json:"schema_version"`
		Key           []byte         `json:"key"`
		Before        map[string]any `json:"before,omitempty"`
		After         map[string]any `json:"after,omitempty"`
		Unchanged     []string       `json:"unchanged,omitempty"`
		DDL           string         `json:"ddl,omitempty"`
		Timestamp     time.Time      `json:"timestamp"`
	}
	payload := recordJSON{
		Table:         record.Table,
		Operation:     string(record.Operation),
		SchemaVersion: record.SchemaVersion,
		Key:           record.Key,
		Before:        record.Before,
		After:         record.After,
		Unchanged:     record.Unchanged,
		DDL:           record.DDL,
		Timestamp:     record.Timestamp,
	}
	return json.Marshal(payload)
}

func (d *Destination) sendWithRetry(ctx context.Context, payload []byte, contentType, idempotencyKey, txnID string, meta *schemaMeta) error {
	attempts := d.maxRetries + 1
	if attempts < 1 {
		attempts = 1
	}

	for attempt := 1; attempt <= attempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, d.method, d.url, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		for k, v := range d.headers {
			req.Header.Set(k, v)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if d.idempotencyHeader != "" && idempotencyKey != "" {
			req.Header.Set(d.idempotencyHeader, idempotencyKey)
		}
		if d.transactionHeader != "" && txnID != "" {
			req.Header.Set(d.transactionHeader, txnID)
		}
		if meta != nil {
			req.Header.Set("X-Wallaby-Registry-Subject", meta.Subject)
			req.Header.Set("X-Wallaby-Registry-Id", meta.ID)
			if meta.Version > 0 {
				req.Header.Set("X-Wallaby-Registry-Version", fmt.Sprintf("%d", meta.Version))
			}
		}

		resp, err := d.client.Do(req)
		if err == nil && resp != nil {
			_ = resp.Body.Close()
		}

		if err == nil && resp != nil && resp.StatusCode < 300 {
			return nil
		}

		if attempt >= attempts || !retryable(err, resp) {
			if err != nil {
				return err
			}
			if resp != nil {
				return fmt.Errorf("http destination status %d", resp.StatusCode)
			}
			return errors.New("http destination failed")
		}

		sleep := d.backoffDuration(attempt)
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return errors.New("http destination retries exhausted")
}

type schemaMeta struct {
	Subject string
	ID      string
	Version int
}

func (d *Destination) ensureSchema(ctx context.Context, schema connector.Schema) (*schemaMeta, error) {
	if d.registry == nil || d.codec == nil {
		return nil, nil
	}
	subject := d.registrySubjectFor(schema)
	switch d.codec.Name() {
	case connector.WireFormatAvro:
		return d.registerAvroSchema(ctx, subject, schema)
	case connector.WireFormatProto:
		return d.registerProtoSchema(ctx, subject)
	default:
		return nil, nil
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
	name := strings.TrimSuffix(path.Base(ref), ".proto")
	if name == "" {
		name = "types"
	}
	return fmt.Sprintf("%s.%s", subject, name)
}

func (d *Destination) backoffDuration(attempt int) time.Duration {
	base := float64(d.backoffBase)
	if base <= 0 {
		base = float64(200 * time.Millisecond)
	}
	factor := d.backoffFactor
	if factor <= 0 {
		factor = 2.0
	}
	pow := math.Pow(factor, float64(attempt-1))
	delay := time.Duration(base * pow)
	if d.backoffMax > 0 && delay > d.backoffMax {
		delay = d.backoffMax
	}

	// #nosec G404 -- jitter does not require cryptographic randomness.
	jitter := 0.5 + rand.Float64()
	return time.Duration(float64(delay) * jitter)
}

func (d *Destination) shouldSkip(idempotencyKey string) bool {
	if d.dedupeWindow <= 0 || idempotencyKey == "" {
		return false
	}
	d.dedupeMu.Lock()
	defer d.dedupeMu.Unlock()
	now := time.Now()
	for key, ts := range d.dedupe {
		if now.Sub(ts) > d.dedupeWindow {
			delete(d.dedupe, key)
		}
	}
	if ts, ok := d.dedupe[idempotencyKey]; ok {
		if now.Sub(ts) <= d.dedupeWindow {
			return true
		}
	}
	d.dedupe[idempotencyKey] = now
	return false
}

func (d *Destination) buildIdempotencyKey(record connector.Record, lsn string, payload []byte) string {
	if d.idempotencyHeader == "" {
		return ""
	}

	keyPart := string(record.Key)
	if keyPart == "" {
		keyPart = string(payload)
	}
	base := fmt.Sprintf("%s|%s|%s", record.Table, keyPart, lsn)
	sum := sha256.Sum256([]byte(base))
	return hex.EncodeToString(sum[:])
}

func (d *Destination) transactionID(record connector.Record, lsn string) string {
	if lsn != "" {
		return lsn
	}
	keyPart := string(record.Key)
	if keyPart == "" {
		keyPart = string(record.Table)
	}
	base := fmt.Sprintf("%s|%s", record.Table, keyPart)
	sum := sha256.Sum256([]byte(base))
	return hex.EncodeToString(sum[:])
}

func retryable(err error, resp *http.Response) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return true
	}

	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusRequestTimeout:
		return true
	default:
		return resp.StatusCode >= 500
	}
}

func parseHeaders(value string) map[string]string {
	out := map[string]string{}
	if value == "" {
		return out
	}
	pairs := strings.Split(value, ",")
	for _, pair := range pairs {
		item := strings.TrimSpace(pair)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if key != "" {
			out[key] = val
		}
	}
	return out
}

func parseInt(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}

func parseFloat(value string, fallback float64) float64 {
	if value == "" {
		return fallback
	}
	var parsed float64
	if _, err := fmt.Sscanf(value, "%f", &parsed); err != nil {
		return fallback
	}
	return parsed
}

func parseDuration(value string, fallback time.Duration) time.Duration {
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
