package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/josephjohncox/wallaby/internal/options"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	"github.com/josephjohncox/wallaby/pkg/wire"
	"golang.org/x/net/http/httpguts"
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

var payloadModeAliases = map[string]string{
	"":            payloadModeWire,
	"wire":        payloadModeWire,
	"record":      payloadModeRecordJSON,
	"record_json": payloadModeRecordJSON,
	"raw":         payloadModeRecordJSON,
	"wal":         payloadModeWAL,
}

type destinationConfig struct {
	url               string
	method            string
	format            string
	payloadMode       string
	timeout           time.Duration
	headers           map[string]string
	maxRetries        int
	backoffBase       time.Duration
	backoffMax        time.Duration
	backoffFactor     float64
	idempotencyHeader string
	dedupeWindow      time.Duration
	transactionHeader string
	registrySubject   string
	protoTypesSubject string
	registryConfig    schemaregistry.Config
}

type dedupeEntry struct {
	done        chan struct{}
	completed   bool
	completedAt time.Time
}

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
	dedupe            map[string]*dedupeEntry
	dedupeMu          sync.Mutex
	now               func() time.Time
	registry          schemaregistry.Registry
	registrySubject   string
	protoTypesSubject string
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	cfg, err := parseDestinationConfig(spec)
	if err != nil {
		return err
	}

	var codec wire.Codec
	if cfg.payloadMode == payloadModeWire {
		codec, err = wire.NewCodec(cfg.format)
		if err != nil {
			return err
		}
	}
	var registry schemaregistry.Registry
	if cfg.payloadMode == payloadModeWire && codec != nil {
		switch codec.Name() {
		case connector.WireFormatAvro, connector.WireFormatProto:
			registry, err = schemaregistry.NewRegistry(ctx, cfg.registryConfig)
			if err != nil && !errors.Is(err, schemaregistry.ErrRegistryDisabled) {
				if registry != nil {
					return errors.Join(err, registry.Close())
				}
				return err
			}
			if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
				if registry != nil {
					if cleanupErr := registry.Close(); cleanupErr != nil {
						return cleanupErr
					}
				}
				registry = nil
			}
		}
	}

	d.spec = spec
	d.url = cfg.url
	d.method = cfg.method
	d.codec = codec
	d.payloadMode = cfg.payloadMode
	d.registrySubject = cfg.registrySubject
	d.protoTypesSubject = cfg.protoTypesSubject
	d.registry = registry
	d.client = &http.Client{Timeout: cfg.timeout}
	d.headers = cfg.headers
	d.maxRetries = cfg.maxRetries
	d.backoffBase = cfg.backoffBase
	d.backoffMax = cfg.backoffMax
	d.backoffFactor = cfg.backoffFactor
	d.idempotencyHeader = cfg.idempotencyHeader
	d.transactionHeader = cfg.transactionHeader
	d.dedupeWindow = cfg.dedupeWindow
	d.dedupe = nil
	d.now = time.Now
	if d.dedupeWindow > 0 {
		d.dedupe = make(map[string]*dedupeEntry)
	}
	return nil
}

func parseDestinationConfig(spec connector.Spec) (destinationConfig, error) {
	decoder := options.NewDecoder("http options", spec.Options)
	registryConfig, registryErr := schemaregistry.ConfigFromOptions(spec.Options)
	cfg := destinationConfig{
		url:               decoder.Raw(optURL, ""),
		method:            strings.ToUpper(decoder.Raw(optMethod, http.MethodPost)),
		format:            decoder.Raw(optFormat, string(connector.WireFormatJSON)),
		payloadMode:       decoder.AliasedEnum(optPayloadMode, payloadModeWire, payloadModeAliases),
		timeout:           decoder.Duration(optTimeout, 10*time.Second),
		headers:           decoder.HeaderList(optHeaders),
		maxRetries:        decoder.Int(optMaxRetries, 3),
		backoffBase:       decoder.Duration(optBackoffBase, 200*time.Millisecond),
		backoffMax:        decoder.Duration(optBackoffMax, 5*time.Second),
		backoffFactor:     decoder.Float64(optBackoffFactor, 2.0),
		idempotencyHeader: decoder.Raw(optIdempotencyHeader, "Idempotency-Key"),
		dedupeWindow:      decoder.Duration(optDedupeWindow, 0),
		transactionHeader: decoder.String(optTransactionHeader, "X-Wallaby-Transaction-Id"),
		registrySubject:   decoder.String(schemaregistry.OptRegistrySubject, ""),
		protoTypesSubject: decoder.String(schemaregistry.OptRegistryProtoTypes, ""),
		registryConfig:    registryConfig,
	}
	if err := errors.Join(decoder.Err(), registryErr); err != nil {
		return destinationConfig{}, err
	}
	if cfg.maxRetries < 0 {
		return destinationConfig{}, fmt.Errorf("http options.%s: must be non-negative", optMaxRetries)
	}
	if cfg.maxRetries == int(^uint(0)>>1) {
		return destinationConfig{}, fmt.Errorf("http options.%s: exceeds the supported retry count", optMaxRetries)
	}
	if cfg.backoffBase <= 0 {
		return destinationConfig{}, fmt.Errorf("http options.%s: must be positive", optBackoffBase)
	}
	// A zero maximum intentionally disables the configured cap; the runtime
	// still saturates at the largest representable duration.
	if cfg.backoffMax < 0 {
		return destinationConfig{}, fmt.Errorf("http options.%s: must be non-negative", optBackoffMax)
	}
	if cfg.backoffFactor <= 0 {
		return destinationConfig{}, fmt.Errorf("http options.%s: must be positive and finite", optBackoffFactor)
	}
	parsedURL, err := url.Parse(cfg.url)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return destinationConfig{}, fmt.Errorf("http options.%s: must be an absolute http or https URL", optURL)
	}
	if cfg.method == "" {
		cfg.method = http.MethodPost
	}
	if !httpguts.ValidHeaderFieldName(cfg.method) {
		return destinationConfig{}, fmt.Errorf("http options.%s: invalid HTTP method %q", optMethod, cfg.method)
	}
	if cfg.format == "" {
		cfg.format = string(connector.WireFormatJSON)
	}
	if cfg.idempotencyHeader == "" {
		cfg.idempotencyHeader = "Idempotency-Key"
	}
	if !httpguts.ValidHeaderFieldName(cfg.idempotencyHeader) {
		return destinationConfig{}, fmt.Errorf("http options.%s: invalid header name %q", optIdempotencyHeader, cfg.idempotencyHeader)
	}
	if cfg.transactionHeader == "" {
		cfg.transactionHeader = "X-Wallaby-Transaction-Id"
	}
	if !httpguts.ValidHeaderFieldName(cfg.transactionHeader) {
		return destinationConfig{}, fmt.Errorf("http options.%s: invalid header name %q", optTransactionHeader, cfg.transactionHeader)
	}
	return cfg, nil
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
		if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
			meta = nil
		} else {
			return err
		}
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
		reservation, skip, err := d.reserveDelivery(ctx, idempotencyKey)
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		txnID := d.transactionID(record, batch.Checkpoint.LSN)
		err = d.sendWithRetry(ctx, payload, contentType, idempotencyKey, txnID, meta)
		d.finishDelivery(idempotencyKey, reservation, err == nil)
		if err != nil {
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
		Support:               connector.SupportExperimental,
		TableWrites:           connector.TableWriteSemantics{Append: true},
		Delivery:              connector.DeliverySemantics{},
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
	attempts := d.maxRetries
	if attempts < 0 {
		attempts = 0
	}
	if attempts < int(^uint(0)>>1) {
		attempts++
	}

	for attempt := 1; ; attempt++ {
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
	name := strings.TrimSuffix(path.Base(ref), ".proto")
	if name == "" {
		name = "types"
	}
	return fmt.Sprintf("%s.%s", subject, name)
}

func (d *Destination) backoffDuration(attempt int) time.Duration {
	base := d.backoffBase
	if base <= 0 {
		base = 200 * time.Millisecond
	}
	factor := d.backoffFactor
	if math.IsNaN(factor) || math.IsInf(factor, 0) || factor <= 0 {
		factor = 2.0
	}
	limit := d.backoffMax
	if limit <= 0 {
		limit = time.Duration(1<<63 - 1)
	}
	if base > limit {
		base = limit
	}

	exponent := 0
	if attempt > 1 {
		exponent = attempt - 1
	}
	delay := base
	if exponent > 0 {
		logCandidate := math.Log(float64(base)) + float64(exponent)*math.Log(factor)
		if math.IsInf(logCandidate, 1) || logCandidate >= math.Log(float64(limit)) {
			delay = limit
		} else {
			candidate := float64(base) * math.Pow(factor, float64(exponent))
			switch {
			case math.IsNaN(candidate), math.IsInf(candidate, 0), candidate >= float64(limit):
				delay = limit
			case candidate < 1:
				delay = 1
			default:
				delay = time.Duration(candidate)
			}
		}
	}
	if delay >= limit {
		return limit
	}
	jitterLimit := delay / 4
	if remaining := limit - delay; jitterLimit > remaining {
		jitterLimit = remaining
	}
	if jitterLimit <= 0 {
		return delay
	}
	// #nosec G404 -- jitter does not require cryptographic randomness.
	return delay + time.Duration(rand.Int63n(int64(jitterLimit)+1))
}

func (d *Destination) reserveDelivery(ctx context.Context, idempotencyKey string) (*dedupeEntry, bool, error) {
	if d.dedupeWindow <= 0 || idempotencyKey == "" {
		return nil, false, nil
	}

	for {
		d.dedupeMu.Lock()
		now := d.currentTime()
		for key, entry := range d.dedupe {
			if entry.completed && now.Sub(entry.completedAt) > d.dedupeWindow {
				delete(d.dedupe, key)
			}
		}
		entry, exists := d.dedupe[idempotencyKey]
		if !exists {
			entry = &dedupeEntry{done: make(chan struct{})}
			d.dedupe[idempotencyKey] = entry
			d.dedupeMu.Unlock()
			return entry, false, nil
		}
		if entry.completed {
			d.dedupeMu.Unlock()
			return nil, true, nil
		}
		done := entry.done
		d.dedupeMu.Unlock()

		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-done:
		}
	}
}

func (d *Destination) finishDelivery(idempotencyKey string, reservation *dedupeEntry, delivered bool) {
	if reservation == nil {
		return
	}

	d.dedupeMu.Lock()
	defer d.dedupeMu.Unlock()
	if d.dedupe[idempotencyKey] != reservation {
		return
	}
	if delivered {
		reservation.completed = true
		reservation.completedAt = d.currentTime()
	} else {
		delete(d.dedupe, idempotencyKey)
	}
	close(reservation.done)
}

func (d *Destination) currentTime() time.Time {
	if d.now != nil {
		return d.now()
	}
	return time.Now()
}

func (d *Destination) buildIdempotencyKey(record connector.Record, lsn string, payload []byte) string {
	if d.idempotencyHeader == "" {
		return ""
	}

	position := strings.TrimSpace(record.SourcePosition)
	if position == "" {
		position = lsn
	}
	hash := sha256.New()
	writePart := func(value []byte) {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(value)
	}
	writePart([]byte(record.Table))
	writePart([]byte(record.Operation))
	writePart([]byte(position))
	writePart(record.Key)
	writePart(payload)
	return hex.EncodeToString(hash.Sum(nil))
}

func (d *Destination) transactionID(record connector.Record, lsn string) string {
	if lsn != "" {
		return lsn
	}
	keyPart := string(record.Key)
	if keyPart == "" {
		keyPart = record.Table
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
