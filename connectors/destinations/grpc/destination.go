package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/options"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	"github.com/josephjohncox/wallaby/pkg/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	optEndpoint      = "endpoint"
	optInsecure      = "insecure"
	optTLSCAFile     = "tls_ca_file"
	optTLSServerName = "tls_server_name"
	optTimeout       = "timeout"
	optFormat        = "format"
	optPayloadMode   = "payload_mode"
	optHeaders       = "headers"
	optMaxRetries    = "max_retries"
	optBackoffBase   = "backoff_base"
	optBackoffMax    = "backoff_max"
	optBackoffFactor = "backoff_factor"
	optFlowID        = "flow_id"
	optDestination   = "destination"
)

const (
	payloadModeWire       = "wire"
	payloadModeRecordJSON = "record_json"
	payloadModeWAL        = "wal"
)

var payloadModes = map[string]string{
	"":            payloadModeWire,
	"wire":        payloadModeWire,
	"record_json": payloadModeRecordJSON,
	"wal":         payloadModeWAL,
}

type destinationConfig struct {
	endpoint          string
	format            string
	payloadMode       string
	headers           map[string]string
	timeout           time.Duration
	maxRetries        int
	backoffBase       time.Duration
	backoffMax        time.Duration
	backoffFactor     float64
	insecure          bool
	tlsCAFile         string
	tlsServerName     string
	flowID            string
	destination       string
	registrySubject   string
	protoTypesSubject string
	registryConfig    schemaregistry.Config
}

type destinationFactories struct {
	newClient   func(string, ...grpc.DialOption) (*grpc.ClientConn, error)
	newRegistry func(context.Context, schemaregistry.Config) (schemaregistry.Registry, error)
}

// Destination delivers batches to a gRPC ingest endpoint.
type Destination struct {
	resourceMu        sync.Mutex
	spec              connector.RuntimeSpec
	endpoint          string
	codec             wire.Codec
	payloadMode       string
	client            wallabypb.IngestServiceClient
	conn              *grpc.ClientConn
	headers           map[string]string
	timeout           time.Duration
	maxRetries        int
	backoffBase       time.Duration
	backoffMax        time.Duration
	backoffFactor     float64
	flowID            string
	destination       string
	registry          schemaregistry.Registry
	registrySubject   string
	protoTypesSubject string
}

func (d *Destination) Open(ctx context.Context, spec connector.RuntimeSpec) error {
	return d.open(ctx, spec, destinationFactories{newClient: grpc.NewClient, newRegistry: schemaregistry.NewRegistry})
}

func (d *Destination) open(ctx context.Context, spec connector.RuntimeSpec, factories destinationFactories) error {
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
	var creds credentials.TransportCredentials
	switch {
	case cfg.insecure:
		creds = insecure.NewCredentials()
	case cfg.tlsCAFile != "":
		creds, err = credentials.NewClientTLSFromFile(cfg.tlsCAFile, cfg.tlsServerName)
		if err != nil {
			return fmt.Errorf("load tls ca: %w", err)
		}
	default:
		creds = credentials.NewClientTLSFromCert(nil, cfg.tlsServerName)
	}

	conn, err := factories.newClient(
		cfg.endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: cfg.timeout}),
	)
	if err != nil {
		if conn != nil {
			return errors.Join(fmt.Errorf("grpc dial: %w", err), conn.Close())
		}
		return fmt.Errorf("grpc dial: %w", err)
	}

	var registry schemaregistry.Registry
	if cfg.payloadMode == payloadModeWire && codec != nil {
		switch codec.Name() {
		case connector.WireFormatAvro, connector.WireFormatProto:
			registry, err = factories.newRegistry(ctx, cfg.registryConfig)
			if err != nil && !errors.Is(err, schemaregistry.ErrRegistryDisabled) {
				var cleanupErr error
				if registry != nil {
					cleanupErr = registry.Close()
				}
				cleanupErr = errors.Join(cleanupErr, conn.Close())
				return errors.Join(err, cleanupErr)
			}
			if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
				if registry != nil {
					if cleanupErr := registry.Close(); cleanupErr != nil {
						return errors.Join(cleanupErr, conn.Close())
					}
				}
				registry = nil
			}
		}
	}

	d.resourceMu.Lock()
	d.spec = spec
	d.endpoint = cfg.endpoint
	d.codec = codec
	d.payloadMode = cfg.payloadMode
	d.client = wallabypb.NewIngestServiceClient(conn)
	d.conn = conn
	d.headers = cfg.headers
	d.timeout = cfg.timeout
	d.maxRetries = cfg.maxRetries
	d.backoffBase = cfg.backoffBase
	d.backoffMax = cfg.backoffMax
	d.backoffFactor = cfg.backoffFactor
	d.flowID = cfg.flowID
	d.destination = cfg.destination
	d.registry = registry
	d.registrySubject = cfg.registrySubject
	d.protoTypesSubject = cfg.protoTypesSubject
	d.resourceMu.Unlock()
	return nil
}

func parseDestinationConfig(spec connector.RuntimeSpec) (destinationConfig, error) {
	decoder := options.NewDecoder("grpc options", spec.Options)
	registryConfig, registryErr := schemaregistry.ConfigFromOptions(spec.Options)
	cfg := destinationConfig{
		endpoint:          decoder.String(optEndpoint, ""),
		format:            decoder.String(optFormat, string(connector.WireFormatJSON)),
		payloadMode:       decoder.AliasedEnum(optPayloadMode, payloadModeWire, payloadModes),
		headers:           decoder.CaseInsensitiveKeyValueList(optHeaders),
		timeout:           decoder.Duration(optTimeout, 10*time.Second),
		maxRetries:        decoder.Int(optMaxRetries, 3),
		backoffBase:       decoder.Duration(optBackoffBase, 200*time.Millisecond),
		backoffMax:        decoder.Duration(optBackoffMax, 5*time.Second),
		backoffFactor:     decoder.Float64(optBackoffFactor, 2.0),
		insecure:          decoder.Bool(optInsecure, true),
		tlsCAFile:         decoder.String(optTLSCAFile, ""),
		tlsServerName:     decoder.String(optTLSServerName, ""),
		flowID:            decoder.String(optFlowID, ""),
		destination:       decoder.String(optDestination, spec.Name),
		registrySubject:   decoder.String(schemaregistry.OptRegistrySubject, ""),
		protoTypesSubject: decoder.String(schemaregistry.OptRegistryProtoTypes, ""),
		registryConfig:    registryConfig,
	}
	if err := errors.Join(decoder.Err(), registryErr); err != nil {
		return destinationConfig{}, err
	}
	if cfg.maxRetries < 0 {
		return destinationConfig{}, fmt.Errorf("grpc options.%s: must be non-negative", optMaxRetries)
	}
	if cfg.maxRetries == int(^uint(0)>>1) {
		return destinationConfig{}, fmt.Errorf("grpc options.%s: exceeds the supported retry count", optMaxRetries)
	}
	if cfg.backoffBase <= 0 {
		return destinationConfig{}, fmt.Errorf("grpc options.%s: must be positive", optBackoffBase)
	}
	// A zero maximum intentionally disables the configured cap; the runtime
	// still saturates at the largest representable duration.
	if cfg.backoffMax < 0 {
		return destinationConfig{}, fmt.Errorf("grpc options.%s: must be non-negative", optBackoffMax)
	}
	if cfg.backoffFactor <= 0 {
		return destinationConfig{}, fmt.Errorf("grpc options.%s: must be positive and finite", optBackoffFactor)
	}
	if cfg.endpoint == "" {
		return destinationConfig{}, errors.New("grpc endpoint is required")
	}
	if cfg.format == "" {
		cfg.format = string(connector.WireFormatJSON)
	}
	if cfg.destination == "" {
		cfg.destination = spec.Name
	}
	if cfg.insecure && cfg.tlsCAFile != "" {
		return destinationConfig{}, fmt.Errorf("grpc options.%s: cannot be set when insecure is true", optTLSCAFile)
	}
	if cfg.insecure && cfg.tlsServerName != "" {
		return destinationConfig{}, fmt.Errorf("grpc options.%s: cannot be set when insecure is true", optTLSServerName)
	}
	if err := validateMetadata(cfg.headers); err != nil {
		return destinationConfig{}, fmt.Errorf("grpc options.%s: %w", optHeaders, err)
	}
	return cfg, nil
}

func validateMetadata(headers map[string]string) error {
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if strings.HasPrefix(key, "grpc-") {
			return fmt.Errorf("metadata key %q uses the reserved grpc- prefix", key)
		}
		for _, char := range key {
			if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '_' || char == '.' {
				continue
			}
			return fmt.Errorf("metadata key %q contains an invalid character", key)
		}
		if strings.HasSuffix(key, "-bin") {
			continue
		}
		for _, value := range []byte(headers[key]) {
			if value < 0x20 || value > 0x7e {
				return fmt.Errorf("metadata value for %q contains a non-ASCII character", key)
			}
		}
	}
	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.client == nil {
		return errors.New("grpc destination not initialized")
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
	if d.payloadMode == payloadModeWire {
		if d.codec == nil {
			return errors.New("wire codec not initialized")
		}
		payload, err := d.codec.Encode(batch)
		if err != nil {
			return err
		}
		if len(payload) == 0 {
			return nil
		}
		req := d.buildRequest(payload, wireFormatToProto(d.codec.Name()), batch.Checkpoint)
		return d.sendWithRetry(ctx, req, d.payloadMode, meta)
	}

	for _, record := range batch.Records {
		payloadBatch := connector.Batch{
			Records:    []connector.Record{record},
			Schema:     batch.Schema,
			Checkpoint: batch.Checkpoint,
			WireFormat: batch.WireFormat,
		}
		payload, wf, err := d.encodePayload(payloadBatch, record)
		if err != nil {
			return err
		}
		if len(payload) == 0 {
			continue
		}
		req := d.buildRequest(payload, wf, batch.Checkpoint)
		if err := d.sendWithRetry(ctx, req, d.payloadMode, meta); err != nil {
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
	d.resourceMu.Lock()
	registry := d.registry
	conn := d.conn
	d.registry = nil
	d.conn = nil
	d.client = nil
	d.resourceMu.Unlock()

	var registryErr, connErr error
	if registry != nil {
		registryErr = registry.Close()
	}
	if conn != nil {
		connErr = conn.Close()
	}
	return errors.Join(registryErr, connErr)
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

func (d *Destination) encodePayload(batch connector.Batch, record connector.Record) ([]byte, wallabypb.WireFormat, error) {
	switch d.payloadMode {
	case payloadModeRecordJSON:
		payload, err := marshalRecordJSON(record)
		if err != nil {
			return nil, wallabypb.WireFormat_WIRE_FORMAT_UNSPECIFIED, err
		}
		return payload, wallabypb.WireFormat_WIRE_FORMAT_JSON, nil
	case payloadModeWAL:
		if len(record.Payload) == 0 {
			return nil, wallabypb.WireFormat_WIRE_FORMAT_UNSPECIFIED, errors.New("wal payload not available on record")
		}
		return record.Payload, wallabypb.WireFormat_WIRE_FORMAT_UNSPECIFIED, nil
	default:
		if d.codec == nil {
			return nil, wallabypb.WireFormat_WIRE_FORMAT_UNSPECIFIED, errors.New("wire codec not initialized")
		}
		payload, err := d.codec.Encode(batch)
		if err != nil {
			return nil, wallabypb.WireFormat_WIRE_FORMAT_UNSPECIFIED, err
		}
		return payload, wireFormatToProto(d.codec.Name()), nil
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

func (d *Destination) buildRequest(payload []byte, format wallabypb.WireFormat, checkpoint connector.Checkpoint) *wallabypb.IngestBatchRequest {
	return &wallabypb.IngestBatchRequest{
		FlowId:      d.flowID,
		Destination: d.destination,
		WireFormat:  format,
		Payload:     payload,
		Checkpoint:  checkpointToProto(checkpoint),
	}
}

func checkpointToProto(cp connector.Checkpoint) *wallabypb.Checkpoint {
	if cp.LSN == "" && cp.Timestamp.IsZero() && len(cp.Metadata) == 0 {
		return nil
	}
	return &wallabypb.Checkpoint{
		Lsn:                 cp.LSN,
		TimestampUnixMillis: cp.Timestamp.UnixMilli(),
		Metadata:            cp.Metadata,
	}
}

func wireFormatToProto(format connector.WireFormat) wallabypb.WireFormat {
	switch format {
	case connector.WireFormatArrow:
		return wallabypb.WireFormat_WIRE_FORMAT_ARROW
	case connector.WireFormatParquet:
		return wallabypb.WireFormat_WIRE_FORMAT_PARQUET
	case connector.WireFormatProto:
		return wallabypb.WireFormat_WIRE_FORMAT_PROTO
	case connector.WireFormatAvro:
		return wallabypb.WireFormat_WIRE_FORMAT_AVRO
	case connector.WireFormatJSON:
		return wallabypb.WireFormat_WIRE_FORMAT_JSON
	default:
		return wallabypb.WireFormat_WIRE_FORMAT_UNSPECIFIED
	}
}

func retryable(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return true
	}
	switch st.Code() {
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func (d *Destination) sendWithRetry(ctx context.Context, req *wallabypb.IngestBatchRequest, payloadMode string, meta *schemaMeta) error {
	attempts := d.maxRetries
	if attempts < 0 {
		attempts = 0
	}
	if attempts < int(^uint(0)>>1) {
		attempts++
	}

	for attempt := 1; ; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, d.timeout)
		md := metadata.New(nil)
		for k, v := range d.headers {
			md.Set(k, v)
		}
		if payloadMode != "" {
			md.Set("x-wallaby-payload-mode", payloadMode)
		}
		if meta != nil {
			md.Set("x-wallaby-registry-subject", meta.Subject)
			md.Set("x-wallaby-registry-id", meta.ID)
			if meta.Version > 0 {
				md.Set("x-wallaby-registry-version", fmt.Sprintf("%d", meta.Version))
			}
		}
		callCtx = metadata.NewOutgoingContext(callCtx, md)

		resp, err := d.client.IngestBatch(callCtx, req)
		cancel()

		if err == nil {
			if resp == nil || resp.Accepted {
				return nil
			}
			return fmt.Errorf("grpc destination rejected: %s", resp.Message)
		}

		if attempt >= attempts || !retryable(err) {
			return err
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
