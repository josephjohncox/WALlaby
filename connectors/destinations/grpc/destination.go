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
	"strconv"
	"strings"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
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
	optAddress       = "address"
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

// Destination delivers batches to a gRPC ingest endpoint.
type Destination struct {
	spec              connector.Spec
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

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	d.spec = spec
	endpoint := strings.TrimSpace(spec.Options[optEndpoint])
	if endpoint == "" {
		endpoint = strings.TrimSpace(spec.Options[optAddress])
	}
	if endpoint == "" {
		return errors.New("grpc endpoint is required")
	}
	d.endpoint = endpoint
	d.flowID = strings.TrimSpace(spec.Options[optFlowID])
	d.destination = strings.TrimSpace(spec.Options[optDestination])
	if d.destination == "" {
		d.destination = spec.Name
	}

	d.payloadMode = normalizePayloadMode(spec.Options[optPayloadMode])

	format := strings.TrimSpace(spec.Options[optFormat])
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
			if err != nil && !errors.Is(err, schemaregistry.ErrRegistryDisabled) {
				return err
			}
			if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
				registry = nil
			}
			d.registry = registry
		}
	}

	d.headers = parseHeaders(spec.Options[optHeaders])
	d.timeout = parseDuration(spec.Options[optTimeout], 10*time.Second)
	d.maxRetries = parseInt(spec.Options[optMaxRetries], 3)
	d.backoffBase = parseDuration(spec.Options[optBackoffBase], 200*time.Millisecond)
	d.backoffMax = parseDuration(spec.Options[optBackoffMax], 5*time.Second)
	d.backoffFactor = parseFloat(spec.Options[optBackoffFactor], 2.0)

	insecureMode := parseBool(spec.Options[optInsecure], true)
	var creds credentials.TransportCredentials
	if insecureMode {
		creds = insecure.NewCredentials()
	} else {
		caFile := strings.TrimSpace(spec.Options[optTLSCAFile])
		serverName := strings.TrimSpace(spec.Options[optTLSServerName])
		if caFile != "" {
			c, err := credentials.NewClientTLSFromFile(caFile, serverName)
			if err != nil {
				return fmt.Errorf("load tls ca: %w", err)
			}
			creds = c
		} else {
			creds = credentials.NewClientTLSFromCert(nil, serverName)
		}
	}

	conn, err := grpc.NewClient(
		d.endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{MinConnectTimeout: d.timeout}),
	)
	if err != nil {
		return fmt.Errorf("grpc dial: %w", err)
	}
	d.conn = conn
	d.client = wallabypb.NewIngestServiceClient(conn)
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
	if d.registry != nil {
		_ = d.registry.Close()
	}
	if d.conn != nil {
		return d.conn.Close()
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
	attempts := d.maxRetries + 1
	if attempts < 1 {
		attempts = 1
	}

	for attempt := 1; attempt <= attempts; attempt++ {
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

	return errors.New("grpc destination retries exhausted")
}

func (d *Destination) backoffDuration(attempt int) time.Duration {
	base := d.backoffBase
	if base <= 0 {
		base = 200 * time.Millisecond
	}
	factor := d.backoffFactor
	if factor <= 0 {
		factor = 2.0
	}
	exp := math.Pow(factor, float64(attempt-1))
	delay := time.Duration(float64(base) * exp)
	if delay > d.backoffMax && d.backoffMax > 0 {
		delay = d.backoffMax
	}
	// #nosec G404 -- jitter does not require cryptographic randomness.
	jitter := time.Duration(rand.Int63n(int64(delay/4 + 1)))
	return delay + jitter
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

func parseHeaders(raw string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		pieces := strings.SplitN(part, ":", 2)
		if len(pieces) != 2 {
			continue
		}
		key := strings.TrimSpace(pieces[0])
		val := strings.TrimSpace(pieces[1])
		if key == "" {
			continue
		}
		out[key] = val
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseDuration(raw string, fallback time.Duration) time.Duration {
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return value
}

func parseInt(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func parseFloat(raw string, fallback float64) float64 {
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback
	}
	return value
}

func parseBool(raw string, fallback bool) bool {
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}
