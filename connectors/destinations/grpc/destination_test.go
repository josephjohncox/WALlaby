package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/typemapping"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func TestShippedGRPCTypedExampleUsesProductionParsers(t *testing.T) {
	t.Parallel()
	options := shippedGRPCExampleOptions(t)
	cfg, err := parseDestinationConfig(connector.Spec{Name: "grpc_typed", Type: connector.EndpointGRPC, Options: options})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.endpoint != "localhost:9090" || cfg.payloadMode != payloadModeRecordJSON || !cfg.insecure || cfg.tlsCAFile != "" || cfg.tlsServerName != "" || cfg.timeout != 6*time.Second || cfg.maxRetries != 5 || cfg.backoffBase != 125*time.Millisecond || cfg.backoffMax != 4*time.Second || cfg.backoffFactor != 1.5 {
		t.Fatalf("parsed gRPC typed options = %+v", cfg)
	}
	if got := cfg.headers["x-routing-tags"]; got != "blue,green" {
		t.Fatalf("decoded comma-bearing metadata = %q", got)
	}
	got, err := typemapping.Load(options)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"double precision": "number", "timestamp with time zone": "string", "jsonb": "object"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("type mappings = %#v, want %#v", got, want)
	}
}

func TestShippedGRPCTypedExampleMutationsAreRejected(t *testing.T) {
	t.Parallel()
	options := shippedGRPCExampleOptions(t)

	malformedOptions := copyExampleOptions(options)
	malformedOptions[optBackoffFactor] = "many"
	if _, err := parseDestinationConfig(connector.Spec{Name: "grpc_typed", Type: connector.EndpointGRPC, Options: malformedOptions}); err == nil || !strings.Contains(err.Error(), optBackoffFactor) {
		t.Fatalf("parseDestinationConfig() error = %v, want %s", err, optBackoffFactor)
	}

	malformedMappings := copyExampleOptions(options)
	malformedMappings[typemapping.OptTypeMappings] = "double precision: [number"
	if _, err := typemapping.Load(malformedMappings); err == nil || !strings.Contains(err.Error(), "parse type_mappings") {
		t.Fatalf("typemapping.Load() error = %v, want parse failure", err)
	}
}

func shippedGRPCExampleOptions(t *testing.T) map[string]string {
	t.Helper()
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	payload, err := os.ReadFile(filepath.Join(root, "examples", "flows", "postgres_to_grpc_typed.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Destinations []struct {
			Name    string            `json:"name"`
			Type    string            `json:"type"`
			Options map[string]string `json:"options"`
		} `json:"destinations"`
	}
	if err := json.Unmarshal(payload, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, destination := range fixture.Destinations {
		if destination.Name == "grpc_typed" && destination.Type == string(connector.EndpointGRPC) {
			return destination.Options
		}
	}
	t.Fatal("gRPC typed example omits grpc_typed destination")
	return nil
}

func copyExampleOptions(options map[string]string) map[string]string {
	out := make(map[string]string, len(options))
	for key, value := range options {
		out[key] = value
	}
	return out
}

type closeTrackingRegistry struct {
	closeCalls int
	closeErr   error
}

func (r *closeTrackingRegistry) Register(context.Context, schemaregistry.RegisterRequest) (schemaregistry.RegisterResult, error) {
	return schemaregistry.RegisterResult{}, nil
}

func (r *closeTrackingRegistry) Close() error {
	r.closeCalls++
	return r.closeErr
}

type recordingIngestClient struct {
	requests []*wallabypb.IngestBatchRequest
}

func (c *recordingIngestClient) IngestBatch(_ context.Context, request *wallabypb.IngestBatchRequest, _ ...gogrpc.CallOption) (*wallabypb.IngestBatchResponse, error) {
	c.requests = append(c.requests, request)
	return &wallabypb.IngestBatchResponse{Accepted: true}, nil
}

func TestOpenRejectsTypedOptionsBeforeRegistryTLSAndDial(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{name: "numeric", key: optBackoffFactor, value: "twice", wantErr: "grpc options.backoff_factor"},
		{name: "headers", key: optHeaders, value: `"X-Broken: value`, wantErr: "grpc options.headers"},
		{name: "payload mode", key: optPayloadMode, value: "unexpected", wantErr: "grpc options.payload_mode"},
		{name: "registry timeout", key: schemaregistry.OptRegistryTimeout, value: "soon", wantErr: "schema registry options.schema_registry_timeout"},
		{name: "registry bool", key: schemaregistry.OptRegistryApicurioCompat, value: "yes", wantErr: "schema registry options.schema_registry_apicurio_compat"},
		{name: "case-colliding headers", key: optHeaders, value: "X-Test:one,x-test:two", wantErr: "case normalization"},
		{name: "grpc-invalid metadata key", key: optHeaders, value: "X%Test:value", wantErr: "invalid character"},
		{name: "negative retries", key: optMaxRetries, value: "-1", wantErr: optMaxRetries},
		{name: "maximum integer retries", key: optMaxRetries, value: strconv.Itoa(int(^uint(0) >> 1)), wantErr: "exceeds the supported retry count"},
		{name: "zero backoff base", key: optBackoffBase, value: "0", wantErr: optBackoffBase},
		{name: "negative backoff max", key: optBackoffMax, value: "-1s", wantErr: optBackoffMax},
		{name: "zero backoff factor", key: optBackoffFactor, value: "0", wantErr: optBackoffFactor},
		{name: "negative backoff factor", key: optBackoffFactor, value: "-1", wantErr: optBackoffFactor},
		{name: "NaN backoff factor", key: optBackoffFactor, value: "NaN", wantErr: optBackoffFactor},
		{name: "infinite backoff factor", key: optBackoffFactor, value: "+Inf", wantErr: optBackoffFactor},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := map[string]string{
				optEndpoint:                    "unused.invalid:443",
				optFormat:                      string(connector.WireFormatAvro),
				optInsecure:                    "false",
				optTLSCAFile:                   t.TempDir() + "/missing.pem",
				schemaregistry.OptRegistryType: "postgres",
				schemaregistry.OptRegistryDSN:  "://invalid registry DSN",
				test.key:                       test.value,
			}
			err := (&Destination{}).Open(context.Background(), connector.Spec{Options: options})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Open() error = %v, want %q", err, test.wantErr)
			}
			if strings.Contains(err.Error(), "load tls ca") || strings.Contains(err.Error(), "grpc dial") || strings.Contains(err.Error(), "schema_registry_dsn") {
				t.Fatalf("Open() performed a side effect before option validation: %v", err)
			}
		})
	}
}

func TestParseDestinationConfigPayloadModeAliases(t *testing.T) {
	for raw, want := range map[string]string{"": payloadModeWire, "wire": payloadModeWire, "record": payloadModeRecordJSON, "record_json": payloadModeRecordJSON, "raw": payloadModeRecordJSON, "wal": payloadModeWAL} {
		cfg, err := parseDestinationConfig(connector.Spec{Name: "sink", Options: map[string]string{optEndpoint: "unused.invalid:443", optPayloadMode: raw}})
		if err != nil {
			t.Fatalf("payload_mode %q: %v", raw, err)
		}
		if cfg.payloadMode != want {
			t.Errorf("payload_mode %q = %q, want %q", raw, cfg.payloadMode, want)
		}
	}
}

func TestParseDestinationConfigRejectsInvalidMetadataAndTLSCombinations(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]string
		wantErr string
	}{
		{name: "case collision", options: map[string]string{optEndpoint: "unused:443", optHeaders: "X-Test:one,x-test:two"}, wantErr: "case normalization"},
		{name: "grpc-invalid key", options: map[string]string{optEndpoint: "unused:443", optHeaders: "X%Test:value"}, wantErr: "invalid character"},
		{name: "reserved key", options: map[string]string{optEndpoint: "unused:443", optHeaders: "grpc-test:value"}, wantErr: "reserved grpc- prefix"},
		{name: "non-ASCII value", options: map[string]string{optEndpoint: "unused:443", optHeaders: "X-Test:café"}, wantErr: "non-ASCII"},
		{name: "NUL normal value", options: map[string]string{optEndpoint: "unused:443", optHeaders: "X-Test:value\x00bad"}, wantErr: "non-ASCII"},
		{name: "CA with insecure", options: map[string]string{optEndpoint: "unused:443", optTLSCAFile: "ca.pem"}, wantErr: optTLSCAFile},
		{name: "server name with insecure", options: map[string]string{optEndpoint: "unused:443", optTLSServerName: "example.test"}, wantErr: optTLSServerName},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseDestinationConfig(connector.Spec{Options: test.options})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("parseDestinationConfig() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestParseDestinationConfigAllowsBinaryMetadataBytes(t *testing.T) {
	binaryValue := string([]byte{0, 0xff, 'x'})
	cfg, err := parseDestinationConfig(connector.Spec{Name: "sink", Options: map[string]string{
		optEndpoint: "unused.invalid:443",
		optHeaders:  "Trace-Bin:" + binaryValue,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.headers["trace-bin"]; got != binaryValue {
		t.Fatalf("binary metadata = %v, want %v", []byte(got), []byte(binaryValue))
	}
}

func TestParseDestinationConfigSupportsQuotedHeaderComma(t *testing.T) {
	cfg, err := parseDestinationConfig(connector.Spec{Name: "sink", Options: map[string]string{
		optEndpoint: "unused.invalid:443",
		optHeaders:  `"X-List: alpha,beta","X-Colon: one:two"`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.headers["x-list"] != "alpha,beta" || cfg.headers["x-colon"] != "one:two" {
		t.Fatalf("headers = %#v", cfg.headers)
	}
}

func TestBackoffDurationSaturatesSafely(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	tests := []struct {
		name    string
		base    time.Duration
		max     time.Duration
		factor  float64
		attempt int
	}{
		{name: "max float and huge attempt", base: time.Millisecond, max: 5 * time.Second, factor: math.MaxFloat64, attempt: maxInt},
		{name: "huge base", base: time.Duration(1<<63 - 1), max: 5 * time.Second, factor: 2, attempt: maxInt},
		{name: "underflow remains positive", base: time.Millisecond, max: 5 * time.Second, factor: math.SmallestNonzeroFloat64, attempt: maxInt},
		{name: "unconfigured max saturates duration", base: time.Second, max: 0, factor: math.MaxFloat64, attempt: maxInt},
		{name: "invalid direct factor is safe", base: time.Millisecond, max: 5 * time.Second, factor: math.Inf(-1), attempt: maxInt},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			destination := &Destination{backoffBase: test.base, backoffMax: test.max, backoffFactor: test.factor}
			got := destination.backoffDuration(test.attempt)
			if got <= 0 {
				t.Fatalf("backoffDuration() = %v", got)
			}
			if test.max > 0 && got > test.max {
				t.Fatalf("backoffDuration() = %v, exceeds max %v", got, test.max)
			}
		})
	}

	cfg, err := parseDestinationConfig(connector.Spec{Options: map[string]string{optEndpoint: "unused:443", optBackoffFactor: "1.7976931348623157e308", optBackoffMax: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.backoffFactor != math.MaxFloat64 || cfg.backoffMax != 0 {
		t.Fatalf("backoff config: factor=%v max=%v", cfg.backoffFactor, cfg.backoffMax)
	}
}

func TestOpenLoadsTLSBeforeCreatingClientOrRegistry(t *testing.T) {
	factories := destinationFactories{
		newClient: func(string, ...gogrpc.DialOption) (*gogrpc.ClientConn, error) {
			t.Fatal("gRPC client created before invalid CA was rejected")
			return nil, nil
		},
		newRegistry: func(context.Context, schemaregistry.Config) (schemaregistry.Registry, error) {
			t.Fatal("registry created before invalid CA was rejected")
			return nil, nil
		},
	}
	err := (&Destination{}).open(context.Background(), connector.Spec{Options: map[string]string{
		optEndpoint:  "unused.invalid:443",
		optFormat:    string(connector.WireFormatAvro),
		optInsecure:  "false",
		optTLSCAFile: t.TempDir() + "/missing.pem",
	}}, factories)
	if err == nil || !strings.Contains(err.Error(), "load tls ca") {
		t.Fatalf("open() error = %v", err)
	}
}

func TestOpenCleansUpClientAndPartialRegistryOnRegistryFailure(t *testing.T) {
	var conn *gogrpc.ClientConn
	registry := &closeTrackingRegistry{}
	registryErr := errors.New("registry failed")
	factories := destinationFactories{
		newClient: func(target string, opts ...gogrpc.DialOption) (*gogrpc.ClientConn, error) {
			var err error
			conn, err = gogrpc.NewClient(target, opts...)
			return conn, err
		},
		newRegistry: func(context.Context, schemaregistry.Config) (schemaregistry.Registry, error) {
			return registry, registryErr
		},
	}
	destination := &Destination{}
	err := destination.open(context.Background(), connector.Spec{Options: map[string]string{
		optEndpoint: "unused.invalid:443",
		optFormat:   string(connector.WireFormatAvro),
	}}, factories)
	if !errors.Is(err, registryErr) {
		t.Fatalf("open() error = %v", err)
	}
	if conn == nil || conn.GetState() != connectivity.Shutdown {
		t.Fatalf("connection state = %v, want shutdown", conn)
	}
	if registry.closeCalls != 1 {
		t.Fatalf("partial registry close calls = %d, want 1", registry.closeCalls)
	}
	if err := destination.Close(context.Background()); err != nil {
		t.Fatalf("Close() after failed Open = %v", err)
	}
	if registry.closeCalls != 1 {
		t.Fatalf("partial registry close calls after destination Close = %d, want 1", registry.closeCalls)
	}
	if destination.conn != nil || destination.registry != nil {
		t.Fatalf("destination retained failed resources: conn=%v registry=%v", destination.conn, destination.registry)
	}
}

func TestCloseIsSequentiallyIdempotentAndJoinsErrors(t *testing.T) {
	conn, err := gogrpc.NewClient("unused.invalid:443", gogrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	registryErr := errors.New("close registry")
	registry := &closeTrackingRegistry{closeErr: registryErr}
	destination := &Destination{registry: registry, conn: conn, client: wallabypb.NewIngestServiceClient(conn)}

	if err := destination.Close(context.Background()); !errors.Is(err, registryErr) {
		t.Fatalf("first Close() error = %v", err)
	}
	if registry.closeCalls != 1 || conn.GetState() != connectivity.Shutdown {
		t.Fatalf("first Close(): registry calls=%d conn state=%v", registry.closeCalls, conn.GetState())
	}
	if destination.registry != nil || destination.conn != nil || destination.client != nil {
		t.Fatalf("owned fields not cleared: registry=%v conn=%v client=%v", destination.registry, destination.conn, destination.client)
	}
	if err := destination.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if registry.closeCalls != 1 {
		t.Fatalf("second Close() repeated registry close: calls=%d", registry.closeCalls)
	}
}

func TestWriteSendsMappedAppendRequest(t *testing.T) {
	client := &recordingIngestClient{}
	destination := &Destination{client: client, payloadMode: payloadModeRecordJSON, timeout: time.Second, flowID: "flow-1", destination: "sink"}
	batch := connector.Batch{
		Schema:      connector.Schema{Namespace: "mapped", Name: "events", Columns: []connector.Column{{Name: "event_id", Type: "int8"}}},
		Checkpoint:  connector.Checkpoint{LSN: "0/20"},
		WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
		Records:     []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 3, After: map[string]any{"event_id": float64(7)}}},
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(client.requests) != 1 {
		t.Fatalf("ingest requests=%d", len(client.requests))
	}
	request := client.requests[0]
	if request.FlowId != "flow-1" || request.Destination != "sink" || request.WireFormat != wallabypb.WireFormat_WIRE_FORMAT_JSON || request.Checkpoint.GetLsn() != "0/20" {
		t.Fatalf("request=%+v", request)
	}
	var payload struct {
		Table     string         `json:"table"`
		Operation string         `json:"operation"`
		After     map[string]any `json:"after"`
	}
	if err := json.Unmarshal(request.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Table != "events" || payload.Operation != "insert" || payload.After["event_id"] != float64(7) {
		t.Fatalf("payload=%+v", payload)
	}
}
