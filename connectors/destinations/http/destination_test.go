package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/typemapping"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"
)

func TestShippedHTTPTypedExampleUsesProductionParsers(t *testing.T) {
	t.Parallel()
	_, options := shippedHTTPExampleOptions(t)
	cfg, err := parseDestinationConfig(connector.RuntimeSpec{Name: "webhook_typed", Type: connector.EndpointHTTP, Options: options})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.payloadMode != payloadModeRecordJSON || cfg.timeout != 7*time.Second || cfg.maxRetries != 4 || cfg.backoffBase != 150*time.Millisecond || cfg.backoffMax != 3*time.Second || cfg.backoffFactor != 1.75 {
		t.Fatalf("parsed HTTP typed options = %+v", cfg)
	}
	if got := cfg.headers["x-labels"]; got != "alpha,beta" {
		t.Fatalf("decoded comma-bearing header = %q", got)
	}

	got, err := typemapping.Load(options)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"double precision":         "number",
		"timestamp with time zone": "string",
		"jsonb":                    "object",
		"bytea":                    "base64_string",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("type mappings = %#v, want %#v", got, want)
	}
}

func TestShippedHTTPTypedExampleMutationsAreRejected(t *testing.T) {
	t.Parallel()
	_, options := shippedHTTPExampleOptions(t)

	malformedOptions := copyOptions(options)
	malformedOptions[optTimeout] = "eventually"
	if _, err := parseDestinationConfig(connector.RuntimeSpec{Name: "webhook_typed", Type: connector.EndpointHTTP, Options: malformedOptions}); err == nil || !strings.Contains(err.Error(), optTimeout) {
		t.Fatalf("parseDestinationConfig() error = %v, want %s", err, optTimeout)
	}

	if _, err := typemapping.Load(map[string]string{typemapping.OptTypeMappings: "[not: a: mapping"}); err == nil || !strings.Contains(err.Error(), "parse type_mappings") {
		t.Fatalf("typemapping.Load() error = %v, want parse failure", err)
	}
}

func shippedHTTPExampleOptions(t *testing.T) (string, map[string]string) {
	t.Helper()
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	payload, err := os.ReadFile(filepath.Join(root, "examples", "flows", "postgres_to_http_typed.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Destinations []map[string]any `yaml:"destinations"`
	}
	if err := yaml.Unmarshal(payload, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, document := range fixture.Destinations {
		encoded, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		var endpoint wallabypb.Endpoint
		if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, &endpoint); err != nil {
			t.Fatal(err)
		}
		if endpoint.GetName() != "webhook_typed" {
			continue
		}
		spec, err := endpointcodec.Decode(&endpoint, endpointcodec.RoleDestination)
		if err != nil {
			t.Fatal(err)
		}
		if spec.Type == connector.EndpointHTTP {
			return root, spec.Options
		}
	}
	t.Fatal("HTTP typed example omits webhook_typed destination")
	return "", nil
}

func copyOptions(options map[string]string) map[string]string {
	out := make(map[string]string, len(options))
	for key, value := range options {
		out[key] = value
	}
	return out
}

func TestAppendWriteSendsMappedBatch(t *testing.T) {
	var body []byte
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, request *nethttp.Request) {
		var err error
		body, err = io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
		}
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()
	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	batch.Schema = connector.Schema{Namespace: "mapped", Name: "events", Columns: []connector.Column{{Name: "event_id", Type: "int8"}}}
	batch.WritePolicy = connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}
	batch.Records[0].Table = "events"
	batch.Records[0].After = map[string]any{"event_id": int64(7)}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	encoded := string(body)
	if !strings.Contains(encoded, `"table":"events"`) || !strings.Contains(encoded, `"event_id":7`) {
		t.Fatalf("request body=%s", encoded)
	}
}

func TestFailedDeliveryIsNotRemembered(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	var succeed atomic.Bool
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		requests.Add(1)
		if !succeed.Load() {
			nethttp.Error(w, "unavailable", nethttp.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	if err := destination.Write(context.Background(), batch); err == nil {
		t.Fatal("first Write() unexpectedly succeeded")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests after failed write = %d, want 1", got)
	}

	succeed.Store(true)
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatalf("retry Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after retry = %d, want 2", got)
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatalf("deduplicated Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after completed duplicate = %d, want 2", got)
	}
}

func TestDistinctUpdatesToSameKeyAreDelivered(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	keys := make(chan string, 2)
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, request *nethttp.Request) {
		requests.Add(1)
		keys <- request.Header.Get("Idempotency-Key")
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	batch.Records = []connector.Record{
		{
			Table:          "widgets",
			Operation:      connector.OpUpdate,
			SchemaVersion:  1,
			Key:            []byte("widget-1"),
			After:          map[string]any{"id": int64(1), "status": "first"},
			SourcePosition: "0/10",
		},
		{
			Table:          "widgets",
			Operation:      connector.OpUpdate,
			SchemaVersion:  1,
			Key:            []byte("widget-1"),
			After:          map[string]any{"id": int64(1), "status": "second"},
			SourcePosition: "0/20",
		},
	}
	batch.Checkpoint.LSN = "0/20"

	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want both updates delivered", got)
	}
	firstKey, secondKey := <-keys, <-keys
	if firstKey == "" || secondKey == "" || firstKey == secondKey {
		t.Fatalf("idempotency keys = (%q, %q), want distinct non-empty event identities", firstKey, secondKey)
	}

	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatalf("replay Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after exact replay = %d, want 2", got)
	}
}

func TestCompletedDeliveryIsSkippedOnlyUntilExpiry(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		requests.Add(1)
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	now := time.Unix(1_000, 0)
	destination.now = func() time.Time { return now }
	batch := testBatch()

	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests before expiry = %d, want 1", got)
	}

	now = now.Add(2 * time.Hour)
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after expiry = %d, want 2", got)
	}
}

func TestConcurrentDuplicateRetriesAfterLeaderFailure(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		if requests.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
			nethttp.Error(w, "unavailable", nethttp.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	firstResult := make(chan error, 1)
	secondResult := make(chan error, 1)
	go func() { firstResult <- destination.Write(context.Background(), batch) }()
	<-firstEntered
	go func() { secondResult <- destination.Write(context.Background(), batch) }()

	select {
	case err := <-secondResult:
		t.Fatalf("duplicate returned before leader completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("concurrent requests before leader completion = %d, want 1", got)
	}

	close(releaseFirst)
	if err := <-firstResult; err == nil {
		t.Fatal("leader Write() unexpectedly succeeded")
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("waiting duplicate Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after leader failure = %d, want 2", got)
	}
}

func TestConcurrentDuplicateSkipsAfterLeaderSuccess(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		if requests.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
		}
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	firstResult := make(chan error, 1)
	secondResult := make(chan error, 1)
	go func() { firstResult <- destination.Write(context.Background(), batch) }()
	<-firstEntered
	go func() { secondResult <- destination.Write(context.Background(), batch) }()
	close(releaseFirst)

	if err := <-firstResult; err != nil {
		t.Fatalf("leader Write() error = %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("duplicate Write() error = %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests after leader success = %d, want 1", got)
	}
}

func TestCancellationReleasesReservation(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		if requests.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
			return
		}
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	leaderContext, cancelLeader := context.WithCancel(context.Background())
	leaderResult := make(chan error, 1)
	waitingResult := make(chan error, 1)
	go func() { leaderResult <- destination.Write(leaderContext, batch) }()
	<-firstEntered
	go func() { waitingResult <- destination.Write(context.Background(), batch) }()
	cancelLeader()
	leaderErr := <-leaderResult
	// Do not let the test server complete the request before the HTTP client has
	// observed cancellation; otherwise response completion and cancellation race.
	close(releaseFirst)

	if !errors.Is(leaderErr, context.Canceled) {
		t.Fatalf("leader Write() error = %v, want context canceled", leaderErr)
	}
	if err := <-waitingResult; err != nil {
		t.Fatalf("waiting Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after cancellation = %d, want 2", got)
	}
}

func TestOpenRejectsTypedOptionsBeforeRegistryCreation(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{name: "numeric", key: optMaxRetries, value: "many", wantErr: "http options.max_retries"},
		{name: "maximum integer retries", key: optMaxRetries, value: strconv.Itoa(int(^uint(0) >> 1)), wantErr: "exceeds the supported retry count"},
		{name: "headers", key: optHeaders, value: `"X-Broken: value`, wantErr: "http options.headers"},
		{name: "payload mode", key: optPayloadMode, value: "unexpected", wantErr: "http options.payload_mode"},
		{name: "registry timeout", key: schemaregistry.OptRegistryTimeout, value: "soon", wantErr: "schema registry options.schema_registry_timeout"},
		{name: "registry bool", key: schemaregistry.OptRegistryApicurioCompat, value: "yes", wantErr: "schema registry options.schema_registry_apicurio_compat"},
		{name: "relative URL", key: optURL, value: "/relative", wantErr: "absolute http or https URL"},
		{name: "invalid method", key: optMethod, value: "BAD METHOD", wantErr: "invalid HTTP method"},
		{name: "case-colliding headers", key: optHeaders, value: "X-Test:one,x-test:two", wantErr: "case normalization"},
		{name: "invalid idempotency header", key: optIdempotencyHeader, value: "Bad Header", wantErr: optIdempotencyHeader},
		{name: "negative retries", key: optMaxRetries, value: "-1", wantErr: optMaxRetries},
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
				optURL:                         "http://unused.invalid",
				optFormat:                      string(connector.WireFormatAvro),
				schemaregistry.OptRegistryType: "postgres",
				schemaregistry.OptRegistryDSN:  "://invalid registry DSN",
				test.key:                       test.value,
			}
			err := (&Destination{}).Open(context.Background(), connector.RuntimeSpec{Options: options})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Open() error = %v, want %q", err, test.wantErr)
			}
			if strings.Contains(err.Error(), "schema_registry_dsn") || strings.Contains(err.Error(), "connect") {
				t.Fatalf("Open() reached registry creation before option validation: %v", err)
			}
		})
	}
}

func TestParseDestinationConfigPayloadModesAreCanonical(t *testing.T) {
	for raw, want := range map[string]string{"": payloadModeWire, "wire": payloadModeWire, "record_json": payloadModeRecordJSON, "wal": payloadModeWAL} {
		cfg, err := parseDestinationConfig(connector.RuntimeSpec{Options: map[string]string{optURL: "https://example.test/hook", optPayloadMode: raw}})
		if err != nil {
			t.Fatalf("payload_mode %q: %v", raw, err)
		}
		if cfg.payloadMode != want {
			t.Errorf("payload_mode %q = %q, want %q", raw, cfg.payloadMode, want)
		}
	}
	for _, alias := range []string{"record", "raw"} {
		if _, err := parseDestinationConfig(connector.RuntimeSpec{Options: map[string]string{optURL: "https://example.test/hook", optPayloadMode: alias}}); err == nil {
			t.Fatalf("legacy payload mode %q was accepted", alias)
		}
	}
}

func TestParseDestinationConfigValidatesHTTPFields(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]string
		wantErr string
	}{
		{name: "relative URL", options: map[string]string{optURL: "/relative"}, wantErr: "absolute http or https URL"},
		{name: "wrong URL scheme", options: map[string]string{optURL: "ftp://example.test/file"}, wantErr: "absolute http or https URL"},
		{name: "invalid method", options: map[string]string{optURL: "https://example.test", optMethod: "BAD METHOD"}, wantErr: "invalid HTTP method"},
		{name: "invalid idempotency header", options: map[string]string{optURL: "https://example.test", optIdempotencyHeader: "Bad Header"}, wantErr: optIdempotencyHeader},
		{name: "invalid transaction header", options: map[string]string{optURL: "https://example.test", optTransactionHeader: "Bad Header"}, wantErr: optTransactionHeader},
		{name: "case-colliding headers", options: map[string]string{optURL: "https://example.test", optHeaders: "X-Test:one,x-test:two"}, wantErr: "case normalization"},
		{name: "invalid header value", options: map[string]string{optURL: "https://example.test", optHeaders: "X-Test:value\x00bad"}, wantErr: "invalid value"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseDestinationConfig(connector.RuntimeSpec{Options: test.options})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("parseDestinationConfig() error = %v, want %q", err, test.wantErr)
			}
		})
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
		{name: "invalid direct factor is safe", base: time.Millisecond, max: 5 * time.Second, factor: math.Inf(1), attempt: maxInt},
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

	cfg, err := parseDestinationConfig(connector.RuntimeSpec{Options: map[string]string{optURL: "https://example.test", optBackoffFactor: "1.7976931348623157e308", optBackoffMax: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.backoffFactor != math.MaxFloat64 || cfg.backoffMax != 0 {
		t.Fatalf("backoff config: factor=%v max=%v", cfg.backoffFactor, cfg.backoffMax)
	}
}

func TestQuotedHeaderCommaIsDelivered(t *testing.T) {
	var header string
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, request *nethttp.Request) {
		header = request.Header.Get("X-List")
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := &Destination{}
	err := destination.Open(context.Background(), connector.RuntimeSpec{Options: map[string]string{
		optURL:         server.URL,
		optPayloadMode: payloadModeRecordJSON,
		optMaxRetries:  "0",
		optHeaders:     `"X-List: alpha,beta"`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.Write(context.Background(), testBatch()); err != nil {
		t.Fatal(err)
	}
	if header != "alpha,beta" {
		t.Fatalf("X-List header = %q", header)
	}
}

func openTestDestination(t *testing.T, url string, dedupeWindow time.Duration) *Destination {
	t.Helper()
	destination := &Destination{}
	err := destination.Open(context.Background(), connector.RuntimeSpec{Options: map[string]string{
		optURL:          url,
		optPayloadMode:  payloadModeRecordJSON,
		optMaxRetries:   "0",
		optDedupeWindow: dedupeWindow.String(),
	}})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return destination
}

func testBatch() connector.Batch {
	return connector.Batch{
		Schema: connector.Schema{Name: "widgets", Namespace: "public", Version: 1},
		Records: []connector.Record{{
			Table:         "widgets",
			Operation:     connector.OpInsert,
			SchemaVersion: 1,
			After:         map[string]any{"id": int64(1)},
		}},
		Checkpoint: connector.Checkpoint{LSN: "0/10"},
	}
}
