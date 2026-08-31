package snowflake

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type staticStreamRESTToken string

func (t staticStreamRESTToken) KeypairJWT(context.Context) (string, error) { return string(t), nil }

func streamRESTTestConfig() streamConfig {
	return streamConfig{database: "DB", schema: "PUBLIC", pipe: "PIPE", pipeCreatedOn: "2026-01-01T00:00:00Z", destinationRevision: "revision-1"}
}

func streamRESTTestRequest(cfg streamConfig) streamAppendRequest {
	return streamAppendRequest{
		cfg: cfg, requestID: "wallaby-stream-request-" + strings.Repeat("a", 64), channelName: "CHANNEL_1",
		channelRevision: 7, pipeRevision: cfg.pipeCreatedOn, continuationToken: "cont-1", offsetToken: "offset-1",
		manifestHash: strings.Repeat("b", 64), rowsContentHash: strings.Repeat("c", 64), rowCount: 2,
		rows: []streamAppendRow{{rowHash: "h1", ordinal: 0, payload: []byte(`{"id":1}`)}, {rowHash: "h2", ordinal: 1, payload: []byte(`{"id":2}`)}},
	}
}

func TestStreamRESTTransportConformance(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	var server *httptest.Server
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.RequestURI())
		mu.Unlock()
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			if r.Header.Get("Authorization") != "Bearer jwt" || r.Header.Get("X-Snowflake-Authorization-Token-Type") != "KEYPAIR_JWT" {
				t.Errorf("hostname auth headers=%q/%q", r.Header.Get("Authorization"), r.Header.Get("X-Snowflake-Authorization-Token-Type"))
			}
			_ = json.NewEncoder(w).Encode(streamRESTHostnameResponse{Hostname: r.Host})
		case r.URL.Path == "/oauth/token":
			body, _ := io.ReadAll(r.Body)
			values, _ := url.ParseQuery(string(body))
			if values.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" || values.Get("scope") != r.Host {
				t.Errorf("token form=%q", body)
			}
			_ = json.NewEncoder(w).Encode(streamRESTTokenResponse{Token: "scoped"})
		case r.Method == http.MethodPut:
			if _, err := uuid.Parse(r.URL.Query().Get("requestId")); err != nil {
				t.Errorf("open requestId=%q: %v", r.URL.Query().Get("requestId"), err)
			}
			body, _ := io.ReadAll(r.Body)
			if string(body) != `{"fail_on_uncommitted_rows":true}` {
				t.Errorf("open body=%q", body)
			}
			_ = json.NewEncoder(w).Encode(streamRESTOpenResponse{NextContinuationToken: "cont-1", ChannelStatus: streamRESTChannelStatus{DatabaseName: "DB", SchemaName: "PUBLIC", PipeName: "PIPE", ChannelName: "CHANNEL_1", ChannelStatusCode: "ACTIVE", CreatedOnMS: 1}})
		case strings.HasSuffix(r.URL.Path, "/rows"):
			if r.Header.Get("Authorization") != "Bearer scoped" || r.Header.Get("X-Snowflake-Authorization-Token-Type") != "" {
				t.Errorf("append scoped auth headers=%q/%q", r.Header.Get("Authorization"), r.Header.Get("X-Snowflake-Authorization-Token-Type"))
			}
			query := r.URL.Query()
			if query.Get("continuationToken") != "cont-1" || query.Get("startOffsetToken") != "offset-1" || query.Get("endOffsetToken") != "offset-1" {
				t.Errorf("append query=%v", query)
			}
			if _, err := uuid.Parse(query.Get("requestId")); err != nil {
				t.Errorf("append requestId=%q: %v", query.Get("requestId"), err)
			}
			body, _ := io.ReadAll(r.Body)
			if string(body) != "{\"id\":1}\n{\"id\":2}\n" {
				t.Errorf("append body=%q", body)
			}
			_ = json.NewEncoder(w).Encode(streamRESTAppendResponse{NextContinuationToken: "cont-2"})
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			_ = json.NewEncoder(w).Encode(streamRESTBulkStatusResponse{ChannelStatuses: map[string]streamRESTChannelStatus{"CHANNEL_1": {DatabaseName: "DB", SchemaName: "PUBLIC", PipeName: "PIPE", ChannelName: "CHANNEL_1", ChannelStatusCode: "ACTIVE", LastCommittedOffsetToken: "offset-1", CreatedOnMS: 1}}})
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	})
	server = httptest.NewTLSServer(handler)
	defer server.Close()
	transport, err := newStreamRESTTransport(server.URL, server.Client(), staticStreamRESTToken("jwt"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := streamRESTTestConfig()
	opened, err := transport.OpenChannel(context.Background(), cfg, "CHANNEL_1")
	if err != nil || !opened.valid || opened.continuationToken != "cont-1" || opened.committedOffsetToken != "" {
		t.Fatalf("open=%+v/%v", opened, err)
	}
	request := streamRESTTestRequest(cfg)
	result, err := transport.AppendRows(context.Background(), request)
	if err != nil || result.disposition != streamAppendAccepted || result.continuationToken != "cont-2" {
		t.Fatalf("append=%+v/%v", result, err)
	}
	status, err := transport.ChannelStatus(context.Background(), cfg, "CHANNEL_1")
	if err != nil || !status.valid || status.committedOffsetToken != "offset-1" {
		t.Fatalf("status=%+v/%v", status, err)
	}
	managed := managedStreamRequest{requestID: request.requestID, channelName: request.channelName, pipeName: cfg.pipe, channelRevision: opened.channelRevision, pipeRevision: cfg.pipeCreatedOn, inputContinuation: request.continuationToken, requestedOffset: request.offsetToken, responseContinuation: result.continuationToken, manifestHash: request.manifestHash, rowsContentHash: request.rowsContentHash, rowCount: request.rowCount}
	evidence, err := transport.RequestStatus(context.Background(), cfg, managed)
	if err != nil || evidence.disposition != streamRequestStatusCommitted || evidence.committedOffset != "offset-1" {
		t.Fatalf("request status=%+v/%v", evidence, err)
	}
	if err := transport.DropChannel(context.Background(), cfg, "CHANNEL_1"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 7 {
		t.Fatalf("request count=%d requests=%v", len(requests), requests)
	}
}

func TestStreamRESTAppendStatusClassification(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     int
		wantError  error
		wantResult streamAppendDisposition
	}{
		{name: "request timeout", status: http.StatusRequestTimeout, wantError: connector.ErrDeliveryIndeterminate},
		{name: "throttle", status: http.StatusTooManyRequests, wantError: errStreamThrottled},
		{name: "server", status: http.StatusServiceUnavailable, wantError: connector.ErrDeliveryIndeterminate},
		{name: "auth", status: http.StatusUnauthorized, wantError: errStreamAuthExpired},
		{name: "forbidden", status: http.StatusForbidden, wantError: errStreamAuthExpired},
		{name: "invalidated", status: http.StatusConflict, wantError: errStreamChannelInvalidated},
		{name: "client rejection remains ambiguous", status: http.StatusUnprocessableEntity, wantError: connector.ErrDeliveryIndeterminate},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport, closeServer := newStreamRESTStatusFixture(t, test.status, "{}")
			defer closeServer()
			result, err := transport.AppendRows(context.Background(), streamRESTTestRequest(streamRESTTestConfig()))
			if test.wantError != nil {
				if !errors.Is(err, test.wantError) {
					t.Fatalf("error=%v want %v", err, test.wantError)
				}
				return
			}
			if err != nil || result.disposition != test.wantResult {
				t.Fatalf("result/error=%+v/%v", result, err)
			}
		})
	}
}

func TestStreamRESTMalformedOversizedRedirectCancellationAndHostDrift(t *testing.T) {
	t.Run("malformed", func(t *testing.T) {
		transport, closeServer := newStreamRESTStatusFixture(t, http.StatusOK, `{"next_continuation_token":`)
		defer closeServer()
		if _, err := transport.AppendRows(context.Background(), streamRESTTestRequest(streamRESTTestConfig())); err == nil {
			t.Fatal("malformed response accepted")
		}
	})
	t.Run("non-advancing continuation", func(t *testing.T) {
		transport, closeServer := newStreamRESTStatusFixture(t, http.StatusOK, `{"next_continuation_token":"cont-1"}`)
		defer closeServer()
		if _, err := transport.AppendRows(context.Background(), streamRESTTestRequest(streamRESTTestConfig())); !errors.Is(err, connector.ErrDeliveryConflict) {
			t.Fatalf("non-advancing continuation error=%v", err)
		}
	})
	t.Run("unknown field", func(t *testing.T) {
		transport, closeServer := newStreamRESTStatusFixture(t, http.StatusOK, `{"next_continuation_token":"cont-2","secret":"x"}`)
		defer closeServer()
		if _, err := transport.AppendRows(context.Background(), streamRESTTestRequest(streamRESTTestConfig())); err == nil {
			t.Fatal("unknown response field accepted")
		}
	})
	t.Run("oversized request", func(t *testing.T) {
		request := streamRESTTestRequest(streamRESTTestConfig())
		request.rows = []streamAppendRow{{payload: append([]byte(`{"v":"`), append(bytesOf('x', streamRESTMaxAppendBytes), []byte(`"}`)...)...)}}
		request.rowCount = 1
		transport, closeServer := newStreamRESTStatusFixture(t, http.StatusOK, `{"next_continuation_token":"cont-2"}`)
		defer closeServer()
		if _, err := transport.AppendRows(context.Background(), request); !errors.Is(err, errStreamOversize) {
			t.Fatalf("oversize error=%v", err)
		}
	})
	t.Run("oversized response", func(t *testing.T) {
		transport, closeServer := newStreamRESTStatusFixture(t, http.StatusOK, strings.Repeat("x", streamRESTMaxResponseBytes+1))
		defer closeServer()
		if _, err := transport.AppendRows(context.Background(), streamRESTTestRequest(streamRESTTestConfig())); err == nil || !strings.Contains(err.Error(), "response exceeds") {
			t.Fatalf("oversized response error=%v", err)
		}
	})
	t.Run("redirect", func(t *testing.T) {
		transport, closeServer := newStreamRESTStatusFixture(t, http.StatusTemporaryRedirect, "")
		defer closeServer()
		if _, err := transport.AppendRows(context.Background(), streamRESTTestRequest(streamRESTTestConfig())); !errors.Is(err, connector.ErrDeliveryIndeterminate) {
			t.Fatalf("redirect error=%v", err)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		transport, closeServer := newStreamRESTStatusFixture(t, http.StatusOK, `{"next_continuation_token":"cont-2"}`)
		defer closeServer()
		if _, err := transport.AppendRows(ctx, streamRESTTestRequest(streamRESTTestConfig())); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error=%v", err)
		}
	})
	t.Run("host drift", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(streamRESTHostnameResponse{Hostname: "other.invalid"})
		}))
		defer server.Close()
		transport, err := newStreamRESTTransport(server.URL, server.Client(), staticStreamRESTToken("jwt"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := transport.OpenChannel(context.Background(), streamRESTTestConfig(), "CHANNEL_1"); err == nil || !strings.Contains(err.Error(), "host drifted") {
			t.Fatalf("host drift error=%v", err)
		}
	})
}

func TestStreamRESTAcceptedThenDisconnectIsIndeterminate(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_ = json.NewEncoder(w).Encode(streamRESTHostnameResponse{Hostname: r.Host})
		case r.URL.Path == "/oauth/token":
			_ = json.NewEncoder(w).Encode(streamRESTTokenResponse{Token: "scoped"})
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			_ = json.NewEncoder(w).Encode(streamRESTBulkStatusResponse{ChannelStatuses: map[string]streamRESTChannelStatus{"CHANNEL_1": {DatabaseName: "DB", SchemaName: "PUBLIC", PipeName: "PIPE", ChannelName: "CHANNEL_1", ChannelStatusCode: "ACTIVE", LastCommittedOffsetToken: "offset-1", CreatedOnMS: 1}}})
		case r.Method == http.MethodPut:
			_ = json.NewEncoder(w).Encode(streamRESTOpenResponse{NextContinuationToken: "cont-recovered", ChannelStatus: streamRESTChannelStatus{DatabaseName: "DB", SchemaName: "PUBLIC", PipeName: "PIPE", ChannelName: "CHANNEL_1", ChannelStatusCode: "ACTIVE", LastCommittedOffsetToken: "offset-1", CreatedOnMS: 2}})
		default:
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatal("test server cannot hijack")
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()
		}
	}))
	defer server.Close()
	transport, err := newStreamRESTTransport(server.URL, server.Client(), staticStreamRESTToken("jwt"))
	if err != nil {
		t.Fatal(err)
	}
	request := streamRESTTestRequest(streamRESTTestConfig())
	_, err = transport.AppendRows(context.Background(), request)
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("disconnect error=%v", err)
	}
	managed := managedStreamRequest{requestID: request.requestID, channelName: request.channelName, pipeName: request.cfg.pipe, channelRevision: request.channelRevision, pipeRevision: request.pipeRevision, inputContinuation: request.continuationToken, requestedOffset: request.offsetToken, manifestHash: request.manifestHash, rowsContentHash: request.rowsContentHash, rowCount: request.rowCount}
	evidence, err := transport.RequestStatus(context.Background(), request.cfg, managed)
	if err != nil || evidence.disposition != streamRequestStatusCommitted || evidence.responseContinuation != "cont-recovered" {
		t.Fatalf("disconnect recovery evidence=%+v error=%v", evidence, err)
	}
}

func TestStreamRESTRejectsNonLoopbackHTTP(t *testing.T) {
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}}
	if _, err := newStreamRESTTransport("http://example.com", client, staticStreamRESTToken("jwt")); err == nil {
		t.Fatal("non-loopback HTTP accepted")
	}
}

func newStreamRESTStatusFixture(t *testing.T, appendStatus int, appendBody string) (*streamRESTTransport, func()) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/streaming/hostname":
			_ = json.NewEncoder(w).Encode(streamRESTHostnameResponse{Hostname: r.Host})
		case "/oauth/token":
			_ = json.NewEncoder(w).Encode(streamRESTTokenResponse{Token: "scoped"})
		default:
			w.WriteHeader(appendStatus)
			_, _ = fmt.Fprint(w, appendBody)
		}
	}))
	transport, err := newStreamRESTTransport(server.URL, server.Client(), staticStreamRESTToken("jwt"))
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return transport, server.Close
}

func bytesOf(value byte, count int) []byte {
	return []byte(strings.Repeat(string(value), count))
}

func TestStreamRESTTransportRemainsUnlinked(t *testing.T) {
	if streamingTransportLinked || ManagedStreamingTransportAvailable() {
		t.Fatal("REST transport was promoted without commercial evidence")
	}
}

func TestStreamRESTRequestStatusNeverInventsProvenAbsence(t *testing.T) {
	transport, closeServer := newStreamRESTStatusFixture(t, http.StatusOK, `{"channel_statuses":{"CHANNEL_1":{"database_name":"DB","schema_name":"PUBLIC","pipe_name":"PIPE","channel_name":"CHANNEL_1","channel_status_code":"ACTIVE","last_committed_offset_token":"offset-0","created_on_ms":1,"rows_inserted":0,"rows_parsed":0,"rows_errors":0,"rows_error_count":0,"last_error_offset_upper_bound":"","last_error_message":"","last_error_timestamp":"","snowflake_avg_processing_latency_ms":0}}}`)
	defer closeServer()
	// Preload the scoped session so the fixture's generic response is used only
	// for the status endpoint.
	_, _, err := transport.session(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	request := managedStreamRequest{requestID: "request", channelName: "CHANNEL_1", pipeName: "PIPE", channelRevision: 1, pipeRevision: streamRESTTestConfig().pipeCreatedOn, inputContinuation: "cont", requestedOffset: "offset-1", manifestHash: "manifest", rowsContentHash: "rows", rowCount: 1}
	evidence, err := transport.RequestStatus(context.Background(), streamRESTTestConfig(), request)
	if err != nil || evidence.disposition == streamRequestStatusProvenAbsent {
		t.Fatalf("status evidence=%+v error=%v", evidence, err)
	}
}

func TestStreamRESTContextDeadline(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { time.Sleep(time.Second) }))
	defer server.Close()
	transport, err := newStreamRESTTransport(server.URL, server.Client(), staticStreamRESTToken("jwt"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = transport.OpenChannel(ctx, streamRESTTestConfig(), "CHANNEL_1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error=%v", err)
	}
}
