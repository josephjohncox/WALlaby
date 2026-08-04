package iceberg

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	icerest "github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestEndpointBoundTransportRejectsCredentialRedirects(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	transport, err := newEndpointBoundTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody, Header: make(http.Header)}, nil
	}), "https://catalog.example/iceberg")
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://catalog.example/v1/config", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.RoundTrip(allowed); err != nil {
		t.Fatalf("allowed catalog endpoint: %v", err)
	}
	for _, rawURL := range []string{"https://auth.example/token", "https://attacker.example/steal"} {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := transport.RoundTrip(request); err == nil || !strings.Contains(err.Error(), "outside deployment-bound Iceberg endpoints") {
			t.Fatalf("redirect %s error=%v", rawURL, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("base transport calls=%d, want only one catalog request", calls.Load())
	}
}

func TestAWSRoleIdentityMatchesAssumedRole(t *testing.T) {
	t.Parallel()

	expected := "arn:aws:iam::123456789012:role/platform/wallaby-iceberg-writer"
	for _, tt := range []struct {
		name   string
		actual string
		want   bool
	}{
		{name: "assumed role", actual: "arn:aws:sts::123456789012:assumed-role/wallaby-iceberg-writer/session-1", want: true},
		{name: "exact role", actual: expected, want: true},
		{name: "wrong account", actual: "arn:aws:sts::999999999999:assumed-role/wallaby-iceberg-writer/session-1"},
		{name: "wrong role", actual: "arn:aws:sts::123456789012:assumed-role/attacker/session-1"},
		{name: "IAM user", actual: "arn:aws:iam::123456789012:user/wallaby"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := awsRoleIdentityMatches(expected, tt.actual); got != tt.want {
				t.Fatalf("awsRoleIdentityMatches()=%t, want %t", got, tt.want)
			}
		})
	}
}

func TestRESTCatalogRejectsInsecureCredentialOrigins(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "bearer token over HTTP", cfg: Config{URI: "http://127.0.0.1:8181", Warehouse: "warehouse", OAuthToken: "token", AllowHTTP: true, RequestTimeout: time.Second}, want: "authenticated Iceberg REST requires HTTPS"},
		{name: "SigV4 over HTTP", cfg: Config{URI: "http://127.0.0.1:8181", Warehouse: "warehouse", SigV4: true, Region: "us-east-1", AllowHTTP: true, RequestTimeout: time.Second}, want: "authenticated Iceberg REST requires HTTPS"},
		{name: "cross-origin OAuth", cfg: Config{URI: "https://catalog.example/iceberg", Warehouse: "warehouse", OAuthCredential: "client:secret", OAuthURI: "https://auth.example/token", RequestTimeout: time.Second}, want: "same origin"},
		{name: "HTTP OAuth", cfg: Config{URI: "https://catalog.example/iceberg", Warehouse: "warehouse", OAuthCredential: "client:secret", OAuthURI: "http://catalog.example/token", RequestTimeout: time.Second}, want: "HTTPS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := newRESTCatalog(context.Background(), tt.cfg); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestCatalogResponsePropertyPolicyRejectsEndpointRedirection(t *testing.T) {
	t.Parallel()

	cfg := Config{Profile: CatalogProfileREST, URI: "https://catalog.example/iceberg", Warehouse: "warehouse", S3Endpoint: "https://s3.example", S3Region: "us-east-1"}
	for _, tt := range []struct {
		name  string
		props map[string]string
		want  string
	}{
		{name: "catalog URI", props: map[string]string{"uri": "https://attacker.example/iceberg"}, want: "catalog uri"},
		{name: "S3 endpoint", props: map[string]string{"s3.endpoint": "https://attacker.example"}, want: "s3.endpoint"},
		{name: "S3 region", props: map[string]string{"s3.region": "us-west-2"}, want: "s3.region"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := validateCatalogResponseProperties(cfg, tt.props); err == nil || !strings.Contains(strings.ToLower(err.Error()), tt.want) {
				t.Fatalf("error=%v, want substring %q", err, tt.want)
			}
		})
	}
	if err := validateCatalogResponseProperties(cfg, map[string]string{"uri": cfg.URI, "s3.endpoint": cfg.S3Endpoint, "s3.region": cfg.S3Region}); err != nil {
		t.Fatalf("deployment-bound catalog properties: %v", err)
	}
	metadataResponse := []byte(`{"metadata":{"properties":{"s3.endpoint":"https://attacker.example"}}}`)
	if err := validateCatalogResponseBody(cfg, metadataResponse); err == nil || !strings.Contains(err.Error(), "s3.endpoint") {
		t.Fatalf("metadata endpoint error=%v, want rejection", err)
	}
}

func TestRESTCatalogRejectsCatalogVendedS3EndpointRedirection(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/config" {
			http.NotFound(response, request)
			return
		}
		_ = json.NewEncoder(response).Encode(map[string]any{
			"defaults":  map[string]string{},
			"overrides": map[string]string{"s3.endpoint": "https://attacker.example"},
		})
	}))
	defer server.Close()
	cfg := testIcebergConfig()
	cfg.URI = server.URL
	cfg.S3Endpoint = "http://127.0.0.1:9000"
	if _, err := newRESTCatalog(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "s3.endpoint") {
		t.Fatalf("error=%v, want catalog-vended endpoint rejection", err)
	}
}

func TestRESTCatalogConformanceAuthConflictAndUnknownCommit(t *testing.T) {
	t.Parallel()
	var status atomic.Int32
	status.Store(http.StatusConflict)
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/config":
			if request.URL.Query().Get("warehouse") != "warehouse" {
				t.Errorf("warehouse query=%q", request.URL.RawQuery)
			}
			if request.Header.Get("Authorization") != "Bearer test-token" {
				t.Errorf("config authorization=%q", request.Header.Get("Authorization"))
			}
			_ = json.NewEncoder(response).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})
		case "/v1/namespaces/lake/tables/events":
			if request.Method != http.MethodPost {
				t.Errorf("method=%s", request.Method)
			}
			if request.Header.Get("Authorization") != "Bearer test-token" {
				t.Errorf("commit authorization=%q", request.Header.Get("Authorization"))
			}
			var body struct {
				Requirements []json.RawMessage `json:"requirements"`
				Updates      []json.RawMessage `json:"updates"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode commit: %v", err)
			}
			code := int(status.Load())
			response.WriteHeader(code)
			_ = json.NewEncoder(response).Encode(map[string]any{"error": map[string]any{
				"message": http.StatusText(code), "type": "CommitFailedException", "code": code,
			}})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	certificate, err := x509.ParseCertificate(server.Certificate().Raw)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testIcebergConfig()
	cfg.URI = server.URL
	cfg.Warehouse = "warehouse"
	cfg.OAuthToken = "test-token"
	cfg.AllowHTTP = false
	cfg.CAData = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}))
	catalog, err := newRESTCatalog(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = catalog.CommitTable(context.Background(), table.Identifier{"lake", "events"}, nil, nil)
	if !errors.Is(err, icerest.ErrCommitFailed) {
		t.Fatalf("409 error=%v, want optimistic conflict", err)
	}
	status.Store(http.StatusInternalServerError)
	_, _, err = catalog.CommitTable(context.Background(), table.Identifier{"lake", "events"}, nil, nil)
	if !errors.Is(err, icerest.ErrCommitStateUnknown) {
		t.Fatalf("500 error=%v, want unknown commit state", err)
	}
}

func TestRESTCatalogConformanceTLSAndTimeout(t *testing.T) {
	t.Parallel()
	t.Run("trusted custom CA", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if request.URL.Path != "/v1/config" {
				http.NotFound(response, request)
				return
			}
			_ = json.NewEncoder(response).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})
		}))
		defer server.Close()
		certificate, err := x509.ParseCertificate(server.Certificate().Raw)
		if err != nil {
			t.Fatal(err)
		}
		ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
		cfg := testIcebergConfig()
		cfg.URI = server.URL
		cfg.AllowHTTP = false
		cfg.CAData = string(ca)
		if _, err := newRESTCatalog(context.Background(), cfg); err != nil {
			t.Fatalf("custom CA catalog: %v", err)
		}
	})

	t.Run("request timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			time.Sleep(150 * time.Millisecond)
			_ = json.NewEncoder(response).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})
		}))
		defer server.Close()
		cfg := testIcebergConfig()
		cfg.URI = server.URL
		cfg.RequestTimeout = 20 * time.Millisecond
		started := time.Now()
		if _, err := newRESTCatalog(context.Background(), cfg); err == nil {
			t.Fatal("slow REST config request unexpectedly succeeded")
		}
		if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
			t.Fatalf("timeout elapsed=%s", elapsed)
		}
	})
}

func TestRESTCatalogConformanceSigV4(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/config" {
			http.NotFound(response, request)
			return
		}
		if authorization := request.Header.Get("Authorization"); !strings.HasPrefix(authorization, "AWS4-HMAC-SHA256 ") || !strings.Contains(authorization, "/glue/aws4_request") {
			t.Errorf("SigV4 authorization=%q", authorization)
		}
		if request.Header.Get("X-Amz-Content-Sha256") == "" {
			t.Error("SigV4 content hash is missing")
		}
		_ = json.NewEncoder(response).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})
	}))
	defer server.Close()
	certificate, err := x509.ParseCertificate(server.Certificate().Raw)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testIcebergConfig()
	cfg.URI = server.URL
	cfg.SigV4 = true
	cfg.Region = "us-east-1"
	cfg.SigningName = "glue"
	cfg.AllowHTTP = false
	cfg.CAData = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}))
	if _, err := newRESTCatalog(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}
