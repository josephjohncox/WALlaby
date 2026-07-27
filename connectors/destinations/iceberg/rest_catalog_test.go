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

func TestRESTCatalogConformanceAuthConflictAndUnknownCommit(t *testing.T) {
	t.Parallel()
	var status atomic.Int32
	status.Store(http.StatusConflict)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
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

	cfg := testIcebergConfig()
	cfg.URI = server.URL
	cfg.Warehouse = "warehouse"
	cfg.OAuthToken = "test-token"
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
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
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
	cfg := testIcebergConfig()
	cfg.URI = server.URL
	cfg.SigV4 = true
	cfg.Region = "us-east-1"
	cfg.SigningName = "glue"
	if _, err := newRESTCatalog(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}
