package schemaregistry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConfluentRegistryRegister(t *testing.T) {
	var gotPath string
	var gotAuth string
	var gotContentType string
	var gotPayload confluentRegisterRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotPayload)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":42,"version":3}`))
	}))
	defer srv.Close()

	reg, err := newConfluentRegistry(Config{
		URL:      srv.URL,
		Username: "user",
		Password: "pass",
	})
	if err != nil {
		t.Fatalf("new confluent registry: %v", err)
	}
	defer reg.Close()

	res, err := reg.Register(context.Background(), RegisterRequest{
		Subject:    "wallaby.test",
		Schema:     `{"type":"record","name":"t"}`,
		SchemaType: SchemaTypeAvro,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if res.ID != "42" || res.Version != 3 {
		t.Fatalf("unexpected result: %#v", res)
	}
	if gotPath != "/subjects/wallaby.test/versions" {
		t.Fatalf("unexpected path: %s", gotPath)
	}
	if gotContentType != "application/vnd.schemaregistry.v1+json" {
		t.Fatalf("unexpected content type: %s", gotContentType)
	}
	if !strings.HasPrefix(gotAuth, "Basic ") {
		t.Fatalf("expected basic auth, got %q", gotAuth)
	}
	if gotPayload.Schema == "" || gotPayload.SchemaType != SchemaTypeAvro {
		t.Fatalf("unexpected payload: %#v", gotPayload)
	}
}

func TestApicurioCompatRegister(t *testing.T) {
	var gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":7,"version":1}`))
	}))
	defer srv.Close()

	reg, err := newApicurioRegistry(Config{
		URL:            srv.URL,
		ApicurioCompat: true,
	})
	if err != nil {
		t.Fatalf("new apicurio registry: %v", err)
	}
	defer reg.Close()

	_, err = reg.Register(context.Background(), RegisterRequest{
		Subject:    "wallaby.proto",
		Schema:     `syntax = "proto3"; message Test {}`,
		SchemaType: SchemaTypeProtobuf,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if gotPath != "/apis/ccompat/v7/subjects/wallaby.proto/versions" {
		t.Fatalf("unexpected path: %s", gotPath)
	}
}
