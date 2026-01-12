package tests

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	httpdest "github.com/josephjohncox/wallaby/connectors/destinations/http"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestHTTPDestinationRecordJSON(t *testing.T) {
	baseURL := strings.TrimSpace(os.Getenv("WALLABY_TEST_HTTP_URL"))
	if baseURL == "" {
		t.Skip("WALLABY_TEST_HTTP_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dest := &httpdest.Destination{}
	spec := connector.Spec{
		Name: "http-test",
		Type: connector.EndpointHTTP,
		Options: map[string]string{
			"url":                strings.TrimRight(baseURL, "/") + "/capture",
			"payload_mode":       "record_json",
			"idempotency_header": "Idempotency-Key",
			"timeout":            "3s",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open http destination: %v", err)
	}
	defer dest.Close(ctx)

	now := time.Now().UTC()
	batch := connector.Batch{
		Schema: connector.Schema{
			Name:      "orders",
			Namespace: "public",
			Version:   1,
			Columns: []connector.Column{
				{Name: "id", Type: "int8"},
				{Name: "status", Type: "text"},
			},
		},
		Checkpoint: connector.Checkpoint{LSN: "0/1", Timestamp: now},
		Records: []connector.Record{
			{
				Table:         "public.orders",
				Operation:     connector.OpInsert,
				SchemaVersion: 1,
				Key:           []byte("order-1"),
				After: map[string]any{
					"id":     1,
					"status": "paid",
				},
				Timestamp: now,
			},
		},
	}

	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write http batch: %v", err)
	}

	last, err := waitForHTTPRecord(ctx, baseURL, "/capture")
	if err != nil {
		t.Fatalf("fetch http capture: %v", err)
	}

	body, err := base64.StdEncoding.DecodeString(last.BodyBase64)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode record json: %v", err)
	}
	if payload["table"] != "public.orders" {
		t.Fatalf("expected table public.orders, got %v", payload["table"])
	}
	if payload["operation"] != string(connector.OpInsert) {
		t.Fatalf("expected insert operation, got %v", payload["operation"])
	}

	expectedKey := buildIdempotencyKey(batch.Records[0], batch.Checkpoint.LSN)
	gotKey := headerLookup(last.Headers, "Idempotency-Key")
	if gotKey == "" {
		t.Fatalf("missing Idempotency-Key header: %v", last.Headers)
	}
	if gotKey != expectedKey {
		t.Fatalf("unexpected idempotency key: %s", gotKey)
	}
}

type httpCapture struct {
	Method     string            `json:"method"`
	Path       string            `json:"path"`
	Headers    map[string]string `json:"headers"`
	BodyBase64 string            `json:"body_base64"`
}

func waitForHTTPRecord(ctx context.Context, baseURL, path string) (httpCapture, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		capture, err := fetchHTTPRecord(baseURL)
		if err == nil && capture.Path == path {
			return capture, nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return httpCapture{}, err
			}
			return httpCapture{}, fmt.Errorf("timed out waiting for http capture")
		}
		select {
		case <-ctx.Done():
			return httpCapture{}, ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

func fetchHTTPRecord(baseURL string) (httpCapture, error) {
	url := strings.TrimRight(baseURL, "/") + "/last"
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return httpCapture{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return httpCapture{}, fmt.Errorf("http test status %d", resp.StatusCode)
	}
	var capture httpCapture
	if err := json.NewDecoder(resp.Body).Decode(&capture); err != nil {
		return httpCapture{}, err
	}
	return capture, nil
}

func buildIdempotencyKey(record connector.Record, lsn string) string {
	keyPart := string(record.Key)
	base := fmt.Sprintf("%s|%s|%s", record.Table, keyPart, lsn)
	sum := sha256.Sum256([]byte(base))
	return hex.EncodeToString(sum[:])
}

func headerLookup(headers map[string]string, key string) string {
	for k, v := range headers {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}
