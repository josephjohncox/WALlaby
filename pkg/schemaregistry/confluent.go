package schemaregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type confluentRegistry struct {
	baseURL string
	client  *http.Client
	user    string
	pass    string
	token   string
}

type confluentRegisterRequest struct {
	Schema     string      `json:"schema"`
	SchemaType SchemaType  `json:"schemaType,omitempty"`
	References []Reference `json:"references,omitempty"`
}

type confluentRegisterResponse struct {
	ID      int `json:"id"`
	Version int `json:"version"`
}

func newConfluentRegistry(cfg Config) (*confluentRegistry, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("schema_registry_url is required for confluent registry")
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	return &confluentRegistry{
		baseURL: strings.TrimSuffix(cfg.URL, "/"),
		client:  &http.Client{Timeout: timeout},
		user:    cfg.Username,
		pass:    cfg.Password,
		token:   cfg.Token,
	}, nil
}

func newApicurioRegistry(cfg Config) (*confluentRegistry, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("schema_registry_url is required for apicurio registry")
	}
	base := strings.TrimSuffix(cfg.URL, "/")
	if cfg.ApicurioCompat && !strings.Contains(base, "/apis/ccompat/") {
		base += "/apis/ccompat/v7"
	}
	cfg.URL = base
	return newConfluentRegistry(cfg)
}

func (r *confluentRegistry) Register(ctx context.Context, req RegisterRequest) (RegisterResult, error) {
	if req.Subject == "" {
		return RegisterResult{}, fmt.Errorf("schema registry subject is required")
	}
	if req.Schema == "" {
		return RegisterResult{}, fmt.Errorf("schema registry schema is required")
	}
	payload := confluentRegisterRequest{
		Schema:     req.Schema,
		SchemaType: req.SchemaType,
		References: req.References,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("marshal registry payload: %w", err)
	}
	url := fmt.Sprintf("%s/subjects/%s/versions", r.baseURL, req.Subject)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return RegisterResult{}, fmt.Errorf("create registry request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	if r.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+r.token)
	} else if r.user != "" {
		httpReq.SetBasicAuth(r.user, r.pass)
	}
	resp, err := r.client.Do(httpReq)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("register schema: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return RegisterResult{}, fmt.Errorf("read registry response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return RegisterResult{}, fmt.Errorf("registry error (%d): %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var decoded confluentRegisterResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		return RegisterResult{}, fmt.Errorf("decode registry response: %w", err)
	}
	return RegisterResult{
		ID:      fmt.Sprintf("%d", decoded.ID),
		Version: decoded.Version,
	}, nil
}

func (r *confluentRegistry) Close() error { return nil }
