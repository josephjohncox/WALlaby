package config

import (
	"strings"
	"testing"
	"time"
)

func TestWorkflowStoreValidation(t *testing.T) {
	t.Parallel()

	validConfig := func() *Config {
		return &Config{
			Environment: "dev",
			Postgres:    PostgresConfig{DSN: "postgres://example"},
			Workflow:    WorkflowConfig{Store: "postgres"},
			Telemetry: TelemetryConfig{
				OTLPProtocol:    "grpc",
				MetricsExporter: "none",
				TracesExporter:  "none",
				MetricsInterval: time.Second,
			},
			Kubernetes: KubernetesConfig{JobImagePullPolicy: "IfNotPresent"},
			DDL:        DDLConfig{CatalogInterval: time.Second},
		}
	}

	t.Run("postgres requires dsn", func(t *testing.T) {
		cfg := validConfig()
		cfg.Postgres.DSN = ""
		err := validateConfig(cfg)
		if err == nil || !strings.Contains(err.Error(), "postgres dsn is required") {
			t.Fatalf("validateConfig() error = %v, want missing postgres dsn", err)
		}
	})

	t.Run("memory allowed in development", func(t *testing.T) {
		cfg := validConfig()
		cfg.Workflow.Store = "memory"
		cfg.Postgres.DSN = ""
		if err := validateConfig(cfg); err != nil {
			t.Fatalf("validateConfig() error = %v", err)
		}
	})

	t.Run("memory rejected with durable dispatch", func(t *testing.T) {
		cfg := validConfig()
		cfg.Workflow.Store = "memory"
		cfg.Postgres.DSN = ""
		cfg.Kubernetes.Enabled = true
		err := validateConfig(cfg)
		if err == nil || !strings.Contains(err.Error(), "cannot be used with DBOS or Kubernetes") {
			t.Fatalf("validateConfig() error = %v, want memory dispatch rejection", err)
		}
	})

	t.Run("memory rejected in production", func(t *testing.T) {
		cfg := validConfig()
		cfg.Environment = "production"
		cfg.Workflow.Store = "memory"
		err := validateConfig(cfg)
		if err == nil || !strings.Contains(err.Error(), "allowed only in dev") {
			t.Fatalf("validateConfig() error = %v, want production memory rejection", err)
		}
	})
}
