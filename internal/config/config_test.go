package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadIcebergCatalogConfig(t *testing.T) {
	t.Setenv("WALLABY_ENV", "test")
	t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
	t.Setenv("WALLABY_ICEBERG_URI", "https://catalog.example.test")
	t.Setenv("WALLABY_ICEBERG_WAREHOUSE", "warehouse")
	t.Setenv("WALLABY_ICEBERG_OAUTH_TOKEN", "secret-token")
	t.Setenv("WALLABY_ICEBERG_REQUEST_TIMEOUT", "12s")
	t.Setenv("WALLABY_ICEBERG_RECONCILIATION_HORIZON", "48h")
	t.Setenv("WALLABY_ICEBERG_S3TABLES_TABLE_BUCKET_ARN", "arn:aws:s3tables:us-east-1:123456789012:bucket/wallaby")
	t.Setenv("WALLABY_ICEBERG_S3TABLES_MIN_SNAPSHOTS_TO_KEEP", "250")

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Iceberg.URI != "https://catalog.example.test" || cfg.Iceberg.Warehouse != "warehouse" ||
		cfg.Iceberg.OAuthToken != "secret-token" || cfg.Iceberg.RequestTimeout != 12*time.Second ||
		cfg.Iceberg.ReconciliationHorizon != 48*time.Hour || cfg.Iceberg.S3TablesMinSnapshotsToKeep != 250 {
		t.Fatalf("iceberg config=%+v", cfg.Iceberg)
	}
}

func TestLoadArtifactPublicationConfig(t *testing.T) {
	t.Setenv("WALLABY_ENV", "test")
	t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
	t.Setenv("WALLABY_ARTIFACT_BUCKET", "canonical")
	t.Setenv("WALLABY_ARTIFACT_ENDPOINT", "http://minio:9000")
	t.Setenv("WALLABY_ARTIFACT_FORCE_PATH_STYLE", "true")
	t.Setenv("WALLABY_ARTIFACT_HARD_RETAINED_BYTES", "1048576")
	t.Setenv("WALLABY_ARTIFACT_BACKLOG_BATCH_HIGH", "12")
	t.Setenv("WALLABY_ARTIFACT_BACKLOG_BYTES_HIGH", "524288")
	t.Setenv("WALLABY_ARTIFACT_BACKLOG_AGE_HIGH", "2h")
	t.Setenv("WALLABY_ARTIFACT_ORPHAN_GRACE", "15m")
	t.Setenv("WALLABY_ARTIFACT_RETENTION", "48h")

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Artifacts.Bucket != "canonical" || !cfg.Artifacts.ForcePathStyle ||
		cfg.Artifacts.HardRetainedBytes != 1048576 || cfg.Artifacts.BacklogBatchHigh != 12 ||
		cfg.Artifacts.BacklogBytesHigh != 524288 || cfg.Artifacts.BacklogAgeHigh != 2*time.Hour ||
		cfg.Artifacts.OrphanGrace != 15*time.Minute || cfg.Artifacts.Retention != 48*time.Hour {
		t.Fatalf("artifact config=%+v", cfg.Artifacts)
	}
}

func TestLoadIndependentTelemetryEndpoints(t *testing.T) {
	t.Setenv("WALLABY_ENV", "test")
	t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "metrics.example:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://traces.example:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Telemetry.MetricsEndpoint != "metrics.example:4317" ||
		cfg.Telemetry.TracesEndpoint != "https://traces.example:4318" {
		t.Fatalf("signal endpoints not independent: %+v", cfg.Telemetry)
	}
	if cfg.Telemetry.MetricsProtocol != "grpc" || cfg.Telemetry.TracesProtocol != "http/protobuf" {
		t.Fatalf("signal protocols not independent: %+v", cfg.Telemetry)
	}
}

func TestTelemetryExporterRequiresItsOwnEndpoint(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Environment: "test",
		Workflow:    WorkflowConfig{Store: "memory"},
		Telemetry: TelemetryConfig{
			OTLPProtocol:    "grpc",
			MetricsProtocol: "grpc",
			TracesEndpoint:  "traces.example:4317",
			TracesProtocol:  "grpc",
			MetricsExporter: "otlp",
			TracesExporter:  "otlp",
			MetricsInterval: time.Second,
		},
		Kubernetes: KubernetesConfig{JobImagePullPolicy: "IfNotPresent"},
		DDL:        DDLConfig{CatalogInterval: time.Second},
	}
	err := validateConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "metrics endpoint is required") {
		t.Fatalf("validateConfig() error = %v, want metrics endpoint requirement", err)
	}
}

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
