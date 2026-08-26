package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSnowflakeConfigDefaultEnvAndStrictFilePrecedence(t *testing.T) {
	for _, key := range []string{"WALLABY_SNOWFLAKE_ENABLED", "WALLABY_WORKER_SNOWFLAKE_ENABLED", "WALLABY_SNOWFLAKE_ACCOUNT", "WALLABY_WORKER_SNOWFLAKE_ACCOUNT", "WALLABY_SNOWFLAKE_USER", "WALLABY_WORKER_SNOWFLAKE_USER", "WALLABY_SNOWFLAKE_HOST", "WALLABY_WORKER_SNOWFLAKE_HOST", "WALLABY_SNOWFLAKE_PRIVATE_KEY_FILE", "WALLABY_WORKER_SNOWFLAKE_PRIVATE_KEY_FILE", "WALLABY_SNOWFLAKE_PRIVATE_KEY_SECRET_NAME", "WALLABY_SNOWFLAKE_PRIVATE_KEY_SECRET_KEY"} {
		key := key
		old, existed := os.LookupEnv(key)
		_ = os.Unsetenv(key)
		t.Cleanup(func() {
			if existed {
				_ = os.Setenv(key, old)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
	t.Setenv("WALLABY_ENV", "test")
	t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Snowflake.Enabled || cfg.Snowflake.PrivateKeyFile != "" {
		t.Fatalf("default Snowflake config=%+v", cfg.Snowflake)
	}
	cfg.Snowflake.Enabled = true
	if err := cfg.Snowflake.ValidateExecution(); err == nil {
		t.Fatal("enabled Snowflake execution without a deployment key was accepted")
	}
	cfg.Snowflake.Enabled = false
	t.Setenv("WALLABY_SNOWFLAKE_ENABLED", "true")
	t.Setenv("WALLABY_SNOWFLAKE_ACCOUNT", "account")
	t.Setenv("WALLABY_SNOWFLAKE_USER", "user")
	t.Setenv("WALLABY_SNOWFLAKE_HOST", "account.snowflakecomputing.com")
	t.Setenv("WALLABY_SNOWFLAKE_PRIVATE_KEY_FILE", "/env/key.pem")
	cfg, err = Load("")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Snowflake.Enabled || cfg.Snowflake.Account != "account" || cfg.Snowflake.User != "user" || cfg.Snowflake.Host != "account.snowflakecomputing.com" || cfg.Snowflake.PrivateKeyFile != "/env/key.pem" {
		t.Fatalf("environment Snowflake config=%+v", cfg.Snowflake)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("environment: test\nworkflow:\n  store: memory\nsnowflake:\n  enabled: false\n  account: file-account\n  user: file-user\n  host: file.snowflakecomputing.com\n  private_key_file: /file/key.pem\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Snowflake.Enabled || cfg.Snowflake.Account != "file-account" || cfg.Snowflake.User != "file-user" || cfg.Snowflake.Host != "file.snowflakecomputing.com" || cfg.Snowflake.PrivateKeyFile != "/file/key.pem" {
		t.Fatalf("file precedence Snowflake config=%+v", cfg.Snowflake)
	}
	bad := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(bad, []byte("environment: test\nworkflow:\n  store: memory\nsnowflake:\n  enabled: false\n  unknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(bad); err == nil || !strings.Contains(err.Error(), "snowflake.unknown") {
		t.Fatalf("strict Snowflake config error=%v", err)
	}
}

func TestDocumentedSemanticDefaultsRemainOrdinaryDefaults(t *testing.T) {
	t.Setenv("WALLABY_ENV", "test")
	t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
	for _, key := range []string{
		"WALLABY_GRPC_LISTEN", "WALLABY_GRPC_REFLECTION", "WALLABY_WIRE_ENFORCE",
		"WALLABY_DDL_CATALOG_INTERVAL", "WALLABY_DDL_AUTO_APPROVE", "WALLABY_DDL_GATE", "WALLABY_DDL_AUTO_APPLY",
		"WALLABY_OTEL_METRICS_INTERVAL", "WALLABY_DBOS_MAX_EMPTY_READS",
	} {
		key := key
		old, existed := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if existed {
				_ = os.Setenv(key, old)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.API.GRPCListen != ":8080" || cfg.API.GRPCReflection || !cfg.Wire.Enforce {
		t.Fatalf("API/wire defaults=%+v/%+v", cfg.API, cfg.Wire)
	}
	if cfg.DDL.CatalogInterval != 30*time.Second || !cfg.DDL.AutoApprove || cfg.DDL.Gate || !cfg.DDL.AutoApply {
		t.Fatalf("DDL defaults=%+v", cfg.DDL)
	}
	if cfg.Telemetry.MetricsInterval != 30*time.Second || cfg.DBOS.MaxEmptyReads != 1 {
		t.Fatalf("telemetry/DBOS defaults=%+v/%+v", cfg.Telemetry, cfg.DBOS)
	}
	if cfg.Artifacts.MetadataRetention != 7*24*time.Hour || cfg.Artifacts.MetadataMaxPublications != 100 || cfg.Artifacts.MetadataMaxRows != 1000 {
		t.Fatalf("artifact metadata defaults=%+v", cfg.Artifacts)
	}
}

func TestShippedWorkerConfigurationExampleLoads(t *testing.T) {
	t.Setenv("WALLABY_ENV", "test")
	t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
	cfg, err := Load("../../examples/config/postgres_to_iceberg_s3tables.worker.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Iceberg.Profile != "s3tables" || cfg.Artifacts.Bucket != "wallaby-canonical-artifacts" {
		t.Fatalf("loaded example config=%+v", cfg)
	}
}

func TestLoadIcebergCatalogConfig(t *testing.T) {
	t.Setenv("WALLABY_ENV", "test")
	t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
	t.Setenv("WALLABY_ICEBERG_PROFILE", "rest")
	t.Setenv("WALLABY_ICEBERG_URI", "https://catalog.example.test")
	t.Setenv("WALLABY_ICEBERG_WAREHOUSE", "warehouse")
	t.Setenv("WALLABY_ICEBERG_OAUTH_TOKEN", "secret-token")
	t.Setenv("WALLABY_ICEBERG_S3_ENDPOINT", "https://s3.example.test")
	t.Setenv("WALLABY_ICEBERG_S3_REGION", "us-east-1")
	t.Setenv("WALLABY_ICEBERG_EXPECTED_AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/wallaby")
	t.Setenv("WALLABY_ICEBERG_REQUEST_TIMEOUT", "12s")
	t.Setenv("WALLABY_ICEBERG_RECONCILIATION_HORIZON", "48h")
	t.Setenv("WALLABY_ICEBERG_S3TABLES_TABLE_BUCKET_ARN", "arn:aws:s3tables:us-east-1:123456789012:bucket/wallaby")
	t.Setenv("WALLABY_ICEBERG_S3TABLES_MIN_SNAPSHOTS_TO_KEEP", "250")

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Iceberg.Profile != "rest" || cfg.Iceberg.URI != "https://catalog.example.test" || cfg.Iceberg.Warehouse != "warehouse" ||
		cfg.Iceberg.OAuthToken != "secret-token" || cfg.Iceberg.S3Endpoint != "https://s3.example.test" || cfg.Iceberg.S3Region != "us-east-1" || cfg.Iceberg.ExpectedAWSRoleARN != "arn:aws:iam::123456789012:role/wallaby" || cfg.Iceberg.RequestTimeout != 12*time.Second ||
		cfg.Iceberg.ReconciliationHorizon != 48*time.Hour || cfg.Iceberg.S3TablesMinSnapshotsToKeep != 250 {
		t.Fatalf("iceberg config=%+v", cfg.Iceberg)
	}
}

func TestLoadIcebergExpectedAWSRoleFromWorkerYAML(t *testing.T) {
	t.Setenv("WALLABY_ICEBERG_EXPECTED_AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/environment-role")
	path := filepath.Join(t.TempDir(), "wallaby-worker.yaml")
	if err := os.WriteFile(path, []byte("environment: test\nworkflow:\n  store: memory\niceberg:\n  expected_aws_role_arn: arn:aws:iam::123456789012:role/file-role\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Iceberg.ExpectedAWSRoleARN != "arn:aws:iam::123456789012:role/file-role" {
		t.Fatalf("expected AWS role ARN=%q", cfg.Iceberg.ExpectedAWSRoleARN)
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
	t.Setenv("WALLABY_ARTIFACT_METADATA_RETENTION", "72h")
	t.Setenv("WALLABY_ARTIFACT_METADATA_MAX_PUBLICATIONS", "7")
	t.Setenv("WALLABY_ARTIFACT_METADATA_MAX_ROWS", "19")

	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Artifacts.Bucket != "canonical" || !cfg.Artifacts.ForcePathStyle ||
		cfg.Artifacts.HardRetainedBytes != 1048576 || cfg.Artifacts.BacklogBatchHigh != 12 ||
		cfg.Artifacts.BacklogBytesHigh != 524288 || cfg.Artifacts.BacklogAgeHigh != 2*time.Hour ||
		cfg.Artifacts.OrphanGrace != 15*time.Minute || cfg.Artifacts.Retention != 48*time.Hour ||
		cfg.Artifacts.MetadataRetention != 72*time.Hour || cfg.Artifacts.MetadataMaxPublications != 7 || cfg.Artifacts.MetadataMaxRows != 19 {
		t.Fatalf("artifact config=%+v", cfg.Artifacts)
	}
}

func TestArtifactMetadataRetentionRejectsInvalidLimits(t *testing.T) {
	for _, test := range []struct {
		name, key, value string
	}{
		{name: "retention zero", key: "WALLABY_ARTIFACT_METADATA_RETENTION", value: "0s"},
		{name: "publication limit negative", key: "WALLABY_ARTIFACT_METADATA_MAX_PUBLICATIONS", value: "-1"},
		{name: "row limit zero", key: "WALLABY_ARTIFACT_METADATA_MAX_ROWS", value: "0"},
		{name: "row limit below atomic minimum", key: "WALLABY_ARTIFACT_METADATA_MAX_ROWS", value: "2"},
		{name: "worker row limit below atomic minimum", key: "WALLABY_WORKER_ARTIFACT_METADATA_MAX_ROWS", value: "2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("WALLABY_ENV", "test")
			t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
			t.Setenv("WALLABY_ARTIFACT_BUCKET", "canonical")
			t.Setenv(test.key, test.value)
			if _, err := Load(""); err == nil || !strings.Contains(err.Error(), "artifact") {
				t.Fatalf("Load() error=%v, want metadata validation", err)
			}
		})
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
