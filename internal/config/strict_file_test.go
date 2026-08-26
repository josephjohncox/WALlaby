package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStrictCurrentFileSchemaAcceptsEveryKey(t *testing.T) {
	path := writeConfigTestFile(t, "current.yaml", `environment: test
api:
  grpc_listen: ":9090"
  grpc_reflection: true
postgres:
  dsn: ""
workflow:
  store: memory
telemetry:
  service_name: strict-wallaby
  otlp_endpoint: collector:4317
  otlp_insecure: true
  otlp_protocol: grpc
  metrics_endpoint: metrics:4317
  metrics_insecure: true
  metrics_protocol: grpc
  traces_endpoint: traces:4318
  traces_insecure: false
  traces_protocol: http/protobuf
  metrics_exporter: none
  traces_exporter: none
  metrics_interval: 5s
trace:
  path: /tmp/wallaby-trace.jsonl
profiling:
  enabled: true
  listen: ":6061"
dbos:
  enabled: false
  app_name: strict-app
  schedule: ""
  queue: strict-queue
  max_empty_reads: 2
  max_retries: 3
kubernetes:
  enabled: false
  kubeconfig_path: /tmp/kubeconfig
  context: strict-context
  api_server: https://kubernetes.example
  bearer_token: token
  ca_file: /tmp/ca
  ca_data: ca-data
  client_cert_file: /tmp/cert
  client_key_file: /tmp/key
  insecure_skip_tls: false
  namespace: wallaby
  job_image: wallaby:test
  job_image_pull_policy: IfNotPresent
  job_service_account: wallaby
  job_automount_service_account_token: false
  job_name_prefix: strict-worker
  job_ttl_seconds: 30
  job_backoff_limit: 2
  job_max_empty_reads: 4
  job_labels:
    app: wallaby
  job_annotations:
    owner: tests
  job_command: [wallaby-worker]
  job_args: ["--run-once"]
  job_env:
    CURRENT: value
  job_env_from: [wallaby-secret]
wire:
  format: json
  enforce: true
ddl:
  catalog_enabled: false
  catalog_interval: 10s
  catalog_schemas: [public, audit]
  auto_approve: true
  gate: false
  auto_apply: true
checkpoints:
  backend: sqlite
  dsn: ""
  path: /tmp/checkpoints.db
artifacts:
  bucket: canonical
  region: us-east-1
  endpoint: https://s3.example
  access_key: access
  secret_key: secret
  session_token: session
  force_path_style: false
  hard_retained_bytes: 1024
  backlog_batch_high: 10
  backlog_bytes_high: 2048
  backlog_age_high: 1h
  backpressure_poll_interval: 1s
  orphan_grace: 2h
  retention: 24h
  metadata_retention: 168h
  metadata_max_publications: 100
  metadata_max_rows: 1000
  gc_interval: 1m
iceberg:
  profile: rest
  uri: https://catalog.example
  warehouse: warehouse
  prefix: prefix
  control_table: __wallaby_control
  region: us-east-1
  signing_name: execute-api
  expected_aws_role_arn: arn:aws:iam::123456789012:role/wallaby
  sigv4: true
  allow_http: false
  oauth_token: token
  oauth_credential: credential
  oauth_scope: scope
  oauth_uri: https://oauth.example
  ca_file: /tmp/iceberg-ca
  ca_data: iceberg-ca
  client_cert_file: /tmp/iceberg-cert
  client_key_file: /tmp/iceberg-key
  server_name: catalog.example
  s3_endpoint: https://s3.example
  s3_region: us-east-1
  max_commit_retries: 5
  request_timeout: 15s
  reconciliation_horizon: 48h
  s3tables_table_bucket_arn: arn:aws:s3tables:us-east-1:123456789012:bucket/wallaby
  s3tables_configure_maintenance: true
  s3tables_min_snapshots_to_keep: 200
  s3tables_max_snapshot_age_hours: 48
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.API.GRPCListen != ":9090" || !cfg.Profiling.Enabled || cfg.Profiling.Listen != ":6061" || cfg.DBOS.MaxRetries != 3 || !cfg.DBOS.MaxRetriesSet || cfg.Kubernetes.BearerToken != "token" || cfg.Artifacts.Retention != 24*time.Hour || cfg.Iceberg.RequestTimeout != 15*time.Second {
		t.Fatalf("current config not retained: %+v", cfg)
	}
}

func TestStrictConfigRejectsEveryRemovedFileAlias(t *testing.T) {
	aliases := []string{
		"api.grpc-listen", "api.grpc-reflection", "workflow_store",
		"telemetry.service-name", "telemetry.otlp-endpoint", "telemetry.endpoint", "telemetry.otel_endpoint", "telemetry.otlp-insecure", "telemetry.otlp-protocol", "telemetry.metrics-endpoint", "telemetry.metrics-insecure", "telemetry.metrics-protocol", "telemetry.traces-endpoint", "telemetry.traces-insecure", "telemetry.traces-protocol", "telemetry.metrics-exporter", "telemetry.traces-exporter", "telemetry.metrics-interval",
		"trace.file", "profiling.pprof", "dbos.dispatcher_enabled", "dbos.app-name", "dbos.cron", "dbos.max-empty-reads", "dbos.max-retries",
		"k8s.enabled", "k8s.kubeconfig_path", "k8s.kubeconfig-path", "k8s.kubeconfig", "k8s.context", "k8s.api_server", "k8s.api-server", "k8s.token", "k8s.bearer_token", "k8s.bearer-token", "k8s.ca_file", "k8s.ca-file", "k8s.ca_data", "k8s.ca-data", "k8s.client_cert_file", "k8s.client-cert-file", "k8s.client_key_file", "k8s.client-key-file", "k8s.insecure_skip_tls", "k8s.insecure-skip-tls", "k8s.namespace", "k8s.job_image", "k8s.job-image", "k8s.job_image_pull_policy", "k8s.job-image-pull-policy", "k8s.job_service_account", "k8s.job-service-account", "k8s.job_name_prefix", "k8s.job-name-prefix", "k8s.job_ttl_seconds", "k8s.job-ttl-seconds", "k8s.job_backoff_limit", "k8s.job-backoff-limit", "k8s.job_max_empty_reads", "k8s.job-max-empty-reads", "k8s.job_labels", "k8s.job-labels", "k8s.job_annotations", "k8s.job-annotations", "k8s.job_command", "k8s.job-command", "k8s.job_args", "k8s.job-args", "k8s.job_env", "k8s.job-env", "k8s.job_env_from", "k8s.job-env-from",
		"kubernetes.dispatcher_enabled", "kubernetes.kubeconfig", "kubernetes.kubeconfig-path", "kubernetes.api-server", "kubernetes.token", "kubernetes.bearer-token", "kubernetes.ca-file", "kubernetes.ca-data", "kubernetes.client-cert-file", "kubernetes.client-key-file", "kubernetes.insecure-skip-tls", "kubernetes.job-image", "kubernetes.job-image-pull-policy", "kubernetes.job-service-account", "kubernetes.job-automount-service-account-token", "kubernetes.job-name-prefix", "kubernetes.job-ttl-seconds", "kubernetes.job-backoff-limit", "kubernetes.job-max-empty-reads", "kubernetes.job-labels", "kubernetes.job-annotations", "kubernetes.job-command", "kubernetes.job-args", "kubernetes.job-env", "kubernetes.job-env-from",
		"wire.default_format", "wire.default-format", "wire.enforce_format", "wire.enforce-format",
		"ddl.catalog-enabled", "ddl.catalog-interval", "ddl.catalog-schemas", "ddl.auto-approve", "ddl.auto-apply",
		"checkpoint.backend", "checkpoint.dsn", "checkpoint.path",
		"artifact.bucket", "artifact.region", "artifact.endpoint", "artifact.access_key", "artifact.secret_key", "artifact.session_token", "artifact.force_path_style", "artifact.hard_retained_bytes", "artifact.backlog_batch_high", "artifact.backlog_bytes_high", "artifact.backlog_age_high", "artifact.backpressure_poll_interval", "artifact.orphan_grace", "artifact.retention", "artifact.gc_interval",
	}
	for _, alias := range aliases {
		alias := alias
		t.Run(strings.ReplaceAll(alias, ".", "_"), func(t *testing.T) {
			path := writeConfigTestFile(t, "removed.yaml", yamlForConfigPath(alias, "true"))
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("unknown key %q", alias)) {
				t.Fatalf("Load(%s) error=%v", alias, err)
			}
		})
	}
}

func TestStrictConfigRejectsUnknownDuplicateMultiDocumentAndTypes(t *testing.T) {
	tests := []struct {
		name, extension, body, want string
	}{
		{"unknown nested", ".yaml", "api:\n  grpc_listenn: ':8080'\n", `unknown key "api.grpc_listenn"`},
		{"duplicate yaml", ".yaml", "api:\n  grpc_listen: ':8080'\n  grpc_listen: ':9090'\n", `duplicate key "api.grpc_listen"`},
		{"multi document", ".yaml", "environment: test\n---\nenvironment: dev\n", "expected exactly one YAML document"},
		{"bool type", ".yaml", "profiling:\n  enabled: 'true'\n", "profiling.enabled: expected bool, got str"},
		{"int type", ".yaml", "dbos:\n  max_retries: '3'\n", "dbos.max_retries: expected integer, got str"},
		{"duration type", ".yaml", "ddl:\n  catalog_interval: 30\n", "ddl.catalog_interval: expected duration string, got int"},
		{"unknown json", ".json", `{"api":{"grpc_listenn":":8080"}}`, `unknown key "api.grpc_listenn"`},
		{"duplicate json", ".json", `{"api":{"grpc_listen":":8080","grpc_listen":":9090"}}`, `duplicate key "api.grpc_listen"`},
		{"trailing json", ".json", `{} {}`, "expected exactly one JSON document"}, {"unsupported extension", ".toml", "environment = 'test'", "unsupported extension"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeConfigTestFile(t, "strict"+test.extension, test.body)
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want %q", err, test.want)
			}
		})
	}
}

func TestDocumentedWorkerEnvironmentKeysRemainCurrent(t *testing.T) {
	for _, key := range []string{"WALLABY_ENV", "WALLABY_WORKFLOW_STORE", "WALLABY_GRPC_LISTEN", "WALLABY_PPROF_ENABLED", "WALLABY_ARTIFACT_BUCKET", "WALLABY_ICEBERG_PROFILE"} {
		value, exists := os.LookupEnv(key)
		if exists {
			t.Cleanup(func() { _ = os.Setenv(key, value) })
		} else {
			t.Cleanup(func() { _ = os.Unsetenv(key) })
		}
		_ = os.Unsetenv(key)
	}
	t.Setenv("WALLABY_WORKER_ENV", "test")
	t.Setenv("WALLABY_WORKER_WORKFLOW_STORE", "memory")
	t.Setenv("WALLABY_WORKER_GRPC_LISTEN", ":7070")
	t.Setenv("WALLABY_WORKER_PPROF_ENABLED", "true")
	t.Setenv("WALLABY_WORKER_ARTIFACT_BUCKET", "worker-canonical")
	t.Setenv("WALLABY_WORKER_ARTIFACT_METADATA_RETENTION", "96h")
	t.Setenv("WALLABY_WORKER_ARTIFACT_METADATA_MAX_PUBLICATIONS", "9")
	t.Setenv("WALLABY_WORKER_ARTIFACT_METADATA_MAX_ROWS", "3")
	t.Setenv("WALLABY_WORKER_ICEBERG_PROFILE", "rest")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Environment != "test" || cfg.Workflow.Store != "memory" || cfg.API.GRPCListen != ":7070" || !cfg.Profiling.Enabled || cfg.Artifacts.Bucket != "worker-canonical" || cfg.Artifacts.MetadataRetention != 96*time.Hour || cfg.Artifacts.MetadataMaxPublications != 9 || cfg.Artifacts.MetadataMaxRows != 3 || cfg.Iceberg.Profile != "rest" {
		t.Fatalf("worker config=%+v", cfg)
	}
}

func TestStrictArtifactMetadataRowMinimumInYAMLAndJSON(t *testing.T) {
	for _, test := range []struct {
		name, extension, body string
	}{
		{name: "yaml", extension: ".yaml", body: "environment: test\nworkflow:\n  store: memory\nartifacts:\n  metadata_max_rows: 2\n"},
		{name: "json", extension: ".json", body: `{"environment":"test","workflow":{"store":"memory"},"artifacts":{"metadata_max_rows":2}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writeConfigTestFile(t, "metadata-min"+test.extension, test.body)
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "metadata_max_rows") {
				t.Fatalf("Load() error=%v, want metadata_max_rows minimum", err)
			}
		})
	}
}

func TestStrictArtifactMetadataNonDefaultLimitsInYAMLAndJSON(t *testing.T) {
	for _, test := range []struct {
		name, extension, body string
	}{
		{name: "yaml", extension: ".yaml", body: "environment: test\nworkflow:\n  store: memory\nartifacts:\n  metadata_retention: 72h\n  metadata_max_publications: 7\n  metadata_max_rows: 3\n"},
		{name: "json", extension: ".json", body: `{"environment":"test","workflow":{"store":"memory"},"artifacts":{"metadata_retention":"72h","metadata_max_publications":7,"metadata_max_rows":3}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writeConfigTestFile(t, "metadata-current"+test.extension, test.body)
			cfg, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Artifacts.MetadataRetention != 72*time.Hour || cfg.Artifacts.MetadataMaxPublications != 7 || cfg.Artifacts.MetadataMaxRows != 3 {
				t.Fatalf("metadata config=%+v", cfg.Artifacts)
			}
		})
	}
}

func TestStrictJSONCurrentSchema(t *testing.T) {
	path := writeConfigTestFile(t, "current.json", `{"environment":"test","workflow":{"store":"memory"},"profiling":{"enabled":true,"listen":":6060"},"ddl":{"catalog_interval":"30s"}}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Profiling.Enabled || cfg.DDL.CatalogInterval != 30*time.Second {
		t.Fatalf("json config=%+v", cfg)
	}
}

func TestStrictConfigFilePrecedenceAndDeprecatedEnvironmentAliases(t *testing.T) {
	t.Setenv("WALLABY_ENV", "production")
	t.Setenv("WALLABY_WORKER_ENV", "development")
	t.Setenv("WALLABY_WORKFLOW_STORE", "postgres")
	t.Setenv("WALLABY_WORKER_WORKFLOW_STORE", "postgres")
	t.Setenv("WALLABY_POSTGRES_DSN", "postgres://environment")
	t.Setenv("WALLABY_WORKER_GRPC_LISTEN", ":7070")
	t.Setenv("WALLABY_OTEL_SERVICE", "deprecated-service")
	t.Setenv("KUBECONFIG", "/deprecated/kubeconfig")
	path := writeConfigTestFile(t, "precedence.yaml", "environment: test\nworkflow:\n  store: memory\napi:\n  grpc_listen: ':9090'\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Environment != "test" || cfg.Workflow.Store != "memory" || cfg.API.GRPCListen != ":9090" {
		t.Fatalf("file did not override environment: %+v", cfg)
	}
	if cfg.Telemetry.ServiceName != "wallaby" || cfg.Kubernetes.KubeconfigPath != "" {
		t.Fatalf("deprecated environment alias was accepted: telemetry=%+v kubernetes=%+v", cfg.Telemetry, cfg.Kubernetes)
	}
}

func TestStrictDecoderRejectsInvalidUserPaths(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"", "invalid\x00.yaml"} {
		if _, err := decodeStrictConfigFile(path, &Config{}); err == nil {
			t.Fatalf("decodeStrictConfigFile(%q) accepted an invalid path", path)
		}
	}
}

func TestStrictDecoderTracksExactlyPresentSchemaLeaves(t *testing.T) {
	path := writeConfigTestFile(t, "presence.yaml", "environment: test\napi:\n  grpc_reflection: true\nkubernetes:\n  job_labels:\n    app: wallaby\n  job_command: [wallaby-worker]\n")
	present, err := decodeStrictConfigFile(path, &Config{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"environment", "api.grpc_reflection", "kubernetes.job_labels", "kubernetes.job_command"}
	if len(present) != len(want) {
		t.Fatalf("present=%v, want exactly %v", present, want)
	}
	for _, key := range want {
		if _, ok := present[key]; !ok {
			t.Fatalf("missing presence path %q in %v", key, present)
		}
	}
}

func TestStrictFilePresenceSuppressesInvalidLowerPrecedenceEnvironment(t *testing.T) {
	tests := []struct {
		name, body, envKey, envValue string
		check                        func(*Config) bool
	}{{"bool", "profiling:\n  enabled: true\n", "WALLABY_PPROF_ENABLED", "not-a-bool", func(cfg *Config) bool { return cfg.Profiling.Enabled }}, {"integer", "dbos:\n  max_empty_reads: 7\n", "WALLABY_DBOS_MAX_EMPTY_READS", "not-an-integer", func(cfg *Config) bool { return cfg.DBOS.MaxEmptyReads == 7 }}, {"duration", "telemetry:\n  metrics_interval: 5s\n", "WALLABY_OTEL_METRICS_INTERVAL", "not-a-duration", func(cfg *Config) bool { return cfg.Telemetry.MetricsInterval == 5*time.Second }}, {"list", "ddl:\n  catalog_schemas: [file_schema, audit]\n", "WALLABY_DDL_CATALOG_SCHEMAS", "environment_schema", func(cfg *Config) bool {
		return len(cfg.DDL.CatalogSchemas) == 2 && cfg.DDL.CatalogSchemas[0] == "file_schema"
	}}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("WALLABY_ENV", "test")
			t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
			t.Setenv(test.envKey, test.envValue)
			cfg, err := Load(writeConfigTestFile(t, "precedence.yaml", test.body))
			if err != nil {
				t.Fatal(err)
			}
			if !test.check(cfg) {
				t.Fatalf("file value was not retained: %+v", cfg)
			}
		})
	}
}

func TestStrictFileErrorsPrecedeEnvironmentParsing(t *testing.T) {
	t.Setenv("WALLABY_PPROF_ENABLED", "not-a-bool")
	_, err := Load(writeConfigTestFile(t, "invalid.yaml", "profiling:\n  enabledd: true\n"))
	if err == nil || !strings.Contains(err.Error(), `unknown key "profiling.enabledd"`) {
		t.Fatalf("error=%v", err)
	}
	if strings.Contains(err.Error(), "WALLABY_PPROF_ENABLED") {
		t.Fatalf("lower-precedence environment was parsed before file validation: %v", err)
	}
}

func TestEnvironmentListAppliesWhenFileFieldAbsent(t *testing.T) {
	t.Setenv("WALLABY_ENV", "test")
	t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
	t.Setenv("WALLABY_DDL_CATALOG_SCHEMAS", "public,audit")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.DDL.CatalogSchemas) != 2 || cfg.DDL.CatalogSchemas[1] != "audit" {
		t.Fatalf("catalog schemas=%v", cfg.DDL.CatalogSchemas)
	}
}

func TestInvalidEnvironmentFailsWithExactWinningKeyWhenFileFieldAbsent(t *testing.T) {
	tests := []struct{ name, key, value, want string }{{"bool", "WALLABY_PPROF_ENABLED", "not-a-bool", "invalid environment value for WALLABY_PPROF_ENABLED"}, {"integer", "WALLABY_DBOS_MAX_EMPTY_READS", "not-an-integer", "invalid environment value for WALLABY_DBOS_MAX_EMPTY_READS"}, {"duration", "WALLABY_OTEL_METRICS_INTERVAL", "not-a-duration", "invalid environment value for WALLABY_OTEL_METRICS_INTERVAL"}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("WALLABY_ENV", "test")
			t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
			t.Setenv(test.key, test.value)
			_, err := Load("")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want %q", err, test.want)
			}
		})
	}
}

func TestStrictNestedContainerErrorsReportCompleteLeafPaths(t *testing.T) {
	tests := []struct{ name, extension, body, want string }{{"yaml map integer", ".yaml", "kubernetes:\n  job_labels:\n    app: 1\n", "kubernetes.job_labels.app: expected string, got integer"}, {"yaml map object", ".yaml", "kubernetes:\n  job_annotations:\n    owner:\n      team: platform\n", "kubernetes.job_annotations.owner: expected string, got object"}, {"yaml map key", ".yaml", "kubernetes:\n  job_env:\n    7: value\n", "kubernetes.job_env[0].key: expected string, got integer"}, {"yaml list integer", ".yaml", "kubernetes:\n  job_command: [wallaby-worker, 9]\n", "kubernetes.job_command[1]: expected string, got integer"}, {"yaml nested list", ".yaml", "kubernetes:\n  job_args:\n    - [nested]\n", "kubernetes.job_args[0]: expected string, got list"}, {"json map bool", ".json", `{"kubernetes":{"job_labels":{"app":true}}}`, "kubernetes.job_labels.app: expected string, got bool"}, {"json list object", ".json", `{"kubernetes":{"job_env_from":[{"secret":"name"}]}}`, "kubernetes.job_env_from[0]: expected string, got object"}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(writeConfigTestFile(t, "nested"+test.extension, test.body))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want %q", err, test.want)
			}
		})
	}
}

func TestStrictConfigPreservesCollectionDefaultsWhenFileAndEnvironmentAreAbsent(t *testing.T) {
	t.Setenv("WALLABY_ENV", "test")
	t.Setenv("WALLABY_WORKFLOW_STORE", "memory")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.DDL.CatalogSchemas) != 1 || cfg.DDL.CatalogSchemas[0] != "public" {
		t.Fatalf("catalog schemas=%v, want default [public]", cfg.DDL.CatalogSchemas)
	}
}

func TestStrictConfigMisspellingDoesNotSilentlyDefault(t *testing.T) {
	path := writeConfigTestFile(t, "misspelled.yaml", "environment: test\nworkflow:\n  store: memory\nprofiling:\n  enabledd: true\n")
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), `unknown key "profiling.enabledd"`) {
		t.Fatalf("misspelling error=%v", err)
	}
}

func writeConfigTestFile(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func yamlForConfigPath(path, value string) string {
	parts := strings.Split(path, ".")
	var out strings.Builder
	for index, part := range parts {
		out.WriteString(strings.Repeat("  ", index))
		out.WriteString(part)
		out.WriteString(":")
		if index == len(parts)-1 {
			out.WriteString(" ")
			out.WriteString(value)
		}
		out.WriteString("\n")
	}
	return out.String()
}
