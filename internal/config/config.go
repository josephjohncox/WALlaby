package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
)

// Config holds runtime settings for the WALlaby service.
type Config struct {
	Environment string           `json:"environment" yaml:"environment"`
	API         APIConfig        `json:"api" yaml:"api"`
	Postgres    PostgresConfig   `json:"postgres" yaml:"postgres"`
	Workflow    WorkflowConfig   `json:"workflow" yaml:"workflow"`
	Telemetry   TelemetryConfig  `json:"telemetry" yaml:"telemetry"`
	Trace       TraceConfig      `json:"trace" yaml:"trace"`
	Profiling   ProfilingConfig  `json:"profiling" yaml:"profiling"`
	DBOS        DBOSConfig       `json:"dbos" yaml:"dbos"`
	Kubernetes  KubernetesConfig `json:"kubernetes" yaml:"kubernetes"`
	Wire        WireConfig       `json:"wire" yaml:"wire"`
	DDL         DDLConfig        `json:"ddl" yaml:"ddl"`
	Checkpoints CheckpointConfig `json:"checkpoints" yaml:"checkpoints"`
	Artifacts   ArtifactConfig   `json:"artifacts" yaml:"artifacts"`
	Iceberg     IcebergConfig    `json:"iceberg" yaml:"iceberg"`
	Snowflake   SnowflakeConfig  `json:"snowflake" yaml:"snowflake"`
}

type APIConfig struct {
	GRPCListen     string `json:"grpc_listen" yaml:"grpc_listen"`
	GRPCReflection bool   `json:"grpc_reflection" yaml:"grpc_reflection"`
}
type PostgresConfig struct {
	DSN string `json:"dsn" yaml:"dsn"`
}

// SnowflakeConfig is deployment-owned execution and key-file configuration.
// It is never serialized into flow state.
type SnowflakeConfig struct {
	Enabled              bool   `json:"enabled" yaml:"enabled"`
	Account              string `json:"account" yaml:"account"`
	User                 string `json:"user" yaml:"user"`
	Host                 string `json:"host" yaml:"host"`
	PrivateKeyFile       string `json:"private_key_file" yaml:"private_key_file"`
	PrivateKeySecretName string `json:"private_key_secret_name" yaml:"private_key_secret_name"`
	PrivateKeySecretKey  string `json:"private_key_secret_key" yaml:"private_key_secret_key"`
}

// ValidateExecution ensures an enabled deployment has a reviewed external
// credential source. Callers apply authoritative worker flags before invoking
// it so a Kubernetes-supplied false value cannot be widened by stale files or
// environment variables.
func (c SnowflakeConfig) ValidateExecution() error {
	if !c.Enabled {
		return nil
	}
	if strings.TrimSpace(c.Account) == "" || strings.TrimSpace(c.User) == "" || strings.TrimSpace(c.Host) == "" || strings.TrimSpace(c.PrivateKeyFile) == "" {
		return errors.New("snowflake account, user, host, and private_key_file are required when snowflake.enabled=true")
	}
	return nil
}

// WorkflowConfig selects the lifecycle metadata store. Memory is explicitly
// development/test-only and is never an implicit production fallback.
type WorkflowConfig struct {
	Store string `json:"store" yaml:"store" validate:"oneof=postgres memory"`
}

type TelemetryConfig struct {
	ServiceName     string        `json:"service_name" yaml:"service_name"`
	OTLPEndpoint    string        `json:"otlp_endpoint" yaml:"otlp_endpoint"`
	OTLPInsecure    bool          `json:"otlp_insecure" yaml:"otlp_insecure"`
	OTLPProtocol    string        `json:"otlp_protocol" yaml:"otlp_protocol" validate:"omitempty,oneof=grpc http http/protobuf"`
	MetricsEndpoint string        `json:"metrics_endpoint" yaml:"metrics_endpoint"`
	MetricsInsecure bool          `json:"metrics_insecure" yaml:"metrics_insecure"`
	MetricsProtocol string        `json:"metrics_protocol" yaml:"metrics_protocol" validate:"omitempty,oneof=grpc http http/protobuf"`
	TracesEndpoint  string        `json:"traces_endpoint" yaml:"traces_endpoint"`
	TracesInsecure  bool          `json:"traces_insecure" yaml:"traces_insecure"`
	TracesProtocol  string        `json:"traces_protocol" yaml:"traces_protocol" validate:"omitempty,oneof=grpc http http/protobuf"`
	MetricsExporter string        `json:"metrics_exporter" yaml:"metrics_exporter" validate:"omitempty,oneof=otlp none"`
	TracesExporter  string        `json:"traces_exporter" yaml:"traces_exporter" validate:"omitempty,oneof=otlp none"`
	MetricsInterval time.Duration `json:"metrics_interval" yaml:"metrics_interval" validate:"gt=0"`
}
type TraceConfig struct {
	Path string `json:"path" yaml:"path"`
}
type ProfilingConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Listen  string `json:"listen" yaml:"listen"`
}

type DBOSConfig struct {
	Enabled       bool   `json:"enabled" yaml:"enabled"`
	AppName       string `json:"app_name" yaml:"app_name"`
	Schedule      string `json:"schedule" yaml:"schedule"`
	Queue         string `json:"queue" yaml:"queue"`
	MaxEmptyReads int    `json:"max_empty_reads" yaml:"max_empty_reads" validate:"gte=0"`
	MaxRetries    int    `json:"max_retries" yaml:"max_retries" validate:"gte=0"`
	MaxRetriesSet bool   `json:"-" yaml:"-"`
}

type KubernetesConfig struct {
	Enabled                         bool              `json:"enabled" yaml:"enabled"`
	KubeconfigPath                  string            `json:"kubeconfig_path" yaml:"kubeconfig_path"`
	KubeContext                     string            `json:"context" yaml:"context"`
	APIServer                       string            `json:"api_server" yaml:"api_server"`
	BearerToken                     string            `json:"bearer_token" yaml:"bearer_token"`
	CAFile                          string            `json:"ca_file" yaml:"ca_file"`
	CAData                          string            `json:"ca_data" yaml:"ca_data"`
	ClientCertFile                  string            `json:"client_cert_file" yaml:"client_cert_file"`
	ClientKeyFile                   string            `json:"client_key_file" yaml:"client_key_file"`
	InsecureSkipTLS                 bool              `json:"insecure_skip_tls" yaml:"insecure_skip_tls"`
	Namespace                       string            `json:"namespace" yaml:"namespace"`
	JobImage                        string            `json:"job_image" yaml:"job_image"`
	JobImagePullPolicy              string            `json:"job_image_pull_policy" yaml:"job_image_pull_policy"`
	JobServiceAccount               string            `json:"job_service_account" yaml:"job_service_account"`
	JobAutomountServiceAccountToken bool              `json:"job_automount_service_account_token" yaml:"job_automount_service_account_token"`
	JobNamePrefix                   string            `json:"job_name_prefix" yaml:"job_name_prefix"`
	JobTTLSeconds                   int               `json:"job_ttl_seconds" yaml:"job_ttl_seconds" validate:"gte=0"`
	JobBackoffLimit                 int               `json:"job_backoff_limit" yaml:"job_backoff_limit" validate:"gte=0"`
	MaxEmptyReads                   int               `json:"job_max_empty_reads" yaml:"job_max_empty_reads" validate:"gte=0"`
	JobLabels                       map[string]string `json:"job_labels" yaml:"job_labels"`
	JobAnnotations                  map[string]string `json:"job_annotations" yaml:"job_annotations"`
	JobCommand                      []string          `json:"job_command" yaml:"job_command"`
	JobArgs                         []string          `json:"job_args" yaml:"job_args"`
	JobEnv                          map[string]string `json:"job_env" yaml:"job_env"`
	JobEnvFrom                      []string          `json:"job_env_from" yaml:"job_env_from"`
}
type WireConfig struct {
	DefaultFormat string `json:"format" yaml:"format" validate:"omitempty,oneof=arrow avro parquet proto json"`
	Enforce       bool   `json:"enforce" yaml:"enforce"`
}
type DDLConfig struct {
	CatalogEnabled  bool          `json:"catalog_enabled" yaml:"catalog_enabled"`
	CatalogInterval time.Duration `json:"catalog_interval" yaml:"catalog_interval"`
	CatalogSchemas  []string      `json:"catalog_schemas" yaml:"catalog_schemas"`
	AutoApprove     bool          `json:"auto_approve" yaml:"auto_approve"`
	Gate            bool          `json:"gate" yaml:"gate"`
	AutoApply       bool          `json:"auto_apply" yaml:"auto_apply"`
}
type CheckpointConfig struct {
	Backend string `json:"backend" yaml:"backend" validate:"omitempty,oneof=postgres sqlite none"`
	DSN     string `json:"dsn" yaml:"dsn"`
	Path    string `json:"path" yaml:"path"`
}

// ArtifactConfig is deployment-level immutable-object and admission config.
// A flow selects materialization explicitly; credentials are never persisted in
// the flow API or PostgreSQL publication metadata.
type ArtifactConfig struct {
	Bucket                   string        `json:"bucket" yaml:"bucket"`
	Region                   string        `json:"region" yaml:"region"`
	Endpoint                 string        `json:"endpoint" yaml:"endpoint"`
	AccessKey                string        `json:"access_key" yaml:"access_key"`
	SecretKey                string        `json:"secret_key" yaml:"secret_key"`
	SessionToken             string        `json:"session_token" yaml:"session_token"`
	ForcePathStyle           bool          `json:"force_path_style" yaml:"force_path_style"`
	HardRetainedBytes        int           `json:"hard_retained_bytes" yaml:"hard_retained_bytes" validate:"omitempty,gt=0"`
	BacklogBatchHigh         int           `json:"backlog_batch_high" yaml:"backlog_batch_high" validate:"omitempty,gt=0"`
	BacklogBytesHigh         int           `json:"backlog_bytes_high" yaml:"backlog_bytes_high" validate:"omitempty,gt=0"`
	BacklogAgeHigh           time.Duration `json:"backlog_age_high" yaml:"backlog_age_high" validate:"omitempty,gt=0"`
	BackpressurePollInterval time.Duration `json:"backpressure_poll_interval" yaml:"backpressure_poll_interval" validate:"omitempty,gt=0"`
	OrphanGrace              time.Duration `json:"orphan_grace" yaml:"orphan_grace" validate:"omitempty,gt=0"`
	Retention                time.Duration `json:"retention" yaml:"retention" validate:"omitempty,gt=0"`
	GCInterval               time.Duration `json:"gc_interval" yaml:"gc_interval" validate:"omitempty,gt=0"`
}

// IcebergConfig supplies deployment-level REST authentication, TLS, timeout,
// and S3 Tables maintenance defaults. Flow options select non-secret target
// mapping and catalog profile values.
type IcebergConfig struct {
	Profile                      string        `json:"profile" yaml:"profile"`
	URI                          string        `json:"uri" yaml:"uri"`
	Warehouse                    string        `json:"warehouse" yaml:"warehouse"`
	Prefix                       string        `json:"prefix" yaml:"prefix"`
	ControlTable                 string        `json:"control_table" yaml:"control_table"`
	Region                       string        `json:"region" yaml:"region"`
	SigningName                  string        `json:"signing_name" yaml:"signing_name"`
	ExpectedAWSRoleARN           string        `json:"expected_aws_role_arn" yaml:"expected_aws_role_arn"`
	SigV4                        bool          `json:"sigv4" yaml:"sigv4"`
	AllowHTTP                    bool          `json:"allow_http" yaml:"allow_http"`
	OAuthToken                   string        `json:"oauth_token" yaml:"oauth_token"`
	OAuthCredential              string        `json:"oauth_credential" yaml:"oauth_credential"`
	OAuthScope                   string        `json:"oauth_scope" yaml:"oauth_scope"`
	OAuthURI                     string        `json:"oauth_uri" yaml:"oauth_uri"`
	CAFile                       string        `json:"ca_file" yaml:"ca_file"`
	CAData                       string        `json:"ca_data" yaml:"ca_data"`
	ClientCertFile               string        `json:"client_cert_file" yaml:"client_cert_file"`
	ClientKeyFile                string        `json:"client_key_file" yaml:"client_key_file"`
	ServerName                   string        `json:"server_name" yaml:"server_name"`
	S3Endpoint                   string        `json:"s3_endpoint" yaml:"s3_endpoint"`
	S3Region                     string        `json:"s3_region" yaml:"s3_region"`
	MaxCommitRetries             int           `json:"max_commit_retries" yaml:"max_commit_retries" validate:"omitempty,gt=0,lte=32"`
	RequestTimeout               time.Duration `json:"request_timeout" yaml:"request_timeout" validate:"omitempty,gt=0"`
	ReconciliationHorizon        time.Duration `json:"reconciliation_horizon" yaml:"reconciliation_horizon" validate:"omitempty,gt=0"`
	S3TablesTableBucketARN       string        `json:"s3tables_table_bucket_arn" yaml:"s3tables_table_bucket_arn"`
	S3TablesConfigureMaintenance bool          `json:"s3tables_configure_maintenance" yaml:"s3tables_configure_maintenance"`
	S3TablesMinSnapshotsToKeep   int           `json:"s3tables_min_snapshots_to_keep" yaml:"s3tables_min_snapshots_to_keep" validate:"omitempty,gt=1,lte=2147483647"`
	S3TablesMaxSnapshotAgeHours  int           `json:"s3tables_max_snapshot_age_hours" yaml:"s3tables_max_snapshot_age_hours" validate:"omitempty,gt=0,lte=2147483647"`
}

// Load strictly decodes the selected runtime file, then applies environment
// values only for absent file fields. Precedence is file > environment > default.
func Load(configPath string) (*Config, error) {
	cfgPath := strings.TrimSpace(configPath)
	// Viper selects the server/worker file path, but it never decodes runtime
	// configuration. The strict decoder below owns the current file schema.
	fileCfg := &environmentConfig{present: make(map[string]struct{})}

	cfg := &Config{
		Environment: "dev",
		API: APIConfig{
			GRPCListen:     ":8080",
			GRPCReflection: false,
		},
		Postgres: PostgresConfig{
			DSN: "",
		},
		Workflow: WorkflowConfig{
			Store: "postgres",
		},
		Telemetry: TelemetryConfig{
			ServiceName:     "wallaby",
			OTLPEndpoint:    "",
			OTLPInsecure:    true,
			OTLPProtocol:    "grpc",
			MetricsInsecure: true,
			MetricsProtocol: "grpc",
			TracesInsecure:  true,
			TracesProtocol:  "grpc",
			MetricsExporter: "none",
			TracesExporter:  "none",
			MetricsInterval: 30 * time.Second,
		},
		Trace: TraceConfig{
			Path: "",
		},
		Profiling: ProfilingConfig{
			Enabled: false,
			Listen:  ":6060",
		},
		DBOS: DBOSConfig{
			Enabled:       false,
			AppName:       "wallaby",
			Schedule:      "",
			Queue:         "wallaby",
			MaxEmptyReads: 1,
		},
		Kubernetes: KubernetesConfig{
			Enabled:                         false,
			KubeconfigPath:                  "",
			KubeContext:                     "",
			APIServer:                       "",
			BearerToken:                     "",
			CAFile:                          "",
			CAData:                          "",
			ClientCertFile:                  "",
			ClientKeyFile:                   "",
			InsecureSkipTLS:                 false,
			Namespace:                       "",
			JobImage:                        "",
			JobImagePullPolicy:              "IfNotPresent",
			JobServiceAccount:               "",
			JobAutomountServiceAccountToken: false,
			JobNamePrefix:                   "wallaby-worker",
			JobTTLSeconds:                   0,
			JobBackoffLimit:                 1,
			MaxEmptyReads:                   0,
			JobLabels:                       nil,
			JobAnnotations:                  nil,
			JobCommand:                      nil,
			JobArgs:                         nil,
			JobEnv:                          nil,
			JobEnvFrom:                      nil,
		},
		Wire: WireConfig{
			DefaultFormat: "",
			Enforce:       true,
		},
		DDL: DDLConfig{
			CatalogEnabled:  false,
			CatalogInterval: 30 * time.Second,
			CatalogSchemas:  []string{"public"},
			AutoApprove:     true,
			Gate:            false,
			AutoApply:       true,
		},
		Checkpoints: CheckpointConfig{
			Backend: "",
			DSN:     "",
			Path:    "",
		},
		Artifacts: ArtifactConfig{
			Region:                   "us-east-1",
			HardRetainedBytes:        64 << 30,
			BacklogBatchHigh:         10_000,
			BacklogBytesHigh:         32 << 30,
			BacklogAgeHigh:           24 * time.Hour,
			BackpressurePollInterval: time.Second,
			OrphanGrace:              time.Hour,
			Retention:                7 * 24 * time.Hour,
			GCInterval:               time.Minute,
		},
		Snowflake: SnowflakeConfig{
			Enabled:             false,
			PrivateKeySecretKey: "private-key.pem",
		},
		Iceberg: IcebergConfig{
			ControlTable:                "__wallaby_control",
			SigningName:                 "execute-api",
			MaxCommitRetries:            4,
			RequestTimeout:              30 * time.Second,
			ReconciliationHorizon:       24 * time.Hour,
			S3TablesMinSnapshotsToKeep:  100,
			S3TablesMaxSnapshotAgeHours: 24,
		},
	}

	if cfgPath != "" {
		present, err := decodeStrictConfigFile(cfgPath, cfg)
		if err != nil {
			return nil, err
		}
		fileCfg.present = present
	}

	var err error

	cfg.Environment = stringValue(fileCfg, []string{"environment"}, []string{"WALLABY_ENV", "WALLABY_WORKER_ENV"}, cfg.Environment)
	cfg.API.GRPCListen = stringValue(fileCfg, []string{"api.grpc_listen"}, []string{"WALLABY_GRPC_LISTEN", "WALLABY_WORKER_GRPC_LISTEN"}, cfg.API.GRPCListen)
	cfg.API.GRPCReflection, err = boolValue(fileCfg, []string{"api.grpc_reflection"}, []string{"WALLABY_GRPC_REFLECTION", "WALLABY_WORKER_GRPC_REFLECTION"}, cfg.API.GRPCReflection)
	if err != nil {
		return nil, err
	}
	cfg.Postgres.DSN = stringValue(fileCfg, []string{"postgres.dsn"}, []string{"WALLABY_POSTGRES_DSN", "WALLABY_WORKER_POSTGRES_DSN"}, cfg.Postgres.DSN)
	cfg.Snowflake.Enabled, err = boolValue(fileCfg, []string{"snowflake.enabled"}, []string{"WALLABY_SNOWFLAKE_ENABLED", "WALLABY_WORKER_SNOWFLAKE_ENABLED"}, cfg.Snowflake.Enabled)
	if err != nil {
		return nil, err
	}
	cfg.Snowflake.Account = stringValue(fileCfg, []string{"snowflake.account"}, []string{"WALLABY_SNOWFLAKE_ACCOUNT", "WALLABY_WORKER_SNOWFLAKE_ACCOUNT"}, cfg.Snowflake.Account)
	cfg.Snowflake.User = stringValue(fileCfg, []string{"snowflake.user"}, []string{"WALLABY_SNOWFLAKE_USER", "WALLABY_WORKER_SNOWFLAKE_USER"}, cfg.Snowflake.User)
	cfg.Snowflake.Host = stringValue(fileCfg, []string{"snowflake.host"}, []string{"WALLABY_SNOWFLAKE_HOST", "WALLABY_WORKER_SNOWFLAKE_HOST"}, cfg.Snowflake.Host)
	cfg.Snowflake.PrivateKeyFile = stringValue(fileCfg, []string{"snowflake.private_key_file"}, []string{"WALLABY_SNOWFLAKE_PRIVATE_KEY_FILE", "WALLABY_WORKER_SNOWFLAKE_PRIVATE_KEY_FILE"}, cfg.Snowflake.PrivateKeyFile)
	cfg.Snowflake.PrivateKeySecretName = stringValue(fileCfg, []string{"snowflake.private_key_secret_name"}, []string{"WALLABY_SNOWFLAKE_PRIVATE_KEY_SECRET_NAME"}, cfg.Snowflake.PrivateKeySecretName)
	cfg.Snowflake.PrivateKeySecretKey = stringValue(fileCfg, []string{"snowflake.private_key_secret_key"}, []string{"WALLABY_SNOWFLAKE_PRIVATE_KEY_SECRET_KEY"}, cfg.Snowflake.PrivateKeySecretKey)
	cfg.Workflow.Store = stringValue(fileCfg, []string{"workflow.store"}, []string{"WALLABY_WORKFLOW_STORE", "WALLABY_WORKER_WORKFLOW_STORE"}, cfg.Workflow.Store)

	cfg.Telemetry.ServiceName = stringValue(fileCfg, []string{"telemetry.service_name"}, []string{"OTEL_SERVICE_NAME"}, cfg.Telemetry.ServiceName)
	cfg.Telemetry.OTLPEndpoint = stringValue(fileCfg, []string{"telemetry.otlp_endpoint"}, []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "WALLABY_OTEL_ENDPOINT", "WALLABY_WORKER_OTEL_ENDPOINT"}, cfg.Telemetry.OTLPEndpoint)
	cfg.Telemetry.OTLPInsecure, err = boolValue(fileCfg, []string{"telemetry.otlp_insecure"}, []string{"OTEL_EXPORTER_OTLP_INSECURE", "WALLABY_OTEL_EXPORTER_OTLP_INSECURE", "WALLABY_WORKER_OTEL_EXPORTER_OTLP_INSECURE"}, cfg.Telemetry.OTLPInsecure)
	if err != nil {
		return nil, err
	}
	cfg.Telemetry.OTLPProtocol = stringValue(fileCfg, []string{"telemetry.otlp_protocol"}, []string{"OTEL_EXPORTER_OTLP_PROTOCOL", "WALLABY_OTEL_EXPORTER_OTLP_PROTOCOL", "WALLABY_WORKER_OTEL_EXPORTER_OTLP_PROTOCOL"}, cfg.Telemetry.OTLPProtocol)
	cfg.Telemetry.MetricsEndpoint = stringValue(fileCfg, []string{"telemetry.metrics_endpoint"}, []string{"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "WALLABY_OTEL_METRICS_ENDPOINT", "WALLABY_WORKER_OTEL_METRICS_ENDPOINT"}, cfg.Telemetry.OTLPEndpoint)
	cfg.Telemetry.MetricsInsecure, err = boolValue(fileCfg, []string{"telemetry.metrics_insecure"}, []string{"WALLABY_OTEL_METRICS_INSECURE", "WALLABY_WORKER_OTEL_METRICS_INSECURE"}, cfg.Telemetry.OTLPInsecure)
	if err != nil {
		return nil, err
	}
	cfg.Telemetry.MetricsProtocol = stringValue(fileCfg, []string{"telemetry.metrics_protocol"}, []string{"OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "WALLABY_OTEL_METRICS_PROTOCOL", "WALLABY_WORKER_OTEL_METRICS_PROTOCOL"}, cfg.Telemetry.OTLPProtocol)
	cfg.Telemetry.TracesEndpoint = stringValue(fileCfg, []string{"telemetry.traces_endpoint"}, []string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "WALLABY_OTEL_TRACES_ENDPOINT", "WALLABY_WORKER_OTEL_TRACES_ENDPOINT"}, cfg.Telemetry.OTLPEndpoint)
	cfg.Telemetry.TracesInsecure, err = boolValue(fileCfg, []string{"telemetry.traces_insecure"}, []string{"WALLABY_OTEL_TRACES_INSECURE", "WALLABY_WORKER_OTEL_TRACES_INSECURE"}, cfg.Telemetry.OTLPInsecure)
	if err != nil {
		return nil, err
	}
	cfg.Telemetry.TracesProtocol = stringValue(fileCfg, []string{"telemetry.traces_protocol"}, []string{"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "WALLABY_OTEL_TRACES_PROTOCOL", "WALLABY_WORKER_OTEL_TRACES_PROTOCOL"}, cfg.Telemetry.OTLPProtocol)
	cfg.Telemetry.MetricsExporter = stringValue(fileCfg, []string{"telemetry.metrics_exporter"}, []string{"OTEL_METRICS_EXPORTER", "WALLABY_OTEL_METRICS_EXPORTER", "WALLABY_WORKER_OTEL_METRICS_EXPORTER"}, cfg.Telemetry.MetricsExporter)
	cfg.Telemetry.TracesExporter = stringValue(fileCfg, []string{"telemetry.traces_exporter"}, []string{"OTEL_TRACES_EXPORTER", "WALLABY_OTEL_TRACES_EXPORTER", "WALLABY_WORKER_OTEL_TRACES_EXPORTER"}, cfg.Telemetry.TracesExporter)
	cfg.Telemetry.MetricsInterval, err = durationValue(fileCfg, []string{"telemetry.metrics_interval"}, []string{"WALLABY_OTEL_METRICS_INTERVAL", "WALLABY_WORKER_OTEL_METRICS_INTERVAL"}, cfg.Telemetry.MetricsInterval)
	if err != nil {
		return nil, err
	}

	cfg.Trace.Path = stringValue(fileCfg, []string{"trace.path"}, []string{"WALLABY_TRACE_PATH", "WALLABY_WORKER_TRACE_PATH"}, cfg.Trace.Path)

	cfg.Profiling.Enabled, err = boolValue(fileCfg, []string{"profiling.enabled"}, []string{"WALLABY_PPROF_ENABLED", "WALLABY_WORKER_PPROF_ENABLED"}, cfg.Profiling.Enabled)
	if err != nil {
		return nil, err
	}
	cfg.Profiling.Listen = stringValue(fileCfg, []string{"profiling.listen"}, []string{"WALLABY_PPROF_LISTEN", "WALLABY_WORKER_PPROF_LISTEN"}, cfg.Profiling.Listen)

	cfg.DBOS.Enabled, err = boolValue(fileCfg, []string{"dbos.enabled"}, []string{"WALLABY_DBOS_ENABLED", "WALLABY_WORKER_DBOS_ENABLED"}, cfg.DBOS.Enabled)
	if err != nil {
		return nil, err
	}
	cfg.DBOS.AppName = stringValue(fileCfg, []string{"dbos.app_name"}, []string{"WALLABY_DBOS_APP", "WALLABY_WORKER_DBOS_APP"}, cfg.DBOS.AppName)
	cfg.DBOS.Schedule = stringValue(fileCfg, []string{"dbos.schedule"}, []string{"WALLABY_DBOS_SCHEDULE", "WALLABY_WORKER_DBOS_SCHEDULE"}, cfg.DBOS.Schedule)
	cfg.DBOS.Queue = stringValue(fileCfg, []string{"dbos.queue"}, []string{"WALLABY_DBOS_QUEUE", "WALLABY_WORKER_DBOS_QUEUE"}, cfg.DBOS.Queue)
	cfg.DBOS.MaxEmptyReads, err = intValue(fileCfg, []string{"dbos.max_empty_reads"}, []string{"WALLABY_DBOS_MAX_EMPTY_READS", "WALLABY_WORKER_DBOS_MAX_EMPTY_READS"}, cfg.DBOS.MaxEmptyReads)
	if err != nil {
		return nil, err
	}
	if maxRetries, ok, err := intValueOptional(fileCfg, []string{"dbos.max_retries"}, []string{"WALLABY_DBOS_MAX_RETRIES", "WALLABY_WORKER_DBOS_MAX_RETRIES"}); err != nil {
		return nil, err
	} else if ok {
		cfg.DBOS.MaxRetries = maxRetries
		cfg.DBOS.MaxRetriesSet = true
	}

	cfg.Kubernetes.Enabled, err = boolValue(fileCfg, []string{"kubernetes.enabled"}, []string{"WALLABY_K8S_ENABLED", "WALLABY_WORKER_K8S_ENABLED"}, cfg.Kubernetes.Enabled)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.KubeconfigPath = stringValue(fileCfg, []string{"kubernetes.kubeconfig_path"}, []string{"WALLABY_K8S_KUBECONFIG", "WALLABY_WORKER_K8S_KUBECONFIG"}, cfg.Kubernetes.KubeconfigPath)
	cfg.Kubernetes.KubeContext = stringValue(fileCfg, []string{"kubernetes.context"}, []string{"WALLABY_K8S_CONTEXT", "WALLABY_WORKER_K8S_CONTEXT"}, cfg.Kubernetes.KubeContext)
	cfg.Kubernetes.APIServer = stringValue(fileCfg, []string{"kubernetes.api_server"}, []string{"WALLABY_K8S_API_SERVER", "WALLABY_WORKER_K8S_API_SERVER"}, cfg.Kubernetes.APIServer)
	cfg.Kubernetes.BearerToken = stringValue(fileCfg, []string{"kubernetes.bearer_token"}, []string{"WALLABY_K8S_TOKEN", "WALLABY_WORKER_K8S_TOKEN"}, cfg.Kubernetes.BearerToken)
	cfg.Kubernetes.CAFile = stringValue(fileCfg, []string{"kubernetes.ca_file"}, []string{"WALLABY_K8S_CA_FILE", "WALLABY_WORKER_K8S_CA_FILE"}, cfg.Kubernetes.CAFile)
	cfg.Kubernetes.CAData = stringValue(fileCfg, []string{"kubernetes.ca_data"}, []string{"WALLABY_K8S_CA_DATA", "WALLABY_WORKER_K8S_CA_DATA"}, cfg.Kubernetes.CAData)
	cfg.Kubernetes.ClientCertFile = stringValue(fileCfg, []string{"kubernetes.client_cert_file"}, []string{"WALLABY_K8S_CLIENT_CERT", "WALLABY_WORKER_K8S_CLIENT_CERT"}, cfg.Kubernetes.ClientCertFile)
	cfg.Kubernetes.ClientKeyFile = stringValue(fileCfg, []string{"kubernetes.client_key_file"}, []string{"WALLABY_K8S_CLIENT_KEY", "WALLABY_WORKER_K8S_CLIENT_KEY"}, cfg.Kubernetes.ClientKeyFile)
	cfg.Kubernetes.InsecureSkipTLS, err = boolValue(fileCfg, []string{"kubernetes.insecure_skip_tls"}, []string{"WALLABY_K8S_INSECURE_SKIP_TLS", "WALLABY_WORKER_K8S_INSECURE_SKIP_TLS"}, cfg.Kubernetes.InsecureSkipTLS)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.Namespace = stringValue(fileCfg, []string{"kubernetes.namespace"}, []string{"WALLABY_K8S_NAMESPACE", "WALLABY_WORKER_K8S_NAMESPACE"}, cfg.Kubernetes.Namespace)
	cfg.Kubernetes.JobImage = stringValue(fileCfg, []string{"kubernetes.job_image"}, []string{"WALLABY_K8S_JOB_IMAGE", "WALLABY_WORKER_K8S_JOB_IMAGE"}, cfg.Kubernetes.JobImage)
	cfg.Kubernetes.JobImagePullPolicy = stringValue(fileCfg, []string{"kubernetes.job_image_pull_policy"}, []string{"WALLABY_K8S_JOB_IMAGE_PULL_POLICY", "WALLABY_WORKER_K8S_JOB_IMAGE_PULL_POLICY"}, cfg.Kubernetes.JobImagePullPolicy)
	cfg.Kubernetes.JobServiceAccount = stringValue(fileCfg, []string{"kubernetes.job_service_account"}, []string{"WALLABY_K8S_JOB_SERVICE_ACCOUNT", "WALLABY_WORKER_K8S_JOB_SERVICE_ACCOUNT"}, cfg.Kubernetes.JobServiceAccount)
	cfg.Kubernetes.JobAutomountServiceAccountToken, err = boolValue(fileCfg, []string{"kubernetes.job_automount_service_account_token"}, []string{"WALLABY_K8S_JOB_AUTOMOUNT_SERVICE_ACCOUNT_TOKEN", "WALLABY_WORKER_K8S_JOB_AUTOMOUNT_SERVICE_ACCOUNT_TOKEN"}, cfg.Kubernetes.JobAutomountServiceAccountToken)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.JobNamePrefix = stringValue(fileCfg, []string{"kubernetes.job_name_prefix"}, []string{"WALLABY_K8S_JOB_NAME_PREFIX", "WALLABY_WORKER_K8S_JOB_NAME_PREFIX"}, cfg.Kubernetes.JobNamePrefix)
	cfg.Kubernetes.JobTTLSeconds, err = intValue(fileCfg, []string{"kubernetes.job_ttl_seconds"}, []string{"WALLABY_K8S_JOB_TTL_SECONDS", "WALLABY_WORKER_K8S_JOB_TTL_SECONDS"}, cfg.Kubernetes.JobTTLSeconds)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.JobBackoffLimit, err = intValue(fileCfg, []string{"kubernetes.job_backoff_limit"}, []string{"WALLABY_K8S_JOB_BACKOFF_LIMIT", "WALLABY_WORKER_K8S_JOB_BACKOFF_LIMIT"}, cfg.Kubernetes.JobBackoffLimit)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.MaxEmptyReads, err = intValue(fileCfg, []string{"kubernetes.job_max_empty_reads"}, []string{"WALLABY_K8S_JOB_MAX_EMPTY_READS", "WALLABY_WORKER_K8S_JOB_MAX_EMPTY_READS"}, cfg.Kubernetes.MaxEmptyReads)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.JobLabels = mapValue(fileCfg, []string{"kubernetes.job_labels"}, []string{"WALLABY_K8S_JOB_LABELS", "WALLABY_WORKER_K8S_JOB_LABELS"}, cfg.Kubernetes.JobLabels)
	cfg.Kubernetes.JobAnnotations = mapValue(fileCfg, []string{"kubernetes.job_annotations"}, []string{"WALLABY_K8S_JOB_ANNOTATIONS", "WALLABY_WORKER_K8S_JOB_ANNOTATIONS"}, cfg.Kubernetes.JobAnnotations)
	cfg.Kubernetes.JobCommand = stringSliceValue(fileCfg, []string{"kubernetes.job_command"}, []string{"WALLABY_K8S_JOB_COMMAND", "WALLABY_WORKER_K8S_JOB_COMMAND"}, cfg.Kubernetes.JobCommand)
	cfg.Kubernetes.JobArgs = stringSliceValue(fileCfg, []string{"kubernetes.job_args"}, []string{"WALLABY_K8S_JOB_ARGS", "WALLABY_WORKER_K8S_JOB_ARGS"}, cfg.Kubernetes.JobArgs)
	cfg.Kubernetes.JobEnv = mapValue(fileCfg, []string{"kubernetes.job_env"}, []string{"WALLABY_K8S_JOB_ENV", "WALLABY_WORKER_K8S_JOB_ENV"}, cfg.Kubernetes.JobEnv)
	cfg.Kubernetes.JobEnvFrom = stringSliceValue(fileCfg, []string{"kubernetes.job_env_from"}, []string{"WALLABY_K8S_JOB_ENV_FROM", "WALLABY_WORKER_K8S_JOB_ENV_FROM"}, cfg.Kubernetes.JobEnvFrom)

	cfg.Wire.DefaultFormat = stringValue(fileCfg, []string{"wire.format"}, []string{"WALLABY_WIRE_FORMAT", "WALLABY_WORKER_WIRE_FORMAT"}, cfg.Wire.DefaultFormat)
	cfg.Wire.Enforce, err = boolValue(fileCfg, []string{"wire.enforce"}, []string{"WALLABY_WIRE_ENFORCE", "WALLABY_WORKER_WIRE_ENFORCE"}, cfg.Wire.Enforce)
	if err != nil {
		return nil, err
	}

	cfg.DDL.CatalogEnabled, err = boolValue(fileCfg, []string{"ddl.catalog_enabled"}, []string{"WALLABY_DDL_CATALOG_ENABLED", "WALLABY_WORKER_DDL_CATALOG_ENABLED"}, cfg.DDL.CatalogEnabled)
	if err != nil {
		return nil, err
	}
	cfg.DDL.CatalogInterval, err = durationValue(fileCfg, []string{"ddl.catalog_interval"}, []string{"WALLABY_DDL_CATALOG_INTERVAL", "WALLABY_WORKER_DDL_CATALOG_INTERVAL"}, cfg.DDL.CatalogInterval)
	if err != nil {
		return nil, err
	}
	cfg.DDL.CatalogSchemas = stringSliceValue(fileCfg, []string{"ddl.catalog_schemas"}, []string{"WALLABY_DDL_CATALOG_SCHEMAS", "WALLABY_WORKER_DDL_CATALOG_SCHEMAS"}, cfg.DDL.CatalogSchemas)
	cfg.DDL.AutoApprove, err = boolValue(fileCfg, []string{"ddl.auto_approve"}, []string{"WALLABY_DDL_AUTO_APPROVE", "WALLABY_WORKER_DDL_AUTO_APPROVE"}, cfg.DDL.AutoApprove)
	if err != nil {
		return nil, err
	}
	cfg.DDL.Gate, err = boolValue(fileCfg, []string{"ddl.gate"}, []string{"WALLABY_DDL_GATE", "WALLABY_WORKER_DDL_GATE"}, cfg.DDL.Gate)
	if err != nil {
		return nil, err
	}
	cfg.DDL.AutoApply, err = boolValue(fileCfg, []string{"ddl.auto_apply"}, []string{"WALLABY_DDL_AUTO_APPLY", "WALLABY_WORKER_DDL_AUTO_APPLY"}, cfg.DDL.AutoApply)
	if err != nil {
		return nil, err
	}
	cfg.Checkpoints.Backend = stringValue(fileCfg, []string{"checkpoints.backend"}, []string{"WALLABY_CHECKPOINT_BACKEND", "WALLABY_WORKER_CHECKPOINT_BACKEND"}, cfg.Checkpoints.Backend)
	cfg.Checkpoints.DSN = stringValue(fileCfg, []string{"checkpoints.dsn"}, []string{"WALLABY_CHECKPOINT_DSN", "WALLABY_WORKER_CHECKPOINT_DSN"}, cfg.Checkpoints.DSN)
	cfg.Checkpoints.Path = stringValue(fileCfg, []string{"checkpoints.path"}, []string{"WALLABY_CHECKPOINT_PATH", "WALLABY_WORKER_CHECKPOINT_PATH"}, cfg.Checkpoints.Path)

	cfg.Artifacts.Bucket = stringValue(fileCfg, []string{"artifacts.bucket"}, []string{"WALLABY_ARTIFACT_BUCKET", "WALLABY_WORKER_ARTIFACT_BUCKET"}, cfg.Artifacts.Bucket)
	cfg.Artifacts.Region = stringValue(fileCfg, []string{"artifacts.region"}, []string{"WALLABY_ARTIFACT_REGION", "WALLABY_WORKER_ARTIFACT_REGION"}, cfg.Artifacts.Region)
	cfg.Artifacts.Endpoint = stringValue(fileCfg, []string{"artifacts.endpoint"}, []string{"WALLABY_ARTIFACT_ENDPOINT", "WALLABY_WORKER_ARTIFACT_ENDPOINT"}, cfg.Artifacts.Endpoint)
	cfg.Artifacts.AccessKey = stringValue(fileCfg, []string{"artifacts.access_key"}, []string{"WALLABY_ARTIFACT_ACCESS_KEY", "WALLABY_WORKER_ARTIFACT_ACCESS_KEY"}, cfg.Artifacts.AccessKey)
	cfg.Artifacts.SecretKey = stringValue(fileCfg, []string{"artifacts.secret_key"}, []string{"WALLABY_ARTIFACT_SECRET_KEY", "WALLABY_WORKER_ARTIFACT_SECRET_KEY"}, cfg.Artifacts.SecretKey)
	cfg.Artifacts.SessionToken = stringValue(fileCfg, []string{"artifacts.session_token"}, []string{"WALLABY_ARTIFACT_SESSION_TOKEN", "WALLABY_WORKER_ARTIFACT_SESSION_TOKEN"}, cfg.Artifacts.SessionToken)
	cfg.Artifacts.ForcePathStyle, err = boolValue(fileCfg, []string{"artifacts.force_path_style"}, []string{"WALLABY_ARTIFACT_FORCE_PATH_STYLE", "WALLABY_WORKER_ARTIFACT_FORCE_PATH_STYLE"}, cfg.Artifacts.ForcePathStyle)
	if err != nil {
		return nil, err
	}
	cfg.Artifacts.HardRetainedBytes, err = intValue(fileCfg, []string{"artifacts.hard_retained_bytes"}, []string{"WALLABY_ARTIFACT_HARD_RETAINED_BYTES", "WALLABY_WORKER_ARTIFACT_HARD_RETAINED_BYTES"}, cfg.Artifacts.HardRetainedBytes)
	if err != nil {
		return nil, err
	}
	cfg.Artifacts.BacklogBatchHigh, err = intValue(fileCfg, []string{"artifacts.backlog_batch_high"}, []string{"WALLABY_ARTIFACT_BACKLOG_BATCH_HIGH", "WALLABY_WORKER_ARTIFACT_BACKLOG_BATCH_HIGH"}, cfg.Artifacts.BacklogBatchHigh)
	if err != nil {
		return nil, err
	}
	cfg.Artifacts.BacklogBytesHigh, err = intValue(fileCfg, []string{"artifacts.backlog_bytes_high"}, []string{"WALLABY_ARTIFACT_BACKLOG_BYTES_HIGH", "WALLABY_WORKER_ARTIFACT_BACKLOG_BYTES_HIGH"}, cfg.Artifacts.BacklogBytesHigh)
	if err != nil {
		return nil, err
	}
	cfg.Artifacts.BacklogAgeHigh, err = durationValue(fileCfg, []string{"artifacts.backlog_age_high"}, []string{"WALLABY_ARTIFACT_BACKLOG_AGE_HIGH", "WALLABY_WORKER_ARTIFACT_BACKLOG_AGE_HIGH"}, cfg.Artifacts.BacklogAgeHigh)
	if err != nil {
		return nil, err
	}
	cfg.Artifacts.BackpressurePollInterval, err = durationValue(fileCfg, []string{"artifacts.backpressure_poll_interval"}, []string{"WALLABY_ARTIFACT_BACKPRESSURE_POLL_INTERVAL", "WALLABY_WORKER_ARTIFACT_BACKPRESSURE_POLL_INTERVAL"}, cfg.Artifacts.BackpressurePollInterval)
	if err != nil {
		return nil, err
	}
	cfg.Artifacts.OrphanGrace, err = durationValue(fileCfg, []string{"artifacts.orphan_grace"}, []string{"WALLABY_ARTIFACT_ORPHAN_GRACE", "WALLABY_WORKER_ARTIFACT_ORPHAN_GRACE"}, cfg.Artifacts.OrphanGrace)
	if err != nil {
		return nil, err
	}
	cfg.Artifacts.Retention, err = durationValue(fileCfg, []string{"artifacts.retention"}, []string{"WALLABY_ARTIFACT_RETENTION", "WALLABY_WORKER_ARTIFACT_RETENTION"}, cfg.Artifacts.Retention)
	if err != nil {
		return nil, err
	}
	cfg.Artifacts.GCInterval, err = durationValue(fileCfg, []string{"artifacts.gc_interval"}, []string{"WALLABY_ARTIFACT_GC_INTERVAL", "WALLABY_WORKER_ARTIFACT_GC_INTERVAL"}, cfg.Artifacts.GCInterval)
	if err != nil {
		return nil, err
	}

	cfg.Iceberg.Profile = stringValue(fileCfg, []string{"iceberg.profile"}, []string{"WALLABY_ICEBERG_PROFILE", "WALLABY_WORKER_ICEBERG_PROFILE"}, cfg.Iceberg.Profile)
	cfg.Iceberg.URI = stringValue(fileCfg, []string{"iceberg.uri"}, []string{"WALLABY_ICEBERG_URI", "WALLABY_WORKER_ICEBERG_URI"}, cfg.Iceberg.URI)
	cfg.Iceberg.Warehouse = stringValue(fileCfg, []string{"iceberg.warehouse"}, []string{"WALLABY_ICEBERG_WAREHOUSE", "WALLABY_WORKER_ICEBERG_WAREHOUSE"}, cfg.Iceberg.Warehouse)
	cfg.Iceberg.Prefix = stringValue(fileCfg, []string{"iceberg.prefix"}, []string{"WALLABY_ICEBERG_PREFIX", "WALLABY_WORKER_ICEBERG_PREFIX"}, cfg.Iceberg.Prefix)
	cfg.Iceberg.ControlTable = stringValue(fileCfg, []string{"iceberg.control_table"}, []string{"WALLABY_ICEBERG_CONTROL_TABLE", "WALLABY_WORKER_ICEBERG_CONTROL_TABLE"}, cfg.Iceberg.ControlTable)
	cfg.Iceberg.Region = stringValue(fileCfg, []string{"iceberg.region"}, []string{"WALLABY_ICEBERG_REGION", "WALLABY_WORKER_ICEBERG_REGION"}, cfg.Iceberg.Region)
	cfg.Iceberg.SigningName = stringValue(fileCfg, []string{"iceberg.signing_name"}, []string{"WALLABY_ICEBERG_SIGNING_NAME", "WALLABY_WORKER_ICEBERG_SIGNING_NAME"}, cfg.Iceberg.SigningName)
	cfg.Iceberg.ExpectedAWSRoleARN = stringValue(fileCfg, []string{"iceberg.expected_aws_role_arn"}, []string{"WALLABY_ICEBERG_EXPECTED_AWS_ROLE_ARN", "WALLABY_WORKER_ICEBERG_EXPECTED_AWS_ROLE_ARN"}, cfg.Iceberg.ExpectedAWSRoleARN)
	cfg.Iceberg.OAuthToken = stringValue(fileCfg, []string{"iceberg.oauth_token"}, []string{"WALLABY_ICEBERG_OAUTH_TOKEN", "WALLABY_WORKER_ICEBERG_OAUTH_TOKEN"}, cfg.Iceberg.OAuthToken)
	cfg.Iceberg.OAuthCredential = stringValue(fileCfg, []string{"iceberg.oauth_credential"}, []string{"WALLABY_ICEBERG_OAUTH_CREDENTIAL", "WALLABY_WORKER_ICEBERG_OAUTH_CREDENTIAL"}, cfg.Iceberg.OAuthCredential)
	cfg.Iceberg.OAuthScope = stringValue(fileCfg, []string{"iceberg.oauth_scope"}, []string{"WALLABY_ICEBERG_OAUTH_SCOPE", "WALLABY_WORKER_ICEBERG_OAUTH_SCOPE"}, cfg.Iceberg.OAuthScope)
	cfg.Iceberg.OAuthURI = stringValue(fileCfg, []string{"iceberg.oauth_uri"}, []string{"WALLABY_ICEBERG_OAUTH_URI", "WALLABY_WORKER_ICEBERG_OAUTH_URI"}, cfg.Iceberg.OAuthURI)
	cfg.Iceberg.CAFile = stringValue(fileCfg, []string{"iceberg.ca_file"}, []string{"WALLABY_ICEBERG_CA_FILE", "WALLABY_WORKER_ICEBERG_CA_FILE"}, cfg.Iceberg.CAFile)
	cfg.Iceberg.CAData = stringValue(fileCfg, []string{"iceberg.ca_data"}, []string{"WALLABY_ICEBERG_CA_DATA", "WALLABY_WORKER_ICEBERG_CA_DATA"}, cfg.Iceberg.CAData)
	cfg.Iceberg.ClientCertFile = stringValue(fileCfg, []string{"iceberg.client_cert_file"}, []string{"WALLABY_ICEBERG_CLIENT_CERT", "WALLABY_WORKER_ICEBERG_CLIENT_CERT"}, cfg.Iceberg.ClientCertFile)
	cfg.Iceberg.ClientKeyFile = stringValue(fileCfg, []string{"iceberg.client_key_file"}, []string{"WALLABY_ICEBERG_CLIENT_KEY", "WALLABY_WORKER_ICEBERG_CLIENT_KEY"}, cfg.Iceberg.ClientKeyFile)
	cfg.Iceberg.ServerName = stringValue(fileCfg, []string{"iceberg.server_name"}, []string{"WALLABY_ICEBERG_SERVER_NAME", "WALLABY_WORKER_ICEBERG_SERVER_NAME"}, cfg.Iceberg.ServerName)
	cfg.Iceberg.S3Endpoint = stringValue(fileCfg, []string{"iceberg.s3_endpoint"}, []string{"WALLABY_ICEBERG_S3_ENDPOINT", "WALLABY_WORKER_ICEBERG_S3_ENDPOINT"}, cfg.Iceberg.S3Endpoint)
	cfg.Iceberg.S3Region = stringValue(fileCfg, []string{"iceberg.s3_region"}, []string{"WALLABY_ICEBERG_S3_REGION", "WALLABY_WORKER_ICEBERG_S3_REGION"}, cfg.Iceberg.S3Region)
	cfg.Iceberg.S3TablesTableBucketARN = stringValue(fileCfg, []string{"iceberg.s3tables_table_bucket_arn"}, []string{"WALLABY_ICEBERG_S3TABLES_TABLE_BUCKET_ARN", "WALLABY_WORKER_ICEBERG_S3TABLES_TABLE_BUCKET_ARN"}, cfg.Iceberg.S3TablesTableBucketARN)
	cfg.Iceberg.SigV4, err = boolValue(fileCfg, []string{"iceberg.sigv4"}, []string{"WALLABY_ICEBERG_SIGV4", "WALLABY_WORKER_ICEBERG_SIGV4"}, cfg.Iceberg.SigV4)
	if err != nil {
		return nil, err
	}
	cfg.Iceberg.AllowHTTP, err = boolValue(fileCfg, []string{"iceberg.allow_http"}, []string{"WALLABY_ICEBERG_ALLOW_HTTP", "WALLABY_WORKER_ICEBERG_ALLOW_HTTP"}, cfg.Iceberg.AllowHTTP)
	if err != nil {
		return nil, err
	}
	cfg.Iceberg.S3TablesConfigureMaintenance, err = boolValue(fileCfg, []string{"iceberg.s3tables_configure_maintenance"}, []string{"WALLABY_ICEBERG_S3TABLES_CONFIGURE_MAINTENANCE", "WALLABY_WORKER_ICEBERG_S3TABLES_CONFIGURE_MAINTENANCE"}, cfg.Iceberg.S3TablesConfigureMaintenance)
	if err != nil {
		return nil, err
	}
	cfg.Iceberg.MaxCommitRetries, err = intValue(fileCfg, []string{"iceberg.max_commit_retries"}, []string{"WALLABY_ICEBERG_MAX_COMMIT_RETRIES", "WALLABY_WORKER_ICEBERG_MAX_COMMIT_RETRIES"}, cfg.Iceberg.MaxCommitRetries)
	if err != nil {
		return nil, err
	}
	cfg.Iceberg.S3TablesMinSnapshotsToKeep, err = intValue(fileCfg, []string{"iceberg.s3tables_min_snapshots_to_keep"}, []string{"WALLABY_ICEBERG_S3TABLES_MIN_SNAPSHOTS_TO_KEEP", "WALLABY_WORKER_ICEBERG_S3TABLES_MIN_SNAPSHOTS_TO_KEEP"}, cfg.Iceberg.S3TablesMinSnapshotsToKeep)
	if err != nil {
		return nil, err
	}
	cfg.Iceberg.S3TablesMaxSnapshotAgeHours, err = intValue(fileCfg, []string{"iceberg.s3tables_max_snapshot_age_hours"}, []string{"WALLABY_ICEBERG_S3TABLES_MAX_SNAPSHOT_AGE_HOURS", "WALLABY_WORKER_ICEBERG_S3TABLES_MAX_SNAPSHOT_AGE_HOURS"}, cfg.Iceberg.S3TablesMaxSnapshotAgeHours)
	if err != nil {
		return nil, err
	}
	cfg.Iceberg.RequestTimeout, err = durationValue(fileCfg, []string{"iceberg.request_timeout"}, []string{"WALLABY_ICEBERG_REQUEST_TIMEOUT", "WALLABY_WORKER_ICEBERG_REQUEST_TIMEOUT"}, cfg.Iceberg.RequestTimeout)
	if err != nil {
		return nil, err
	}
	cfg.Iceberg.ReconciliationHorizon, err = durationValue(fileCfg, []string{"iceberg.reconciliation_horizon"}, []string{"WALLABY_ICEBERG_RECONCILIATION_HORIZON", "WALLABY_WORKER_ICEBERG_RECONCILIATION_HORIZON"}, cfg.Iceberg.ReconciliationHorizon)
	if err != nil {
		return nil, err
	}

	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config is required")
	}

	protocol := strings.ToLower(strings.TrimSpace(cfg.Telemetry.OTLPProtocol))
	cfg.Telemetry.OTLPProtocol = protocol
	if cfg.Telemetry.MetricsEndpoint == "" {
		cfg.Telemetry.MetricsEndpoint = cfg.Telemetry.OTLPEndpoint
	}
	if cfg.Telemetry.TracesEndpoint == "" {
		cfg.Telemetry.TracesEndpoint = cfg.Telemetry.OTLPEndpoint
	}
	if cfg.Telemetry.MetricsProtocol == "" {
		cfg.Telemetry.MetricsProtocol = protocol
	}
	if cfg.Telemetry.TracesProtocol == "" {
		cfg.Telemetry.TracesProtocol = protocol
	}
	cfg.Telemetry.MetricsProtocol = strings.ToLower(strings.TrimSpace(cfg.Telemetry.MetricsProtocol))
	cfg.Telemetry.TracesProtocol = strings.ToLower(strings.TrimSpace(cfg.Telemetry.TracesProtocol))
	cfg.Telemetry.MetricsExporter = strings.ToLower(strings.TrimSpace(cfg.Telemetry.MetricsExporter))
	cfg.Telemetry.TracesExporter = strings.ToLower(strings.TrimSpace(cfg.Telemetry.TracesExporter))
	cfg.Wire.DefaultFormat = strings.ToLower(strings.TrimSpace(cfg.Wire.DefaultFormat))
	cfg.Workflow.Store = strings.ToLower(strings.TrimSpace(cfg.Workflow.Store))
	cfg.Environment = strings.ToLower(strings.TrimSpace(cfg.Environment))
	k8sImagePullPolicy := strings.TrimSpace(cfg.Kubernetes.JobImagePullPolicy)
	cfg.Kubernetes.JobImagePullPolicy = k8sImagePullPolicy

	var errs []string
	validator := validator.New()
	if err := validator.Struct(cfg); err != nil {
		errs = append(errs, formatValidationErrors(err)...)
	}

	metricsEnabled := cfg.Telemetry.MetricsExporter != "none" && cfg.Telemetry.MetricsExporter != ""
	tracesEnabled := cfg.Telemetry.TracesExporter != "none" && cfg.Telemetry.TracesExporter != ""

	if cfg.Telemetry.MetricsEndpoint == "" && metricsEnabled {
		errs = append(errs, "telemetry metrics endpoint is required when the metrics exporter is enabled")
	}
	if cfg.Telemetry.TracesEndpoint == "" && tracesEnabled {
		errs = append(errs, "telemetry traces endpoint is required when the traces exporter is enabled")
	}
	jobImagePullPolicy, err := normalizeKubernetesImagePullPolicy(cfg.Kubernetes.JobImagePullPolicy)
	if err != nil {
		errs = append(errs, "kubernetes.job_image_pull_policy: "+err.Error())
	} else {
		cfg.Kubernetes.JobImagePullPolicy = jobImagePullPolicy
	}

	if cfg.DDL.CatalogEnabled && cfg.DDL.CatalogInterval <= 0 {
		errs = append(errs, "ddl.catalog_interval must be greater than 0 when ddl catalog scanning is enabled")
	}
	if cfg.Workflow.Store == "postgres" && strings.TrimSpace(cfg.Postgres.DSN) == "" {
		errs = append(errs, "postgres dsn is required when workflow.store=postgres")
	}
	if cfg.Workflow.Store == "memory" {
		switch cfg.Environment {
		case "dev", "development", "test":
		default:
			errs = append(errs, "workflow.store=memory is allowed only in dev, development, or test")
		}
		if cfg.DBOS.Enabled || cfg.Kubernetes.Enabled {
			errs = append(errs, "workflow.store=memory cannot be used with DBOS or Kubernetes dispatch")
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid config: %s", strings.Join(errs, "; "))
}

func formatValidationErrors(err error) []string {
	var errs []string
	var validatorErrs validator.ValidationErrors
	if !errors.As(err, &validatorErrs) {
		errs = append(errs, err.Error())
		return errs
	}

	for _, verr := range validatorErrs {
		field := normalizedConfigField(verr.StructNamespace())
		switch verr.Tag() {
		case "oneof":
			errs = append(errs, fmt.Sprintf("%s must be one of %s", field, verr.Param()))
		case "gt":
			errs = append(errs, fmt.Sprintf("%s must be greater than %s", field, verr.Param()))
		case "gte":
			errs = append(errs, fmt.Sprintf("%s must be greater than or equal to %s", field, verr.Param()))
		default:
			errs = append(errs, fmt.Sprintf("%s failed validation %q", field, verr.Tag()))
		}
	}
	return errs
}

func normalizedConfigField(namespace string) string {
	switch namespace {
	case "Config.Telemetry.OTLPProtocol":
		return "telemetry.otlp_protocol"
	case "Config.Telemetry.MetricsProtocol":
		return "telemetry.metrics_protocol"
	case "Config.Telemetry.TracesProtocol":
		return "telemetry.traces_protocol"
	case "Config.Telemetry.MetricsExporter":
		return "telemetry.metrics_exporter"
	case "Config.Telemetry.TracesExporter":
		return "telemetry.traces_exporter"
	case "Config.Telemetry.MetricsInterval":
		return "telemetry.metrics_interval"
	case "Config.Wire.DefaultFormat":
		return "wire.format"
	case "Config.DBOS.MaxEmptyReads":
		return "dbos.max_empty_reads"
	case "Config.DBOS.MaxRetries":
		return "dbos.max_retries"
	case "Config.Kubernetes.JobTTLSeconds":
		return "kubernetes.job_ttl_seconds"
	case "Config.Kubernetes.JobBackoffLimit":
		return "kubernetes.job_backoff_limit"
	case "Config.Kubernetes.MaxEmptyReads":
		return "kubernetes.job_max_empty_reads"
	case "Config.Checkpoints.Backend":
		return "checkpoints.backend"
	default:
		return strings.ToLower(strings.ReplaceAll(namespace, ".", "_"))
	}
}

func normalizeKubernetesImagePullPolicy(policy string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case "", "ifnotpresent":
		return "IfNotPresent", nil
	case "always":
		return "Always", nil
	case "never":
		return "Never", nil
	default:
		return "", fmt.Errorf("unsupported image pull policy %q", policy)
	}
}

func stringValue(fileCfg *environmentConfig, fileKeys, envKeys []string, fallback string) string {
	if fileCfg.has(fileKeys) {
		return fallback
	}
	raw, _, ok := readEnvValue(envKeys)
	if !ok {
		return fallback
	}
	return strings.TrimSpace(raw)
}
func boolValue(fileCfg *environmentConfig, fileKeys, envKeys []string, fallback bool) (bool, error) {
	if fileCfg.has(fileKeys) {
		return fallback, nil
	}
	raw, key, ok := readEnvValue(envKeys)
	if !ok {
		return fallback, nil
	}
	value, err := parseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid environment value for %s: %w", key, err)
	}
	return value, nil
}
func intValue(fileCfg *environmentConfig, fileKeys, envKeys []string, fallback int) (int, error) {
	if fileCfg.has(fileKeys) {
		return fallback, nil
	}
	raw, key, ok := readEnvValue(envKeys)
	if !ok {
		return fallback, nil
	}
	value, err := parseInt(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid environment value for %s: %w", key, err)
	}
	return value, nil
}
func intValueOptional(fileCfg *environmentConfig, fileKeys, envKeys []string) (int, bool, error) {
	if fileCfg.has(fileKeys) {
		return 0, false, nil
	}
	raw, key, ok := readEnvValue(envKeys)
	if !ok {
		return 0, false, nil
	}
	value, err := parseInt(raw)
	if err != nil {
		return 0, true, fmt.Errorf("invalid environment value for %s: %w", key, err)
	}
	return value, true, nil
}
func durationValue(fileCfg *environmentConfig, fileKeys, envKeys []string, fallback time.Duration) (time.Duration, error) {
	if fileCfg.has(fileKeys) {
		return fallback, nil
	}
	raw, key, ok := readEnvValue(envKeys)
	if !ok {
		return fallback, nil
	}
	value, err := parseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid environment value for %s: %w", key, err)
	}
	return value, nil
}
func stringSliceValue(fileCfg *environmentConfig, fileKeys, envKeys []string, fallback []string) []string {
	if fileCfg.has(fileKeys) {
		return fallback
	}
	raw, _, ok := readEnvValue(envKeys)
	if !ok {
		return fallback
	}
	values := parseCSV(raw)
	if len(values) == 0 {
		return nil
	}
	return values
}
func mapValue(fileCfg *environmentConfig, fileKeys, envKeys []string, fallback map[string]string) map[string]string {
	if fileCfg.has(fileKeys) {
		return fallback
	}
	raw, _, ok := readEnvValue(envKeys)
	if !ok {
		return fallback
	}
	values := parseKVPairs(raw)
	if len(values) == 0 {
		return map[string]string{}
	}
	return values
}

type environmentConfig struct{ present map[string]struct{} }

func (c *environmentConfig) has(paths []string) bool {
	if c == nil {
		return false
	}
	for _, path := range paths {
		if _, ok := c.present[path]; ok {
			return true
		}
	}
	return false
}
func readEnvValue(keys []string) (string, string, bool) {
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			return strings.TrimSpace(value), key, true
		}
	}
	return "", "", false
}

func parseBool(value any) (bool, error) {
	v, ok := value.(string)
	if !ok {
		return false, fmt.Errorf("invalid bool type: %T", value)
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "t", "yes", "y", "on":
		return true, nil
	case "0", "false", "f", "no", "n", "off":
		return false, nil
	default:
		return false, fmt.Errorf("invalid bool value: %q", v)
	}
}

func parseInt(value any) (int, error) {
	raw, ok := value.(string)
	if !ok {
		return 0, fmt.Errorf("invalid int type: %T", value)
	}
	return strconv.Atoi(strings.TrimSpace(raw))
}

func parseDuration(value any) (time.Duration, error) {
	v, ok := value.(string)
	if !ok {
		return 0, fmt.Errorf("invalid duration type: %T", value)
	}
	return time.ParseDuration(strings.TrimSpace(v))
}

func parseKVPairs(raw string) map[string]string {
	out := make(map[string]string)
	parts := strings.Split(raw, ",")
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		pair := strings.SplitN(item, "=", 2)
		if len(pair) == 0 {
			continue
		}
		key := strings.TrimSpace(pair[0])
		if key == "" {
			continue
		}
		val := ""
		if len(pair) > 1 {
			val = strings.TrimSpace(pair[1])
		}
		out[key] = val
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseCSV(value string) []string {
	out := make([]string, 0)
	for _, item := range strings.Split(value, ",") {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
