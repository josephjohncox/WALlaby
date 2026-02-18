package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

// Config holds runtime settings for the WALlaby service.
type Config struct {
	Environment string
	API         APIConfig
	Postgres    PostgresConfig
	Telemetry   TelemetryConfig
	Trace       TraceConfig
	Profiling   ProfilingConfig
	DBOS        DBOSConfig
	Kubernetes  KubernetesConfig
	Wire        WireConfig
	DDL         DDLConfig
	Checkpoints CheckpointConfig
}

type APIConfig struct {
	GRPCListen     string
	GRPCReflection bool
}

type PostgresConfig struct {
	DSN string
}

type TelemetryConfig struct {
	ServiceName     string
	OTLPEndpoint    string
	OTLPInsecure    bool
	OTLPProtocol    string        `validate:"omitempty,oneof=grpc http http/protobuf"` // "grpc" or "http/protobuf"
	MetricsExporter string        `validate:"omitempty,oneof=otlp none"`
	TracesExporter  string        `validate:"omitempty,oneof=otlp none"`
	MetricsInterval time.Duration `validate:"gt=0"`
}

type TraceConfig struct {
	Path string
}

type ProfilingConfig struct {
	Enabled bool
	Listen  string
}

type DBOSConfig struct {
	Enabled       bool
	AppName       string
	Schedule      string
	Queue         string
	MaxEmptyReads int `validate:"gte=0"`
	MaxRetries    int `validate:"gte=0"`
	MaxRetriesSet bool
}

type KubernetesConfig struct {
	Enabled            bool
	KubeconfigPath     string
	KubeContext        string
	APIServer          string
	BearerToken        string
	CAFile             string
	CAData             string
	ClientCertFile     string
	ClientKeyFile      string
	InsecureSkipTLS    bool
	Namespace          string
	JobImage           string
	JobImagePullPolicy string
	JobServiceAccount  string
	JobNamePrefix      string
	JobTTLSeconds      int `validate:"gte=0"`
	JobBackoffLimit    int `validate:"gte=0"`
	MaxEmptyReads      int `validate:"gte=0"`
	JobLabels          map[string]string
	JobAnnotations     map[string]string
	JobCommand         []string
	JobArgs            []string
	JobEnv             map[string]string
	JobEnvFrom         []string
}

type WireConfig struct {
	DefaultFormat string `validate:"omitempty,oneof=arrow avro parquet proto json"`
	Enforce       bool
}

type DDLConfig struct {
	CatalogEnabled  bool
	CatalogInterval time.Duration
	CatalogSchemas  []string
	AutoApprove     bool
	Gate            bool
	AutoApply       bool
}

type CheckpointConfig struct {
	Backend string `validate:"omitempty,oneof=postgres sqlite none"`
	DSN     string
	Path    string
}

// Load loads config from a config file when provided or active viper configfile, then falls back to environment and defaults.
// Precedence is file > environment > default.
func Load(configPath string) (*Config, error) {
	cfgPath := strings.TrimSpace(configPath)
	if cfgPath == "" {
		cfgPath = strings.TrimSpace(viper.ConfigFileUsed())
	}

	fileCfg := viper.New()
	if cfgPath != "" {
		fileCfg.SetConfigFile(cfgPath)
		if err := fileCfg.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config file: %w", err)
		}
	}

	cfg := &Config{
		Environment: getenv("WALLABY_ENV", "dev"),
		API: APIConfig{
			GRPCListen:     ":8080",
			GRPCReflection: false,
		},
		Postgres: PostgresConfig{
			DSN: "",
		},
		Telemetry: TelemetryConfig{
			ServiceName:     "wallaby",
			OTLPEndpoint:    "",
			OTLPInsecure:    true,
			OTLPProtocol:    "grpc",
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
			Enabled:            false,
			KubeconfigPath:     "",
			KubeContext:        "",
			APIServer:          "",
			BearerToken:        "",
			CAFile:             "",
			CAData:             "",
			ClientCertFile:     "",
			ClientKeyFile:      "",
			InsecureSkipTLS:    false,
			Namespace:          "",
			JobImage:           "",
			JobImagePullPolicy: "IfNotPresent",
			JobServiceAccount:  "",
			JobNamePrefix:      "wallaby-worker",
			JobTTLSeconds:      0,
			JobBackoffLimit:    1,
			MaxEmptyReads:      0,
			JobLabels:          nil,
			JobAnnotations:     nil,
			JobCommand:         nil,
			JobArgs:            nil,
			JobEnv:             nil,
			JobEnvFrom:         nil,
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
	}

	var err error

	cfg.Environment = stringValue(fileCfg, []string{"environment"}, []string{"WALLABY_ENV", "WALLABY_WORKER_ENV"}, cfg.Environment)
	cfg.API.GRPCListen = stringValue(fileCfg, []string{"api.grpc_listen", "api.grpc-listen"}, []string{"WALLABY_GRPC_LISTEN", "WALLABY_WORKER_GRPC_LISTEN"}, cfg.API.GRPCListen)
	cfg.API.GRPCReflection, err = boolValue(fileCfg, []string{"api.grpc_reflection", "api.grpc-reflection"}, []string{"WALLABY_GRPC_REFLECTION", "WALLABY_WORKER_GRPC_REFLECTION"}, cfg.API.GRPCReflection)
	if err != nil {
		return nil, err
	}
	cfg.Postgres.DSN = stringValue(fileCfg, []string{"postgres.dsn"}, []string{"WALLABY_POSTGRES_DSN", "WALLABY_WORKER_POSTGRES_DSN"}, cfg.Postgres.DSN)

	cfg.Telemetry.ServiceName = stringValue(fileCfg, []string{"telemetry.service_name", "telemetry.service-name"}, []string{"OTEL_SERVICE_NAME", "WALLABY_OTEL_SERVICE"}, cfg.Telemetry.ServiceName)
	cfg.Telemetry.OTLPEndpoint = stringValue(fileCfg, []string{"telemetry.otlp_endpoint", "telemetry.otlp-endpoint", "telemetry.endpoint", "telemetry.otel_endpoint"}, []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "WALLABY_OTEL_ENDPOINT", "WALLABY_WORKER_OTEL_ENDPOINT"}, cfg.Telemetry.OTLPEndpoint)
	cfg.Telemetry.OTLPInsecure, err = boolValue(fileCfg, []string{"telemetry.otlp_insecure", "telemetry.otlp-insecure"}, []string{"OTEL_EXPORTER_OTLP_INSECURE", "WALLABY_OTEL_EXPORTER_OTLP_INSECURE", "WALLABY_WORKER_OTEL_EXPORTER_OTLP_INSECURE"}, cfg.Telemetry.OTLPInsecure)
	if err != nil {
		return nil, err
	}
	cfg.Telemetry.OTLPProtocol = stringValue(fileCfg, []string{"telemetry.otlp_protocol", "telemetry.otlp-protocol"}, []string{"OTEL_EXPORTER_OTLP_PROTOCOL", "WALLABY_OTEL_EXPORTER_OTLP_PROTOCOL", "WALLABY_WORKER_OTEL_EXPORTER_OTLP_PROTOCOL"}, cfg.Telemetry.OTLPProtocol)
	cfg.Telemetry.MetricsExporter = stringValue(fileCfg, []string{"telemetry.metrics_exporter", "telemetry.metrics-exporter"}, []string{"OTEL_METRICS_EXPORTER", "WALLABY_OTEL_METRICS_EXPORTER", "WALLABY_WORKER_OTEL_METRICS_EXPORTER"}, cfg.Telemetry.MetricsExporter)
	cfg.Telemetry.TracesExporter = stringValue(fileCfg, []string{"telemetry.traces_exporter", "telemetry.traces-exporter"}, []string{"OTEL_TRACES_EXPORTER", "WALLABY_OTEL_TRACES_EXPORTER", "WALLABY_WORKER_OTEL_TRACES_EXPORTER"}, cfg.Telemetry.TracesExporter)
	cfg.Telemetry.MetricsInterval, err = durationValue(fileCfg, []string{"telemetry.metrics_interval", "telemetry.metrics-interval"}, []string{"WALLABY_OTEL_METRICS_INTERVAL", "WALLABY_WORKER_OTEL_METRICS_INTERVAL"}, cfg.Telemetry.MetricsInterval)
	if err != nil {
		return nil, err
	}

	cfg.Trace.Path = stringValue(fileCfg, []string{"trace.path", "trace.file"}, []string{"WALLABY_TRACE_PATH", "WALLABY_WORKER_TRACE_PATH"}, cfg.Trace.Path)

	cfg.Profiling.Enabled, err = boolValue(fileCfg, []string{"profiling.enabled", "profiling.pprof"}, []string{"WALLABY_PPROF_ENABLED", "WALLABY_WORKER_PPROF_ENABLED"}, cfg.Profiling.Enabled)
	if err != nil {
		return nil, err
	}
	cfg.Profiling.Listen = stringValue(fileCfg, []string{"profiling.listen", "profiling.pprof"}, []string{"WALLABY_PPROF_LISTEN", "WALLABY_WORKER_PPROF_LISTEN"}, cfg.Profiling.Listen)

	cfg.DBOS.Enabled, err = boolValue(fileCfg, []string{"dbos.enabled", "dbos.dispatcher_enabled"}, []string{"WALLABY_DBOS_ENABLED", "WALLABY_WORKER_DBOS_ENABLED"}, cfg.DBOS.Enabled)
	if err != nil {
		return nil, err
	}
	cfg.DBOS.AppName = stringValue(fileCfg, []string{"dbos.app_name", "dbos.app-name"}, []string{"WALLABY_DBOS_APP", "WALLABY_WORKER_DBOS_APP"}, cfg.DBOS.AppName)
	cfg.DBOS.Schedule = stringValue(fileCfg, []string{"dbos.schedule", "dbos.cron"}, []string{"WALLABY_DBOS_SCHEDULE", "WALLABY_WORKER_DBOS_SCHEDULE"}, cfg.DBOS.Schedule)
	cfg.DBOS.Queue = stringValue(fileCfg, []string{"dbos.queue"}, []string{"WALLABY_DBOS_QUEUE", "WALLABY_WORKER_DBOS_QUEUE"}, cfg.DBOS.Queue)
	cfg.DBOS.MaxEmptyReads, err = intValue(fileCfg, []string{"dbos.max_empty_reads", "dbos.max-empty-reads"}, []string{"WALLABY_DBOS_MAX_EMPTY_READS", "WALLABY_WORKER_DBOS_MAX_EMPTY_READS"}, cfg.DBOS.MaxEmptyReads)
	if err != nil {
		return nil, err
	}
	if maxRetries, ok, err := intValueOptional(fileCfg, []string{"dbos.max_retries", "dbos.max-retries"}, []string{"WALLABY_DBOS_MAX_RETRIES", "WALLABY_WORKER_DBOS_MAX_RETRIES"}); err != nil {
		return nil, err
	} else if ok {
		cfg.DBOS.MaxRetries = maxRetries
		cfg.DBOS.MaxRetriesSet = true
	}

	cfg.Kubernetes.Enabled, err = boolValue(fileCfg,
		[]string{"kubernetes.enabled", "kubernetes.dispatcher_enabled", "k8s.enabled"},
		[]string{"WALLABY_K8S_ENABLED", "WALLABY_WORKER_K8S_ENABLED"},
		cfg.Kubernetes.Enabled,
	)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.KubeconfigPath = stringValue(fileCfg,
		[]string{"kubernetes.kubeconfig_path", "kubernetes.kubeconfig-path", "kubernetes.kubeconfig", "k8s.kubeconfig_path", "k8s.kubeconfig-path", "k8s.kubeconfig"},
		[]string{"WALLABY_K8S_KUBECONFIG", "WALLABY_WORKER_K8S_KUBECONFIG", "KUBECONFIG"},
		cfg.Kubernetes.KubeconfigPath,
	)
	cfg.Kubernetes.KubeContext = stringValue(fileCfg, []string{"kubernetes.context", "k8s.context"}, []string{"WALLABY_K8S_CONTEXT", "WALLABY_WORKER_K8S_CONTEXT"}, cfg.Kubernetes.KubeContext)
	cfg.Kubernetes.APIServer = stringValue(fileCfg, []string{"kubernetes.api_server", "kubernetes.api-server", "k8s.api_server", "k8s.api-server"}, []string{"WALLABY_K8S_API_SERVER", "WALLABY_WORKER_K8S_API_SERVER"}, cfg.Kubernetes.APIServer)
	cfg.Kubernetes.BearerToken = stringValue(fileCfg, []string{"kubernetes.token", "kubernetes.bearer_token", "kubernetes.bearer-token", "k8s.token", "k8s.bearer_token", "k8s.bearer-token"}, []string{"WALLABY_K8S_TOKEN", "WALLABY_WORKER_K8S_TOKEN"}, cfg.Kubernetes.BearerToken)
	cfg.Kubernetes.CAFile = stringValue(fileCfg, []string{"kubernetes.ca_file", "kubernetes.ca-file", "k8s.ca_file", "k8s.ca-file"}, []string{"WALLABY_K8S_CA_FILE", "WALLABY_WORKER_K8S_CA_FILE"}, cfg.Kubernetes.CAFile)
	cfg.Kubernetes.CAData = stringValue(fileCfg, []string{"kubernetes.ca_data", "kubernetes.ca-data", "k8s.ca_data", "k8s.ca-data"}, []string{"WALLABY_K8S_CA_DATA", "WALLABY_WORKER_K8S_CA_DATA"}, cfg.Kubernetes.CAData)
	cfg.Kubernetes.ClientCertFile = stringValue(fileCfg, []string{"kubernetes.client_cert_file", "kubernetes.client-cert-file", "k8s.client_cert_file", "k8s.client-cert-file"}, []string{"WALLABY_K8S_CLIENT_CERT", "WALLABY_WORKER_K8S_CLIENT_CERT"}, cfg.Kubernetes.ClientCertFile)
	cfg.Kubernetes.ClientKeyFile = stringValue(fileCfg, []string{"kubernetes.client_key_file", "kubernetes.client-key-file", "k8s.client_key_file", "k8s.client-key-file"}, []string{"WALLABY_K8S_CLIENT_KEY", "WALLABY_WORKER_K8S_CLIENT_KEY"}, cfg.Kubernetes.ClientKeyFile)
	cfg.Kubernetes.InsecureSkipTLS, err = boolValue(fileCfg, []string{"kubernetes.insecure_skip_tls", "kubernetes.insecure-skip-tls", "k8s.insecure_skip_tls", "k8s.insecure-skip-tls"}, []string{"WALLABY_K8S_INSECURE_SKIP_TLS", "WALLABY_WORKER_K8S_INSECURE_SKIP_TLS"}, cfg.Kubernetes.InsecureSkipTLS)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.Namespace = stringValue(fileCfg, []string{"kubernetes.namespace", "k8s.namespace"}, []string{"WALLABY_K8S_NAMESPACE", "WALLABY_WORKER_K8S_NAMESPACE"}, cfg.Kubernetes.Namespace)
	cfg.Kubernetes.JobImage = stringValue(fileCfg, []string{"kubernetes.job_image", "kubernetes.job-image", "k8s.job_image", "k8s.job-image"}, []string{"WALLABY_K8S_JOB_IMAGE", "WALLABY_WORKER_K8S_JOB_IMAGE"}, cfg.Kubernetes.JobImage)
	cfg.Kubernetes.JobImagePullPolicy = stringValue(fileCfg, []string{"kubernetes.job_image_pull_policy", "kubernetes.job-image-pull-policy", "k8s.job_image_pull_policy", "k8s.job-image-pull-policy"}, []string{"WALLABY_K8S_JOB_IMAGE_PULL_POLICY", "WALLABY_WORKER_K8S_JOB_IMAGE_PULL_POLICY"}, cfg.Kubernetes.JobImagePullPolicy)
	cfg.Kubernetes.JobServiceAccount = stringValue(fileCfg, []string{"kubernetes.job_service_account", "kubernetes.job-service-account", "k8s.job_service_account", "k8s.job-service-account"}, []string{"WALLABY_K8S_JOB_SERVICE_ACCOUNT", "WALLABY_WORKER_K8S_JOB_SERVICE_ACCOUNT"}, cfg.Kubernetes.JobServiceAccount)
	cfg.Kubernetes.JobNamePrefix = stringValue(fileCfg, []string{"kubernetes.job_name_prefix", "kubernetes.job-name-prefix", "k8s.job_name_prefix", "k8s.job-name-prefix"}, []string{"WALLABY_K8S_JOB_NAME_PREFIX", "WALLABY_WORKER_K8S_JOB_NAME_PREFIX"}, cfg.Kubernetes.JobNamePrefix)
	cfg.Kubernetes.JobTTLSeconds, err = intValue(fileCfg, []string{"kubernetes.job_ttl_seconds", "kubernetes.job-ttl-seconds", "k8s.job_ttl_seconds", "k8s.job-ttl-seconds"}, []string{"WALLABY_K8S_JOB_TTL_SECONDS", "WALLABY_WORKER_K8S_JOB_TTL_SECONDS"}, cfg.Kubernetes.JobTTLSeconds)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.JobBackoffLimit, err = intValue(fileCfg, []string{"kubernetes.job_backoff_limit", "kubernetes.job-backoff-limit", "k8s.job_backoff_limit", "k8s.job-backoff-limit"}, []string{"WALLABY_K8S_JOB_BACKOFF_LIMIT", "WALLABY_WORKER_K8S_JOB_BACKOFF_LIMIT"}, cfg.Kubernetes.JobBackoffLimit)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.MaxEmptyReads, err = intValue(fileCfg, []string{"kubernetes.job_max_empty_reads", "kubernetes.job-max-empty-reads", "k8s.job_max_empty_reads", "k8s.job-max-empty-reads"}, []string{"WALLABY_K8S_JOB_MAX_EMPTY_READS", "WALLABY_WORKER_K8S_JOB_MAX_EMPTY_READS"}, cfg.Kubernetes.MaxEmptyReads)
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.JobLabels, err = mapValue(fileCfg, []string{"kubernetes.job_labels", "kubernetes.job-labels", "k8s.job_labels", "k8s.job-labels"}, []string{"WALLABY_K8S_JOB_LABELS", "WALLABY_WORKER_K8S_JOB_LABELS"})
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.JobAnnotations, err = mapValue(fileCfg, []string{"kubernetes.job_annotations", "kubernetes.job-annotations", "k8s.job_annotations", "k8s.job-annotations"}, []string{"WALLABY_K8S_JOB_ANNOTATIONS", "WALLABY_WORKER_K8S_JOB_ANNOTATIONS"})
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.JobCommand, err = stringSliceValue(fileCfg, []string{"kubernetes.job_command", "kubernetes.job-command", "k8s.job_command", "k8s.job-command"}, []string{"WALLABY_K8S_JOB_COMMAND", "WALLABY_WORKER_K8S_JOB_COMMAND"})
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.JobArgs, err = stringSliceValue(fileCfg, []string{"kubernetes.job_args", "kubernetes.job-args", "k8s.job_args", "k8s.job-args"}, []string{"WALLABY_K8S_JOB_ARGS", "WALLABY_WORKER_K8S_JOB_ARGS"})
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.JobEnv, err = mapValue(fileCfg, []string{"kubernetes.job_env", "kubernetes.job-env", "k8s.job_env", "k8s.job-env"}, []string{"WALLABY_K8S_JOB_ENV", "WALLABY_WORKER_K8S_JOB_ENV"})
	if err != nil {
		return nil, err
	}
	cfg.Kubernetes.JobEnvFrom, err = stringSliceValue(fileCfg, []string{"kubernetes.job_env_from", "kubernetes.job-env-from", "k8s.job_env_from", "k8s.job-env-from"}, []string{"WALLABY_K8S_JOB_ENV_FROM", "WALLABY_WORKER_K8S_JOB_ENV_FROM"})
	if err != nil {
		return nil, err
	}

	cfg.Wire.DefaultFormat = stringValue(fileCfg, []string{"wire.format", "wire.default_format", "wire.default-format"}, []string{"WALLABY_WIRE_FORMAT", "WALLABY_WORKER_WIRE_FORMAT"}, cfg.Wire.DefaultFormat)
	cfg.Wire.Enforce, err = boolValue(fileCfg, []string{"wire.enforce", "wire.enforce_format", "wire.enforce-format"}, []string{"WALLABY_WIRE_ENFORCE", "WALLABY_WORKER_WIRE_ENFORCE"}, cfg.Wire.Enforce)
	if err != nil {
		return nil, err
	}

	cfg.DDL.CatalogEnabled, err = boolValue(fileCfg, []string{"ddl.catalog_enabled", "ddl.catalog-enabled"}, []string{"WALLABY_DDL_CATALOG_ENABLED", "WALLABY_WORKER_DDL_CATALOG_ENABLED"}, cfg.DDL.CatalogEnabled)
	if err != nil {
		return nil, err
	}
	cfg.DDL.CatalogInterval, err = durationValue(fileCfg, []string{"ddl.catalog_interval", "ddl.catalog-interval"}, []string{"WALLABY_DDL_CATALOG_INTERVAL", "WALLABY_WORKER_DDL_CATALOG_INTERVAL"}, cfg.DDL.CatalogInterval)
	if err != nil {
		return nil, err
	}
	cfg.DDL.CatalogSchemas, err = stringSliceValue(fileCfg, []string{"ddl.catalog_schemas", "ddl.catalog-schemas"}, []string{"WALLABY_DDL_CATALOG_SCHEMAS", "WALLABY_WORKER_DDL_CATALOG_SCHEMAS"})
	if err != nil {
		return nil, err
	}
	cfg.DDL.AutoApprove, err = boolValue(fileCfg, []string{"ddl.auto_approve", "ddl.auto-approve"}, []string{"WALLABY_DDL_AUTO_APPROVE", "WALLABY_WORKER_DDL_AUTO_APPROVE"}, cfg.DDL.AutoApprove)
	if err != nil {
		return nil, err
	}
	cfg.DDL.Gate, err = boolValue(fileCfg, []string{"ddl.gate"}, []string{"WALLABY_DDL_GATE", "WALLABY_WORKER_DDL_GATE"}, cfg.DDL.Gate)
	if err != nil {
		return nil, err
	}
	cfg.DDL.AutoApply, err = boolValue(fileCfg, []string{"ddl.auto_apply", "ddl.auto-apply"}, []string{"WALLABY_DDL_AUTO_APPLY", "WALLABY_WORKER_DDL_AUTO_APPLY"}, cfg.DDL.AutoApply)
	if err != nil {
		return nil, err
	}
	cfg.Checkpoints.Backend = stringValue(fileCfg, []string{"checkpoints.backend", "checkpoint.backend"}, []string{"WALLABY_CHECKPOINT_BACKEND", "WALLABY_WORKER_CHECKPOINT_BACKEND"}, cfg.Checkpoints.Backend)
	cfg.Checkpoints.DSN = stringValue(fileCfg, []string{"checkpoints.dsn", "checkpoint.dsn"}, []string{"WALLABY_CHECKPOINT_DSN", "WALLABY_WORKER_CHECKPOINT_DSN"}, cfg.Checkpoints.DSN)
	cfg.Checkpoints.Path = stringValue(fileCfg, []string{"checkpoints.path", "checkpoint.path"}, []string{"WALLABY_CHECKPOINT_PATH", "WALLABY_WORKER_CHECKPOINT_PATH"}, cfg.Checkpoints.Path)

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
	cfg.Telemetry.MetricsExporter = strings.ToLower(strings.TrimSpace(cfg.Telemetry.MetricsExporter))
	cfg.Telemetry.TracesExporter = strings.ToLower(strings.TrimSpace(cfg.Telemetry.TracesExporter))
	cfg.Wire.DefaultFormat = strings.ToLower(strings.TrimSpace(cfg.Wire.DefaultFormat))
	k8sImagePullPolicy := strings.TrimSpace(cfg.Kubernetes.JobImagePullPolicy)
	cfg.Kubernetes.JobImagePullPolicy = k8sImagePullPolicy

	var errs []string
	validator := validator.New()
	if err := validator.Struct(cfg); err != nil {
		errs = append(errs, formatValidationErrors(err)...)
	}

	metricsEnabled := cfg.Telemetry.MetricsExporter != "none" && cfg.Telemetry.MetricsExporter != ""
	tracesEnabled := cfg.Telemetry.TracesExporter != "none" && cfg.Telemetry.TracesExporter != ""

	if cfg.Telemetry.OTLPEndpoint == "" && (metricsEnabled || tracesEnabled) {
		errs = append(errs, "telemetry endpoint is required when telemetry exporters are enabled")
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
	case "", "ifnotpresent", "if-not-present":
		return "IfNotPresent", nil
	case "always":
		return "Always", nil
	case "never":
		return "Never", nil
	default:
		return "", fmt.Errorf("unsupported image pull policy %q", policy)
	}
}

func stringValue(fileCfg *viper.Viper, fileKeys, envKeys []string, fallback string) string {
	if value, ok := readFileValue(fileCfg, fileKeys); ok {
		return strings.TrimSpace(value)
	}
	raw, ok := readEnvValue(envKeys)
	if !ok {
		return fallback
	}
	return strings.TrimSpace(raw)
}

func boolValue(fileCfg *viper.Viper, fileKeys, envKeys []string, fallback bool) (bool, error) {
	if raw, ok := readFileValue(fileCfg, fileKeys); ok {
		value, err := parseBool(raw)
		if err != nil {
			return false, fmt.Errorf("invalid file value for %q: %w", strings.Join(fileKeys, ", "), err)
		}
		return value, nil
	}
	raw, ok := readEnvValue(envKeys)
	if !ok {
		return fallback, nil
	}
	value, err := parseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid environment value for %q: %w", strings.Join(envKeys, ", "), err)
	}
	return value, nil
}

func intValue(fileCfg *viper.Viper, fileKeys, envKeys []string, fallback int) (int, error) {
	if raw, ok := readFileValue(fileCfg, fileKeys); ok {
		value, err := parseInt(raw)
		if err != nil {
			return 0, fmt.Errorf("invalid file value for %q: %w", strings.Join(fileKeys, ", "), err)
		}
		return value, nil
	}
	raw, ok := readEnvValue(envKeys)
	if !ok {
		return fallback, nil
	}
	value, err := parseInt(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid environment value for %q: %w", strings.Join(envKeys, ", "), err)
	}
	return value, nil
}

func intValueOptional(fileCfg *viper.Viper, fileKeys, envKeys []string) (int, bool, error) {
	if raw, ok := readFileValue(fileCfg, fileKeys); ok {
		value, err := parseInt(raw)
		if err != nil {
			return 0, true, fmt.Errorf("invalid file value for %q: %w", strings.Join(fileKeys, ", "), err)
		}
		return value, true, nil
	}
	raw, ok := readEnvValue(envKeys)
	if !ok {
		return 0, false, nil
	}
	value, err := parseInt(raw)
	if err != nil {
		return 0, true, fmt.Errorf("invalid environment value for %q: %w", strings.Join(envKeys, ", "), err)
	}
	return value, true, nil
}

func durationValue(fileCfg *viper.Viper, fileKeys, envKeys []string, fallback time.Duration) (time.Duration, error) {
	if raw, ok := readFileValue(fileCfg, fileKeys); ok {
		value, err := parseDuration(raw)
		if err != nil {
			return 0, fmt.Errorf("invalid file value for %q: %w", strings.Join(fileKeys, ", "), err)
		}
		return value, nil
	}
	raw, ok := readEnvValue(envKeys)
	if !ok {
		return fallback, nil
	}
	value, err := parseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid environment value for %q: %w", strings.Join(envKeys, ", "), err)
	}
	return value, nil
}

func stringSliceValue(fileCfg *viper.Viper, fileKeys, envKeys []string) ([]string, error) {
	if raw, ok := readFileValue(fileCfg, fileKeys); ok {
		values, err := parseStringSlice(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid file value for %q: %w", strings.Join(fileKeys, ", "), err)
		}
		if len(values) == 0 {
			return nil, nil
		}
		return values, nil
	}
	if raw, ok := readEnvValue(envKeys); ok {
		values := parseCSV(raw)
		if len(values) == 0 {
			return nil, nil
		}
		return values, nil
	}
	return nil, nil
}

func mapValue(fileCfg *viper.Viper, fileKeys, envKeys []string) (map[string]string, error) {
	if raw, ok := readFileValue(fileCfg, fileKeys); ok {
		values, err := parseStringMap(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid file value for %q: %w", strings.Join(fileKeys, ", "), err)
		}
		if len(values) == 0 {
			return map[string]string{}, nil
		}
		return values, nil
	}
	if raw, ok := readEnvValue(envKeys); ok {
		values := parseKVPairs(raw)
		if len(values) == 0 {
			return map[string]string{}, nil
		}
		return values, nil
	}
	return map[string]string{}, nil
}

func readFileValue(fileCfg *viper.Viper, keys []string) (string, bool) {
	if fileCfg == nil {
		return "", false
	}
	for _, key := range keys {
		if fileCfg.IsSet(key) {
			return strings.TrimSpace(fileCfg.GetString(key)), true
		}
	}
	return "", false
}

func readEnvValue(keys []string) (string, bool) {
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			return strings.TrimSpace(value), true
		}
	}
	return "", false
}

func parseBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "t", "yes", "y", "on":
			return true, nil
		case "0", "false", "f", "no", "n", "off":
			return false, nil
		default:
			return false, fmt.Errorf("invalid bool value: %q", v)
		}
	default:
		return false, fmt.Errorf("invalid bool type: %T", value)
	}
}

func parseInt(value any) (int, error) {
	maxIntSigned := int64(^uint(0) >> 1)
	maxIntUnsigned := ^uint(0) >> 1
	minInt := -maxIntSigned - 1

	switch v := value.(type) {
	case int:
		return v, nil
	case int8:
		return int(v), nil
	case int16:
		return int(v), nil
	case int32:
		return int(v), nil
	case int64:
		if v > maxIntSigned || v < minInt {
			return 0, fmt.Errorf("integer overflow: %d", v)
		}
		return int(v), nil
	case uint:
		if v > maxIntUnsigned {
			return 0, fmt.Errorf("integer overflow: %d", v)
		}
		return int(v), nil
	case uint8:
		return int(v), nil
	case uint16:
		return int(v), nil
	case uint32:
		if uint(v) > maxIntUnsigned {
			return 0, fmt.Errorf("integer overflow: %d", v)
		}
		return int(v), nil
	case uint64:
		if v > uint64(maxIntUnsigned) {
			return 0, fmt.Errorf("integer overflow: %d", v)
		}
		parsed, err := strconv.Atoi(strconv.FormatUint(v, 10))
		if err != nil {
			return 0, fmt.Errorf("integer overflow: %d", v)
		}
		return parsed, nil
	case float64:
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("integer value required, got float: %v", v)
		}
		if v > float64(maxIntSigned) || v < float64(minInt) {
			return 0, fmt.Errorf("integer overflow: %v", v)
		}
		return int(v), nil
	case float32:
		if math.Trunc(float64(v)) != float64(v) {
			return 0, fmt.Errorf("integer value required, got float: %v", v)
		}
		if float64(v) > float64(maxIntSigned) || float64(v) < float64(minInt) {
			return 0, fmt.Errorf("integer overflow: %v", v)
		}
		return int(v), nil
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("invalid int type: %T", value)
	}
}

func parseDuration(value any) (time.Duration, error) {
	switch v := value.(type) {
	case time.Duration:
		return v, nil
	case string:
		parsed, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("invalid duration type: %T", value)
	}
}

func parseStringSlice(value any) ([]string, error) {
	switch v := value.(type) {
	case string:
		return parseCSV(v), nil
	case []string:
		out := make([]string, 0, len(v))
		for _, item := range v {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
		return out, nil
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			canonical := strings.TrimSpace(fmt.Sprint(item))
			if canonical != "" {
				out = append(out, canonical)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("invalid slice type: %T", value)
	}
}

func parseStringMap(value any) (map[string]string, error) {
	switch v := value.(type) {
	case map[string]string:
		if len(v) == 0 {
			return map[string]string{}, nil
		}
		out := make(map[string]string, len(v))
		for k, val := range v {
			k = strings.TrimSpace(k)
			if k == "" {
				continue
			}
			out[k] = strings.TrimSpace(val)
		}
		if len(out) == 0 {
			return map[string]string{}, nil
		}
		return out, nil
	case map[string]interface{}:
		if len(v) == 0 {
			return map[string]string{}, nil
		}
		out := make(map[string]string, len(v))
		for k, val := range v {
			key := strings.TrimSpace(k)
			if key == "" {
				continue
			}
			out[key] = strings.TrimSpace(fmt.Sprint(val))
		}
		if len(out) == 0 {
			return map[string]string{}, nil
		}
		return out, nil
	case map[interface{}]interface{}:
		if len(v) == 0 {
			return map[string]string{}, nil
		}
		out := make(map[string]string, len(v))
		for key, val := range v {
			k := strings.TrimSpace(fmt.Sprint(key))
			if k == "" {
				continue
			}
			out[k] = strings.TrimSpace(fmt.Sprint(val))
		}
		if len(out) == 0 {
			return map[string]string{}, nil
		}
		return out, nil
	default:
		return nil, fmt.Errorf("invalid map type: %T", value)
	}
}

func parseKVPairs(value any) map[string]string {
	raw := fmt.Sprint(value)
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

func getenv(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
