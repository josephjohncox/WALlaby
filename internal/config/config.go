package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime settings for the DuctStream service.
type Config struct {
	Environment string
	API         APIConfig
	Postgres    PostgresConfig
	Telemetry   TelemetryConfig
	DBOS        DBOSConfig
	Kubernetes  KubernetesConfig
	Wire        WireConfig
	DDL         DDLConfig
	Checkpoints CheckpointConfig
}

type APIConfig struct {
	GRPCListen string
}

type PostgresConfig struct {
	DSN string
}

type TelemetryConfig struct {
	ServiceName string
}

type DBOSConfig struct {
	Enabled       bool
	AppName       string
	Schedule      string
	Queue         string
	MaxEmptyReads int
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
	JobTTLSeconds      int
	JobBackoffLimit    int
	MaxEmptyReads      int
	JobLabels          map[string]string
	JobAnnotations     map[string]string
	JobCommand         []string
	JobArgs            []string
	JobEnv             map[string]string
	JobEnvFrom         []string
}

type WireConfig struct {
	DefaultFormat string
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
	Backend string
	DSN     string
	Path    string
}

// Load loads config from environment for now. File parsing will be added later.
func Load(_ string) (*Config, error) {
	cfg := &Config{
		Environment: getenv("DUCTSTREAM_ENV", "dev"),
		API: APIConfig{
			GRPCListen: getenv("DUCTSTREAM_GRPC_LISTEN", ":8080"),
		},
		Postgres: PostgresConfig{
			DSN: getenv("DUCTSTREAM_POSTGRES_DSN", ""),
		},
		Telemetry: TelemetryConfig{
			ServiceName: getenv("DUCTSTREAM_OTEL_SERVICE", "ductstream"),
		},
		DBOS: DBOSConfig{
			Enabled:       getenvBool("DUCTSTREAM_DBOS_ENABLED", false),
			AppName:       getenv("DUCTSTREAM_DBOS_APP", "ductstream"),
			Schedule:      getenv("DUCTSTREAM_DBOS_SCHEDULE", ""),
			Queue:         getenv("DUCTSTREAM_DBOS_QUEUE", "ductstream"),
			MaxEmptyReads: getenvInt("DUCTSTREAM_DBOS_MAX_EMPTY_READS", 1),
		},
		Kubernetes: KubernetesConfig{
			Enabled:            getenvBool("DUCTSTREAM_K8S_ENABLED", false),
			KubeconfigPath:     getenv("DUCTSTREAM_K8S_KUBECONFIG", getenv("KUBECONFIG", "")),
			KubeContext:        getenv("DUCTSTREAM_K8S_CONTEXT", ""),
			APIServer:          getenv("DUCTSTREAM_K8S_API_SERVER", ""),
			BearerToken:        getenv("DUCTSTREAM_K8S_TOKEN", ""),
			CAFile:             getenv("DUCTSTREAM_K8S_CA_FILE", ""),
			CAData:             getenv("DUCTSTREAM_K8S_CA_DATA", ""),
			ClientCertFile:     getenv("DUCTSTREAM_K8S_CLIENT_CERT", ""),
			ClientKeyFile:      getenv("DUCTSTREAM_K8S_CLIENT_KEY", ""),
			InsecureSkipTLS:    getenvBool("DUCTSTREAM_K8S_INSECURE_SKIP_TLS", false),
			Namespace:          getenv("DUCTSTREAM_K8S_NAMESPACE", ""),
			JobImage:           getenv("DUCTSTREAM_K8S_JOB_IMAGE", ""),
			JobImagePullPolicy: getenv("DUCTSTREAM_K8S_JOB_IMAGE_PULL_POLICY", "IfNotPresent"),
			JobServiceAccount:  getenv("DUCTSTREAM_K8S_JOB_SERVICE_ACCOUNT", ""),
			JobNamePrefix:      getenv("DUCTSTREAM_K8S_JOB_NAME_PREFIX", "ductstream-worker"),
			JobTTLSeconds:      getenvInt("DUCTSTREAM_K8S_JOB_TTL_SECONDS", 0),
			JobBackoffLimit:    getenvInt("DUCTSTREAM_K8S_JOB_BACKOFF_LIMIT", 1),
			MaxEmptyReads:      getenvInt("DUCTSTREAM_K8S_JOB_MAX_EMPTY_READS", 0),
			JobLabels:          getenvKeyValueMap("DUCTSTREAM_K8S_JOB_LABELS"),
			JobAnnotations:     getenvKeyValueMap("DUCTSTREAM_K8S_JOB_ANNOTATIONS"),
			JobCommand:         getenvCSV("DUCTSTREAM_K8S_JOB_COMMAND", ""),
			JobArgs:            getenvCSV("DUCTSTREAM_K8S_JOB_ARGS", ""),
			JobEnv:             getenvKeyValueMap("DUCTSTREAM_K8S_JOB_ENV"),
			JobEnvFrom:         getenvCSV("DUCTSTREAM_K8S_JOB_ENV_FROM", ""),
		},
		Wire: WireConfig{
			DefaultFormat: getenv("DUCTSTREAM_WIRE_FORMAT", ""),
			Enforce:       getenvBool("DUCTSTREAM_WIRE_ENFORCE", true),
		},
		DDL: DDLConfig{
			CatalogEnabled:  getenvBool("DUCTSTREAM_DDL_CATALOG_ENABLED", false),
			CatalogInterval: getenvDuration("DUCTSTREAM_DDL_CATALOG_INTERVAL", 30*time.Second),
			CatalogSchemas:  getenvCSV("DUCTSTREAM_DDL_CATALOG_SCHEMAS", "public"),
			AutoApprove:     getenvBool("DUCTSTREAM_DDL_AUTO_APPROVE", false),
			Gate:            getenvBool("DUCTSTREAM_DDL_GATE", false),
			AutoApply:       getenvBool("DUCTSTREAM_DDL_AUTO_APPLY", false),
		},
		Checkpoints: CheckpointConfig{
			Backend: getenv("DUCTSTREAM_CHECKPOINT_BACKEND", ""),
			DSN:     getenv("DUCTSTREAM_CHECKPOINT_DSN", ""),
			Path:    getenv("DUCTSTREAM_CHECKPOINT_PATH", ""),
		},
	}

	return cfg, nil
}

func getenv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		switch value {
		case "1", "true", "TRUE", "yes", "YES":
			return true
		case "0", "false", "FALSE", "no", "NO":
			return false
		default:
			return fallback
		}
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	if value, ok := os.LookupEnv(key); ok {
		parsed, err := time.ParseDuration(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func getenvCSV(key, fallback string) []string {
	value := getenv(key, fallback)
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trim := strings.TrimSpace(part)
		if trim != "" {
			out = append(out, trim)
		}
	}
	return out
}

func getenvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		parsed, err := strconv.Atoi(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func getenvKeyValueMap(key string) map[string]string {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
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
		k := strings.TrimSpace(pair[0])
		if k == "" {
			continue
		}
		val := ""
		if len(pair) > 1 {
			val = strings.TrimSpace(pair[1])
		}
		out[k] = val
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
