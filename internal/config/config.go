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
	Wire        WireConfig
	DDL         DDLConfig
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
