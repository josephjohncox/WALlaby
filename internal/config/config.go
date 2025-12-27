package config

import "os"

// Config holds runtime settings for the DuctStream service.
type Config struct {
	Environment string
	API         APIConfig
	Postgres    PostgresConfig
	Telemetry   TelemetryConfig
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
	}

	return cfg, nil
}

func getenv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
