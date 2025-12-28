package tests

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snowflakedb/gosnowflake"
)

func recordKey(t testing.TB, key map[string]any) []byte {
	if key == nil {
		return nil
	}
	payload, err := json.Marshal(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return payload
}

func snowflakeTestDSN(t testing.TB) (string, string, bool) {
	if usingFakesnow() {
		host := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_HOST"))
		portRaw := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_PORT"))
		if host == "" {
			host = "localhost"
		}
		if portRaw == "" {
			portRaw = "8000"
		}
		port, err := strconv.Atoi(portRaw)
		if err != nil {
			t.Fatalf("invalid WALLABY_TEST_FAKESNOW_PORT: %v", err)
		}

		account := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_ACCOUNT"))
		if account == "" {
			account = "fakesnow"
		}
		user := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_USER"))
		if user == "" {
			user = "fake"
		}
		password := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_PASSWORD"))
		if password == "" {
			password = "snow"
		}
		database := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_DATABASE"))
		if database == "" {
			database = "WALLABY"
		}
		schema := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_SCHEMA"))
		if schema == "" {
			schema = "PUBLIC"
		}

		disableTelemetry := "false"
		params := map[string]*string{
			"CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED": &disableTelemetry,
		}

		cfg := &gosnowflake.Config{
			Account:           account,
			User:              user,
			Password:          password,
			Database:          database,
			Schema:            schema,
			Host:              host,
			Port:              port,
			Protocol:          "http",
			DisableOCSPChecks: true,
			Params:            params,
		}
		cfg.LoginTimeout = 5 * time.Second
		cfg.RequestTimeout = 5 * time.Second
		cfg.ClientTimeout = 5 * time.Second
		cfg.JWTClientTimeout = 5 * time.Second
		cfg.MaxRetryCount = 1

		dsn, err := gosnowflake.DSN(cfg)
		if err != nil {
			t.Fatalf("build fakesnow dsn: %v", err)
		}
		return dsn, schema, true
	}

	if dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_DSN")); dsn != "" {
		schema := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_SCHEMA"))
		if schema == "" {
			schema = "PUBLIC"
		}
		return dsn, schema, true
	}

	return "", "", false
}

func usingFakesnow() bool {
	host := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_HOST"))
	portRaw := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_PORT"))
	forceFake := strings.TrimSpace(os.Getenv("WALLABY_TEST_FORCE_FAKESNOW")) == "1"
	return host != "" || portRaw != "" || forceFake
}

func allowFakesnowSnowflake() bool {
	return strings.TrimSpace(os.Getenv("WALLABY_TEST_RUN_FAKESNOW")) == "1"
}

func snowflakeTestTimeout() time.Duration {
	if usingFakesnow() {
		return 15 * time.Second
	}
	return 2 * time.Minute
}
