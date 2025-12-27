package tests

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

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
	if dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_DSN")); dsn != "" {
		schema := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_SCHEMA"))
		if schema == "" {
			schema = "PUBLIC"
		}
		return dsn, schema, true
	}

	host := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_HOST"))
	portRaw := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_PORT"))
	if host == "" && portRaw == "" {
		return "", "", false
	}
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

	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		t.Fatalf("build fakesnow dsn: %v", err)
	}
	return dsn, schema, true
}
