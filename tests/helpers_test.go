package tests

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
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
		t.Skip("fakesnow uses password authentication and insecure HTTP; issue #75 requires deployment-bound JWT over verified HTTPS")
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

func snowflakeDeploymentPolicyForTest(t testing.TB) connector.SnowflakeDeploymentPolicy {
	t.Helper()
	cfg := connector.SnowflakeDeploymentConfig{
		Enabled:        true,
		Account:        strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_ACCOUNT")),
		User:           strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_USER")),
		Host:           strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_HOST")),
		PrivateKeyFile: strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_PRIVATE_KEY_FILE")),
	}
	policy, err := connector.NewSnowflakeDeploymentPolicy(cfg)
	if err != nil {
		t.Fatalf("load Snowflake deployment policy: %v", err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	return policy
}

func usingFakesnow() bool {
	host := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_HOST"))
	portRaw := strings.TrimSpace(os.Getenv("WALLABY_TEST_FAKESNOW_PORT"))
	forceFake := strings.TrimSpace(os.Getenv("WALLABY_TEST_FORCE_FAKESNOW")) == "1"
	return host != "" || portRaw != "" || forceFake
}

func snowflakeTestTimeout() time.Duration {
	if usingFakesnow() {
		return 15 * time.Second
	}
	return 2 * time.Minute
}
