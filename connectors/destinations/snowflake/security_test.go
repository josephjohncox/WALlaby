package snowflake

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql"
	"errors"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
)

func snowflakeTestDSN() string {
	return "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"
}

func snowflakeTestPolicy(t *testing.T) connector.SnowflakeDeploymentPolicy {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	return policy
}

func TestSnowflakeRejectsUnsafeDSNBeforeDatabaseOpen(t *testing.T) {
	openCalls := 0
	destination := NewDestination(snowflakeTestPolicy(t))
	err := destination.open(context.Background(), connector.RuntimeSpec{
		Type: connector.EndpointSnowflake,
		Options: map[string]string{
			"dsn": "user:never-print-this@account/db/schema",
		},
	}, destinationFactories{
		openDB: func(string, string) (*sql.DB, error) {
			openCalls++
			return nil, errors.New("unexpected database open")
		},
		newRegistry: func(context.Context, schemaregistry.Config) (schemaregistry.Registry, error) {
			return nil, errors.New("unexpected registry open")
		},
	})
	if !errors.Is(err, connector.ErrUnsafeSnowflakeDSN) {
		t.Fatalf("open() error=%v", err)
	}
	if openCalls != 0 {
		t.Fatalf("unsafe DSN reached database open %d times", openCalls)
	}
}

func TestSnowflakeRejectsMismatchedEndpointTypeBeforeDatabaseOpen(t *testing.T) {
	openCalls := 0
	destination := NewDestination(snowflakeTestPolicy(t))
	err := destination.open(context.Background(), connector.RuntimeSpec{
		Type: connector.EndpointPostgres, Options: map[string]string{"dsn": snowflakeTestDSN()},
	}, destinationFactories{openDB: func(string, string) (*sql.DB, error) { openCalls++; return nil, nil }})
	if err == nil || openCalls != 0 {
		t.Fatalf("mismatched endpoint error=%v openCalls=%d", err, openCalls)
	}
}

func TestSnowflakeDisabledBeforeDatabaseOpen(t *testing.T) {
	openCalls := 0
	destination := NewDestination(connector.SnowflakeDeploymentPolicy{})
	err := destination.open(context.Background(), connector.RuntimeSpec{
		Type:    connector.EndpointSnowflake,
		Options: map[string]string{"dsn": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"},
	}, destinationFactories{
		openDB: func(string, string) (*sql.DB, error) {
			openCalls++
			return nil, errors.New("unexpected database open")
		},
		newRegistry: func(context.Context, schemaregistry.Config) (schemaregistry.Registry, error) {
			return nil, errors.New("unexpected registry open")
		},
	})
	if !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
		t.Fatalf("open() error=%v", err)
	}
	if openCalls != 0 {
		t.Fatalf("disabled execution reached database open %d times", openCalls)
	}
}
