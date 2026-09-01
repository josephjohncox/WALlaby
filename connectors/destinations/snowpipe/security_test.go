package snowpipe

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

func snowpipeTestDSN() string {
	return "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"
}

func snowpipeTestPolicy(t *testing.T) connector.SnowflakeDeploymentPolicy {
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

func TestSnowpipeRejectsMismatchedEndpointTypeBeforeDatabaseOpen(t *testing.T) {
	openCalls := 0
	destination := NewDestination(snowpipeTestPolicy(t))
	err := destination.open(context.Background(), connector.RuntimeSpec{
		Type: connector.EndpointPostgres, Options: map[string]string{"dsn": snowpipeTestDSN()},
	}, destinationFactories{openDB: func(string, string) (*sql.DB, error) { openCalls++; return nil, nil }})
	if err == nil || openCalls != 0 {
		t.Fatalf("mismatched endpoint error=%v openCalls=%d", err, openCalls)
	}
}

func TestSnowpipeRejectsUnsafeOrDisabledDSNBeforeDatabaseOpen(t *testing.T) {
	tests := []struct {
		name   string
		policy connector.SnowflakeDeploymentPolicy
		dsn    string
		want   error
	}{
		{name: "unsafe", policy: snowpipeTestPolicy(t), dsn: "user@account/db/schema?refresh_token=never-print-this", want: connector.ErrUnsafeSnowflakeDSN},
		{name: "disabled", policy: connector.SnowflakeDeploymentPolicy{}, dsn: snowpipeTestDSN(), want: connector.ErrSnowflakeExecutionDisabled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			openCalls := 0
			destination := NewDestination(test.policy)
			err := destination.open(context.Background(), connector.RuntimeSpec{
				Type:    connector.EndpointSnowpipe,
				Options: map[string]string{"dsn": test.dsn},
			}, destinationFactories{
				openDB: func(string, string) (*sql.DB, error) {
					openCalls++
					return nil, errors.New("unexpected database open")
				},
				newRegistry: func(context.Context, schemaregistry.Config) (schemaregistry.Registry, error) {
					return nil, errors.New("unexpected registry open")
				},
			})
			if !errors.Is(err, test.want) {
				t.Fatalf("open() error=%v want=%v", err, test.want)
			}
			if openCalls != 0 {
				t.Fatalf("rejected execution reached database open %d times", openCalls)
			}
		})
	}
}
