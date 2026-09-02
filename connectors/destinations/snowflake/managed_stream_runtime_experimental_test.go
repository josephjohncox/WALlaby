//go:build snowpipe_streaming_rest_experimental

package snowflake

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func experimentalStreamPolicy(t *testing.T, enabled bool) connector.SnowflakeDeploymentPolicy {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key, enabled)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	return policy
}

func experimentalStreamSpec(t *testing.T) connector.RuntimeSpec {
	t.Helper()
	_, options := streamValidOptions(t)
	return connector.RuntimeSpec{Name: "stream", Type: connector.EndpointSnowflake, Options: options}
}

func TestExperimentalStreamingRuntimeAssemblyRequiresDeploymentCapability(t *testing.T) {
	if !ManagedStreamingTransportAvailable() {
		t.Fatal("experimental build did not link the Streaming REST adapter")
	}
	spec := experimentalStreamSpec(t)
	for name, policy := range map[string]connector.SnowflakeDeploymentPolicy{
		"disabled base":      {},
		"streaming disabled": experimentalStreamPolicy(t, false),
	} {
		t.Run(name, func(t *testing.T) {
			openCalls := 0
			destination := NewDestination(policy)
			err := destination.open(context.Background(), spec, destinationFactories{
				openDB: func(string, string) (*sql.DB, error) { openCalls++; return nil, errors.New("must not open") },
			})
			if err == nil || openCalls != 0 {
				t.Fatalf("deployment-disabled assembly error/calls=%v/%d", err, openCalls)
			}
		})
	}
}

func TestExperimentalStreamingRuntimeCloseWaitsForOperationSnapshot(t *testing.T) {
	policy := experimentalStreamPolicy(t, true)
	streamingPolicy, err := policy.StreamingRESTPolicy()
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectClose()
	destination := NewDestination(policy)
	destination.spec = experimentalStreamSpec(t)
	destination.db = db
	destination.managedProfile = connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
	destination.streamRuntimeProtocol = newFakeStreamProtocol()
	destination.streamConfig = streamTestConfig(t)
	destination.streamCatalogFingerprint = strings.Repeat("a", 64)
	destination.streamingPolicy = streamingPolicy
	_, unlock, err := destination.lockStreamRuntime()
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- destination.Close(context.Background()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned while an operation snapshot was active: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	unlock()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestExperimentalStreamingRuntimeRejectsMissingComposedProtocol(t *testing.T) {
	policy := experimentalStreamPolicy(t, true)
	destination := NewDestination(policy)
	destination.spec = experimentalStreamSpec(t)
	destination.db = &sql.DB{}
	destination.managedProfile = connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
	streamingPolicy, err := policy.StreamingRESTPolicy()
	if err != nil {
		t.Fatal(err)
	}
	destination.streamingPolicy = streamingPolicy
	if _, unlock, err := destination.lockStreamRuntime(); err == nil {
		unlock()
		t.Fatal("missing composed protocol unexpectedly admitted")
	}
}

func TestExperimentalPreparedStreamingRejectsRuntimeDriftBeforeSideEffects(t *testing.T) {
	policy := experimentalStreamPolicy(t, true)
	streamingPolicy, err := policy.StreamingRESTPolicy()
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeStreamProtocol()
	destination := NewDestination(policy)
	destination.spec = experimentalStreamSpec(t)
	destination.db = &sql.DB{}
	destination.managedProfile = connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1
	destination.streamRuntimeProtocol = fake
	destination.streamConfig = streamTestConfig(t)
	destination.streamConfig.configDigest = strings.Repeat("c", 64)
	destination.streamCatalogFingerprint = strings.Repeat("b", 64)
	destination.streamingPolicy = streamingPolicy
	prepared := &preparedManagedStreamTransaction{
		destination: destination,
		intent:      connector.DeliveryIntent{LogicalBatchID: "prepared-drift"},
		plan: managedStreamPlan{
			catalogFingerprint: strings.Repeat("a", 64),
			receipt:            managedStreamReceipt{catalogFingerprint: strings.Repeat("a", 64)},
		},
		runtimeFingerprint: strings.Repeat("a", 64),
		configDigest:       strings.Repeat("c", 64),
	}
	if _, err := prepared.Apply(context.Background()); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("prepared runtime drift error=%v, want conflict", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.appendedPayloads) != 0 || len(fake.requests) != 0 || len(fake.receipts) != 0 {
		t.Fatalf("prepared drift produced side effects: appends=%d requests=%d receipts=%d", len(fake.appendedPayloads), len(fake.requests), len(fake.receipts))
	}
}

func TestExperimentalStreamingRuntimeAssemblyComposesRESTAndSQLAndRollsBack(t *testing.T) {
	spec := experimentalStreamSpec(t)
	policy := experimentalStreamPolicy(t, true)
	for _, test := range []struct {
		name        string
		openRuntime func(context.Context, *sql.DB, streamConfig, connector.SnowflakeStreamingRESTPolicy) (streamProtocol, string, error)
		wantErr     string
	}{
		{name: "success", openRuntime: func(_ context.Context, db *sql.DB, _ streamConfig, capability connector.SnowflakeStreamingRESTPolicy) (streamProtocol, string, error) {
			if !capability.Enabled() || db == nil {
				return nil, "", errors.New("missing runtime authority")
			}
			return newFakeStreamProtocol(), strings.Repeat("a", 64), nil
		}},
		{name: "catalog failure", openRuntime: func(context.Context, *sql.DB, streamConfig, connector.SnowflakeStreamingRESTPolicy) (streamProtocol, string, error) {
			return nil, "", errors.New("catalog rejected")
		}, wantErr: "catalog rejected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectPing()
			mock.ExpectClose()
			destination := NewDestination(policy)
			err = destination.open(context.Background(), spec, destinationFactories{openDB: func(string, string) (*sql.DB, error) { return db, nil }, openStreamRuntime: test.openRuntime})
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) || destination.db != nil || destination.streamRuntimeProtocol != nil {
					t.Fatalf("rollback error/state=%v/%v/%v", err, destination.db, destination.streamRuntimeProtocol)
				}
			} else {
				if err != nil || destination.db == nil || destination.streamRuntimeProtocol == nil || destination.streamCatalogFingerprint == "" {
					t.Fatalf("assembled state error/db/protocol/fingerprint=%v/%v/%v/%q", err, destination.db, destination.streamRuntimeProtocol, destination.streamCatalogFingerprint)
				}
				_, unlock, err := destination.lockStreamRuntime()
				if err != nil {
					t.Fatal(err)
				}
				unlock()
				if err := destination.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
