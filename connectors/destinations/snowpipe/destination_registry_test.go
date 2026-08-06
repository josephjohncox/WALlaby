package snowpipe

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
)

type closeTrackingRegistry struct {
	closeCalls int
	closeErr   error
}

func (*closeTrackingRegistry) Register(context.Context, schemaregistry.RegisterRequest) (schemaregistry.RegisterResult, error) {
	return schemaregistry.RegisterResult{}, nil
}

func (r *closeTrackingRegistry) Close() error {
	r.closeCalls++
	return r.closeErr
}

func TestOpenRejectsRegistryOptionsBeforeDatabaseOrRegistryCreation(t *testing.T) {
	for key, value := range map[string]string{
		schemaregistry.OptRegistryTimeout:        "soon",
		schemaregistry.OptRegistryApicurioCompat: "yes",
	} {
		t.Run(key, func(t *testing.T) {
			dbCalls := 0
			registryCalls := 0
			factories := destinationFactories{
				openDB: func(string, string) (*sql.DB, error) {
					dbCalls++
					return nil, nil
				},
				newRegistry: func(context.Context, schemaregistry.Config) (schemaregistry.Registry, error) {
					registryCalls++
					return nil, nil
				},
			}
			err := (&Destination{}).open(context.Background(), connector.Spec{Options: map[string]string{optDSN: "unused", key: value}}, factories)
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("open() error = %v", err)
			}
			if dbCalls != 0 || registryCalls != 0 {
				t.Fatalf("side effects before config error: db=%d registry=%d", dbCalls, registryCalls)
			}
		})
	}
}

func TestOpenRegistryFailureClosesDatabaseAndPartialRegistry(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectPing()
	mock.ExpectClose()
	closeErr := errors.New("registry close failed")
	registry := &closeTrackingRegistry{closeErr: closeErr}
	registryErr := errors.New("registry creation failed")
	factories := destinationFactories{
		openDB: func(string, string) (*sql.DB, error) { return db, nil },
		newRegistry: func(context.Context, schemaregistry.Config) (schemaregistry.Registry, error) {
			return registry, registryErr
		},
	}
	destination := &Destination{}
	err = destination.open(context.Background(), connector.Spec{Options: map[string]string{optDSN: "unused"}}, factories)
	if !errors.Is(err, registryErr) || !errors.Is(err, closeErr) {
		t.Fatalf("open() error = %v", err)
	}
	if registry.closeCalls != 1 || destination.db != nil || destination.registry != nil || destination.stagedTransport != nil {
		t.Fatalf("cleanup: registry calls=%d db=%v registry=%v transport=%v", registry.closeCalls, destination.db, destination.registry, destination.stagedTransport)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseJoinsDatabaseAndRegistryErrorsAndIsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	dbErr := errors.New("database close failed")
	registryErr := errors.New("registry close failed")
	mock.ExpectClose().WillReturnError(dbErr)
	registry := &closeTrackingRegistry{closeErr: registryErr}
	destination := &Destination{db: db, stagedTransport: db, registry: registry}

	err = destination.Close(context.Background())
	if !errors.Is(err, dbErr) || !errors.Is(err, registryErr) {
		t.Fatalf("first Close() error = %v", err)
	}
	if destination.db != nil || destination.stagedTransport != nil || destination.registry != nil || registry.closeCalls != 1 {
		t.Fatalf("cleanup: db=%v transport=%v registry=%v calls=%d", destination.db, destination.stagedTransport, destination.registry, registry.closeCalls)
	}
	if err := destination.Close(context.Background()); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	if registry.closeCalls != 1 {
		t.Fatalf("registry close calls = %d", registry.closeCalls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureSchemaRegistryVersionChanges(t *testing.T) {
	ctx := context.Background()
	registry, err := schemaregistry.NewRegistry(ctx, schemaregistry.Config{Type: "local"})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	defer registry.Close()

	dest := &Destination{registry: registry}
	schemaV1 := connector.Schema{
		Name:      "events",
		Namespace: "public",
		Columns: []connector.Column{
			{Name: "id", Type: "int8"},
		},
	}
	metaV1, err := dest.ensureSchema(ctx, schemaV1)
	if err != nil {
		t.Fatalf("ensure schema v1: %v", err)
	}
	if metaV1.Subject != "public.events" {
		t.Fatalf("unexpected subject: %s", metaV1.Subject)
	}

	schemaV2 := schemaV1
	schemaV2.Columns = append(schemaV2.Columns, connector.Column{Name: "payload", Type: "jsonb"})
	metaV2, err := dest.ensureSchema(ctx, schemaV2)
	if err != nil {
		t.Fatalf("ensure schema v2: %v", err)
	}
	if metaV2.Version == metaV1.Version {
		t.Fatalf("expected schema evolution to change registry version (v1=%d v2=%d)", metaV1.Version, metaV2.Version)
	}
}

func TestEnsureSchemaRespectsRegistrySubjectOverride(t *testing.T) {
	ctx := context.Background()
	registry, err := schemaregistry.NewRegistry(ctx, schemaregistry.Config{Type: "local"})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	defer registry.Close()

	dest := &Destination{
		registry:        registry,
		registrySubject: "snowpipe.custom.subject",
	}
	schema := connector.Schema{
		Name: "events",
		Columns: []connector.Column{
			{Name: "id", Type: "int8"},
		},
	}
	meta, err := dest.ensureSchema(ctx, schema)
	if err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if meta.Subject != "snowpipe.custom.subject" {
		t.Fatalf("unexpected subject: %s", meta.Subject)
	}
}
