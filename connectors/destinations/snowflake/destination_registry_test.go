package snowflake

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
			err := (&Destination{}).open(context.Background(), connector.RuntimeSpec{Options: map[string]string{optDSN: "unused", key: value}}, factories)
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
	err = destination.open(context.Background(), connector.RuntimeSpec{Options: map[string]string{optDSN: "unused"}}, factories)
	if !errors.Is(err, registryErr) || !errors.Is(err, closeErr) {
		t.Fatalf("open() error = %v", err)
	}
	if registry.closeCalls != 1 || destination.db != nil || destination.registry != nil {
		t.Fatalf("cleanup: registry calls=%d db=%v registry=%v", registry.closeCalls, destination.db, destination.registry)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseManagedClosesDatabaseAndRegistryOnce(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectClose()
	registry := &closeTrackingRegistry{}
	destination := &Destination{db: db, registry: registry, managedProfile: connector.ManagedProfilePostgresToSnowflakeSQLV1, managedFlowIncarnation: "incarnation"}

	if err := destination.Close(context.Background()); err != nil {
		t.Fatalf("first Close() = %v", err)
	}
	if destination.db != nil || destination.registry != nil || destination.managedFlowIncarnation != "" || registry.closeCalls != 1 {
		t.Fatalf("first Close cleanup: db=%v registry=%v incarnation=%q calls=%d", destination.db, destination.registry, destination.managedFlowIncarnation, registry.closeCalls)
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

func TestCloseUnmanagedJoinsFinalizeDatabaseAndRegistryErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	finalizeErr := errors.New("finalize failed")
	dbErr := errors.New("database close failed")
	registryErr := errors.New("registry close failed")
	mock.ExpectExec("INSERT INTO").WillReturnError(finalizeErr)
	mock.ExpectClose().WillReturnError(dbErr)
	registry := &closeTrackingRegistry{closeErr: registryErr}
	schema := connector.Schema{Namespace: "public", Name: "events", Columns: []connector.Column{{Name: "id", Type: "int8"}}}
	destination := &Destination{
		db: db, registry: registry, batchMode: batchModeStaging, batchResolve: batchResolveAppend,
		stagingTables: map[string]tableInfo{"public.events": {schema: schema, table: "events"}},
	}

	err = destination.Close(context.Background())
	for _, want := range []error{finalizeErr, dbErr, registryErr} {
		if !errors.Is(err, want) {
			t.Errorf("Close() error = %v, missing %v", err, want)
		}
	}
	if destination.db != nil || destination.registry != nil || registry.closeCalls != 1 {
		t.Fatalf("cleanup: db=%v registry=%v calls=%d", destination.db, destination.registry, registry.closeCalls)
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
	registry, err := schemaregistry.NewRegistry(ctx, schemaregistry.Config{Type: "local", LocalDirectory: t.TempDir()})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	defer registry.Close()

	dest := &Destination{registry: registry}
	schemaV1 := connector.Schema{
		Name:      "orders",
		Namespace: "public",
		Columns: []connector.Column{
			{Name: "id", Type: "int8"},
		},
	}
	metaV1, err := dest.ensureSchema(ctx, schemaV1)
	if err != nil {
		t.Fatalf("ensure schema v1: %v", err)
	}
	if metaV1.Subject != "public.orders" {
		t.Fatalf("unexpected subject: %s", metaV1.Subject)
	}

	metaV1Again, err := dest.ensureSchema(ctx, schemaV1)
	if err != nil {
		t.Fatalf("ensure schema v1 again: %v", err)
	}
	if metaV1Again.Version != metaV1.Version {
		t.Fatalf("expected identical schema to keep version (v1=%d v1again=%d)", metaV1.Version, metaV1Again.Version)
	}

	schemaV2 := schemaV1
	schemaV2.Columns = append(schemaV2.Columns, connector.Column{Name: "status", Type: "text"})
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
	registry, err := schemaregistry.NewRegistry(ctx, schemaregistry.Config{Type: "local", LocalDirectory: t.TempDir()})
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	defer registry.Close()

	dest := &Destination{
		registry:        registry,
		registrySubject: "custom.subject",
	}
	schema := connector.Schema{
		Name:      "orders",
		Namespace: "public",
		Columns: []connector.Column{
			{Name: "id", Type: "int8"},
		},
	}
	meta, err := dest.ensureSchema(ctx, schema)
	if err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if meta.Subject != "custom.subject" {
		t.Fatalf("unexpected subject: %s", meta.Subject)
	}
}
