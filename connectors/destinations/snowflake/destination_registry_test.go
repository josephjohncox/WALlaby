package snowflake

import (
	"context"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
)

func TestEnsureSchemaRegistryVersionChanges(t *testing.T) {
	ctx := context.Background()
	registry, err := schemaregistry.NewRegistry(ctx, schemaregistry.Config{Type: "local"})
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
	registry, err := schemaregistry.NewRegistry(ctx, schemaregistry.Config{Type: "local"})
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
