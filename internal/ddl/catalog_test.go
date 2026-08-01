package ddl

import (
	"context"
	"errors"
	"testing"

	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCatalogScannerDoesNotAdvancePastRegistryFailure(t *testing.T) {
	t.Parallel()

	oldSchema := connector.Schema{
		Namespace: "public",
		Name:      "events",
		Version:   1,
		Columns:   []connector.Column{{Name: "id", Type: "bigint"}},
	}
	newSchema := oldSchema
	newSchema.Columns = append(newSchema.Columns, connector.Column{Name: "name", Type: "text"})

	t.Run("initial schema", func(t *testing.T) {
		registry := &catalogRegistryStub{registerErr: errors.New("register failed")}
		scanner := &CatalogScanner{Registry: registry}
		err := scanner.persist(context.Background(), map[string]connector.Schema{"public.events": oldSchema})
		if err == nil {
			t.Fatal("persist() error = nil, want registry failure")
		}
		if _, ok := scanner.last["public.events"]; ok {
			t.Fatal("scanner advanced its in-memory schema after persistence failed")
		}
	})

	t.Run("schema change", func(t *testing.T) {
		registry := &catalogRegistryStub{recordErr: errors.New("record failed")}
		scanner := &CatalogScanner{
			Registry: registry,
			last:     map[string]connector.Schema{"public.events": oldSchema},
		}
		err := scanner.persist(context.Background(), map[string]connector.Schema{"public.events": newSchema})
		if err == nil {
			t.Fatal("persist() error = nil, want registry failure")
		}
		if got := len(scanner.last["public.events"].Columns); got != 1 {
			t.Fatalf("persisted in-memory schema has %d columns, want old schema", got)
		}
		if registry.registerCalls != 0 {
			t.Fatalf("RegisterSchema() calls = %d, want none for an atomic catalog change", registry.registerCalls)
		}
	})

	t.Run("successful schema change", func(t *testing.T) {
		registry := &catalogRegistryStub{}
		scanner := &CatalogScanner{
			Registry: registry,
			last:     map[string]connector.Schema{"public.events": oldSchema},
		}
		if err := scanner.persist(context.Background(), map[string]connector.Schema{"public.events": newSchema}); err != nil {
			t.Fatal(err)
		}
		if got := scanner.last["public.events"].Version; got != 2 {
			t.Fatalf("schema version = %d, want 2", got)
		}
		if registry.recordCalls != 1 {
			t.Fatalf("RecordCatalogChange() calls = %d, want 1", registry.recordCalls)
		}
	})
}

type catalogRegistryStub struct {
	registerErr   error
	recordErr     error
	registerCalls int
	recordCalls   int
}

func (s *catalogRegistryStub) RegisterSchema(context.Context, connector.Schema) error {
	s.registerCalls++
	return s.registerErr
}

func (s *catalogRegistryStub) RecordCatalogChange(context.Context, connector.Schema, schema.Plan, string) (int64, error) {
	s.recordCalls++
	return 1, s.recordErr
}
