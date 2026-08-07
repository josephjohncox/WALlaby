package runner

import (
	"context"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

type customTestSource struct{}

func (*customTestSource) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (*customTestSource) Read(context.Context) (connector.Batch, error) {
	return connector.Batch{}, nil
}
func (*customTestSource) Ack(context.Context, connector.Checkpoint) error { return nil }
func (*customTestSource) Close(context.Context) error                     { return nil }
func (*customTestSource) Capabilities() connector.Capabilities            { return connector.Capabilities{} }

type customTestDestination struct{}

func (*customTestDestination) Open(context.Context, connector.RuntimeSpec) error { return nil }
func (*customTestDestination) Write(context.Context, connector.Batch) error      { return nil }
func (*customTestDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (*customTestDestination) TypeMappings() map[string]string      { return nil }
func (*customTestDestination) Close(context.Context) error          { return nil }
func (*customTestDestination) Capabilities() connector.Capabilities { return connector.Capabilities{} }

func TestFactoryRejectsModeIncompatiblePostgresSelection(t *testing.T) {
	t.Parallel()
	factory := Factory{}
	for _, spec := range []connector.RuntimeSpec{
		{Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeBackfill, "publication_tables": "public.cdc"}},
		{Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeBackfill, "sync_publication": "false"}},
	} {
		if _, err := factory.Source(spec); err == nil {
			t.Fatalf("mode-incompatible source accepted: %v", spec.Options)
		}
	}
	if _, err := factory.Source(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeCDC, "publication_tables": "public.cdc", "bootstrap": "required", "tables": "public.snapshot"}}); err != nil {
		t.Fatalf("valid CDC/bootstrap selection rejected: %v", err)
	}
	if _, err := factory.Source(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeBackfill, "tables": "public.backfill"}}); err != nil {
		t.Fatalf("valid backfill selection rejected: %v", err)
	}
}

func TestFactoryUsesInjectedCustomRegistryForBothRoles(t *testing.T) {
	registry := connector.NewRegistry()
	if err := registry.RegisterSource("custom-source", func() connector.Source { return &customTestSource{} }); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterDestination("custom-destination", func() connector.Destination { return &customTestDestination{} }); err != nil {
		t.Fatal(err)
	}
	factory := Factory{ConnectorRegistry: registry}
	if _, err := factory.Source(connector.RuntimeSpec{Type: "custom-source"}); err != nil {
		t.Fatalf("custom source: %v", err)
	}
	if _, err := factory.Destinations([]connector.RuntimeSpec{{Name: "custom", Type: "custom-destination"}}); err != nil {
		t.Fatalf("custom destination: %v", err)
	}
	if _, err := factory.Source(connector.RuntimeSpec{Type: "unknown"}); err == nil {
		t.Fatal("unknown custom source accepted")
	}
	if _, err := factory.Destinations([]connector.RuntimeSpec{{Type: "unknown"}}); err == nil {
		t.Fatal("unknown custom destination accepted")
	}
}
