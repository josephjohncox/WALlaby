package workflow

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestTypedEndpointPersistenceIsDeterministicAndRejectsLegacyRows(t *testing.T) {
	destination := connector.RuntimeSpec{Name: "destination", Type: connector.EndpointHTTP, Options: map[string]string{
		"url": "https://example.test/hook", "headers": "x-a:one,x-b:two", "max_retries": "2",
	}}
	definition := mappedTestFlow(flow.Flow{
		ID: "typed-persistence",
		Source: workflowTestSource(connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{
			"dsn": "postgres://source", "publication_tables": "public.a,public.b", "create_slot": "false",
		}}),
		Destinations: []*wallabypb.Endpoint{workflowTestDestination(destination)},
	})
	firstSource, firstDestinations, _, err := marshalFlowFields(definition)
	if err != nil {
		t.Fatal(err)
	}
	secondSource, secondDestinations, _, err := marshalFlowFields(definition)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstSource, secondSource) || !bytes.Equal(firstDestinations, secondDestinations) {
		t.Fatalf("typed persistence is nondeterministic:\n%s\n%s", firstSource, secondSource)
	}
	for _, payload := range [][]byte{firstSource, firstDestinations} {
		if bytes.Contains(payload, []byte(`"type"`)) || bytes.Contains(payload, []byte(`"options"`)) {
			t.Fatalf("legacy endpoint shape persisted: %s", payload)
		}
	}

	legacy := []byte(`{"Name":"source","Type":"postgres","Options":{"dsn":"postgres://legacy"}}`)
	if _, err := unmarshalPersistedEndpoint(legacy, endpointcodec.RoleSource); err == nil || !strings.Contains(err.Error(), "recreate") {
		t.Fatalf("legacy endpoint row error=%v, want explicit recreate instruction", err)
	}
}

func TestPersistedCustomEndpointsHydrateOnlyThroughInjectedRegistry(t *testing.T) {
	t.Parallel()
	registry := connector.NewRegistry()
	if err := registry.RegisterSource("workflow-test-source", func() connector.Source { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterDestination("workflow-test-destination", func() connector.Destination { return nil }); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		role endpointcodec.Role
		spec connector.RuntimeSpec
	}{
		{role: endpointcodec.RoleSource, spec: connector.RuntimeSpec{Name: "source", Type: "workflow-test-source", Options: map[string]string{"exact": " source bytes "}}},
		{role: endpointcodec.RoleDestination, spec: connector.RuntimeSpec{Name: "destination", Type: "workflow-test-destination", Options: map[string]string{"exact": " destination bytes "}}},
	}
	for _, test := range cases {
		endpoint, err := endpointcodec.Encode(test.spec, test.role)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := marshalPersistedEndpoint(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := unmarshalPersistedEndpoint(payload, test.role); err == nil {
			t.Fatalf("default registry unexpectedly hydrated %q", test.spec.Type)
		}
		got, err := unmarshalPersistedEndpointWithRegistry(payload, test.role, registry)
		if err != nil {
			t.Fatal(err)
		}
		if got.GetName() != test.spec.Name || got.GetCustom().GetConnectorType() != string(test.spec.Type) || got.GetCustom().GetOptions()["exact"] != test.spec.Options["exact"] {
			t.Fatalf("hydrated custom endpoint=%#v, want %#v", got, test.spec)
		}
	}
}

func TestWorkflowPersistenceSourceDoesNotJSONEncodeRuntimeSpecs(t *testing.T) {
	t.Parallel()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate workflow source")
	}
	payload, err := os.ReadFile(filepath.Join(filepath.Dir(current), "postgres.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(payload)
	for _, forbidden := range []string{
		"json.Marshal(f.Source", "json.Marshal(f.Destinations", "json.Marshal(f.Source.Options", "json.Marshal(f.Destinations[",
		"json.Unmarshal(data, &connector.RuntimeSpec", "json.Unmarshal(data, &[]connector.RuntimeSpec",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("workflow persistence directly JSON-encodes runtime RuntimeSpec via %q", forbidden)
		}
	}
	for _, required := range []string{"marshalPersistedEndpoint(f.Source)", "marshalPersistedEndpoints(f.Destinations)", "protojson.MarshalOptions"} {
		if !strings.Contains(source, required) {
			t.Fatalf("typed endpoint persistence boundary %q is missing", required)
		}
	}
}

func TestExecutionIdentityIncludesTypedEndpointsAndMatchesMemoryDecision(t *testing.T) {
	base := mappedTestFlow(flow.Flow{ID: "identity", Source: workflowTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres}), Destinations: []*wallabypb.Endpoint{workflowTestDestination(connector.RuntimeSpec{Name: "destination", Type: connector.EndpointPostgres})}})
	changed := flow.Clone(base)
	changed.Source.GetPostgresSource().Connection = &wallabypb.PostgresConnectionConfig{Dsn: "postgres://changed"}
	equal, err := flow.ExecutionIdentityEqual(base, changed)
	if err != nil {
		t.Fatal(err)
	}
	if equal {
		t.Fatal("source typed configuration change did not change execution identity")
	}
	engine := NewMemoryEngine()
	created, err := engine.Create(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	changed.State = flow.StateRunning
	if _, err := engine.Update(context.Background(), changed); err == nil || !strings.Contains(err.Error(), "invalid flow state transition") {
		t.Fatalf("memory identity decision=%v, want running update rejection", err)
	}
}
