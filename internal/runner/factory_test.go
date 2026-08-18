package runner

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestFactorySnowflakePolicyDeniesBeforeConstruction(t *testing.T) {
	specs := []connector.RuntimeSpec{
		{Type: connector.EndpointSnowflake, Options: map[string]string{"dsn": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"}},
		{Type: connector.EndpointSnowpipe, Options: map[string]string{"dsn": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"}},
	}
	for _, spec := range specs {
		if _, err := (Factory{}).Destinations([]connector.RuntimeSpec{spec}); !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
			t.Fatalf("destination %s error=%v", spec.Type, err)
		}
	}
}

func TestFactoryConstructionConsumesAuthoritativeDestinationRegistry(t *testing.T) {
	factory := Factory{SnowflakePolicy: testSnowflakePolicy(t)}
	for _, registration := range DestinationRegistrations() {
		if registration.New == nil {
			if _, err := factory.destination(connector.RuntimeSpec{Type: registration.Type}); err == nil {
				t.Fatalf("placeholder %s unexpectedly constructed", registration.Type)
			}
			continue
		}
		spec := connector.RuntimeSpec{Type: registration.Type}
		if connector.IsSnowflakeEndpoint(registration.Type) {
			spec.Options = map[string]string{"dsn": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"}
		}
		got, err := factory.destination(spec)
		if err != nil {
			t.Fatal(err)
		}
		want := registration.New()
		if reflect.TypeOf(got) != reflect.TypeOf(want) {
			t.Fatalf("%s factory type=%T registry type=%T", registration.Type, got, want)
		}
	}
}

func TestDestinationCatalogCoversEveryEndpointType(t *testing.T) {
	contracts, err := DestinationContracts()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[connector.EndpointType]DestinationContract, len(contracts))
	for _, contract := range contracts {
		if _, exists := got[contract.Type]; exists {
			t.Fatalf("duplicate destination contract for %s", contract.Type)
		}
		got[contract.Type] = contract
		if err := contract.Capabilities.ValidateSupport(); err != nil {
			t.Fatalf("%s support contract: %v", contract.Type, err)
		}
	}
	endpointTypes := destinationEndpointConstants(t)
	for endpointType := range endpointTypes {
		if _, ok := got[endpointType]; !ok {
			t.Errorf("destination registry missing endpoint constant %s", endpointType)
		}
	}
	if len(got) != len(endpointTypes) {
		t.Fatalf("destination registry count=%d, endpoint constants=%d", len(got), len(endpointTypes))
	}
	if !got[connector.EndpointPostgres].ReconcilesDDL {
		t.Fatal("postgres destination must reconcile ambiguous DDL execution")
	}
	for endpointType, contract := range got {
		if endpointType != connector.EndpointPostgres && contract.ReconcilesDDL {
			t.Errorf("%s unexpectedly declares DDL reconciliation", endpointType)
		}
	}
}

func TestDestinationRegistryPlaceholderContracts(t *testing.T) {
	for _, registration := range DestinationRegistrations() {
		if registration.New != nil {
			continue
		}
		capabilities, err := registration.ResolveCapabilities(nil, connector.RuntimeSpec{Type: registration.Type})
		if err != nil {
			t.Fatal(err)
		}
		if capabilities.Support != connector.SupportPlaceholder || capabilities.TableWrites != (connector.TableWriteSemantics{}) || capabilities.Delivery != (connector.DeliverySemantics{}) {
			t.Fatalf("placeholder %s capabilities=%+v", registration.Type, capabilities)
		}
	}
}

func TestConfigurationControlledCapabilityProfilesAreExactAndExhaustive(t *testing.T) {
	for _, registration := range DestinationRegistrations() {
		if registration.New == nil {
			continue
		}
		destination := registration.New()
		configured, configurationAware := destination.(connector.ConfiguredDestinationCapabilities)
		if configurationAware != (len(registration.Profiles) > 0) {
			t.Fatalf("%s configuration-aware=%t profiles=%d", registration.Type, configurationAware, len(registration.Profiles))
		}
		if !configurationAware {
			continue
		}
		declared := make(map[connector.CapabilityProfileID]struct{})
		for _, profileID := range configured.CapabilityProfileIDs() {
			if _, duplicate := declared[profileID]; duplicate {
				t.Fatalf("%s duplicate classifier profile %q", registration.Type, profileID)
			}
			declared[profileID] = struct{}{}
		}
		registered := make(map[connector.CapabilityProfileID]struct{})
		for _, profile := range registration.Profiles {
			if _, duplicate := registered[profile.ID]; duplicate {
				t.Fatalf("%s duplicate registry profile %q", registration.Type, profile.ID)
			}
			registered[profile.ID] = struct{}{}
			if _, ok := declared[profile.ID]; !ok {
				t.Fatalf("%s registry profile %q is not classifier-declared", registration.Type, profile.ID)
			}
			spec := connector.RuntimeSpec{Name: string(registration.Type), Type: registration.Type, Options: profile.Options}
			classified, err := configured.ClassifyCapabilityProfile(spec)
			if err != nil {
				t.Fatalf("%s/%s classifier: %v", registration.Type, profile.ID, err)
			}
			if classified != profile.ID {
				t.Fatalf("%s/%s classified as %s", registration.Type, profile.ID, classified)
			}
			capabilities, err := registration.ResolveCapabilities(destination, spec)
			if err != nil {
				t.Fatalf("%s/%s: %v", registration.Type, profile.ID, err)
			}
			if !reflect.DeepEqual(capabilities, profile.Capabilities) {
				t.Fatalf("%s/%s capabilities=\n%+v\nregistry oracle=\n%+v", registration.Type, profile.ID, capabilities, profile.Capabilities)
			}
			assertPreIOPolicyMatrix(t, destination, spec, profile.Capabilities)
		}
		for profileID := range declared {
			if _, ok := registered[profileID]; !ok {
				t.Fatalf("%s classifier profile %q is not registered", registration.Type, profileID)
			}
		}
	}
}

func assertPreIOPolicyMatrix(t *testing.T, destination connector.Destination, spec connector.RuntimeSpec, capabilities connector.Capabilities) {
	t.Helper()
	config := stream.DestinationConfig{Spec: spec, Dest: destination}
	policies := []connector.TableWritePolicy{
		{Mode: connector.ResolvedWriteAppend},
		{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}},
		{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"id"}, WatermarkColumn: "updated_at"},
	}
	for _, policy := range policies {
		err := stream.ValidateDestinationTablePolicy(config, policy)
		claimed := capabilities.TableWrites.Append && policy.Mode == connector.ResolvedWriteAppend ||
			capabilities.TableWrites.Upsert && capabilities.TableWrites.ExplicitKey && policy.Mode == connector.ResolvedWriteUpsert &&
				(policy.WatermarkColumn == "" || capabilities.TableWrites.WatermarkGuard)
		if claimed != (err == nil) {
			t.Fatalf("%s policy %+v pre-I/O validation error=%v capabilities=%+v", spec.Type, policy, err, capabilities.TableWrites)
		}
	}
	if !capabilities.Delivery.ExecutesDDL {
		if err := stream.ValidateDestinationContracts([]stream.DestinationConfig{config}, stream.AckPolicyAll, "", true); err == nil {
			t.Fatalf("%s unclaimed DDL reached executable flow admission", spec.Type)
		}
	}
}

func TestFactoryRejectsRegisteredCapabilityOracleDrift(t *testing.T) {
	var registrationIndex int
	found := false
	for i := range destinationRegistry {
		if destinationRegistry[i].Type == connector.EndpointKafka {
			registrationIndex = i
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Kafka registry row not found")
	}
	original := destinationRegistry[registrationIndex].Profiles[0].Capabilities
	destinationRegistry[registrationIndex].Profiles[0].Capabilities.Delivery.Lossy = true
	t.Cleanup(func() { destinationRegistry[registrationIndex].Profiles[0].Capabilities = original })
	if _, err := (Factory{}).destination(connector.RuntimeSpec{Type: connector.EndpointKafka}); err == nil {
		t.Fatal("factory accepted connector capabilities that differ from the registered full oracle")
	}
}

func TestConfiguredCapabilityProfilesRejectUnknownValuesBeforeIO(t *testing.T) {
	tests := []connector.RuntimeSpec{
		{Name: "kafka-invalid-transactional-bool", Type: connector.EndpointKafka, Options: map[string]string{"transactional_producer": "sometimes"}},
		{Name: "kafka-invalid-lossy-bool", Type: connector.EndpointKafka, Options: map[string]string{"allow_oversize_skip": "1"}},
		{Name: "kafka-missing-transaction-id", Type: connector.EndpointKafka, Options: map[string]string{"transactional_producer": "true"}},
		{Name: "kafka-unclaimed-transaction-id", Type: connector.EndpointKafka, Options: map[string]string{"transactional_id": "unclassified"}},
		{Name: "redpanda-invalid-transactional-bool", Type: connector.EndpointRedpanda, Options: map[string]string{"transactional_producer": "TRUE"}},
		{Name: "redpanda-invalid-lossy-bool", Type: connector.EndpointRedpanda, Options: map[string]string{"allow_oversize_skip": "drop"}},
		{Name: "snowflake", Type: connector.EndpointSnowflake, Options: map[string]string{"managed_profile": "future-profile"}},
		{Name: "clickhouse", Type: connector.EndpointClickHouse, Options: map[string]string{"managed_profile": "future-profile"}},
	}
	for _, spec := range tests {
		registration, ok := destinationRegistration(spec.Type)
		if !ok {
			t.Fatal(spec.Type)
		}
		destination := registration.New()
		if _, err := registration.ResolveCapabilities(destination, spec); err == nil {
			t.Fatalf("%s unknown profile resolved", spec.Type)
		}
		if _, err := (Factory{}).destination(spec); err == nil {
			t.Fatalf("%s unknown profile passed factory construction", spec.Type)
		}
		config := stream.DestinationConfig{Spec: spec, Dest: destination}
		if err := stream.ValidateDestinationTablePolicy(config, connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}); err == nil {
			t.Fatalf("%s unknown profile passed pre-I/O policy validation", spec.Type)
		}
	}
}

func TestDestinationRegistryRejectsUnclaimedPoliciesBeforeWrite(t *testing.T) {
	for _, registration := range DestinationRegistrations() {
		if registration.New == nil {
			continue
		}
		destination := registration.New()
		spec := connector.RuntimeSpec{Name: string(registration.Type), Type: registration.Type}
		capabilities, err := registration.ResolveCapabilities(destination, spec)
		if err != nil {
			t.Fatal(err)
		}
		assertPreIOPolicyMatrix(t, destination, spec, capabilities)
	}
}

func TestFactoryDestinationsResolveRegistryCapabilities(t *testing.T) {
	contracts, err := DestinationContracts()
	if err != nil {
		t.Fatal(err)
	}
	for _, contract := range contracts {
		if err := contract.Capabilities.ValidateSupport(); err != nil {
			t.Fatalf("%s support contract: %v", contract.Type, err)
		}
		if contract.Runtime && contract.Capabilities.Support == connector.SupportPlaceholder {
			t.Fatalf("runtime destination %s is marked placeholder", contract.Type)
		}
		if !contract.Runtime && contract.Capabilities.Support != connector.SupportPlaceholder {
			t.Fatalf("non-runtime destination %s is not a placeholder", contract.Type)
		}
	}
}

func destinationEndpointConstants(t *testing.T) map[connector.EndpointType]struct{} {
	t.Helper()
	source, err := os.ReadFile(filepath.Join(repositoryRoot(t), "pkg", "connector", "connector.go"))
	if err != nil {
		t.Fatal(err)
	}
	matches := regexp.MustCompile(`(?m)^\s*Endpoint\w+\s+EndpointType\s*=\s*"([^"]+)"`).FindAllSubmatch(source, -1)
	if len(matches) == 0 {
		t.Fatal("no endpoint constants found")
	}
	result := make(map[connector.EndpointType]struct{}, len(matches))
	for _, match := range matches {
		result[connector.EndpointType(match[1])] = struct{}{}
	}
	return result
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve runner test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(current), "..", ".."))
}
