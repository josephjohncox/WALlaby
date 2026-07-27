package runner

import (
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestDestinationCatalogCoversEveryEndpointType(t *testing.T) {
	t.Parallel()

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
	allTypes := []connector.EndpointType{
		connector.EndpointPostgres,
		connector.EndpointSnowflake,
		connector.EndpointS3,
		connector.EndpointKafka,
		connector.EndpointHTTP,
		connector.EndpointGRPC,
		connector.EndpointProto,
		connector.EndpointPGStream,
		connector.EndpointSnowpipe,
		connector.EndpointParquet,
		connector.EndpointDuckDB,
		connector.EndpointDuckLake,
		connector.EndpointBufStream,
		connector.EndpointClickHouse,
		connector.EndpointIceberg,
	}
	for _, endpointType := range allTypes {
		if _, ok := got[endpointType]; !ok {
			t.Errorf("destination contract missing for %s", endpointType)
		}
	}
	if len(got) != len(allTypes) {
		t.Fatalf("destination contract count=%d, want %d", len(got), len(allTypes))
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

func TestFactoryDestinationsDeclareOperationalContracts(t *testing.T) {
	t.Parallel()

	factory := Factory{}
	types := []connector.EndpointType{
		connector.EndpointPostgres,
		connector.EndpointSnowflake,
		connector.EndpointS3,
		connector.EndpointKafka,
		connector.EndpointHTTP,
		connector.EndpointGRPC,
		connector.EndpointPGStream,
		connector.EndpointSnowpipe,
		connector.EndpointDuckDB,
		connector.EndpointDuckLake,
		connector.EndpointBufStream,
		connector.EndpointClickHouse,
		connector.EndpointIceberg,
	}
	for _, endpointType := range types {
		t.Run(string(endpointType), func(t *testing.T) {
			t.Parallel()
			spec := connector.Spec{Name: string(endpointType), Type: endpointType}
			destination, err := factory.destination(spec)
			if err != nil {
				t.Fatal(err)
			}
			capabilities := connector.ResolveDestinationCapabilities(destination, spec)
			if err := capabilities.ValidateSupport(); err != nil {
				t.Fatalf("invalid support contract: %v", err)
			}
			if !capabilities.Delivery.Declared {
				t.Fatal("delivery semantics are not declared")
			}
			if capabilities.Delivery.ExecutesDDL && !capabilities.SupportsDDL {
				t.Fatal("DDL execution declared without DDL support")
			}
		})
	}
}
