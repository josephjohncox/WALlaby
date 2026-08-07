package runner

import (
	"fmt"
	"reflect"

	"github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	"github.com/josephjohncox/wallaby/connectors/destinations/duckdb"
	"github.com/josephjohncox/wallaby/connectors/destinations/ducklake"
	grpcdest "github.com/josephjohncox/wallaby/connectors/destinations/grpc"
	httpdest "github.com/josephjohncox/wallaby/connectors/destinations/http"
	icebergdest "github.com/josephjohncox/wallaby/connectors/destinations/iceberg"
	"github.com/josephjohncox/wallaby/connectors/destinations/kafka"
	"github.com/josephjohncox/wallaby/connectors/destinations/pgstream"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	"github.com/josephjohncox/wallaby/connectors/destinations/redpanda"
	"github.com/josephjohncox/wallaby/connectors/destinations/s3"
	"github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	"github.com/josephjohncox/wallaby/connectors/destinations/snowpipe"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// DestinationProfile is one closed configuration-controlled capability cell.
// Capabilities is an independent full oracle, not a derivative of connector output.
type DestinationProfile struct {
	ID           connector.CapabilityProfileID
	Options      map[string]string
	Capabilities connector.Capabilities
}

// DestinationRegistration is the sole first-party destination registry row.
// Factory construction, capability resolution, and generated support matrices
// all consume this table. Behavior evidence lives only in executable tests.
type DestinationRegistration struct {
	Type     connector.EndpointType
	New      func() connector.Destination
	Profiles []DestinationProfile
}

// ResolveCapabilities classifies a runtime spec, requires a registered profile,
// and compares the connector output with the registry's independent full oracle.
func (registration DestinationRegistration) ResolveCapabilities(destination connector.Destination, spec connector.RuntimeSpec) (connector.Capabilities, error) {
	if registration.Type != spec.Type {
		return connector.Capabilities{}, fmt.Errorf("destination capability registry row %s cannot resolve endpoint %s", registration.Type, spec.Type)
	}
	if destination == nil {
		if registration.New != nil {
			return connector.Capabilities{}, fmt.Errorf("destination %s capability resolution requires a runtime connector", spec.Type)
		}
		return connector.Capabilities{Support: connector.SupportPlaceholder}, nil
	}
	configured, configurationAware := destination.(connector.ConfiguredDestinationCapabilities)
	if !configurationAware {
		if len(registration.Profiles) != 0 {
			return connector.Capabilities{}, fmt.Errorf("destination %s has registered profiles but no classifier", spec.Type)
		}
		return connector.ResolveDestinationCapabilities(destination, spec)
	}
	profileID, err := configured.ClassifyCapabilityProfile(spec)
	if err != nil {
		return connector.Capabilities{}, err
	}
	var oracle *DestinationProfile
	for i := range registration.Profiles {
		if registration.Profiles[i].ID == profileID {
			oracle = &registration.Profiles[i]
			break
		}
	}
	if oracle == nil {
		return connector.Capabilities{}, fmt.Errorf("destination %s classified unregistered capability profile %q", spec.Type, profileID)
	}
	actual, err := connector.ResolveDestinationCapabilities(destination, spec)
	if err != nil {
		return connector.Capabilities{}, err
	}
	if !reflect.DeepEqual(actual, oracle.Capabilities) {
		return connector.Capabilities{}, fmt.Errorf("destination %s capability profile %q differs from registered oracle: actual=%+v oracle=%+v", spec.Type, profileID, actual, oracle.Capabilities)
	}
	return actual, nil
}

var destinationRegistry = []DestinationRegistration{
	{Type: connector.EndpointPostgres, New: func() connector.Destination { return &pgdest.Destination{} }},
	{Type: connector.EndpointPGStream, New: func() connector.Destination { return &pgstream.Destination{} }},
	{Type: connector.EndpointKafka, New: func() connector.Destination { return &kafka.Destination{} }, Profiles: []DestinationProfile{
		{ID: kafka.CapabilityProfileBase, Options: map[string]string{"transactional_producer": "false", "allow_oversize_skip": "false"}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: false, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: false, Lossy: false},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: false, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
		{ID: kafka.CapabilityProfileTransactionalOnly, Options: map[string]string{"transactional_producer": "true", "transactional_id": "wallaby-profile", "allow_oversize_skip": "false"}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: true, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: false, Lossy: false},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: false, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
		{ID: kafka.CapabilityProfileLossyOnly, Options: map[string]string{"transactional_producer": "false", "allow_oversize_skip": "true"}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: false, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: false, Lossy: true},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: false, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
		{ID: kafka.CapabilityProfileTransactionalLossy, Options: map[string]string{"transactional_producer": "true", "transactional_id": "wallaby-profile", "allow_oversize_skip": "true"}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: true, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: false, Lossy: true},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: false, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
	}},
	{Type: connector.EndpointRedpanda, New: func() connector.Destination { return &redpanda.Destination{} }, Profiles: []DestinationProfile{
		{ID: redpanda.CapabilityProfileBase, Options: map[string]string{"transactional_producer": "false", "allow_oversize_skip": "false"}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: false, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: false, Lossy: false},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: false, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
		{ID: redpanda.CapabilityProfileTransactionalOnly, Options: map[string]string{"transactional_producer": "true", "transactional_id": "wallaby-profile", "allow_oversize_skip": "false"}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: true, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: false, Lossy: false},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: false, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
		{ID: redpanda.CapabilityProfileLossyOnly, Options: map[string]string{"transactional_producer": "false", "allow_oversize_skip": "true"}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: false, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: false, Lossy: true},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: false, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
		{ID: redpanda.CapabilityProfileTransactionalLossy, Options: map[string]string{"transactional_producer": "true", "transactional_id": "wallaby-profile", "allow_oversize_skip": "true"}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: true, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: false, Lossy: true},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: false, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
	}},
	{Type: connector.EndpointS3, New: func() connector.Destination { return &s3.Destination{} }},
	{Type: connector.EndpointHTTP, New: func() connector.Destination { return &httpdest.Destination{} }},
	{Type: connector.EndpointGRPC, New: func() connector.Destination { return &grpcdest.Destination{} }},
	{Type: connector.EndpointSnowflake, New: func() connector.Destination { return &snowflake.Destination{} }, Profiles: []DestinationProfile{
		{ID: snowflake.CapabilityProfileBase, Options: map[string]string{"managed_profile": ""}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: false, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: true, Lossy: false},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: true, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatParquet, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
		{ID: snowflake.CapabilityProfileManagedSQL, Options: map[string]string{"managed_profile": connector.ManagedProfilePostgresToSnowflakeSQLV1}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: true, IdempotentReplay: true, ReplaySafe: true, ExecutesDDL: false, Lossy: false},
			TableWrites:           connector.TableWriteSemantics{Append: false, Upsert: true, ExplicitKey: true, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: true, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatParquet, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
		{ID: snowflake.CapabilityProfileManagedStaged, Options: map[string]string{"managed_profile": connector.ManagedProfilePostgresToSnowflakeStagedAppendV1}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: false, IdempotentReplay: true, ReplaySafe: true, ExecutesDDL: false, Lossy: false},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: true, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatParquet, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
		{ID: snowflake.CapabilityProfileManagedStreaming, Options: map[string]string{"managed_profile": connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: false, IdempotentReplay: true, ReplaySafe: true, ExecutesDDL: false, Lossy: false},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: true, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatParquet, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
	}},
	{Type: connector.EndpointSnowpipe, New: func() connector.Destination { return &snowpipe.Destination{} }},
	{Type: connector.EndpointClickHouse, New: func() connector.Destination { return &clickhouse.Destination{} }, Profiles: []DestinationProfile{
		{ID: clickhouse.CapabilityProfileBase, Options: map[string]string{"managed_profile": ""}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: false, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: true, Lossy: false},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: true, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatParquet, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
		{ID: clickhouse.CapabilityProfileManaged, Options: map[string]string{"managed_profile": connector.ManagedProfilePostgresToClickHouseAppendV1}, Capabilities: connector.Capabilities{
			Support: connector.SupportExperimental, Evidence: connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
			Delivery:              connector.DeliverySemantics{TransactionalBatch: false, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: false, Lossy: false},
			TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
			SupportsSchemaChanges: true, SupportsStreaming: true, SupportsBulkLoad: true, SupportsTypeMapping: true,
			SupportedWireFormats: []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatParquet, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
		}},
	}},
	{Type: connector.EndpointDuckDB, New: func() connector.Destination { return &duckdb.Destination{} }},
	{Type: connector.EndpointDuckLake, New: func() connector.Destination { return &ducklake.Destination{} }},
	{Type: connector.EndpointIceberg, New: func() connector.Destination { return &icebergdest.Destination{} }},
}

// DestinationRegistrations returns a defensive copy of the authoritative registry.
func DestinationRegistrations() []DestinationRegistration {
	registrations := make([]DestinationRegistration, len(destinationRegistry))
	copy(registrations, destinationRegistry)
	for i := range registrations {
		registrations[i].Profiles = append([]DestinationProfile(nil), registrations[i].Profiles...)
		for j := range registrations[i].Profiles {
			registrations[i].Profiles[j].Options = cloneOptions(registrations[i].Profiles[j].Options)
			registrations[i].Profiles[j].Capabilities.SupportedWireFormats = append([]connector.WireFormat(nil), registrations[i].Profiles[j].Capabilities.SupportedWireFormats...)
		}
	}
	return registrations
}

func cloneOptions(options map[string]string) map[string]string {
	if options == nil {
		return nil
	}
	clone := make(map[string]string, len(options))
	for key, value := range options {
		clone[key] = value
	}
	return clone
}

func destinationRegistration(endpointType connector.EndpointType) (DestinationRegistration, bool) {
	for _, registration := range destinationRegistry {
		if registration.Type == endpointType {
			return registration, true
		}
	}
	return DestinationRegistration{}, false
}

// DestinationContract is one executable support-matrix row.
type DestinationContract struct {
	Type          connector.EndpointType
	Capabilities  connector.Capabilities
	Runtime       bool
	ReconcilesDDL bool
	Profiles      []DestinationProfile
}

// DestinationContracts validates and resolves every authoritative registry row.
func DestinationContracts() ([]DestinationContract, error) {
	contracts := make([]DestinationContract, 0, len(destinationRegistry))
	seen := make(map[connector.EndpointType]struct{}, len(destinationRegistry))
	for _, registration := range destinationRegistry {
		if registration.Type == "" {
			return nil, fmt.Errorf("destination registry row lacks type")
		}
		if _, exists := seen[registration.Type]; exists {
			return nil, fmt.Errorf("duplicate destination registry row %s", registration.Type)
		}
		seen[registration.Type] = struct{}{}
		var destination connector.Destination
		if registration.New != nil {
			destination = registration.New()
			if destination == nil {
				return nil, fmt.Errorf("destination registry constructor %s returned nil", registration.Type)
			}
		}
		configured, configurationAware := destination.(connector.ConfiguredDestinationCapabilities)
		if configurationAware != (len(registration.Profiles) > 0) {
			return nil, fmt.Errorf("destination registry row %s configuration-aware resolver=%t profiles=%d", registration.Type, configurationAware, len(registration.Profiles))
		}
		if configurationAware {
			declared := make(map[connector.CapabilityProfileID]struct{})
			for _, profileID := range configured.CapabilityProfileIDs() {
				if profileID == "" {
					return nil, fmt.Errorf("destination %s classifier declares an empty profile ID", registration.Type)
				}
				if _, duplicate := declared[profileID]; duplicate {
					return nil, fmt.Errorf("destination %s classifier declares duplicate profile %q", registration.Type, profileID)
				}
				declared[profileID] = struct{}{}
			}
			registered := make(map[connector.CapabilityProfileID]struct{})
			for _, profile := range registration.Profiles {
				if profile.ID == "" {
					return nil, fmt.Errorf("destination registry row %s has unnamed capability profile", registration.Type)
				}
				if _, duplicate := registered[profile.ID]; duplicate {
					return nil, fmt.Errorf("destination registry row %s has duplicate capability profile %s", registration.Type, profile.ID)
				}
				registered[profile.ID] = struct{}{}
				if _, ok := declared[profile.ID]; !ok {
					return nil, fmt.Errorf("destination registry row %s contains classifier-undeclared profile %q", registration.Type, profile.ID)
				}
				spec := connector.RuntimeSpec{Name: string(registration.Type), Type: registration.Type, Options: cloneOptions(profile.Options)}
				classified, err := configured.ClassifyCapabilityProfile(spec)
				if err != nil {
					return nil, fmt.Errorf("destination registry row %s profile %s classifier: %w", registration.Type, profile.ID, err)
				}
				if classified != profile.ID {
					return nil, fmt.Errorf("destination registry row %s profile %s options classify as %s", registration.Type, profile.ID, classified)
				}
				profileCapabilities, err := registration.ResolveCapabilities(destination, spec)
				if err != nil {
					return nil, fmt.Errorf("destination registry row %s profile %s: %w", registration.Type, profile.ID, err)
				}
				if !reflect.DeepEqual(profileCapabilities, profile.Capabilities) {
					return nil, fmt.Errorf("destination registry row %s profile %s did not resolve its exact oracle", registration.Type, profile.ID)
				}
				if err := profileCapabilities.ValidateSupport(); err != nil {
					return nil, fmt.Errorf("destination registry row %s profile %s: %w", registration.Type, profile.ID, err)
				}
			}
			for profileID := range declared {
				if _, ok := registered[profileID]; !ok {
					return nil, fmt.Errorf("destination %s classifier profile %q is absent from the registry", registration.Type, profileID)
				}
			}
		}
		spec := connector.RuntimeSpec{Name: string(registration.Type), Type: registration.Type}
		capabilities, err := registration.ResolveCapabilities(destination, spec)
		if err != nil {
			return nil, fmt.Errorf("destination registry row %s default profile: %w", registration.Type, err)
		}
		_, reconcilesDDL := destination.(connector.DDLReconciler)
		contracts = append(contracts, DestinationContract{
			Type: registration.Type, Capabilities: capabilities, Runtime: destination != nil,
			ReconcilesDDL: reconcilesDDL, Profiles: append([]DestinationProfile(nil), registration.Profiles...),
		})
	}
	return contracts, nil
}
