package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"google.golang.org/protobuf/proto"
)

func TestEndpointTerraformRoundTrips(t *testing.T) {
	t.Parallel()
	for _, endpoint := range []*wallabypb.Endpoint{
		{Name: "redpanda", Config: &wallabypb.Endpoint_Redpanda{Redpanda: &wallabypb.RedpandaDestinationConfig{Kafka: &wallabypb.KafkaDestinationConfig{Brokers: []string{"broker:9092"}, Acks: wallabypb.KafkaAcks_KAFKA_ACKS_ALL}}}},
		{Name: "iceberg", Config: &wallabypb.Endpoint_Iceberg{Iceberg: &wallabypb.IcebergDestinationConfig{CatalogProfile: wallabypb.IcebergCatalogProfile_ICEBERG_CATALOG_PROFILE_S3_TABLES, DestinationRevisionId: "revision-1"}}},
		{Name: "ducklake", Config: &wallabypb.Endpoint_Ducklake{Ducklake: &wallabypb.DuckLakeDestinationConfig{Dsn: "ducklake:db", Catalog: "postgres:catalog", DataPath: "s3://bucket/data", OverrideDataPath: proto.Bool(true), InstallExtensions: proto.Bool(false), TypeMappings: &wallabypb.TypeMappingsConfig{Mappings: map[string]string{"jsonb": "JSON"}}}}},
	} {
		model := endpointFromProto(endpoint)
		roundTrip, diagnostics := endpointModelToProto(context.Background(), model, false)
		if diagnostics.HasError() {
			t.Fatalf("%s diagnostics=%v", endpoint.GetName(), diagnostics)
		}
		if !proto.Equal(roundTrip, endpoint) {
			t.Fatalf("%s round trip=%v, want %v", endpoint.GetName(), roundTrip, endpoint)
		}
	}
}

func TestEndpointTerraformFlattensEmptyCollectionsAsKnownEmpty(t *testing.T) {
	t.Parallel()
	kafkaModel := endpointFromProto(&wallabypb.Endpoint{Config: &wallabypb.Endpoint_Kafka{Kafka: &wallabypb.KafkaDestinationConfig{}}})
	brokers := kafkaModel.Kafka.Attributes()["brokers"].(types.List)
	if brokers.IsNull() || brokers.IsUnknown() || len(brokers.Elements()) != 0 {
		t.Fatalf("empty brokers flattened as %#v, want known empty list", brokers)
	}

	duckModel := endpointFromProto(&wallabypb.Endpoint{Config: &wallabypb.Endpoint_Duckdb{Duckdb: &wallabypb.DuckDBDestinationConfig{TypeMappings: &wallabypb.TypeMappingsConfig{Mappings: map[string]string{}}}}})
	typeMappings := duckModel.DuckDB.Attributes()["type_mappings"].(types.Object)
	mappings := typeMappings.Attributes()["mappings"].(types.Map)
	if mappings.IsNull() || mappings.IsUnknown() || len(mappings.Elements()) != 0 {
		t.Fatalf("empty mappings flattened as %#v, want known empty map", mappings)
	}
}

func TestEndpointTerraformRejectsNestedOneofAmbiguityAndUnknowns(t *testing.T) {
	t.Parallel()

	registryDescriptor := (&wallabypb.SchemaRegistryConfig{}).ProtoReflect().Descriptor()
	registryAttributes := protoMessageToObject((&wallabypb.SchemaRegistryConfig{}).ProtoReflect()).Attributes()
	registryAttributes["confluent"] = protoMessageToObject((&wallabypb.ConfluentSchemaRegistryConfig{Url: "https://registry.example"}).ProtoReflect())
	registryAttributes["local"] = protoMessageToObject((&wallabypb.LocalSchemaRegistryConfig{Directory: "/tmp/registry"}).ProtoReflect())
	registryObject := types.ObjectValueMust(protoObjectAttributeTypes(registryDescriptor), registryAttributes)
	var diagnostics diag.Diagnostics
	populateProtoMessage(context.Background(), (&wallabypb.SchemaRegistryConfig{}).ProtoReflect(), registryObject, "schema_registry", &diagnostics)
	if !diagnostics.HasError() {
		t.Fatal("schema registry with two backend branches was accepted")
	}

	snapshotDescriptor := (&wallabypb.SnapshotStateConfig{}).ProtoReflect().Descriptor()
	snapshotAttributes := protoMessageToObject((&wallabypb.SnapshotStateConfig{}).ProtoReflect()).Attributes()
	snapshotAttributes["disabled"] = types.BoolUnknown()
	snapshotObject := types.ObjectValueMust(protoObjectAttributeTypes(snapshotDescriptor), snapshotAttributes)
	diagnostics = nil
	populateProtoMessage(context.Background(), (&wallabypb.SnapshotStateConfig{}).ProtoReflect(), snapshotObject, "snapshot_state", &diagnostics)
	if !diagnostics.HasError() {
		t.Fatal("snapshot state with unknown backend branch was accepted at apply conversion")
	}

	snapshotAttributes = protoMessageToObject((&wallabypb.SnapshotStateConfig{}).ProtoReflect()).Attributes()
	snapshotAttributes["disabled"] = types.BoolValue(true)
	snapshotAttributes["file_path"] = types.StringValue("snapshot.json")
	snapshotObject = types.ObjectValueMust(protoObjectAttributeTypes(snapshotDescriptor), snapshotAttributes)
	diagnostics = nil
	populateProtoMessage(context.Background(), (&wallabypb.SnapshotStateConfig{}).ProtoReflect(), snapshotObject, "snapshot_state", &diagnostics)
	if !diagnostics.HasError() {
		t.Fatal("snapshot state with two backend branches was accepted")
	}
}

func TestEndpointTerraformValidatesExactlyOneKnownLegalBranch(t *testing.T) {
	t.Parallel()

	missing := nullEndpointModel()
	if _, diagnostics := endpointModelToProto(context.Background(), missing, false); !diagnostics.HasError() {
		t.Fatal("endpoint without a branch was accepted")
	}

	unknown := nullEndpointModel()
	unknown.GRPC = types.ObjectUnknown(protoObjectAttributeTypes((&wallabypb.GRPCDestinationConfig{}).ProtoReflect().Descriptor()))
	if _, diagnostics := endpointModelToProto(context.Background(), unknown, false); !diagnostics.HasError() {
		t.Fatal("unknown endpoint branch was accepted")
	}

	multiple := endpointFromProto(&wallabypb.Endpoint{Name: "target", Config: &wallabypb.Endpoint_Grpc{Grpc: &wallabypb.GRPCDestinationConfig{Endpoint: "grpc.example:443"}}})
	multiple.S3 = protoMessageToObject((&wallabypb.S3DestinationConfig{Bucket: "archive"}).ProtoReflect())
	if _, diagnostics := endpointModelToProto(context.Background(), multiple, false); !diagnostics.HasError() {
		t.Fatal("multiple endpoint branches were accepted")
	}

	sourceAsDestination := endpointFromProto(&wallabypb.Endpoint{Name: "source", Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: &wallabypb.PostgresSourceConfig{}}})
	if _, diagnostics := endpointModelToProto(context.Background(), sourceAsDestination, false); !diagnostics.HasError() {
		t.Fatal("postgres_source was accepted as a destination")
	}
	destinationAsSource := endpointFromProto(&wallabypb.Endpoint{Name: "target", Config: &wallabypb.Endpoint_Duckdb{Duckdb: &wallabypb.DuckDBDestinationConfig{}}})
	if _, diagnostics := endpointModelToProto(context.Background(), destinationAsSource, true); !diagnostics.HasError() {
		t.Fatal("destination branch was accepted as a source")
	}

	unknownName := endpointFromProto(&wallabypb.Endpoint{Name: "target", Config: &wallabypb.Endpoint_Grpc{Grpc: &wallabypb.GRPCDestinationConfig{}}})
	unknownName.Name = types.StringUnknown()
	if _, diagnostics := endpointModelToProto(context.Background(), unknownName, false); !diagnostics.HasError() {
		t.Fatal("unknown endpoint name was accepted")
	}
}

func TestEndpointTerraformSchemaSensitivityAndNoLegacyShape(t *testing.T) {
	t.Parallel()
	attributes := endpointSchemaAttributes()
	custom := attributes["custom"].(schema.SingleNestedAttribute)
	if options := custom.Attributes["options"].(schema.MapAttribute); !options.Sensitive {
		t.Fatal("custom.options is not sensitive")
	}
	s3 := attributes["s3"].(schema.SingleNestedAttribute)
	for _, name := range []string{"access_key", "secret_key", "session_token"} {
		if field := s3.Attributes[name].(schema.StringAttribute); !field.Sensitive {
			t.Fatalf("s3.%s is not sensitive", name)
		}
	}
	postgres := attributes["postgres_destination"].(schema.SingleNestedAttribute)
	connection := postgres.Attributes["connection"].(schema.SingleNestedAttribute)
	if dsn := connection.Attributes["dsn"].(schema.StringAttribute); !dsn.Sensitive {
		t.Fatal("postgres_destination.connection.dsn is not sensitive")
	}
	iam := connection.Attributes["rds_iam"].(schema.SingleNestedAttribute)
	if externalID := iam.Attributes["role_external_id"].(schema.StringAttribute); !externalID.Sensitive {
		t.Fatal("rds_iam.role_external_id is not sensitive")
	}

	modelType := reflect.TypeOf(endpointModel{})
	for index := 0; index < modelType.NumField(); index++ {
		tag := modelType.Field(index).Tag.Get("tfsdk")
		if tag == "type" || tag == "options" {
			t.Fatalf("legacy Terraform endpoint field %q remains", tag)
		}
	}
}

func TestFlowModelToProtoRejectsUnknownUserConfiguration(t *testing.T) {
	t.Parallel()
	baseline := flowResourceModel{
		Name:             types.StringValue("flow"),
		WireFormat:       types.StringValue("arrow"),
		Parallelism:      types.Int64Value(1),
		StartImmediately: types.BoolValue(false),
		Source: endpointFromProto(&wallabypb.Endpoint{Name: "source", Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: &wallabypb.PostgresSourceConfig{
			Mode: wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_CDC,
		}}}),
		Destinations: []endpointModel{endpointFromProto(&wallabypb.Endpoint{Name: "sink", Config: &wallabypb.Endpoint_Http{Http: &wallabypb.HTTPDestinationConfig{
			Url: "https://example.test/events",
		}}})},
		Config: &flowConfigModel{
			AckPolicy:          types.StringValue("all"),
			PrimaryDestination: types.StringNull(),
			FailureMode:        types.StringNull(),
			GiveUpPolicy:       types.StringNull(),
			TableMappings:      testAppendMappings("sink"),
		},
	}
	if _, diagnostics := flowModelToProto(context.Background(), baseline); diagnostics.HasError() {
		t.Fatalf("baseline conversion diagnostics=%v", diagnostics)
	}

	tests := []struct {
		name string
		edit func(*flowResourceModel)
	}{
		{name: "name", edit: func(model *flowResourceModel) { model.Name = types.StringUnknown() }},
		{name: "wire format", edit: func(model *flowResourceModel) { model.WireFormat = types.StringUnknown() }},
		{name: "parallelism", edit: func(model *flowResourceModel) { model.Parallelism = types.Int64Unknown() }},
		{name: "start immediately", edit: func(model *flowResourceModel) { model.StartImmediately = types.BoolUnknown() }},
		{name: "ack policy", edit: func(model *flowResourceModel) { model.Config.AckPolicy = types.StringUnknown() }},
		{name: "primary destination", edit: func(model *flowResourceModel) { model.Config.PrimaryDestination = types.StringUnknown() }},
		{name: "failure mode", edit: func(model *flowResourceModel) { model.Config.FailureMode = types.StringUnknown() }},
		{name: "give up policy", edit: func(model *flowResourceModel) { model.Config.GiveUpPolicy = types.StringUnknown() }},
		{name: "DDL gate", edit: func(model *flowResourceModel) {
			model.Config.DDL = &flowDDLConfigModel{Gate: types.BoolUnknown(), AutoApprove: types.BoolNull(), AutoApply: types.BoolNull()}
		}},
		{name: "DDL auto approve", edit: func(model *flowResourceModel) {
			model.Config.DDL = &flowDDLConfigModel{Gate: types.BoolNull(), AutoApprove: types.BoolUnknown(), AutoApply: types.BoolNull()}
		}},
		{name: "DDL auto apply", edit: func(model *flowResourceModel) {
			model.Config.DDL = &flowDDLConfigModel{Gate: types.BoolNull(), AutoApprove: types.BoolNull(), AutoApply: types.BoolUnknown()}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := baseline
			config := *baseline.Config
			candidate.Config = &config
			test.edit(&candidate)
			if _, diagnostics := flowModelToProto(context.Background(), candidate); !diagnostics.HasError() {
				t.Fatal("unknown user configuration was accepted")
			}
		})
	}

	computed := baseline
	computed.ID = types.StringUnknown()
	computed.State = types.StringUnknown()
	if _, diagnostics := flowModelToProto(context.Background(), computed); diagnostics.HasError() {
		t.Fatalf("computed id/state unknowns must be ignored, diagnostics=%v", diagnostics)
	}
}

func TestValidateFlowResourceModel(t *testing.T) {
	managed := true
	valid := flowResourceModel{
		Source: endpointFromProto(&wallabypb.Endpoint{Name: "source", Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: &wallabypb.PostgresSourceConfig{
			Managed: &managed, Bootstrap: wallabypb.BootstrapMode_BOOTSTRAP_MODE_NEVER, PublicationTables: []string{"public.events"},
		}}}),
		Destinations: []endpointModel{endpointFromProto(&wallabypb.Endpoint{Name: "iceberg", Config: &wallabypb.Endpoint_Iceberg{Iceberg: &wallabypb.IcebergDestinationConfig{
			CatalogProfile: wallabypb.IcebergCatalogProfile_ICEBERG_CATALOG_PROFILE_S3_TABLES, DestinationRevisionId: "iceberg-s3tables-v1",
		}}})},
		Config: &flowConfigModel{AckPolicy: types.StringValue("materialized"), Materialization: &flowMaterializationPolicyModel{ProjectionID: types.StringValue("canonical_cdc_parquet_v2")}, TableMappings: testAppendMappings("iceberg")},
	}
	if diagnostics := validateFlowResourceModel(context.Background(), valid); diagnostics.HasError() {
		t.Fatalf("valid materialized config diagnostics=%v", diagnostics)
	}

	unknown := valid
	unknown.Config = &flowConfigModel{AckPolicy: types.StringValue("sometimes"), TableMappings: testAppendMappings("iceberg")}
	if diagnostics := validateFlowResourceModel(context.Background(), unknown); !diagnostics.HasError() {
		t.Fatal("unknown acknowledgement policy was silently mapped to unspecified")
	}

	wrongDestination := valid
	wrongDestination.Destinations = []endpointModel{endpointFromProto(&wallabypb.Endpoint{Name: "iceberg", Config: &wallabypb.Endpoint_PostgresDestination{PostgresDestination: &wallabypb.PostgresDestinationConfig{}}})}
	if diagnostics := validateFlowResourceModel(context.Background(), wrongDestination); !diagnostics.HasError() {
		t.Fatal("materialized flow accepted a non-Iceberg destination")
	}

	missingRevision := valid
	missingRevision.Destinations = []endpointModel{endpointFromProto(&wallabypb.Endpoint{Name: "iceberg", Config: &wallabypb.Endpoint_Iceberg{Iceberg: &wallabypb.IcebergDestinationConfig{}}})}
	if diagnostics := validateFlowResourceModel(context.Background(), missingRevision); !diagnostics.HasError() {
		t.Fatal("materialized Iceberg flow accepted a missing destination revision")
	}
}
