package main

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	frameworkresource "github.com/hashicorp/terraform-plugin-framework/resource"
	frameworkschema "github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
)

func TestFlowFrameworkPlanDefaultsOmittedCollectionsAndSupportsCreateConversion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	resourceImplementation := NewFlowResource()
	var frameworkSchemaResponse frameworkresource.SchemaResponse
	resourceImplementation.Schema(ctx, frameworkresource.SchemaRequest{}, &frameworkSchemaResponse)
	assertFrameworkDiagnostics(t, frameworkSchemaResponse.Diagnostics)
	frameworkSchema := frameworkSchemaResponse.Schema

	configModel := frameworkPlanTestModel(false)
	proposedModel := frameworkPlanTestModel(true)
	config := flowModelDynamicValue(t, frameworkSchema, configModel)
	proposed := flowModelDynamicValue(t, frameworkSchema, proposedModel)

	server := providerserver.NewProtocol6(New("test")())()
	providerSchema, err := server.GetProviderSchema(ctx, &tfprotov6.GetProviderSchemaRequest{})
	if err != nil {
		t.Fatal(err)
	}
	assertProtocolDiagnostics(t, providerSchema.Diagnostics)
	wireSchema := providerSchema.ResourceSchemas["wallaby_flow"]
	if wireSchema == nil {
		t.Fatal("wallaby_flow protocol schema is missing")
	}
	prior, err := tfprotov6.NewDynamicValue(wireSchema.ValueType(), tftypes.NewValue(wireSchema.ValueType(), nil))
	if err != nil {
		t.Fatal(err)
	}

	validation, err := server.ValidateResourceConfig(ctx, &tfprotov6.ValidateResourceConfigRequest{TypeName: "wallaby_flow", Config: &config})
	if err != nil {
		t.Fatal(err)
	}
	assertProtocolDiagnostics(t, validation.Diagnostics)

	planned, err := server.PlanResourceChange(ctx, &tfprotov6.PlanResourceChangeRequest{
		TypeName:         "wallaby_flow",
		PriorState:       &prior,
		ProposedNewState: &proposed,
		Config:           &config,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertProtocolDiagnostics(t, planned.Diagnostics)
	if planned.PlannedState == nil {
		t.Fatal("framework returned a nil planned state")
	}
	rawPlan, err := planned.PlannedState.Unmarshal(wireSchema.ValueType())
	if err != nil {
		t.Fatal(err)
	}
	plan := tfsdk.Plan{Raw: rawPlan, Schema: frameworkSchema}
	var model flowResourceModel
	assertFrameworkDiagnostics(t, plan.Get(ctx, &model))

	if !model.ID.IsUnknown() || !model.State.IsUnknown() {
		t.Fatalf("computed id/state = %v/%v, want unknown", model.ID, model.State)
	}
	if model.StartImmediately.IsUnknown() || model.StartImmediately.IsNull() || model.StartImmediately.ValueBool() {
		t.Fatalf("omitted start_immediately = %v, want known false", model.StartImmediately)
	}
	publicationTables := model.Source.PostgresSource.Attributes()["publication_tables"].(types.List)
	if publicationTables.IsUnknown() || publicationTables.IsNull() || len(publicationTables.Elements()) != 0 {
		t.Fatalf("omitted publication_tables = %v, want known empty list", publicationTables)
	}
	headers := model.Destinations[0].HTTP.Attributes()["headers"].(types.Map)
	if headers.IsUnknown() || headers.IsNull() || len(headers.Elements()) != 0 {
		t.Fatalf("omitted headers = %v, want known empty map", headers)
	}
	if _, diagnostics := flowModelToProto(ctx, model); diagnostics.HasError() {
		t.Fatalf("planned model cannot be converted for Create: %v", diagnostics)
	}

	// Defaults apply only to omitted (null) collections. An explicitly unknown
	// expression must remain unknown in the framework plan and fail the apply
	// conversion instead of silently becoming an empty collection.
	explicitUnknownConfig := frameworkPlanTestModel(true)
	explicitUnknownConfig.ID = types.StringNull()
	explicitUnknownConfig.State = types.StringNull()
	explicitUnknownConfig.StartImmediately = types.BoolValue(false)
	explicitUnknownProposed := explicitUnknownConfig
	explicitUnknownProposed.ID = types.StringUnknown()
	explicitUnknownProposed.State = types.StringUnknown()
	explicitConfigValue := flowModelDynamicValue(t, frameworkSchema, explicitUnknownConfig)
	explicitProposedValue := flowModelDynamicValue(t, frameworkSchema, explicitUnknownProposed)
	explicitPlan, err := server.PlanResourceChange(ctx, &tfprotov6.PlanResourceChangeRequest{
		TypeName:         "wallaby_flow",
		PriorState:       &prior,
		ProposedNewState: &explicitProposedValue,
		Config:           &explicitConfigValue,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertProtocolDiagnostics(t, explicitPlan.Diagnostics)
	explicitRaw, err := explicitPlan.PlannedState.Unmarshal(wireSchema.ValueType())
	if err != nil {
		t.Fatal(err)
	}
	explicitFrameworkPlan := tfsdk.Plan{Raw: explicitRaw, Schema: frameworkSchema}
	var explicitModel flowResourceModel
	assertFrameworkDiagnostics(t, explicitFrameworkPlan.Get(ctx, &explicitModel))
	if !explicitModel.Source.PostgresSource.Attributes()["publication_tables"].(types.List).IsUnknown() {
		t.Fatal("explicitly unknown publication_tables was replaced by a default")
	}
	if !explicitModel.Destinations[0].HTTP.Attributes()["headers"].(types.Map).IsUnknown() {
		t.Fatal("explicitly unknown headers was replaced by a default")
	}
	if _, diagnostics := flowModelToProto(ctx, explicitModel); !diagnostics.HasError() {
		t.Fatal("Create conversion accepted explicitly unknown endpoint collections")
	}
}

func frameworkPlanTestModel(proposed bool) flowResourceModel {
	source := endpointFromProto(&wallabypb.Endpoint{Name: "source", Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: &wallabypb.PostgresSourceConfig{
		Mode: wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_CDC,
	}}})
	sourceAttributes := source.PostgresSource.Attributes()
	if proposed {
		sourceAttributes["publication_tables"] = types.ListUnknown(types.StringType)
	} else {
		sourceAttributes["publication_tables"] = types.ListNull(types.StringType)
	}
	source.PostgresSource = types.ObjectValueMust(protoObjectAttributeTypes((&wallabypb.PostgresSourceConfig{}).ProtoReflect().Descriptor()), sourceAttributes)

	destination := endpointFromProto(&wallabypb.Endpoint{Name: "sink", Config: &wallabypb.Endpoint_Http{Http: &wallabypb.HTTPDestinationConfig{
		Url: "https://example.test/events",
	}}})
	httpAttributes := destination.HTTP.Attributes()
	if proposed {
		httpAttributes["headers"] = types.MapUnknown(types.StringType)
	} else {
		httpAttributes["headers"] = types.MapNull(types.StringType)
	}
	destination.HTTP = types.ObjectValueMust(protoObjectAttributeTypes((&wallabypb.HTTPDestinationConfig{}).ProtoReflect().Descriptor()), httpAttributes)

	model := flowResourceModel{
		ID:               types.StringNull(),
		Name:             types.StringValue("offline-framework-plan"),
		WireFormat:       types.StringValue("arrow"),
		Parallelism:      types.Int64Value(1),
		State:            types.StringNull(),
		StartImmediately: types.BoolNull(),
		Source:           source,
		Destinations:     []endpointModel{destination},
		Config: &flowConfigModel{
			AckPolicy:          types.StringValue("all"),
			PrimaryDestination: types.StringNull(),
			FailureMode:        types.StringNull(),
			GiveUpPolicy:       types.StringNull(),
			TableMappings:      testAppendMappings("sink"),
		},
	}
	if proposed {
		model.ID = types.StringUnknown()
		model.State = types.StringUnknown()
		model.StartImmediately = types.BoolUnknown()
	}
	return model
}

func flowModelDynamicValue(t *testing.T, resourceSchema frameworkschema.Schema, model flowResourceModel) tfprotov6.DynamicValue {
	t.Helper()
	plan := tfsdk.Plan{Schema: resourceSchema}
	assertFrameworkDiagnostics(t, plan.Set(context.Background(), model))
	value, err := tfprotov6.NewDynamicValue(plan.Raw.Type(), plan.Raw)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func assertProtocolDiagnostics(t *testing.T, diagnostics []*tfprotov6.Diagnostic) {
	t.Helper()
	for _, diagnostic := range diagnostics {
		if diagnostic != nil && diagnostic.Severity == tfprotov6.DiagnosticSeverityError {
			t.Fatalf("protocol diagnostic: %s: %s", diagnostic.Summary, diagnostic.Detail)
		}
	}
}

func assertFrameworkDiagnostics(t *testing.T, diagnostics interface{ HasError() bool }) {
	t.Helper()
	if diagnostics.HasError() {
		t.Fatalf("framework diagnostics contain an error: %v", diagnostics)
	}
}
