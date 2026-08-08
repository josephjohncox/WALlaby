package main

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/types"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type flowResource struct {
	client *Client
}

type flowResourceModel struct {
	ID               types.String     `tfsdk:"id"`
	Name             types.String     `tfsdk:"name"`
	WireFormat       types.String     `tfsdk:"wire_format"`
	Parallelism      types.Int64      `tfsdk:"parallelism"`
	State            types.String     `tfsdk:"state"`
	StartImmediately types.Bool       `tfsdk:"start_immediately"`
	Source           endpointModel    `tfsdk:"source"`
	Destinations     []endpointModel  `tfsdk:"destinations"`
	Config           *flowConfigModel `tfsdk:"config"`
}

type flowConfigModel struct {
	AckPolicy          types.String                    `tfsdk:"ack_policy"`
	PrimaryDestination types.String                    `tfsdk:"primary_destination"`
	FailureMode        types.String                    `tfsdk:"failure_mode"`
	GiveUpPolicy       types.String                    `tfsdk:"give_up_policy"`
	DDL                *flowDDLConfigModel             `tfsdk:"ddl"`
	Materialization    *flowMaterializationPolicyModel `tfsdk:"materialization"`
	TableMappings      types.Object                    `tfsdk:"table_mappings"`
}

type flowMaterializationPolicyModel struct {
	ProjectionID types.String `tfsdk:"projection_id"`
}

type flowDDLConfigModel struct {
	Gate        types.Bool `tfsdk:"gate"`
	AutoApprove types.Bool `tfsdk:"auto_approve"`
	AutoApply   types.Bool `tfsdk:"auto_apply"`
}

func NewFlowResource() resource.Resource {
	return &flowResource{}
}

func (r *flowResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_flow"
}

func (r *flowResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
			},
			"name":        schema.StringAttribute{Optional: true},
			"wire_format": schema.StringAttribute{Optional: true},
			"parallelism": schema.Int64Attribute{
				Optional: true,
			},
			"state": schema.StringAttribute{
				Computed: true,
			},
			"start_immediately": schema.BoolAttribute{
				Optional: true,
				Computed: true,
				Default:  booldefault.StaticBool(false),
			},
			"source": schema.SingleNestedAttribute{
				Required:   true,
				Attributes: endpointSchemaAttributes(),
			},
			"destinations": schema.ListNestedAttribute{
				Required: true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: endpointSchemaAttributes(),
				},
			},
			"config": schema.SingleNestedAttribute{
				Required: true,
				Attributes: map[string]schema.Attribute{
					"ack_policy": schema.StringAttribute{
						Optional: true,
					},
					"primary_destination": schema.StringAttribute{
						Optional: true,
					},
					"failure_mode": schema.StringAttribute{
						Optional: true,
					},
					"give_up_policy": schema.StringAttribute{Optional: true},
					"table_mappings": tableMappingsSchema(),
					"ddl": schema.SingleNestedAttribute{
						Optional: true,
						Attributes: map[string]schema.Attribute{
							"gate": schema.BoolAttribute{
								Optional: true,
							},
							"auto_approve": schema.BoolAttribute{
								Optional: true,
							},
							"auto_apply": schema.BoolAttribute{
								Optional: true,
							},
						},
					},
					"materialization": schema.SingleNestedAttribute{
						Optional: true,
						Attributes: map[string]schema.Attribute{
							"projection_id": schema.StringAttribute{Required: true},
						},
					},
				},
			},
		},
	}
}

func (r *flowResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

func (r *flowResource) ValidateConfig(ctx context.Context, req resource.ValidateConfigRequest, resp *resource.ValidateConfigResponse) {
	if !req.Config.Raw.IsFullyKnown() {
		return
	}
	var model flowResourceModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &model)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(validateFlowResourceModel(ctx, model)...)
}

func validateFlowResourceModel(ctx context.Context, model flowResourceModel) diag.Diagnostics {
	var diagnostics diag.Diagnostics
	diagnostics.Append(validateTerraformTableMappings(ctx, model)...)
	source, sourceDiagnostics := endpointModelToProto(ctx, model.Source, true)
	diagnostics.Append(sourceDiagnostics...)
	destinations := make([]*wallabypb.Endpoint, 0, len(model.Destinations))
	for _, destination := range model.Destinations {
		converted, destinationDiagnostics := endpointModelToProto(ctx, destination, false)
		diagnostics.Append(destinationDiagnostics...)
		if converted != nil {
			if iceberg := converted.GetIceberg(); iceberg != nil && strings.TrimSpace(iceberg.GetDestinationRevisionId()) == "" {
				diagnostics.AddError("Invalid Iceberg destination", "iceberg.destination_revision_id is required")
			}
			destinations = append(destinations, converted)
		}
	}
	if model.Config == nil {
		return diagnostics
	}
	config := model.Config
	ackPolicy := "all"
	if !config.AckPolicy.IsNull() && !config.AckPolicy.IsUnknown() && strings.TrimSpace(config.AckPolicy.ValueString()) != "" {
		ackPolicy = strings.TrimSpace(config.AckPolicy.ValueString())
	}
	switch ackPolicy {
	case "all", "primary", "materialized":
	default:
		diagnostics.AddError("Invalid acknowledgement policy", "ack_policy must be one of all, primary, or materialized")
	}
	if ackPolicy != "materialized" {
		if config.Materialization != nil {
			diagnostics.AddError("Invalid materialization policy", "materialization requires ack_policy=materialized")
		}
		return diagnostics
	}
	if config.Materialization == nil || config.Materialization.ProjectionID.IsNull() || (!config.Materialization.ProjectionID.IsUnknown() && config.Materialization.ProjectionID.ValueString() != "canonical_cdc_parquet_v2") {
		diagnostics.AddError("Invalid materialization policy", "ack_policy=materialized requires materialization.projection_id=canonical_cdc_parquet_v2")
	}
	if sourceConfig := source.GetPostgresSource(); sourceConfig == nil || sourceConfig.Managed == nil || !sourceConfig.GetManaged() || sourceConfig.GetManagedProfile() != wallabypb.ManagedProfile_MANAGED_PROFILE_UNSPECIFIED || sourceConfig.GetBootstrap() != wallabypb.BootstrapMode_BOOTSTRAP_MODE_NEVER {
		diagnostics.AddError("Invalid materialized source", "ack_policy=materialized requires postgres_source with managed=true, no managed_profile, and bootstrap=BOOTSTRAP_MODE_NEVER")
	}
	if len(destinations) != 1 || destinations[0].GetIceberg() == nil {
		diagnostics.AddError("Invalid materialized destination", "ack_policy=materialized requires exactly one iceberg destination")
	}
	return diagnostics
}

func (r *flowResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	client, ok := req.ProviderData.(*Client)
	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data type", "Expected *Client")
		return
	}
	r.client = client
}

func (r *flowResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan flowResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	flow, conversionDiagnostics := flowModelToProto(ctx, plan)
	resp.Diagnostics.Append(conversionDiagnostics...)
	if resp.Diagnostics.HasError() {
		return
	}
	result, err := r.client.Flow.CreateFlow(ctx, &wallabypb.CreateFlowRequest{Flow: flow, StartImmediately: plan.StartImmediately.ValueBool()})
	if err != nil {
		resp.Diagnostics.AddError("Create flow failed", err.Error())
		return
	}
	state, stateDiagnostics := flowResourceModelFromProto(ctx, result, plan.StartImmediately)
	resp.Diagnostics.Append(stateDiagnostics...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
}

func (r *flowResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var id types.String
	resp.Diagnostics.Append(req.State.GetAttribute(ctx, path.Root("id"), &id)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if id.IsNull() || id.ValueString() == "" {
		resp.State.RemoveResource(ctx)
		return
	}
	result, err := r.client.Flow.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: id.ValueString()})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Read flow failed", err.Error())
		return
	}
	var startImmediately types.Bool
	resp.Diagnostics.Append(req.State.GetAttribute(ctx, path.Root("start_immediately"), &startImmediately)...)
	if resp.Diagnostics.HasError() {
		return
	}
	state, stateDiagnostics := flowResourceModelFromProto(ctx, result, startImmediately)
	resp.Diagnostics.Append(stateDiagnostics...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
}

func (r *flowResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, prior flowResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &prior)...)
	if resp.Diagnostics.HasError() {
		return
	}
	planned, plannedDiagnostics := flowModelToProto(ctx, plan)
	resp.Diagnostics.Append(plannedDiagnostics...)
	priorWire, priorDiagnostics := flowModelToProto(ctx, prior)
	resp.Diagnostics.Append(priorDiagnostics...)
	if resp.Diagnostics.HasError() {
		return
	}
	var result *wallabypb.Flow
	var err error
	if flowRequiresReconfigure(priorWire, planned) {
		pause, resume := true, true
		result, err = r.client.Flow.ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{Flow: planned, PauseFirst: &pause, ResumeAfter: &resume})
	} else {
		result, err = r.client.Flow.UpdateFlow(ctx, &wallabypb.UpdateFlowRequest{Flow: planned})
	}
	if err != nil {
		resp.Diagnostics.AddError("Update flow failed", err.Error())
		return
	}
	state, stateDiagnostics := flowResourceModelFromProto(ctx, result, plan.StartImmediately)
	resp.Diagnostics.Append(stateDiagnostics...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
}

func flowRequiresReconfigure(prior, planned *wallabypb.Flow) bool {
	if prior == nil || planned == nil {
		return true
	}
	if prior.WireFormat != planned.WireFormat || !proto.Equal(prior.Config, planned.Config) || !proto.Equal(prior.Source, planned.Source) || len(prior.Destinations) != len(planned.Destinations) {
		return true
	}
	for index := range prior.Destinations {
		if !proto.Equal(prior.Destinations[index], planned.Destinations[index]) {
			return true
		}
	}
	return false
}

func (r *flowResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state flowResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if state.ID.IsNull() || state.ID.ValueString() == "" {
		resp.State.RemoveResource(ctx)
		return
	}
	_, err := r.client.Flow.DeleteFlow(ctx, &wallabypb.DeleteFlowRequest{FlowId: state.ID.ValueString()})
	if err != nil && status.Code(err) != codes.NotFound {
		resp.Diagnostics.AddError("Delete flow failed", err.Error())
		return
	}
	resp.State.RemoveResource(ctx)
}

func flowModelToProto(ctx context.Context, model flowResourceModel) (*wallabypb.Flow, diag.Diagnostics) {
	var diags diag.Diagnostics
	for _, field := range []struct {
		name    string
		unknown bool
	}{
		{name: "name", unknown: model.Name.IsUnknown()},
		{name: "wire_format", unknown: model.WireFormat.IsUnknown()},
		{name: "parallelism", unknown: model.Parallelism.IsUnknown()},
		{name: "start_immediately", unknown: model.StartImmediately.IsUnknown()},
	} {
		if field.unknown {
			diags.AddError("Unknown flow value", field.name+" must be known before apply")
		}
	}
	if diags.HasError() {
		return nil, diags
	}

	source, diag := endpointModelToProto(ctx, model.Source, true)
	diags.Append(diag...)
	if diags.HasError() {
		return nil, diags
	}

	dests := make([]*wallabypb.Endpoint, 0, len(model.Destinations))
	for _, dest := range model.Destinations {
		endpoint, diag := endpointModelToProto(ctx, dest, false)
		diags.Append(diag...)
		if diag.HasError() {
			return nil, diags
		}
		dests = append(dests, endpoint)
	}

	config, configDiagnostics := flowConfigModelToProto(ctx, model.Config, dests)
	diags.Append(configDiagnostics...)
	if diags.HasError() {
		return nil, diags
	}
	return &wallabypb.Flow{Id: model.ID.ValueString(), Name: model.Name.ValueString(), WireFormat: wireFormatFromString(model.WireFormat.ValueString()), Parallelism: int32(model.Parallelism.ValueInt64()), Source: source, Destinations: dests, Config: config}, diags
}

func flowConfigModelToProto(ctx context.Context, model *flowConfigModel, destinations []*wallabypb.Endpoint) (*wallabypb.FlowConfig, diag.Diagnostics) {
	var diagnostics diag.Diagnostics
	if model == nil {
		diagnostics.AddError("Missing flow config", "config with table_mappings is required")
		return nil, diagnostics
	}
	cfg := &wallabypb.FlowConfig{}
	for _, field := range []struct {
		name    string
		unknown bool
	}{
		{name: "config.ack_policy", unknown: model.AckPolicy.IsUnknown()},
		{name: "config.primary_destination", unknown: model.PrimaryDestination.IsUnknown()},
		{name: "config.failure_mode", unknown: model.FailureMode.IsUnknown()},
		{name: "config.give_up_policy", unknown: model.GiveUpPolicy.IsUnknown()},
	} {
		if field.unknown {
			diagnostics.AddError("Unknown flow config value", field.name+" must be known before apply")
		}
	}
	if !model.AckPolicy.IsNull() && !model.AckPolicy.IsUnknown() {
		cfg.AckPolicy = ackPolicyFromString(strings.TrimSpace(model.AckPolicy.ValueString()))
	}
	if !model.PrimaryDestination.IsNull() && !model.PrimaryDestination.IsUnknown() {
		cfg.PrimaryDestination = strings.TrimSpace(model.PrimaryDestination.ValueString())
	}
	if !model.FailureMode.IsNull() && !model.FailureMode.IsUnknown() {
		cfg.FailureMode = failureModeFromString(strings.TrimSpace(model.FailureMode.ValueString()))
	}
	if !model.GiveUpPolicy.IsNull() && !model.GiveUpPolicy.IsUnknown() {
		cfg.GiveUpPolicy = giveUpPolicyFromString(strings.TrimSpace(model.GiveUpPolicy.ValueString()))
	}
	if model.DDL != nil {
		for _, field := range []struct {
			name    string
			unknown bool
		}{
			{name: "config.ddl.gate", unknown: model.DDL.Gate.IsUnknown()},
			{name: "config.ddl.auto_approve", unknown: model.DDL.AutoApprove.IsUnknown()},
			{name: "config.ddl.auto_apply", unknown: model.DDL.AutoApply.IsUnknown()},
		} {
			if field.unknown {
				diagnostics.AddError("Unknown DDL policy value", field.name+" must be known before apply")
			}
		}
	}
	cfg.Ddl = ddlPolicyModelToProto(model.DDL)
	if model.Materialization != nil {
		if model.Materialization.ProjectionID.IsUnknown() {
			diagnostics.AddError("Unknown materialization projection", "materialization.projection_id must be known before apply")
		} else if !model.Materialization.ProjectionID.IsNull() {
			cfg.Materialization = &wallabypb.MaterializationPolicy{ProjectionId: strings.TrimSpace(model.Materialization.ProjectionID.ValueString())}
		}
	}
	mappings, deferred, mappingDiagnostics := tableMappingsModelToInternal(ctx, model.TableMappings, false)
	diagnostics.Append(mappingDiagnostics...)
	if deferred {
		diagnostics.AddError("Unknown table mappings", "all table mapping fields must be known before apply")
	}
	if !mappingDiagnostics.HasError() && !deferred {
		specs := make([]connector.RuntimeSpec, 0, len(destinations))
		for _, destination := range destinations {
			if destination != nil {
				spec, err := endpointcodec.Decode(destination, endpointcodec.RoleDestination)
				if err != nil {
					diagnostics.AddError("Invalid typed destination", err.Error())
					continue
				}
				specs = append(specs, spec)
			}
		}
		if err := mappings.Validate(specs); err != nil {
			diagnostics.AddError("Invalid table mappings", err.Error())
		} else {
			cfg.TableMappings = tableMappingsInternalToProto(mappings)
		}
	}
	if diagnostics.HasError() {
		return nil, diagnostics
	}
	return cfg, diagnostics
}

func flowConfigModelFromProto(ctx context.Context, pb *wallabypb.FlowConfig) (*flowConfigModel, diag.Diagnostics) {
	if pb == nil {
		return nil, nil
	}
	mappings, mappingDiagnostics := tableMappingsModelFromProto(ctx, pb.TableMappings)
	model := &flowConfigModel{AckPolicy: types.StringNull(), PrimaryDestination: types.StringNull(), FailureMode: types.StringNull(), GiveUpPolicy: types.StringNull(), TableMappings: mappings}
	if pb.AckPolicy != wallabypb.AckPolicy_ACK_POLICY_UNSPECIFIED {
		model.AckPolicy = types.StringValue(ackPolicyToString(pb.AckPolicy))
	}
	if pb.PrimaryDestination != "" {
		model.PrimaryDestination = types.StringValue(pb.PrimaryDestination)
	}
	if pb.FailureMode != wallabypb.FailureMode_FAILURE_MODE_UNSPECIFIED {
		model.FailureMode = types.StringValue(failureModeToString(pb.FailureMode))
	}
	if pb.GiveUpPolicy != wallabypb.GiveUpPolicy_GIVE_UP_POLICY_UNSPECIFIED {
		model.GiveUpPolicy = types.StringValue(giveUpPolicyToString(pb.GiveUpPolicy))
	}
	model.DDL = ddlPolicyModelFromProto(pb.Ddl)
	if pb.Materialization != nil {
		model.Materialization = &flowMaterializationPolicyModel{ProjectionID: nullableString(pb.Materialization.ProjectionId)}
	}
	return model, mappingDiagnostics
}

func flowResourceModelFromProto(ctx context.Context, pb *wallabypb.Flow, startImmediately types.Bool) (flowResourceModel, diag.Diagnostics) {
	var diagnostics diag.Diagnostics
	if pb == nil {
		diagnostics.AddError("Invalid flow response", "flow response is null")
		return flowResourceModel{}, diagnostics
	}
	config, configDiagnostics := flowConfigModelFromProto(ctx, pb.Config)
	diagnostics.Append(configDiagnostics...)
	model := flowResourceModel{ID: types.StringValue(pb.Id), Name: types.StringValue(pb.Name), WireFormat: types.StringValue(wireFormatToString(pb.WireFormat)), Parallelism: types.Int64Value(int64(pb.Parallelism)), State: types.StringValue(flowStateToString(pb.State)), StartImmediately: startImmediately, Source: endpointFromProto(pb.Source), Destinations: endpointsFromProto(pb.Destinations), Config: config}
	if !diagnostics.HasError() {
		diagnostics.Append(validateFlowResourceModel(ctx, model)...)
	}
	return model, diagnostics
}

func ddlPolicyModelToProto(model *flowDDLConfigModel) *wallabypb.DDLPolicy {
	if model == nil {
		return nil
	}
	out := &wallabypb.DDLPolicy{}
	has := false
	if !model.Gate.IsNull() && !model.Gate.IsUnknown() {
		value := model.Gate.ValueBool()
		out.Gate = &value
		has = true
	}
	if !model.AutoApprove.IsNull() && !model.AutoApprove.IsUnknown() {
		value := model.AutoApprove.ValueBool()
		out.AutoApprove = &value
		has = true
	}
	if !model.AutoApply.IsNull() && !model.AutoApply.IsUnknown() {
		value := model.AutoApply.ValueBool()
		out.AutoApply = &value
		has = true
	}
	if !has {
		return nil
	}
	return out
}

func ddlPolicyModelFromProto(pb *wallabypb.DDLPolicy) *flowDDLConfigModel {
	if pb == nil {
		return nil
	}
	model := &flowDDLConfigModel{
		Gate:        types.BoolNull(),
		AutoApprove: types.BoolNull(),
		AutoApply:   types.BoolNull(),
	}
	has := false
	if pb.Gate != nil {
		model.Gate = types.BoolValue(*pb.Gate)
		has = true
	}
	if pb.AutoApprove != nil {
		model.AutoApprove = types.BoolValue(*pb.AutoApprove)
		has = true
	}
	if pb.AutoApply != nil {
		model.AutoApply = types.BoolValue(*pb.AutoApply)
		has = true
	}
	if !has {
		return nil
	}
	return model
}

func ackPolicyFromString(value string) wallabypb.AckPolicy {
	switch strings.ToLower(value) {
	case "all":
		return wallabypb.AckPolicy_ACK_POLICY_ALL
	case "primary":
		return wallabypb.AckPolicy_ACK_POLICY_PRIMARY
	case "materialized":
		return wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED
	default:
		return wallabypb.AckPolicy_ACK_POLICY_UNSPECIFIED
	}
}

func ackPolicyToString(value wallabypb.AckPolicy) string {
	switch value {
	case wallabypb.AckPolicy_ACK_POLICY_ALL:
		return "all"
	case wallabypb.AckPolicy_ACK_POLICY_PRIMARY:
		return "primary"
	case wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED:
		return "materialized"
	default:
		return ""
	}
}

func failureModeFromString(value string) wallabypb.FailureMode {
	switch strings.ToLower(value) {
	case "hold_slot":
		return wallabypb.FailureMode_FAILURE_MODE_HOLD_SLOT
	case "drop_slot":
		return wallabypb.FailureMode_FAILURE_MODE_DROP_SLOT
	default:
		return wallabypb.FailureMode_FAILURE_MODE_UNSPECIFIED
	}
}

func failureModeToString(value wallabypb.FailureMode) string {
	switch value {
	case wallabypb.FailureMode_FAILURE_MODE_HOLD_SLOT:
		return "hold_slot"
	case wallabypb.FailureMode_FAILURE_MODE_DROP_SLOT:
		return "drop_slot"
	default:
		return ""
	}
}

func giveUpPolicyFromString(value string) wallabypb.GiveUpPolicy {
	switch strings.ToLower(value) {
	case "never":
		return wallabypb.GiveUpPolicy_GIVE_UP_POLICY_NEVER
	case "on_retry_exhaustion":
		return wallabypb.GiveUpPolicy_GIVE_UP_POLICY_ON_RETRY_EXHAUSTION
	default:
		return wallabypb.GiveUpPolicy_GIVE_UP_POLICY_UNSPECIFIED
	}
}

func giveUpPolicyToString(value wallabypb.GiveUpPolicy) string {
	switch value {
	case wallabypb.GiveUpPolicy_GIVE_UP_POLICY_NEVER:
		return "never"
	case wallabypb.GiveUpPolicy_GIVE_UP_POLICY_ON_RETRY_EXHAUSTION:
		return "on_retry_exhaustion"
	default:
		return ""
	}
}

func wireFormatFromString(value string) wallabypb.WireFormat {
	switch strings.ToLower(value) {
	case "arrow":
		return wallabypb.WireFormat_WIRE_FORMAT_ARROW
	case "parquet":
		return wallabypb.WireFormat_WIRE_FORMAT_PARQUET
	case "proto":
		return wallabypb.WireFormat_WIRE_FORMAT_PROTO
	case "avro":
		return wallabypb.WireFormat_WIRE_FORMAT_AVRO
	case "json":
		return wallabypb.WireFormat_WIRE_FORMAT_JSON
	default:
		return wallabypb.WireFormat_WIRE_FORMAT_UNSPECIFIED
	}
}

func wireFormatToString(value wallabypb.WireFormat) string {
	switch value {
	case wallabypb.WireFormat_WIRE_FORMAT_ARROW:
		return "arrow"
	case wallabypb.WireFormat_WIRE_FORMAT_PARQUET:
		return "parquet"
	case wallabypb.WireFormat_WIRE_FORMAT_PROTO:
		return "proto"
	case wallabypb.WireFormat_WIRE_FORMAT_AVRO:
		return "avro"
	case wallabypb.WireFormat_WIRE_FORMAT_JSON:
		return "json"
	default:
		return ""
	}
}

func flowStateToString(value wallabypb.FlowState) string {
	switch value {
	case wallabypb.FlowState_FLOW_STATE_CREATED:
		return "created"
	case wallabypb.FlowState_FLOW_STATE_RUNNING:
		return "running"
	case wallabypb.FlowState_FLOW_STATE_PAUSED:
		return "paused"
	case wallabypb.FlowState_FLOW_STATE_STOPPING:
		return "stopping"
	case wallabypb.FlowState_FLOW_STATE_STOPPED:
		return "stopped"
	case wallabypb.FlowState_FLOW_STATE_FAILED:
		return "failed"
	default:
		return ""
	}
}
