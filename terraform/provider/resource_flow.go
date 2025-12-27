package main

import (
	"context"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
)

type flowResource struct {
	client *Client
}

type flowResourceModel struct {
	ID               types.String    `tfsdk:"id"`
	Name             types.String    `tfsdk:"name"`
	WireFormat       types.String    `tfsdk:"wire_format"`
	Parallelism      types.Int64     `tfsdk:"parallelism"`
	State            types.String    `tfsdk:"state"`
	StartImmediately types.Bool      `tfsdk:"start_immediately"`
	Source           endpointModel   `tfsdk:"source"`
	Destinations     []endpointModel `tfsdk:"destinations"`
}

type endpointModel struct {
	Name    types.String `tfsdk:"name"`
	Type    types.String `tfsdk:"type"`
	Options types.Map    `tfsdk:"options"`
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
			"name": schema.StringAttribute{
				Optional:      true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"wire_format": schema.StringAttribute{
				Optional:      true,
				PlanModifiers: []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"parallelism": schema.Int64Attribute{
				Optional: true,
			},
			"state": schema.StringAttribute{
				Computed: true,
			},
			"start_immediately": schema.BoolAttribute{
				Optional: true,
			},
			"source": schema.SingleNestedAttribute{
				Required: true,
				Attributes: map[string]schema.Attribute{
					"name": schema.StringAttribute{
						Optional: true,
					},
					"type": schema.StringAttribute{
						Required: true,
					},
					"options": schema.MapAttribute{
						Optional:    true,
						ElementType: types.StringType,
					},
				},
			},
			"destinations": schema.ListNestedAttribute{
				Required: true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							Optional: true,
						},
						"type": schema.StringAttribute{
							Required: true,
						},
						"options": schema.MapAttribute{
							Optional:    true,
							ElementType: types.StringType,
						},
					},
				},
			},
		},
	}
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

	flow, diags := flowModelToProto(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	result, err := r.client.Flow.CreateFlow(ctx, &wallabypb.CreateFlowRequest{
		Flow:             flow,
		StartImmediately: plan.StartImmediately.ValueBool(),
	})
	if err != nil {
		resp.Diagnostics.AddError("Create flow failed", err.Error())
		return
	}

	state := flowResourceModel{
		ID:               types.StringValue(result.Id),
		Name:             types.StringValue(result.Name),
		WireFormat:       types.StringValue(wireFormatToString(result.WireFormat)),
		Parallelism:      types.Int64Value(int64(result.Parallelism)),
		State:            types.StringValue(flowStateToString(result.State)),
		StartImmediately: plan.StartImmediately,
		Source:           endpointFromProto(result.Source),
		Destinations:     endpointsFromProto(result.Destinations),
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
}

func (r *flowResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state flowResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if state.ID.IsNull() || state.ID.ValueString() == "" {
		resp.State.RemoveResource(ctx)
		return
	}

	result, err := r.client.Flow.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: state.ID.ValueString()})
	if err != nil {
		resp.State.RemoveResource(ctx)
		return
	}

	newState := flowResourceModel{
		ID:               types.StringValue(result.Id),
		Name:             types.StringValue(result.Name),
		WireFormat:       types.StringValue(wireFormatToString(result.WireFormat)),
		Parallelism:      types.Int64Value(int64(result.Parallelism)),
		State:            types.StringValue(flowStateToString(result.State)),
		StartImmediately: state.StartImmediately,
		Source:           endpointFromProto(result.Source),
		Destinations:     endpointsFromProto(result.Destinations),
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, newState)...)
}

func (r *flowResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan flowResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	flow, diags := flowModelToProto(ctx, plan)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	result, err := r.client.Flow.UpdateFlow(ctx, &wallabypb.UpdateFlowRequest{Flow: flow})
	if err != nil {
		resp.Diagnostics.AddError("Update flow failed", err.Error())
		return
	}

	state := flowResourceModel{
		ID:               types.StringValue(result.Id),
		Name:             types.StringValue(result.Name),
		WireFormat:       types.StringValue(wireFormatToString(result.WireFormat)),
		Parallelism:      types.Int64Value(int64(result.Parallelism)),
		State:            types.StringValue(flowStateToString(result.State)),
		StartImmediately: plan.StartImmediately,
		Source:           endpointFromProto(result.Source),
		Destinations:     endpointsFromProto(result.Destinations),
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, state)...)
}

func (r *flowResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state flowResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if state.ID.IsNull() || state.ID.ValueString() == "" {
		return
	}

	_, _ = r.client.Flow.DeleteFlow(ctx, &wallabypb.DeleteFlowRequest{FlowId: state.ID.ValueString()})
	resp.State.RemoveResource(ctx)
}

func flowModelToProto(ctx context.Context, model flowResourceModel) (*wallabypb.Flow, diag.Diagnostics) {
	var diags diag.Diagnostics

	source, diag := endpointModelToProto(ctx, model.Source)
	diags.Append(diag...)
	if diags.HasError() {
		return nil, diags
	}

	dests := make([]*wallabypb.Endpoint, 0, len(model.Destinations))
	for _, dest := range model.Destinations {
		endpoint, diag := endpointModelToProto(ctx, dest)
		diags.Append(diag...)
		if diag.HasError() {
			return nil, diags
		}
		dests = append(dests, endpoint)
	}

	return &wallabypb.Flow{
		Id:           model.ID.ValueString(),
		Name:         model.Name.ValueString(),
		WireFormat:   wireFormatFromString(model.WireFormat.ValueString()),
		Parallelism:  int32(model.Parallelism.ValueInt64()),
		Source:       source,
		Destinations: dests,
	}, diags
}

func endpointModelToProto(ctx context.Context, model endpointModel) (*wallabypb.Endpoint, diag.Diagnostics) {
	var diags diag.Diagnostics
	options := map[string]string{}
	if !model.Options.IsNull() {
		diags.Append(model.Options.ElementsAs(ctx, &options, false)...)
		if diags.HasError() {
			return nil, diags
		}
	}

	return &wallabypb.Endpoint{
		Name:    model.Name.ValueString(),
		Type:    endpointTypeFromString(model.Type.ValueString()),
		Options: options,
	}, diags
}

func endpointsFromProto(items []*wallabypb.Endpoint) []endpointModel {
	out := make([]endpointModel, 0, len(items))
	for _, item := range items {
		out = append(out, endpointFromProto(item))
	}
	return out
}

func endpointFromProto(item *wallabypb.Endpoint) endpointModel {
	if item == nil {
		return endpointModel{}
	}
	options := map[string]string{}
	for key, value := range item.Options {
		options[key] = value
	}
	optionsValue, _ := types.MapValue(types.StringType, options)
	return endpointModel{
		Name:    types.StringValue(item.Name),
		Type:    types.StringValue(endpointTypeToString(item.Type)),
		Options: optionsValue,
	}
}

func endpointTypeFromString(value string) wallabypb.EndpointType {
	switch strings.ToLower(value) {
	case "postgres":
		return wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES
	case "snowflake":
		return wallabypb.EndpointType_ENDPOINT_TYPE_SNOWFLAKE
	case "s3":
		return wallabypb.EndpointType_ENDPOINT_TYPE_S3
	case "kafka":
		return wallabypb.EndpointType_ENDPOINT_TYPE_KAFKA
	case "http":
		return wallabypb.EndpointType_ENDPOINT_TYPE_HTTP
	case "grpc":
		return wallabypb.EndpointType_ENDPOINT_TYPE_GRPC
	case "proto":
		return wallabypb.EndpointType_ENDPOINT_TYPE_PROTO
	case "pgstream":
		return wallabypb.EndpointType_ENDPOINT_TYPE_PGSTREAM
	case "snowpipe":
		return wallabypb.EndpointType_ENDPOINT_TYPE_SNOWPIPE
	case "parquet":
		return wallabypb.EndpointType_ENDPOINT_TYPE_PARQUET
	case "duckdb":
		return wallabypb.EndpointType_ENDPOINT_TYPE_DUCKDB
	case "bufstream":
		return wallabypb.EndpointType_ENDPOINT_TYPE_BUFSTREAM
	case "clickhouse":
		return wallabypb.EndpointType_ENDPOINT_TYPE_CLICKHOUSE
	default:
		return wallabypb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED
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

func endpointTypeToString(value wallabypb.EndpointType) string {
	switch value {
	case wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES:
		return "postgres"
	case wallabypb.EndpointType_ENDPOINT_TYPE_SNOWFLAKE:
		return "snowflake"
	case wallabypb.EndpointType_ENDPOINT_TYPE_S3:
		return "s3"
	case wallabypb.EndpointType_ENDPOINT_TYPE_KAFKA:
		return "kafka"
	case wallabypb.EndpointType_ENDPOINT_TYPE_HTTP:
		return "http"
	case wallabypb.EndpointType_ENDPOINT_TYPE_GRPC:
		return "grpc"
	case wallabypb.EndpointType_ENDPOINT_TYPE_PROTO:
		return "proto"
	case wallabypb.EndpointType_ENDPOINT_TYPE_PGSTREAM:
		return "pgstream"
	case wallabypb.EndpointType_ENDPOINT_TYPE_SNOWPIPE:
		return "snowpipe"
	case wallabypb.EndpointType_ENDPOINT_TYPE_PARQUET:
		return "parquet"
	case wallabypb.EndpointType_ENDPOINT_TYPE_DUCKDB:
		return "duckdb"
	case wallabypb.EndpointType_ENDPOINT_TYPE_BUFSTREAM:
		return "bufstream"
	case wallabypb.EndpointType_ENDPOINT_TYPE_CLICKHOUSE:
		return "clickhouse"
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
	case wallabypb.FlowState_FLOW_STATE_FAILED:
		return "failed"
	default:
		return ""
	}
}
