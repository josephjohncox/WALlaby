package main

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	providerschema "github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type ductstreamProvider struct {
	version string
}

type providerModel struct {
	Endpoint types.String `tfsdk:"endpoint"`
	Insecure types.Bool   `tfsdk:"insecure"`
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &ductstreamProvider{version: version}
	}
}

func (p *ductstreamProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "ductstream"
	resp.Version = p.version
}

func (p *ductstreamProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = providerschema.Schema{
		Attributes: map[string]providerschema.Attribute{
			"endpoint": providerschema.StringAttribute{
				Required: true,
			},
			"insecure": providerschema.BoolAttribute{
				Optional: true,
			},
		},
	}
}

func (p *ductstreamProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data providerModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	endpoint := data.Endpoint.ValueString()
	insecure := true
	if !data.Insecure.IsNull() {
		insecure = data.Insecure.ValueBool()
	}

	client, err := newClient(ctx, endpoint, insecure)
	if err != nil {
		resp.Diagnostics.AddError("Client Error", err.Error())
		return
	}

	resp.DataSourceData = client
	resp.ResourceData = client
}

func (p *ductstreamProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewFlowResource,
	}
}

func (p *ductstreamProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return nil
}
