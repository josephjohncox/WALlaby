package main

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/types"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
)

func TestIcebergEndpointTerraformRoundTrip(t *testing.T) {
	t.Parallel()
	wire := endpointTypeFromString("iceberg")
	if wire != wallabypb.EndpointType_ENDPOINT_TYPE_ICEBERG || endpointTypeToString(wire) != "iceberg" {
		t.Fatalf("Iceberg Terraform endpoint round trip=%d/%q", wire, endpointTypeToString(wire))
	}
}

func TestValidateFlowResourceModel(t *testing.T) {
	managedOptions := types.MapValueMust(types.StringType, map[string]attr.Value{"managed": types.StringValue("true"), "bootstrap": types.StringValue("never")})
	icebergOptions := types.MapValueMust(types.StringType, map[string]attr.Value{"catalog_profile": types.StringValue("s3tables"), "destination_revision_id": types.StringValue("iceberg-s3tables-v1")})
	valid := flowResourceModel{
		Source:       endpointModel{Name: types.StringValue("source"), Type: types.StringValue("postgres"), Options: managedOptions},
		Destinations: []endpointModel{{Name: types.StringValue("iceberg"), Type: types.StringValue("iceberg"), Options: icebergOptions}},
		Config:       &flowConfigModel{AckPolicy: types.StringValue("materialized"), Materialization: &flowMaterializationPolicyModel{ProjectionID: types.StringValue("canonical_cdc_parquet_v2")}, TableMappings: testAppendMappings("iceberg")},
	}
	if diagnostics := validateFlowResourceModel(context.Background(), valid); diagnostics.HasError() {
		t.Fatalf("valid materialized config diagnostics=%v", diagnostics)
	}

	unknown := valid
	unknown.Config = &flowConfigModel{AckPolicy: types.StringValue("sometimes"), TableMappings: testAppendMappings("iceberg")}
	if diagnostics := validateFlowResourceModel(context.Background(), unknown); !diagnostics.HasError() {
		t.Fatal("unknown acknowledgement policy was silently mapped to unspecified")
	}

	mismatched := valid
	mismatched.Config = &flowConfigModel{AckPolicy: types.StringValue("all"), Materialization: &flowMaterializationPolicyModel{ProjectionID: types.StringValue("canonical_cdc_parquet_v2")}, TableMappings: testAppendMappings("iceberg")}
	if diagnostics := validateFlowResourceModel(context.Background(), mismatched); !diagnostics.HasError() {
		t.Fatal("materialization was accepted without ack_policy=materialized")
	}

	wrongDestination := valid
	wrongDestination.Destinations = []endpointModel{{Name: types.StringValue("iceberg"), Type: types.StringValue("postgres"), Options: icebergOptions}}
	if diagnostics := validateFlowResourceModel(context.Background(), wrongDestination); !diagnostics.HasError() {
		t.Fatal("materialized flow accepted a non-Iceberg destination")
	}

	missingRevision := valid
	missingRevision.Destinations = []endpointModel{{Name: types.StringValue("iceberg"), Type: types.StringValue("iceberg"), Options: types.MapValueMust(types.StringType, map[string]attr.Value{})}}
	if diagnostics := validateFlowResourceModel(context.Background(), missingRevision); !diagnostics.HasError() {
		t.Fatal("materialized Iceberg flow accepted a missing destination revision")
	}

	persistedSecret := valid
	persistedSecret.Destinations = []endpointModel{{Name: types.StringValue("iceberg"), Type: types.StringValue("iceberg"), Options: types.MapValueMust(types.StringType, map[string]attr.Value{"destination_revision_id": types.StringValue("iceberg-v1"), "aws_session_token": types.StringValue("secret")})}}
	if diagnostics := validateFlowResourceModel(context.Background(), persistedSecret); !diagnostics.HasError() {
		t.Fatal("materialized Iceberg flow accepted a persisted AWS session token")
	}
	persistedSecret.Config = nil
	if diagnostics := validateFlowResourceModel(context.Background(), persistedSecret); !diagnostics.HasError() {
		t.Fatal("default-ack Iceberg flow accepted a persisted AWS session token or missing mappings")
	}
}
