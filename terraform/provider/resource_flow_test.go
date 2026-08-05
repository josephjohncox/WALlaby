package main

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

func TestEndpointTerraformRoundTrips(t *testing.T) {
	t.Parallel()

	for _, endpointType := range []string{"redpanda", "iceberg"} {
		endpointType := endpointType
		t.Run(endpointType, func(t *testing.T) {
			t.Parallel()
			wire := endpointTypeFromString(endpointType)
			if got := endpointTypeToString(wire); got != endpointType {
				t.Fatalf("endpoint round trip=%d/%q, want %q", wire, got, endpointType)
			}
		})
	}
}

func TestValidateFlowResourceModel(t *testing.T) {
	t.Parallel()

	managedOptions := types.MapValueMust(types.StringType, map[string]attr.Value{
		"managed": types.StringValue("true"), "bootstrap": types.StringValue("never"),
	})
	icebergOptions := types.MapValueMust(types.StringType, map[string]attr.Value{
		"catalog_profile": types.StringValue("s3tables"), "destination_revision_id": types.StringValue("iceberg-s3tables-v1"),
	})
	valid := flowResourceModel{
		Source:       endpointModel{Type: types.StringValue("postgres"), Options: managedOptions},
		Destinations: []endpointModel{{Type: types.StringValue("iceberg"), Options: icebergOptions}},
		Config: &flowConfigModel{
			AckPolicy:       types.StringValue("materialized"),
			Materialization: &flowMaterializationPolicyModel{ProjectionID: types.StringValue("canonical_cdc_parquet_v1")},
		},
	}
	if diagnostics := validateFlowResourceModel(context.Background(), valid); diagnostics.HasError() {
		t.Fatalf("valid materialized config diagnostics=%v", diagnostics)
	}

	unknown := valid
	unknown.Config = &flowConfigModel{AckPolicy: types.StringValue("sometimes")}
	if diagnostics := validateFlowResourceModel(context.Background(), unknown); !diagnostics.HasError() {
		t.Fatal("unknown acknowledgement policy was silently mapped to unspecified")
	}

	mismatched := valid
	mismatched.Config = &flowConfigModel{
		AckPolicy:       types.StringValue("all"),
		Materialization: &flowMaterializationPolicyModel{ProjectionID: types.StringValue("canonical_cdc_parquet_v1")},
	}
	if diagnostics := validateFlowResourceModel(context.Background(), mismatched); !diagnostics.HasError() {
		t.Fatal("materialization was accepted without ack_policy=materialized")
	}

	wrongDestination := valid
	wrongDestination.Destinations = []endpointModel{{Type: types.StringValue("postgres"), Options: icebergOptions}}
	if diagnostics := validateFlowResourceModel(context.Background(), wrongDestination); !diagnostics.HasError() {
		t.Fatal("materialized flow accepted a non-Iceberg destination")
	}

	missingRevision := valid
	missingRevision.Destinations = []endpointModel{{Type: types.StringValue("iceberg"), Options: types.MapValueMust(types.StringType, map[string]attr.Value{})}}
	if diagnostics := validateFlowResourceModel(context.Background(), missingRevision); !diagnostics.HasError() {
		t.Fatal("materialized Iceberg flow accepted a missing destination revision")
	}

	persistedSecret := valid
	persistedSecret.Destinations = []endpointModel{{Type: types.StringValue("iceberg"), Options: types.MapValueMust(types.StringType, map[string]attr.Value{
		"destination_revision_id": types.StringValue("iceberg-v1"), "aws_session_token": types.StringValue("secret"),
	})}}
	if diagnostics := validateFlowResourceModel(context.Background(), persistedSecret); !diagnostics.HasError() {
		t.Fatal("materialized Iceberg flow accepted a persisted AWS session token")
	}
	persistedSecret.Config = nil
	if diagnostics := validateFlowResourceModel(context.Background(), persistedSecret); !diagnostics.HasError() {
		t.Fatal("default-ack Iceberg flow accepted a persisted AWS session token")
	}
}
