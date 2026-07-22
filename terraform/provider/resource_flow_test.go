package main

import (
	"context"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

func TestValidateFlowResourceModel(t *testing.T) {
	t.Parallel()

	managedOptions := types.MapValueMust(types.StringType, map[string]attr.Value{
		"managed": types.StringValue("true"),
	})
	valid := flowResourceModel{
		Source: endpointModel{Type: types.StringValue("postgres"), Options: managedOptions},
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
}
