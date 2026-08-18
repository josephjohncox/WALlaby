package flow

import (
	"errors"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestSnowflakeDeploymentPolicyDoesNotDecodeOrRejectCustomDestinations(t *testing.T) {
	definition := Flow{Destinations: []*wallabypb.Endpoint{{
		Name: "custom", Config: &wallabypb.Endpoint_Custom{Custom: &wallabypb.CustomEndpointConfig{ConnectorType: "registered-custom"}},
	}}}
	if err := ValidateSnowflakeDeploymentPolicy(definition, nil, connector.SnowflakeDeploymentPolicy{}); err != nil {
		t.Fatalf("custom destination was coupled to Snowflake admission: %v", err)
	}
	snowflake := Flow{Destinations: []*wallabypb.Endpoint{{
		Name: "snowflake", Config: &wallabypb.Endpoint_Snowflake{Snowflake: &wallabypb.SnowflakeDestinationConfig{
			Dsn: "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false",
		}},
	}}}
	if err := ValidateSnowflakeDeploymentPolicy(snowflake, nil, connector.SnowflakeDeploymentPolicy{}); !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
		t.Fatalf("Snowflake destination admission error=%v", err)
	}
}
