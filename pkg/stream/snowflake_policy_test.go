package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

type snowflakeOpenCountingDestination struct{ opens int }

func (d *snowflakeOpenCountingDestination) Open(context.Context, connector.RuntimeSpec) error {
	d.opens++
	return nil
}
func (*snowflakeOpenCountingDestination) Write(context.Context, connector.Batch) error { return nil }
func (*snowflakeOpenCountingDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (*snowflakeOpenCountingDestination) TypeMappings() map[string]string { return nil }
func (*snowflakeOpenCountingDestination) Close(context.Context) error     { return nil }
func (*snowflakeOpenCountingDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{TableWrites: connector.TableWriteSemantics{Append: true}, SupportsStreaming: true}
}

func TestRunnerSnowflakePolicyDeniesBeforeAnyConnectorOpen(t *testing.T) {
	source := &fakeSource{}
	destination := &snowflakeOpenCountingDestination{}
	runner := Runner{
		Source: source, SourceSpec: connector.RuntimeSpec{Type: connector.EndpointPostgres}, FlowID: "denied",
		Destinations: []DestinationConfig{{
			Spec: connector.RuntimeSpec{Name: "snowflake", Type: connector.EndpointSnowflake, Options: map[string]string{"dsn": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"}},
			Dest: destination,
		}},
	}
	err := runner.Run(context.Background())
	if !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
		t.Fatalf("Run() error=%v", err)
	}
	if source.opens != 0 || destination.opens != 0 {
		t.Fatalf("denied runtime opened connectors: source=%d destination=%d", source.opens, destination.opens)
	}
}
