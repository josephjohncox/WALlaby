package grpc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type snowflakePolicyDispatcher struct{ calls int }

func (d *snowflakePolicyDispatcher) EnqueueRunOnce(context.Context, string, int64) error {
	d.calls++
	return nil
}

func grpcTestSnowflakePolicy(t *testing.T) connector.SnowflakeDeploymentPolicy {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = policy.Close() })
	return policy
}

func snowflakePolicyFlow(t *testing.T, id string, destinationType connector.EndpointType) flow.Flow {
	t.Helper()
	destination := connector.RuntimeSpec{Name: "target", Type: destinationType, Options: map[string]string{"dsn": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"}}
	definition := flow.Flow{
		ID: id, Source: testSourceEndpoint(connector.RuntimeSpec{Type: connector.EndpointPostgres}),
		Destinations: []*wallabypb.Endpoint{testDestinationEndpoint(destination)}, State: flow.StateCreated,
	}
	definition.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{destination})
	return definition
}

func TestFlowServiceValidateFlowReportsDeploymentAdmission(t *testing.T) {
	definition := snowflakePolicyFlow(t, "snowflake-plan", connector.EndpointSnowflake)
	request := &wallabypb.ValidateFlowRequest{Flow: flowToProtoForTest(definition)}
	disabled := NewFlowServiceWithRegistryAndPolicy(workflow.NewMemoryEngine(), nil, connector.DefaultRegistry, connector.SnowflakeDeploymentPolicy{})
	if _, err := disabled.ValidateFlow(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("disabled ValidateFlow() error=%v", err)
	}
	enabled := NewFlowServiceWithRegistryAndPolicy(workflow.NewMemoryEngine(), nil, connector.DefaultRegistry, grpcTestSnowflakePolicy(t))
	response, err := enabled.ValidateFlow(context.Background(), request)
	if err != nil || response == nil || !response.Admitted {
		t.Fatalf("enabled ValidateFlow() response=%+v error=%v", response, err)
	}
}

func TestFlowServiceRejectsSnowflakeCredentialBeforePersistenceWithoutDisclosure(t *testing.T) {
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	service := NewFlowServiceWithRegistryAndPolicy(engine, nil, connector.DefaultRegistry, grpcTestSnowflakePolicy(t))
	definition := snowflakePolicyFlow(t, "snowflake-secret-denied", connector.EndpointSnowflake)
	secret := "never-print-this"
	definition.Destinations[0].GetSnowflake().Dsn = "user:" + secret + "@account/db/schema"
	_, err := service.CreateFlow(ctx, &wallabypb.CreateFlowRequest{Flow: flowToProtoForTest(definition)})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateFlow() error=%v", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), definition.Destinations[0].GetSnowflake().Dsn) {
		t.Fatalf("CreateFlow() disclosed credential material: %v", err)
	}
	if _, getErr := engine.Get(ctx, definition.ID); getErr == nil {
		t.Fatal("credential-bearing flow reached persistence")
	}
}

func TestFlowServiceSnowflakeCreateUpdateReconfigureFailBeforePersistence(t *testing.T) {
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	disabled := NewFlowServiceWithRegistryAndPolicy(engine, nil, connector.DefaultRegistry, connector.SnowflakeDeploymentPolicy{})
	unsafe := snowflakePolicyFlow(t, "snowflake-create-denied", connector.EndpointSnowflake)
	if _, err := disabled.CreateFlow(ctx, &wallabypb.CreateFlowRequest{Flow: flowToProtoForTest(unsafe)}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateFlow() error=%v", err)
	}
	if _, err := engine.Get(ctx, unsafe.ID); err == nil {
		t.Fatal("denied create persisted a flow")
	}

	safe := snowflakePolicyFlow(t, "snowflake-update-denied", connector.EndpointPostgres)
	created, err := engine.Create(ctx, safe)
	if err != nil {
		t.Fatal(err)
	}
	candidate := snowflakePolicyFlow(t, created.ID, connector.EndpointSnowflake)
	if _, err := disabled.UpdateFlow(ctx, &wallabypb.UpdateFlowRequest{Flow: flowToProtoForTest(candidate)}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UpdateFlow() error=%v", err)
	}
	persisted, err := engine.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	destinations, err := persisted.DecodeDestinations(connector.DefaultRegistry)
	if err != nil || destinations[0].Type != connector.EndpointPostgres {
		t.Fatalf("denied update changed persistence: destinations=%v err=%v", destinations, err)
	}
	if _, err := disabled.ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{Flow: flowToProtoForTest(candidate)}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ReconfigureFlow() error=%v", err)
	}
	persisted, _ = engine.Get(ctx, created.ID)
	destinations, _ = persisted.DecodeDestinations(connector.DefaultRegistry)
	if destinations[0].Type != connector.EndpointPostgres {
		t.Fatal("denied reconfigure changed persistence")
	}
}

func TestFlowServiceSnowflakeStartResumeRunOnceDenyBeforeMutationOrDispatch(t *testing.T) {
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	definition := snowflakePolicyFlow(t, "snowflake-lifecycle-denied", connector.EndpointSnowflake)
	if _, err := engine.Create(ctx, definition); err != nil {
		t.Fatal(err)
	}
	dispatcher := &snowflakePolicyDispatcher{}
	disabled := NewFlowServiceWithRegistryAndPolicy(engine, dispatcher, connector.DefaultRegistry, connector.SnowflakeDeploymentPolicy{})
	if _, err := disabled.StartFlow(ctx, &wallabypb.StartFlowRequest{FlowId: definition.ID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartFlow() error=%v", err)
	}
	persisted, _ := engine.Get(ctx, definition.ID)
	if persisted.State != flow.StateCreated {
		t.Fatalf("denied start state=%s", persisted.State)
	}

	enabled := NewFlowServiceWithRegistryAndPolicy(engine, dispatcher, connector.DefaultRegistry, grpcTestSnowflakePolicy(t))
	if _, err := enabled.StartFlow(ctx, &wallabypb.StartFlowRequest{FlowId: definition.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := enabled.PauseFlow(ctx, &wallabypb.PauseFlowRequest{FlowId: definition.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := disabled.ResumeFlow(ctx, &wallabypb.ResumeFlowRequest{FlowId: definition.ID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ResumeFlow() error=%v", err)
	}
	persisted, _ = engine.Get(ctx, definition.ID)
	if persisted.State != flow.StatePaused {
		t.Fatalf("denied resume state=%s", persisted.State)
	}
	if _, err := enabled.ResumeFlow(ctx, &wallabypb.ResumeFlowRequest{FlowId: definition.ID}); err != nil {
		t.Fatal(err)
	}
	before := dispatcher.calls
	if _, err := disabled.RunFlowOnce(ctx, &wallabypb.RunFlowOnceRequest{FlowId: definition.ID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RunFlowOnce() error=%v", err)
	}
	if dispatcher.calls != before {
		t.Fatalf("denied run-once dispatched: before=%d after=%d", before, dispatcher.calls)
	}
}
