package orchestrator

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func dbosPolicyTestFlow(t *testing.T, id string, destination connector.RuntimeSpec) flow.Flow {
	t.Helper()
	source, err := endpointcodec.Encode(connector.RuntimeSpec{Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeCDC}}, endpointcodec.RoleSource)
	if err != nil {
		t.Fatal(err)
	}
	encodedDestination, err := endpointcodec.Encode(destination, endpointcodec.RoleDestination)
	if err != nil {
		t.Fatal(err)
	}
	definition := flow.Flow{ID: id, Name: id, Source: source, Destinations: []*wallabypb.Endpoint{encodedDestination}, State: flow.StateCreated}
	definition.Config.TableMappings = flow.NewTableMappings([]connector.RuntimeSpec{destination})
	return definition
}

func TestDBOSSnowflakePolicyDeniesDirectEnqueueAndDoesNotStarveOtherScheduledFlows(t *testing.T) {
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	snowflakeSpec := connector.RuntimeSpec{Name: "snowflake", Type: connector.EndpointSnowflake, Options: map[string]string{"dsn": "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false"}}
	postgresSpec := connector.RuntimeSpec{Name: "postgres", Type: connector.EndpointPostgres}
	for _, definition := range []flow.Flow{dbosPolicyTestFlow(t, "snowflake-old", snowflakeSpec), dbosPolicyTestFlow(t, "postgres-new", postgresSpec)} {
		if _, err := engine.Create(ctx, definition); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Start(ctx, definition.ID); err != nil {
			t.Fatal(err)
		}
	}
	orchestrator := &DBOSOrchestrator{engine: engine, factory: runner.Factory{ConnectorRegistry: connector.DefaultRegistry}}
	if err := orchestrator.EnqueueGeneration(ctx, "snowflake-old", 1); !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
		t.Fatalf("EnqueueGeneration() error=%v", err)
	}
	if err := orchestrator.EnqueueRunOnce(ctx, "snowflake-old", 1); !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
		t.Fatalf("EnqueueRunOnce() error=%v", err)
	}
	var enqueued []string
	count, err := orchestrator.dispatchScheduledFlows(ctx, time.Unix(123, 0), func(flowID string, _ int64, _, _ string) error {
		enqueued = append(enqueued, flowID)
		return nil
	})
	if count != 1 || !slices.Equal(enqueued, []string{"postgres-new"}) || !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
		t.Fatalf("scheduled count=%d flows=%v error=%v", count, enqueued, err)
	}
}

func TestDBOSStreamingWorkflowBindsCurrentDeploymentFingerprint(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = policy.Close() }()
	spec := connector.RuntimeSpec{Name: "stream", Type: connector.EndpointSnowflake, Options: map[string]string{
		"dsn":             "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false",
		"managed_profile": connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
	}}
	definition := dbosPolicyTestFlow(t, "streaming", spec)
	orchestrator := &DBOSOrchestrator{factory: runner.Factory{ConnectorRegistry: connector.DefaultRegistry, SnowflakePolicy: policy}, snowflakePolicy: policy}
	fingerprint, err := orchestrator.flowSnowflakePolicyFingerprint(definition)
	if err != nil || fingerprint == "" {
		t.Fatalf("fingerprint=%q error=%v", fingerprint, err)
	}
	input := FlowRunInput{FlowID: definition.ID, Generation: 1, SnowflakePolicyFingerprint: fingerprint}
	if err := orchestrator.validateFlowRunPolicy(definition, input); err != nil {
		t.Fatalf("matching workflow policy rejected: %v", err)
	}
	if _, err := orchestrator.factory.Destinations([]connector.RuntimeSpec{spec}); err != nil {
		t.Fatalf("matching policy did not reach destination factory: %v", err)
	}
	if err := orchestrator.validateFlowRunPolicy(definition, FlowRunInput{FlowID: definition.ID, Generation: 1}); err == nil || !strings.Contains(err.Error(), "fingerprint differs") {
		t.Fatalf("missing persisted fingerprint error=%v", err)
	}

	rotatedKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", rotatedKey, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rotated.Close() }()
	rotatedOrchestrator := &DBOSOrchestrator{factory: runner.Factory{ConnectorRegistry: connector.DefaultRegistry, SnowflakePolicy: rotated}, snowflakePolicy: rotated}
	if err := rotatedOrchestrator.validateFlowRunPolicy(definition, input); err == nil || !strings.Contains(err.Error(), "fingerprint differs") {
		t.Fatalf("key rotation adopted stale workflow input: %v", err)
	}
	disabled := &DBOSOrchestrator{factory: runner.Factory{ConnectorRegistry: connector.DefaultRegistry}, snowflakePolicy: connector.SnowflakeDeploymentPolicy{}}
	if err := disabled.validateFlowRunPolicy(definition, input); !errors.Is(err, connector.ErrSnowflakeExecutionDisabled) {
		t.Fatalf("disabled recovery policy error=%v", err)
	}
}

func TestDBOSScheduledStreamingDispatchCarriesPolicyFingerprint(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewSnowflakeDeploymentPolicyWithPrivateKey("account", "user", "account.snowflakecomputing.com", key, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = policy.Close() }()
	ctx := context.Background()
	engine := workflow.NewMemoryEngine()
	spec := connector.RuntimeSpec{Name: "stream", Type: connector.EndpointSnowflake, Options: map[string]string{
		"dsn":             "user:@account/db/schema?authenticator=snowflake_jwt&ocspFailOpen=false",
		"managed_profile": connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
	}}
	definition := dbosPolicyTestFlow(t, "scheduled-stream", spec)
	if _, err := engine.Create(ctx, definition); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Start(ctx, definition.ID); err != nil {
		t.Fatal(err)
	}
	orchestrator := &DBOSOrchestrator{engine: engine, factory: runner.Factory{ConnectorRegistry: connector.DefaultRegistry, SnowflakePolicy: policy}, snowflakePolicy: policy}
	var captured string
	count, err := orchestrator.dispatchScheduledFlows(ctx, time.Unix(123, 0), func(_ string, _ int64, _ string, fingerprint string) error {
		captured = fingerprint
		return nil
	})
	if err != nil || count != 1 || captured == "" {
		t.Fatalf("scheduled count/fingerprint/error=%d/%q/%v", count, captured, err)
	}
}

func TestNewDBOSOrchestratorRequiresCheckpointStore(t *testing.T) {
	t.Parallel()
	_, err := NewDBOSOrchestrator(context.Background(), Config{}, workflow.NewMemoryEngine(), nil, runner.Factory{})
	if err == nil || !strings.Contains(err.Error(), "durable checkpoint storage is required") {
		t.Fatalf("NewDBOSOrchestrator() error=%v, want durable checkpoint requirement", err)
	}
}

func TestDBOSWorkflowGenerationIdentity(t *testing.T) {
	t.Parallel()
	prefix := flowWorkflowPrefix("orders/eu")
	workflowID := prefix + "g-42"
	generation, ok := dbosWorkflowGeneration(prefix, workflowID)
	if !ok || generation != 42 {
		t.Fatalf("dbosWorkflowGeneration()=(%d,%v)", generation, ok)
	}
	scheduledGeneration, ok := dbosWorkflowGeneration(prefix, prefix+"g-42-s-123")
	if !ok || scheduledGeneration != 42 {
		t.Fatalf("scheduled generation parse=(%d,%v)", scheduledGeneration, ok)
	}
	if _, ok := dbosWorkflowGeneration(prefix, prefix+"legacy-random"); ok {
		t.Fatal("legacy random workflow id unexpectedly parsed as generation")
	}
}

func TestClassifyDBOSWorkflowsReturnsExactTerminalExecutionIDs(t *testing.T) {
	t.Parallel()
	prefix := flowWorkflowPrefix("orders")
	terminalOne := prefix + "g-1"
	terminalTwo := prefix + "g-2-s-10"
	pending := prefix + "g-2-r-pending"
	future := prefix + "g-3"
	terminal, cancellable, remaining := classifyDBOSWorkflows(prefix, 2, []dbos.WorkflowStatus{
		{ID: terminalTwo, Status: dbos.WorkflowStatusCancelled},
		{ID: future, Status: dbos.WorkflowStatusSuccess},
		{ID: "other", Status: dbos.WorkflowStatusSuccess},
		{ID: terminalOne, Status: dbos.WorkflowStatusMaxRecoveryAttemptsExceeded},
		{ID: pending, Status: dbos.WorkflowStatusPending},
	})
	if !slices.Equal(terminal, []string{terminalOne, terminalTwo}) {
		t.Fatalf("terminal ids=%v, want exact in-fence workflow ids", terminal)
	}
	if !slices.Equal(cancellable, []string{pending}) || remaining != 1 {
		t.Fatalf("cancellable=%v remaining=%d", cancellable, remaining)
	}
}

func TestFlowWorkflowPrefixDisambiguatesSanitizedFlowIDs(t *testing.T) {
	t.Parallel()

	leftID := "orders/eu"
	rightID := "orders eu"
	if sanitizeName(leftID) != sanitizeName(rightID) {
		t.Fatalf("test inputs do not collide after sanitization")
	}

	left := flowWorkflowPrefix(leftID)
	right := flowWorkflowPrefix(rightID)
	if left == right {
		t.Fatalf("flowWorkflowPrefix() collision: %q", left)
	}
	if got := flowWorkflowPrefix(leftID); got != left {
		t.Fatalf("flowWorkflowPrefix() is not stable: first=%q second=%q", left, got)
	}
	if !strings.HasPrefix(left, "wallaby-flow-orders-eu-") || !strings.HasSuffix(left, "-") {
		t.Fatalf("unexpected workflow prefix %q", left)
	}
}
