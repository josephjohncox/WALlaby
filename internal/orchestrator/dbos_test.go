package orchestrator

import (
	"slices"
	"strings"
	"testing"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

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
