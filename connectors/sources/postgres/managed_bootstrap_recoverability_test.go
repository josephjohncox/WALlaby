package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

type failingRecoveredBootstrapFinalizer struct {
	recordErr    error
	handoffErr   error
	recordCalls  int
	handoffCalls int
}

func (f *failingRecoveredBootstrapFinalizer) RecordPublication(context.Context, authority.RunFence, bootstrap.ExportedSnapshot, string, string, uuid.UUID) error {
	f.recordCalls++
	return f.recordErr
}

func (f *failingRecoveredBootstrapFinalizer) Handoff(context.Context, authority.RunFence, bootstrap.ExportedSnapshot) (connector.Checkpoint, error) {
	f.handoffCalls++
	return connector.Checkpoint{}, f.handoffErr
}

func TestCleanupManagedResourcesRequiresGuardBeforeConnectorSetup(t *testing.T) {
	t.Parallel()
	fence := connector.CleanupFence{RunFence: connector.RunFence{
		FlowIncarnationID: uuid.New(), FlowID: "cleanup", Generation: 1,
		AcquisitionID: uuid.New(), ExecutionID: "cleanup-execution", LeaseEpoch: 1,
	}}
	err := (&Source{}).CleanupManagedResources(context.Background(), fence, connector.RuntimeSpec{}, nil)
	if err == nil || err.Error() != "managed PostgreSQL cleanup requires cleanup authority guard" {
		t.Fatalf("CleanupManagedResources() error=%v, want mandatory guard rejection", err)
	}
}

func TestFinalizeRecoveredBootstrapPublicationFailuresStayRecoverable(t *testing.T) {
	t.Parallel()

	recordCause := errors.New("record publication unavailable")
	recordFailure := &failingRecoveredBootstrapFinalizer{recordErr: recordCause}
	_, err := finalizeRecoveredBootstrapPublication(context.Background(), recordFailure, authority.RunFence{}, bootstrap.ExportedSnapshot{}, "revision", "hash", uuid.New())
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) || !errors.Is(err, recordCause) {
		t.Fatalf("record error=%v, want indeterminate preserving cause", err)
	}
	if recordFailure.recordCalls != 1 || recordFailure.handoffCalls != 0 {
		t.Fatalf("record failure calls=(record:%d handoff:%d), want (1,0)", recordFailure.recordCalls, recordFailure.handoffCalls)
	}

	handoffCause := errors.New("handoff unavailable")
	handoffFailure := &failingRecoveredBootstrapFinalizer{handoffErr: handoffCause}
	_, err = finalizeRecoveredBootstrapPublication(context.Background(), handoffFailure, authority.RunFence{}, bootstrap.ExportedSnapshot{}, "revision", "hash", uuid.New())
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) || !errors.Is(err, handoffCause) {
		t.Fatalf("handoff error=%v, want indeterminate preserving cause", err)
	}
	if handoffFailure.recordCalls != 1 || handoffFailure.handoffCalls != 1 {
		t.Fatalf("handoff failure calls=(record:%d handoff:%d), want (1,1)", handoffFailure.recordCalls, handoffFailure.handoffCalls)
	}
}

func TestRecoverableBootstrapPublicationErrorPreservesCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("control receipt commit unavailable")
	err := recoverableBootstrapPublicationError("record destination publication", cause)
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("error=%v, want ErrDeliveryIndeterminate", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error=%v, want original cause", err)
	}
	if recoverableBootstrapPublicationError("no-op", nil) != nil {
		t.Fatal("nil publication error was not preserved")
	}
}
