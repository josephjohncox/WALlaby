package iggy

import (
	"context"
	"errors"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestRecoveryClassifiesUnknownRefusedAndLostOperations(t *testing.T) {
	for _, test := range []struct {
		name  string
		err   error
		fresh bool
	}{
		{name: "still uncertain", err: ierror.OperationUncertain{OperationID: 7}},
		{name: "cancelled", err: context.Canceled},
		{name: "client shutdown", err: ierror.ErrClientShutdown},
		{name: "unclassified", err: errors.New("unclassified transport failure")},
		{name: "refused", err: ierror.ErrUnauthorized, fresh: true},
		{name: "receipt lost", err: ierror.OperationNotRetained{OperationID: 7}, fresh: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			producer := &fakeProducer{sendErr: ierror.OperationUncertain{OperationID: 7}}
			destination := openFake(t, testSpec(), producer)
			if err := destination.Write(context.Background(), testBatch()); err == nil {
				t.Fatal("uncertain send acknowledged")
			}
			producer.resolveErr = test.err
			if err := destination.Write(context.Background(), testBatch()); err == nil {
				t.Fatal("failed resolve acknowledged")
			}
			if (destination.pending == nil) != test.fresh || (len(producer.abandoned) == 1) != test.fresh {
				t.Fatalf("pending=%v abandoned=%v", destination.pending, producer.abandoned)
			}
			producer.sendErr = nil
			if test.fresh {
				if err := destination.Write(context.Background(), testBatch()); err != nil || len(producer.messages) != 2 {
					t.Fatalf("fresh retry err=%v sends=%d", err, len(producer.messages))
				}
			} else if len(producer.messages) != 1 {
				t.Fatal("resent an unknown retained request")
			}
		})
	}
}

func TestCancelledRecoveryDoesNotResolveOrDiscardPending(t *testing.T) {
	producer := &fakeProducer{sendErr: ierror.OperationUncertain{OperationID: 7}}
	destination := openFake(t, testSpec(), producer)
	_ = destination.Write(context.Background(), testBatch())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := destination.Write(ctx, testBatch()); !errors.Is(err, context.Canceled) || destination.pending == nil || len(producer.resolved) != 0 || len(producer.abandoned) != 0 {
		t.Fatalf("cancelled recovery changed ownership: %v", err)
	}
}

func TestFailedAbandonKeepsPending(t *testing.T) {
	producer := &fakeProducer{sendErr: ierror.OperationUncertain{OperationID: 7}, resolveErr: ierror.ErrUnauthorized, abandonErr: context.Canceled}
	destination := openFake(t, testSpec(), producer)
	_ = destination.Write(context.Background(), testBatch())
	if err := destination.Write(context.Background(), testBatch()); !errors.Is(err, context.Canceled) || destination.pending == nil || len(producer.messages) != 1 {
		t.Fatalf("failed release discarded recovery state: %v", err)
	}
}

func TestValidationRejectsSDKTimeoutOverLimit(t *testing.T) {
	spec := testSpec()
	spec.Options["timeout"] = "31s"
	if err := (&Destination{}).ValidateSpec(spec); err == nil {
		t.Fatal("timeout above SDK cap accepted")
	}
}
