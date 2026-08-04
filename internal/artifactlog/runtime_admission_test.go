package artifactlog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

type artifactTestProjector struct{ fingerprint string }

func (p artifactTestProjector) Fingerprint() string { return p.fingerprint }
func (artifactTestProjector) ProjectBatch(batch connector.Batch) (connector.Batch, stream.ProjectionDecision, error) {
	return batch, stream.ProjectionIncluded, nil
}
func (artifactTestProjector) ProjectTransaction(transaction connector.SourceTransaction) (connector.SourceTransaction, stream.ProjectionDecision, error) {
	return transaction, stream.ProjectionIncluded, nil
}

func TestCanonicalV2RuntimeRejectsUnboundOrMismatchedProjection(t *testing.T) {
	base := RuntimeConfig{Stream: StreamConfig{ProjectionID: ProjectionIDV2, MappingFingerprint: "expected"}, OrphanGrace: time.Second, Retention: time.Second, GCInterval: time.Second}
	if _, err := NewRuntime(context.Background(), nil, nil, base); err == nil || !containsError(err, "requires the immutable destination projector") {
		t.Fatalf("unbound v2 error=%v", err)
	}
	base.Projector = artifactTestProjector{fingerprint: "different"}
	if _, err := NewRuntime(context.Background(), nil, nil, base); err == nil || !containsError(err, "fingerprint mismatch") {
		t.Fatalf("mismatched projector error=%v", err)
	}
	base.Projector = artifactTestProjector{fingerprint: "expected"}
	base.Stream.MappingFingerprint = "other"
	if _, err := NewRuntime(context.Background(), nil, nil, base); err == nil || !containsError(err, "fingerprint mismatch") {
		t.Fatalf("recovery fingerprint mismatch error=%v", err)
	}
}

func containsError(err error, text string) bool {
	return err != nil && strings.Contains(err.Error(), text)
}

func TestResolveRuntimeReadAdmissionDefersOnlyRetryableConsumersBelowWatermark(t *testing.T) {
	t.Parallel()

	retryCause := errors.New("catalog temporarily unavailable")
	retryErr := errors.Join(ErrConsumerRetryable, retryCause)
	backpressureErr := errors.Join(ErrBackpressure, errors.New("high watermark"))
	terminalErr := errors.New("catalog schema conflict")
	controlErr := errors.New("control store unavailable")

	for _, test := range []struct {
		name         string
		consumerErr  error
		admissionErr error
		hasConsumers bool
		wantAdmitted bool
		wantWait     bool
		wantErr      error
	}{
		{name: "retry below watermark admits source read", consumerErr: retryErr, hasConsumers: true, wantAdmitted: true},
		{name: "retry at watermark waits", consumerErr: retryErr, admissionErr: backpressureErr, hasConsumers: true, wantWait: true},
		{name: "healthy consumer at watermark waits", admissionErr: backpressureErr, hasConsumers: true, wantWait: true},
		{name: "terminal consumer failure blocks", consumerErr: terminalErr, hasConsumers: true, wantErr: terminalErr},
		{name: "control failure blocks despite retryable consumer", consumerErr: retryErr, admissionErr: controlErr, hasConsumers: true, wantErr: controlErr},
		{name: "backpressure without consumer is returned", admissionErr: backpressureErr, wantErr: backpressureErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			admitted, wait, err := resolveRuntimeReadAdmission(test.consumerErr, test.admissionErr, test.hasConsumers)
			if admitted != test.wantAdmitted || wait != test.wantWait {
				t.Fatalf("decision=(admitted:%t wait:%t), want (%t,%t)", admitted, wait, test.wantAdmitted, test.wantWait)
			}
			if test.wantErr == nil && err != nil {
				t.Fatalf("error=%v, want nil", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("error=%v, want %v", err, test.wantErr)
			}
		})
	}
}
