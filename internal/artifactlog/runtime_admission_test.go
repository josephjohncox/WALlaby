package artifactlog

import (
	"errors"
	"testing"
)

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
