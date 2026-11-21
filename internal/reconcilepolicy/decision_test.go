package reconcilepolicy_test

import (
	"math"
	"testing"

	"github.com/josephjohncox/wallaby/internal/reconcilepolicy"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// This compile-time instantiation fails if the wire type stops being uint8.
func requireUint8Wire[T ~uint8]() {}

var _ = requireUint8Wire[connector.DeliveryDisposition]

func TestRuntimeWireValuesMatchDecisionTable(t *testing.T) {
	if uint64(connector.DeliveryIndeterminate) != 0 || uint64(connector.DeliveryNotApplied) != 1 || uint64(connector.DeliveryApplied) != 2 {
		t.Fatal("runtime wire values drifted from the proof subject")
	}
}

func TestDecideAllRuntimeValuesAndMachineBoundaries(t *testing.T) {
	values := make([]uint64, 0, 260)
	for value := uint64(0); value <= 255; value++ {
		values = append(values, value)
	}
	values = append(values, 256, 1<<32, 1<<63, math.MaxUint64)
	for _, value := range values {
		for _, failed := range []bool{false, true} {
			got := reconcilepolicy.Decide(value, failed)
			want := reconcilepolicy.Reject
			if failed || value == 0 {
				want = reconcilepolicy.Wait
			} else if value == 1 {
				want = reconcilepolicy.Retry
			} else if value == 2 {
				want = reconcilepolicy.Adopt
			}
			if got != want {
				t.Fatalf("D-SEMANTICS: observation=%d failed=%v got=%d want=%d", value, failed, got, want)
			}
		}
	}
}
