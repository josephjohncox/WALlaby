package snowflake

import "testing"

func TestStreamRequestTransitionGraphFailsClosed(t *testing.T) {
	phases := []streamRequestPhase{streamRequestPrepared, streamRequestSendingUnknown, streamRequestAccepted, streamRequestCommitted, streamRequestProvenAbsent, streamRequestReceipted, "UNKNOWN"}
	allowed := map[[2]streamRequestPhase]bool{
		{streamRequestPrepared, streamRequestSendingUnknown}:       true,
		{streamRequestPrepared, streamRequestProvenAbsent}:         true,
		{streamRequestSendingUnknown, streamRequestSendingUnknown}: true,
		{streamRequestSendingUnknown, streamRequestAccepted}:       true,
		{streamRequestSendingUnknown, streamRequestCommitted}:      true,
		{streamRequestSendingUnknown, streamRequestProvenAbsent}:   true,
		{streamRequestAccepted, streamRequestCommitted}:            true,
		{streamRequestAccepted, streamRequestProvenAbsent}:         true,
		{streamRequestCommitted, streamRequestCommitted}:           true,
		{streamRequestCommitted, streamRequestReceipted}:           true,
		{streamRequestProvenAbsent, streamRequestProvenAbsent}:     true,
		{streamRequestReceipted, streamRequestReceipted}:           true,
	}
	for _, from := range phases {
		for _, to := range phases {
			if got, want := validStreamRequestTransition(from, to), allowed[[2]streamRequestPhase{from, to}]; got != want {
				t.Fatalf("transition %s -> %s allowed=%t want=%t", from, to, got, want)
			}
		}
	}
}
