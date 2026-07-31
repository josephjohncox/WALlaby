package postgres

import (
	"errors"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

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
