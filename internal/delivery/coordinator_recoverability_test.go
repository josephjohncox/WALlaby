package delivery

import (
	"errors"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestRecoverablePostCommitErrorPreservesCauseAndClassification(t *testing.T) {
	t.Parallel()

	cause := errors.New("control store unavailable after target commit")
	err := recoverablePostCommitError("record delivery evidence", cause)
	if !errors.Is(err, connector.ErrDeliveryIndeterminate) {
		t.Fatalf("error=%v, want ErrDeliveryIndeterminate", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error=%v, want original control-store cause", err)
	}
	if recoverablePostCommitError("no-op", nil) != nil {
		t.Fatal("nil post-commit error was not preserved")
	}
}
