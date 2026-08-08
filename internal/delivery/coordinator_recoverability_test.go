package delivery

import (
	"errors"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestStringMapEqualRequiresIdenticalKeysAndValues(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		left, right map[string]string
		want        bool
	}{
		{name: "nil and empty", left: nil, right: map[string]string{}, want: true},
		{name: "same key and empty value", left: map[string]string{"a": ""}, right: map[string]string{"a": ""}, want: true},
		{name: "different empty-valued keys", left: map[string]string{"a": ""}, right: map[string]string{"b": ""}},
		{name: "different values", left: map[string]string{"a": "one"}, right: map[string]string{"a": "two"}},
		{name: "different lengths", left: map[string]string{"a": "one"}, right: map[string]string{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := stringMapEqual(test.left, test.right); got != test.want {
				t.Fatalf("stringMapEqual(%v,%v)=%t, want %t", test.left, test.right, got, test.want)
			}
		})
	}
}

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
