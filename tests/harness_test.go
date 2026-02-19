package tests

import (
	"os"
	"testing"

	"github.com/josephjohncox/wallaby/tests/integrationharness"
)

func TestMain(m *testing.M) {
	os.Exit(integrationharness.RunIntegrationHarness(m))
}
