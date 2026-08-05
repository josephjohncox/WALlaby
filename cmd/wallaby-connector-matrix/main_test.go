package main

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/internal/runner"
)

func TestGeneratedMatrixContainsEveryRegistryRowAndConfiguredProfile(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdout
	os.Stdout = write
	runErr := run()
	_ = write.Close()
	os.Stdout = original
	output, readErr := io.ReadAll(read)
	_ = read.Close()
	if runErr != nil {
		t.Fatal(runErr)
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	matrix := string(output)
	if !strings.Contains(matrix, "Snowpipe is append-only staged delivery") || !strings.Contains(matrix, "errors are returned unchanged") {
		t.Fatal("matrix lacks Snowpipe staged-delivery failure semantics")
	}
	for _, registration := range runner.DestinationRegistrations() {
		if !strings.Contains(matrix, "`"+string(registration.Type)+"`") {
			t.Errorf("matrix missing destination %s", registration.Type)
		}
		for _, profile := range registration.Profiles {
			if !strings.Contains(matrix, "`"+string(profile.ID)+"`") {
				t.Errorf("matrix missing %s capability profile %s", registration.Type, profile.ID)
			}
		}
	}
}
