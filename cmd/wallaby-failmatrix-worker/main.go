// Command wallaby-failmatrix-worker is the prebuilt child executable used by
// the credential-free OS-process failure evidence runner. It operates only on
// the runner's fsync-backed protocol-model state; it is not a destination.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/josephjohncox/wallaby/internal/failmatrix"
)

func main() {
	mode := flag.String("mode", "", "worker mode")
	state := flag.String("state", "", "durable state path")
	profile := flag.String("profile", "", "supported protocol profile")
	boundary := flag.String("boundary", "", "requested fault boundary")
	generation := flag.Int64("generation", 1, "worker generation")
	schedule := flag.String("schedule", "", "deterministic durable-transition handshake schedule")
	flag.Parse()

	if *state == "" {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix-worker: -state is required")
		os.Exit(2)
	}
	err := failmatrix.RunProcessWorker(failmatrix.ProcessWorkerConfig{
		Mode: failmatrix.ProcessWorkerMode(*mode), StatePath: *state,
		Profile: *profile, Boundary: failmatrix.Boundary(*boundary),
		Generation: *generation, Schedule: *schedule,
	})
	if errors.Is(err, failmatrix.ErrPlannedRestart) {
		os.Exit(failmatrix.PlannedRestartExitCode)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "wallaby-failmatrix-worker:", err)
		os.Exit(1)
	}
}
