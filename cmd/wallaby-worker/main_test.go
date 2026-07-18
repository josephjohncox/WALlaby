package main

import "testing"

func TestManualPositiveGenerationDefaultsToWorkerBackend(t *testing.T) {
	command := newWallabyWorkerCommand()
	if err := command.ParseFlags([]string{"--flow-id", "manual", "--generation", "7"}); err != nil {
		t.Fatal(err)
	}
	backend, err := command.Flags().GetString("execution-backend")
	if err != nil {
		t.Fatal(err)
	}
	if backend != "worker" {
		t.Fatalf("execution-backend=%q, want worker for a manual positive-generation invocation", backend)
	}
}
