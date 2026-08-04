package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServerRejectsUnknownRuntimeConfigPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wallaby.yaml")
	if err := os.WriteFile(path, []byte("api:\n  grpc-listen: ':8080'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := newWallabyCommand()
	command.SetArgs([]string{"--config", path})
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), `unknown key "api.grpc-listen"`) {
		t.Fatalf("error=%v", err)
	}
}
