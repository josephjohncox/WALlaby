package cli

import (
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"testing"
)

func TestStrictRuntimeConfigKeepsCurrentFlagEnvironmentBindings(t *testing.T) {
	t.Setenv("WALLABY_WORKER_FLOW_ID", "environment-flow")
	t.Setenv("WALLABY_WORKER_EXECUTION_BACKEND", "kubernetes")
	t.Setenv("WALLABY_WORKER_MAX_EMPTY_READS", "17")
	command := &cobra.Command{Use: "worker"}
	command.Flags().String("flow-id", "", "")
	command.Flags().String("execution-backend", "worker", "")
	command.Flags().Int("max-empty-reads", 0, "")
	if err := InitViperFromCommand(command, ViperConfig{EnvPrefix: "WALLABY_WORKER", StrictRuntimeConfig: true}); err != nil {
		t.Fatal(err)
	}
	if got := ResolveStringFlag(command, "flow-id"); got != "environment-flow" {
		t.Fatalf("flow-id=%q", got)
	}
	if got := ResolveStringFlag(command, "execution-backend"); got != "kubernetes" {
		t.Fatalf("execution-backend=%q", got)
	}
	if got := ResolveIntFlag(command, "max-empty-reads"); got != 17 {
		t.Fatalf("max-empty-reads=%d", got)
	}
}

func TestStrictRuntimeConfigFlagOverridesEnvironment(t *testing.T) {
	t.Setenv("WALLABY_WORKER_FLOW_ID", "environment-flow")
	t.Setenv("WALLABY_WORKER_MAX_EMPTY_READS", "17")
	command := &cobra.Command{Use: "worker"}
	command.Flags().String("flow-id", "", "")
	command.Flags().Int("max-empty-reads", 0, "")
	if err := command.ParseFlags([]string{"--flow-id", "flag-flow", "--max-empty-reads", "9"}); err != nil {
		t.Fatal(err)
	}
	if err := InitViperFromCommand(command, ViperConfig{EnvPrefix: "WALLABY_WORKER", StrictRuntimeConfig: true}); err != nil {
		t.Fatal(err)
	}
	if got := ResolveStringFlag(command, "flow-id"); got != "flag-flow" {
		t.Fatalf("flow-id=%q", got)
	}
	if got := ResolveIntFlag(command, "max-empty-reads"); got != 9 {
		t.Fatalf("max-empty-reads=%d", got)
	}
}

func TestStrictRuntimeConfigSelectsWithoutViperDecode(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "wallaby.yaml")
	if err := os.WriteFile(path, []byte("api:\n  unknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := &cobra.Command{Use: "wallaby"}
	command.PersistentFlags().String("config", "", "")
	if err := InitViperFromCommand(command, ViperConfig{EnvPrefix: "WALLABY", ConfigName: "wallaby", ConfigSearchPath: []string{directory}, StrictRuntimeConfig: true}); err != nil {
		t.Fatalf("strict selector decoded runtime contents: %v", err)
	}
	selected, err := command.PersistentFlags().GetString("config")
	if err != nil {
		t.Fatal(err)
	}
	if selected != path {
		t.Fatalf("selected=%q, want %q", selected, path)
	}
}
