package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/internal/cli"
	"github.com/josephjohncox/wallaby/internal/config"
)

func TestWorkerRejectsUnknownRuntimeConfigPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.json")
	if err := os.WriteFile(path, []byte(`{"api":{"grpc-listen":":8080"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	command := newWallabyWorkerCommand()
	command.SetArgs([]string{"--config", path, "--flow-id", "fixture"})
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), `unknown key "api.grpc-listen"`) {
		t.Fatalf("error=%v", err)
	}
}

func TestWorkerCurrentEnvironmentOnlyFlags(t *testing.T) {
	stringsByFlag := map[string]string{"config": "/tmp/worker-env.yaml", "flow-id": "environment-flow", "execution-backend": "kubernetes", "execution-id": "execution-7", "mode": "backfill", "tables": "public.events", "schemas": "public", "start-lsn": "0/123", "partition-column": "id", "snowflake-account": "account", "snowflake-user": "user", "snowflake-host": "account.snowflakecomputing.com", "snowflake-private-key-file": "/deployment/key.pem"}
	for flag, value := range stringsByFlag {
		t.Setenv("WALLABY_WORKER_"+strings.ToUpper(strings.ReplaceAll(flag, "-", "_")), value)
	}
	intsByFlag := map[string]int{"max-empty-reads": 23, "snapshot-workers": 4, "partition-count": 8}
	for flag, value := range intsByFlag {
		t.Setenv("WALLABY_WORKER_"+strings.ToUpper(strings.ReplaceAll(flag, "-", "_")), fmt.Sprint(value))
	}
	t.Setenv("WALLABY_WORKER_GENERATION", "7")
	t.Setenv("WALLABY_WORKER_RESOLVE_STAGING", "true")
	t.Setenv("WALLABY_WORKER_SNOWFLAKE_ENABLED", "true")
	command := newWallabyWorkerCommand()
	if err := command.ParseFlags(nil); err != nil {
		t.Fatal(err)
	}
	if err := initWallabyWorkerConfig(command); err != nil {
		t.Fatal(err)
	}
	for flag, want := range stringsByFlag {
		if got := cli.ResolveStringFlag(command, flag); got != want {
			t.Fatalf("%s=%q, want %q", flag, got, want)
		}
	}
	for flag, want := range intsByFlag {
		if got := cli.ResolveIntFlag(command, flag); got != want {
			t.Fatalf("%s=%d, want %d", flag, got, want)
		}
	}
	if got := cli.ResolveInt64Flag(command, "generation"); got != 7 {
		t.Fatalf("generation=%d", got)
	}
	if got := cli.ResolveBoolFlag(command, "resolve-staging"); !got {
		t.Fatalf("resolve-staging=false")
	}
	if got := cli.ResolveBoolFlag(command, "snowflake-enabled"); !got {
		t.Fatalf("snowflake-enabled=false")
	}
}

func TestWorkerEnvironmentOnlyFlowIDReachesRuntimeConfiguration(t *testing.T) {
	t.Setenv("WALLABY_WORKER_FLOW_ID", "environment-flow")
	t.Setenv("WALLABY_WORKER_ENV", "test")
	t.Setenv("WALLABY_WORKER_WORKFLOW_STORE", "memory")
	t.Setenv("WALLABY_POSTGRES_DSN", "")
	t.Setenv("WALLABY_WORKER_POSTGRES_DSN", "")
	command := newWallabyWorkerCommand()
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), "WALLABY_POSTGRES_DSN is required") {
		t.Fatalf("error=%v", err)
	}
	if strings.Contains(err.Error(), "flow-id is required") {
		t.Fatalf("worker ignored WALLABY_WORKER_FLOW_ID: %v", err)
	}
}

func TestWorkerFlagsOverrideCurrentEnvironment(t *testing.T) {
	t.Setenv("WALLABY_WORKER_FLOW_ID", "environment-flow")
	t.Setenv("WALLABY_WORKER_EXECUTION_BACKEND", "kubernetes")
	t.Setenv("WALLABY_WORKER_MAX_EMPTY_READS", "23")
	t.Setenv("WALLABY_WORKER_SNOWFLAKE_ENABLED", "true")
	t.Setenv("WALLABY_WORKER_SNOWFLAKE_ACCOUNT", "environment-account")
	t.Setenv("WALLABY_WORKER_SNOWFLAKE_USER", "environment-user")
	t.Setenv("WALLABY_WORKER_SNOWFLAKE_HOST", "environment.snowflakecomputing.com")
	t.Setenv("WALLABY_WORKER_SNOWFLAKE_PRIVATE_KEY_FILE", "/environment/key.pem")
	command := newWallabyWorkerCommand()
	if err := command.ParseFlags([]string{"--flow-id", "flag-flow", "--execution-backend", "worker", "--max-empty-reads", "11", "--snowflake-enabled=false", "--snowflake-account", "flag-account", "--snowflake-user", "flag-user", "--snowflake-host", "flag.snowflakecomputing.com", "--snowflake-private-key-file", "/flag/key.pem"}); err != nil {
		t.Fatal(err)
	}
	if err := initWallabyWorkerConfig(command); err != nil {
		t.Fatal(err)
	}
	if got := cli.ResolveStringFlag(command, "flow-id"); got != "flag-flow" {
		t.Fatalf("flow-id=%q", got)
	}
	if got := cli.ResolveStringFlag(command, "execution-backend"); got != "worker" {
		t.Fatalf("execution-backend=%q", got)
	}
	if got := cli.ResolveIntFlag(command, "max-empty-reads"); got != 11 {
		t.Fatalf("max-empty-reads=%d", got)
	}
	if got := cli.ResolveBoolFlag(command, "snowflake-enabled"); got {
		t.Fatal("explicit false Snowflake flag did not override environment true")
	}
	if got := cli.ResolveStringFlag(command, "snowflake-private-key-file"); got != "/flag/key.pem" {
		t.Fatalf("snowflake-private-key-file=%q", got)
	}
}

func TestAuthoritativeWorkerSnowflakeFalseOverridesStaleEnabledConfig(t *testing.T) {
	command := newWallabyWorkerCommand()
	if err := command.ParseFlags([]string{"--snowflake-enabled=false", "--snowflake-private-key-file="}); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Snowflake: config.SnowflakeConfig{Enabled: true, StreamingREST: config.SnowflakeStreamingRESTConfig{Enabled: true}}}
	policy, err := resolveWorkerSnowflakePolicy(command, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Enabled() || cfg.Snowflake.PrivateKeyFile != "" || cfg.Snowflake.Enabled || cfg.Snowflake.StreamingREST.Enabled {
		t.Fatalf("authoritative false was widened: policy=%+v config=%+v", policy, cfg.Snowflake)
	}
}

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
