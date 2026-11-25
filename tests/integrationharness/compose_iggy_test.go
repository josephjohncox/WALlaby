package integrationharness

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestIggyComposeMaterializationPreservesImageAndRoutesAdvertisedPort(t *testing.T) {
	t.Parallel()
	fixture := []byte(`services:
  iggy:
    image: apache/iggy:0.10.0
    environment:
      IGGY_ROOT_USERNAME: iggy
      IGGY_ROOT_PASSWORD: development
      IGGY_TCP_ADDRESS: 0.0.0.0:8090
    ports: ["127.0.0.1:8090:8090"]
    healthcheck:
      test: [CMD, iggy]
`)
	data, image, user, password, err := materializeIggyFixture(fixture, 19090)
	if err != nil || image != "apache/iggy:0.10.0" || user != "iggy" || password != "development" {
		t.Fatalf("materialize failed: image=%s user=%s err=%v", image, user, err)
	}
	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	service := config["services"].(map[string]any)["iggy"].(map[string]any)
	env := service["environment"].(map[string]any)
	if env["IGGY_TCP_ADDRESS"] != "0.0.0.0:19090" || env["IGGY_NODE_ADVERTISED_ADDRESS"] != "127.0.0.1" || service["ports"].([]any)[0] != "127.0.0.1:19090:19090" || service["healthcheck"].(map[string]any)["disable"] != true {
		t.Fatal("SDK metadata and published port can diverge, or readiness did not move to Go")
	}
}

func TestNativeIggyProbeDoesNotIgnoreCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := waitForNativeIggy(ctx, func(context.Context) error {
		calls++
		cancel()
		return errors.New("not ready")
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("probe ignored cancellation: calls=%d err=%v", calls, err)
	}
}

type recordingComposeExecutor struct {
	calls [][]string
	err   error
}

func (e *recordingComposeExecutor) output(_ context.Context, args ...string) ([]byte, error) {
	e.calls = append(e.calls, append([]string(nil), args...))
	return nil, e.err
}

func TestComposeCleanupIsScopedToOwnedProject(t *testing.T) {
	t.Parallel()
	executor := &recordingComposeExecutor{}
	dir := t.TempDir()
	session := &composeIggySession{executor: executor, project: "wallaby-owned-test", file: filepath.Join(dir, "compose.json"), dir: dir}
	if err := session.close(); err != nil {
		t.Fatal(err)
	}
	if session.evidence.StoppedAt == nil {
		t.Fatal("successful scoped cleanup lacks a stop receipt")
	}
	want := []string{"compose", "--project-name", "wallaby-owned-test", "--file", session.file, "down", "--remove-orphans"}
	if len(executor.calls) != 1 || !reflect.DeepEqual(executor.calls[0], want) {
		t.Fatalf("cleanup can escape owned project: %v", executor.calls)
	}
}

func TestFailedComposeCleanupDoesNotClaimStopped(t *testing.T) {
	t.Parallel()
	failure := errors.New("docker unavailable")
	dir := t.TempDir()
	session := &composeIggySession{executor: &recordingComposeExecutor{err: failure}, project: "wallaby-owned-failed", file: filepath.Join(dir, "compose.json"), dir: dir}
	if err := session.close(); !errors.Is(err, failure) {
		t.Fatalf("cleanup failure lost: %v", err)
	}
	if session.evidence.StoppedAt != nil {
		t.Fatal("failed cleanup falsely recorded a successful stop")
	}
}
