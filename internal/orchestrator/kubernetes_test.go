package orchestrator

import (
	"regexp"
	"testing"
)

func TestEnsureFlowArgs_AppendsDefaults(t *testing.T) {
	args := []string{"--foo=bar"}
	out := ensureFlowArgs(args, "flow-1", 5)
	if !hasFlag(out, "flow-id") {
		t.Fatalf("expected flow-id flag added: %v", out)
	}
	if !hasFlag(out, "max-empty-reads") {
		t.Fatalf("expected max-empty-reads flag added: %v", out)
	}
}

func TestEnsureFlowArgs_DoesNotDuplicate(t *testing.T) {
	args := []string{"--flow-id=custom", "--max-empty-reads=2"}
	out := ensureFlowArgs(args, "flow-2", 10)
	if len(out) != len(args) {
		t.Fatalf("expected args unchanged, got %v", out)
	}
}

func TestBuildJobName_SanitizesAndBounds(t *testing.T) {
	name := buildJobName("Wallaby$Worker", "Flow_ABC")
	if len(name) > 63 {
		t.Fatalf("expected name <= 63 chars, got %d", len(name))
	}
	if !regexp.MustCompile(`^[a-z0-9-]+$`).MatchString(name) {
		t.Fatalf("expected name sanitized, got %s", name)
	}
}

func TestParseEnvFrom(t *testing.T) {
	entries := []string{"secret:foo", "configmap:bar", "bad"}
	out := parseEnvFrom(entries)
	if len(out) != 2 {
		t.Fatalf("expected 2 envFrom entries, got %d", len(out))
	}
	if out[0].SecretRef == nil || out[0].SecretRef.Name != "foo" {
		t.Fatalf("expected secret ref foo, got %#v", out[0].SecretRef)
	}
	if out[1].ConfigMapRef == nil || out[1].ConfigMapRef.Name != "bar" {
		t.Fatalf("expected configmap ref bar, got %#v", out[1].ConfigMapRef)
	}
}
