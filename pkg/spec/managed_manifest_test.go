package spec

import (
	"path/filepath"
	"reflect"
	"testing"
)

// TestManagedSpecsRegistered locks in the registration of the managed durability
// (ArtifactPublication) and managed PostgreSQL delivery (SourceFeedback) specs so
// their coverage manifests, name parsing, and disk manifests stay consistent.
func TestManagedSpecsRegistered(t *testing.T) {
	for _, name := range []SpecName{SpecManagedDurability, SpecManagedPostgresDel} {
		t.Run(string(name), func(t *testing.T) {
			runtimeManifest, ok := ManifestForSpec(name)
			if !ok {
				t.Fatalf("runtime manifest missing for %s", name)
			}
			if len(runtimeManifest.Actions) == 0 || len(runtimeManifest.Invariants) == 0 {
				t.Fatalf("%s manifest has no actions/invariants", name)
			}
			file := DefaultManifestFiles[name]
			if file == "" {
				t.Fatalf("%s has no default manifest file", name)
			}
			fileManifest, err := LoadManifest(filepath.Join("..", "..", "specs", file))
			if err != nil {
				t.Fatalf("LoadManifest(%s): %v", file, err)
			}
			if !reflect.DeepEqual(fileManifest.Actions, runtimeManifest.Actions) {
				t.Fatalf("%s actions drift:\n file=%v\n runtime=%v", name, fileManifest.Actions, runtimeManifest.Actions)
			}
			if !reflect.DeepEqual(fileManifest.Invariants, runtimeManifest.Invariants) {
				t.Fatalf("%s invariants drift:\n file=%v\n runtime=%v", name, fileManifest.Invariants, runtimeManifest.Invariants)
			}
			parsed, ok := ParseSpecName(string(name))
			if !ok || parsed != name {
				t.Fatalf("ParseSpecName(%s) = %s, %t", name, parsed, ok)
			}
		})
	}

	// Both managed specs must appear in AllManifests so spec-manifest regenerates
	// their coverage files.
	seen := map[SpecName]bool{}
	for _, m := range AllManifests() {
		seen[m.Spec] = true
	}
	for _, name := range []SpecName{SpecManagedDurability, SpecManagedPostgresDel} {
		if !seen[name] {
			t.Fatalf("AllManifests() is missing %s", name)
		}
	}
}

// TestManagedInvariantsAreSafetyCritical asserts the safety-critical invariants
// named by the managed models are present in their manifests. These are mirrored
// as executable checks in internal/failmatrix.
func TestManagedInvariantsAreSafetyCritical(t *testing.T) {
	durability, _ := ManifestForSpec(SpecManagedDurability)
	requireInvariant(t, durability, InvReceiptRequiresExternalCommit)
	requireInvariant(t, durability, InvAckSafety)
	requireInvariant(t, durability, InvRetentionSafety)
	requireInvariant(t, durability, InvAuthoritativeWritesHaveFence)

	delivery, _ := ManifestForSpec(SpecManagedPostgresDel)
	requireInvariant(t, delivery, InvSourceFlushRequiresAuthorization)
	requireInvariant(t, delivery, InvFlushReceiptRequiresObservedFlush)
	requireInvariant(t, delivery, InvRetryBounded)
	requireInvariant(t, delivery, InvRetentionRootProtectsCheckpoint)
}

func requireInvariant(t *testing.T, manifest Manifest, inv Invariant) {
	t.Helper()
	if _, ok := manifest.InvariantSet()[inv]; !ok {
		t.Fatalf("%s manifest missing safety-critical invariant %s", manifest.Spec, inv)
	}
}
