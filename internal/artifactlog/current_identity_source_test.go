package artifactlog

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestArtifactCatalogRuntimeHasNoLegacyAttemptAdoption(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate artifactlog source")
	}
	directory := filepath.Dir(filename)
	for _, name := range []string{"consumer.go", "catalog.go", "runtime.go"} {
		raw, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			t.Fatal(err)
		}
		source := string(raw)
		for _, forbidden := range []string{"legacy:", "upgrade legacy catalog attempt", "ReconcileRequest", "CatalogCommit =", "CatalogDisposition =", "_ ...string"} {
			if strings.Contains(source, forbidden) {
				t.Errorf("%s retains compatibility token %q", name, forbidden)
			}
		}
	}
}
