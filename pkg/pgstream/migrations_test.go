package pgstream

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"reflect"
	"sort"
	"testing"
)

func TestMigrationOrderAndChecksums(t *testing.T) {
	files, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	wantFiles := []string{"migrations/001_init.sql", "migrations/002_registry.sql"}
	if !reflect.DeepEqual(files, wantFiles) {
		t.Fatalf("migration order=%v", files)
	}
	wantChecksums := []string{"320c0f364fe90e68ce49c9efcec1a752d8d7b456bacead21d5fe3cc7fe2de040", "248103b69cf77651e715e5194e5d4ab1c1f00825957595dcef4209ae0289562a"}
	for index, file := range files {
		contents, err := migrationFS.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(contents)
		if checksum := hex.EncodeToString(digest[:]); checksum != wantChecksums[index] {
			t.Fatalf("migration %s checksum=%s", file, checksum)
		}
	}
}
