package schemaregistry

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestMigrationOrderAndChecksums(t *testing.T) {
	files, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	wantFiles := []string{"migrations/001_init.sql", "migrations/002_unique_subject_version.sql"}
	if !reflect.DeepEqual(files, wantFiles) {
		t.Fatalf("migration order=%v", files)
	}
	wantChecksums := []string{"f57e10cda6ff364ac1e46c4254f40db998df64b4573122faca4754317abe0f43", "d5e824a221b7b360af504c943b68dc0644069c6974322427db196e195d752572"}
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

func TestUniqueSubjectVersionMigrationFailsWithRepairGuidance(t *testing.T) {
	t.Parallel()

	contents, err := migrationFS.ReadFile("migrations/002_unique_subject_version.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	migration := string(contents)
	for _, required := range []string{
		"HAVING count(*) > 1",
		"ERRCODE = '23505'",
		"cannot enforce unique subject/version",
		"without changing externally published schema versions",
		"CREATE UNIQUE INDEX IF NOT EXISTS",
	} {
		if !strings.Contains(migration, required) {
			t.Fatalf("migration lacks %q", required)
		}
	}
}
