package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSnowflakeStagedProvisionCommandsAreWired(t *testing.T) {
	command := newAdminCommand()
	for _, path := range [][]string{{"snowflake"}, {"snowflake", "staged"}, {"snowflake", "staged", "provision"}, {"snowflake", "staged", "provision", "inspect"}, {"snowflake", "staged", "provision", "start"}, {"snowflake", "staged", "provision", "resume"}, {"snowflake", "staged", "provision", "abort"}} {
		current := command
		for _, name := range path {
			child, _, err := current.Find([]string{name})
			if err != nil || child == nil || child.Name() != name {
				t.Fatalf("missing command path %v at %q: %v", path, name, err)
			}
			current = child
		}
	}
}

func TestLoadManagedStagedProvisionSpecRejectsUnknownAndTrailingJSON(t *testing.T) {
	directory := t.TempDir()
	for name, content := range map[string]string{"unknown": `{"unknown":true}`, "trailing": `{"endpoint":{"name":"x","type":"snowflake","options":{}}} {}`} {
		path := filepath.Join(directory, name+".json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadManagedStagedProvisionSpec(path); err == nil {
			t.Fatalf("accepted %s provision JSON", name)
		}
	}
}
