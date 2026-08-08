package tests

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestProductionSourcesCreateOnlyAuthoritativeMigrationLedger(t *testing.T) {
	createLedger := regexp.MustCompile(`(?i)^CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+([a-zA-Z0-9_."]+_migrations)\b`)
	var violations []string
	err := filepath.WalkDir("..", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != ".." && (entry.Name() == ".git" || entry.Name() == ".cache" || entry.Name() == ".pi-subagents" || entry.Name() == ".review-recovery" || entry.Name() == "vendor" || entry.Name() == "gen") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, "_test.go") || (!strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".sql")) {
			return nil
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		source := string(raw)
		lowerSource := strings.ToLower(source)
		for offset := 0; offset < len(source); {
			relative := strings.Index(lowerSource[offset:], "create")
			if relative < 0 {
				break
			}
			start := offset + relative
			end := start + 512
			if end > len(source) {
				end = len(source)
			}
			fragment := strings.Join(strings.Fields(source[start:end]), " ")
			if match := createLedger.FindStringSubmatch(fragment); len(match) > 0 {
				name := strings.ToLower(strings.ReplaceAll(match[1], `"`, ""))
				if name != "public.wallaby_control_migrations" {
					violations = append(violations, path+": creates non-authoritative migration ledger "+name)
				}
			}
			offset = start + len("create")
		}

		for offset := 0; offset < len(lowerSource); {
			relative := strings.Index(lowerSource[offset:], "migration")
			if relative < 0 {
				break
			}
			position := offset + relative
			lineStart := strings.LastIndex(lowerSource[:position], "\n") + 1
			lineEndRelative := strings.Index(lowerSource[position:], "\n")
			lineEnd := len(source)
			if lineEndRelative >= 0 {
				lineEnd = position + lineEndRelative
			}
			trimmed := strings.TrimSpace(source[lineStart:lineEnd])
			lowerLine := strings.ToLower(trimmed)
			if (strings.HasPrefix(trimmed, "const ") || strings.HasPrefix(trimmed, "var ")) && (strings.Contains(lowerLine, "table") || strings.Contains(lowerLine, "ledger")) {
				violations = append(violations, path+": declares a package-level migration table/ledger constant")
			}
			offset = position + len("migration")
		}

		for _, forbidden := range []string{"wallaby_stream_migrations", "wallaby_schema_registry_migrations"} {
			if strings.Contains(source, forbidden) {
				violations = append(violations, path+": retains old migration ledger token "+forbidden)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("production migration-ledger audit failed:\n%s", strings.Join(violations, "\n"))
	}
}
