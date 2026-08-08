package snowpipe

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

const snowpipeAuditFixtureMarker = "wallaby:snowpipe-audit-fixture"

type snowpipeAuditFinding struct {
	Path   string
	Line   int
	Reason string
}

func (f snowpipeAuditFinding) String() string {
	return fmt.Sprintf("%s:%d: %s", f.Path, f.Line, f.Reason)
}

func TestRepositoryHasNoRemovedSnowpipeBehavior(t *testing.T) {
	findings, err := scanSnowpipeRepository(snowpipeRepositoryRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) == 0 {
		return
	}
	lines := make([]string, 0, len(findings))
	for _, finding := range findings {
		lines = append(lines, finding.String())
	}
	t.Fatalf("removed Snowpipe behavior found:\n%s", strings.Join(lines, "\n"))
}

func TestSnowpipeRepositoryScanFindsFilesOutsideKnownDirectories(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "previously", "unlisted", "deployment", "flow.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	removedOption := "compat" + "_" + "mode" // wallaby:snowpipe-audit-fixture
	removedService := "fake" + "snow"        // wallaby:snowpipe-audit-fixture
	contents := "destination:\n  type: snowpipe\n  options:\n    " + removedOption + ": " + removedService + "\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	consumerPath := filepath.Join(root, "internal", "previously-unlisted", "options.go")
	if err := os.MkdirAll(filepath.Dir(consumerPath), 0o755); err != nil {
		t.Fatal(err)
	}
	consumer := "package hidden\nfunc read(options map[string]string) string { return options[\"" + removedOption + "\"] }\n"
	if err := os.WriteFile(consumerPath, []byte(consumer), 0o600); err != nil {
		t.Fatal(err)
	}
	claimPath := filepath.Join(root, "reference", "new", "connector-notes.md")
	if err := os.MkdirAll(filepath.Dir(claimPath), 0o755); err != nil {
		t.Fatal(err)
	}
	removedRuntime := "emu" + "lator" // wallaby:snowpipe-audit-fixture
	claim := "# Connectors\n\n## Snowpipe\n\n### Failure behavior\n\nAn " + removedRuntime + " may replace staged writes.\n"
	if err := os.WriteFile(claimPath, []byte(claim), 0o600); err != nil {
		t.Fatal(err)
	}

	findings, err := scanSnowpipeRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) == 0 {
		t.Fatal("recursive repository scan missed a forbidden file in an unlisted directory")
	}
	wantPaths := map[string]bool{
		"previously/unlisted/deployment/flow.yaml": false,
		"internal/previously-unlisted/options.go":  false,
		"reference/new/connector-notes.md":         false,
	}
	for _, finding := range findings {
		if _, ok := wantPaths[finding.Path]; ok {
			wantPaths[finding.Path] = true
		}
	}
	for path, found := range wantPaths {
		if !found {
			t.Errorf("recursive repository scan missed %s", path)
		}
	}
}

func TestSnowpipeRepositoryScanKeepsSeparateSnowflakeFixtures(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "docs", "connectors", "snowflake.md")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	removedService := "fake" + "snow" // wallaby:snowpipe-audit-fixture
	contents := "# Snowflake\n\n" + removedService + " is allowed only for Snowflake unit coverage.\n\n# Snowpipe\n\nSnowpipe uses staged service delivery.\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "examples", "mixed.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatal(err)
	}
	config := "destinations:\n  - type: snowflake\n    options:\n      test_service: " + removedService + "\n  - type: snowpipe\n    options:\n      stage: '@real_stage'\n"
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(root, "connectors", "destinations", "snowpipe", "removed_behavior_test.go")
	if err := os.MkdirAll(filepath.Dir(fixturePath), 0o755); err != nil {
		t.Fatal(err)
	}
	fixture := "package snowpipe\nconst explicitFixture = \"" + removedService + "\" // " + snowpipeAuditFixtureMarker + "\n"
	if err := os.WriteFile(fixturePath, []byte(fixture), 0o600); err != nil {
		t.Fatal(err)
	}

	findings, err := scanSnowpipeRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("separate Snowflake fixture was treated as Snowpipe context: %v", findings)
	}
}

func TestProductionSnowpipeInsertIsMetadataOnly(t *testing.T) {
	root := snowpipeRepositoryRoot(t)
	productionDir := filepath.Join(root, "connectors", "destinations", "snowpipe")
	var production strings.Builder
	err := filepath.WalkDir(productionDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		production.Write(contents)
		production.WriteByte('\n')
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	text := production.String()
	if count := strings.Count(text, "INSERT INTO"); count != 1 {
		t.Fatalf("production Snowpipe INSERT statements=%d; only metadata persistence is allowed", count)
	}
	if !strings.Contains(text, "insert meta row") {
		t.Fatal("the sole production Snowpipe INSERT is not identified as metadata persistence")
	}
}

func scanSnowpipeRepository(root string) ([]snowpipeAuditFinding, error) {
	findings := map[string]snowpipeAuditFinding{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && excludedSnowpipeAuditDirectory(entry.Name(), path) {
				return filepath.SkipDir
			}
			return nil
		}
		if !snowpipeAuditExtension(filepath.Ext(path)) {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		for _, finding := range scanSnowpipeFile(relative, string(contents)) {
			key := fmt.Sprintf("%s:%d:%s", finding.Path, finding.Line, finding.Reason)
			findings[key] = finding
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]snowpipeAuditFinding, 0, len(findings))
	for _, finding := range findings {
		out = append(out, finding)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		if out[i].Line != out[j].Line {
			return out[i].Line < out[j].Line
		}
		return out[i].Reason < out[j].Reason
	})
	return out, nil
}

func scanSnowpipeFile(path, contents string) []snowpipeAuditFinding {
	lines := strings.Split(contents, "\n")
	lowerPath := strings.ToLower(filepath.ToSlash(path))
	extension := strings.ToLower(filepath.Ext(path))
	pathHasSnowpipe := strings.Contains(lowerPath, "snowpipe")
	productionOptionConsumer := isProductionOptionConsumer(path, contents)
	markdownSnowpipeSectionLevel := 0
	findings := make([]snowpipeAuditFinding, 0)

	for index, line := range lines {
		lowerLine := strings.ToLower(line)
		if extension == ".md" {
			if level := markdownHeadingLevel(lowerLine); level > 0 {
				switch {
				case strings.Contains(lowerLine, "snowpipe"):
					markdownSnowpipeSectionLevel = level
				case markdownSnowpipeSectionLevel > 0 && level <= markdownSnowpipeSectionLevel:
					markdownSnowpipeSectionLevel = 0
				}
			}
		}
		if strings.HasSuffix(lowerPath, "_test.go") && strings.Contains(line, snowpipeAuditFixtureMarker) {
			continue
		}
		reason := removedSnowpipeBehavior(lowerLine)
		if reason == "" {
			continue
		}

		contextual := pathHasSnowpipe
		switch extension {
		case ".md":
			contextual = contextual || markdownSnowpipeSectionLevel > 0 || snowpipeSameSentence(lowerLine)
		case ".json", ".yaml", ".yml", ".tf":
			contextual = contextual || snowpipeStructuredContext(lines, index)
		case ".proto", ".sh", ".go":
			contextual = contextual || snowpipeNearLine(lines, index, 8)
		}
		if contextual {
			findings = append(findings, snowpipeAuditFinding{Path: path, Line: index + 1, Reason: reason})
		}
		if productionOptionConsumer && productionOptionLineRemoved(lowerLine) {
			findings = append(findings, snowpipeAuditFinding{Path: path, Line: index + 1, Reason: "production option consumer contains removed Snowpipe option"})
		}
	}
	return findings
}

func removedSnowpipeBehavior(lowerLine string) string {
	removedOption := "compat" + "_" + "mode" // wallaby:snowpipe-audit-fixture
	removedService := "fake" + "snow"        // wallaby:snowpipe-audit-fixture
	removedRuntime := "emu" + "lator"        // wallaby:snowpipe-audit-fixture
	directRow := "direct" + " row"           // wallaby:snowpipe-audit-fixture
	directRowHyphen := "direct" + "-row"     // wallaby:snowpipe-audit-fixture
	directInsert := "direct" + " insert"     // wallaby:snowpipe-audit-fixture
	compact := strings.NewReplacer(" ", "", "-", "", "_", "", "\"", "", "'", "", "`", "", "+", "").Replace(lowerLine)

	switch {
	case strings.Contains(lowerLine, removedOption), strings.Contains(compact, "compatmode"): // wallaby:snowpipe-audit-fixture
		return "removed Snowpipe compatibility option"
	case strings.Contains(lowerLine, removedService), strings.Contains(compact, "fakesnow"): // wallaby:snowpipe-audit-fixture
		return "removed Snowpipe fake-service selection"
	case strings.Contains(lowerLine, removedRuntime):
		return "removed Snowpipe emulation claim"
	case strings.Contains(lowerLine, directRow), strings.Contains(lowerLine, directRowHyphen), strings.Contains(lowerLine, directInsert):
		return "removed Snowpipe row-mutation claim"
	case strings.Contains(compact, "fallbackifcompat"), strings.Contains(compact, "writecompat"), strings.Contains(compact, "compatnotx"): // wallaby:snowpipe-audit-fixture
		return "removed Snowpipe error branch"
	case strings.Contains(lowerLine, "fallback") && containsAny(lowerLine, "insert", "row", "delivery", "semantic", "option", "compat"): // wallaby:snowpipe-audit-fixture
		return "removed Snowpipe fallback claim"
	default:
		return ""
	}
}

func productionOptionLineRemoved(lowerLine string) bool {
	removedOption := "compat" + "_" + "mode" // wallaby:snowpipe-audit-fixture
	compactLine := strings.NewReplacer(" ", "", "-", "", "_", "", "\"", "", "'", "", "`", "", "+", "").Replace(lowerLine)
	return strings.Contains(lowerLine, removedOption) || strings.Contains(compactLine, "compatmode") // wallaby:snowpipe-audit-fixture
}

func isProductionOptionConsumer(path, contents string) bool {
	lowerPath := strings.ToLower(filepath.ToSlash(path))
	if filepath.Ext(lowerPath) != ".go" || strings.HasSuffix(lowerPath, "_test.go") || strings.HasPrefix(lowerPath, "tests/") {
		return false
	}
	lower := strings.ToLower(contents)
	return containsAny(lower, "options[", ".options", "lookupenv(", "getenv(", "flag.", "json:\"", "yaml:\"")
}

func snowpipeStructuredContext(lines []string, index int) bool {
	for candidate := index; candidate >= 0; candidate-- {
		if endpointType, ok := structuredEndpointType(lines[candidate]); ok {
			return endpointType == "snowpipe"
		}
	}
	for candidate := index + 1; candidate < len(lines); candidate++ {
		if endpointType, ok := structuredEndpointType(lines[candidate]); ok {
			return endpointType == "snowpipe"
		}
	}
	return false
}

func structuredEndpointType(line string) (string, bool) {
	lower := strings.ToLower(line)
	if !containsAny(lower, "type:", "type =", "\"type\"") {
		return "", false
	}
	for _, endpointType := range []string{"snowpipe", "snowflake", "postgres", "clickhouse", "kafka", "redpanda", "http", "grpc", "s3", "iceberg", "duckdb", "ducklake", "pgstream"} {
		if strings.Contains(lower, endpointType) {
			return endpointType, true
		}
	}
	return "", false
}

func snowpipeNearLine(lines []string, index, radius int) bool {
	start := index - radius
	if start < 0 {
		start = 0
	}
	end := index + radius + 1
	if end > len(lines) {
		end = len(lines)
	}
	for _, line := range lines[start:end] {
		if strings.Contains(strings.ToLower(line), "snowpipe") {
			return true
		}
	}
	return false
}

func markdownHeadingLevel(line string) int {
	trimmed := strings.TrimSpace(line)
	level := 0
	for level < len(trimmed) && trimmed[level] == '#' {
		level++
	}
	if level == 0 || level == len(trimmed) || trimmed[level] != ' ' {
		return 0
	}
	return level
}

func snowpipeSameSentence(lowerLine string) bool {
	for _, sentence := range strings.FieldsFunc(lowerLine, func(r rune) bool { return r == '.' || r == '!' || r == '?' }) {
		if strings.Contains(sentence, "snowpipe") && removedSnowpipeBehavior(sentence) != "" {
			return true
		}
	}
	return false
}

func excludedSnowpipeAuditDirectory(name, path string) bool {
	lowerName := strings.ToLower(name)
	switch lowerName {
	case ".git", "build", "dist", "vendor", "node_modules", "site", ".cache", "artifact", "artifacts", ".pi-subagents":
		return true
	}
	lowerPath := filepath.ToSlash(strings.ToLower(path))
	return strings.HasSuffix(lowerPath, "/bench/evidence") || strings.Contains(lowerPath, "/bench/evidence/")
}

func snowpipeAuditExtension(extension string) bool {
	switch strings.ToLower(extension) {
	case ".go", ".proto", ".sh", ".md", ".json", ".yaml", ".yml", ".tf":
		return true
	default:
		return false
	}
}

func containsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}

func snowpipeRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve source audit path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
