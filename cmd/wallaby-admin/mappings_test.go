package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/mappinggen"
	"github.com/spf13/afero"
)

func TestFullFlowEncodingPreservesEndpointOptionsWhileMappingsExcludeThem(t *testing.T) {
	cfg := completeFlowFile()
	cfg.Source.Options = map[string]string{"dsn": "postgres://user:source-secret@db/source", "host": "db"}
	cfg.Destinations[0].Options = map[string]string{"password": "destination-secret", "host": "sink"}
	fullPayload, err := encodeDeterministic(cfg, "json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(fullPayload, []byte("source-secret")) || !bytes.Contains(fullPayload, []byte("destination-secret")) {
		t.Fatalf("full-flow encoding silently redacted input: %s", fullPayload)
	}
	mappingPayload, err := encodeDeterministic(*cfg.Config.TableMappings, "json")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(mappingPayload, []byte("source-secret")) || bytes.Contains(mappingPayload, []byte("destination-secret")) || bytes.Contains(mappingPayload, []byte("dsn")) {
		t.Fatalf("mapping-only encoding leaked endpoint options: %s", mappingPayload)
	}
	var decoded flowConfig
	if err := decodeStrictDocument(fullPayload, "flow.json", &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.Source.Options, cfg.Source.Options) || !reflect.DeepEqual(decoded.Destinations[0].Options, cfg.Destinations[0].Options) {
		t.Fatalf("full-flow endpoint options were not lossless: %+v", decoded)
	}
}

func TestMappingsGenerateHelpExplainsCredentialBoundary(t *testing.T) {
	root := newAdminCommand()
	command, _, err := root.Find([]string{"flow", "mappings", "generate"})
	if err != nil {
		t.Fatal(err)
	}
	text := command.Long + " " + command.Flags().Lookup("output-mode").Usage
	if !strings.Contains(text, "Mappings output contains no endpoint credentials") || !strings.Contains(text, "lossless") || !strings.Contains(text, "secrets") || !strings.Contains(text, "protect") {
		t.Fatalf("credential boundary missing from help: %s", text)
	}
}

func TestCompleteFlowMappingsFillsEveryMissingDestinationAndPreservesValidExisting(t *testing.T) {
	cfg := completeFlowFile()
	cfg.Destinations = append(cfg.Destinations, endpointConfig{Name: "second", Type: "postgres", Options: map[string]string{"dsn": "second"}})
	catalog := []mappinggen.CatalogTable{{Schema: "public", Table: "events", PrimaryKeyColumns: []string{"id"}, Columns: []mappinggen.CatalogColumn{{Attnum: 1, Name: "id"}}}}
	complete, err := completeFlowMappings(cfg, catalog, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(complete.Destinations) != 2 || complete.Destinations[0].Destination != "target" || complete.Destinations[0].Tables[0].TargetTable != "events" || complete.Destinations[1].Destination != "second" || len(complete.Destinations[1].Tables) != 1 {
		t.Fatalf("complete mappings=%+v", complete)
	}
	cfg.Config.TableMappings = nil
	generated, err := completeFlowMappings(cfg, catalog, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(generated.Destinations) != 2 {
		t.Fatalf("generated mappings=%+v", generated)
	}
}

func TestMappingsGenerateLiveDeterministicNoCredentialLeakage(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgsource.OpenPool(ctx, dsn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS wallaby_mapping_cli CASCADE`)
	if _, err := pool.Exec(ctx, `CREATE SCHEMA wallaby_mapping_cli;CREATE TABLE wallaby_mapping_cli.composite(b text,a bigint,generated text GENERATED ALWAYS AS (b || a::text) STORED,PRIMARY KEY(b,a));CREATE TABLE wallaby_mapping_cli.no_key(payload text)`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS wallaby_mapping_cli CASCADE`) }()
	old := adminFileSystem
	adminFileSystem = afero.NewMemMapFs()
	t.Cleanup(func() { adminFileSystem = old })
	cfg := completeFlowFile()
	cfg.Config.TableMappings = nil
	cfg.Destinations = append(cfg.Destinations, endpointConfig{Name: "second", Type: "postgres", Options: map[string]string{"dsn": "second-secret"}})
	cfg.Source.Options = map[string]string{"dsn": dsn, "password": "must-not-leak"}
	raw, _ := encodeDeterministic(cfg, "json")
	_ = afero.WriteFile(adminFileSystem, "flow.json", raw, 0600)
	run := func(output string) error {
		cmd := newAdminCommand()
		cmd.SetArgs([]string{"flow", "mappings", "generate", "--file", "flow.json", "--destination", "target", "--schema", "wallaby_mapping_cli", "--table", `"wallaby_mapping_cli"."composite"`, "--format", "json", "--output", output, "--match-column", "wallaby_mapping_cli.composite=a,b", "--watermark", "wallaby_mapping_cli.composite=a"})
		return cmd.Execute()
	}
	if err := run("first.json"); err != nil {
		t.Fatal(err)
	}
	if err := run("second.json"); err != nil {
		t.Fatal(err)
	}
	first, _ := afero.ReadFile(adminFileSystem, "first.json")
	second, _ := afero.ReadFile(adminFileSystem, "second.json")
	yamlCmd := newAdminCommand()
	yamlCmd.SetArgs([]string{"flow", "mappings", "generate", "--file", "flow.json", "--destination", "target", "--schema", "wallaby_mapping_cli", "--format", "yaml", "--output", "third.yaml", "--match-column", "wallaby_mapping_cli.composite=a,b", "--watermark", "wallaby_mapping_cli.composite=a"})
	if err := yamlCmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("generation differs\n%s\n%s", first, second)
	}
	if !bytes.HasSuffix(first, []byte("\n")) || bytes.Contains(first, []byte(dsn)) || bytes.Contains(first, []byte("must-not-leak")) || bytes.Contains(first, []byte("second-secret")) || bytes.Contains(first, []byte("flow.json")) {
		t.Fatalf("mapping output leaked local input: %s", first)
	}
	text := string(first)
	if strings.Index(text, `"source_table": "composite"`) > strings.Index(text, `"source_table": "no_key"`) {
		t.Fatalf("table order: %s", text)
	}
	var decoded flow.TableMappings
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatal(err)
	}
	third, _ := afero.ReadFile(adminFileSystem, "third.yaml")
	if !bytes.HasSuffix(third, []byte("\n")) {
		t.Fatalf("YAML lacks trailing newline: %q", third)
	}
	var yamlDecoded flow.TableMappings
	if err := decodeStrictDocument(third, "third.yaml", &yamlDecoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Equal(yamlDecoded) {
		t.Fatalf("JSON/YAML differ: %+v %+v", decoded, yamlDecoded)
	}
	policy := decoded.Destinations[0].Tables[0].Write
	if !reflect.DeepEqual(policy.KeyColumns, []string{"a", "b"}) || policy.WatermarkColumn != "a" {
		t.Fatalf("override absent: %+v", policy)
	}
	if !strings.Contains(text, `"source_column": "generated"`) {
		t.Fatalf("generated column absent: %s", text)
	}
	fullCmd := newAdminCommand()
	fullCmd.SetArgs([]string{"flow", "mappings", "generate", "--file", "flow.json", "--destination", "target", "--schema", "wallaby_mapping_cli", "--output-mode", "flow", "--format", "json", "--output", "full.json"})
	if err := fullCmd.Execute(); err != nil {
		t.Fatal(err)
	}
	full, err := loadFlowConfigFile("full.json")
	if err != nil {
		t.Fatal(err)
	}
	if full.Config.TableMappingsFile != "" || full.Config.TableMappings == nil || len(full.Config.TableMappings.Destinations) != 2 || len(full.Config.TableMappings.Destinations[0].Tables) != 2 || len(full.Config.TableMappings.Destinations[1].Tables) != 2 || !reflect.DeepEqual(full.Config.TableMappings.Destinations[0].Tables[0].Write.KeyColumns, []string{"b", "a"}) {
		t.Fatalf("full output=%+v", full.Config)
	}
	if !reflect.DeepEqual(full.Source.Options, cfg.Source.Options) || !reflect.DeepEqual(full.Destinations[0].Options, cfg.Destinations[0].Options) || !reflect.DeepEqual(full.Destinations[1].Options, cfg.Destinations[1].Options) {
		t.Fatalf("full-flow expansion lost endpoint options: source=%+v destinations=%+v", full.Source.Options, full.Destinations)
	}
	fullPayload, _ := afero.ReadFile(adminFileSystem, "full.json")
	if !bytes.Contains(fullPayload, []byte("must-not-leak")) || !bytes.Contains(fullPayload, []byte("second-secret")) {
		t.Fatalf("full-flow output silently redacted input secrets: %s", fullPayload)
	}
	cmd := newAdminCommand()
	cmd.SetArgs([]string{"flow", "mappings", "generate", "--file", "flow.json", "--destination", "target", "--output", "none.json"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "explicit table, schema, or publication scope") {
		t.Fatalf("scope error=%v", err)
	}
}
