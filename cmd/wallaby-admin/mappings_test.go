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
	if command.Flags().Lookup("write-mode") == nil {
		t.Fatal("mapping generation omits --write-mode")
	}
}

func TestCompleteFlowMappingsFillsEveryMissingDestinationAndPreservesValidExisting(t *testing.T) {
	cfg := completeFlowFile()
	cfg.Destinations = append(cfg.Destinations, endpointConfig{Name: "second", Type: "postgres", Options: map[string]string{"dsn": "second"}})
	catalog := []mappinggen.CatalogTable{{Schema: "public", Table: "events", PrimaryKeyColumns: []string{"id"}, Columns: []mappinggen.CatalogColumn{{Attnum: 1, Name: "id"}}}}
	complete, err := completeFlowMappings(cfg, catalog, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(complete.Destinations) != 2 || complete.Destinations[0].Destination != "target" || complete.Destinations[0].Tables[0].TargetTable != "events" || complete.Destinations[1].Destination != "second" || len(complete.Destinations[1].Tables) != 1 {
		t.Fatalf("complete mappings=%+v", complete)
	}
	overridden, err := completeFlowMappings(cfg, catalog, nil, nil, map[mappinggen.TableRef]flow.TableWriteMode{{Schema: "public", Table: "events"}: flow.TableWriteModeAppend})
	if err != nil {
		t.Fatal(err)
	}
	if policy := overridden.Destinations[0].Tables[0].Write; policy.Mode != flow.TableWriteModeAppend || len(policy.KeyColumns) != 0 || overridden.Destinations[0].Tables[0].TargetTable != "events" {
		t.Fatalf("existing mapping override=%+v", overridden.Destinations[0])
	}
	cfg.Config.TableMappings = nil
	generated, err := completeFlowMappings(cfg, catalog, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(generated.Destinations) != 2 {
		t.Fatalf("generated mappings=%+v", generated)
	}
}

func TestCompleteFlowMappingsUsesExactDestinationCapabilitiesAndWriteModeOverrides(t *testing.T) {
	catalog := []mappinggen.CatalogTable{{Schema: "public", Table: "events", PrimaryKeyColumns: []string{"id"}, Columns: []mappinggen.CatalogColumn{{Attnum: 1, Name: "id"}, {Attnum: 2, Name: "updated_at"}}}}
	cfg := completeFlowFile()
	cfg.Config.TableMappings = nil
	cfg.Destinations = []endpointConfig{{Name: "postgres", Type: "postgres"}, {Name: "stream", Type: "pgstream"}, {Name: "snowflake-generic", Type: "snowflake"}, {Name: "snowflake-managed", Type: "snowflake", Options: map[string]string{"managed_profile": "postgresql-to-snowflake-sql-v1"}}}
	generated, err := completeFlowMappings(cfg, catalog, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	modes := map[string]flow.TableWritePolicy{}
	for _, destination := range generated.Destinations {
		modes[destination.Destination] = destination.Tables[0].Write
	}
	if modes["postgres"].Mode != flow.TableWriteModeUpsert || modes["stream"].Mode != flow.TableWriteModeAppend || modes["snowflake-generic"].Mode != flow.TableWriteModeAppend || modes["snowflake-managed"].Mode != flow.TableWriteModeUpsert {
		t.Fatalf("capability-aware policies=%+v", modes)
	}
	appendOnly := cfg
	appendOnly.Destinations = []endpointConfig{{Name: "stream", Type: "pgstream"}}
	withWatermark, err := completeFlowMappings(appendOnly, catalog, map[mappinggen.TableRef]string{{Schema: "public", Table: "events"}: "updated_at"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if policy := withWatermark.Destinations[0].Tables[0].Write; policy.Mode != flow.TableWriteModeAppend || policy.WatermarkColumn != "updated_at" {
		t.Fatalf("append watermark lost: %+v", policy)
	}
	override := map[mappinggen.TableRef]flow.TableWriteMode{{Schema: "public", Table: "events"}: flow.TableWriteModeAppend}
	overrideCfg := cfg
	overrideCfg.Destinations = append([]endpointConfig(nil), cfg.Destinations[:3]...)
	generated, err = completeFlowMappings(overrideCfg, catalog, nil, nil, override)
	if err != nil {
		t.Fatal(err)
	}
	for _, destination := range generated.Destinations {
		if policy := destination.Tables[0].Write; policy.Mode != flow.TableWriteModeAppend || len(policy.KeyColumns) != 0 {
			t.Fatalf("append override %s=%+v", destination.Destination, policy)
		}
	}
	cfg.Destinations = []endpointConfig{{Name: "stream", Type: "pgstream"}}
	override[mappinggen.TableRef{Schema: "public", Table: "events"}] = flow.TableWriteModeUpsert
	if _, err := completeFlowMappings(cfg, catalog, nil, nil, override); err == nil || !strings.Contains(err.Error(), "does not support explicit-key upsert") {
		t.Fatalf("append-only upsert override error=%v", err)
	}
	catalog[0].PrimaryKeyColumns = nil
	cfg.Destinations = []endpointConfig{{Name: "postgres", Type: "postgres"}}
	if _, err := completeFlowMappings(cfg, catalog, nil, nil, override); err == nil || !strings.Contains(err.Error(), "requires match columns or a source primary key") {
		t.Fatalf("keyless upsert override error=%v", err)
	}
}

func TestManagedSnowflakeSQLGenerationJSONYAMLAndFullFlow(t *testing.T) {
	catalog := []mappinggen.CatalogTable{{Schema: "sales", Table: "orders", PrimaryKeyColumns: []string{"tenant_id", "id"}, Columns: []mappinggen.CatalogColumn{{Attnum: 1, Name: "tenant_id"}, {Attnum: 2, Name: "id"}, {Attnum: 3, Name: "payload"}}}}
	cfg := completeFlowFile()
	cfg.Config.TableMappings = nil
	cfg.Destinations = []endpointConfig{{Name: "snowflake", Type: "snowflake", Options: map[string]string{"managed_profile": "postgresql-to-snowflake-sql-v1"}}}
	ref := mappinggen.TableRef{Schema: "sales", Table: "orders"}
	generated, err := completeFlowMappings(cfg, catalog, nil, map[mappinggen.TableRef][]string{ref: {"tenant_id", "id"}}, map[mappinggen.TableRef]flow.TableWriteMode{ref: flow.TableWriteModeUpsert})
	if err != nil {
		t.Fatal(err)
	}
	mapping := generated.Destinations[0]
	if mapping.FutureTables.Action != flow.MappingActionExclude || len(mapping.Tables) != 1 || mapping.Tables[0].Write.Mode != flow.TableWriteModeUpsert || !reflect.DeepEqual(mapping.Tables[0].Write.KeyColumns, []string{"tenant_id", "id"}) {
		t.Fatalf("managed mapping=%+v", mapping)
	}
	jsonPayload, err := encodeDeterministic(generated, "json")
	if err != nil {
		t.Fatal(err)
	}
	yamlPayload, err := encodeDeterministic(generated, "yaml")
	if err != nil {
		t.Fatal(err)
	}
	var fromJSON, fromYAML flow.TableMappings
	if err := decodeStrictDocument(jsonPayload, "mapping.json", &fromJSON); err != nil {
		t.Fatal(err)
	}
	if err := decodeStrictDocument(yamlPayload, "mapping.yaml", &fromYAML); err != nil {
		t.Fatal(err)
	}
	if !fromJSON.Equal(fromYAML) || !fromJSON.Equal(*generated) {
		t.Fatalf("json/yaml mapping mismatch json=%+v yaml=%+v", fromJSON, fromYAML)
	}
	cfg.Config.TableMappings = generated
	fullJSON, err := encodeDeterministic(cfg, "json")
	if err != nil {
		t.Fatal(err)
	}
	fullYAML, err := encodeDeterministic(cfg, "yaml")
	if err != nil {
		t.Fatal(err)
	}
	var jsonFlow, yamlFlow flowConfig
	if err := decodeStrictDocument(fullJSON, "flow.json", &jsonFlow); err != nil {
		t.Fatal(err)
	}
	if err := decodeStrictDocument(fullYAML, "flow.yaml", &yamlFlow); err != nil {
		t.Fatal(err)
	}
	if jsonFlow.Config.TableMappings == nil || yamlFlow.Config.TableMappings == nil || !jsonFlow.Config.TableMappings.Equal(*yamlFlow.Config.TableMappings) {
		t.Fatalf("full-flow mapping mismatch")
	}
}

func TestManagedSnowflakeSQLGenerationIsPerDestinationInFullFlow(t *testing.T) {
	catalog := []mappinggen.CatalogTable{{Schema: "public", Table: "events", PrimaryKeyColumns: []string{"id"}, Columns: []mappinggen.CatalogColumn{{Attnum: 1, Name: "id"}}}}
	cfg := completeFlowFile()
	cfg.Config.TableMappings = nil
	cfg.Destinations = []endpointConfig{{Name: "stream", Type: "pgstream"}, {Name: "generic-snowflake", Type: "snowflake"}, {Name: "managed-snowflake", Type: "snowflake", Options: map[string]string{"managed_profile": "postgresql-to-snowflake-sql-v1"}}}
	generated, err := completeFlowMappings(cfg, catalog, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]flow.DestinationTableMappings{}
	for _, mapping := range generated.Destinations {
		byName[mapping.Destination] = mapping
	}
	for _, name := range []string{"stream", "generic-snowflake"} {
		if byName[name].Tables[0].Write.Mode != flow.TableWriteModeAppend || byName[name].FutureTables.Action != flow.MappingActionInclude {
			t.Fatalf("%s mapping=%+v", name, byName[name])
		}
	}
	managed := byName["managed-snowflake"]
	if managed.Tables[0].Write.Mode != flow.TableWriteModeUpsert || managed.FutureTables.Action != flow.MappingActionExclude {
		t.Fatalf("managed mapping=%+v", managed)
	}
}

func TestManagedSnowflakeSQLGenerationRejectsContractViolations(t *testing.T) {
	base := mappinggen.CatalogTable{Schema: "public", Table: "events", PrimaryKeyColumns: []string{"tenant_id", "id"}, Columns: []mappinggen.CatalogColumn{{Attnum: 1, Name: "tenant_id"}, {Attnum: 2, Name: "id"}, {Attnum: 3, Name: "extra"}, {Attnum: 4, Name: "updated_at"}}}
	ref := mappinggen.TableRef{Schema: "public", Table: "events"}
	tests := []struct {
		name       string
		catalog    []mappinggen.CatalogTable
		watermarks map[mappinggen.TableRef]string
		matches    map[mappinggen.TableRef][]string
		modes      map[mappinggen.TableRef]flow.TableWriteMode
		want       string
	}{{"no relation", nil, nil, nil, nil, "exactly one selected relation"}, {"multiple relations", []mappinggen.CatalogTable{base, {Schema: "public", Table: "other", PrimaryKeyColumns: []string{"id"}, Columns: []mappinggen.CatalogColumn{{Attnum: 1, Name: "id"}}}}, nil, nil, nil, "exactly one selected relation"}, {"no primary key", []mappinggen.CatalogTable{{Schema: "public", Table: "events", Columns: base.Columns}}, nil, nil, nil, "complete source primary key"}, {"append override", []mappinggen.CatalogTable{base}, nil, nil, map[mappinggen.TableRef]flow.TableWriteMode{ref: flow.TableWriteModeAppend}, "rejects append"}, {"watermark", []mappinggen.CatalogTable{base}, map[mappinggen.TableRef]string{ref: "updated_at"}, nil, nil, "rejects watermark"}, {"partial match", []mappinggen.CatalogTable{base}, nil, map[mappinggen.TableRef][]string{ref: {"tenant_id"}}, nil, "complete ordered source primary key"}, {"reordered match", []mappinggen.CatalogTable{base}, nil, map[mappinggen.TableRef][]string{ref: {"id", "tenant_id"}}, nil, "complete ordered source primary key"}, {"extra match", []mappinggen.CatalogTable{base}, nil, map[mappinggen.TableRef][]string{ref: {"tenant_id", "id", "extra"}}, nil, "complete ordered source primary key"}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := completeFlowFile()
			cfg.Config.TableMappings = nil
			cfg.Destinations = []endpointConfig{{Name: "snowflake", Type: "snowflake", Options: map[string]string{"managed_profile": "postgresql-to-snowflake-sql-v1"}}}
			_, err := completeFlowMappings(cfg, test.catalog, test.watermarks, test.matches, test.modes)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want %q", err, test.want)
			}
		})
	}
}

func TestParseWriteModeOverridesRejectsInvalidAndDuplicateValues(t *testing.T) {
	if got, err := parseWriteModeOverrides([]string{"public.events=append"}); err != nil || got[mappinggen.TableRef{Schema: "public", Table: "events"}] != flow.TableWriteModeAppend {
		t.Fatalf("got=%v err=%v", got, err)
	}
	for _, values := range [][]string{{"public.events=merge"}, {"public.events"}, {"public.events=append", "public.events=upsert"}} {
		if _, err := parseWriteModeOverrides(values); err == nil {
			t.Fatalf("values=%v accepted", values)
		}
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
