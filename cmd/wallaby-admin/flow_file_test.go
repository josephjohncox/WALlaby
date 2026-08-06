package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/spf13/afero"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

var removedLogicalEndpointOptions = map[string]struct{}{"schema": {}, "table": {}, "database": {}, "write_mode": {}, "append_mode": {}, "soft_delete": {}, "meta_enabled": {}, "meta_synced_at": {}, "meta_deleted": {}, "meta_watermark": {}, "meta_op": {}, "watermark_source": {}, "namespace": {}, "table_prefix": {}, "fixed_table": {}, "target_namespace": {}, "target_table": {}}

func TestShippedFlowExamplesStrictLoadValidateAndUseCurrentMappings(t *testing.T) {
	t.Parallel()
	root := "../../examples"
	flowsRoot := filepath.Join(root, "flows")
	expected := []string{
		"flows/postgres_to_clickhouse.json",
		"flows/postgres_to_duckdb.json",
		"flows/postgres_to_ducklake.json",
		"flows/postgres_to_grpc.json",
		"flows/postgres_to_grpc_typed.json",
		"flows/postgres_to_http.json",
		"flows/postgres_to_http_toast_full.json",
		"flows/postgres_to_http_typed.yaml",
		"flows/postgres_to_iceberg_s3tables.json",
		"flows/postgres_to_kafka.json",
		"flows/postgres_to_kafka_http_primary.json",
		"flows/postgres_to_pgstream.json",
		"flows/postgres_to_redpanda.json",
		"flows/postgres_to_s3_parquet.json",
		"flows/postgres_to_snowflake.json",
		"flows/postgres_to_snowpipe.json",
		"quickstart/postgres-to-postgres.json",
	}
	var found []string
	err := filepath.WalkDir(flowsRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		extension := strings.ToLower(filepath.Ext(path))
		if extension != ".json" && extension != ".yaml" && extension != ".yml" {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		found = append(found, relative)
		assertShippedFlowExample(t, path, relative)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) == 0 {
		t.Fatal("examples/flows manifest is vacuous")
	}
	quickstart := "quickstart/postgres-to-postgres.json"
	found = append(found, quickstart)
	assertShippedFlowExample(t, filepath.Join(root, filepath.FromSlash(quickstart)), quickstart)
	sort.Strings(found)
	if !reflect.DeepEqual(found, expected) {
		t.Fatalf("flow example manifest mismatch\nfound: %v\nwant:  %v", found, expected)
	}
}

func assertShippedFlowExample(t *testing.T, path, relative string) {
	t.Helper()
	if relative == "flows/postgres_to_http_typed.yaml" {
		payload, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("read unexpanded %s: %v", path, err)
			return
		}
		var declared flowConfig
		if err := decodeStrictDocument(payload, path, &declared); err != nil {
			t.Errorf("strict-decode unexpanded %s: %v", path, err)
			return
		}
		if declared.Config.TableMappingsFile != "../mappings/http_typed.yaml" || declared.Config.TableMappings != nil {
			t.Errorf("%s mapping import declaration=%q inline=%v", path, declared.Config.TableMappingsFile, declared.Config.TableMappings != nil)
		}
	}
	cfg, err := loadFlowConfigFile(path)
	if err != nil {
		t.Errorf("strict-load %s: %v", path, err)
		return
	}
	if cfg.Config.TableMappings == nil {
		t.Errorf("%s omits config.table_mappings", path)
		return
	}
	if cfg.Config.TableMappingsFile != "" {
		t.Errorf("%s retains table_mappings_file", path)
	}
	if cfg.Config.TableMappings.Version != flow.TableMappingsVersion {
		t.Errorf("%s mapping version=%d, want %d", path, cfg.Config.TableMappings.Version, flow.TableMappingsVersion)
	}
	var destinationNames []string
	for _, destination := range cfg.Destinations {
		destinationNames = append(destinationNames, destination.Name)
		for option := range destination.Options {
			if _, obsolete := removedLogicalEndpointOptions[option]; obsolete {
				t.Errorf("%s destination %s uses removed option %q", path, destination.Name, option)
			}
		}
	}
	var mappingDestinations []string
	for _, mapping := range cfg.Config.TableMappings.Destinations {
		mappingDestinations = append(mappingDestinations, mapping.Destination)
	}
	sort.Strings(destinationNames)
	sort.Strings(mappingDestinations)
	if !reflect.DeepEqual(mappingDestinations, destinationNames) {
		t.Errorf("%s mapping destinations=%v, want %v", path, mappingDestinations, destinationNames)
	}
	pb, err := flowConfigToProto(cfg)
	if err != nil {
		t.Errorf("flow-validate %s: %v", path, err)
		return
	}
	if pb.Config == nil || pb.Config.TableMappings == nil {
		t.Errorf("%s protobuf omits expanded mappings", path)
	}
	if pb.Config.GetAckPolicy() == wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED && pb.Config.GetMaterialization().GetProjectionId() != "canonical_cdc_parquet_v2" {
		t.Errorf("%s materialized projection=%q", path, pb.Config.GetMaterialization().GetProjectionId())
	}
}

func TestShippedGRPCCreateFlowExampleStrictlyDecodesAndValidates(t *testing.T) {
	t.Parallel()
	payload := shippedGRPCCreateFlowPayload(t)
	if _, err := decodeAndValidateGRPCCreateFlowPayload(payload); err != nil {
		t.Fatalf("validate shipped gRPC create-flow payload: %v", err)
	}
}

func TestShippedGRPCCreateFlowExampleMutationsAreRejected(t *testing.T) {
	t.Parallel()
	payload := shippedGRPCCreateFlowPayload(t)
	mutate := func(t *testing.T, old, replacement string) []byte {
		t.Helper()
		if count := bytes.Count(payload, []byte(old)); count != 1 {
			t.Fatalf("mutation anchor %q count=%d, want 1", old, count)
		}
		return bytes.Replace(payload, []byte(old), []byte(replacement), 1)
	}
	for _, test := range []struct {
		name    string
		payload []byte
		want    string
	}{
		{
			name:    "unknown protobuf field",
			payload: mutate(t, `"start_immediately": true`, `"start_immediately": true, "unknown_request_field": true`),
			want:    "unknown_request_field",
		},
		{
			name:    "version 1",
			payload: mutate(t, `"version":2`, `"version":1`),
			want:    "version 1",
		},
		{
			name:    "wrong mapping destination",
			payload: mutate(t, `"destination":"kafka-out"`, `"destination":"other"`),
			want:    "unknown destination",
		},
		{
			name:    "legacy template syntax",
			payload: mutate(t, `"target_schema":"{{ .Schema }}"`, `"target_schema":"{schema}"`),
			want:    "action delimiter",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeAndValidateGRPCCreateFlowPayload(test.payload); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("decodeAndValidateGRPCCreateFlowPayload() error = %v, want %q", err, test.want)
			}
		})
	}
}

func shippedGRPCCreateFlowPayload(t *testing.T) []byte {
	t.Helper()
	script, err := os.ReadFile("../../examples/grpc/create_flow.sh")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := extractSingleQuotedHeredoc(script, "JSON")
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func extractSingleQuotedHeredoc(script []byte, delimiter string) ([]byte, error) {
	if delimiter == "" || strings.ContainsAny(delimiter, "\r\n'") {
		return nil, fmt.Errorf("invalid heredoc delimiter %q", delimiter)
	}
	marker := []byte("<<'" + delimiter + "'")
	if count := bytes.Count(script, marker); count != 1 {
		return nil, fmt.Errorf("expected exactly one %s heredoc marker, found %d", delimiter, count)
	}
	markerStart := bytes.Index(script, marker)
	markerEnd := markerStart + len(marker)
	lineEndOffset := bytes.IndexByte(script[markerEnd:], '\n')
	if lineEndOffset < 0 {
		return nil, fmt.Errorf("%s heredoc marker has no body", delimiter)
	}
	lineEnd := markerEnd + lineEndOffset
	if trailing := strings.TrimSpace(strings.TrimSuffix(string(script[markerEnd:lineEnd]), "\r")); trailing != "" {
		return nil, fmt.Errorf("unexpected bytes after %s heredoc marker", delimiter)
	}
	bodyStart := lineEnd + 1
	for lineStart := bodyStart; lineStart <= len(script); {
		lineEndOffset := bytes.IndexByte(script[lineStart:], '\n')
		lineEnd := len(script)
		next := len(script) + 1
		if lineEndOffset >= 0 {
			lineEnd = lineStart + lineEndOffset
			next = lineEnd + 1
		}
		line := bytes.TrimSuffix(script[lineStart:lineEnd], []byte{'\r'})
		if bytes.Equal(line, []byte(delimiter)) {
			payload := script[bodyStart:lineStart]
			if len(bytes.TrimSpace(payload)) == 0 {
				return nil, fmt.Errorf("%s heredoc payload is empty", delimiter)
			}
			return payload, nil
		}
		if next > len(script) {
			break
		}
		lineStart = next
	}
	return nil, fmt.Errorf("%s heredoc terminator not found", delimiter)
}

func TestExtractSingleQuotedHeredoc(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		script string
		want   string
		fail   bool
	}{
		{name: "LF", script: "command <<'JSON'\n{\n  \"value\": \"{{ braces }}\"\n}\nJSON\nafter\n", want: "{\n  \"value\": \"{{ braces }}\"\n}\n"},
		{name: "CRLF", script: "command <<'JSON'\r\n{}\r\nJSON\r\n", want: "{}\r\n"},
		{name: "missing marker", script: "command\n", fail: true},
		{name: "duplicate marker", script: "command <<'JSON'\n{}\nJSON\ncommand <<'JSON'\n{}\nJSON\n", fail: true},
		{name: "bytes after marker", script: "command <<'JSON' trailing\n{}\nJSON\n", fail: true},
		{name: "missing terminator", script: "command <<'JSON'\n{}\n", fail: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := extractSingleQuotedHeredoc([]byte(test.script), "JSON")
			if test.fail {
				if err == nil {
					t.Fatalf("extractSingleQuotedHeredoc() = %q, want error", got)
				}
				return
			}
			if err != nil || string(got) != test.want {
				t.Fatalf("extractSingleQuotedHeredoc() = %q, %v; want %q", got, err, test.want)
			}
		})
	}
}

func decodeAndValidateGRPCCreateFlowPayload(payload []byte) (flow.Flow, error) {
	var request wallabypb.CreateFlowRequest
	if err := protojson.Unmarshal(payload, &request); err != nil {
		return flow.Flow{}, fmt.Errorf("strictly decode CreateFlowRequest: %w", err)
	}
	if request.Flow == nil {
		return flow.Flow{}, fmt.Errorf("CreateFlowRequest.flow is required")
	}
	model, err := flowFromProto(request.Flow)
	if err != nil {
		return flow.Flow{}, fmt.Errorf("convert CreateFlowRequest.flow: %w", err)
	}
	if err := flow.ValidateDefinition(model); err != nil {
		return flow.Flow{}, fmt.Errorf("validate CreateFlowRequest.flow: %w", err)
	}
	mappings := model.Config.TableMappings
	if mappings.Version != flow.TableMappingsVersion {
		return flow.Flow{}, fmt.Errorf("mapping version=%d, want %d", mappings.Version, flow.TableMappingsVersion)
	}
	destinationNames := make([]string, 0, len(model.Destinations))
	for _, destination := range model.Destinations {
		destinationNames = append(destinationNames, destination.Name)
		for option := range destination.Options {
			if _, obsolete := removedLogicalEndpointOptions[option]; obsolete {
				return flow.Flow{}, fmt.Errorf("destination %s uses removed option %q", destination.Name, option)
			}
		}
	}
	mappingDestinations := make([]string, 0, len(mappings.Destinations))
	for _, mapping := range mappings.Destinations {
		mappingDestinations = append(mappingDestinations, mapping.Destination)
		future := mapping.FutureTables
		if future.Action != flow.MappingActionInclude || future.TargetSchema != "{{ .Schema }}" || future.TargetTable != "{{ .Table }}" ||
			future.FutureColumns.Action != flow.MappingActionInclude || future.FutureColumns.TargetColumn != "{{ .Column }}" {
			return flow.Flow{}, fmt.Errorf("destination %s does not use the shipped component templates", mapping.Destination)
		}
	}
	sort.Strings(destinationNames)
	sort.Strings(mappingDestinations)
	if !reflect.DeepEqual(mappingDestinations, destinationNames) {
		return flow.Flow{}, fmt.Errorf("mapping destinations=%v, want %v", mappingDestinations, destinationNames)
	}
	return model, nil
}

func TestTypedFlowExamplesRejectUnknownTopLevelKeys(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"postgres_to_http_typed.yaml", "postgres_to_grpc_typed.json"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("../../examples/flows", name)
			payload, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var document map[string]any
			if strings.HasSuffix(name, ".json") {
				err = json.Unmarshal(payload, &document)
			} else {
				err = yaml.Unmarshal(payload, &document)
			}
			if err != nil {
				t.Fatal(err)
			}
			document["unknown_flow_key"] = true
			var mutated []byte
			if strings.HasSuffix(name, ".json") {
				mutated, err = json.Marshal(document)
			} else {
				mutated, err = yaml.Marshal(document)
			}
			if err != nil {
				t.Fatal(err)
			}
			mutatedPath := filepath.Join(t.TempDir(), name)
			if err := os.WriteFile(mutatedPath, mutated, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadFlowConfigFile(mutatedPath); err == nil || !strings.Contains(err.Error(), "unknown_flow_key") {
				t.Fatalf("loadFlowConfigFile() error = %v, want unknown_flow_key", err)
			}
		})
	}
}

func boolp(v bool) *bool { return &v }
func completeTestMappings() flow.TableMappings {
	return flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{{Destination: "target", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionInclude, TargetSchema: "{{ .Schema }}", TargetTable: "{{ .Table }}", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{{ .Column }}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}}, Tables: []flow.TableMapping{{SourceSchema: "public", SourceTable: "events", Action: flow.MappingActionInclude, TargetSchema: "public", TargetTable: "events", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{{ .Column }}"}, Columns: []flow.ColumnMapping{{SourceColumn: "id", Action: flow.MappingActionInclude, TargetColumn: "id"}}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}}}}}}}
}
func completeFlowFile() flowConfig {
	m := completeTestMappings()
	return flowConfig{ID: "flow-1", Name: "flow", WireFormat: "arrow", Parallelism: 3, Source: endpointConfig{Name: "source", Type: "postgres", Options: map[string]string{"dsn": "secret-source"}}, Destinations: []endpointConfig{{Name: "target", Type: "postgres", Options: map[string]string{"dsn": "secret-target"}}}, Config: flowRuntimeConfig{AckPolicy: "primary", PrimaryDestination: "target", FailureMode: "hold_slot", GiveUpPolicy: "never", DDL: &flowDDLConfig{Gate: boolp(false), AutoApprove: boolp(true), AutoApply: boolp(false)}, SchemaRegistrySubject: "subject", SchemaRegistryProtoTypesSubject: "types", SchemaRegistrySubjectMode: "record", TableMappings: &m}}
}
func TestStrictFlowLoaderJSONYAMLEquivalenceAndRelativeMappingExpansion(t *testing.T) {
	old := adminFileSystem
	adminFileSystem = afero.NewMemMapFs()
	t.Cleanup(func() { adminFileSystem = old })
	m := completeTestMappings()
	mappingJSON, _ := encodeDeterministic(m, "json")
	if err := afero.WriteFile(adminFileSystem, "dir/mappings.json", mappingJSON, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := completeFlowFile()
	cfg.Config.TableMappings = nil
	cfg.Config.TableMappingsFile = "mappings.json"
	flowYAML, _ := encodeDeterministic(cfg, "yaml")
	if err := afero.WriteFile(adminFileSystem, "dir/flow.yaml", flowYAML, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadFlowConfigFile("dir/flow.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.TableMappingsFile != "" || loaded.Config.TableMappings == nil || !loaded.Config.TableMappings.Equal(m) {
		t.Fatalf("loaded=%+v", loaded.Config)
	}
	jsonBytes, _ := encodeDeterministic(completeFlowFile(), "json")
	yamlBytes, _ := encodeDeterministic(completeFlowFile(), "yaml")
	yamlAgain, _ := encodeDeterministic(completeFlowFile(), "yaml")
	if !bytes.Equal(yamlBytes, yamlAgain) || !bytes.HasSuffix(yamlBytes, []byte("\n")) {
		t.Fatalf("YAML is not deterministic/newline terminated")
	}
	var jsonCfg, yamlCfg flowConfig
	if err := decodeStrictDocument(jsonBytes, "flow.json", &jsonCfg); err != nil {
		t.Fatal(err)
	}
	if err := decodeStrictDocument(yamlBytes, "flow.yaml", &yamlCfg); err != nil {
		t.Fatal(err)
	}
	jsonPB, _ := flowConfigToProto(jsonCfg)
	yamlPB, _ := flowConfigToProto(yamlCfg)
	if !proto.Equal(jsonPB, yamlPB) {
		t.Fatalf("json=%v yaml=%v", jsonPB, yamlPB)
	}
}
func TestRelativeMappingImportUsesLexicalSymlinkDirectory(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	linkDir := filepath.Join(root, "link")
	if err := os.MkdirAll(realDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(linkDir, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := completeFlowFile()
	cfg.Config.TableMappings = nil
	cfg.Config.TableMappingsFile = "mappings.json"
	flowBytes, _ := encodeDeterministic(cfg, "json")
	realFlow := filepath.Join(realDir, "flow.json")
	if err := os.WriteFile(realFlow, flowBytes, 0600); err != nil {
		t.Fatal(err)
	}
	wanted := completeTestMappings()
	wantedBytes, _ := encodeDeterministic(wanted, "json")
	if err := os.WriteFile(filepath.Join(linkDir, "mappings.json"), wantedBytes, 0600); err != nil {
		t.Fatal(err)
	}
	wrong := wanted.Clone()
	wrong.Destinations[0].Destination = "wrong"
	wrongBytes, _ := encodeDeterministic(wrong, "json")
	if err := os.WriteFile(filepath.Join(realDir, "mappings.json"), wrongBytes, 0600); err != nil {
		t.Fatal(err)
	}
	linkFlow := filepath.Join(linkDir, "flow.json")
	if err := os.Symlink(realFlow, linkFlow); err != nil {
		t.Fatal(err)
	}
	old := adminFileSystem
	adminFileSystem = afero.NewOsFs()
	t.Cleanup(func() { adminFileSystem = old })
	loaded, err := loadFlowConfigFile(linkFlow)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.TableMappingsFile != "" || loaded.Config.TableMappings == nil || !loaded.Config.TableMappings.Equal(wanted) {
		t.Fatalf("lexical import resolved incorrectly: %+v", loaded.Config)
	}
	pb, err := flowConfigToProto(loaded)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(pb)
	if bytes.Contains(encoded, []byte("mappings.json")) || bytes.Contains(encoded, []byte(linkDir)) {
		t.Fatalf("local import path leaked: %s", encoded)
	}
}

func TestStrictFlowLoaderRejectsUnknownMultipleAndMappingPathConflicts(t *testing.T) {
	var cfg flowConfig
	for name, payload := range map[string]string{"json unknown": `{"unknown":1}`, "json multiple": `{} {}`, "yaml unknown": "unknown: 1\n", "yaml multiple": "{}\n---\n{}\n"} {
		ext := ".json"
		if strings.HasPrefix(name, "yaml") {
			ext = ".yaml"
		}
		if err := decodeStrictDocument([]byte(payload), "flow"+ext, &cfg); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	old := adminFileSystem
	adminFileSystem = afero.NewMemMapFs()
	t.Cleanup(func() { adminFileSystem = old })
	conflict := completeFlowFile()
	conflict.Config.TableMappingsFile = "mappings.json"
	raw, _ := encodeDeterministic(conflict, "json")
	_ = afero.WriteFile(adminFileSystem, "flow.json", raw, 0600)
	if _, err := loadFlowConfigFile("flow.json"); err == nil {
		t.Fatal("inline/path conflict accepted")
	}
	_ = afero.WriteFile(adminFileSystem, "mapping.yaml", []byte("version: 2\ndestinations: []\ntable_mappings_file: nested.yaml\n"), 0600)
	outer := completeFlowFile()
	outer.Config.TableMappings = nil
	outer.Config.TableMappingsFile = "mapping.yaml"
	raw, _ = encodeDeterministic(outer, "json")
	_ = afero.WriteFile(adminFileSystem, "nested.json", raw, 0600)
	if _, err := loadFlowConfigFile("nested.json"); err == nil {
		t.Fatal("nested mapping path accepted")
	}
}
func TestFlowConfigProtoDetailRoundTripEveryField(t *testing.T) {
	cfg := completeFlowFile()
	pb, err := flowConfigToProto(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if pb.Config == nil || pb.Config.Ddl == nil || pb.Config.Ddl.Gate == nil || *pb.Config.Ddl.Gate || pb.Config.TableMappings == nil {
		t.Fatalf("pb=%+v", pb.Config)
	}
	model, err := flowFromProto(pb)
	if err != nil {
		t.Fatal(err)
	}
	if model.Config.SchemaRegistrySubject != "subject" || model.Config.DDL.Gate == nil || *model.Config.DDL.Gate || !model.Config.TableMappings.Equal(*cfg.Config.TableMappings) {
		t.Fatalf("model=%+v", model.Config)
	}
	detail := flowDetailFromProto(pb)
	if detail.Config.DDL == nil || detail.Config.DDL.AutoApprove == nil || !*detail.Config.DDL.AutoApprove || detail.Config.TableMappings == nil || !detail.Config.TableMappings.Equal(*cfg.Config.TableMappings) {
		t.Fatalf("detail=%+v", detail.Config)
	}
	encoded, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("table_mappings_file")) {
		t.Fatalf("path leaked: %s", encoded)
	}
	empty := flowRuntimeConfigToProto(flowRuntimeConfig{})
	if empty != nil {
		t.Fatalf("zero config=%+v", empty)
	}
	if mappingsFromProto((*wallabypb.TableMappings)(nil)) != nil {
		t.Fatal("nil mapping semantics lost")
	}
}
func TestMappingsDestinationRequiredForMultipleFlows(t *testing.T) {
	destinations := []endpointConfig{{Name: "a"}, {Name: "b"}}
	if _, err := selectFlowConfigDestination(destinations, ""); err == nil {
		t.Fatal("missing destination accepted")
	}
	selected, err := selectFlowConfigDestination(destinations, "b")
	if err != nil || selected.Name != "b" {
		t.Fatalf("selected=%+v err=%v", selected, err)
	}
}

func TestGeneratedOutputExclusiveCreateIsAtomic(t *testing.T) {
	old := adminFileSystem
	adminFileSystem = afero.NewMemMapFs()
	t.Cleanup(func() { adminFileSystem = old })
	results := make(chan error, 2)
	start := make(chan struct{})
	for _, payload := range [][]byte{[]byte("one\n"), []byte("two\n")} {
		go func(payload []byte) { <-start; results <- writeGeneratedOutput("race.json", payload, false) }(payload)
	}
	close(start)
	success := 0
	for range 2 {
		if err := <-results; err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("exclusive writers succeeded=%d, want 1", success)
	}
}

func TestGeneratedOutputOverwriteRequiresForce(t *testing.T) {
	old := adminFileSystem
	adminFileSystem = afero.NewMemMapFs()
	t.Cleanup(func() { adminFileSystem = old })
	if err := writeGeneratedOutput("out.json", []byte("one\n"), false); err != nil {
		t.Fatal(err)
	}
	if err := writeGeneratedOutput("out.json", []byte("two\n"), false); err == nil {
		t.Fatal("overwrite accepted")
	}
	if err := writeGeneratedOutput("out.json", []byte("two\n"), true); err != nil {
		t.Fatal(err)
	}
	got, _ := afero.ReadFile(adminFileSystem, "out.json")
	if !reflect.DeepEqual(got, []byte("two\n")) {
		t.Fatalf("got=%q", got)
	}
}
