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
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

func TestShippedFlowExamplesStrictLoadValidateAndUseCurrentMappings(t *testing.T) {
	t.Parallel()
	root := "../../examples"
	expected := []string{"flows/postgres_to_bufstream.json", "flows/postgres_to_clickhouse.json", "flows/postgres_to_duckdb.json", "flows/postgres_to_ducklake.json", "flows/postgres_to_grpc.json", "flows/postgres_to_http.json", "flows/postgres_to_http_toast_full.json", "flows/postgres_to_iceberg_s3tables.json", "flows/postgres_to_kafka.json", "flows/postgres_to_kafka_http_primary.json", "flows/postgres_to_pgstream.json", "flows/postgres_to_s3_parquet.json", "flows/postgres_to_snowflake.json", "flows/postgres_to_snowpipe.json", "quickstart/postgres-to-postgres.json"}
	removed := map[string]struct{}{"schema": {}, "table": {}, "database": {}, "write_mode": {}, "append_mode": {}, "soft_delete": {}, "meta_enabled": {}, "meta_synced_at": {}, "meta_deleted": {}, "meta_watermark": {}, "meta_op": {}, "watermark_source": {}, "namespace": {}, "table_prefix": {}, "fixed_table": {}, "target_namespace": {}, "target_table": {}}
	var found []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
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
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var shape map[string]any
		if extension == ".json" {
			err = json.Unmarshal(raw, &shape)
		} else {
			err = yaml.Unmarshal(raw, &shape)
		}
		if err != nil {
			return fmt.Errorf("decode example shape %s: %w", path, err)
		}
		if _, ok := shape["source"]; !ok {
			return nil
		}
		if _, ok := shape["destinations"]; !ok {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		found = append(found, filepath.ToSlash(relative))
		cfg, err := loadFlowConfigFile(path)
		if err != nil {
			t.Errorf("strict-load %s: %v", path, err)
			return nil
		}
		if cfg.Config.TableMappings == nil {
			t.Errorf("%s omits config.table_mappings", path)
			return nil
		}
		if cfg.Config.TableMappingsFile != "" {
			t.Errorf("%s retains table_mappings_file", path)
		}
		for _, destination := range cfg.Destinations {
			for option := range destination.Options {
				if _, obsolete := removed[option]; obsolete {
					t.Errorf("%s destination %s uses removed option %q", path, destination.Name, option)
				}
			}
		}
		pb, err := flowConfigToProto(cfg)
		if err != nil {
			t.Errorf("flow-validate %s: %v", path, err)
			return nil
		}
		if pb.Config == nil || pb.Config.TableMappings == nil {
			t.Errorf("%s protobuf omits expanded mappings", path)
		}
		if pb.Config.GetAckPolicy() == wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED && pb.Config.GetMaterialization().GetProjectionId() != "canonical_cdc_parquet_v2" {
			t.Errorf("%s materialized projection=%q", path, pb.Config.GetMaterialization().GetProjectionId())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(found)
	if !reflect.DeepEqual(found, expected) {
		t.Fatalf("recursive flow example manifest mismatch\nfound: %v\nwant:  %v", found, expected)
	}
	if len(found) < 15 {
		t.Fatalf("found only %d flow-shaped examples", len(found))
	}
	grpcExample, err := os.ReadFile("../../examples/grpc/create_flow.sh")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(grpcExample, []byte("\"table_mappings\"")) {
		t.Fatal("gRPC create-flow example omits mappings")
	}
	start, end := bytes.IndexByte(grpcExample, '{'), bytes.LastIndexByte(grpcExample, '}')
	if start < 0 || end <= start {
		t.Fatal("gRPC create-flow example omits JSON payload")
	}
	var request struct {
		Flow struct {
			Destinations []struct {
				Options map[string]string `json:"options"`
			} `json:"destinations"`
		} `json:"flow"`
	}
	if err := json.Unmarshal(grpcExample[start:end+1], &request); err != nil {
		t.Fatalf("decode gRPC example payload: %v", err)
	}
	for _, destination := range request.Flow.Destinations {
		for option := range destination.Options {
			if _, obsolete := removed[option]; obsolete {
				t.Errorf("gRPC create-flow destination uses removed option %q", option)
			}
		}
	}
}

func boolp(v bool) *bool { return &v }
func completeTestMappings() flow.TableMappings {
	return flow.TableMappings{Version: 1, Destinations: []flow.DestinationTableMappings{{Destination: "target", FutureTables: flow.FutureTableMapping{Action: flow.MappingActionInclude, TargetSchema: "{schema}", TargetTable: "{table}", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeAppend}}, Tables: []flow.TableMapping{{SourceSchema: "public", SourceTable: "events", Action: flow.MappingActionInclude, TargetSchema: "public", TargetTable: "events", FutureColumns: flow.FutureColumnMapping{Action: flow.MappingActionInclude, TargetColumn: "{column}"}, Columns: []flow.ColumnMapping{{SourceColumn: "id", Action: flow.MappingActionInclude, TargetColumn: "id"}}, Write: flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: []string{"id"}}}}}}}
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
	_ = afero.WriteFile(adminFileSystem, "mapping.yaml", []byte("version: 1\ndestinations: []\ntable_mappings_file: nested.yaml\n"), 0600)
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
