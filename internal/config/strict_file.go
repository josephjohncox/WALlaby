package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var durationType = reflect.TypeOf(time.Duration(0))

func decodeStrictConfigFile(path string, cfg *Config) (map[string]struct{}, error) {
	present := make(map[string]struct{})
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %s: %w", path, err)
	}
	extension := strings.ToLower(filepath.Ext(path))
	if extension != ".yaml" && extension != ".yml" && extension != ".json" {
		return nil, fmt.Errorf("config file %s: unsupported extension %q; use .yaml, .yml, or .json", path, extension)
	}
	if extension == ".json" {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		var document json.RawMessage
		if err := decoder.Decode(&document); err != nil {
			return nil, fmt.Errorf("config file %s: decode JSON: %w", path, err)
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			if err == nil {
				return nil, fmt.Errorf("config file %s: expected exactly one JSON document", path)
			}
			return nil, fmt.Errorf("config file %s: decode trailing JSON: %w", path, err)
		}
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("config file %s: decode: %w", path, err)
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("config file %s: expected exactly one YAML document", path)
		}
		return nil, fmt.Errorf("config file %s: decode trailing document: %w", path, err)
	}
	if len(document.Content) != 1 {
		return nil, fmt.Errorf("config file %s: expected one mapping document", path)
	}
	root := document.Content[0]
	if err := applyStrictNode(root, reflect.ValueOf(cfg).Elem(), "", path, present); err != nil {
		return nil, err
	}
	applyTelemetryFileInheritance(root, cfg)
	return present, nil
}

func applyStrictNode(node *yaml.Node, target reflect.Value, keyPath, filePath string, present map[string]struct{}) error {
	if target.Type() == durationType {
		if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
			return configTypeError(filePath, keyPath, "duration string", node)
		}
		value, err := time.ParseDuration(strings.TrimSpace(node.Value))
		if err != nil {
			return fmt.Errorf("config file %s: %s: invalid duration %q: %w", filePath, keyPath, node.Value, err)
		}
		target.SetInt(int64(value))
		present[keyPath] = struct{}{}
		return nil
	}
	switch target.Kind() {
	case reflect.Struct:
		if node.Kind != yaml.MappingNode {
			return configTypeError(filePath, keyPath, "object", node)
		}
		fields := configFields(target.Type())
		seen := map[string]struct{}{}
		for index := 0; index < len(node.Content); index += 2 {
			keyNode, valueNode := node.Content[index], node.Content[index+1]
			if keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" {
				return configTypeError(filePath, keyPath, "string key", keyNode)
			}
			name := keyNode.Value
			path := joinConfigPath(keyPath, name)
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("config file %s: duplicate key %q", filePath, path)
			}
			seen[name] = struct{}{}
			fieldIndex, exists := fields[name]
			if !exists {
				return fmt.Errorf("config file %s: unknown key %q", filePath, deepestConfigPath(path, valueNode))
			}
			if err := applyStrictNode(valueNode, target.Field(fieldIndex), path, filePath, present); err != nil {
				return err
			}
			if path == "dbos.max_retries" {
				target.FieldByName("MaxRetriesSet").SetBool(true)
			}
		}
		return nil
	case reflect.String:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
			return configTypeError(filePath, keyPath, "string", node)
		}
		target.SetString(node.Value)
		present[keyPath] = struct{}{}
		return nil
	case reflect.Bool:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!bool" {
			return configTypeError(filePath, keyPath, "bool", node)
		}
		value, err := strconv.ParseBool(node.Value)
		if err != nil {
			return fmt.Errorf("config file %s: %s: invalid bool %q", filePath, keyPath, node.Value)
		}
		target.SetBool(value)
		present[keyPath] = struct{}{}
		return nil
	case reflect.Int:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
			return configTypeError(filePath, keyPath, "integer", node)
		}
		value, err := strconv.ParseInt(node.Value, 10, target.Type().Bits())
		if err != nil {
			return fmt.Errorf("config file %s: %s: invalid integer %q: %w", filePath, keyPath, node.Value, err)
		}
		target.SetInt(value)
		present[keyPath] = struct{}{}
		return nil
	case reflect.Slice:
		if target.Type().Elem().Kind() != reflect.String || node.Kind != yaml.SequenceNode {
			return configTypeError(filePath, keyPath, "string list", node)
		}
		values := reflect.MakeSlice(target.Type(), len(node.Content), len(node.Content))
		for index, item := range node.Content {
			if item.Kind != yaml.ScalarNode || item.Tag != "!!str" {
				return configTypeError(filePath, fmt.Sprintf("%s[%d]", keyPath, index), "string", item)
			}
			values.Index(index).SetString(item.Value)
		}
		target.Set(values)
		present[keyPath] = struct{}{}
		return nil
	case reflect.Map:
		if target.Type().Key().Kind() != reflect.String || target.Type().Elem().Kind() != reflect.String || node.Kind != yaml.MappingNode {
			return configTypeError(filePath, keyPath, "string map", node)
		}
		values := reflect.MakeMapWithSize(target.Type(), len(node.Content)/2)
		seen := map[string]struct{}{}
		for index := 0; index < len(node.Content); index += 2 {
			keyNode, valueNode := node.Content[index], node.Content[index+1]
			entryIndex := index / 2
			if keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" {
				return configTypeError(filePath, fmt.Sprintf("%s[%d].key", keyPath, entryIndex), "string", keyNode)
			}
			entryPath := joinConfigPath(keyPath, keyNode.Value)
			if valueNode.Kind != yaml.ScalarNode || valueNode.Tag != "!!str" {
				return configTypeError(filePath, entryPath, "string", valueNode)
			}
			if _, duplicate := seen[keyNode.Value]; duplicate {
				return fmt.Errorf("config file %s: duplicate key %q", filePath, entryPath)
			}
			seen[keyNode.Value] = struct{}{}
			values.SetMapIndex(reflect.ValueOf(keyNode.Value), reflect.ValueOf(valueNode.Value))
		}
		target.Set(values)
		present[keyPath] = struct{}{}
		return nil
	default:
		return fmt.Errorf("config file %s: %s: unsupported configuration type %s", filePath, keyPath, target.Type())
	}
}

func applyTelemetryFileInheritance(root *yaml.Node, cfg *Config) {
	if configNodeHasPath(root, "telemetry.otlp_endpoint") && !configNodeHasPath(root, "telemetry.metrics_endpoint") && !environmentSet("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "WALLABY_OTEL_METRICS_ENDPOINT", "WALLABY_WORKER_OTEL_METRICS_ENDPOINT") {
		cfg.Telemetry.MetricsEndpoint = cfg.Telemetry.OTLPEndpoint
	}
	if configNodeHasPath(root, "telemetry.otlp_endpoint") && !configNodeHasPath(root, "telemetry.traces_endpoint") && !environmentSet("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "WALLABY_OTEL_TRACES_ENDPOINT", "WALLABY_WORKER_OTEL_TRACES_ENDPOINT") {
		cfg.Telemetry.TracesEndpoint = cfg.Telemetry.OTLPEndpoint
	}
	if configNodeHasPath(root, "telemetry.otlp_insecure") {
		if !configNodeHasPath(root, "telemetry.metrics_insecure") && !environmentSet("WALLABY_OTEL_METRICS_INSECURE", "WALLABY_WORKER_OTEL_METRICS_INSECURE") {
			cfg.Telemetry.MetricsInsecure = cfg.Telemetry.OTLPInsecure
		}
		if !configNodeHasPath(root, "telemetry.traces_insecure") && !environmentSet("WALLABY_OTEL_TRACES_INSECURE", "WALLABY_WORKER_OTEL_TRACES_INSECURE") {
			cfg.Telemetry.TracesInsecure = cfg.Telemetry.OTLPInsecure
		}
	}
	if configNodeHasPath(root, "telemetry.otlp_protocol") {
		if !configNodeHasPath(root, "telemetry.metrics_protocol") && !environmentSet("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "WALLABY_OTEL_METRICS_PROTOCOL", "WALLABY_WORKER_OTEL_METRICS_PROTOCOL") {
			cfg.Telemetry.MetricsProtocol = cfg.Telemetry.OTLPProtocol
		}
		if !configNodeHasPath(root, "telemetry.traces_protocol") && !environmentSet("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "WALLABY_OTEL_TRACES_PROTOCOL", "WALLABY_WORKER_OTEL_TRACES_PROTOCOL") {
			cfg.Telemetry.TracesProtocol = cfg.Telemetry.OTLPProtocol
		}
	}
}
func environmentSet(keys ...string) bool {
	for _, key := range keys {
		if _, ok := os.LookupEnv(key); ok {
			return true
		}
	}
	return false
}
func configNodeHasPath(root *yaml.Node, path string) bool {
	node := root
	for _, part := range strings.Split(path, ".") {
		if node == nil || node.Kind != yaml.MappingNode {
			return false
		}
		var next *yaml.Node
		for index := 0; index < len(node.Content); index += 2 {
			if node.Content[index].Value == part {
				next = node.Content[index+1]
				break
			}
		}
		if next == nil {
			return false
		}
		node = next
	}
	return true
}

func configFields(value reflect.Type) map[string]int {
	fields := make(map[string]int, value.NumField())
	for index := 0; index < value.NumField(); index++ {
		field := value.Field(index)
		name := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		if name != "-" {
			fields[name] = index
		}
	}
	return fields
}

func configTypeError(filePath, keyPath, expected string, node *yaml.Node) error {
	actual := "unknown"
	if node != nil {
		switch node.Kind {
		case yaml.MappingNode:
			actual = "object"
		case yaml.SequenceNode:
			actual = "list"
		case yaml.ScalarNode:
			actual = configScalarCategory(node.Tag)
		}
	}
	return fmt.Errorf("config file %s: %s: expected %s, got %s", filePath, keyPath, expected, actual)
}

func configScalarCategory(tag string) string {
	switch tag {
	case "!!str":
		return "string"
	case "!!int":
		return "integer"
	case "!!float":
		return "number"
	case "!!bool":
		return "bool"
	case "!!null":
		return "null"
	case "!!timestamp":
		return "timestamp"
	default:
		if trimmed := strings.TrimPrefix(tag, "!!"); trimmed != "" {
			return trimmed
		}
		return "scalar"
	}
}

func deepestConfigPath(path string, node *yaml.Node) string {
	for node != nil && node.Kind == yaml.MappingNode && len(node.Content) >= 2 {
		path = joinConfigPath(path, node.Content[0].Value)
		node = node.Content[1]
	}
	return path
}

func joinConfigPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}
