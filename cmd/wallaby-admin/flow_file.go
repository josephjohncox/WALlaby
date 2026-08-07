package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/josephjohncox/wallaby/internal/flow"
	"gopkg.in/yaml.v3"
)

func loadFlowConfigFile(path string) (flowConfig, error) {
	payload, err := readFile(path)
	if err != nil {
		return flowConfig{}, fmt.Errorf("read flow file: %w", err)
	}
	var cfg flowConfig
	if err := decodeStrictDocument(payload, path, &cfg); err != nil {
		return flowConfig{}, fmt.Errorf("parse flow file: %w", err)
	}
	if cfg.Config.TableMappingsFile != "" {
		if cfg.Config.TableMappings != nil {
			return flowConfig{}, errors.New("config.table_mappings and config.table_mappings_file cannot both be set")
		}
		mappingPath := resolveLocalImportPath(path, cfg.Config.TableMappingsFile)
		mappingPayload, err := readFile(mappingPath)
		if err != nil {
			return flowConfig{}, fmt.Errorf("read table mappings file: %w", err)
		}
		var mappings flow.TableMappings
		if err := decodeStrictDocument(mappingPayload, mappingPath, &mappings); err != nil {
			return flowConfig{}, fmt.Errorf("parse table mappings file: %w", err)
		}
		cfg.Config.TableMappings = &mappings
		cfg.Config.TableMappingsFile = ""
	}
	return cfg, nil
}

// resolveLocalImportPath uses the lexical directory containing the flow-file
// argument. It deliberately does not resolve symlinks, so invocation through a
// symlink imports beside that symlink rather than beside its target.
func resolveLocalImportPath(flowPath, importPath string) string {
	if filepath.IsAbs(importPath) {
		return filepath.Clean(importPath)
	}
	return filepath.Clean(filepath.Join(filepath.Dir(flowPath), importPath))
}

func decodeStrictDocument(payload []byte, path string, out any) error {
	trimmed := bytes.TrimSpace(payload)
	extension := strings.ToLower(filepath.Ext(path))
	jsonInput := extension == ".json" || (extension != ".yaml" && extension != ".yml" && len(trimmed) > 0 && trimmed[0] == '{')
	if jsonInput {
		if err := validateJSONDocument(payload); err != nil {
			return err
		}
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(out); err != nil {
			return err
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			if err == nil {
				return errors.New("multiple documents are not allowed")
			}
			return err
		}
		return nil
	}
	validationDecoder := yaml.NewDecoder(bytes.NewReader(payload))
	var document yaml.Node
	if err := validationDecoder.Decode(&document); err != nil {
		return err
	}
	if err := validateYAMLNode(&document); err != nil {
		return err
	}
	var validationExtra yaml.Node
	if err := validationDecoder.Decode(&validationExtra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple YAML documents are not allowed")
		}
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(payload))
	decoder.KnownFields(true)
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple YAML documents are not allowed")
		}
		return err
	}
	return nil
}

func validateJSONDocument(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := validateJSONValue(decoder, "$", true); err != nil {
		return err
	}
	if token, err := decoder.Token(); err == nil {
		return fmt.Errorf("multiple JSON values are not allowed; unexpected token %v", token)
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func validateJSONValue(decoder *json.Decoder, path string, root bool) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		if root {
			return fmt.Errorf("%s must be a JSON object", path)
		}
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("%s has a non-string object key", path)
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate JSON key %q at %s", key, path)
			}
			seen[key] = struct{}{}
			if err := validateJSONValue(decoder, path+"."+key, false); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return fmt.Errorf("%s has an unterminated object", path)
		}
	case '[':
		index := 0
		for decoder.More() {
			if err := validateJSONValue(decoder, fmt.Sprintf("%s[%d]", path, index), false); err != nil {
				return err
			}
			index++
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return fmt.Errorf("%s has an unterminated array", path)
		}
	default:
		return fmt.Errorf("%s starts with unexpected delimiter %q", path, delimiter)
	}
	return nil
}

func encodeDeterministic(value any, format string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		payload, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return nil, err
		}
		return append(payload, '\n'), nil
	case "yaml":
		var buffer bytes.Buffer
		encoder := yaml.NewEncoder(&buffer)
		encoder.SetIndent(2)
		if err := encoder.Encode(value); err != nil {
			return nil, err
		}
		if err := encoder.Close(); err != nil {
			return nil, err
		}
		return buffer.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported format %q; want json or yaml", format)
	}
}
