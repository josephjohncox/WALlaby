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
