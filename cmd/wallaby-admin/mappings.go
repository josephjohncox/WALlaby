package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/mappinggen"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/spf13/cobra"
)

func addFlowMappingsCommand(flowCommand *cobra.Command) {
	mappings := &cobra.Command{Use: "mappings", Short: "manage destination table mappings"}
	generate := &cobra.Command{Use: "generate", Short: "generate deterministic mappings from an explicit PostgreSQL catalog scope", Long: "Generate deterministic mappings from an explicit PostgreSQL catalog scope.\n\nMappings output contains no endpoint credentials. Full-flow output is a lossless expansion of the input flow and can contain the same secrets as that input; protect the output accordingly.", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error { return runWithConfig(cmd, flowMappingsGenerate, args) }}
	fs := generate.Flags()
	fs.String("file", "", "flow JSON or YAML file")
	fs.String("destination", "", "destination name (required for multiple destinations)")
	fs.String("format", "json", "output format: json|yaml")
	fs.String("output-mode", "mappings", "output mode: mappings (credential-free) | flow (lossless; may contain input secrets)")
	fs.String("output", "", "output path (default stdout)")
	fs.Bool("force", false, "overwrite an existing output file")
	fs.StringArray("table", nil, "source table selector schema.table (repeatable; quoted identifiers supported)")
	fs.StringArray("schema", nil, "source schema selector (repeatable)")
	fs.String("publication", "", "source publication selector")
	fs.StringArray("watermark", nil, "freshness override schema.table=column (repeatable)")
	fs.StringArray("match-column", nil, "match override schema.table=column[,column...] (repeatable)")
	addAWSIAMFlags(generate)
	mappings.AddCommand(generate)
	flowCommand.AddCommand(mappings)
}

func flowMappingsGenerate(cmd *cobra.Command, _ []string) error {
	path, _ := cmd.Flags().GetString("file")
	if strings.TrimSpace(path) == "" {
		return errors.New("--file is required")
	}
	format, _ := cmd.Flags().GetString("format")
	format = strings.ToLower(strings.TrimSpace(format))
	if format != "json" && format != "yaml" {
		return fmt.Errorf("unsupported format %q; want json or yaml", format)
	}
	mode, _ := cmd.Flags().GetString("output-mode")
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode != "mappings" && mode != "flow" {
		return fmt.Errorf("unsupported output mode %q; want mappings or flow", mode)
	}
	cfg, err := loadFlowConfigFile(path)
	if err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(cfg.Source.Type), "postgres") {
		return errors.New("mapping generation requires a PostgreSQL source")
	}
	destinationName, _ := cmd.Flags().GetString("destination")
	var destination endpointConfig
	var destinationSpec connector.Spec
	if mode == "mappings" {
		destination, err = selectFlowConfigDestination(cfg.Destinations, destinationName)
		if err != nil {
			return err
		}
		destinationSpec, err = flowEndpointSpec(destination)
		if err != nil {
			return err
		}
	} else {
		if _, err := validateFlowDestinations(cfg.Destinations, destinationName); err != nil {
			return err
		}
	}
	tableValues, _ := cmd.Flags().GetStringArray("table")
	schemaValues, _ := cmd.Flags().GetStringArray("schema")
	publication, _ := cmd.Flags().GetString("publication")
	scope := mappinggen.CatalogScope{TableSelectors: append([]string(nil), tableValues...), SchemaSelectors: append([]string(nil), schemaValues...), Publication: publication}
	watermarkValues, _ := cmd.Flags().GetStringArray("watermark")
	matchValues, _ := cmd.Flags().GetStringArray("match-column")
	watermarks, err := parseSingleColumnOverrides(watermarkValues, "watermark")
	if err != nil {
		return err
	}
	matches, err := parseMatchOverrides(matchValues)
	if err != nil {
		return err
	}
	iam, err := awsIAMOptions(cmd)
	if err != nil {
		return err
	}
	options := mergeOptionMaps(cfg.Source.Options, iam.options())
	dsn := options["dsn"]
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	catalog, err := mappinggen.InspectPostgres(ctx, dsn, options, scope)
	if err != nil {
		return err
	}
	var output any
	switch mode {
	case "mappings":
		generated, err := mappinggen.Generate(mappinggen.Request{Destination: destination.Name, Tables: catalog, Watermarks: watermarks, MatchColumns: matches})
		if err != nil {
			return err
		}
		if err := generated.Validate([]connector.Spec{destinationSpec}); err != nil {
			return fmt.Errorf("validate generated mappings: %w", err)
		}
		output = generated
	case "flow":
		complete, err := completeFlowMappings(cfg, catalog, watermarks, matches)
		if err != nil {
			return err
		}
		cfg.Config.TableMappings = complete
		if _, err := flowConfigToProto(cfg); err != nil {
			return fmt.Errorf("validate generated flow: %w", err)
		}
		output = cfg
	default:
		return fmt.Errorf("unsupported output mode %q; want mappings or flow", mode)
	}
	payload, err := encodeDeterministic(output, format)
	if err != nil {
		return err
	}
	outputPath, _ := cmd.Flags().GetString("output")
	force, _ := cmd.Flags().GetBool("force")
	return writeGeneratedOutput(outputPath, payload, force)
}

func flowEndpointSpec(destination endpointConfig) (connector.Spec, error) {
	pb, err := endpointConfigToProto(destination)
	if err != nil {
		return connector.Spec{}, err
	}
	return endpointFromProto(pb)
}
func validateFlowDestinations(destinations []endpointConfig, selected string) (map[string]connector.Spec, error) {
	if len(destinations) == 0 {
		return nil, errors.New("flow has no destinations")
	}
	out := make(map[string]connector.Spec, len(destinations))
	for _, destination := range destinations {
		if strings.TrimSpace(destination.Name) == "" {
			return nil, errors.New("flow destination name is required")
		}
		if _, duplicate := out[destination.Name]; duplicate {
			return nil, fmt.Errorf("duplicate destination name %q", destination.Name)
		}
		spec, err := flowEndpointSpec(destination)
		if err != nil {
			return nil, err
		}
		out[destination.Name] = spec
	}
	if selected != "" {
		if _, ok := out[selected]; !ok {
			return nil, fmt.Errorf("destination %q not found", selected)
		}
	}
	return out, nil
}
func completeFlowMappings(cfg flowConfig, tables []mappinggen.CatalogTable, watermarks map[mappinggen.TableRef]string, matches map[mappinggen.TableRef][]string) (*flow.TableMappings, error) {
	specs, err := validateFlowDestinations(cfg.Destinations, "")
	if err != nil {
		return nil, err
	}
	existingByName := map[string]flow.DestinationTableMappings{}
	if cfg.Config.TableMappings != nil {
		if cfg.Config.TableMappings.Version != flow.TableMappingsVersion {
			return nil, fmt.Errorf("table mappings version must be %d", flow.TableMappingsVersion)
		}
		for _, mapping := range cfg.Config.TableMappings.Destinations {
			if _, known := specs[mapping.Destination]; !known {
				return nil, fmt.Errorf("table mappings reference unknown destination %q", mapping.Destination)
			}
			if _, duplicate := existingByName[mapping.Destination]; duplicate {
				return nil, fmt.Errorf("duplicate table mappings for destination %q", mapping.Destination)
			}
			existingByName[mapping.Destination] = mapping
		}
	}
	out := &flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: make([]flow.DestinationTableMappings, 0, len(cfg.Destinations))}
	for _, destination := range cfg.Destinations {
		spec := specs[destination.Name]
		if existing, ok := existingByName[destination.Name]; ok {
			single := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{existing}}
			if err := single.Validate([]connector.Spec{spec}); err != nil {
				return nil, fmt.Errorf("validate existing mappings for destination %s: %w", destination.Name, err)
			}
			out.Destinations = append(out.Destinations, existing)
			continue
		}
		generated, err := mappinggen.Generate(mappinggen.Request{Destination: destination.Name, Tables: tables, Watermarks: watermarks, MatchColumns: matches})
		if err != nil {
			return nil, err
		}
		if err := generated.Validate([]connector.Spec{spec}); err != nil {
			return nil, fmt.Errorf("validate generated mappings for destination %s: %w", destination.Name, err)
		}
		out.Destinations = append(out.Destinations, generated.Destinations[0])
	}
	return out, nil
}

func selectFlowConfigDestination(destinations []endpointConfig, name string) (endpointConfig, error) {
	if len(destinations) == 0 {
		return endpointConfig{}, errors.New("flow has no destinations")
	}
	seen := map[string]struct{}{}
	for _, destination := range destinations {
		if strings.TrimSpace(destination.Name) == "" {
			return endpointConfig{}, errors.New("flow destination name is required")
		}
		if _, duplicate := seen[destination.Name]; duplicate {
			return endpointConfig{}, fmt.Errorf("duplicate destination name %q", destination.Name)
		}
		seen[destination.Name] = struct{}{}
	}
	if strings.TrimSpace(name) == "" {
		if len(destinations) != 1 {
			return endpointConfig{}, errors.New("--destination is required when flow has multiple destinations")
		}
		return destinations[0], nil
	}
	for _, destination := range destinations {
		if destination.Name == name {
			return destination, nil
		}
	}
	return endpointConfig{}, fmt.Errorf("destination %q not found", name)
}
func parseSingleColumnOverrides(values []string, kind string) (map[mappinggen.TableRef]string, error) {
	out := map[mappinggen.TableRef]string{}
	for _, value := range values {
		left, right, ok := strings.Cut(value, "=")
		if !ok || strings.TrimSpace(right) == "" {
			return nil, fmt.Errorf("%s override %q must be schema.table=column", kind, value)
		}
		table, err := pgsource.ParseCatalogTableName(left)
		if err != nil {
			return nil, err
		}
		ref := mappinggen.TableRef{Schema: table.Schema, Table: table.Table}
		if _, duplicate := out[ref]; duplicate {
			return nil, fmt.Errorf("duplicate %s override for %s.%s", kind, ref.Schema, ref.Table)
		}
		out[ref] = strings.TrimSpace(right)
	}
	return out, nil
}
func parseMatchOverrides(values []string) (map[mappinggen.TableRef][]string, error) {
	out := map[mappinggen.TableRef][]string{}
	for _, value := range values {
		left, right, ok := strings.Cut(value, "=")
		if !ok {
			return nil, fmt.Errorf("match-column override %q must be schema.table=column[,column...]", value)
		}
		table, err := pgsource.ParseCatalogTableName(left)
		if err != nil {
			return nil, err
		}
		rawColumns := strings.Split(right, ",")
		columns := make([]string, 0, len(rawColumns))
		seen := map[string]struct{}{}
		for _, raw := range rawColumns {
			column := strings.TrimSpace(raw)
			if column == "" {
				return nil, fmt.Errorf("match-column override for %s.%s contains an empty column", table.Schema, table.Table)
			}
			if _, duplicate := seen[column]; duplicate {
				return nil, fmt.Errorf("match-column override for %s.%s repeats column %s", table.Schema, table.Table, column)
			}
			seen[column] = struct{}{}
			columns = append(columns, column)
		}
		ref := mappinggen.TableRef{Schema: table.Schema, Table: table.Table}
		if _, duplicate := out[ref]; duplicate {
			return nil, fmt.Errorf("duplicate match-column override for %s.%s", ref.Schema, ref.Table)
		}
		out[ref] = columns
	}
	return out, nil
}

var generatedOutputMu sync.Mutex

func writeGeneratedOutput(path string, payload []byte, force bool) error {
	generatedOutputMu.Lock()
	defer generatedOutputMu.Unlock()
	if path == "" || path == "-" {
		_, err := os.Stdout.Write(payload)
		return err
	}
	flags := os.O_WRONLY | os.O_CREATE
	if force {
		flags |= os.O_TRUNC
	} else {
		flags |= os.O_EXCL
	}
	file, err := adminFileSystem.OpenFile(path, flags, 0600)
	if err != nil {
		if !force && os.IsExist(err) {
			return fmt.Errorf("output %s already exists; use --force to overwrite", path)
		}
		return fmt.Errorf("create output: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return fmt.Errorf("write output: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close output: %w", err)
	}
	return nil
}
