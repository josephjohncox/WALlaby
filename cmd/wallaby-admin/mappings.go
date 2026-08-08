package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
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
	fs.StringArray("write-mode", nil, "write policy override schema.table=append|upsert (repeatable)")
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
	sourcePB, err := cfg.Source.toProto(endpointcodec.RoleSource)
	if err != nil {
		return fmt.Errorf("decode source endpoint: %w", err)
	}
	sourceSpec, err := endpointcodec.Decode(sourcePB, endpointcodec.RoleSource)
	if err != nil || sourceSpec.Type != connector.EndpointPostgres {
		return errors.New("mapping generation requires a PostgreSQL source")
	}
	destinationName, _ := cmd.Flags().GetString("destination")
	var destination endpointConfig
	var destinationSpec connector.RuntimeSpec
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
	writeModeValues, _ := cmd.Flags().GetStringArray("write-mode")
	writeModes, err := parseWriteModeOverrides(writeModeValues)
	if err != nil {
		return err
	}
	iam, err := awsIAMOptions(cmd)
	if err != nil {
		return err
	}
	options := mergeOptionMaps(sourceSpec.Options, iam.options())
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
		generated, err := mappinggen.Generate(mappinggen.Request{Destination: destinationSpec.Name, Tables: catalog, Watermarks: watermarks, MatchColumns: matches})
		if err != nil {
			return err
		}
		if err := applyGeneratedWritePolicies(&generated.Destinations[0], destinationSpec, catalog, watermarks, matches, writeModes, true); err != nil {
			return err
		}
		if err := generated.Validate([]connector.RuntimeSpec{destinationSpec}); err != nil {
			return fmt.Errorf("validate generated mappings: %w", err)
		}
		output = generated
	case "flow":
		complete, err := completeFlowMappings(cfg, catalog, watermarks, matches, writeModes)
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

func flowEndpointSpec(destination endpointConfig) (connector.RuntimeSpec, error) {
	pb, err := destination.toProto(endpointcodec.RoleDestination)
	if err != nil {
		return connector.RuntimeSpec{}, err
	}
	return endpointcodec.Decode(pb, endpointcodec.RoleDestination)
}
func validateFlowDestinations(destinations []endpointConfig, selected string) (map[string]connector.RuntimeSpec, error) {
	if len(destinations) == 0 {
		return nil, errors.New("flow has no destinations")
	}
	out := make(map[string]connector.RuntimeSpec, len(destinations))
	for _, destination := range destinations {
		spec, err := flowEndpointSpec(destination)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(spec.Name) == "" {
			return nil, errors.New("flow destination name is required")
		}
		if _, duplicate := out[spec.Name]; duplicate {
			return nil, fmt.Errorf("duplicate destination name %q", spec.Name)
		}
		out[spec.Name] = spec
	}
	if selected != "" {
		if _, ok := out[selected]; !ok {
			return nil, fmt.Errorf("destination %q not found", selected)
		}
	}
	return out, nil
}
func completeFlowMappings(cfg flowConfig, tables []mappinggen.CatalogTable, watermarks map[mappinggen.TableRef]string, matches map[mappinggen.TableRef][]string, writeModes map[mappinggen.TableRef]flow.TableWriteMode) (*flow.TableMappings, error) {
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
		destinationSpec, err := flowEndpointSpec(destination)
		if err != nil {
			return nil, err
		}
		destinationName := destinationSpec.Name
		spec := specs[destinationName]
		if existing, ok := existingByName[destinationName]; ok {
			single := flow.TableMappings{Version: flow.TableMappingsVersion, Destinations: []flow.DestinationTableMappings{existing}}.Clone()
			if err := applyGeneratedWritePolicies(&single.Destinations[0], spec, tables, watermarks, matches, writeModes, false); err != nil {
				return nil, fmt.Errorf("apply write-mode overrides for destination %s: %w", destinationName, err)
			}
			if err := single.Validate([]connector.RuntimeSpec{spec}); err != nil {
				return nil, fmt.Errorf("validate existing mappings for destination %s: %w", destinationName, err)
			}
			out.Destinations = append(out.Destinations, single.Destinations[0])
			continue
		}
		generated, err := mappinggen.Generate(mappinggen.Request{Destination: destinationName, Tables: tables, Watermarks: watermarks, MatchColumns: matches})
		if err != nil {
			return nil, err
		}
		if err := applyGeneratedWritePolicies(&generated.Destinations[0], spec, tables, watermarks, matches, writeModes, true); err != nil {
			return nil, fmt.Errorf("generate policies for destination %s: %w", destinationName, err)
		}
		if err := generated.Validate([]connector.RuntimeSpec{spec}); err != nil {
			return nil, fmt.Errorf("validate generated mappings for destination %s: %w", destinationName, err)
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
		spec, err := flowEndpointSpec(destination)
		if err != nil {
			return endpointConfig{}, err
		}
		if strings.TrimSpace(spec.Name) == "" {
			return endpointConfig{}, errors.New("flow destination name is required")
		}
		if _, duplicate := seen[spec.Name]; duplicate {
			return endpointConfig{}, fmt.Errorf("duplicate destination name %q", spec.Name)
		}
		seen[spec.Name] = struct{}{}
	}
	if strings.TrimSpace(name) == "" {
		if len(destinations) != 1 {
			return endpointConfig{}, errors.New("--destination is required when flow has multiple destinations")
		}
		return destinations[0], nil
	}
	for _, destination := range destinations {
		spec, err := flowEndpointSpec(destination)
		if err != nil {
			return endpointConfig{}, err
		}
		if spec.Name == name {
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
		column, err := pgsource.ParseCatalogColumnName(right)
		if err != nil {
			return nil, fmt.Errorf("%s override column: %w", kind, err)
		}
		ref := mappinggen.TableRef{Schema: table.Schema, Table: table.Table}
		if _, duplicate := out[ref]; duplicate {
			return nil, fmt.Errorf("duplicate %s override for %s.%s", kind, ref.Schema, ref.Table)
		}
		out[ref] = column
	}
	return out, nil
}
func parseWriteModeOverrides(values []string) (map[mappinggen.TableRef]flow.TableWriteMode, error) {
	out := map[mappinggen.TableRef]flow.TableWriteMode{}
	for _, value := range values {
		left, right, ok := strings.Cut(value, "=")
		if !ok {
			return nil, fmt.Errorf("write-mode override %q must be schema.table=append|upsert", value)
		}
		table, err := pgsource.ParseCatalogTableName(left)
		if err != nil {
			return nil, err
		}
		mode := flow.TableWriteMode(strings.ToLower(strings.TrimSpace(right)))
		if mode != flow.TableWriteModeAppend && mode != flow.TableWriteModeUpsert {
			return nil, fmt.Errorf("write-mode override for %s.%s must be append or upsert", table.Schema, table.Table)
		}
		ref := mappinggen.TableRef{Schema: table.Schema, Table: table.Table}
		if _, duplicate := out[ref]; duplicate {
			return nil, fmt.Errorf("duplicate write-mode override for %s.%s", ref.Schema, ref.Table)
		}
		out[ref] = mode
	}
	return out, nil
}

func applyGeneratedWritePolicies(mapping *flow.DestinationTableMappings, spec connector.RuntimeSpec, catalog []mappinggen.CatalogTable, watermarks map[mappinggen.TableRef]string, matches map[mappinggen.TableRef][]string, overrides map[mappinggen.TableRef]flow.TableWriteMode, applyCapabilityDefault bool) error {
	if connector.IsPostgresToSnowflakeSQLV1Spec(spec) {
		return applyManagedSnowflakeSQLGeneratedPolicy(mapping, catalog, watermarks, matches, overrides, applyCapabilityDefault)
	}
	catalogByRef := make(map[mappinggen.TableRef]mappinggen.CatalogTable, len(catalog))
	for _, table := range catalog {
		catalogByRef[mappinggen.TableRef{Schema: table.Schema, Table: table.Table}] = table
	}
	for ref := range overrides {
		if _, ok := catalogByRef[ref]; !ok {
			return fmt.Errorf("write-mode override references unselected table %s.%s", ref.Schema, ref.Table)
		}
	}
	applied := make(map[mappinggen.TableRef]struct{}, len(overrides))
	for index := range mapping.Tables {
		table := &mapping.Tables[index]
		ref := mappinggen.TableRef{Schema: table.SourceSchema, Table: table.SourceTable}
		override, hasOverride := overrides[ref]
		if table.Action == flow.MappingActionExclude {
			if hasOverride {
				return fmt.Errorf("write-mode override references excluded table %s.%s", ref.Schema, ref.Table)
			}
			continue
		}
		mode := table.Write.Mode
		switch {
		case hasOverride:
			mode = override
			applied[ref] = struct{}{}
		case applyCapabilityDefault && mode == flow.TableWriteModeUpsert && !flow.SupportsExplicitKeyUpsert(spec):
			mode = flow.TableWriteModeAppend
		default:
			continue
		}
		switch mode {
		case flow.TableWriteModeAppend:
			table.Write.Mode = mode
			table.Write.KeyColumns = nil
		case flow.TableWriteModeUpsert:
			if !flow.SupportsExplicitKeyUpsert(spec) {
				return fmt.Errorf("destination type %s profile %q does not support explicit-key upsert for %s.%s", spec.Type, strings.TrimSpace(spec.Options["managed_profile"]), ref.Schema, ref.Table)
			}
			keys := append([]string(nil), matches[ref]...)
			if len(keys) == 0 {
				keys = append(keys, catalogByRef[ref].PrimaryKeyColumns...)
			}
			if len(keys) == 0 {
				return fmt.Errorf("upsert write-mode override for %s.%s requires match columns or a source primary key", ref.Schema, ref.Table)
			}
			table.Write.Mode = mode
			table.Write.KeyColumns = keys
		default:
			return fmt.Errorf("generated write mode for %s.%s must be append or upsert", ref.Schema, ref.Table)
		}
	}
	for ref := range overrides {
		if _, ok := applied[ref]; !ok {
			return fmt.Errorf("write-mode override for %s.%s requires an exact included table mapping", ref.Schema, ref.Table)
		}
	}
	return nil
}

func applyManagedSnowflakeSQLGeneratedPolicy(mapping *flow.DestinationTableMappings, catalog []mappinggen.CatalogTable, watermarks map[mappinggen.TableRef]string, matches map[mappinggen.TableRef][]string, overrides map[mappinggen.TableRef]flow.TableWriteMode, generated bool) error {
	if len(catalog) != 1 {
		return fmt.Errorf("managed Snowflake SQL mapping generation requires exactly one selected relation; got %d", len(catalog))
	}
	table := catalog[0]
	ref := mappinggen.TableRef{Schema: table.Schema, Table: table.Table}
	primaryKey := append([]string(nil), table.PrimaryKeyColumns...)
	if len(primaryKey) == 0 {
		return fmt.Errorf("managed Snowflake SQL relation %s.%s requires a complete source primary key", ref.Schema, ref.Table)
	}
	for watermarkRef := range watermarks {
		if watermarkRef != ref {
			return fmt.Errorf("watermark override references unselected table %s.%s", watermarkRef.Schema, watermarkRef.Table)
		}
		return fmt.Errorf("managed Snowflake SQL profile rejects watermark for %s.%s", ref.Schema, ref.Table)
	}
	for matchRef, columns := range matches {
		if matchRef != ref {
			return fmt.Errorf("match-column override references unselected table %s.%s", matchRef.Schema, matchRef.Table)
		}
		if !slices.Equal(columns, primaryKey) {
			return fmt.Errorf("managed Snowflake SQL match-column override for %s.%s must equal the complete ordered source primary key %v", ref.Schema, ref.Table, primaryKey)
		}
	}
	for overrideRef, mode := range overrides {
		if overrideRef != ref {
			return fmt.Errorf("write-mode override references unselected table %s.%s", overrideRef.Schema, overrideRef.Table)
		}
		if mode != flow.TableWriteModeUpsert {
			return fmt.Errorf("managed Snowflake SQL profile rejects append write-mode override for %s.%s", ref.Schema, ref.Table)
		}
	}
	if generated {
		if len(mapping.Tables) != 1 {
			return fmt.Errorf("managed Snowflake SQL generation produced %d exact table mappings; want 1", len(mapping.Tables))
		}
		if mapping.Tables[0].Write.WatermarkColumn != "" {
			return fmt.Errorf("managed Snowflake SQL profile rejects watermark for %s.%s", ref.Schema, ref.Table)
		}
		mapping.FutureTables = flow.FutureTableMapping{Action: flow.MappingActionExclude}
		mapping.Tables[0].Write = flow.TableWritePolicy{Mode: flow.TableWriteModeUpsert, KeyColumns: primaryKey}
	}
	if mapping.FutureTables.Action != flow.MappingActionExclude {
		return errors.New("managed Snowflake SQL mappings require future_tables.action=exclude")
	}
	if len(mapping.Tables) != 1 {
		return fmt.Errorf("managed Snowflake SQL mappings require exactly one exact table; got %d", len(mapping.Tables))
	}
	mapped := mapping.Tables[0]
	if mapped.Action != flow.MappingActionInclude || mapped.SourceSchema != ref.Schema || mapped.SourceTable != ref.Table {
		return fmt.Errorf("managed Snowflake SQL exact mapping must include selected relation %s.%s", ref.Schema, ref.Table)
	}
	if mapped.Write.WatermarkColumn != "" {
		return fmt.Errorf("managed Snowflake SQL profile rejects watermark for %s.%s", ref.Schema, ref.Table)
	}
	if mapped.Write.Mode != flow.TableWriteModeUpsert || !slices.Equal(mapped.Write.KeyColumns, primaryKey) {
		return fmt.Errorf("managed Snowflake SQL mapping for %s.%s must upsert by complete ordered source primary key %v", ref.Schema, ref.Table, primaryKey)
	}
	return nil
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
		columns, err := pgsource.ParseCatalogColumnNames(right)
		if err != nil {
			return nil, fmt.Errorf("match-column override for %s.%s: %w", table.Schema, table.Table, err)
		}
		seen := map[string]struct{}{}
		for _, column := range columns {
			if _, duplicate := seen[column]; duplicate {
				return nil, fmt.Errorf("match-column override for %s.%s repeats column %s", table.Schema, table.Table, column)
			}
			seen[column] = struct{}{}
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
