package flow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

const TableMappingsVersion uint32 = 1

type MappingAction string

const (
	MappingActionInclude MappingAction = "include"
	MappingActionExclude MappingAction = "exclude"
)

type TableWriteMode string

const (
	TableWriteModeAppend TableWriteMode = "append"
	TableWriteModeUpsert TableWriteMode = "upsert"
)

// TableMappings is the complete, durable destination-scoped logical projection.
type TableMappings struct {
	Version      uint32                     `json:"version" yaml:"version"`
	Destinations []DestinationTableMappings `json:"destinations" yaml:"destinations"`
}

type DestinationTableMappings struct {
	Destination  string             `json:"destination" yaml:"destination"`
	FutureTables FutureTableMapping `json:"future_tables" yaml:"future_tables"`
	Tables       []TableMapping     `json:"tables" yaml:"tables"`
}

type FutureTableMapping struct {
	Action        MappingAction       `json:"action" yaml:"action"`
	TargetSchema  string              `json:"target_schema,omitempty" yaml:"target_schema,omitempty"`
	TargetTable   string              `json:"target_table,omitempty" yaml:"target_table,omitempty"`
	FutureColumns FutureColumnMapping `json:"future_columns" yaml:"future_columns"`
	Write         TableWritePolicy    `json:"write" yaml:"write"`
}

type TableMapping struct {
	SourceSchema  string              `json:"source_schema" yaml:"source_schema"`
	SourceTable   string              `json:"source_table" yaml:"source_table"`
	Action        MappingAction       `json:"action" yaml:"action"`
	TargetSchema  string              `json:"target_schema,omitempty" yaml:"target_schema,omitempty"`
	TargetTable   string              `json:"target_table,omitempty" yaml:"target_table,omitempty"`
	FutureColumns FutureColumnMapping `json:"future_columns" yaml:"future_columns"`
	Columns       []ColumnMapping     `json:"columns" yaml:"columns"`
	Write         TableWritePolicy    `json:"write" yaml:"write"`
}

type FutureColumnMapping struct {
	Action       MappingAction `json:"action" yaml:"action"`
	TargetColumn string        `json:"target_column,omitempty" yaml:"target_column,omitempty"`
}

type ColumnMapping struct {
	SourceColumn string        `json:"source_column" yaml:"source_column"`
	Action       MappingAction `json:"action" yaml:"action"`
	TargetColumn string        `json:"target_column,omitempty" yaml:"target_column,omitempty"`
}

type TableWritePolicy struct {
	Mode            TableWriteMode `json:"mode" yaml:"mode"`
	KeyColumns      []string       `json:"key_columns" yaml:"key_columns"`
	WatermarkColumn string         `json:"watermark_column,omitempty" yaml:"watermark_column,omitempty"`
}

// NewTableMappings returns the required include-by-name, append-safe policy for the supplied destinations.
func NewTableMappings(destinations []connector.Spec) TableMappings {
	mappings := TableMappings{Version: TableMappingsVersion, Destinations: make([]DestinationTableMappings, 0, len(destinations))}
	for _, destination := range destinations {
		mappings.Destinations = append(mappings.Destinations, DestinationTableMappings{
			Destination: destination.Name,
			FutureTables: FutureTableMapping{
				Action: MappingActionInclude, TargetSchema: "{schema}", TargetTable: "{table}",
				FutureColumns: FutureColumnMapping{Action: MappingActionInclude, TargetColumn: "{column}"},
				Write:         TableWritePolicy{Mode: TableWriteModeAppend},
			},
		})
	}
	return mappings
}

func (m TableMappings) Equal(other TableMappings) bool {
	return reflect.DeepEqual(m.canonical(), other.canonical())
}

func (m TableMappings) Clone() TableMappings {
	out := m
	out.Destinations = make([]DestinationTableMappings, len(m.Destinations))
	for index, destination := range m.Destinations {
		out.Destinations[index] = destination
		out.Destinations[index].Tables = make([]TableMapping, len(destination.Tables))
		for tableIndex, table := range destination.Tables {
			out.Destinations[index].Tables[tableIndex] = table
			out.Destinations[index].Tables[tableIndex].Columns = append([]ColumnMapping(nil), table.Columns...)
			out.Destinations[index].Tables[tableIndex].Write.KeyColumns = append([]string(nil), table.Write.KeyColumns...)
		}
		out.Destinations[index].FutureTables.Write.KeyColumns = append([]string(nil), destination.FutureTables.Write.KeyColumns...)
	}
	return out
}

func (m TableMappings) Fingerprint() (string, error) {
	if m.Version != TableMappingsVersion {
		return "", fmt.Errorf("table mappings version must be %d", TableMappingsVersion)
	}
	encoded, err := json.Marshal(m.canonical())
	if err != nil {
		return "", fmt.Errorf("marshal table mappings: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (m TableMappings) canonical() TableMappings {
	out := m.Clone()
	if out.Destinations == nil {
		out.Destinations = []DestinationTableMappings{}
	}
	for destinationIndex := range out.Destinations {
		destination := &out.Destinations[destinationIndex]
		if destination.Tables == nil {
			destination.Tables = []TableMapping{}
		}
		if destination.FutureTables.Write.KeyColumns == nil {
			destination.FutureTables.Write.KeyColumns = []string{}
		}
		for tableIndex := range destination.Tables {
			table := &destination.Tables[tableIndex]
			if table.Columns == nil {
				table.Columns = []ColumnMapping{}
			}
			if table.Write.KeyColumns == nil {
				table.Write.KeyColumns = []string{}
			}
			sort.SliceStable(table.Columns, func(left, right int) bool {
				return table.Columns[left].SourceColumn < table.Columns[right].SourceColumn
			})
		}
		sort.SliceStable(destination.Tables, func(left, right int) bool {
			if destination.Tables[left].SourceSchema != destination.Tables[right].SourceSchema {
				return destination.Tables[left].SourceSchema < destination.Tables[right].SourceSchema
			}
			return destination.Tables[left].SourceTable < destination.Tables[right].SourceTable
		})
	}
	sort.SliceStable(out.Destinations, func(left, right int) bool {
		return out.Destinations[left].Destination < out.Destinations[right].Destination
	})
	return out
}

func (m TableMappings) ForDestination(name string) (DestinationTableMappings, bool) {
	for _, destination := range m.Destinations {
		if destination.Destination == name {
			return destination, true
		}
	}
	return DestinationTableMappings{}, false
}

func (m TableMappings) IdentityForDestination(name string) bool {
	destination, ok := m.ForDestination(name)
	if !ok || destination.FutureTables.Action != MappingActionInclude ||
		destination.FutureTables.TargetSchema != "{schema}" || destination.FutureTables.TargetTable != "{table}" ||
		destination.FutureTables.FutureColumns.Action != MappingActionInclude || destination.FutureTables.FutureColumns.TargetColumn != "{column}" {
		return false
	}
	// Future tables must use append. Append changes update/delete operations and
	// adds reserved columns, so the durable policy is never raw-WAL identity.
	if destination.FutureTables.Write.Mode == TableWriteModeAppend {
		return false
	}
	for _, table := range destination.Tables {
		if table.Action != MappingActionInclude || table.TargetSchema != table.SourceSchema || table.TargetTable != table.SourceTable ||
			table.FutureColumns.Action != MappingActionInclude || table.FutureColumns.TargetColumn != "{column}" ||
			table.Write.Mode == TableWriteModeAppend {
			return false
		}
		for _, column := range table.Columns {
			if column.Action != MappingActionInclude || column.TargetColumn != column.SourceColumn {
				return false
			}
		}
	}
	return true
}

func (m TableMappings) Validate(destinations []connector.Spec) error {
	if m.Version != TableMappingsVersion {
		return fmt.Errorf("table mappings version must be %d", TableMappingsVersion)
	}
	if len(m.Destinations) == 0 {
		return errors.New("table mappings require at least one destination mapping")
	}
	byName := make(map[string]connector.Spec, len(destinations))
	for _, destination := range destinations {
		name := destination.Name
		if err := validateIdentifier(name, "destination name"); err != nil {
			return err
		}
		if _, duplicate := byName[name]; duplicate {
			return fmt.Errorf("duplicate destination name %q", name)
		}
		byName[name] = destination
	}
	seen := make(map[string]struct{}, len(m.Destinations))
	for _, mapping := range m.Destinations {
		name := mapping.Destination
		if err := validateIdentifier(name, "table mapping destination"); err != nil {
			return err
		}
		destination, ok := byName[name]
		if !ok {
			return fmt.Errorf("table mappings reference unknown destination %q", name)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate table mappings for destination %q", name)
		}
		seen[name] = struct{}{}
		if err := validateDestinationMappings(mapping, destination); err != nil {
			return fmt.Errorf("validate table mappings for destination %s: %w", name, err)
		}
		if strings.EqualFold(strings.TrimSpace(destination.Options["payload_mode"]), "wal") && !m.IdentityForDestination(name) {
			return fmt.Errorf("destination %s payload_mode=wal cannot be used with a nonidentity table projection", name)
		}
	}
	for name := range byName {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("destination %q has no table mappings", name)
		}
	}
	return nil
}

func validateDestinationMappings(mapping DestinationTableMappings, destination connector.Spec) error {
	if err := validateFutureTable(mapping.FutureTables, destination); err != nil {
		return err
	}
	seenSource := make(map[string]struct{}, len(mapping.Tables))
	seenTarget := make(map[string]string, len(mapping.Tables))
	for _, table := range mapping.Tables {
		sourceSchema := table.SourceSchema
		sourceTable := table.SourceTable
		if err := validateIdentifier(sourceSchema, "source_schema"); err != nil {
			return fmt.Errorf("exact table mapping: %w", err)
		}
		if err := validateIdentifier(sourceTable, "source_table"); err != nil {
			return fmt.Errorf("exact table mapping: %w", err)
		}
		sourceKey := sourceSchema + "\x00" + sourceTable
		if _, duplicate := seenSource[sourceKey]; duplicate {
			return fmt.Errorf("duplicate source table mapping %s.%s", sourceSchema, sourceTable)
		}
		seenSource[sourceKey] = struct{}{}
		if err := validateAction(table.Action, "table"); err != nil {
			return fmt.Errorf("table %s.%s: %w", sourceSchema, sourceTable, err)
		}
		if table.Action == MappingActionExclude {
			if table.TargetSchema != "" || table.TargetTable != "" || len(table.Columns) != 0 || table.FutureColumns != (FutureColumnMapping{}) || table.Write.Mode != "" || len(table.Write.KeyColumns) != 0 || table.Write.WatermarkColumn != "" {
				return fmt.Errorf("excluded table %s.%s cannot define a target, columns, or write policy", sourceSchema, sourceTable)
			}
			continue
		}
		if err := validateIdentifier(table.TargetSchema, "target_schema"); err != nil {
			return fmt.Errorf("included table %s.%s: %w", sourceSchema, sourceTable, err)
		}
		if err := validateIdentifier(table.TargetTable, "target_table"); err != nil {
			return fmt.Errorf("included table %s.%s: %w", sourceSchema, sourceTable, err)
		}
		if containsTemplate(table.TargetSchema) || containsTemplate(table.TargetTable) {
			return fmt.Errorf("exact table %s.%s target names cannot contain templates", sourceSchema, sourceTable)
		}
		targetKey := table.TargetSchema + "\x00" + table.TargetTable
		if prior, collision := seenTarget[targetKey]; collision {
			return fmt.Errorf("source tables %s and %s.%s map to the same target %s.%s", prior, sourceSchema, sourceTable, table.TargetSchema, table.TargetTable)
		}
		seenTarget[targetKey] = sourceSchema + "." + sourceTable
		if err := validateFutureColumn(table.FutureColumns, "table "+sourceSchema+"."+sourceTable); err != nil {
			return err
		}
		if err := validateColumns(table); err != nil {
			return err
		}
		if err := validateWritePolicy(table.Write, destination, false); err != nil {
			return fmt.Errorf("table %s.%s: %w", sourceSchema, sourceTable, err)
		}
		for _, key := range table.Write.KeyColumns {
			if !tableIncludesColumn(table, key) {
				return fmt.Errorf("table %s.%s key column %q is excluded", sourceSchema, sourceTable, key)
			}
		}
		if table.Write.WatermarkColumn != "" && !tableIncludesColumn(table, table.Write.WatermarkColumn) {
			return fmt.Errorf("table %s.%s watermark column %q is excluded", sourceSchema, sourceTable, table.Write.WatermarkColumn)
		}
	}
	return nil
}

func validateFutureTable(future FutureTableMapping, destination connector.Spec) error {
	if err := validateAction(future.Action, "future table"); err != nil {
		return err
	}
	if future.Action == MappingActionExclude {
		if future.TargetSchema != "" || future.TargetTable != "" || future.FutureColumns != (FutureColumnMapping{}) || future.Write.Mode != "" || len(future.Write.KeyColumns) != 0 || future.Write.WatermarkColumn != "" {
			return errors.New("excluded future tables cannot define targets, columns, or write policy")
		}
		return nil
	}
	if err := validateFutureTemplate(future.TargetSchema, "schema"); err != nil {
		return fmt.Errorf("future target_schema: %w", err)
	}
	if err := validateFutureTemplate(future.TargetTable, "table"); err != nil {
		return fmt.Errorf("future target_table: %w", err)
	}
	if err := validateFutureColumn(future.FutureColumns, "future tables"); err != nil {
		return err
	}
	if err := validateWritePolicy(future.Write, destination, true); err != nil {
		return fmt.Errorf("future tables: %w", err)
	}
	return nil
}

func validateColumns(table TableMapping) error {
	seenSource := make(map[string]struct{}, len(table.Columns))
	seenTarget := make(map[string]string, len(table.Columns))
	for _, column := range table.Columns {
		source := column.SourceColumn
		if err := validateIdentifier(source, "source_column"); err != nil {
			return fmt.Errorf("table %s.%s: %w", table.SourceSchema, table.SourceTable, err)
		}
		if _, duplicate := seenSource[source]; duplicate {
			return fmt.Errorf("table %s.%s repeats source column %q", table.SourceSchema, table.SourceTable, source)
		}
		seenSource[source] = struct{}{}
		if err := validateAction(column.Action, "column"); err != nil {
			return fmt.Errorf("table %s.%s column %s: %w", table.SourceSchema, table.SourceTable, source, err)
		}
		if column.Action == MappingActionExclude {
			if column.TargetColumn != "" {
				return fmt.Errorf("excluded column %s.%s.%s cannot define target_column", table.SourceSchema, table.SourceTable, source)
			}
			continue
		}
		target := column.TargetColumn
		if err := validateIdentifier(target, "target_column"); err != nil {
			return fmt.Errorf("included column %s.%s.%s: %w", table.SourceSchema, table.SourceTable, source, err)
		}
		if containsTemplate(target) {
			return fmt.Errorf("included column %s.%s.%s requires a literal target_column", table.SourceSchema, table.SourceTable, source)
		}
		if prior, collision := seenTarget[target]; collision {
			return fmt.Errorf("source columns %s and %s map to target column %q", prior, source, target)
		}
		seenTarget[target] = source
	}
	return nil
}

func validateFutureColumn(future FutureColumnMapping, scope string) error {
	if err := validateAction(future.Action, "future column"); err != nil {
		return fmt.Errorf("%s: %w", scope, err)
	}
	if future.Action == MappingActionExclude {
		if future.TargetColumn != "" {
			return fmt.Errorf("%s: excluded future columns cannot define target_column", scope)
		}
		return nil
	}
	if err := validateFutureTemplate(future.TargetColumn, "column"); err != nil {
		return fmt.Errorf("%s future target_column: %w", scope, err)
	}
	return nil
}

func validateWritePolicy(write TableWritePolicy, destination connector.Spec, future bool) error {
	switch write.Mode {
	case TableWriteModeAppend:
		if len(write.KeyColumns) != 0 {
			return errors.New("append write policy cannot define key_columns")
		}
	case TableWriteModeUpsert:
		if future {
			return errors.New("future tables must use append because their key contract is unknown")
		}
		if !SupportsExplicitKeyUpsert(destination) {
			return fmt.Errorf("destination type %s profile %q does not support upsert table mappings", destination.Type, strings.TrimSpace(destination.Options["managed_profile"]))
		}
		if len(write.KeyColumns) == 0 {
			return errors.New("upsert write policy requires at least one key_columns entry")
		}
		seen := make(map[string]struct{}, len(write.KeyColumns))
		for _, column := range write.KeyColumns {
			if err := validateIdentifier(column, "upsert key column"); err != nil {
				return err
			}
			if _, duplicate := seen[column]; duplicate {
				return fmt.Errorf("upsert key repeats column %q", column)
			}
			seen[column] = struct{}{}
		}
	default:
		return errors.New("write mode must be append or upsert")
	}
	if write.WatermarkColumn != "" {
		if err := validateIdentifier(write.WatermarkColumn, "watermark column"); err != nil {
			return err
		}
		if write.Mode == TableWriteModeUpsert && destination.Type != connector.EndpointPostgres {
			return fmt.Errorf("destination type %s does not support watermark-guarded upsert", destination.Type)
		}
	}
	return nil
}

// SupportsExplicitKeyUpsert reports the exact configured destination/profile
// admission used by mapping validation and authoring tools.
func SupportsExplicitKeyUpsert(destination connector.Spec) bool {
	return destination.Type == connector.EndpointPostgres || connector.IsPostgresToSnowflakeSQLV1Spec(destination)
}

func tableIncludesColumn(table TableMapping, name string) bool {
	for _, column := range table.Columns {
		if column.SourceColumn == name {
			return column.Action == MappingActionInclude
		}
	}
	return table.FutureColumns.Action == MappingActionInclude
}

func validateAction(action MappingAction, subject string) error {
	if action != MappingActionInclude && action != MappingActionExclude {
		return fmt.Errorf("%s action must be include or exclude", subject)
	}
	return nil
}

func validateFutureTemplate(value, variable string) error {
	if value == "" {
		return errors.New("template is required")
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("template %q has leading or trailing whitespace", value)
	}
	placeholder := "{" + variable + "}"
	if strings.Count(value, placeholder) != 1 {
		return fmt.Errorf("template %q must contain exactly one %s", value, placeholder)
	}
	if remainder := strings.Replace(value, placeholder, "", 1); strings.ContainsAny(remainder, "{}") {
		return fmt.Errorf("template %q cannot contain placeholders other than %s", value, placeholder)
	}
	return nil
}

func validateIdentifier(value, subject string) error {
	if value == "" {
		return fmt.Errorf("%s is required", subject)
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("%s %q has leading or trailing whitespace", subject, value)
	}
	return nil
}

func containsTemplate(value string) bool { return strings.ContainsAny(value, "{}") }
