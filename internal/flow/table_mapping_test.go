package flow

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestTableMappingsValidateAndFingerprint(t *testing.T) {
	t.Parallel()
	destinations := []connector.Spec{{Name: "warehouse", Type: connector.EndpointPostgres}}
	mappings := richTestMappings()
	if err := mappings.Validate(destinations); err != nil {
		t.Fatalf("Validate(): %v", err)
	}
	first, err := mappings.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	second, err := mappings.Clone().Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || first != second {
		t.Fatalf("fingerprints %q and %q are not stable", first, second)
	}
	changed := mappings.Clone()
	changed.Destinations[0].Tables[0].TargetTable = "other"
	third, err := changed.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("target rename did not change table mapping fingerprint")
	}
}

func TestTableMappingsCanonicalEqualityFingerprintAndJSONRoundTrip(t *testing.T) {
	t.Parallel()
	base := richTestMappings()
	base.Destinations[0].Tables[0].Write.KeyColumns = []string{"id", "updated_at"}
	base.Destinations[0].Tables = append(base.Destinations[0].Tables, TableMapping{SourceSchema: "public", SourceTable: "ignored", Action: MappingActionExclude})
	base.Destinations = append(base.Destinations, NewTableMappings([]connector.Spec{{Name: "archive", Type: connector.EndpointPostgres}}).Destinations[0])
	permuted := base.Clone()
	permuted.Destinations[0], permuted.Destinations[1] = permuted.Destinations[1], permuted.Destinations[0]
	for destinationIndex := range permuted.Destinations {
		destination := &permuted.Destinations[destinationIndex]
		for tableIndex := range destination.Tables {
			table := &destination.Tables[tableIndex]
			for left, right := 0, len(table.Columns)-1; left < right; left, right = left+1, right-1 {
				table.Columns[left], table.Columns[right] = table.Columns[right], table.Columns[left]
			}
			if table.Columns == nil {
				table.Columns = []ColumnMapping{}
			}
		}
		for left, right := 0, len(destination.Tables)-1; left < right; left, right = left+1, right-1 {
			destination.Tables[left], destination.Tables[right] = destination.Tables[right], destination.Tables[left]
		}
		destination.FutureTables.Write.KeyColumns = []string{}
	}
	if !base.Equal(permuted) {
		t.Fatal("semantically irrelevant ordering or nil/empty changed mapping equality")
	}
	first, _ := base.Fingerprint()
	second, _ := permuted.Fingerprint()
	if first != second {
		t.Fatalf("canonical fingerprints differ: %s != %s", first, second)
	}
	encoded, err := json.Marshal(permuted)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"key_columns"`) || strings.Contains(string(encoded), `"key":`) {
		t.Fatalf("mapping JSON did not use direct key_columns: %s", encoded)
	}
	var roundTrip TableMappings
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if !base.Equal(roundTrip) {
		t.Fatal("JSON round trip changed canonical mapping")
	}
	keyOrder := base.Clone()
	for destinationIndex := range keyOrder.Destinations {
		if keyOrder.Destinations[destinationIndex].Destination == "warehouse" {
			keyOrder.Destinations[destinationIndex].Tables[0].Write.KeyColumns = []string{"updated_at", "id"}
		}
	}
	third, _ := keyOrder.Fingerprint()
	if third == first {
		t.Fatal("key-column order did not change fingerprint")
	}
}

func TestTableMappingsRejectInvalidContracts(t *testing.T) {
	t.Parallel()
	postgres := []connector.Spec{{Name: "warehouse", Type: connector.EndpointPostgres}}
	tests := []struct {
		name string
		edit func(*TableMappings)
		dest []connector.Spec
		want string
	}{
		{name: "missing version", edit: func(m *TableMappings) { m.Version = 0 }, dest: postgres, want: "version"},
		{name: "missing destination mapping", edit: func(m *TableMappings) { m.Destinations = nil }, dest: postgres, want: "at least one"},
		{name: "unknown destination", edit: func(m *TableMappings) { m.Destinations[0].Destination = "missing" }, dest: postgres, want: "unknown destination"},
		{name: "duplicate source table", edit: func(m *TableMappings) {
			m.Destinations[0].Tables = append(m.Destinations[0].Tables, m.Destinations[0].Tables[0])
		}, dest: postgres, want: "duplicate source table"},
		{name: "excluded key", edit: func(m *TableMappings) {
			m.Destinations[0].Tables[0].Columns[0].Action = MappingActionExclude
			m.Destinations[0].Tables[0].Columns[0].TargetColumn = ""
		}, dest: postgres, want: "key column"},
		{name: "keyless upsert", edit: func(m *TableMappings) { m.Destinations[0].Tables[0].Write.KeyColumns = nil }, dest: postgres, want: "at least one key"},
		{name: "append with key columns", edit: func(m *TableMappings) { m.Destinations[0].Tables[0].Write.Mode = TableWriteModeAppend }, dest: postgres, want: "append write policy cannot define key_columns"},
		{name: "unknown future template", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "{unknown}" }, dest: postgres, want: "exactly one {table}"},
		{name: "future table missing placeholder", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "fixed" }, dest: postgres, want: "exactly one {table}"},
		{name: "future schema missing placeholder", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetSchema = "analytics" }, dest: postgres, want: "exactly one {schema}"},
		{name: "future schema duplicate placeholder", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetSchema = "{schema}_{schema}" }, dest: postgres, want: "exactly one {schema}"},
		{name: "future schema table variable", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetSchema = "{schema}_{table}" }, dest: postgres, want: "placeholders other than {schema}"},
		{name: "future schema column variable", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetSchema = "{schema}_{column}" }, dest: postgres, want: "placeholders other than {schema}"},
		{name: "future table duplicate placeholder", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "{table}_{table}" }, dest: postgres, want: "exactly one {table}"},
		{name: "future table schema variable", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "{schema}_{table}" }, dest: postgres, want: "placeholders other than {table}"},
		{name: "future table column variable", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "{column}_{table}" }, dest: postgres, want: "placeholders other than {table}"},
		{name: "future schema and table both cross variables", edit: func(m *TableMappings) {
			m.Destinations[0].FutureTables.TargetSchema = "{schema}_{table}"
			m.Destinations[0].FutureTables.TargetTable = "{schema}_{table}"
		}, dest: postgres, want: "placeholders other than {schema}"},
		{name: "future column missing placeholder", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.FutureColumns.TargetColumn = "fixed" }, dest: postgres, want: "exactly one {column}"},
		{name: "future column duplicate placeholder", edit: func(m *TableMappings) {
			m.Destinations[0].FutureTables.FutureColumns.TargetColumn = "{column}_{column}"
		}, dest: postgres, want: "exactly one {column}"},
		{name: "future column schema variable", edit: func(m *TableMappings) {
			m.Destinations[0].FutureTables.FutureColumns.TargetColumn = "{schema}_{column}"
		}, dest: postgres, want: "placeholders other than {column}"},
		{name: "future column table variable", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.FutureColumns.TargetColumn = "{table}_{column}" }, dest: postgres, want: "placeholders other than {column}"},
		{name: "destination whitespace", edit: func(m *TableMappings) { m.Destinations[0].Destination = " warehouse" }, dest: postgres, want: "whitespace"},
		{name: "flow destination whitespace", edit: func(*TableMappings) {}, dest: []connector.Spec{{Name: "warehouse ", Type: connector.EndpointPostgres}}, want: "whitespace"},
		{name: "source identifier whitespace", edit: func(m *TableMappings) { m.Destinations[0].Tables[0].SourceTable = "customers " }, dest: postgres, want: "whitespace"},
		{name: "target identifier whitespace", edit: func(m *TableMappings) { m.Destinations[0].Tables[0].TargetTable = " accounts" }, dest: postgres, want: "whitespace"},
		{name: "key whitespace", edit: func(m *TableMappings) { m.Destinations[0].Tables[0].Write.KeyColumns[0] = " id" }, dest: postgres, want: "whitespace"},
		{name: "watermark whitespace", edit: func(m *TableMappings) { m.Destinations[0].Tables[0].Write.WatermarkColumn = "updated_at " }, dest: postgres, want: "whitespace"},
		{name: "template whitespace", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = " {table}" }, dest: postgres, want: "whitespace"},
		{name: "upsert append only", edit: func(*TableMappings) {}, dest: []connector.Spec{{Name: "warehouse", Type: connector.EndpointIceberg}}, want: "does not support upsert"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := richTestMappings()
			test.edit(&candidate)
			if err := candidate.Validate(test.dest); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error=%v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateDefinitionRejectsMissingMappingsAndProjectedWAL(t *testing.T) {
	t.Parallel()
	destination := connector.Spec{Name: "warehouse", Type: connector.EndpointPostgres}
	definition := Flow{Destinations: []connector.Spec{destination}}
	if err := ValidateDefinition(definition); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("missing mappings error=%v", err)
	}
	definition.Config.TableMappings = richTestMappings()
	definition.Destinations[0].Options = map[string]string{"payload_mode": "wal"}
	if err := ValidateDefinition(definition); err == nil || !strings.Contains(err.Error(), "payload_mode=wal") {
		t.Fatalf("projected WAL error=%v", err)
	}
}

func richTestMappings() TableMappings {
	return TableMappings{Version: TableMappingsVersion, Destinations: []DestinationTableMappings{{
		Destination: "warehouse",
		FutureTables: FutureTableMapping{
			Action: MappingActionInclude, TargetSchema: "{schema}", TargetTable: "{table}",
			FutureColumns: FutureColumnMapping{Action: MappingActionInclude, TargetColumn: "{column}"},
			Write:         TableWritePolicy{Mode: TableWriteModeAppend},
		},
		Tables: []TableMapping{{
			SourceSchema: "public", SourceTable: "customers", Action: MappingActionInclude,
			TargetSchema: "analytics", TargetTable: "accounts",
			FutureColumns: FutureColumnMapping{Action: MappingActionInclude, TargetColumn: "{column}"},
			Columns: []ColumnMapping{
				{SourceColumn: "id", Action: MappingActionInclude, TargetColumn: "account_id"},
				{SourceColumn: "secret", Action: MappingActionExclude},
				{SourceColumn: "updated_at", Action: MappingActionInclude, TargetColumn: "modified_at"},
			},
			Write: TableWritePolicy{Mode: TableWriteModeUpsert, KeyColumns: []string{"id"}, WatermarkColumn: "updated_at"},
		}},
	}}}
}
