package flow

import (
	"encoding/json"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestTableMappingsValidateAndFingerprint(t *testing.T) {
	t.Parallel()
	destinations := []connector.RuntimeSpec{{Name: "warehouse", Type: connector.EndpointPostgres}}
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

func TestTableMappingsPreserveExactCaseAndWhitespaceIdentifiers(t *testing.T) {
	t.Parallel()
	destination := connector.RuntimeSpec{Name: "warehouse", Type: connector.EndpointPostgres}
	mappings := TableMappings{
		Version: TableMappingsVersion,
		Destinations: []DestinationTableMappings{{
			Destination: "warehouse", FutureTables: FutureTableMapping{Action: MappingActionExclude},
			Tables: []TableMapping{
				{SourceSchema: "Exact Schema", SourceTable: "Events", Action: MappingActionExclude},
				{SourceSchema: "Exact Schema", SourceTable: "events", Action: MappingActionExclude},
				{SourceSchema: " ", SourceTable: " ", Action: MappingActionInclude, TargetSchema: " ", TargetTable: " ", FutureColumns: FutureColumnMapping{Action: MappingActionInclude, TargetColumn: "{{ .Column }}"}, Columns: []ColumnMapping{{SourceColumn: " ", Action: MappingActionInclude, TargetColumn: " "}}, Write: TableWritePolicy{Mode: TableWriteModeAppend}},
			},
		}},
	}
	if err := mappings.Validate([]connector.RuntimeSpec{destination}); err != nil {
		t.Fatalf("valid exact PostgreSQL identifiers rejected: %v", err)
	}
	clone := mappings.Clone()
	if clone.Destinations[0].Tables[0].SourceTable != "Events" || clone.Destinations[0].Tables[1].SourceTable != "events" || clone.Destinations[0].Tables[2].SourceSchema != " " || clone.Destinations[0].Tables[2].SourceTable != " " || clone.Destinations[0].Tables[2].TargetSchema != " " || clone.Destinations[0].Tables[2].TargetTable != " " || clone.Destinations[0].Tables[2].Columns[0].SourceColumn != " " || clone.Destinations[0].Tables[2].Columns[0].TargetColumn != " " {
		t.Fatalf("mapping clone changed exact identifiers: %+v", clone.Destinations[0].Tables)
	}
}

func TestTableMappingsCanonicalEqualityFingerprintAndJSONRoundTrip(t *testing.T) {
	t.Parallel()
	base := richTestMappings()
	base.Destinations[0].Tables[0].Write.KeyColumns = []string{"id", "updated_at"}
	base.Destinations[0].Tables = append(base.Destinations[0].Tables, TableMapping{SourceSchema: "public", SourceTable: "ignored", Action: MappingActionExclude})
	base.Destinations = append(base.Destinations, NewTableMappings([]connector.RuntimeSpec{{Name: "archive", Type: connector.EndpointPostgres}}).Destinations[0])
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

func TestExactTargetsAllowLiteralBracesButRejectExecutableTemplates(t *testing.T) {
	t.Parallel()
	destination := []connector.RuntimeSpec{{Name: "warehouse", Type: connector.EndpointPostgres}}
	valid := richTestMappings()
	valid.Destinations[0].Tables[0].TargetSchema = "schema{literal}"
	valid.Destinations[0].Tables[0].TargetTable = "table{{not valid Go template syntax"
	valid.Destinations[0].Tables[0].Columns[0].TargetColumn = "column}literal{"
	valid.Destinations[0].Tables[0].Columns = append(valid.Destinations[0].Tables[0].Columns, ColumnMapping{SourceColumn: "column}literal{", Action: MappingActionExclude})
	valid.Destinations[0].Tables = append(valid.Destinations[0].Tables, TableMapping{SourceSchema: "schema{literal}", SourceTable: "table{{not valid Go template syntax", Action: MappingActionExclude})
	if err := valid.Validate(destination); err != nil {
		t.Fatalf("literal exact target braces rejected: %v", err)
	}
	for _, mutate := range []func(*TableMappings){
		func(m *TableMappings) { m.Destinations[0].Tables[0].TargetSchema = "{{ .Schema }}" },
		func(m *TableMappings) { m.Destinations[0].Tables[0].TargetTable = "{{ print \"table\" }}" },
		func(m *TableMappings) {
			m.Destinations[0].Tables[0].Columns[0].TargetColumn = "{{ if true }}column{{ end }}"
		},
		func(m *TableMappings) { m.Destinations[0].Tables[0].TargetTable = "events{{/* comment */}}" },
	} {
		candidate := richTestMappings()
		mutate(&candidate)
		if err := candidate.Validate(destination); err == nil || !strings.Contains(err.Error(), "executable Go template") {
			t.Fatalf("Validate() error = %v, want executable Go template rejection", err)
		}
	}
}

func TestFutureTableTemplatesDoNotCollideWithExactTargets(t *testing.T) {
	t.Parallel()
	destination := []connector.RuntimeSpec{{Name: "warehouse", Type: connector.EndpointPostgres}}
	makeMappings := func(schemaTemplate, tableTemplate string, exact TableMapping, overrides ...TableMapping) TableMappings {
		tables := append([]TableMapping{exact}, overrides...)
		return TableMappings{Version: TableMappingsVersion, Destinations: []DestinationTableMappings{{
			Destination: "warehouse",
			FutureTables: FutureTableMapping{
				Action: MappingActionInclude, TargetSchema: schemaTemplate, TargetTable: tableTemplate,
				FutureColumns: FutureColumnMapping{Action: MappingActionInclude, TargetColumn: "{{ .Column }}"},
				Write:         TableWritePolicy{Mode: TableWriteModeAppend},
			},
			Tables: tables,
		}}}
	}
	exact := func(sourceSchema, sourceTable, targetSchema, targetTable string) TableMapping {
		return TableMapping{
			SourceSchema: sourceSchema, SourceTable: sourceTable, Action: MappingActionInclude,
			TargetSchema: targetSchema, TargetTable: targetTable,
			FutureColumns: FutureColumnMapping{Action: MappingActionExclude},
			Write:         TableWritePolicy{Mode: TableWriteModeAppend},
		}
	}
	exclude := func(schema, table string) TableMapping {
		return TableMapping{SourceSchema: schema, SourceTable: table, Action: MappingActionExclude}
	}

	tests := []struct {
		name     string
		mappings TableMappings
		wantErr  string
	}{
		{
			name:     "identity future renamed exact collision",
			mappings: makeMappings("{{ .Schema }}", "{{ .Table }}", exact("public", "orders", "archive", "orders")),
			wantErr:  `unoverridden source "archive"."orders"`,
		},
		{
			name:     "prefix suffix inverse collision",
			mappings: makeMappings("dst_{{ .Schema }}_end", "tbl_{{ .Table }}_end", exact("public", "orders", "dst_archive_end", "tbl_history_end")),
			wantErr:  `unoverridden source "archive"."history"`,
		},
		{
			name:     "candidate overridden by exact exclude",
			mappings: makeMappings("{{ .Schema }}", "{{ .Table }}", exact("public", "orders", "archive", "orders"), exclude("archive", "orders")),
		},
		{
			name: "candidate overridden by exact include",
			mappings: makeMappings("dst_{{ .Schema }}_end", "tbl_{{ .Table }}_end",
				exact("public", "orders", "dst_archive_end", "tbl_history_end"),
				exact("archive", "history", "literal", "target")),
		},
		{
			name:     "same source exact precedence",
			mappings: makeMappings("{{ .Schema }}", "{{ .Table }}", exact("public", "orders", "public", "orders")),
		},
		{
			name:     "dots spaces and case are exact bytes",
			mappings: makeMappings("pre{{ .Schema }}post", "pre{{ .Table }}post", exact("source", "table", "pre Exact.Schema post", "pre Table.Name post")),
			wantErr:  `unoverridden source " Exact.Schema "." Table.Name "`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.mappings.Validate(destination)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Validate() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestFutureColumnTemplatesDoNotCollideWithExactTargets(t *testing.T) {
	t.Parallel()
	destination := []connector.RuntimeSpec{{Name: "warehouse", Type: connector.EndpointPostgres}}
	makeMappings := func(columnTemplate string, columns ...ColumnMapping) TableMappings {
		return TableMappings{Version: TableMappingsVersion, Destinations: []DestinationTableMappings{{
			Destination:  "warehouse",
			FutureTables: FutureTableMapping{Action: MappingActionExclude},
			Tables: []TableMapping{{
				SourceSchema: "public", SourceTable: "events", Action: MappingActionInclude,
				TargetSchema: "public", TargetTable: "events",
				FutureColumns: FutureColumnMapping{Action: MappingActionInclude, TargetColumn: columnTemplate},
				Columns:       columns,
				Write:         TableWritePolicy{Mode: TableWriteModeAppend},
			}},
		}}}
	}
	include := func(source, target string) ColumnMapping {
		return ColumnMapping{SourceColumn: source, Action: MappingActionInclude, TargetColumn: target}
	}
	exclude := func(source string) ColumnMapping {
		return ColumnMapping{SourceColumn: source, Action: MappingActionExclude}
	}

	tests := []struct {
		name     string
		mappings TableMappings
		wantErr  string
	}{
		{name: "identity future renamed exact collision", mappings: makeMappings("{{ .Column }}", include("legacy", "current")), wantErr: `source column "current"`},
		{name: "prefix suffix inverse collision", mappings: makeMappings("pre{{ .Column }}post", include("legacy", "precurrentpost")), wantErr: `source column "current"`},
		{name: "candidate overridden by exact exclude", mappings: makeMappings("{{ .Column }}", include("legacy", "current"), exclude("current"))},
		{name: "candidate overridden by exact include", mappings: makeMappings("pre{{ .Column }}post", include("legacy", "precurrentpost"), include("current", "literal"))},
		{name: "same source exact precedence", mappings: makeMappings("pre{{ .Column }}post", include(" Mixed.Name ", "pre Mixed.Name post"))},
		{name: "dots spaces and case are exact bytes", mappings: makeMappings("pre{{ .Column }}post", include("legacy", "pre Exact.Name post")), wantErr: `source column " Exact.Name "`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.mappings.Validate(destination)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Validate() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestTableMappingsRejectInvalidContracts(t *testing.T) {
	t.Parallel()
	postgres := []connector.RuntimeSpec{{Name: "warehouse", Type: connector.EndpointPostgres}}
	tests := []struct {
		name string
		edit func(*TableMappings)
		dest []connector.RuntimeSpec
		want string
	}{
		{name: "missing version", edit: func(m *TableMappings) { m.Version = 0 }, dest: postgres, want: "version"},
		{name: "version 1 explicitly unsupported", edit: func(m *TableMappings) { m.Version = 1 }, dest: postgres, want: "version 1 is unsupported; expected 2"},
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
		{name: "legacy schema placeholder", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetSchema = "{schema}" }, dest: postgres, want: "exactly one action"},
		{name: "legacy table placeholder", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "{table}" }, dest: postgres, want: "exactly one action"},
		{name: "legacy column placeholder", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.FutureColumns.TargetColumn = "{column}" }, dest: postgres, want: "exactly one action"},
		{name: "unknown future template", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "{{ .Unknown }}" }, dest: postgres, want: ".Table"},
		{name: "future table missing action", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "fixed" }, dest: postgres, want: "exactly one action"},
		{name: "future schema missing action", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetSchema = "analytics" }, dest: postgres, want: "exactly one action"},
		{name: "future schema duplicate action", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetSchema = "{{ .Schema }}_{{ .Schema }}" }, dest: postgres, want: "exactly one action"},
		{name: "future schema table field", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetSchema = "{{ .Table }}" }, dest: postgres, want: ".Schema"},
		{name: "future schema column field", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetSchema = "{{ .Column }}" }, dest: postgres, want: ".Schema"},
		{name: "future table duplicate action", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "{{ .Table }}_{{ .Table }}" }, dest: postgres, want: "exactly one action"},
		{name: "future table schema field", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "{{ .Schema }}" }, dest: postgres, want: ".Table"},
		{name: "future table column field", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = "{{ .Column }}" }, dest: postgres, want: ".Table"},
		{name: "future column missing action", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.FutureColumns.TargetColumn = "fixed" }, dest: postgres, want: "exactly one action"},
		{name: "future column duplicate action", edit: func(m *TableMappings) {
			m.Destinations[0].FutureTables.FutureColumns.TargetColumn = "{{ .Column }}_{{ .Column }}"
		}, dest: postgres, want: "exactly one action"},
		{name: "future column schema field", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.FutureColumns.TargetColumn = "{{ .Schema }}" }, dest: postgres, want: ".Column"},
		{name: "future column table field", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.FutureColumns.TargetColumn = "{{ .Table }}" }, dest: postgres, want: ".Column"},
		{name: "destination whitespace", edit: func(m *TableMappings) { m.Destinations[0].Destination = " warehouse" }, dest: postgres, want: "whitespace"},
		{name: "flow destination whitespace", edit: func(*TableMappings) {}, dest: []connector.RuntimeSpec{{Name: "warehouse ", Type: connector.EndpointPostgres}}, want: "whitespace"},
		{name: "source identifier NUL", edit: func(m *TableMappings) { m.Destinations[0].Tables[0].SourceTable = "customers\x00shadow" }, dest: postgres, want: "NUL"},
		{name: "target identifier NUL", edit: func(m *TableMappings) { m.Destinations[0].Tables[0].TargetTable = "accounts\x00shadow" }, dest: postgres, want: "NUL"},
		{name: "key NUL", edit: func(m *TableMappings) { m.Destinations[0].Tables[0].Write.KeyColumns[0] = "id\x00shadow" }, dest: postgres, want: "NUL"},
		{name: "watermark NUL", edit: func(m *TableMappings) { m.Destinations[0].Tables[0].Write.WatermarkColumn = "updated_at\x00shadow" }, dest: postgres, want: "NUL"},
		{name: "template whitespace", edit: func(m *TableMappings) { m.Destinations[0].FutureTables.TargetTable = " {{ .Table }}" }, dest: postgres, want: "whitespace"},
		{name: "upsert append only", edit: func(*TableMappings) {}, dest: []connector.RuntimeSpec{{Name: "warehouse", Type: connector.EndpointIceberg}}, want: "does not support upsert"},
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

func TestAppendWatermarkIsMetadataAndSnowflakeUpsertIsProfileScoped(t *testing.T) {
	t.Parallel()
	appendMapping := richTestMappings()
	appendMapping.Destinations[0].Tables[0].Write = TableWritePolicy{Mode: TableWriteModeAppend, WatermarkColumn: "updated_at"}
	if err := appendMapping.Validate([]connector.RuntimeSpec{{Name: "warehouse", Type: connector.EndpointS3}}); err != nil {
		t.Fatalf("append watermark metadata rejected: %v", err)
	}
	upsert := richTestMappings()
	upsert.Destinations[0].FutureTables = FutureTableMapping{Action: MappingActionExclude}
	upsert.Destinations[0].Tables[0].Write.WatermarkColumn = ""
	generic := connector.RuntimeSpec{Name: "warehouse", Type: connector.EndpointSnowflake}
	if err := upsert.Validate([]connector.RuntimeSpec{generic}); err == nil || !strings.Contains(err.Error(), "does not support upsert") {
		t.Fatalf("generic Snowflake upsert error=%v", err)
	}
	managed := generic
	managed.Options = map[string]string{"managed_profile": connector.ManagedProfilePostgresToSnowflakeSQLV1}
	if err := upsert.Validate([]connector.RuntimeSpec{managed}); err != nil {
		t.Fatalf("managed Snowflake explicit-key upsert rejected: %v", err)
	}
	wrongProfile := generic
	wrongProfile.Options = map[string]string{"managed_profile": "postgresql-to-snowflake-sql-v2"}
	if err := upsert.Validate([]connector.RuntimeSpec{wrongProfile}); err == nil {
		t.Fatal("unknown Snowflake profile admitted upsert")
	}
	upsert.Destinations[0].Tables[0].Write.WatermarkColumn = "updated_at"
	if err := upsert.Validate([]connector.RuntimeSpec{managed}); err == nil || !strings.Contains(err.Error(), "watermark-guarded") {
		t.Fatalf("managed Snowflake watermark upsert error=%v", err)
	}
}

func TestValidateDefinitionRejectsMissingMappingsAndProjectedWAL(t *testing.T) {
	t.Parallel()
	definition := Flow{
		Source:       &wallabypb.Endpoint{Name: "source", Config: &wallabypb.Endpoint_PostgresSource{PostgresSource: &wallabypb.PostgresSourceConfig{Mode: wallabypb.PostgresSourceMode_POSTGRES_SOURCE_MODE_CDC}}},
		Destinations: []*wallabypb.Endpoint{{Name: "warehouse", Config: &wallabypb.Endpoint_Http{Http: &wallabypb.HTTPDestinationConfig{}}}},
	}
	if err := ValidateDefinition(definition); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("missing mappings error=%v", err)
	}
	definition.Config.TableMappings = NewTableMappings([]connector.RuntimeSpec{{Name: "warehouse", Type: connector.EndpointHTTP}})
	definition.Destinations[0].GetHttp().PayloadMode = wallabypb.PayloadMode_PAYLOAD_MODE_WAL
	if err := ValidateDefinition(definition); err == nil || !strings.Contains(err.Error(), "payload_mode=wal") {
		t.Fatalf("projected WAL error=%v", err)
	}
}

func richTestMappings() TableMappings {
	return TableMappings{Version: TableMappingsVersion, Destinations: []DestinationTableMappings{{
		Destination: "warehouse",
		FutureTables: FutureTableMapping{
			Action: MappingActionInclude, TargetSchema: "{{ .Schema }}", TargetTable: "{{ .Table }}",
			FutureColumns: FutureColumnMapping{Action: MappingActionInclude, TargetColumn: "{{ .Column }}"},
			Write:         TableWritePolicy{Mode: TableWriteModeAppend},
		},
		Tables: []TableMapping{{
			SourceSchema: "public", SourceTable: "customers", Action: MappingActionInclude,
			TargetSchema: "analytics", TargetTable: "accounts",
			FutureColumns: FutureColumnMapping{Action: MappingActionInclude, TargetColumn: "{{ .Column }}"},
			Columns: []ColumnMapping{
				{SourceColumn: "id", Action: MappingActionInclude, TargetColumn: "account_id"},
				{SourceColumn: "secret", Action: MappingActionExclude},
				{SourceColumn: "updated_at", Action: MappingActionInclude, TargetColumn: "modified_at"},
				{SourceColumn: "account_id", Action: MappingActionExclude},
				{SourceColumn: "modified_at", Action: MappingActionExclude},
			},
			Write: TableWritePolicy{Mode: TableWriteModeUpsert, KeyColumns: []string{"id"}, WatermarkColumn: "updated_at"},
		}, {
			SourceSchema: "analytics", SourceTable: "accounts", Action: MappingActionExclude,
		}},
	}}}
}
