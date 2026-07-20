package connector

import (
	"strings"
	"testing"

	"pgregory.net/rapid"
)

func TestValidateBatch(t *testing.T) {
	t.Parallel()

	dataRecord := Record{
		Table:         "widgets",
		Operation:     OpInsert,
		SchemaVersion: 7,
		After:         map[string]any{"id": int64(1)},
	}
	ddlRecord := Record{
		Table:         "widgets",
		Operation:     OpDDL,
		SchemaVersion: 7,
		DDL:           "ALTER TABLE widgets ADD COLUMN note text",
	}
	validSchema := Schema{Name: "widgets", Namespace: "public", Version: 7}

	tests := []struct {
		name      string
		batch     Batch
		wantError string
	}{
		{
			name:  "empty control checkpoint",
			batch: Batch{Checkpoint: Checkpoint{LSN: "0/1"}},
		},
		{
			name: "homogeneous data",
			batch: Batch{Schema: validSchema, Records: []Record{
				dataRecord,
				{Table: "widgets", Operation: OpUpdate, SchemaVersion: 7},
			}},
		},
		{
			name: "schema version zero inherits batch schema for compatibility",
			batch: Batch{Schema: validSchema, Records: []Record{
				{Table: "widgets", Operation: OpDelete},
			}},
		},
		{
			name: "homogeneous ddl control",
			batch: Batch{Schema: validSchema, Records: []Record{
				ddlRecord,
				{Table: "widgets", Operation: OpDDL, SchemaVersion: 7, DDLPlan: []byte(`{"changes":[]}`)},
			}},
		},
		{
			name: "tableless logical ddl controls",
			batch: Batch{Records: []Record{
				{Operation: OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN first text", SourcePosition: "0/10"},
				{Operation: OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN second text", SourcePosition: "0/20"},
			}},
		},
		{
			name:      "table-scoped ddl requires schema",
			batch:     Batch{Records: []Record{ddlRecord}},
			wantError: "requires a batch schema",
		},
		{
			name:      "missing schema",
			batch:     Batch{Records: []Record{dataRecord}},
			wantError: "schema name is required",
		},
		{
			name:      "missing table",
			batch:     Batch{Schema: validSchema, Records: []Record{{Operation: OpInsert, SchemaVersion: 7}}},
			wantError: "table is required",
		},
		{
			name: "mixed tables",
			batch: Batch{Schema: validSchema, Records: []Record{
				dataRecord,
				{Table: "gadgets", Operation: OpInsert, SchemaVersion: 7},
			}},
			wantError: "does not match batch schema table",
		},
		{
			name: "mixed schema versions",
			batch: Batch{Schema: validSchema, Records: []Record{
				dataRecord,
				{Table: "widgets", Operation: OpInsert, SchemaVersion: 8},
			}},
			wantError: "does not match batch schema version",
		},
		{
			name: "mixed data and ddl control",
			batch: Batch{Schema: validSchema, Records: []Record{
				dataRecord,
				ddlRecord,
			}},
			wantError: "mixes data and control records",
		},
		{
			name: "ddl text marks a control record even with a data operation",
			batch: Batch{Schema: validSchema, Records: []Record{
				dataRecord,
				{Table: "widgets", Operation: OpUpdate, SchemaVersion: 7, DDL: "ALTER TABLE widgets DROP COLUMN note"},
			}},
			wantError: "mixes data and control records",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateBatch(tt.batch)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("ValidateBatch() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("ValidateBatch() error = %v, want %q", err, tt.wantError)
			}
		})
	}
}

func TestValidateBatchRejectsHeterogeneousBatchesRapid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(t *rapid.T) {
		version := rapid.Int64Range(1, 1_000_000).Draw(t, "version")
		table := rapid.StringMatching(`[a-z][a-z0-9_]{0,20}`).Draw(t, "table")
		invalidity := rapid.SampledFrom([]string{
			"missing-schema",
			"missing-table",
			"mixed-table",
			"mixed-version",
			"mixed-control",
		}).Draw(t, "invalidity")

		batch := Batch{
			Schema: Schema{Name: table, Namespace: "public", Version: version},
			Records: []Record{{
				Table:         table,
				Operation:     OpInsert,
				SchemaVersion: version,
			}},
		}
		switch invalidity {
		case "missing-schema":
			batch.Schema.Name = ""
		case "missing-table":
			batch.Records[0].Table = ""
		case "mixed-table":
			batch.Records = append(batch.Records, Record{Table: table + "_other", Operation: OpInsert, SchemaVersion: version})
		case "mixed-version":
			batch.Records = append(batch.Records, Record{Table: table, Operation: OpUpdate, SchemaVersion: version + 1})
		case "mixed-control":
			batch.Records = append(batch.Records, Record{Table: table, Operation: OpDDL, SchemaVersion: version, DDL: "ALTER TABLE"})
		default:
			t.Fatalf("unknown invalidity %q", invalidity)
		}

		if err := ValidateBatch(batch); err == nil {
			t.Fatalf("ValidateBatch() accepted %s batch: %+v", invalidity, batch)
		}
	})
}
