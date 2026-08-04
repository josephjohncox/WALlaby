package mappinggen

import (
	"reflect"
	"testing"

	"github.com/josephjohncox/wallaby/internal/flow"
)

func TestGenerateOrdersTablesColumnsAndPreservesCompositePK(t *testing.T) {
	got, err := Generate(Request{Destination: "warehouse", Tables: []CatalogTable{{Schema: "z", Table: "no_key", Columns: []CatalogColumn{{Attnum: 2, Name: "b"}, {Attnum: 1, Name: "a"}}}, {Schema: "Odd Schema", Table: "Quoted Table", PrimaryKeyColumns: []string{"Second Key", "First Key"}, Columns: []CatalogColumn{{Attnum: 2, Name: "Second Key"}, {Attnum: 1, Name: "First Key"}, {Attnum: 3, Name: "generated"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	tables := got.Destinations[0].Tables
	if tables[0].SourceSchema != "Odd Schema" || tables[0].SourceTable != "Quoted Table" {
		t.Fatalf("order=%+v", tables)
	}
	if !reflect.DeepEqual(tables[0].Write.KeyColumns, []string{"Second Key", "First Key"}) || tables[0].Write.Mode != flow.TableWriteModeUpsert {
		t.Fatalf("policy=%+v", tables[0].Write)
	}
	if tables[1].Write.Mode != flow.TableWriteModeAppend {
		t.Fatalf("no-PK policy=%+v", tables[1].Write)
	}
	if got.Destinations[0].FutureTables.Write.Mode != flow.TableWriteModeAppend {
		t.Fatal("future tables must append")
	}
	if tables[1].Columns[0].SourceColumn != "a" {
		t.Fatalf("columns=%+v", tables[1].Columns)
	}
}

func TestGenerateExplicitMatchReplacesPKAndWatermarkRequiresKey(t *testing.T) {
	table := CatalogTable{Schema: "public", Table: "events", PrimaryKeyColumns: []string{"id"}, Columns: []CatalogColumn{{Attnum: 1, Name: "id"}, {Attnum: 2, Name: "natural"}, {Attnum: 3, Name: "updated_at"}}}
	got, err := Generate(Request{Destination: "d", Tables: []CatalogTable{table}, MatchColumns: map[TableRef][]string{{Schema: "public", Table: "events"}: {"natural"}}, Watermarks: map[TableRef]string{{Schema: "public", Table: "events"}: "updated_at"}})
	if err != nil {
		t.Fatal(err)
	}
	policy := got.Destinations[0].Tables[0].Write
	if !reflect.DeepEqual(policy.KeyColumns, []string{"natural"}) || policy.WatermarkColumn != "updated_at" {
		t.Fatalf("policy=%+v", policy)
	}
	table.PrimaryKeyColumns = nil
	if _, err := Generate(Request{Destination: "d", Tables: []CatalogTable{table}, Watermarks: map[TableRef]string{{Schema: "public", Table: "events"}: "updated_at"}}); err == nil {
		t.Fatal("expected keyless watermark rejection")
	}
}

func TestGenerateRejectsUnselectedAndMissingOverrideColumns(t *testing.T) {
	table := CatalogTable{Schema: "public", Table: "events", Columns: []CatalogColumn{{Attnum: 1, Name: "id"}}}
	cases := []Request{{Destination: "d", Tables: []CatalogTable{table}, MatchColumns: map[TableRef][]string{{Schema: "public", Table: "missing"}: {"id"}}}, {Destination: "d", Tables: []CatalogTable{table}, Watermarks: map[TableRef]string{{Schema: "public", Table: "events"}: "missing"}}}
	for _, request := range cases {
		if _, err := Generate(request); err == nil {
			t.Fatal("expected invalid override")
		}
	}
}
