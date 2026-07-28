package snowflake

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedSnowflakeTypeMappingFailsClosedForUnboundedNumeric(t *testing.T) {
	t.Parallel()
	cfg := managedConfig{}
	if got := managedSnowflakeType(cfg, "numeric"); got != "" {
		t.Fatalf("unbounded PostgreSQL numeric mapped to %q", got)
	}
	if got := managedSnowflakeType(cfg, "numeric(12,2)"); got != "NUMBER(12,2)" {
		t.Fatalf("bounded PostgreSQL numeric mapped to %q", got)
	}
	if got := managedSnowflakeType(cfg, "character varying(40)"); got != "VARCHAR(40)" {
		t.Fatalf("bounded PostgreSQL varchar mapped to %q", got)
	}
	for _, sourceType := range []string{"money", "timetz", "time with time zone"} {
		if got := managedSnowflakeType(cfg, sourceType); got != "" {
			t.Fatalf("lossy PostgreSQL type %q mapped to %q", sourceType, got)
		}
	}
}

func TestManagedSnowflakeTypeMappingRejectsJSONArraysAndContainerExtensions(t *testing.T) {
	t.Parallel()
	cfg := managedConfig{typeMappings: defaultSnowflakeTypeMappings()}
	columns := []connector.Column{
		{Name: "json", Type: "json"},
		{Name: "jsonb", Type: "jsonb"},
		{Name: "text_array", Type: "text[]"},
		{Name: "numeric_array", Type: "numeric[]"},
		{Name: "embedding", Type: "public.vector", TypeMetadata: map[string]string{"extension": "vector"}},
		{Name: "attributes", Type: "public.hstore", TypeMetadata: map[string]string{"extension": "hstore"}},
		{Name: "integer", Type: "integer"},
		{Name: "floating", Type: "double precision"},
		{Name: "identifier", Type: "uuid"},
		{Name: "date", Type: "date"},
		{Name: "bounded_text", Type: "character varying(32)"},
	}
	for _, column := range columns {
		if got := managedSnowflakeColumnType(cfg, column); got != "" {
			t.Errorf("lossy managed source column %s type %q mapped to %q", column.Name, column.Type, got)
		}
	}
}

func TestManagedSnowflakePlanPreservesTransactionOrderAndImmutableIdentity(t *testing.T) {
	t.Parallel()
	schema := managedTestSchema()
	cfg := managedConfig{
		profile:  connector.ManagedProfilePostgresToSnowflakeSQLV1,
		flowID:   "flow-1",
		database: "DB", schema: "PUBLIC", table: "WIDGETS", receiptsTable: "WALLABY_RECEIPTS",
		sourceSchema: "public", sourceTable: "widgets", schemaContract: schema,
		schemaContractHash: mustManagedSchemaHash(t, schema), destinationRevision: "snowflake-v1",
		maxTransactionRows: 10, maxTransactionBytes: 1 << 20, maxFragments: 4,
	}
	transaction := managedTestTransaction(schema)
	intent := managedTestIntent(t, transaction)

	plan, err := planManagedSnowflakeTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatal(err)
	}
	gotOrder := make([]managedOperationIdentity, 0, len(plan.operations))
	for _, operation := range plan.operations {
		gotOrder = append(gotOrder, operation.identity)
	}
	wantOrder := []managedOperationIdentity{
		{fragmentOrdinal: 0, recordOrdinal: 0, operation: connector.OpInsert},
		{fragmentOrdinal: 0, recordOrdinal: 1, operation: connector.OpUpdate},
		{fragmentOrdinal: 1, recordOrdinal: 0, operation: connector.OpDelete},
	}
	if !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Fatalf("operation order=%+v, want %+v", gotOrder, wantOrder)
	}
	for _, operation := range plan.operations {
		if !strings.Contains(operation.query, `"ID"`) || strings.Contains(operation.query, `"id"`) {
			t.Fatalf("managed SQL did not map source identifiers to admitted unquoted Snowflake names: %s", operation.query)
		}
	}
	if plan.receipt.logicalBatchID != intent.LogicalBatchID || plan.receipt.contentHash != intent.ContentHash {
		t.Fatalf("receipt identity=%+v", plan.receipt)
	}
	if plan.receipt.manifestHash == "" || plan.receipt.externalID == "" {
		t.Fatalf("receipt lacks immutable manifest identity: %+v", plan.receipt)
	}

	replayed, err := planManagedSnowflakeTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.receipt.manifestHash != plan.receipt.manifestHash || replayed.receipt.externalID != plan.receipt.externalID {
		t.Fatalf("replay changed immutable identity: first=%+v replay=%+v", plan.receipt, replayed.receipt)
	}
	changed := intent
	changed.DestinationRevisionID = "snowflake-v2"
	if _, err := planManagedSnowflakeTransaction(cfg, changed, transaction); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("destination revision conflict error=%v", err)
	}
	changed = intent
	changed.PositionID = "0/21"
	if _, err := planManagedSnowflakeTransaction(cfg, changed, transaction); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("checkpoint position conflict error=%v", err)
	}
}

func TestManagedSnowflakeCompositePrimaryKeyUsesDeclaredOrdinal(t *testing.T) {
	t.Parallel()
	schema := connector.Schema{Name: "widgets", Namespace: "public", Columns: []connector.Column{
		{Name: "a", Type: "int8", TypeMetadata: map[string]string{"primary_key": "true", "primary_key_ordinal": "2"}},
		{Name: "b", Type: "int8", TypeMetadata: map[string]string{"primary_key": "true", "primary_key_ordinal": "1"}},
	}}
	columns, err := managedIdentityColumns(schema)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(columns, []string{"b", "a"}) {
		t.Fatalf("composite primary-key order=%v", columns)
	}
	firstHash, err := ManagedSchemaContractHash(schema)
	if err != nil {
		t.Fatal(err)
	}
	schema.Columns[0].TypeMetadata["primary_key_ordinal"] = "1"
	schema.Columns[1].TypeMetadata["primary_key_ordinal"] = "2"
	secondHash, err := ManagedSchemaContractHash(schema)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash == secondHash {
		t.Fatal("composite primary-key order did not change schema identity")
	}
}

func TestManagedSnowflakeSchemaHashCanonicalizesPostgresTypeAliases(t *testing.T) {
	t.Parallel()
	alias := managedTestSchema()
	canonical := managedTestSchema()
	canonical.Columns[0].Type = "bigint"
	aliasHash, err := ManagedSchemaContractHash(alias)
	if err != nil {
		t.Fatal(err)
	}
	canonicalHash, err := ManagedSchemaContractHash(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if aliasHash != canonicalHash {
		t.Fatalf("PostgreSQL type aliases changed schema identity: %s != %s", aliasHash, canonicalHash)
	}
}

func TestManagedSnowflakePlanCopiesMutableBindValues(t *testing.T) {
	t.Parallel()
	cfg, _ := managedCatalogFixture(t)
	cfg.schemaContract.Columns = append(cfg.schemaContract.Columns, connector.Column{
		Name: "blob", Type: "bytea", Nullable: true,
		TypeMetadata: map[string]string{"nullability_known": "true", "generated_known": "true"},
	})
	var err error
	cfg.schemaContractHash, err = ManagedSchemaContractHash(cfg.schemaContract)
	if err != nil {
		t.Fatal(err)
	}
	blob := []byte{1, 2, 3}
	transaction := managedTestTransaction(cfg.schemaContract)
	transaction.Fragments[0].Batch.Records[0].After["blob"] = blob
	transaction.Fragments[0].Batch.Records[1].After["blob"] = nil
	intent := managedTestIntent(t, transaction)
	plan, err := planManagedSnowflakeTransaction(cfg, intent, transaction)
	if err != nil {
		t.Fatal(err)
	}
	blob[0] = 9
	var planned []byte
	for _, value := range plan.operations[0].args {
		if typed, ok := value.([]byte); ok {
			planned = typed
		}
	}
	if !reflect.DeepEqual(planned, []byte{1, 2, 3}) {
		t.Fatalf("planned mutable value=%v, want immutable copy", planned)
	}
}

func TestManagedSnowflakePlanAcceptsPgoutputUnknownMetadataOnlyWhenColumnsAndIdentityMatch(t *testing.T) {
	t.Parallel()
	contract := managedTestSchema()
	cfg := managedConfig{
		profile:  connector.ManagedProfilePostgresToSnowflakeSQLV1,
		flowID:   "flow-1",
		database: "DB", schema: "PUBLIC", table: "WIDGETS", receiptsTable: "WALLABY_RECEIPTS",
		sourceSchema: "public", sourceTable: "widgets", schemaContract: contract,
		schemaContractHash: mustManagedSchemaHash(t, contract), destinationRevision: "snowflake-v1",
		maxTransactionRows: 10, maxTransactionBytes: 1 << 20, maxFragments: 4,
	}
	runtimeSchema := contract
	runtimeSchema.Columns = append([]connector.Column(nil), contract.Columns...)
	for index := range runtimeSchema.Columns {
		runtimeSchema.Columns[index].TypeMetadata = map[string]string{
			"nullability_known": "false", "generated_known": "false",
		}
	}
	runtimeSchema.Columns[0].Nullable = true
	runtimeSchema.Columns[0].TypeMetadata["replica_identity"] = "true"
	transaction := managedTestTransaction(runtimeSchema)
	intent := managedTestIntent(t, transaction)
	if _, err := planManagedSnowflakeTransaction(cfg, intent, transaction); err != nil {
		t.Fatalf("compatible pgoutput runtime metadata rejected: %v", err)
	}

	runtimeSchema.Columns[1].Name = "different"
	transaction = managedTestTransaction(runtimeSchema)
	intent = managedTestIntent(t, transaction)
	if _, err := planManagedSnowflakeTransaction(cfg, intent, transaction); !errors.Is(err, errManagedSnowflakeSchemaNotReconciled) {
		t.Fatalf("column drift error=%v", err)
	}
}

func TestManagedSnowflakeValueCellRejectsLossyOrOutOfRangeValues(t *testing.T) {
	t.Parallel()
	known := map[string]string{"nullability_known": "true", "generated_known": "true"}
	tests := []struct {
		name   string
		column connector.Column
		value  any
		want   string
	}{
		{name: "floating bigint", column: connector.Column{Name: "id", Type: "bigint", TypeMetadata: known}, value: 1.5, want: "type float64"},
		{name: "invalid UTF-8", column: connector.Column{Name: "value", Type: "text", Nullable: true, TypeMetadata: known}, value: string([]byte{0xff}), want: "UTF-8"},
		{name: "non-finite numeric", column: connector.Column{Name: "amount", Type: "numeric(12,2)", Nullable: true, TypeMetadata: known}, value: "NaN", want: "finite plain decimal"},
		{name: "numeric scale", column: connector.Column{Name: "amount", Type: "numeric(12,2)", Nullable: true, TypeMetadata: known}, value: "1.234", want: "numeric(12,2)"},
		{name: "timestamp range", column: connector.Column{Name: "at", Type: "timestamptz", Nullable: true, TypeMetadata: known}, value: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), want: "range 1-9999"},
		{name: "nonnull", column: connector.Column{Name: "id", Type: "bigint", TypeMetadata: known}, value: nil, want: "NULL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := normalizeManagedSnowflakeColumnValue(tt.column, tt.value, false); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("value error=%v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestManagedSnowflakePlanRejectsLossyIdentityAndPartialInsert(t *testing.T) {
	t.Parallel()
	cfg, _ := managedCatalogFixture(t)
	transaction := managedTestTransaction(cfg.schemaContract)
	transaction.Fragments[0].Batch.Records[0].Key = json.RawMessage(`{"id":1.5}`)
	intent := managedTestIntent(t, transaction)
	if _, err := planManagedSnowflakeTransaction(cfg, intent, transaction); err == nil || !strings.Contains(err.Error(), "invalid bigint") {
		t.Fatalf("fractional identity error=%v", err)
	}

	transaction = managedTestTransaction(cfg.schemaContract)
	delete(transaction.Fragments[0].Batch.Records[0].After, "payload")
	intent = managedTestIntent(t, transaction)
	if _, err := planManagedSnowflakeTransaction(cfg, intent, transaction); err == nil || !strings.Contains(err.Error(), "every admitted source column") {
		t.Fatalf("partial insert error=%v", err)
	}
}

func TestManagedSnowflakePlanRejectsPrimaryKeyChangingUpdate(t *testing.T) {
	t.Parallel()
	cfg, _ := managedCatalogFixture(t)
	transaction := managedTestTransaction(cfg.schemaContract)
	transaction.Fragments[0].Batch.Records[1].After["id"] = int64(2)
	intent := managedTestIntent(t, transaction)
	if _, err := planManagedSnowflakeTransaction(cfg, intent, transaction); err == nil || !strings.Contains(err.Error(), "immutable identity") {
		t.Fatalf("primary-key-changing update error=%v", err)
	}
}

func TestManagedSnowflakePlanRejectsConflictRawDDLAndBounds(t *testing.T) {
	t.Parallel()
	schema := managedTestSchema()
	cfg := managedConfig{
		profile:  connector.ManagedProfilePostgresToSnowflakeSQLV1,
		flowID:   "flow-1",
		database: "DB", schema: "PUBLIC", table: "WIDGETS", receiptsTable: "WALLABY_RECEIPTS",
		sourceSchema: "public", sourceTable: "widgets", schemaContract: schema,
		schemaContractHash: mustManagedSchemaHash(t, schema), destinationRevision: "snowflake-v1",
		maxTransactionRows: 3, maxTransactionBytes: 1 << 20, maxFragments: 2,
	}
	transaction := managedTestTransaction(schema)
	intent := managedTestIntent(t, transaction)

	wrongFlow := intent
	wrongFlow.FlowID = "other-flow"
	if _, err := planManagedSnowflakeTransaction(cfg, wrongFlow, transaction); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("flow binding conflict error=%v", err)
	}

	conflicting := intent
	conflicting.ContentHash = "different"
	if _, err := planManagedSnowflakeTransaction(cfg, conflicting, transaction); !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("content conflict error=%v", err)
	}

	rawDDL := transaction
	rawDDL.Fragments = append([]connector.TransactionFragment(nil), transaction.Fragments...)
	rawDDL.Fragments[0].Batch.Records = []connector.Record{{Table: "widgets", Operation: connector.OpDDL, DDL: "ALTER TABLE widgets ADD COLUMN unsafe int"}}
	rawIntent := managedTestIntent(t, rawDDL)
	if _, err := planManagedSnowflakeTransaction(cfg, rawIntent, rawDDL); err == nil || !errors.Is(err, errManagedSnowflakeSchemaNotReconciled) {
		t.Fatalf("raw DDL error=%v", err)
	}
	structuredDDL := transaction
	structuredDDL.Fragments = append([]connector.TransactionFragment(nil), transaction.Fragments...)
	structuredDDL.Fragments[0].Batch.Records = []connector.Record{{Table: "widgets", Operation: connector.OpDDL, DDLPlan: json.RawMessage(`{"Changes":[{"Type":"add_column"}]}`)}}
	structuredIntent := managedTestIntent(t, structuredDDL)
	if _, err := planManagedSnowflakeTransaction(cfg, structuredIntent, structuredDDL); err == nil || !errors.Is(err, errManagedSnowflakeSchemaNotReconciled) {
		t.Fatalf("structured DDL error=%v", err)
	}

	bounded := cfg
	bounded.maxTransactionRows = 2
	if _, err := planManagedSnowflakeTransaction(bounded, intent, transaction); err == nil {
		t.Fatal("transaction row bound was not enforced")
	}
	bounded = cfg
	bounded.maxTransactionBytes = 1
	if _, err := planManagedSnowflakeTransaction(bounded, intent, transaction); err == nil {
		t.Fatal("transaction byte bound was not enforced")
	}
	bounded = cfg
	bounded.maxFragments = 1
	if _, err := planManagedSnowflakeTransaction(bounded, intent, transaction); err == nil {
		t.Fatal("transaction fragment bound was not enforced")
	}
}

func managedTestSchema() connector.Schema {
	return connector.Schema{
		Name: "widgets", Namespace: "public", Version: 7,
		Columns: []connector.Column{
			{Name: "id", Type: "int8", TypeMetadata: map[string]string{"primary_key": "true", "nullability_known": "true", "generated_known": "true"}},
			{Name: "value", Type: "text", Nullable: true, TypeMetadata: map[string]string{"nullability_known": "true", "generated_known": "true"}},
			{Name: "payload", Type: "bytea", Nullable: true, TypeMetadata: map[string]string{"nullability_known": "true", "generated_known": "true"}},
		},
	}
}

func managedTestTransaction(schema connector.Schema) connector.SourceTransaction {
	return connector.SourceTransaction{
		SourceLineageID: "lineage-1", TransactionID: 42,
		BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/20",
		Checkpoint: connector.Checkpoint{LSN: "0/20", Timestamp: time.Unix(100, 0).UTC()},
		Fragments: []connector.TransactionFragment{
			{Ordinal: 0, Batch: connector.Batch{Schema: schema, Records: []connector.Record{
				{Table: "widgets", Operation: connector.OpInsert, Key: json.RawMessage(`{"id":1}`), After: map[string]any{"id": int64(1), "value": "first", "payload": []byte{1, 2, 3}}},
				{Table: "widgets", Operation: connector.OpUpdate, Key: json.RawMessage(`{"id":1}`), After: map[string]any{"id": int64(1), "value": "second"}},
			}}},
			{Ordinal: 1, Batch: connector.Batch{Schema: schema, Records: []connector.Record{
				{Table: "widgets", Operation: connector.OpDelete, Key: json.RawMessage(`{"id":1}`)},
			}}},
		},
	}
}

func managedTestIntent(t *testing.T, transaction connector.SourceTransaction) connector.DeliveryIntent {
	t.Helper()
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		t.Fatal(err)
	}
	return connector.DeliveryIntent{
		FlowID: "flow-1", FlowIncarnationID: "11111111-1111-1111-1111-111111111111",
		SourceLineageID: transaction.SourceLineageID, Generation: 1,
		AcquisitionID: "22222222-2222-2222-2222-222222222222", LeaseEpoch: 1,
		DestinationRevisionID: "snowflake-v1", LogicalBatchID: logicalBatchID,
		PositionID: "0/20", ContentHash: contentHash,
	}
}

func mustManagedSchemaHash(t *testing.T, schema connector.Schema) string {
	t.Helper()
	hash, err := ManagedSchemaContractHash(schema)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}
