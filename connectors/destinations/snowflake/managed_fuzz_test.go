package snowflake

import (
	"fmt"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"pgregory.net/rapid"
)

// managedFuzzConfig returns a minimal but internally consistent managed config
// for the constrained Snowflake SQL profile. It reuses managedTestSchema so the
// fuzz and property targets exercise the real admitted relation shape.
func managedFuzzConfig(t testing.TB) managedConfig {
	t.Helper()
	schema := managedTestSchema()
	hash, err := ManagedSchemaContractHash(schema)
	if err != nil {
		t.Fatalf("hash managed test schema: %v", err)
	}
	return managedConfig{
		profile:  connector.ManagedProfilePostgresToSnowflakeSQLV1,
		flowID:   "flow-1",
		database: "DB", schema: "PUBLIC", table: "WIDGETS", receiptsTable: "WALLABY_RECEIPTS",
		sourceSchema: "public", sourceTable: "widgets", schemaContract: schema,
		schemaContractHash: hash, destinationRevision: "snowflake-v1",
		typeMappings:        defaultSnowflakeTypeMappings(),
		maxTransactionRows:  1000,
		maxTransactionBytes: 8 << 20,
		maxFragments:        128,
	}
}

func managedFuzzTarget(cfg managedConfig) string {
	return quoteIdent(cfg.database, '"') + "." + quoteIdent(cfg.schema, '"') + "." + quoteIdent(cfg.table, '"')
}

// managedSQLSafetyReporter is the subset of the test reporting API shared by
// *testing.T, *testing.F, and *rapid.T so one assertion serves both the fuzz
// and rapid property targets.
type managedSQLSafetyReporter interface {
	Helper()
	Fatalf(format string, args ...any)
}

// assertManagedSnowflakeSQLSafe encodes the SQL-injection-safety invariant: every
// operation the planner emits is fully parameterized. The placeholder count must
// match the bound argument count, no string literal quote may appear, and the
// statement must be one of the admitted DML verbs. A user-controlled value can
// therefore never escape into the SQL text.
func assertManagedSnowflakeSQLSafe(t managedSQLSafetyReporter, operation managedSnowflakeOperation) {
	t.Helper()
	query := operation.query
	if placeholders := strings.Count(query, "?"); placeholders != len(operation.args) {
		t.Fatalf("query %q has %d placeholders but %d bound arguments", query, placeholders, len(operation.args))
	}
	if strings.ContainsRune(query, '\'') {
		t.Fatalf("managed Snowflake query interpolated a string literal: %q", query)
	}
	if strings.ContainsRune(query, ';') {
		t.Fatalf("managed Snowflake query contains a statement separator: %q", query)
	}
	if !strings.HasPrefix(query, "INSERT ") && !strings.HasPrefix(query, "UPDATE ") && !strings.HasPrefix(query, "DELETE ") {
		t.Fatalf("managed Snowflake query is not an admitted parameterized DML verb: %q", query)
	}
}

// FuzzManagedSchemaContractHash proves the immutable schema-contract hash never
// panics, is deterministic for identical input, and only ever returns a 64-char
// lowercase hex digest.
func FuzzManagedSchemaContractHash(f *testing.F) {
	f.Add("public", "widgets", "id", "int8")
	f.Add("", "", "", "")
	f.Add("public", "orders", "amount", "numeric(12,2)")
	f.Fuzz(func(t *testing.T, namespace, name, column, columnType string) {
		schema := connector.Schema{
			Namespace: namespace, Name: name,
			Columns: []connector.Column{{
				Name: column, Type: columnType,
				TypeMetadata: map[string]string{"primary_key": "true", "nullability_known": "true", "generated_known": "true"},
			}},
		}
		first, firstErr := ManagedSchemaContractHash(schema)
		second, secondErr := ManagedSchemaContractHash(schema)
		if (firstErr == nil) != (secondErr == nil) {
			t.Fatalf("nondeterministic error: %v vs %v", firstErr, secondErr)
		}
		if firstErr != nil {
			return
		}
		if first != second {
			t.Fatalf("nondeterministic hash: %q vs %q", first, second)
		}
		if len(first) != 64 || strings.ToLower(first) != first {
			t.Fatalf("hash %q is not a 64-char lowercase hex digest", first)
		}
		for _, character := range first {
			if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
				t.Fatalf("hash %q contains a non-hex character", first)
			}
		}
	})
}

// FuzzNormalizeManagedSourceType proves type normalization never panics and is
// idempotent: normalizing an already-normalized type is a fixed point. Idempotence
// is what lets the contract hash and target-schema comparison agree.
func FuzzNormalizeManagedSourceType(f *testing.F) {
	for _, seed := range []string{"int8", "INT4", "numeric(12,2)", "character varying(40)", "timestamptz", "text[]", "  bool ", ""} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, sourceType string) {
		once := normalizeManagedSourceType(sourceType)
		twice := normalizeManagedSourceType(once)
		if once != twice {
			t.Fatalf("normalizeManagedSourceType is not idempotent: %q -> %q -> %q", sourceType, once, twice)
		}
	})
}

// FuzzManagedRecordKey proves record-key decoding never panics on arbitrary bytes
// and, when it succeeds, returns exactly the admitted identity columns.
func FuzzManagedRecordKey(f *testing.F) {
	f.Add([]byte(`{"id":1}`))
	f.Add([]byte(`{"id":"9"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"id":1}{"id":2}`))
	schema := managedTestSchema()
	identity := []string{"id"}
	f.Fuzz(func(t *testing.T, raw []byte) {
		key, err := managedRecordKey(schema, identity, raw)
		if err != nil {
			return
		}
		if len(key) != len(identity) {
			t.Fatalf("decoded key has %d columns, admitted identity has %d", len(key), len(identity))
		}
		if _, ok := key["id"]; !ok {
			t.Fatalf("decoded key %v lacks the identity column", key)
		}
	})
}

// FuzzBuildManagedSnowflakeOperationSQLSafety proves the DML planner never panics
// and never interpolates a user-controlled value into SQL text, across insert,
// update, and delete shapes.
func FuzzBuildManagedSnowflakeOperationSQLSafety(f *testing.F) {
	f.Add(uint8(0), int64(1), "first", "\x01\x02")
	f.Add(uint8(1), int64(2), "second", "")
	f.Add(uint8(2), int64(3), "", "")
	f.Add(uint8(0), int64(-9), "robert'); DROP TABLE widgets;--", "\x00")
	cfg := managedFuzzConfig(f)
	target := managedFuzzTarget(cfg)
	schema := managedTestSchema()
	f.Fuzz(func(t *testing.T, opCode uint8, id int64, value, payload string) {
		key := []byte(fmt.Sprintf(`{"id":%d}`, id))
		var record connector.Record
		switch opCode % 3 {
		case 0:
			record = connector.Record{Table: "widgets", Operation: connector.OpInsert, Key: key,
				After: map[string]any{"id": id, "value": value, "payload": []byte(payload)}}
		case 1:
			record = connector.Record{Table: "widgets", Operation: connector.OpUpdate, Key: key,
				After: map[string]any{"value": value}}
		default:
			record = connector.Record{Table: "widgets", Operation: connector.OpDelete, Key: key}
		}
		operation, err := buildManagedSnowflakeOperation(cfg, target, schema, []string{"id"}, managedOperationIdentity{operation: record.Operation}, record)
		if err != nil {
			return
		}
		assertManagedSnowflakeSQLSafe(t, operation)
	})
}

// TestManagedSnowflakeOperationSQLInjectionSafetyProperty is the bounded rapid
// counterpart to the SQL-safety fuzz target. It draws hostile string values and
// full-range identity keys and asserts every emitted insert stays parameterized.
func TestManagedSnowflakeOperationSQLInjectionSafetyProperty(t *testing.T) {
	t.Parallel()
	cfg := managedFuzzConfig(t)
	target := managedFuzzTarget(cfg)
	schema := managedTestSchema()
	rapid.Check(t, func(t *rapid.T) {
		id := rapid.Int64().Draw(t, "id")
		value := rapid.String().Draw(t, "value")
		payload := rapid.SliceOf(rapid.Byte()).Draw(t, "payload")
		key := []byte(fmt.Sprintf(`{"id":%d}`, id))
		record := connector.Record{Table: "widgets", Operation: connector.OpInsert, Key: key,
			After: map[string]any{"id": id, "value": value, "payload": payload}}
		operation, err := buildManagedSnowflakeOperation(cfg, target, schema, []string{"id"}, managedOperationIdentity{operation: connector.OpInsert}, record)
		if err != nil {
			// A rejected record is acceptable; only an emitted statement must be
			// proven safe. Invalid UTF-8 text, for example, is refused upstream.
			return
		}
		assertManagedSnowflakeSQLSafe(t, operation)
	})
}

// TestManagedSnowflakeSchemaContractHashDeterminismProperty proves the contract
// hash is stable across repeated encoding of the same admitted schema, which is
// what makes the destination manifest and receipt identities reproducible.
func TestManagedSnowflakeSchemaContractHashDeterminismProperty(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		namespace := rapid.StringMatching(`[a-z][a-z0-9_]{0,16}`).Draw(t, "namespace")
		name := rapid.StringMatching(`[a-z][a-z0-9_]{0,16}`).Draw(t, "name")
		column := rapid.StringMatching(`[a-z][a-z0-9_]{0,16}`).Draw(t, "column")
		schema := connector.Schema{
			Namespace: namespace, Name: name,
			Columns: []connector.Column{{
				Name: column, Type: "int8",
				TypeMetadata: map[string]string{"primary_key": "true", "nullability_known": "true", "generated_known": "true"},
			}},
		}
		first, err := ManagedSchemaContractHash(schema)
		if err != nil {
			return
		}
		for range 8 {
			again, err := ManagedSchemaContractHash(schema)
			if err != nil || again != first {
				t.Fatalf("schema contract hash is not stable: %q vs %q err=%v", first, again, err)
			}
		}
	})
}
