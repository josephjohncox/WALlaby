package certify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// TableCertOptions controls data certificate sampling + hashing.
type TableCertOptions struct {
	SampleRate  float64
	SampleLimit int
	PrimaryKeys []string
	Columns     []string
}

// TableCertResult captures a table hash summary.
type TableCertResult struct {
	Table       string    `json:"table"`
	Rows        int64     `json:"rows"`
	Hash        string    `json:"hash"`
	Sampled     bool      `json:"sampled"`
	SampleRate  float64   `json:"sample_rate"`
	SampleLimit int       `json:"sample_limit,omitempty"`
	PrimaryKeys []string  `json:"primary_keys,omitempty"`
	Columns     []string  `json:"columns,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	EndedAt     time.Time `json:"ended_at"`
}

// TableCertReport compares source vs destination.
type TableCertReport struct {
	Table        string          `json:"table"`
	Source       TableCertResult `json:"source"`
	Destination  TableCertResult `json:"destination"`
	Match        bool            `json:"match"`
	HashMatches  bool            `json:"hash_matches"`
	CountMatches bool            `json:"count_matches"`
}

// CertifyPostgresTable computes a data certificate for a Postgres source/destination pair.
func CertifyPostgresTable(ctx context.Context, sourceDSN string, sourceOptions map[string]string, destDSN string, destOptions map[string]string, table string, opts TableCertOptions) (TableCertReport, error) {
	if sourceDSN == "" || destDSN == "" {
		return TableCertReport{}, errors.New("source and destination DSN are required")
	}
	schemaName, tableName, err := splitTableName(table)
	if err != nil {
		return TableCertReport{}, err
	}

	if opts.SampleRate <= 0 {
		opts.SampleRate = 1
	}
	if opts.SampleRate > 1 {
		return TableCertReport{}, fmt.Errorf("sample_rate must be <= 1 (got %.4f)", opts.SampleRate)
	}

	schemaDef, err := postgres.LoadTableSchema(ctx, sourceDSN, schemaName, tableName, sourceOptions)
	if err != nil {
		return TableCertReport{}, err
	}
	columns := resolveColumns(schemaDef, opts.Columns)
	if len(columns) == 0 {
		return TableCertReport{}, errors.New("no columns available for certificate")
	}

	primaryKeys := opts.PrimaryKeys
	if len(primaryKeys) == 0 {
		keys, err := postgres.LoadPrimaryKeys(ctx, sourceDSN, schemaName, tableName, sourceOptions)
		if err != nil {
			return TableCertReport{}, err
		}
		primaryKeys = keys
	}
	if opts.SampleRate < 1 && len(primaryKeys) == 0 {
		return TableCertReport{}, errors.New("sample_rate requires primary keys or explicit -primary-keys")
	}

	sourceResult, err := certifyPostgres(ctx, sourceDSN, sourceOptions, schemaDef, schemaName, tableName, columns, primaryKeys, opts)
	if err != nil {
		return TableCertReport{}, err
	}
	destResult, err := certifyPostgres(ctx, destDSN, destOptions, schemaDef, schemaName, tableName, columns, primaryKeys, opts)
	if err != nil {
		return TableCertReport{}, err
	}

	report := TableCertReport{
		Table:        schemaName + "." + tableName,
		Source:       sourceResult,
		Destination:  destResult,
		HashMatches:  sourceResult.Hash == destResult.Hash,
		CountMatches: sourceResult.Rows == destResult.Rows,
	}
	report.Match = report.HashMatches && report.CountMatches
	return report, nil
}

func certifyPostgres(ctx context.Context, dsn string, options map[string]string, schema connector.Schema, schemaName, tableName string, columns, primaryKeys []string, opts TableCertOptions) (TableCertResult, error) {
	pool, err := postgres.OpenPool(ctx, dsn, options)
	if err != nil {
		return TableCertResult{}, err
	}
	defer pool.Close()

	started := time.Now().UTC()
	query, err := buildSelectQuery(schemaName, tableName, columns, primaryKeys, opts)
	if err != nil {
		return TableCertResult{}, err
	}
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return TableCertResult{}, fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()

	hashCtx := sha256.New()
	var count int64
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return TableCertResult{}, fmt.Errorf("read row: %w", err)
		}
		rowHash, err := hashRow(schema, columns, values)
		if err != nil {
			return TableCertResult{}, fmt.Errorf("hash row: %w", err)
		}
		hashCtx.Write(rowHash)
		count++
	}
	if err := rows.Err(); err != nil {
		return TableCertResult{}, fmt.Errorf("iterate rows: %w", err)
	}

	return TableCertResult{
		Table:       schemaName + "." + tableName,
		Rows:        count,
		Hash:        hex.EncodeToString(hashCtx.Sum(nil)),
		Sampled:     opts.SampleRate < 1 || opts.SampleLimit > 0,
		SampleRate:  opts.SampleRate,
		SampleLimit: opts.SampleLimit,
		PrimaryKeys: append([]string(nil), primaryKeys...),
		Columns:     append([]string(nil), columns...),
		StartedAt:   started,
		EndedAt:     time.Now().UTC(),
	}, nil
}

func resolveColumns(schema connector.Schema, override []string) []string {
	if len(override) == 0 {
		cols := make([]string, 0, len(schema.Columns))
		for _, col := range schema.Columns {
			cols = append(cols, col.Name)
		}
		return cols
	}
	known := make(map[string]struct{}, len(schema.Columns))
	for _, col := range schema.Columns {
		known[col.Name] = struct{}{}
	}
	out := make([]string, 0, len(override))
	for _, name := range override {
		if _, ok := known[name]; !ok {
			continue
		}
		out = append(out, name)
	}
	return out
}

func buildSelectQuery(schemaName, tableName string, columns, primaryKeys []string, opts TableCertOptions) (string, error) {
	if len(columns) == 0 {
		return "", errors.New("columns are required")
	}
	selectCols := make([]string, 0, len(columns))
	for _, col := range columns {
		selectCols = append(selectCols, pgx.Identifier{col}.Sanitize())
	}
	tableIdent := pgx.Identifier{schemaName, tableName}.Sanitize()
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectCols, ", "), tableIdent)

	if opts.SampleRate < 1 {
		predicate, err := samplePredicate(primaryKeys, opts.SampleRate)
		if err != nil {
			return "", err
		}
		query += " WHERE " + predicate
	}

	orderCols := primaryKeys
	if len(orderCols) == 0 {
		orderCols = columns
	}
	orderParts := make([]string, 0, len(orderCols))
	for _, col := range orderCols {
		orderParts = append(orderParts, pgx.Identifier{col}.Sanitize())
	}
	query += " ORDER BY " + strings.Join(orderParts, ", ")

	if opts.SampleLimit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.SampleLimit)
	}
	return query, nil
}

func samplePredicate(primaryKeys []string, sampleRate float64) (string, error) {
	if len(primaryKeys) == 0 {
		return "", errors.New("primary keys required for sampling")
	}
	if sampleRate <= 0 || sampleRate > 1 {
		return "", fmt.Errorf("sample_rate must be between 0 and 1 (got %.4f)", sampleRate)
	}
	scale := 1000000.0
	threshold := int(sampleRate * scale)
	if threshold < 1 {
		threshold = 1
	}
	parts := make([]string, 0, len(primaryKeys))
	for _, col := range primaryKeys {
		ident := pgx.Identifier{col}.Sanitize()
		parts = append(parts, fmt.Sprintf("COALESCE(%s::text, '')", ident))
	}
	expr := fmt.Sprintf("mod(abs(hashtext(concat_ws('|', %s))), %d) < %d", strings.Join(parts, ", "), int(scale), threshold)
	return expr, nil
}

func splitTableName(table string) (string, string, error) {
	parts := strings.Split(table, ".")
	if len(parts) == 1 {
		return "public", parts[0], nil
	}
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}
	return "", "", fmt.Errorf("invalid table name %q", table)
}

func hashRow(schema connector.Schema, columns []string, values []any) ([]byte, error) {
	row := make(map[string]any, len(columns))
	for i, col := range columns {
		if i < len(values) {
			row[col] = values[i]
		}
	}
	if err := connector.NormalizePostgresRecord(schema, row); err != nil {
		return nil, err
	}
	h := sha256.New()
	for _, col := range columns {
		writeString(h, col)
		if err := hashValue(h, row[col]); err != nil {
			return nil, err
		}
	}
	return h.Sum(nil), nil
}

func hashValue(h hash.Hash, value any) error {
	switch v := value.(type) {
	case nil:
		h.Write([]byte("n"))
		return nil
	case bool:
		if v {
			h.Write([]byte("b1"))
		} else {
			h.Write([]byte("b0"))
		}
		return nil
	case string:
		writeString(h, "s")
		writeString(h, v)
		return nil
	case json.RawMessage:
		if len(v) == 0 {
			return hashValue(h, nil)
		}
		var decoded any
		if err := json.Unmarshal(v, &decoded); err != nil {
			writeString(h, "raw")
			writeString(h, hex.EncodeToString(v))
			return nil //nolint:nilerr // fallback to raw JSON bytes when decoding fails
		}
		writeString(h, "json")
		return hashValue(h, decoded)
	case []byte:
		writeString(h, "x")
		writeBytes(h, v)
		return nil
	case []string:
		writeString(h, "as")
		for _, item := range v {
			if err := hashValue(h, item); err != nil {
				return err
			}
		}
		return nil
	case []any:
		writeString(h, "a")
		for _, item := range v {
			if err := hashValue(h, item); err != nil {
				return err
			}
		}
		return nil
	case time.Time:
		writeString(h, "t")
		writeString(h, v.UTC().Format(time.RFC3339Nano))
		return nil
	case json.Number:
		writeString(h, "num")
		writeString(h, string(v))
		return nil
	case fmt.Stringer:
		writeString(h, "str")
		writeString(h, v.String())
		return nil
	case jsonNumber:
		writeString(h, "num")
		writeString(h, string(v))
		return nil
	case map[string]any:
		writeString(h, "m")
		return hashMap(h, v)
	case map[string]string:
		writeString(h, "ms")
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			writeString(h, key)
			if err := hashValue(h, v[key]); err != nil {
				return err
			}
		}
		return nil
	}

	switch v := value.(type) {
	case float32:
		writeString(h, "f")
		writeString(h, strconvFormatFloat(float64(v)))
		return nil
	case float64:
		writeString(h, "f")
		writeString(h, strconvFormatFloat(v))
		return nil
	case int:
		writeString(h, "i")
		writeString(h, strconvFormatInt(int64(v)))
		return nil
	case int8:
		writeString(h, "i")
		writeString(h, strconvFormatInt(int64(v)))
		return nil
	case int16:
		writeString(h, "i")
		writeString(h, strconvFormatInt(int64(v)))
		return nil
	case int32:
		writeString(h, "i")
		writeString(h, strconvFormatInt(int64(v)))
		return nil
	case int64:
		writeString(h, "i")
		writeString(h, strconvFormatInt(v))
		return nil
	case uint:
		writeString(h, "u")
		writeString(h, strconvFormatUint(uint64(v)))
		return nil
	case uint8:
		writeString(h, "u")
		writeString(h, strconvFormatUint(uint64(v)))
		return nil
	case uint16:
		writeString(h, "u")
		writeString(h, strconvFormatUint(uint64(v)))
		return nil
	case uint32:
		writeString(h, "u")
		writeString(h, strconvFormatUint(uint64(v)))
		return nil
	case uint64:
		writeString(h, "u")
		writeString(h, strconvFormatUint(v))
		return nil
	}

	return hashReflect(h, value)
}
