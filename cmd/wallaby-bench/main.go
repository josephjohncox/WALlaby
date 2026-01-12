package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/connectors/destinations/clickhouse"
	"github.com/josephjohncox/wallaby/connectors/destinations/kafka"
	pgdest "github.com/josephjohncox/wallaby/connectors/destinations/postgres"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/ddl"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type profile struct {
	Name         string
	InitialRows  int
	Operations   int
	Writers      int
	BatchSize    int
	BatchTimeout time.Duration
	EmptyReads   int
}

type tableSpec struct {
	Name        string
	CreateSQL   string
	InsertSQL   string
	UpdateSQL   string
	DeleteSQL   string
	Columns     []string
	SeedRow     func(id int64) []any
	InsertArgs  func(id int64, now time.Time) []any
	UpdateArgs  func(id int64, now time.Time) []any
	RowSizeByte int64
}

type benchData struct {
	NarrowPayload string
	WidePayload   string
	WideNotes     string
	WideMeta      string
	JSONPayload   string
	JSONTags      string
	JSONEvents    string
	JSONMeta      string
}

type tableState struct {
	spec   tableSpec
	mu     sync.Mutex
	ids    []int64
	nextID int64
}

type benchStats struct {
	mu        sync.Mutex
	start     time.Time
	end       time.Time
	records   int64
	bytes     int64
	latencies []int64
}

type benchResult struct {
	Target        string  `json:"target"`
	Profile       string  `json:"profile"`
	Scenario      string  `json:"scenario"`
	Records       int64   `json:"records"`
	Bytes         int64   `json:"bytes"`
	DurationSec   float64 `json:"duration_sec"`
	RecordsPerSec float64 `json:"records_per_sec"`
	MBPerSec      float64 `json:"mb_per_sec"`
	LatencyP50Ms  float64 `json:"latency_p50_ms"`
	LatencyP95Ms  float64 `json:"latency_p95_ms"`
	LatencyP99Ms  float64 `json:"latency_p99_ms"`
	StartedAt     string  `json:"started_at"`
	EndedAt       string  `json:"ended_at"`
}

type metricsDestination struct {
	inner    connector.Destination
	stats    *benchStats
	rowSizes map[string]int64
}

func (m *metricsDestination) Open(ctx context.Context, spec connector.Spec) error {
	return m.inner.Open(ctx, spec)
}

func (m *metricsDestination) Write(ctx context.Context, batch connector.Batch) error {
	if err := m.inner.Write(ctx, batch); err != nil {
		return err
	}
	m.stats.record(batch, m.rowSizes)
	return nil
}

func (m *metricsDestination) ApplyDDL(ctx context.Context, schema connector.Schema, record connector.Record) error {
	return m.inner.ApplyDDL(ctx, schema, record)
}

func (m *metricsDestination) TypeMappings() map[string]string {
	return m.inner.TypeMappings()
}

func (m *metricsDestination) Close(ctx context.Context) error {
	return m.inner.Close(ctx)
}

func (m *metricsDestination) Capabilities() connector.Capabilities {
	return m.inner.Capabilities()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var (
		profileName  = flag.String("profile", "small", "profile: small|medium|large")
		targetsRaw   = flag.String("targets", "all", "targets: all|kafka,postgres,clickhouse")
		scenario     = flag.String("scenario", "base", "scenario: base|ddl")
		pgDSN        = flag.String("pg-dsn", getenv("BENCH_PG_DSN", "postgres://postgres:postgres@localhost:5432/wallaby?sslmode=disable"), "postgres DSN")
		ckDSN        = flag.String("clickhouse-dsn", getenv("BENCH_CLICKHOUSE_DSN", "clickhouse://bench:bench@localhost:9000/bench"), "clickhouse DSN")
		kafkaBrokers = flag.String("kafka-brokers", getenv("BENCH_KAFKA_BROKERS", "localhost:9092"), "kafka brokers")
		outputDir    = flag.String("output-dir", "bench/results", "output directory for results")
		seed         = flag.Int64("seed", 42, "random seed")
		cpuProfile   = flag.String("cpu-profile", "", "write CPU profile to file")
		memProfile   = flag.String("mem-profile", "", "write heap profile to file")
		traceProfile = flag.String("trace", "", "write execution trace to file")
	)
	flag.Parse()

	if *cpuProfile != "" {
		// #nosec G304 -- profile path comes from CLI flag.
		file, err := os.Create(*cpuProfile)
		if err != nil {
			return fmt.Errorf("create cpu profile: %w", err)
		}
		if err := pprof.StartCPUProfile(file); err != nil {
			if closeErr := file.Close(); closeErr != nil {
				fmt.Fprintf(os.Stderr, "close cpu profile: %v\n", closeErr)
			}
			return fmt.Errorf("start cpu profile: %w", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			if err := file.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "close cpu profile: %v\n", err)
			}
		}()
	}

	if *traceProfile != "" {
		// #nosec G304 -- trace path comes from CLI flag.
		file, err := os.Create(*traceProfile)
		if err != nil {
			return fmt.Errorf("create trace: %w", err)
		}
		if err := trace.Start(file); err != nil {
			if closeErr := file.Close(); closeErr != nil {
				fmt.Fprintf(os.Stderr, "close trace: %v\n", closeErr)
			}
			return fmt.Errorf("start trace: %w", err)
		}
		defer func() {
			trace.Stop()
			if err := file.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "close trace: %v\n", err)
			}
		}()
	}

	if *memProfile != "" {
		path := *memProfile
		defer func() {
			// #nosec G304 -- profile path comes from CLI flag.
			file, err := os.Create(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "create heap profile: %v\n", err)
				return
			}
			defer func() {
				if err := file.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "close heap profile: %v\n", err)
				}
			}()
			runtime.GC()
			if err := pprof.WriteHeapProfile(file); err != nil {
				fmt.Fprintf(os.Stderr, "write heap profile: %v\n", err)
			}
		}()
	}

	profile, err := resolveProfile(*profileName)
	if err != nil {
		return err
	}
	if *scenario != "base" && *scenario != "ddl" {
		return fmt.Errorf("unsupported scenario %q", *scenario)
	}

	targets, err := resolveTargets(*targetsRaw)
	if err != nil {
		return err
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *pgDSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	data := buildBenchData(*seed)
	specs := buildTableSpecs("bench_src", data)
	rowSizes := map[string]int64{}
	for _, spec := range specs {
		rowSizes[tableName(spec.Name)] = spec.RowSizeByte
	}

	if err := setupPostgresSchemas(ctx, pool, specs); err != nil {
		return err
	}

	if err := seedTables(ctx, pool, specs, profile.InitialRows); err != nil {
		return err
	}

	if hasTarget(targets, "postgres") {
		if err := setupPostgresSink(ctx, pool, specs); err != nil {
			return err
		}
	}
	if hasTarget(targets, "clickhouse") {
		if err := setupClickHouseSink(ctx, *ckDSN, specs); err != nil {
			return err
		}
	}

	results := make([]benchResult, 0, len(targets))
	for _, target := range targets {
		result, err := runTarget(ctx, target, profile, *scenario, pool, *pgDSN, *ckDSN, *kafkaBrokers, specs, rowSizes)
		if err != nil {
			return err
		}
		results = append(results, result)
	}

	if err := writeResults(*outputDir, results); err != nil {
		return err
	}
	return nil
}

func resolveProfile(name string) (profile, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "small":
		return profile{
			Name:         "small",
			InitialRows:  10000,
			Operations:   50000,
			Writers:      4,
			BatchSize:    500,
			BatchTimeout: 200 * time.Millisecond,
			EmptyReads:   20,
		}, nil
	case "medium":
		return profile{
			Name:         "medium",
			InitialRows:  50000,
			Operations:   250000,
			Writers:      8,
			BatchSize:    1000,
			BatchTimeout: 200 * time.Millisecond,
			EmptyReads:   25,
		}, nil
	case "large":
		return profile{
			Name:         "large",
			InitialRows:  200000,
			Operations:   1000000,
			Writers:      16,
			BatchSize:    2000,
			BatchTimeout: 200 * time.Millisecond,
			EmptyReads:   30,
		}, nil
	default:
		return profile{}, fmt.Errorf("unknown profile %q", name)
	}
}

func resolveTargets(raw string) ([]string, error) {
	trim := strings.ToLower(strings.TrimSpace(raw))
	if trim == "" || trim == "all" {
		return []string{"kafka", "postgres", "clickhouse"}, nil
	}
	parts := strings.Split(trim, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		switch item {
		case "kafka", "postgres", "clickhouse":
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			out = append(out, item)
		default:
			return nil, fmt.Errorf("unsupported target %q", item)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no valid targets")
	}
	return out, nil
}

func hasTarget(targets []string, needle string) bool {
	for _, target := range targets {
		if target == needle {
			return true
		}
	}
	return false
}

func buildBenchData(seed int64) benchData {
	// #nosec G404 -- deterministic RNG for benchmarks.
	rng := rand.New(rand.NewSource(seed))

	narrowBlob := randString(rng, 900)
	wideBlob := randString(rng, 4096)
	wideNotes := randString(rng, 512)
	wideMetaObj := map[string]any{
		"tags": []any{"a", "b", "c", "d", "e"},
		"blob": randString(rng, 256),
	}
	wideMetaBytes, _ := json.Marshal(wideMetaObj)

	jsonPayloadObj := map[string]any{
		"blob":  randString(rng, 8192),
		"items": buildStringArray(rng, 64, 64),
	}
	jsonTagsObj := map[string]any{
		"tags": buildStringArray(rng, 128, 12),
	}
	jsonEventsObj := map[string]any{
		"events": buildEventArray(rng, 64),
	}
	jsonMetaObj := map[string]any{
		"meta": map[string]any{
			"source": "bench",
			"flags":  buildStringArray(rng, 32, 8),
		},
	}

	jsonPayloadBytes, _ := json.Marshal(jsonPayloadObj)
	jsonTagsBytes, _ := json.Marshal(jsonTagsObj)
	jsonEventsBytes, _ := json.Marshal(jsonEventsObj)
	jsonMetaBytes, _ := json.Marshal(jsonMetaObj)

	narrowPayloadObj := map[string]any{
		"blob": narrowBlob,
		"tags": []any{"small", "row"},
	}
	narrowPayloadBytes, _ := json.Marshal(narrowPayloadObj)

	return benchData{
		NarrowPayload: string(narrowPayloadBytes),
		WidePayload:   wideBlob,
		WideNotes:     wideNotes,
		WideMeta:      string(wideMetaBytes),
		JSONPayload:   string(jsonPayloadBytes),
		JSONTags:      string(jsonTagsBytes),
		JSONEvents:    string(jsonEventsBytes),
		JSONMeta:      string(jsonMetaBytes),
	}
}

func buildTableSpecs(schema string, data benchData) []tableSpec {
	narrow := tableSpec{
		Name: fmt.Sprintf("%s.narrow", schema),
		CreateSQL: fmt.Sprintf(`CREATE TABLE %s.narrow (
  id BIGINT PRIMARY KEY,
  account_id BIGINT,
  status TEXT,
  amount NUMERIC(12,2),
  payload JSONB,
  updated_at TIMESTAMPTZ
)`, schema),
		InsertSQL: fmt.Sprintf(`INSERT INTO %s.narrow (id, account_id, status, amount, payload, updated_at)
VALUES ($1, $2, $3, $4, CAST($5 AS JSONB), $6)`, schema),
		UpdateSQL: fmt.Sprintf(`UPDATE %s.narrow SET status = $2, amount = $3, payload = CAST($4 AS JSONB), updated_at = $5 WHERE id = $1`, schema),
		DeleteSQL: fmt.Sprintf(`DELETE FROM %s.narrow WHERE id = $1`, schema),
		Columns:   []string{"id", "account_id", "status", "amount", "payload", "updated_at"},
		SeedRow: func(id int64) []any {
			return []any{id, id % 1000, "active", float64(id%10000) / 100, data.NarrowPayload, time.Now().UTC()}
		},
		InsertArgs: func(id int64, now time.Time) []any {
			return []any{id, id % 1000, "active", float64(id%10000) / 100, data.NarrowPayload, now}
		},
		UpdateArgs: func(id int64, now time.Time) []any {
			return []any{id, "active", float64(id%10000) / 100, data.NarrowPayload, now}
		},
		RowSizeByte: 1024,
	}

	wide := tableSpec{
		Name: fmt.Sprintf("%s.wide", schema),
		CreateSQL: fmt.Sprintf(`CREATE TABLE %s.wide (
  id BIGINT PRIMARY KEY,
  account_id BIGINT,
  status TEXT,
  amount NUMERIC(12,2),
  payload TEXT,
  notes TEXT,
  meta JSONB,
  col_a TEXT,
  col_b TEXT,
  col_c TEXT,
  col_d TEXT,
  updated_at TIMESTAMPTZ
)`, schema),
		InsertSQL: fmt.Sprintf(`INSERT INTO %s.wide (id, account_id, status, amount, payload, notes, meta, col_a, col_b, col_c, col_d, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, CAST($7 AS JSONB), $8, $9, $10, $11, $12)`, schema),
		UpdateSQL: fmt.Sprintf(`UPDATE %s.wide SET status = $2, amount = $3, payload = $4, notes = $5, meta = CAST($6 AS JSONB), col_a = $7, col_b = $8, col_c = $9, col_d = $10, updated_at = $11 WHERE id = $1`, schema),
		DeleteSQL: fmt.Sprintf(`DELETE FROM %s.wide WHERE id = $1`, schema),
		Columns:   []string{"id", "account_id", "status", "amount", "payload", "notes", "meta", "col_a", "col_b", "col_c", "col_d", "updated_at"},
		SeedRow: func(id int64) []any {
			return []any{id, id % 500, "active", float64(id%10000) / 100, data.WidePayload, data.WideNotes, data.WideMeta, "a", "b", "c", "d", time.Now().UTC()}
		},
		InsertArgs: func(id int64, now time.Time) []any {
			return []any{id, id % 500, "active", float64(id%10000) / 100, data.WidePayload, data.WideNotes, data.WideMeta, "a", "b", "c", "d", now}
		},
		UpdateArgs: func(id int64, now time.Time) []any {
			return []any{id, "active", float64(id%10000) / 100, data.WidePayload, data.WideNotes, data.WideMeta, "a", "b", "c", "d", now}
		},
		RowSizeByte: 5120,
	}

	jsonHeavy := tableSpec{
		Name: fmt.Sprintf("%s.json_heavy", schema),
		CreateSQL: fmt.Sprintf(`CREATE TABLE %s.json_heavy (
  id BIGINT PRIMARY KEY,
  payload JSONB,
  tags JSONB,
  events JSONB,
  metadata JSONB,
  updated_at TIMESTAMPTZ
)`, schema),
		InsertSQL: fmt.Sprintf(`INSERT INTO %s.json_heavy (id, payload, tags, events, metadata, updated_at)
VALUES ($1, CAST($2 AS JSONB), CAST($3 AS JSONB), CAST($4 AS JSONB), CAST($5 AS JSONB), $6)`, schema),
		UpdateSQL: fmt.Sprintf(`UPDATE %s.json_heavy SET payload = CAST($2 AS JSONB), tags = CAST($3 AS JSONB), events = CAST($4 AS JSONB), metadata = CAST($5 AS JSONB), updated_at = $6 WHERE id = $1`, schema),
		DeleteSQL: fmt.Sprintf(`DELETE FROM %s.json_heavy WHERE id = $1`, schema),
		Columns:   []string{"id", "payload", "tags", "events", "metadata", "updated_at"},
		SeedRow: func(id int64) []any {
			return []any{id, data.JSONPayload, data.JSONTags, data.JSONEvents, data.JSONMeta, time.Now().UTC()}
		},
		InsertArgs: func(id int64, now time.Time) []any {
			return []any{id, data.JSONPayload, data.JSONTags, data.JSONEvents, data.JSONMeta, now}
		},
		UpdateArgs: func(id int64, now time.Time) []any {
			return []any{id, data.JSONPayload, data.JSONTags, data.JSONEvents, data.JSONMeta, now}
		},
		RowSizeByte: 20480,
	}

	return []tableSpec{narrow, wide, jsonHeavy}
}

func setupPostgresSchemas(ctx context.Context, pool *pgxpool.Pool, specs []tableSpec) error {
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS bench_src"); err != nil {
		return fmt.Errorf("create source schema: %w", err)
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS bench_sink"); err != nil {
		return fmt.Errorf("create sink schema: %w", err)
	}
	for _, spec := range specs {
		if _, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", spec.Name)); err != nil {
			return fmt.Errorf("drop table %s: %w", spec.Name, err)
		}
		if _, err := pool.Exec(ctx, spec.CreateSQL); err != nil {
			return fmt.Errorf("create table %s: %w", spec.Name, err)
		}
	}
	return nil
}

func seedTables(ctx context.Context, pool *pgxpool.Pool, specs []tableSpec, rows int) error {
	if rows <= 0 {
		return nil
	}
	batchSize := 1000
	for _, spec := range specs {
		for offset := 0; offset < rows; offset += batchSize {
			limit := batchSize
			if remaining := rows - offset; remaining < limit {
				limit = remaining
			}
			batch := make([][]any, 0, limit)
			for i := 0; i < limit; i++ {
				id := int64(offset + i + 1)
				batch = append(batch, spec.SeedRow(id))
			}
			_, err := pool.CopyFrom(ctx, pgx.Identifier{schemaName(spec.Name), tableName(spec.Name)}, spec.Columns, pgx.CopyFromRows(batch))
			if err != nil {
				return fmt.Errorf("seed table %s: %w", spec.Name, err)
			}
		}
	}
	return nil
}

func setupPostgresSink(ctx context.Context, pool *pgxpool.Pool, specs []tableSpec) error {
	for _, spec := range specs {
		sinkName := strings.Replace(spec.Name, "bench_src", "bench_sink", 1)
		if _, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", sinkName)); err != nil {
			return fmt.Errorf("drop sink table %s: %w", sinkName, err)
		}
		if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL)", sinkName, spec.Name)); err != nil {
			return fmt.Errorf("create sink table %s: %w", sinkName, err)
		}
	}
	return nil
}

func setupClickHouseSink(ctx context.Context, dsn string, specs []tableSpec) error {
	if dsn == "" {
		return nil
	}
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("open clickhouse: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close clickhouse db: %v\n", err)
		}
	}()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping clickhouse: %w", err)
	}

	if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS bench"); err != nil {
		return fmt.Errorf("create clickhouse database: %w", err)
	}

	for _, spec := range specs {
		stmt := strings.Replace(spec.CreateSQL, "bench_src", "bench", 1)
		translated, err := ddl.TranslatePostgresDDL(
			stmt,
			ddl.DialectConfigFor(ddl.DialectClickHouse),
			(&clickhouse.Destination{}).TypeMappings(),
		)
		if err != nil {
			return fmt.Errorf("translate clickhouse ddl: %w", err)
		}
		for _, query := range translated {
			if strings.TrimSpace(query) == "" {
				continue
			}
			if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", "bench", tableName(spec.Name))); err != nil {
				return fmt.Errorf("drop clickhouse table: %w", err)
			}
			if _, err := db.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("create clickhouse table: %w", err)
			}
		}
	}
	return nil
}

func resetSourceTables(ctx context.Context, pool *pgxpool.Pool, specs []tableSpec, rows int) error {
	for _, spec := range specs {
		if _, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", spec.Name)); err != nil {
			return fmt.Errorf("truncate source table %s: %w", spec.Name, err)
		}
	}
	return seedTables(ctx, pool, specs, rows)
}

func truncatePostgresSink(ctx context.Context, pool *pgxpool.Pool, specs []tableSpec) error {
	for _, spec := range specs {
		sinkName := strings.Replace(spec.Name, "bench_src", "bench_sink", 1)
		if _, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", sinkName)); err != nil {
			return fmt.Errorf("truncate sink table %s: %w", sinkName, err)
		}
	}
	return nil
}

func truncateClickHouseSink(ctx context.Context, dsn string, specs []tableSpec) error {
	if dsn == "" {
		return nil
	}
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("open clickhouse: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close clickhouse db: %v\n", err)
		}
	}()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping clickhouse: %w", err)
	}
	for _, spec := range specs {
		query := fmt.Sprintf("TRUNCATE TABLE bench.%s", tableName(spec.Name))
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("truncate clickhouse table: %w", err)
		}
	}
	return nil
}

func ensureKafkaTopic(ctx context.Context, brokers []string, topic string) error {
	if len(brokers) == 0 {
		return errors.New("kafka brokers are required")
	}
	if strings.TrimSpace(topic) == "" {
		return errors.New("kafka topic is required")
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation())
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	_, err = admin.CreateTopic(ctx, 1, 1, nil, topic)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create kafka topic %s: %w", topic, err)
	}
	return nil
}

func runTarget(ctx context.Context, target string, prof profile, scenario string, pool *pgxpool.Pool, pgDSN, ckDSN, kafkaBrokers string, specs []tableSpec, rowSizes map[string]int64) (benchResult, error) {
	if err := resetSourceTables(ctx, pool, specs, prof.InitialRows); err != nil {
		return benchResult{}, err
	}
	if target == "postgres" {
		if err := truncatePostgresSink(ctx, pool, specs); err != nil {
			return benchResult{}, err
		}
	}
	if target == "clickhouse" {
		if err := truncateClickHouseSink(ctx, ckDSN, specs); err != nil {
			return benchResult{}, err
		}
	}

	batchSize := prof.BatchSize
	batchTimeout := prof.BatchTimeout
	if target == "kafka" {
		if batchSize > 25 {
			batchSize = 25
		}
		if batchTimeout > 200*time.Millisecond {
			batchTimeout = 200 * time.Millisecond
		}
	}

	sourceSpec := connector.Spec{
		Name: "bench-source",
		Type: connector.EndpointPostgres,
		Options: map[string]string{
			"dsn":                   pgDSN,
			"slot":                  fmt.Sprintf("bench_%s_%s", target, prof.Name),
			"publication":           "wallaby_bench",
			"publication_schemas":   "bench_src",
			"ensure_publication":    "true",
			"sync_publication":      "true",
			"sync_publication_mode": "sync",
			"batch_size":            fmt.Sprintf("%d", batchSize),
			"batch_timeout":         batchTimeout.String(),
			"emit_empty":            "true",
			"resolve_types":         "true",
		},
	}
	startLSN, err := currentLSN(ctx, pool)
	if err != nil {
		return benchResult{}, err
	}
	sourceSpec.Options["start_lsn"] = startLSN

	topicSuffix := fmt.Sprintf("%s_%d", prof.Name, time.Now().UnixNano())
	destSpec, dest, err := buildDestination(target, pgDSN, ckDSN, kafkaBrokers, topicSuffix)
	if err != nil {
		return benchResult{}, err
	}
	if target == "kafka" {
		brokers := splitCSV(kafkaBrokers)
		if err := ensureKafkaTopic(ctx, brokers, destSpec.Options["topic"]); err != nil {
			return benchResult{}, err
		}
	}

	stats := &benchStats{}
	metricsDest := &metricsDestination{inner: dest, stats: stats, rowSizes: rowSizes}

	runner := &stream.Runner{
		Source:        &pgsource.Source{},
		SourceSpec:    sourceSpec,
		Destinations:  []stream.DestinationConfig{{Spec: destSpec, Dest: metricsDest}},
		MaxEmptyReads: prof.EmptyReads,
		WireFormat:    connector.WireFormatArrow,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	// allow the source to initialize
	time.Sleep(1 * time.Second)

	tables := buildTableStates(specs, prof.InitialRows)
	workloadErr := make(chan error, 1)
	go func() {
		workloadErr <- runWorkload(ctx, pool, tables, prof, scenario)
	}()

	if err := <-workloadErr; err != nil {
		return benchResult{}, err
	}

	err = <-errCh
	if err != nil {
		return benchResult{}, err
	}

	result := stats.snapshot(target, prof, scenario)
	return result, nil
}

func buildDestination(target, pgDSN, ckDSN, kafkaBrokers, topicSuffix string) (connector.Spec, connector.Destination, error) {
	switch target {
	case "kafka":
		spec := connector.Spec{
			Name: "bench-kafka",
			Type: connector.EndpointKafka,
			Options: map[string]string{
				"brokers":     kafkaBrokers,
				"topic":       fmt.Sprintf("wallaby_bench_%s", topicSuffix),
				"format":      string(connector.WireFormatArrow),
				"compression": "lz4",
			},
		}
		return spec, &kafka.Destination{}, nil
	case "postgres":
		spec := connector.Spec{
			Name: "bench-postgres",
			Type: connector.EndpointPostgres,
			Options: map[string]string{
				"dsn":                pgDSN,
				"schema":             "bench_sink",
				"write_mode":         "target",
				"meta_table_enabled": "false",
				"synchronous_commit": getenv("BENCH_PG_SYNC_COMMIT", "off"),
			},
		}
		return spec, &pgdest.Destination{}, nil
	case "clickhouse":
		writeMode := getenv("BENCH_CLICKHOUSE_WRITE_MODE", "append")
		spec := connector.Spec{
			Name: "bench-clickhouse",
			Type: connector.EndpointClickHouse,
			Options: map[string]string{
				"dsn":                ckDSN,
				"database":           "bench",
				"write_mode":         writeMode,
				"meta_table_enabled": "false",
			},
		}
		return spec, &clickhouse.Destination{}, nil
	default:
		return connector.Spec{}, nil, fmt.Errorf("unsupported target %q", target)
	}
}

func buildTableStates(specs []tableSpec, initialRows int) []*tableState {
	states := make([]*tableState, 0, len(specs))
	for _, spec := range specs {
		ids := make([]int64, 0, initialRows)
		for i := 1; i <= initialRows; i++ {
			ids = append(ids, int64(i))
		}
		states = append(states, &tableState{
			spec:   spec,
			ids:    ids,
			nextID: int64(initialRows),
		})
	}
	return states
}

func runWorkload(ctx context.Context, pool *pgxpool.Pool, tables []*tableState, prof profile, scenario string) error {
	if len(tables) == 0 || prof.Operations <= 0 {
		return nil
	}

	var counter int64
	workers := prof.Writers
	if workers < 1 {
		workers = 1
	}

	if scenario == "ddl" {
		go func() {
			_ = runDDLChurn(ctx, pool)
		}()
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// #nosec G404 -- RNG used for benchmark load shaping.
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for {
				idx := atomic.AddInt64(&counter, 1)
				if idx > int64(prof.Operations) {
					return
				}
				table := tables[rng.Intn(len(tables))]
				if err := execOperation(ctx, pool, table, rng); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}(w)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func execOperation(ctx context.Context, pool *pgxpool.Pool, table *tableState, rng *rand.Rand) error {
	op := rng.Intn(100)
	now := time.Now().UTC()

	switch {
	case op < 20: // insert
		id := table.insertID()
		_, err := pool.Exec(ctx, table.spec.InsertSQL, table.spec.InsertArgs(id, now)...)
		return err
	case op < 90: // update
		id, ok := table.randomID(rng)
		if !ok {
			id = table.insertID()
			_, err := pool.Exec(ctx, table.spec.InsertSQL, table.spec.InsertArgs(id, now)...)
			return err
		}
		_, err := pool.Exec(ctx, table.spec.UpdateSQL, table.spec.UpdateArgs(id, now)...)
		return err
	default: // delete
		id, ok := table.deleteRandomID(rng)
		if !ok {
			id = table.insertID()
			_, err := pool.Exec(ctx, table.spec.InsertSQL, table.spec.InsertArgs(id, now)...)
			return err
		}
		_, err := pool.Exec(ctx, table.spec.DeleteSQL, id)
		return err
	}
}

func (t *tableState) randomID(rng *rand.Rand) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.ids) == 0 {
		return 0, false
	}
	return t.ids[rng.Intn(len(t.ids))], true
}

func (t *tableState) deleteRandomID(rng *rand.Rand) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.ids) == 0 {
		return 0, false
	}
	idx := rng.Intn(len(t.ids))
	id := t.ids[idx]
	t.ids[idx] = t.ids[len(t.ids)-1]
	t.ids = t.ids[:len(t.ids)-1]
	return id, true
}

func (t *tableState) insertID() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextID++
	id := t.nextID
	t.ids = append(t.ids, id)
	return id
}

func (s *benchStats) record(batch connector.Batch, rowSizes map[string]int64) {
	if len(batch.Records) == 0 {
		return
	}
	stamp := time.Now().UTC()
	s.mu.Lock()
	if s.start.IsZero() {
		s.start = stamp
	}
	for _, record := range batch.Records {
		s.records++
		s.bytes += rowSizes[record.Table]
		base := record.Timestamp
		if after := record.After; after != nil {
			if value, ok := after["updated_at"]; ok {
				if ts, ok := value.(time.Time); ok {
					base = ts
				}
			}
		}
		if !base.IsZero() {
			latency := stamp.Sub(base)
			s.latencies = append(s.latencies, latency.Nanoseconds())
		}
	}
	s.end = stamp
	s.mu.Unlock()
}

func (s *benchStats) snapshot(target string, prof profile, scenario string) benchResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	duration := s.end.Sub(s.start)
	if duration <= 0 {
		duration = time.Millisecond
	}

	latencyP50 := quantile(s.latencies, 0.50)
	latencyP95 := quantile(s.latencies, 0.95)
	latencyP99 := quantile(s.latencies, 0.99)

	return benchResult{
		Target:        target,
		Profile:       prof.Name,
		Scenario:      scenario,
		Records:       s.records,
		Bytes:         s.bytes,
		DurationSec:   duration.Seconds(),
		RecordsPerSec: float64(s.records) / duration.Seconds(),
		MBPerSec:      float64(s.bytes) / duration.Seconds() / (1024 * 1024),
		LatencyP50Ms:  float64(latencyP50) / float64(time.Millisecond),
		LatencyP95Ms:  float64(latencyP95) / float64(time.Millisecond),
		LatencyP99Ms:  float64(latencyP99) / float64(time.Millisecond),
		StartedAt:     s.start.Format(time.RFC3339Nano),
		EndedAt:       s.end.Format(time.RFC3339Nano),
	}
}

func quantile(samples []int64, q float64) int64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]int64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pos := int(math.Ceil(q*float64(len(sorted)))) - 1
	if pos < 0 {
		pos = 0
	}
	if pos >= len(sorted) {
		pos = len(sorted) - 1
	}
	return sorted[pos]
}

func currentLSN(ctx context.Context, pool *pgxpool.Pool) (string, error) {
	var lsn string
	if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsn); err != nil {
		return "", fmt.Errorf("read current lsn: %w", err)
	}
	return lsn, nil
}

func runDDLChurn(ctx context.Context, pool *pgxpool.Pool) error {
	stmts := []string{
		"ALTER TABLE bench_src.narrow ADD COLUMN IF NOT EXISTS ddl_extra TEXT",
		"UPDATE bench_src.narrow SET ddl_extra = 'x'",
		"ALTER TABLE bench_src.narrow ALTER COLUMN status TYPE VARCHAR(64)",
		"ALTER TABLE bench_src.narrow DROP COLUMN IF EXISTS ddl_extra",
	}
	for _, stmt := range stmts {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("ddl churn: %w", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

func writeResults(dir string, results []benchResult) error {
	if len(results) == 0 {
		return nil
	}
	// #nosec G301 -- bench output directory is intentionally world-readable.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	jsonPath := filepath.Join(dir, fmt.Sprintf("bench_%s.json", stamp))
	csvPath := filepath.Join(dir, fmt.Sprintf("bench_%s.csv", stamp))

	if err := writeJSON(jsonPath, results); err != nil {
		return err
	}
	if err := writeCSV(csvPath, results); err != nil {
		return err
	}
	fmt.Printf("Wrote benchmark results: %s and %s\n", jsonPath, csvPath)
	return nil
}

func writeJSON(path string, results []benchResult) error {
	// #nosec G304 -- output path comes from CLI flag.
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create json file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close json file: %v\n", err)
		}
	}()

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		return fmt.Errorf("write json: %w", err)
	}
	return nil
}

func writeCSV(path string, results []benchResult) error {
	// #nosec G304 -- output path comes from CLI flag.
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create csv file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close csv file: %v\n", err)
		}
	}()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"target", "profile", "scenario", "records", "bytes", "duration_sec", "records_per_sec", "mb_per_sec", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "started_at", "ended_at"}); err != nil {
		return fmt.Errorf("write csv header: %w", err)
	}
	for _, result := range results {
		row := []string{
			result.Target,
			result.Profile,
			result.Scenario,
			fmt.Sprintf("%d", result.Records),
			fmt.Sprintf("%d", result.Bytes),
			formatFloat(result.DurationSec),
			formatFloat(result.RecordsPerSec),
			formatFloat(result.MBPerSec),
			formatFloat(result.LatencyP50Ms),
			formatFloat(result.LatencyP95Ms),
			formatFloat(result.LatencyP99Ms),
			result.StartedAt,
			result.EndedAt,
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}
	return nil
}

func formatFloat(value float64) string {
	return fmt.Sprintf("%.4f", value)
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	raw := strings.Split(value, ",")
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func schemaName(qualified string) string {
	parts := strings.Split(qualified, ".")
	if len(parts) >= 2 {
		return parts[0]
	}
	return "public"
}

func tableName(qualified string) string {
	parts := strings.Split(qualified, ".")
	if len(parts) >= 2 {
		return parts[1]
	}
	return qualified
}

func randString(rng *rand.Rand, size int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func buildStringArray(rng *rand.Rand, count, size int) []any {
	out := make([]any, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, randString(rng, size))
	}
	return out
}

func buildEventArray(rng *rand.Rand, count int) []any {
	out := make([]any, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, map[string]any{
			"id":    i + 1,
			"kind":  randString(rng, 12),
			"ts":    time.Now().UTC().Format(time.RFC3339Nano),
			"value": rng.Intn(1000),
		})
	}
	return out
}
