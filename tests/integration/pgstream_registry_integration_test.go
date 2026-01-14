package integration_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pgstreamdest "github.com/josephjohncox/wallaby/connectors/destinations/pgstream"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
)

func TestPGStreamSchemaRegistryMetadata(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName, dbDSN := createTempDatabase(t, ctx, adminPool, "wallaby_pgstream_registry")
	defer dropDatabase(t, adminPool, dbName)

	dest := &pgstreamdest.Destination{}
	spec := connector.Spec{
		Name: "pgstream-registry",
		Type: connector.EndpointPGStream,
		Options: map[string]string{
			"dsn":                     dbDSN,
			"stream":                  "orders",
			"format":                  "avro",
			"schema_registry":         "local",
			"schema_registry_subject": "public.orders",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	schema := connector.Schema{
		Namespace: "public",
		Name:      "orders",
		Columns: []connector.Column{
			{Name: "id", Type: "int8"},
			{Name: "name", Type: "text"},
		},
	}
	record := connector.Record{
		Table:     "orders",
		Operation: connector.OpInsert,
		After: map[string]any{
			"id":   1,
			"name": "alpha",
		},
		Timestamp: time.Now().UTC(),
	}
	batch := connector.Batch{
		Records:    []connector.Record{record},
		Schema:     schema,
		Checkpoint: connector.Checkpoint{LSN: "0/1"},
	}
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write batch: %v", err)
	}

	streamPool, err := pgxpool.New(ctx, dbDSN)
	if err != nil {
		t.Fatalf("connect stream db: %v", err)
	}
	defer streamPool.Close()

	var subject, regID string
	var regVersion int
	if err := streamPool.QueryRow(ctx, "SELECT registry_subject, registry_id, registry_version FROM stream_events LIMIT 1").Scan(&subject, &regID, &regVersion); err != nil {
		t.Fatalf("read stream registry metadata: %v", err)
	}
	if subject == "" || regID == "" {
		t.Fatalf("expected registry metadata, got subject=%q id=%q", subject, regID)
	}
	if regVersion == 0 {
		t.Fatalf("expected registry version, got %d", regVersion)
	}

	store, err := pgstream.NewStore(ctx, dbDSN)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	claimed, err := store.Claim(ctx, "orders", "g1", "c1", 1, 10*time.Second)
	if err != nil {
		t.Fatalf("claim messages: %v", err)
	}
	if len(claimed) == 0 {
		t.Fatalf("expected claimed message")
	}
	if claimed[0].RegistrySubject == "" || claimed[0].RegistryID == "" || claimed[0].RegistryVersion == 0 {
		t.Fatalf("expected registry metadata in claimed message")
	}
}
