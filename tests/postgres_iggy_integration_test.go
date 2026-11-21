package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	iggydest "github.com/josephjohncox/wallaby/connectors/destinations/iggy"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestPostgresToIggyCDCCheckpointRestart(t *testing.T) {
	dsn, address := os.Getenv("TEST_PG_DSN"), os.Getenv("WALLABY_TEST_IGGY_ADDRESS")
	if dsn == "" || address == "" {
		t.Skip("TEST_PG_DSN and WALLABY_TEST_IGGY_ADDRESS required")
	}
	if os.Getenv("WALLABY_TEST_IGGY_TLS") != "false" {
		t.Fatal("local pipeline fixture requires explicitly plaintext Iggy")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	identity := "wb_iggy_" + uuid.NewString()[:8]
	schema, slot, publication := identity, identity, identity
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "CREATE TABLE "+pgx.Identifier{schema, "events"}.Sanitize()+"(id bigint PRIMARY KEY, status text)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_, _ = pool.Exec(cleanup, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1 AND NOT active)", slot)
		_, _ = pool.Exec(cleanup, "DROP PUBLICATION IF EXISTS "+pgx.Identifier{publication}.Sanitize())
		_, _ = pool.Exec(cleanup, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})
	username, password := os.Getenv("WALLABY_TEST_IGGY_USERNAME"), os.Getenv("WALLABY_TEST_IGGY_PASSWORD")
	if username == "" || password == "" {
		t.Fatal("explicit local Iggy identity required")
	}
	admin, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(address), tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials(username, password))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.CreateStream(ctx, identity, nil); err != nil {
		t.Fatal(err)
	}
	streamID, err := iggcon.NewIdentifier(identity)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := admin.DeleteStream(cleanup, streamID); err != nil {
			t.Errorf("delete owned Iggy stream: %v", err)
		}
	})
	durability := iggcon.DurabilityPersisted
	if _, err := admin.CreateTopic(ctx, streamID, "cdc", &iggcon.TopicOptions{Durability: &durability}); err != nil {
		t.Fatal(err)
	}
	topicID, err := iggcon.NewIdentifier("cdc")
	if err != nil {
		t.Fatal(err)
	}
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.db")
	store, err := checkpoint.NewSQLiteStore(ctx, checkpointPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	newRunner := func() stream.Runner {
		return stream.Runner{
			FlowID: identity, Source: &pgsource.Source{},
			SourceSpec: connector.RuntimeSpec{Name: identity, Type: connector.EndpointPostgres, Options: map[string]string{
				"dsn": dsn, "slot": slot, "publication": publication,
				"publication_tables": pgx.Identifier{schema, "events"}.Sanitize(),
				"create_slot":        "true", "ensure_publication": "true", "ensure_state": "false",
				"batch_size": "1", "batch_timeout": "50ms", "status_interval": "100ms", "format": "json",
			}},
			Destinations: []stream.DestinationConfig{{Dest: &iggydest.Destination{}, Spec: connector.RuntimeSpec{Name: "iggy", Type: connector.EndpointIggy, Options: map[string]string{
				"address": address, "stream": identity, "topic": "cdc", "username": username, "password": password, "tls": "false", "format": "json",
			}}}},
			Checkpoints: store, AckPolicy: stream.AckPolicyAll, WireFormat: connector.WireFormatJSON, StrictFormat: true,
		}
	}
	var offset uint64
	for iteration := int64(1); iteration <= 2; iteration++ {
		runner := newRunner()
		runCtx, stop := context.WithCancel(ctx)
		finished := make(chan error, 1)
		go func() { finished <- runner.Run(runCtx) }()
		stopped := false
		t.Cleanup(func() {
			stop()
			if !stopped {
				select {
				case <-finished:
				case <-time.After(10 * time.Second):
					t.Error("pipeline goroutine did not stop")
				}
			}
		})
		waitForIggyPipeline(t, ctx, finished, func() (bool, error) {
			var active bool
			err := pool.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active)
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			return active, err
		})
		if _, err := pool.Exec(ctx, "INSERT INTO "+pgx.Identifier{schema, "events"}.Sanitize()+"(id,status) VALUES($1,$2)", iteration, fmt.Sprintf("live-%d", iteration)); err != nil {
			t.Fatal(err)
		}
		var delivered connector.Batch
		found := false
		for !found && ctx.Err() == nil {
			polled, err := admin.PollMessages(ctx, streamID, topicID, 0, iggcon.DefaultConsumer(), iggcon.OffsetPollingStrategy(offset), iggcon.WaitPollOptions(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			for _, message := range polled.Messages {
				offset = message.Header.Offset + 1
				var batch connector.Batch
				if err := json.Unmarshal(message.Payload, &batch); err != nil {
					t.Fatal(err)
				}
				for _, record := range batch.Records {
					if fmt.Sprint(record.After["status"]) == fmt.Sprintf("live-%d", iteration) {
						delivered, found = batch, true
					}
				}
			}
		}
		if !found {
			t.Fatal("real PostgreSQL change never reached Iggy")
		}
		waitForIggyPipeline(t, ctx, finished, func() (bool, error) {
			stored, err := store.Get(ctx, identity)
			if errors.Is(err, connector.ErrCheckpointNotFound) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			comparison, err := connector.CompareCheckpointLSN(stored.LSN, delivered.Checkpoint.LSN)
			if err != nil || comparison < 0 {
				return false, err
			}
			var observed bool
			err = pool.QueryRow(ctx, "SELECT confirmed_flush_lsn >= $2::pg_lsn FROM pg_replication_slots WHERE slot_name=$1", slot, stored.LSN).Scan(&observed)
			return observed, err
		})
		stop()
		select {
		case err := <-finished:
			stopped = true
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("pipeline stopped with error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("pipeline did not stop before checkpoint restart")
		}
		if iteration == 1 {
			before, err := store.Get(ctx, identity)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := checkpoint.NewSQLiteStore(ctx, checkpointPath)
			if err != nil {
				t.Fatal(err)
			}
			store = reopened
			restored, err := store.Get(ctx, identity)
			if err != nil || restored.LSN != before.LSN {
				t.Fatalf("durable checkpoint changed after reopen: got=%q want=%q err=%v", restored.LSN, before.LSN, err)
			}
		}
	}
}

func waitForIggyPipeline(t *testing.T, ctx context.Context, finished <-chan error, ready func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-finished:
			t.Fatalf("pipeline exited before live evidence: %v", err)
		default:
		}
		ok, err := ready()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("live pipeline evidence timed out: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
