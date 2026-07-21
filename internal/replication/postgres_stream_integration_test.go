package replication

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresManagedStreamedSubtransactionAbort(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	const (
		tableName   = "wallaby_managed_stream_subabort"
		publication = "wallaby_managed_stream_publication"
		slot        = "wallaby_managed_stream_slot"
	)
	if _, err := pool.Exec(ctx, `
DROP PUBLICATION IF EXISTS wallaby_managed_stream_publication;
DROP TABLE IF EXISTS public.wallaby_managed_stream_subabort;
CREATE TABLE public.wallaby_managed_stream_subabort (id bigint PRIMARY KEY,payload text NOT NULL);
CREATE PUBLICATION wallaby_managed_stream_publication FOR TABLE public.wallaby_managed_stream_subabort`); err != nil {
		t.Fatal(err)
	}

	stream := NewPostgresStream(
		dsn,
		WithStreamingTransactions(true),
		WithStatusInterval(50*time.Millisecond),
		WithConnConfigFunc(func(_ context.Context, cfg *pgconn.Config) error {
			cfg.RuntimeParams["logical_decoding_work_mem"] = "64kB"
			return nil
		}),
	)
	changes, err := stream.Start(ctx, slot, publication)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = stream.Stop(context.Background())
		_, _ = pool.Exec(context.Background(), `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1`, slot)
		_, _ = pool.Exec(context.Background(), `DROP PUBLICATION IF EXISTS wallaby_managed_stream_publication; DROP TABLE IF EXISTS public.wallaby_managed_stream_subabort`)
	}()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	randomPayload := make([]byte, 128<<10)
	if _, err := rand.Read(randomPayload); err != nil {
		t.Fatal(err)
	}
	payload := base64.StdEncoding.EncodeToString(randomPayload)
	if _, err := tx.Exec(ctx, `INSERT INTO public.wallaby_managed_stream_subabort (id,payload) VALUES ($1,$2)`, int64(1), payload); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SAVEPOINT wallaby_subtransaction"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SAVEPOINT wallaby_nested_subtransaction"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `ALTER TABLE public.wallaby_managed_stream_subabort ADD COLUMN aborted_note text`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.wallaby_managed_stream_subabort (id,payload,aborted_note) VALUES ($1,$2,'must-not-leak')`, int64(2), payload); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT wallaby_nested_subtransaction"); err != nil {
		t.Fatal(err)
	}
	streamDeadline := time.Now().Add(5 * time.Second)
	for {
		starts, _ := managedStreamProtocolEvidence(stream)
		if starts > 0 {
			break
		}
		if time.Now().After(streamDeadline) {
			t.Fatal("PostgreSQL did not stream the in-progress transaction under the 64kB decoding limit")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT wallaby_subtransaction"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO public.wallaby_managed_stream_subabort (id,payload) VALUES ($1,$2)`, int64(3), payload); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	ids := make([]string, 0, 2)
	var transactionID uint32
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("wait for streamed transaction: %v", ctx.Err())
		case change, ok := <-changes:
			if !ok {
				t.Fatalf("replication stream closed: %v", stream.Err())
			}
			if change.Record != nil {
				ids = append(ids, fmt.Sprint(change.Record.After["id"]))
			}
			if transactionID == 0 {
				transactionID = change.TransactionID
			}
			if change.TransactionID != transactionID {
				t.Fatalf("streamed changes crossed transaction IDs: %d then %d", transactionID, change.TransactionID)
			}
			if change.TransactionFinal {
				goto committed
			}
		}
	}

committed:
	starts, subaborts := managedStreamProtocolEvidence(stream)
	if starts == 0 || subaborts == 0 {
		t.Fatalf("stream protocol evidence starts/subaborts=%d/%d, want both positive", starts, subaborts)
	}
	if strings.Join(ids, ",") != "1,3" {
		t.Fatalf("committed streamed row IDs=%v, want parent rows [1 3] without aborted subtransaction row", ids)
	}
	for _, schema := range stream.schemas {
		if schema.Name == tableName && len(schema.Columns) != 2 {
			t.Fatalf("aborted streamed schema leaked after commit: %+v", schema.Columns)
		}
	}
}

func managedStreamProtocolEvidence(stream *PostgresStream) (starts, subaborts uint64) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return stream.streamedStarts, stream.streamedSubaborts
}
