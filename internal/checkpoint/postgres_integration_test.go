package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestPostgresOutboxReplayOrderSurvivesTimestampPrecisionCollapse(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	flowID := fmt.Sprintf("outbox-order-%d", time.Now().UnixNano())
	defer func() {
		_, _ = store.pool.Exec(ctx, "DELETE FROM checkpoint_outbox WHERE flow_id=$1", flowID)
		_, _ = store.pool.Exec(ctx, "DELETE FROM checkpoints WHERE flow_id=$1", flowID)
	}()
	checkpoint := connector.Checkpoint{LSN: "0/20"}
	created := time.Unix(1, 123456789).UTC()
	batch := connector.Batch{Checkpoint: checkpoint, Schema: connector.Schema{Name: "events"}, Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SourcePosition: "0/20"}}}
	entries := []connector.OutboxEntry{{Destination: "dest", PositionID: "0/20/fragment/000000", ProjectionFingerprint: "p", Batch: batch, CreatedAt: created}, {Destination: "dest", PositionID: "0/20", ProjectionFingerprint: "p", Batch: batch, CreatedAt: created}}
	if err := store.PersistCheckpointAndOutbox(ctx, flowID, checkpoint, entries); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE checkpoint_outbox SET created_at=$2 WHERE flow_id=$1", flowID, created.Truncate(time.Microsecond)); err != nil {
		t.Fatal(err)
	}
	got, err := store.ListOutbox(ctx, flowID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].PositionID != entries[0].PositionID || got[1].PositionID != entries[1].PositionID || got[1].ReplayOrder <= got[0].ReplayOrder {
		t.Fatalf("ordered outbox=%+v", got)
	}
}

func TestPostgresStoreRejectsCheckpointRegression(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore() error = %v", err)
	}
	defer store.Close()

	flowPrefix := fmt.Sprintf("checkpoint-monotonic-%d", time.Now().UnixNano())
	defer func() {
		_, _ = store.pool.Exec(ctx, "DELETE FROM checkpoint_outbox WHERE flow_id LIKE $1", flowPrefix+"%")
		_, _ = store.pool.Exec(ctx, "DELETE FROM checkpoints WHERE flow_id LIKE $1", flowPrefix+"%")
	}()

	invalidFlowID := flowPrefix + "-invalid"
	if err := store.Put(ctx, invalidFlowID, connector.Checkpoint{LSN: "not-an-lsn"}); !errors.Is(err, connector.ErrCheckpointPosition) {
		t.Fatalf("Put(invalid first checkpoint) error = %v, want ErrCheckpointPosition", err)
	}
	if _, err := store.Get(ctx, invalidFlowID); !errors.Is(err, connector.ErrCheckpointNotFound) {
		t.Fatalf("Get(invalid flow) error = %v, want ErrCheckpointNotFound", err)
	}
	for _, invalid := range []string{" 9 ", "+9", "-9"} {
		if err := store.Put(ctx, invalidFlowID+invalid, connector.Checkpoint{LSN: invalid}); !errors.Is(err, connector.ErrCheckpointPosition) {
			t.Fatalf("Put(%q) error = %v, want ErrCheckpointPosition", invalid, err)
		}
	}
	if err := store.Put(ctx, flowPrefix+"-backfill", connector.Checkpoint{Metadata: map[string]string{
		"mode":  connector.SourceModeBackfill,
		"table": "public.accounts",
	}}); err != nil {
		t.Fatalf("Put(backfill control checkpoint) error = %v", err)
	}

	tests := []struct {
		name    string
		flowID  string
		initial string
		advance string
		regress string
	}{
		{name: "PostgreSQL LSN", flowID: flowPrefix + "-pg", initial: "0/FFFFFFFF", advance: "1/0", regress: "0/FFFFFFFE"},
		{name: "abstract ordinal", flowID: flowPrefix + "-ordinal", initial: "9", advance: "10", regress: "8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := store.Put(ctx, tt.flowID, connector.Checkpoint{LSN: tt.initial}); err != nil {
				t.Fatalf("Put(initial) error = %v", err)
			}
			if err := store.Put(ctx, tt.flowID, connector.Checkpoint{LSN: tt.advance}); err != nil {
				t.Fatalf("Put(advance) error = %v", err)
			}
			if err := store.Put(ctx, tt.flowID, connector.Checkpoint{LSN: tt.regress}); !errors.Is(err, connector.ErrCheckpointRegression) {
				t.Fatalf("Put(regress) error = %v, want ErrCheckpointRegression", err)
			}
			checkpoint, err := store.Get(ctx, tt.flowID)
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if checkpoint.LSN != tt.advance {
				t.Fatalf("Get().LSN = %q, want %q", checkpoint.LSN, tt.advance)
			}
		})
	}

	canonicalFlow := flowPrefix + "-canonical"
	if err := store.Put(ctx, canonicalFlow, connector.Checkpoint{LSN: "000a/000ff"}); err != nil {
		t.Fatalf("Put(canonical): %v", err)
	}
	canonical, err := store.Get(ctx, canonicalFlow)
	if err != nil || canonical.LSN != "A/FF" {
		t.Fatalf("Get(canonical) = %+v, %v; want A/FF", canonical, err)
	}

	outboxFlow := flowPrefix + "-outbox"
	checkpoint := connector.Checkpoint{LSN: "11", Metadata: map[string]string{"seq": "11"}}
	position, _ := connector.CheckpointPositionID(checkpoint)
	batch := connector.Batch{Checkpoint: checkpoint, Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, Payload: []byte(`{"id":11}`)}}}
	entry := connector.OutboxEntry{Destination: "secondary", PositionID: position, ProjectionFingerprint: "projection", Batch: batch}
	if err := store.PersistCheckpointAndOutbox(ctx, outboxFlow, checkpoint, []connector.OutboxEntry{entry}); err != nil {
		t.Fatalf("PersistCheckpointAndOutbox(): %v", err)
	}
	if err := store.PersistCheckpointAndOutbox(ctx, outboxFlow, checkpoint, []connector.OutboxEntry{entry}); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	pending, err := store.ListOutbox(ctx, outboxFlow)
	if err != nil || len(pending) != 1 {
		t.Fatalf("ListOutbox() = %+v, %v", pending, err)
	}
	conflicting := batch
	conflicting.Records = []connector.Record{{Table: "events", Operation: connector.OpInsert, Payload: []byte(`{"id":12}`)}}
	err = store.PersistCheckpointAndOutbox(ctx, outboxFlow, checkpoint, []connector.OutboxEntry{{Destination: "secondary", PositionID: position, ProjectionFingerprint: "projection", Batch: conflicting}})
	if !errors.Is(err, connector.ErrOutboxConflict) {
		t.Fatalf("conflicting replay error = %v, want ErrOutboxConflict", err)
	}
}
