package grpc

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestCheckpointServiceRetainsSQLiteReadWriteAPI(t *testing.T) {
	ctx := context.Background()
	store, err := checkpoint.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := NewCheckpointService(store, nil)
	put, err := service.PutCheckpoint(ctx, &wallabypb.PutCheckpointRequest{
		FlowId: "sqlite-flow",
		Checkpoint: &wallabypb.Checkpoint{
			Lsn:                 "0/20",
			TimestampUnixMillis: 1700000000123,
			Metadata:            map[string]string{"source": "sqlite"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if put.FlowId != "sqlite-flow" || put.Checkpoint.Lsn != "0/20" {
		t.Fatalf("put response=%+v", put)
	}
	got, err := service.GetCheckpoint(ctx, &wallabypb.GetCheckpointRequest{FlowId: "sqlite-flow"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Checkpoint.Lsn != "0/20" || got.Checkpoint.TimestampUnixMillis != 1700000000123 || !reflect.DeepEqual(got.Checkpoint.Metadata, map[string]string{"source": "sqlite"}) {
		t.Fatalf("get response=%+v", got)
	}
	listed, err := service.ListCheckpoints(ctx, &wallabypb.ListCheckpointsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Checkpoints) != 1 || listed.Checkpoints[0].FlowId != "sqlite-flow" {
		t.Fatalf("list response=%+v", listed)
	}
}

type unmanagedCheckpointStore struct {
	puts int
	data map[string]connector.Checkpoint
}

func (s *unmanagedCheckpointStore) Get(_ context.Context, flowID string) (connector.Checkpoint, error) {
	checkpoint, ok := s.data[flowID]
	if !ok {
		return connector.Checkpoint{}, connector.ErrCheckpointNotFound
	}
	return checkpoint, nil
}

func (s *unmanagedCheckpointStore) Put(_ context.Context, flowID string, checkpoint connector.Checkpoint) error {
	s.puts++
	if s.data == nil {
		s.data = make(map[string]connector.Checkpoint)
	}
	s.data[flowID] = checkpoint
	return nil
}

func (s *unmanagedCheckpointStore) List(context.Context) ([]connector.FlowCheckpoint, error) {
	items := make([]connector.FlowCheckpoint, 0, len(s.data))
	for flowID, checkpoint := range s.data {
		items = append(items, connector.FlowCheckpoint{FlowID: flowID, Checkpoint: checkpoint})
	}
	return items, nil
}

func TestCheckpointServiceRetainsExplicitUnmanagedPut(t *testing.T) {
	ctx := context.Background()
	store := &unmanagedCheckpointStore{}
	service := NewCheckpointService(store, nil)
	_, err := service.PutCheckpoint(ctx, &wallabypb.PutCheckpointRequest{
		FlowId: "unmanaged-flow",
		Checkpoint: &wallabypb.Checkpoint{
			Lsn:                 "42",
			TimestampUnixMillis: time.Unix(1700000000, 0).UnixMilli(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.puts != 1 || store.data["unmanaged-flow"].LSN != "42" {
		t.Fatalf("unmanaged writes=%d data=%+v", store.puts, store.data)
	}
}
