package grpc

import (
	"context"
	"errors"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CheckpointService implements the gRPC CheckpointService API.
type CheckpointService struct {
	wallabypb.UnimplementedCheckpointServiceServer
	store   connector.CheckpointStore
	meters  *telemetry.Meters
	backend string
}

func NewCheckpointService(store connector.CheckpointStore, meters *telemetry.Meters) *CheckpointService {
	return &CheckpointService{store: store, meters: meters, backend: checkpointBackend(store)}
}

func (s *CheckpointService) GetCheckpoint(ctx context.Context, req *wallabypb.GetCheckpointRequest) (*wallabypb.FlowCheckpoint, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	start := time.Now()
	cp, err := s.store.Get(ctx, req.FlowId)
	if s.meters != nil {
		s.meters.RecordCheckpointGet(ctx, s.backend, float64(time.Since(start).Milliseconds()))
	}
	if err != nil {
		return nil, mapCheckpointError(err)
	}
	return &wallabypb.FlowCheckpoint{FlowId: req.FlowId, Checkpoint: checkpointToProto(cp)}, nil
}

func (s *CheckpointService) PutCheckpoint(ctx context.Context, req *wallabypb.PutCheckpointRequest) (*wallabypb.FlowCheckpoint, error) {
	if req == nil || req.FlowId == "" || req.Checkpoint == nil {
		return nil, status.Error(codes.InvalidArgument, "flow_id and checkpoint are required")
	}
	cp := checkpointFromProto(req.Checkpoint)
	start := time.Now()
	if err := s.store.Put(ctx, req.FlowId, cp); err != nil {
		return nil, mapCheckpointError(err)
	}
	if s.meters != nil {
		s.meters.RecordCheckpointPut(ctx, s.backend, float64(time.Since(start).Milliseconds()))
	}
	return &wallabypb.FlowCheckpoint{FlowId: req.FlowId, Checkpoint: checkpointToProto(cp)}, nil
}

func (s *CheckpointService) ListCheckpoints(ctx context.Context, _ *wallabypb.ListCheckpointsRequest) (*wallabypb.ListCheckpointsResponse, error) {
	items, err := s.store.List(ctx)
	if err != nil {
		return nil, mapCheckpointError(err)
	}
	checkpoints := make([]*wallabypb.FlowCheckpoint, 0, len(items))
	for _, item := range items {
		checkpoints = append(checkpoints, &wallabypb.FlowCheckpoint{FlowId: item.FlowID, Checkpoint: checkpointToProto(item.Checkpoint)})
	}
	return &wallabypb.ListCheckpointsResponse{Checkpoints: checkpoints}, nil
}

func checkpointToProto(cp connector.Checkpoint) *wallabypb.Checkpoint {
	return &wallabypb.Checkpoint{
		Lsn:                 cp.LSN,
		TimestampUnixMillis: cp.Timestamp.UnixMilli(),
		Metadata:            cp.Metadata,
	}
}

func checkpointFromProto(pb *wallabypb.Checkpoint) connector.Checkpoint {
	return connector.Checkpoint{
		LSN:       pb.Lsn,
		Timestamp: unixMillisToTime(pb.TimestampUnixMillis),
		Metadata:  pb.Metadata,
	}
}

func unixMillisToTime(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func mapCheckpointError(err error) error {
	switch {
	case errors.Is(err, checkpoint.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func checkpointBackend(store connector.CheckpointStore) string {
	switch store.(type) {
	case *checkpoint.PostgresStore:
		return "postgres"
	case *checkpoint.SQLiteStore:
		return "sqlite"
	default:
		return "unknown"
	}
}
