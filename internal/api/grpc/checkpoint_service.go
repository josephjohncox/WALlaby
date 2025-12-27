package grpc

import (
	"context"
	"errors"
	"time"

	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
	"github.com/josephjohncox/ductstream/internal/checkpoint"
	"github.com/josephjohncox/ductstream/pkg/connector"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CheckpointService implements the gRPC CheckpointService API.
type CheckpointService struct {
	ductstreampb.UnimplementedCheckpointServiceServer
	store connector.CheckpointStore
}

func NewCheckpointService(store connector.CheckpointStore) *CheckpointService {
	return &CheckpointService{store: store}
}

func (s *CheckpointService) GetCheckpoint(ctx context.Context, req *ductstreampb.GetCheckpointRequest) (*ductstreampb.FlowCheckpoint, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	cp, err := s.store.Get(ctx, req.FlowId)
	if err != nil {
		return nil, mapCheckpointError(err)
	}
	return &ductstreampb.FlowCheckpoint{FlowId: req.FlowId, Checkpoint: checkpointToProto(cp)}, nil
}

func (s *CheckpointService) PutCheckpoint(ctx context.Context, req *ductstreampb.PutCheckpointRequest) (*ductstreampb.FlowCheckpoint, error) {
	if req == nil || req.FlowId == "" || req.Checkpoint == nil {
		return nil, status.Error(codes.InvalidArgument, "flow_id and checkpoint are required")
	}
	cp := checkpointFromProto(req.Checkpoint)
	if err := s.store.Put(ctx, req.FlowId, cp); err != nil {
		return nil, mapCheckpointError(err)
	}
	return &ductstreampb.FlowCheckpoint{FlowId: req.FlowId, Checkpoint: checkpointToProto(cp)}, nil
}

func (s *CheckpointService) ListCheckpoints(ctx context.Context, _ *ductstreampb.ListCheckpointsRequest) (*ductstreampb.ListCheckpointsResponse, error) {
	items, err := s.store.List(ctx)
	if err != nil {
		return nil, mapCheckpointError(err)
	}
	checkpoints := make([]*ductstreampb.FlowCheckpoint, 0, len(items))
	for _, item := range items {
		checkpoints = append(checkpoints, &ductstreampb.FlowCheckpoint{FlowId: item.FlowID, Checkpoint: checkpointToProto(item.Checkpoint)})
	}
	return &ductstreampb.ListCheckpointsResponse{Checkpoints: checkpoints}, nil
}

func checkpointToProto(cp connector.Checkpoint) *ductstreampb.Checkpoint {
	return &ductstreampb.Checkpoint{
		Lsn:                 cp.LSN,
		TimestampUnixMillis: cp.Timestamp.UnixMilli(),
		Metadata:            cp.Metadata,
	}
}

func checkpointFromProto(pb *ductstreampb.Checkpoint) connector.Checkpoint {
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
