package grpc

import (
	"context"
	"errors"
	"time"

	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
	"github.com/josephjohncox/ductstream/pkg/pgstream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StreamService implements the gRPC StreamService API.
type StreamService struct {
	ductstreampb.UnimplementedStreamServiceServer
	store *pgstream.Store
}

func NewStreamService(store *pgstream.Store) *StreamService {
	return &StreamService{store: store}
}

func (s *StreamService) Pull(ctx context.Context, req *ductstreampb.StreamPullRequest) (*ductstreampb.StreamPullResponse, error) {
	if req == nil || req.Stream == "" || req.ConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "stream and consumer_group are required")
	}

	maxMessages := int(req.MaxMessages)
	visibility := time.Duration(req.VisibilityTimeoutSeconds) * time.Second
	messages, err := s.store.Claim(ctx, req.Stream, req.ConsumerGroup, req.ConsumerId, maxMessages, visibility)
	if err != nil {
		return nil, mapStreamError(err)
	}

	items := make([]*ductstreampb.StreamMessage, 0, len(messages))
	for _, msg := range messages {
		items = append(items, streamMessageToProto(msg))
	}

	return &ductstreampb.StreamPullResponse{Messages: items}, nil
}

func (s *StreamService) Ack(ctx context.Context, req *ductstreampb.StreamAckRequest) (*ductstreampb.StreamAckResponse, error) {
	if req == nil || req.Stream == "" || req.ConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "stream and consumer_group are required")
	}
	if len(req.Ids) == 0 {
		return &ductstreampb.StreamAckResponse{Acked: 0}, nil
	}

	acked, err := s.store.Ack(ctx, req.Stream, req.ConsumerGroup, req.Ids)
	if err != nil {
		return nil, mapStreamError(err)
	}

	return &ductstreampb.StreamAckResponse{Acked: acked}, nil
}

func (s *StreamService) Replay(ctx context.Context, req *ductstreampb.StreamReplayRequest) (*ductstreampb.StreamReplayResponse, error) {
	if req == nil || req.Stream == "" || req.ConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "stream and consumer_group are required")
	}

	opts := pgstream.ReplayOptions{FromLSN: req.FromLsn}
	if req.Since != nil {
		opts.Since = req.Since.AsTime()
	}

	reset, err := s.store.Replay(ctx, req.Stream, req.ConsumerGroup, opts)
	if err != nil {
		return nil, mapStreamError(err)
	}

	return &ductstreampb.StreamReplayResponse{Reset_: reset}, nil
}

func streamMessageToProto(msg pgstream.Message) *ductstreampb.StreamMessage {
	var createdAt *timestamppb.Timestamp
	if !msg.CreatedAt.IsZero() {
		createdAt = timestamppb.New(msg.CreatedAt)
	}

	return &ductstreampb.StreamMessage{
		Id:         msg.ID,
		Stream:     msg.Stream,
		Namespace:  msg.Namespace,
		Table:      msg.Table,
		Lsn:        msg.LSN,
		WireFormat: string(msg.WireFormat),
		Payload:    msg.Payload,
		CreatedAt:  createdAt,
	}
}

func mapStreamError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
