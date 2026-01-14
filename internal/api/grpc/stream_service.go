package grpc

import (
	"context"
	"errors"
	"math"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StreamService implements the gRPC StreamService API.
type StreamService struct {
	wallabypb.UnimplementedStreamServiceServer
	store *pgstream.Store
}

func NewStreamService(store *pgstream.Store) *StreamService {
	return &StreamService{store: store}
}

func (s *StreamService) Pull(ctx context.Context, req *wallabypb.StreamPullRequest) (*wallabypb.StreamPullResponse, error) {
	if req == nil || req.Stream == "" || req.ConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "stream and consumer_group are required")
	}

	maxMessages := int(req.MaxMessages)
	visibility := time.Duration(req.VisibilityTimeoutSeconds) * time.Second
	messages, err := s.store.Claim(ctx, req.Stream, req.ConsumerGroup, req.ConsumerId, maxMessages, visibility)
	if err != nil {
		return nil, mapStreamError(err)
	}

	items := make([]*wallabypb.StreamMessage, 0, len(messages))
	for _, msg := range messages {
		items = append(items, streamMessageToProto(msg))
	}

	return &wallabypb.StreamPullResponse{Messages: items}, nil
}

func (s *StreamService) Ack(ctx context.Context, req *wallabypb.StreamAckRequest) (*wallabypb.StreamAckResponse, error) {
	if req == nil || req.Stream == "" || req.ConsumerGroup == "" {
		return nil, status.Error(codes.InvalidArgument, "stream and consumer_group are required")
	}
	if len(req.Ids) == 0 {
		return &wallabypb.StreamAckResponse{Acked: 0}, nil
	}

	acked, err := s.store.Ack(ctx, req.Stream, req.ConsumerGroup, req.Ids)
	if err != nil {
		return nil, mapStreamError(err)
	}

	return &wallabypb.StreamAckResponse{Acked: acked}, nil
}

func (s *StreamService) Replay(ctx context.Context, req *wallabypb.StreamReplayRequest) (*wallabypb.StreamReplayResponse, error) {
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

	return &wallabypb.StreamReplayResponse{Reset_: reset}, nil
}

func streamMessageToProto(msg pgstream.Message) *wallabypb.StreamMessage {
	var createdAt *timestamppb.Timestamp
	if !msg.CreatedAt.IsZero() {
		createdAt = timestamppb.New(msg.CreatedAt)
	}

	registryVersion := int32(0)
	if msg.RegistryVersion > 0 {
		if msg.RegistryVersion > math.MaxInt32 {
			registryVersion = math.MaxInt32
		} else {
			registryVersion = int32(msg.RegistryVersion)
		}
	}

	return &wallabypb.StreamMessage{
		Id:              msg.ID,
		Stream:          msg.Stream,
		Namespace:       msg.Namespace,
		Table:           msg.Table,
		Lsn:             msg.LSN,
		WireFormat:      string(msg.WireFormat),
		Payload:         msg.Payload,
		CreatedAt:       createdAt,
		RegistrySubject: msg.RegistrySubject,
		RegistryId:      msg.RegistryID,
		RegistryVersion: registryVersion,
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
