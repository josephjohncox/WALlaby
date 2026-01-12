package grpc

import (
	"context"
	"encoding/json"
	"errors"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DDLService implements the gRPC DDLService API.
type DDLService struct {
	wallabypb.UnimplementedDDLServiceServer
	store registry.Store
}

func NewDDLService(store registry.Store) *DDLService {
	return &DDLService{store: store}
}

func (s *DDLService) ListPendingDDL(ctx context.Context, req *wallabypb.ListPendingDDLRequest) (*wallabypb.ListPendingDDLResponse, error) {
	flowID := ""
	if req != nil {
		flowID = req.FlowId
	}
	events, err := s.store.ListPendingDDL(ctx, flowID)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	items := make([]*wallabypb.DDLEvent, 0, len(events))
	for _, event := range events {
		items = append(items, ddlEventToProto(event))
	}

	return &wallabypb.ListPendingDDLResponse{Events: items}, nil
}

func (s *DDLService) ListDDL(ctx context.Context, req *wallabypb.ListDDLRequest) (*wallabypb.ListDDLResponse, error) {
	statusFilter := ""
	flowID := ""
	if req != nil {
		statusFilter = req.Status
		flowID = req.FlowId
	}

	events, err := s.store.ListDDL(ctx, flowID, statusFilter)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	items := make([]*wallabypb.DDLEvent, 0, len(events))
	for _, event := range events {
		items = append(items, ddlEventToProto(event))
	}

	return &wallabypb.ListDDLResponse{Events: items}, nil
}

func (s *DDLService) ApproveDDL(ctx context.Context, req *wallabypb.ApproveDDLRequest) (*wallabypb.ApproveDDLResponse, error) {
	if req == nil || req.Id == 0 {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if err := s.store.SetDDLStatus(ctx, req.Id, registry.StatusApproved); err != nil {
		return nil, mapRegistryError(err)
	}
	event, err := s.store.GetDDL(ctx, req.Id)
	if err != nil {
		return nil, mapRegistryError(err)
	}
	return &wallabypb.ApproveDDLResponse{Event: ddlEventToProto(event)}, nil
}

func (s *DDLService) RejectDDL(ctx context.Context, req *wallabypb.RejectDDLRequest) (*wallabypb.RejectDDLResponse, error) {
	if req == nil || req.Id == 0 {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if err := s.store.SetDDLStatus(ctx, req.Id, registry.StatusRejected); err != nil {
		return nil, mapRegistryError(err)
	}
	event, err := s.store.GetDDL(ctx, req.Id)
	if err != nil {
		return nil, mapRegistryError(err)
	}
	return &wallabypb.RejectDDLResponse{Event: ddlEventToProto(event)}, nil
}

func (s *DDLService) MarkDDLApplied(ctx context.Context, req *wallabypb.MarkDDLAppliedRequest) (*wallabypb.MarkDDLAppliedResponse, error) {
	if req == nil || req.Id == 0 {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if err := s.store.SetDDLStatus(ctx, req.Id, registry.StatusApplied); err != nil {
		return nil, mapRegistryError(err)
	}
	event, err := s.store.GetDDL(ctx, req.Id)
	if err != nil {
		return nil, mapRegistryError(err)
	}
	return &wallabypb.MarkDDLAppliedResponse{Event: ddlEventToProto(event)}, nil
}

func ddlEventToProto(event registry.DDLEvent) *wallabypb.DDLEvent {
	var planJSON string
	if len(event.Plan.Changes) > 0 {
		if payload, err := json.Marshal(event.Plan); err == nil {
			planJSON = string(payload)
		}
	}

	var createdAt *timestamppb.Timestamp
	if !event.CreatedAt.IsZero() {
		createdAt = timestamppb.New(event.CreatedAt)
	}

	return &wallabypb.DDLEvent{
		Id:        event.ID,
		Ddl:       event.DDL,
		Lsn:       event.LSN,
		Status:    event.Status,
		PlanJson:  planJSON,
		CreatedAt: createdAt,
		FlowId:    event.FlowID,
	}
}

func mapRegistryError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, registry.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
