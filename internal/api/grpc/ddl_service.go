package grpc

import (
	"context"
	"encoding/json"
	"errors"

	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
	"github.com/josephjohncox/ductstream/internal/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DDLService implements the gRPC DDLService API.
type DDLService struct {
	ductstreampb.UnimplementedDDLServiceServer
	store registry.Store
}

func NewDDLService(store registry.Store) *DDLService {
	return &DDLService{store: store}
}

func (s *DDLService) ListPendingDDL(ctx context.Context, _ *ductstreampb.ListPendingDDLRequest) (*ductstreampb.ListPendingDDLResponse, error) {
	events, err := s.store.ListPendingDDL(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	items := make([]*ductstreampb.DDLEvent, 0, len(events))
	for _, event := range events {
		items = append(items, ddlEventToProto(event))
	}

	return &ductstreampb.ListPendingDDLResponse{Events: items}, nil
}

func (s *DDLService) ListDDL(ctx context.Context, req *ductstreampb.ListDDLRequest) (*ductstreampb.ListDDLResponse, error) {
	statusFilter := ""
	if req != nil {
		statusFilter = req.Status
	}

	events, err := s.store.ListDDL(ctx, statusFilter)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	items := make([]*ductstreampb.DDLEvent, 0, len(events))
	for _, event := range events {
		items = append(items, ddlEventToProto(event))
	}

	return &ductstreampb.ListDDLResponse{Events: items}, nil
}

func (s *DDLService) ApproveDDL(ctx context.Context, req *ductstreampb.ApproveDDLRequest) (*ductstreampb.ApproveDDLResponse, error) {
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
	return &ductstreampb.ApproveDDLResponse{Event: ddlEventToProto(event)}, nil
}

func (s *DDLService) RejectDDL(ctx context.Context, req *ductstreampb.RejectDDLRequest) (*ductstreampb.RejectDDLResponse, error) {
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
	return &ductstreampb.RejectDDLResponse{Event: ddlEventToProto(event)}, nil
}

func (s *DDLService) MarkDDLApplied(ctx context.Context, req *ductstreampb.MarkDDLAppliedRequest) (*ductstreampb.MarkDDLAppliedResponse, error) {
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
	return &ductstreampb.MarkDDLAppliedResponse{Event: ddlEventToProto(event)}, nil
}

func ddlEventToProto(event registry.DDLEvent) *ductstreampb.DDLEvent {
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

	return &ductstreampb.DDLEvent{
		Id:        event.ID,
		Ddl:       event.DDL,
		Lsn:       event.LSN,
		Status:    event.Status,
		PlanJson:  planJSON,
		CreatedAt: createdAt,
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
