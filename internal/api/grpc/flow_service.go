package grpc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
	"github.com/josephjohncox/ductstream/internal/flow"
	"github.com/josephjohncox/ductstream/internal/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FlowService implements the gRPC FlowService API.
type FlowService struct {
	ductstreampb.UnimplementedFlowServiceServer
	engine     workflow.Engine
	dispatcher FlowDispatcher
}

type FlowDispatcher interface {
	EnqueueFlow(ctx context.Context, flowID string) error
}

func NewFlowService(engine workflow.Engine, dispatcher FlowDispatcher) *FlowService {
	return &FlowService{engine: engine, dispatcher: dispatcher}
}

func (s *FlowService) CreateFlow(ctx context.Context, req *ductstreampb.CreateFlowRequest) (*ductstreampb.Flow, error) {
	if req == nil || req.Flow == nil {
		return nil, status.Error(codes.InvalidArgument, "flow is required")
	}

	model, err := flowFromProto(req.Flow)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if model.ID == "" {
		model.ID = uuid.NewString()
	}
	if model.State == "" {
		model.State = flow.StateCreated
	}

	created, err := s.engine.Create(ctx, model)
	if err != nil {
		return nil, mapWorkflowError(err)
	}

	if req.StartImmediately {
		created, err = s.engine.Start(ctx, created.ID)
		if err != nil {
			return nil, mapWorkflowError(err)
		}
	}

	return flowToProto(created), nil
}

func (s *FlowService) UpdateFlow(ctx context.Context, req *ductstreampb.UpdateFlowRequest) (*ductstreampb.Flow, error) {
	if req == nil || req.Flow == nil {
		return nil, status.Error(codes.InvalidArgument, "flow is required")
	}

	model, err := flowFromProto(req.Flow)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if model.ID == "" {
		return nil, status.Error(codes.InvalidArgument, "flow id is required")
	}

	existing, err := s.engine.Get(ctx, model.ID)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	model.State = existing.State
	if model.Name == "" {
		model.Name = existing.Name
	}
	if model.WireFormat == "" {
		model.WireFormat = existing.WireFormat
	}
	if model.Parallelism == 0 {
		model.Parallelism = existing.Parallelism
	}

	updated, err := s.engine.Update(ctx, model)
	if err != nil {
		return nil, mapWorkflowError(err)
	}

	return flowToProto(updated), nil
}

func (s *FlowService) StartFlow(ctx context.Context, req *ductstreampb.StartFlowRequest) (*ductstreampb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	started, err := s.engine.Start(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return flowToProto(started), nil
}

func (s *FlowService) RunFlowOnce(ctx context.Context, req *ductstreampb.RunFlowOnceRequest) (*ductstreampb.RunFlowOnceResponse, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	if s.dispatcher == nil {
		return nil, status.Error(codes.FailedPrecondition, "dispatcher not configured")
	}
	if _, err := s.engine.Get(ctx, req.FlowId); err != nil {
		return nil, mapWorkflowError(err)
	}
	if err := s.dispatcher.EnqueueFlow(ctx, req.FlowId); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &ductstreampb.RunFlowOnceResponse{Dispatched: true}, nil
}

func (s *FlowService) StopFlow(ctx context.Context, req *ductstreampb.StopFlowRequest) (*ductstreampb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	stopped, err := s.engine.Stop(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return flowToProto(stopped), nil
}

func (s *FlowService) ResumeFlow(ctx context.Context, req *ductstreampb.ResumeFlowRequest) (*ductstreampb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	resumed, err := s.engine.Resume(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return flowToProto(resumed), nil
}

func (s *FlowService) GetFlow(ctx context.Context, req *ductstreampb.GetFlowRequest) (*ductstreampb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	found, err := s.engine.Get(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return flowToProto(found), nil
}

func (s *FlowService) ListFlows(ctx context.Context, _ *ductstreampb.ListFlowsRequest) (*ductstreampb.ListFlowsResponse, error) {
	flows, err := s.engine.List(ctx)
	if err != nil {
		return nil, mapWorkflowError(err)
	}

	items := make([]*ductstreampb.Flow, 0, len(flows))
	for _, f := range flows {
		items = append(items, flowToProto(f))
	}

	return &ductstreampb.ListFlowsResponse{Flows: items}, nil
}

func (s *FlowService) DeleteFlow(ctx context.Context, req *ductstreampb.DeleteFlowRequest) (*ductstreampb.DeleteFlowResponse, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	if err := s.engine.Delete(ctx, req.FlowId); err != nil {
		return nil, mapWorkflowError(err)
	}
	return &ductstreampb.DeleteFlowResponse{Deleted: true}, nil
}

func mapWorkflowError(err error) error {
	switch {
	case errors.Is(err, workflow.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, workflow.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, workflow.ErrInvalidState):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
