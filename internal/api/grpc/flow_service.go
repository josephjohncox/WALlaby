package grpc

import (
	"context"
	"errors"

	"github.com/google/uuid"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FlowService implements the gRPC FlowService API.
type FlowService struct {
	wallabypb.UnimplementedFlowServiceServer
	engine     workflow.Engine
	dispatcher FlowDispatcher
}

type FlowDispatcher interface {
	EnqueueFlow(ctx context.Context, flowID string) error
}

func NewFlowService(engine workflow.Engine, dispatcher FlowDispatcher) *FlowService {
	return &FlowService{engine: engine, dispatcher: dispatcher}
}

func (s *FlowService) CreateFlow(ctx context.Context, req *wallabypb.CreateFlowRequest) (*wallabypb.Flow, error) {
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

func (s *FlowService) UpdateFlow(ctx context.Context, req *wallabypb.UpdateFlowRequest) (*wallabypb.Flow, error) {
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
	if req.Flow.Config == nil {
		model.Config = existing.Config
	}

	updated, err := s.engine.Update(ctx, model)
	if err != nil {
		return nil, mapWorkflowError(err)
	}

	return flowToProto(updated), nil
}

func (s *FlowService) StartFlow(ctx context.Context, req *wallabypb.StartFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	started, err := s.engine.Start(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return flowToProto(started), nil
}

func (s *FlowService) RunFlowOnce(ctx context.Context, req *wallabypb.RunFlowOnceRequest) (*wallabypb.RunFlowOnceResponse, error) {
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
	return &wallabypb.RunFlowOnceResponse{Dispatched: true}, nil
}

func (s *FlowService) StopFlow(ctx context.Context, req *wallabypb.StopFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	stopped, err := s.engine.Stop(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return flowToProto(stopped), nil
}

func (s *FlowService) ResumeFlow(ctx context.Context, req *wallabypb.ResumeFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	resumed, err := s.engine.Resume(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return flowToProto(resumed), nil
}

func (s *FlowService) GetFlow(ctx context.Context, req *wallabypb.GetFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	found, err := s.engine.Get(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return flowToProto(found), nil
}

func (s *FlowService) ListFlows(ctx context.Context, _ *wallabypb.ListFlowsRequest) (*wallabypb.ListFlowsResponse, error) {
	flows, err := s.engine.List(ctx)
	if err != nil {
		return nil, mapWorkflowError(err)
	}

	items := make([]*wallabypb.Flow, 0, len(flows))
	for _, f := range flows {
		items = append(items, flowToProto(f))
	}

	return &wallabypb.ListFlowsResponse{Flows: items}, nil
}

func (s *FlowService) DeleteFlow(ctx context.Context, req *wallabypb.DeleteFlowRequest) (*wallabypb.DeleteFlowResponse, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	if err := s.engine.Delete(ctx, req.FlowId); err != nil {
		return nil, mapWorkflowError(err)
	}
	return &wallabypb.DeleteFlowResponse{Deleted: true}, nil
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
