package grpc

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
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

func (s *FlowService) ReconfigureFlow(ctx context.Context, req *wallabypb.ReconfigureFlowRequest) (*wallabypb.Flow, error) {
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

	pauseFirst := optionalBool(req.PauseFirst, true)
	resumeAfter := optionalBool(req.ResumeAfter, true)
	syncPublication := optionalBool(req.SyncPublication, parseBool(existing.Source.Options["sync_publication"], false))

	wasRunning := existing.State == flow.StateRunning
	if pauseFirst && wasRunning {
		if _, err := s.engine.Stop(ctx, model.ID); err != nil {
			return nil, mapWorkflowError(err)
		}
	}

	updated, err := s.engine.Update(ctx, model)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	current := updated

	if syncPublication {
		if err := syncFlowPublication(ctx, updated); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	if resumeAfter && wasRunning {
		resumed, err := s.engine.Resume(ctx, model.ID)
		if err != nil {
			return nil, mapWorkflowError(err)
		}
		current = resumed
	}

	return flowToProto(current), nil
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

func (s *FlowService) CleanupFlow(ctx context.Context, req *wallabypb.CleanupFlowRequest) (*wallabypb.CleanupFlowResponse, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}

	f, err := s.engine.Get(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	if f.Source.Type != connector.EndpointPostgres {
		return &wallabypb.CleanupFlowResponse{Cleaned: true}, nil
	}

	dropSlot := optionalBool(req.DropSlot, true)
	dropPublication := optionalBool(req.DropPublication, false)
	dropState := optionalBool(req.DropSourceState, true)

	dsn := strings.TrimSpace(f.Source.Options["dsn"])
	slot := strings.TrimSpace(f.Source.Options["slot"])
	publication := strings.TrimSpace(f.Source.Options["publication"])

	if dsn == "" {
		return nil, status.Error(codes.InvalidArgument, "postgres dsn is required for cleanup")
	}

	if dropSlot || dropPublication {
		if slot == "" || publication == "" {
			if stateInfo, ok, err := pgsource.LookupSourceState(ctx, f.Source, slot); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			} else if ok {
				if slot == "" {
					slot = strings.TrimSpace(stateInfo.Slot)
				}
				if publication == "" {
					publication = strings.TrimSpace(stateInfo.Publication)
				}
			}
		}
	}

	if dropSlot {
		if err := pgsource.DropReplicationSlot(ctx, dsn, slot, f.Source.Options); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if dropPublication {
		if err := pgsource.DropPublication(ctx, dsn, publication, f.Source.Options); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if dropState {
		if err := pgsource.DeleteSourceState(ctx, f.Source, slot); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &wallabypb.CleanupFlowResponse{Cleaned: true}, nil
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

func optionalBool(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func parseBool(raw string, fallback bool) bool {
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y":
		return true
	case "0", "false", "no", "n":
		return false
	default:
		return fallback
	}
}

func syncFlowPublication(ctx context.Context, f flow.Flow) error {
	if f.Source.Type != connector.EndpointPostgres {
		return nil
	}
	opts := f.Source.Options
	if opts == nil {
		return nil
	}
	dsn := strings.TrimSpace(opts["dsn"])
	publication := strings.TrimSpace(opts["publication"])
	if dsn == "" || publication == "" {
		return nil
	}
	tables := splitCSV(opts["publication_tables"])
	if len(tables) == 0 {
		tables = splitCSV(opts["tables"])
	}
	if len(tables) == 0 {
		schemas := splitCSV(opts["publication_schemas"])
		if len(schemas) > 0 {
			var err error
			tables, err = pgsource.ScrapeTables(ctx, dsn, schemas, opts)
			if err != nil {
				return err
			}
		}
	}
	if len(tables) == 0 {
		return nil
	}
	mode := strings.TrimSpace(opts["sync_publication_mode"])
	if mode == "" {
		mode = "add"
	}
	_, _, err := pgsource.SyncPublicationTables(ctx, dsn, publication, tables, mode, opts)
	return err
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}
