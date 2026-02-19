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
		if err := s.requireDispatcher(); err != nil {
			return nil, err
		}
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
	if err := s.requireDispatcher(); err != nil {
		return nil, err
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
	if err := s.requireDispatcher(); err != nil {
		return nil, err
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
	if err := s.requireDispatcher(); err != nil {
		return nil, err
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

func (s *FlowService) requireDispatcher() error {
	if s.dispatcher != nil {
		return nil
	}
	return status.Error(codes.FailedPrecondition, "dispatcher is required for immediate execution, but no dispatcher is configured")
}

func (s *FlowService) ListReplicationSlots(ctx context.Context, req *wallabypb.ListReplicationSlotsRequest) (*wallabypb.ListReplicationSlotsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	cfg, err := s.resolveSlotCommandConfig(ctx, req.FlowId, req.Dsn, strings.TrimSpace(req.Slot), false, req.Options)
	if err != nil {
		return nil, err
	}

	if cfg.slot != "" {
		slot, ok, err := pgsource.GetReplicationSlot(ctx, cfg.dsn, cfg.slot, cfg.options)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if !ok {
			return nil, status.Error(codes.NotFound, "slot not found")
		}
		return &wallabypb.ListReplicationSlotsResponse{FlowId: req.FlowId, Slots: []*wallabypb.ReplicationSlotInfo{replicationSlotInfoFromConnector(slot)}}, nil
	}

	slots, err := pgsource.ListReplicationSlots(ctx, cfg.dsn, cfg.options)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := make([]*wallabypb.ReplicationSlotInfo, 0, len(slots))
	for _, slot := range slots {
		out = append(out, replicationSlotInfoFromConnector(slot))
	}
	return &wallabypb.ListReplicationSlotsResponse{FlowId: req.FlowId, Slots: out}, nil
}

func (s *FlowService) GetReplicationSlot(ctx context.Context, req *wallabypb.GetReplicationSlotRequest) (*wallabypb.GetReplicationSlotResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	if strings.TrimSpace(req.Slot) == "" {
		return nil, status.Error(codes.InvalidArgument, "slot is required")
	}

	cfg, err := s.resolveSlotCommandConfig(ctx, req.FlowId, req.Dsn, req.Slot, true, req.Options)
	if err != nil {
		return nil, err
	}

	slot, ok, err := pgsource.GetReplicationSlot(ctx, cfg.dsn, cfg.slot, cfg.options)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !ok {
		return nil, status.Error(codes.NotFound, "slot not found")
	}
	return &wallabypb.GetReplicationSlotResponse{Slot: replicationSlotInfoFromConnector(slot)}, nil
}

func (s *FlowService) DropReplicationSlot(ctx context.Context, req *wallabypb.DropReplicationSlotRequest) (*wallabypb.DropReplicationSlotResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if strings.TrimSpace(req.Slot) == "" {
		return nil, status.Error(codes.InvalidArgument, "slot is required")
	}

	cfg, err := s.resolveSlotCommandConfig(ctx, req.FlowId, req.Dsn, req.Slot, true, req.Options)
	if err != nil {
		return nil, err
	}

	_, exists, err := pgsource.GetReplicationSlot(ctx, cfg.dsn, cfg.slot, cfg.options)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !exists && !req.IfExists {
		return nil, status.Error(codes.NotFound, "slot not found")
	}

	if err := pgsource.DropReplicationSlot(ctx, cfg.dsn, cfg.slot, cfg.options); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &wallabypb.DropReplicationSlotResponse{FlowId: req.FlowId, Slot: cfg.slot, Found: exists, Dropped: true}, nil
}

func (s *FlowService) ListPublicationTables(ctx context.Context, req *wallabypb.ListPublicationTablesRequest) (*wallabypb.ListPublicationTablesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	cfg, err := s.resolvePublicationCommandConfig(ctx, req.FlowId, req.Dsn, req.Publication, req.Options)
	if err != nil {
		return nil, err
	}

	tables, err := pgsource.ListPublicationTables(ctx, cfg.dsn, cfg.publication, cfg.options)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &wallabypb.ListPublicationTablesResponse{FlowId: req.FlowId, Publication: cfg.publication, Tables: tables}, nil
}

func (s *FlowService) AddPublicationTables(ctx context.Context, req *wallabypb.AddPublicationTablesRequest) (*wallabypb.PublicationTablesMutationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.Tables) == 0 {
		return nil, status.Error(codes.InvalidArgument, "tables are required")
	}

	cfg, err := s.resolvePublicationCommandConfig(ctx, req.FlowId, req.Dsn, req.Publication, req.Options)
	if err != nil {
		return nil, err
	}

	if err := pgsource.AddPublicationTables(ctx, cfg.dsn, cfg.publication, req.Tables, cfg.options); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &wallabypb.PublicationTablesMutationResponse{Publication: cfg.publication, Tables: req.Tables}, nil
}

func (s *FlowService) DropPublicationTables(ctx context.Context, req *wallabypb.DropPublicationTablesRequest) (*wallabypb.PublicationTablesMutationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.Tables) == 0 {
		return nil, status.Error(codes.InvalidArgument, "tables are required")
	}

	cfg, err := s.resolvePublicationCommandConfig(ctx, req.FlowId, req.Dsn, req.Publication, req.Options)
	if err != nil {
		return nil, err
	}

	if err := pgsource.DropPublicationTables(ctx, cfg.dsn, cfg.publication, req.Tables, cfg.options); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &wallabypb.PublicationTablesMutationResponse{Publication: cfg.publication, Tables: req.Tables}, nil
}

func (s *FlowService) SyncPublicationTables(ctx context.Context, req *wallabypb.SyncPublicationTablesRequest) (*wallabypb.SyncPublicationTablesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	cfg, err := s.resolvePublicationCommandConfig(ctx, req.FlowId, req.Dsn, req.Publication, req.Options)
	if err != nil {
		return nil, err
	}
	mode, err := pgsource.NormalizeSyncPublicationMode(req.Mode)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	added, removed, err := pgsource.SyncPublicationTables(ctx, cfg.dsn, cfg.publication, req.Tables, mode, cfg.options)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &wallabypb.SyncPublicationTablesResponse{FlowId: req.FlowId, Publication: cfg.publication, Added: added, Removed: removed}, nil
}

func (s *FlowService) ScrapePublicationTables(ctx context.Context, req *wallabypb.ScrapePublicationTablesRequest) (*wallabypb.ScrapePublicationTablesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if len(req.Schemas) == 0 {
		return nil, status.Error(codes.InvalidArgument, "schemas are required")
	}

	cfg, err := s.resolvePublicationCommandConfig(ctx, req.FlowId, req.Dsn, req.Publication, req.Options)
	if err != nil {
		return nil, err
	}

	allTables, err := pgsource.ScrapeTables(ctx, cfg.dsn, req.Schemas, cfg.options)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	current, err := pgsource.ListPublicationTables(ctx, cfg.dsn, cfg.publication, cfg.options)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	currentSet := make(map[string]struct{}, len(current))
	for _, table := range current {
		currentSet[strings.ToLower(table)] = struct{}{}
	}
	missing := make([]string, 0)
	for _, table := range allTables {
		if _, ok := currentSet[strings.ToLower(table)]; !ok {
			missing = append(missing, table)
		}
	}
	if req.Apply && len(missing) > 0 {
		if err := pgsource.AddPublicationTables(ctx, cfg.dsn, cfg.publication, missing, cfg.options); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	applied := req.Apply && len(missing) > 0
	if req.Apply && len(missing) == 0 {
		applied = false
	}

	return &wallabypb.ScrapePublicationTablesResponse{
		DiscoveredTables: allTables,
		MissingTables:    missing,
		Applied:          applied,
		FlowId:           req.FlowId,
	}, nil
}

type postgresHelperConfig struct {
	dsn         string
	slot        string
	publication string
	options     map[string]string
}

func (s *FlowService) resolveSlotCommandConfig(ctx context.Context, flowID, dsn, slot string, requireSlot bool, options map[string]string) (postgresHelperConfig, error) {
	if flowID == "" {
		if strings.TrimSpace(dsn) == "" {
			return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "flow_id or dsn is required")
		}
		return postgresHelperConfig{dsn: dsn, slot: slot, options: options}, nil
	}

	flowModel, err := flowServiceGetFlow(ctx, s.engine, flowID)
	if err != nil {
		return postgresHelperConfig{}, err
	}
	if flowModel.Source.Type != connector.EndpointPostgres {
		return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "flow source is not postgres")
	}

	baseDSN := strings.TrimSpace(flowModel.Source.Options["dsn"])
	resolvedDSN := strings.TrimSpace(dsn)
	if resolvedDSN == "" {
		resolvedDSN = baseDSN
	}
	if resolvedDSN == "" {
		return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "source dsn not found on flow")
	}

	resolvedSlot := strings.TrimSpace(slot)
	if resolvedSlot == "" {
		resolvedSlot = strings.TrimSpace(flowModel.Source.Options["slot"])
	}

	if requireSlot && resolvedSlot == "" {
		state, found, stateErr := pgsource.LookupSourceState(ctx, flowModel.Source, resolvedSlot)
		if stateErr != nil {
			return postgresHelperConfig{}, status.Error(codes.Internal, stateErr.Error())
		}
		if found && state.Slot != "" {
			resolvedSlot = strings.TrimSpace(state.Slot)
		}
	}
	if requireSlot && resolvedSlot == "" {
		return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "source slot not found on flow")
	}

	return postgresHelperConfig{
		dsn:         resolvedDSN,
		slot:        resolvedSlot,
		publication: strings.TrimSpace(flowModel.Source.Options["publication"]),
		options:     mergeOptionMaps(flowModel.Source.Options, options),
	}, nil
}

func (s *FlowService) resolvePublicationCommandConfig(ctx context.Context, flowID, dsn, publication string, options map[string]string) (postgresHelperConfig, error) {
	if flowID == "" {
		resolvedPublication := strings.TrimSpace(publication)
		if strings.TrimSpace(dsn) == "" {
			return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "flow_id or dsn is required")
		}
		if resolvedPublication == "" {
			return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "publication is required")
		}
		return postgresHelperConfig{dsn: dsn, publication: resolvedPublication, options: options}, nil
	}

	flowModel, err := flowServiceGetFlow(ctx, s.engine, flowID)
	if err != nil {
		return postgresHelperConfig{}, err
	}
	if flowModel.Source.Type != connector.EndpointPostgres {
		return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "flow source is not postgres")
	}
	baseDSN := strings.TrimSpace(flowModel.Source.Options["dsn"])
	resolvedDSN := strings.TrimSpace(dsn)
	if resolvedDSN == "" {
		resolvedDSN = baseDSN
	}
	if resolvedDSN == "" {
		return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "source dsn not found on flow")
	}

	resolvedPublication := strings.TrimSpace(publication)
	if resolvedPublication == "" {
		resolvedPublication = strings.TrimSpace(flowModel.Source.Options["publication"])
	}
	if resolvedPublication == "" {
		if state, found, stateErr := pgsource.LookupSourceState(ctx, flowModel.Source, ""); stateErr != nil {
			return postgresHelperConfig{}, status.Error(codes.Internal, stateErr.Error())
		} else if found {
			resolvedPublication = strings.TrimSpace(state.Publication)
		}
	}
	if resolvedPublication == "" {
		return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "publication is required")
	}

	return postgresHelperConfig{
		dsn:         resolvedDSN,
		publication: resolvedPublication,
		options:     mergeOptionMaps(flowModel.Source.Options, options),
	}, nil
}

func flowServiceGetFlow(ctx context.Context, engine workflow.Engine, flowID string) (flow.Flow, error) {
	f, err := engine.Get(ctx, flowID)
	if err != nil {
		return flow.Flow{}, mapWorkflowError(err)
	}
	return f, nil
}

func replicationSlotInfoFromConnector(item pgsource.ReplicationSlotInfo) *wallabypb.ReplicationSlotInfo {
	out := &wallabypb.ReplicationSlotInfo{
		SlotName:          item.SlotName,
		Plugin:            item.Plugin,
		SlotType:          item.SlotType,
		Database:          item.Database,
		Active:            item.Active,
		Temporary:         item.Temporary,
		WalStatus:         item.WalStatus,
		RestartLsn:        item.RestartLSN,
		ConfirmedFlushLsn: item.ConfirmedLSN,
	}
	if item.ActivePID != nil {
		out.ActivePid = *item.ActivePID
		out.ActivePidPresent = true
	}
	return out
}

func mergeOptionMaps(base map[string]string, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}

func mapWorkflowError(err error) error {
	if st, ok := status.FromError(err); ok {
		return st.Err()
	}

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
	mode, err := pgsource.NormalizeSyncPublicationMode(mode)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	_, _, err = pgsource.SyncPublicationTables(ctx, dsn, publication, tables, mode, opts)
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
