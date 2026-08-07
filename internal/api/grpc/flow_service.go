package grpc

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	engine            workflow.ControlEngine
	dispatcher        RunOnceDispatcher
	connectorRegistry *connector.Registry
}

// RunOnceDispatcher schedules one attempt against a captured lifecycle fence.
type RunOnceDispatcher interface {
	EnqueueRunOnce(ctx context.Context, flowID string, generation int64) error
}

func NewFlowService(engine workflow.ControlEngine, dispatcher RunOnceDispatcher) *FlowService {
	return NewFlowServiceWithRegistry(engine, dispatcher, connector.DefaultRegistry)
}

// NewFlowServiceWithRegistry injects the custom connector registry shared with workers.
func NewFlowServiceWithRegistry(engine workflow.ControlEngine, dispatcher RunOnceDispatcher, registry *connector.Registry) *FlowService {
	if registry == nil {
		registry = connector.NewRegistry()
	}
	return &FlowService{engine: engine, dispatcher: dispatcher, connectorRegistry: registry}
}

func (s *FlowService) CreateFlow(ctx context.Context, req *wallabypb.CreateFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.Flow == nil {
		return nil, status.Error(codes.InvalidArgument, "flow is required")
	}

	model, err := flowFromProtoWithRegistry(req.Flow, s.connectorRegistry)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if model.ID == "" {
		model.ID = uuid.NewString()
	}
	if model.State != "" && model.State != flow.StateCreated {
		return nil, status.Error(codes.InvalidArgument, "flows must be created in created state")
	}
	model.State = flow.StateCreated
	if err := flow.ValidateDefinitionWithRegistry(model, s.connectorRegistry); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
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

	return s.flowToProto(created)
}

func (s *FlowService) UpdateFlow(ctx context.Context, req *wallabypb.UpdateFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.Flow == nil {
		return nil, status.Error(codes.InvalidArgument, "flow is required")
	}

	model, err := flowFromProtoWithRegistry(req.Flow, s.connectorRegistry)
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
	existingSource, err := existing.DecodeSource(s.connectorRegistry)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	modelSource, err := model.DecodeSource(s.connectorRegistry)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if connector.IsManagedSourceSpec(existingSource) || connector.IsManagedSourceSpec(modelSource) {
		return nil, status.Error(codes.FailedPrecondition, "managed flow updates require a fenced source-resource revision")
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
	if err := flow.ValidateDefinitionWithRegistry(model, s.connectorRegistry); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	updated, err := s.engine.Update(ctx, model)
	if err != nil {
		return nil, mapWorkflowError(err)
	}

	return s.flowToProto(updated)
}

func (s *FlowService) ReconfigureFlow(ctx context.Context, req *wallabypb.ReconfigureFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.Flow == nil {
		return nil, status.Error(codes.InvalidArgument, "flow is required")
	}

	model, err := flowFromProtoWithRegistry(req.Flow, s.connectorRegistry)
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
	existingSource, err := existing.DecodeSource(s.connectorRegistry)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	modelSource, err := model.DecodeSource(s.connectorRegistry)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if connector.IsManagedSourceSpec(existingSource) || connector.IsManagedSourceSpec(modelSource) {
		return nil, status.Error(codes.FailedPrecondition, "managed flow reconfiguration requires a fenced source-resource revision")
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
	existingPostgres := existing.Source.GetPostgresSource()
	modelPostgres := model.Source.GetPostgresSource()
	if modelPostgres == nil {
		return nil, status.Error(codes.InvalidArgument, "flow source is not postgres")
	}
	syncPublication := optionalBool(req.SyncPublication, existingPostgres != nil && existingPostgres.GetSyncPublication())
	modelPostgres.SyncPublication = &syncPublication
	if err := flow.ValidateDefinitionWithRegistry(model, s.connectorRegistry); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	pauseFirst := optionalBool(req.PauseFirst, true)
	resumeAfter := optionalBool(req.ResumeAfter, true)
	if syncPublication {
		if err := checkFlowPublicationMutation(ctx, s.engine, model); err != nil {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
	}

	wasRunning := existing.State == flow.StateRunning
	if pauseFirst && wasRunning {
		if _, err := s.engine.Pause(ctx, model.ID); err != nil {
			return nil, mapWorkflowError(err)
		}
	}

	updated, err := s.engine.Update(ctx, model)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	current := updated

	if syncPublication {
		if err := syncFlowPublication(ctx, s.engine, updated); err != nil {
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

	return s.flowToProto(current)
}

func (s *FlowService) StartFlow(ctx context.Context, req *wallabypb.StartFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	started, err := s.engine.Start(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return s.flowToProto(started)
}

func (s *FlowService) RunFlowOnce(ctx context.Context, req *wallabypb.RunFlowOnceRequest) (*wallabypb.RunFlowOnceResponse, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	if err := s.requireDispatcher(); err != nil {
		return nil, err
	}
	control, err := s.engine.Control(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	if control.State != flow.StateRunning || control.Target != workflow.TargetRunning {
		return nil, status.Error(codes.FailedPrecondition, "flow is not running")
	}
	if err := s.dispatcher.EnqueueRunOnce(ctx, req.FlowId, control.Generation); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &wallabypb.RunFlowOnceResponse{Dispatched: true}, nil
}

func (s *FlowService) PauseFlow(ctx context.Context, req *wallabypb.PauseFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	paused, err := s.engine.Pause(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return s.flowToProto(paused)
}

func (s *FlowService) StopFlow(ctx context.Context, req *wallabypb.StopFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	stopped, err := s.engine.Stop(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return s.flowToProto(stopped)
}

func (s *FlowService) ResumeFlow(ctx context.Context, req *wallabypb.ResumeFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	resumed, err := s.engine.Resume(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return s.flowToProto(resumed)
}

func (s *FlowService) GetFlow(ctx context.Context, req *wallabypb.GetFlowRequest) (*wallabypb.Flow, error) {
	if req == nil || req.FlowId == "" {
		return nil, status.Error(codes.InvalidArgument, "flow_id is required")
	}
	found, err := s.engine.Get(ctx, req.FlowId)
	if err != nil {
		return nil, mapWorkflowError(err)
	}
	return s.flowToProto(found)
}

func (s *FlowService) ListFlows(ctx context.Context, _ *wallabypb.ListFlowsRequest) (*wallabypb.ListFlowsResponse, error) {
	flows, err := s.engine.List(ctx)
	if err != nil {
		return nil, mapWorkflowError(err)
	}

	items := make([]*wallabypb.Flow, 0, len(flows))
	for _, f := range flows {
		item, conversionErr := s.flowToProto(f)
		if conversionErr != nil {
			return nil, conversionErr
		}
		items = append(items, item)
	}

	return &wallabypb.ListFlowsResponse{Flows: items}, nil
}

func (s *FlowService) flowToProto(definition flow.Flow) (*wallabypb.Flow, error) {
	encoded, err := flowToProto(definition, s.connectorRegistry)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return encoded, nil
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
	source, err := f.DecodeSource(s.connectorRegistry)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if source.Type != connector.EndpointPostgres {
		return &wallabypb.CleanupFlowResponse{Cleaned: true}, nil
	}
	if connector.IsManagedSourceSpec(source) {
		return nil, status.Error(codes.FailedPrecondition, "managed source cleanup requires exact fenced ownership and cannot use the legacy cleanup RPC")
	}

	dropSlot := optionalBool(req.DropSlot, true)
	dropPublication := optionalBool(req.DropPublication, false)
	dropState := optionalBool(req.DropSourceState, true)

	dsn := strings.TrimSpace(source.Options["dsn"])
	slot := strings.TrimSpace(source.Options["slot"])
	publication := strings.TrimSpace(source.Options["publication"])

	if dsn == "" {
		return nil, status.Error(codes.InvalidArgument, "postgres dsn is required for cleanup")
	}
	// Legacy cleanup may not discover managed physical names by consulting the
	// source. Authorize every configured resource against control PostgreSQL
	// before the first source-network operation.
	if dropSlot {
		if err := s.authorizeLegacyResourceMutation(ctx, req.FlowId, "", "", nil, "slot"); err != nil {
			return nil, err
		}
	}
	if dropPublication {
		if err := s.authorizeLegacyResourceMutation(ctx, req.FlowId, "", "", nil, "publication"); err != nil {
			return nil, err
		}
	}

	if dropSlot || dropPublication {
		if slot == "" || publication == "" {
			if stateInfo, ok, err := pgsource.LookupSourceState(ctx, source, slot); err != nil {
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
		if err := pgsource.DropReplicationSlot(ctx, dsn, slot, source.Options); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if dropPublication {
		if err := pgsource.DropPublication(ctx, dsn, publication, source.Options); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if dropState {
		if err := pgsource.DeleteSourceState(ctx, source, slot); err != nil {
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

	cfg, err := s.resolveSlotCommandConfig(ctx, req.FlowId, req.Dsn, strings.TrimSpace(req.Slot), false, postgresAdminOptions(req.GetRdsIam()))
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

	cfg, err := s.resolveSlotCommandConfig(ctx, req.FlowId, req.Dsn, req.Slot, true, postgresAdminOptions(req.GetRdsIam()))
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
	if err := s.authorizeLegacyResourceMutation(ctx, req.FlowId, req.Dsn, req.Slot, postgresAdminOptions(req.GetRdsIam()), "slot"); err != nil {
		return nil, err
	}

	cfg, err := s.resolveSlotCommandConfig(ctx, req.FlowId, req.Dsn, req.Slot, true, postgresAdminOptions(req.GetRdsIam()))
	if err != nil {
		return nil, err
	}
	if cfg.managed {
		return nil, status.Error(codes.FailedPrecondition, "managed slot cleanup requires the current fenced resource owner")
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

	cfg, err := s.resolvePublicationCommandConfig(ctx, req.FlowId, req.Dsn, req.Publication, postgresAdminOptions(req.GetRdsIam()))
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
	if err := s.authorizeLegacyResourceMutation(ctx, req.FlowId, req.Dsn, req.Publication, postgresAdminOptions(req.GetRdsIam()), "publication"); err != nil {
		return nil, err
	}
	if len(req.Tables) == 0 {
		return nil, status.Error(codes.InvalidArgument, "tables are required")
	}

	cfg, err := s.resolvePublicationCommandConfig(ctx, req.FlowId, req.Dsn, req.Publication, postgresAdminOptions(req.GetRdsIam()))
	if err != nil {
		return nil, err
	}
	if cfg.managed {
		return nil, status.Error(codes.FailedPrecondition, "managed publication changes require a fenced source-resource revision")
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
	if err := s.authorizeLegacyResourceMutation(ctx, req.FlowId, req.Dsn, req.Publication, postgresAdminOptions(req.GetRdsIam()), "publication"); err != nil {
		return nil, err
	}
	if len(req.Tables) == 0 {
		return nil, status.Error(codes.InvalidArgument, "tables are required")
	}

	cfg, err := s.resolvePublicationCommandConfig(ctx, req.FlowId, req.Dsn, req.Publication, postgresAdminOptions(req.GetRdsIam()))
	if err != nil {
		return nil, err
	}
	if cfg.managed {
		return nil, status.Error(codes.FailedPrecondition, "managed publication changes require a fenced source-resource revision")
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
	if err := s.authorizeLegacyResourceMutation(ctx, req.FlowId, req.Dsn, req.Publication, postgresAdminOptions(req.GetRdsIam()), "publication"); err != nil {
		return nil, err
	}

	cfg, err := s.resolvePublicationCommandConfig(ctx, req.FlowId, req.Dsn, req.Publication, postgresAdminOptions(req.GetRdsIam()))
	if err != nil {
		return nil, err
	}
	if cfg.managed {
		return nil, status.Error(codes.FailedPrecondition, "managed publication synchronization requires a fenced source-resource revision")
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
	if req.Apply {
		if err := s.authorizeLegacyResourceMutation(ctx, req.FlowId, req.Dsn, req.Publication, postgresAdminOptions(req.GetRdsIam()), "publication"); err != nil {
			return nil, err
		}
	}
	if len(req.Schemas) == 0 {
		return nil, status.Error(codes.InvalidArgument, "schemas are required")
	}

	cfg, err := s.resolvePublicationCommandConfig(ctx, req.FlowId, req.Dsn, req.Publication, postgresAdminOptions(req.GetRdsIam()))
	if err != nil {
		return nil, err
	}
	if req.Apply && cfg.managed {
		return nil, status.Error(codes.FailedPrecondition, "managed publication changes require a fenced source-resource revision")
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
		currentSet[table] = struct{}{}
	}
	missing := make([]string, 0)
	for _, table := range allTables {
		if _, ok := currentSet[table]; !ok {
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

type legacyResourceMutationGuard interface {
	CheckLegacySourceResourceMutation(context.Context, string, string, string, string) error
}

func (s *FlowService) authorizeLegacyResourceMutation(ctx context.Context, flowID, dsn, physicalName string, options map[string]string, resourceKind string) error {
	if strings.TrimSpace(flowID) == "" {
		return status.Error(codes.FailedPrecondition, "direct-DSN source-resource mutation is disabled; bind the operation to an unmanaged flow")
	}
	if strings.TrimSpace(dsn) != "" || len(options) != 0 {
		return status.Error(codes.InvalidArgument, "flow-bound source-resource mutation rejects DSN and connection-option overrides")
	}
	f, err := flowServiceGetFlow(ctx, s.engine, flowID)
	if err != nil {
		return err
	}
	source, err := f.DecodeSource(s.connectorRegistry)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if source.Type != connector.EndpointPostgres {
		return status.Error(codes.InvalidArgument, "flow source is not postgres")
	}
	if connector.IsManagedSourceSpec(source) {
		return status.Error(codes.FailedPrecondition, "managed source-resource mutation requires the current fenced resource owner")
	}
	optionName := resourceKind
	if resourceKind == "publication" {
		optionName = "publication"
	}
	expectedName := strings.TrimSpace(source.Options[optionName])
	if expectedName == "" || strings.TrimSpace(physicalName) != "" {
		return status.Error(codes.InvalidArgument, "flow-bound source-resource mutation rejects physical-name overrides")
	}
	guard, ok := s.engine.(legacyResourceMutationGuard)
	if !ok {
		return nil
	}
	databaseName := ""
	if config, parseErr := pgx.ParseConfig(strings.TrimSpace(source.Options["dsn"])); parseErr == nil {
		databaseName = config.Database
	}
	if err := guard.CheckLegacySourceResourceMutation(ctx, strings.TrimSpace(source.Options["source_system_identifier"]), databaseName, resourceKind, expectedName); err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return nil
}

type postgresHelperConfig struct {
	dsn         string
	slot        string
	publication string
	options     map[string]string
	managed     bool
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
	source, err := flowModel.DecodeSource(s.connectorRegistry)
	if err != nil {
		return postgresHelperConfig{}, status.Error(codes.Internal, err.Error())
	}
	if source.Type != connector.EndpointPostgres {
		return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "flow source is not postgres")
	}

	baseDSN := strings.TrimSpace(source.Options["dsn"])
	resolvedDSN := strings.TrimSpace(dsn)
	if resolvedDSN == "" {
		resolvedDSN = baseDSN
	}
	if resolvedDSN == "" {
		return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "source dsn not found on flow")
	}

	resolvedSlot := strings.TrimSpace(slot)
	if resolvedSlot == "" {
		resolvedSlot = strings.TrimSpace(source.Options["slot"])
	}

	if requireSlot && resolvedSlot == "" {
		state, found, stateErr := pgsource.LookupSourceState(ctx, source, resolvedSlot)
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
		publication: strings.TrimSpace(source.Options["publication"]),
		options:     mergeOptionMaps(source.Options, options),
		managed:     connector.IsManagedSourceSpec(source),
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
	source, err := flowModel.DecodeSource(s.connectorRegistry)
	if err != nil {
		return postgresHelperConfig{}, status.Error(codes.Internal, err.Error())
	}
	if source.Type != connector.EndpointPostgres {
		return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "flow source is not postgres")
	}
	baseDSN := strings.TrimSpace(source.Options["dsn"])
	resolvedDSN := strings.TrimSpace(dsn)
	if resolvedDSN == "" {
		resolvedDSN = baseDSN
	}
	if resolvedDSN == "" {
		return postgresHelperConfig{}, status.Error(codes.InvalidArgument, "source dsn not found on flow")
	}

	resolvedPublication := strings.TrimSpace(publication)
	if resolvedPublication == "" {
		resolvedPublication = strings.TrimSpace(source.Options["publication"])
	}
	if resolvedPublication == "" {
		if state, found, stateErr := pgsource.LookupSourceState(ctx, source, ""); stateErr != nil {
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
		options:     mergeOptionMaps(source.Options, options),
		managed:     connector.IsManagedSourceSpec(source),
	}, nil
}

func flowServiceGetFlow(ctx context.Context, engine workflow.Engine, flowID string) (flow.Flow, error) {
	f, err := engine.Get(ctx, flowID)
	if err != nil {
		return flow.Flow{}, mapWorkflowError(err)
	}
	return f, nil
}

func postgresAdminOptions(iam *wallabypb.RDSIAMConfig) map[string]string {
	if iam == nil {
		return nil
	}
	options := map[string]string{"aws_rds_iam": "true"}
	for key, value := range map[string]string{
		"aws_region":            iam.GetRegion(),
		"aws_profile":           iam.GetProfile(),
		"aws_role_arn":          iam.GetRoleArn(),
		"aws_role_session_name": iam.GetRoleSessionName(),
		"aws_role_external_id":  iam.GetRoleExternalId(),
		"aws_endpoint":          iam.GetEndpoint(),
	} {
		if value != "" {
			options[key] = value
		}
	}
	return options
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

func syncFlowPublication(ctx context.Context, engine workflow.ControlEngine, f flow.Flow) error {
	if err := checkFlowPublicationMutation(ctx, engine, f); err != nil {
		return err
	}
	source, err := f.DecodeSource(connector.DefaultRegistry)
	if err != nil {
		return err
	}
	if source.Type != connector.EndpointPostgres || source.Options == nil {
		return nil
	}
	opts := source.Options
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
	mode, err = pgsource.NormalizeSyncPublicationMode(mode)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	_, _, err = pgsource.SyncPublicationTables(ctx, dsn, publication, tables, mode, opts)
	return err
}

func checkFlowPublicationMutation(ctx context.Context, engine workflow.ControlEngine, f flow.Flow) error {
	source, err := f.DecodeSource(connector.DefaultRegistry)
	if err != nil {
		return err
	}
	if source.Type != connector.EndpointPostgres || source.Options == nil {
		return nil
	}
	opts := source.Options
	dsn := strings.TrimSpace(opts["dsn"])
	publication := strings.TrimSpace(opts["publication"])
	if dsn == "" || publication == "" {
		return nil
	}
	guard, ok := engine.(legacyResourceMutationGuard)
	if !ok {
		return errors.New("publication synchronization requires the managed source-resource ownership guard")
	}
	databaseName := ""
	if config, parseErr := pgx.ParseConfig(dsn); parseErr == nil {
		databaseName = config.Database
	}
	if err := guard.CheckLegacySourceResourceMutation(ctx, strings.TrimSpace(opts["source_system_identifier"]), databaseName, "publication", publication); err != nil {
		return fmt.Errorf("publication synchronization ownership check: %w", err)
	}
	return nil
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
