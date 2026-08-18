package grpc

import (
	"net"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	HealthServiceStartup   = "wallaby.startup"
	HealthServiceReadiness = "wallaby.readiness"
	HealthServiceLiveness  = "wallaby.liveness"
)

// Server wraps the gRPC server lifecycle.
type Server struct {
	server *gogrpc.Server
	health *health.Server
}

func New(engine workflow.ControlEngine, dispatcher RunOnceDispatcher, checkpoints connector.CheckpointStore, registryStore registry.Store, streamStore *pgstream.Store, enableReflection bool, meters *telemetry.Meters) *Server {
	return NewWithConnectorRegistry(engine, dispatcher, checkpoints, registryStore, streamStore, enableReflection, meters, connector.DefaultRegistry)
}

// NewWithConnectorRegistry wires the same custom connector registry through
// API validation and all runtime/persistence construction paths.
func NewWithConnectorRegistry(engine workflow.ControlEngine, dispatcher RunOnceDispatcher, checkpoints connector.CheckpointStore, registryStore registry.Store, streamStore *pgstream.Store, enableReflection bool, meters *telemetry.Meters, connectorRegistry *connector.Registry) *Server {
	return NewWithConnectorRegistryAndPolicy(engine, dispatcher, checkpoints, registryStore, streamStore, enableReflection, meters, connectorRegistry, connector.SnowflakeDeploymentPolicy{})
}

// NewWithConnectorRegistryAndPolicy wires custom registrations and deployment
// Snowflake admission through the API boundary.
func NewWithConnectorRegistryAndPolicy(engine workflow.ControlEngine, dispatcher RunOnceDispatcher, checkpoints connector.CheckpointStore, registryStore registry.Store, streamStore *pgstream.Store, enableReflection bool, meters *telemetry.Meters, connectorRegistry *connector.Registry, policy connector.SnowflakeDeploymentPolicy) *Server {
	var opts []gogrpc.ServerOption
	if meters != nil {
		opts = append(opts, gogrpc.UnaryInterceptor(MetricsInterceptor(meters)))
	}
	server := gogrpc.NewServer(opts...)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	for _, service := range []string{"", HealthServiceStartup, HealthServiceReadiness, HealthServiceLiveness} {
		healthServer.SetServingStatus(service, healthpb.HealthCheckResponse_SERVING)
	}
	wallabypb.RegisterFlowServiceServer(server, NewFlowServiceWithRegistryAndPolicy(engine, dispatcher, connectorRegistry, policy))
	if checkpoints != nil {
		wallabypb.RegisterCheckpointServiceServer(server, NewCheckpointService(checkpoints, meters))
	}
	if registryStore != nil {
		wallabypb.RegisterDDLServiceServer(server, NewDDLService(registryStore))
	}
	if streamStore != nil {
		wallabypb.RegisterStreamServiceServer(server, NewStreamService(streamStore))
	}
	if enableReflection {
		reflection.Register(server)
	}

	return &Server{server: server, health: healthServer}
}

func (s *Server) Serve(listener net.Listener) error {
	return s.server.Serve(listener)
}

func (s *Server) Stop() {
	for _, service := range []string{"", HealthServiceStartup, HealthServiceReadiness, HealthServiceLiveness} {
		s.health.SetServingStatus(service, healthpb.HealthCheckResponse_NOT_SERVING)
	}
	s.server.GracefulStop()
}
