package grpc

import (
	"net"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server wraps the gRPC server lifecycle.
type Server struct {
	server *gogrpc.Server
}

func New(engine workflow.Engine, dispatcher FlowDispatcher, checkpoints connector.CheckpointStore, registryStore registry.Store, streamStore *pgstream.Store, enableReflection bool) *Server {
	server := gogrpc.NewServer()
	wallabypb.RegisterFlowServiceServer(server, NewFlowService(engine, dispatcher))
	if checkpoints != nil {
		wallabypb.RegisterCheckpointServiceServer(server, NewCheckpointService(checkpoints))
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

	return &Server{server: server}
}

func (s *Server) Serve(listener net.Listener) error {
	return s.server.Serve(listener)
}

func (s *Server) Stop() {
	s.server.GracefulStop()
}
