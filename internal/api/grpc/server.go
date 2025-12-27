package grpc

import (
	"net"

	"github.com/josephjohncox/ductstream/internal/workflow"
	"github.com/josephjohncox/ductstream/pkg/connector"
	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
	gogrpc "google.golang.org/grpc"
)

// Server wraps the gRPC server lifecycle.
type Server struct {
	server *gogrpc.Server
}

func New(engine workflow.Engine, checkpoints connector.CheckpointStore) *Server {
	server := gogrpc.NewServer()
	ductstreampb.RegisterFlowServiceServer(server, NewFlowService(engine))
	if checkpoints != nil {
		ductstreampb.RegisterCheckpointServiceServer(server, NewCheckpointService(checkpoints))
	}

	return &Server{server: server}
}

func (s *Server) Serve(listener net.Listener) error {
	return s.server.Serve(listener)
}

func (s *Server) Stop() {
	s.server.GracefulStop()
}
