package app

import (
	"context"
	"errors"
	"net"

	apigrpc "github.com/josephjohncox/ductstream/internal/api/grpc"
	"github.com/josephjohncox/ductstream/internal/checkpoint"
	"github.com/josephjohncox/ductstream/internal/config"
	"github.com/josephjohncox/ductstream/internal/telemetry"
	"github.com/josephjohncox/ductstream/internal/workflow"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

// Run wires up core services. It will grow as implementations land.
func Run(ctx context.Context, cfg *config.Config) error {
	_ = telemetry.Tracer(cfg.Telemetry.ServiceName)
	var engine workflow.Engine = workflow.NewNoopEngine()
	var checkpoints connector.CheckpointStore
	if cfg.Postgres.DSN != "" {
		postgresEngine, err := workflow.NewPostgresEngine(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
		engine = postgresEngine

		checkpointStore, err := checkpoint.NewPostgresStore(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
		checkpoints = checkpointStore
	}

	listener, err := net.Listen("tcp", cfg.API.GRPCListen)
	if err != nil {
		return err
	}

	server := apigrpc.New(engine, checkpoints)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
			server.Stop()
			if closer, ok := engine.(interface{ Close() }); ok {
				closer.Close()
			}
			if closer, ok := checkpoints.(interface{ Close() }); ok {
				closer.Close()
			}
			return nil
		}
		return ctx.Err()
	}
}
