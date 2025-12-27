package app

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	apigrpc "github.com/josephjohncox/ductstream/internal/api/grpc"
	"github.com/josephjohncox/ductstream/internal/checkpoint"
	"github.com/josephjohncox/ductstream/internal/config"
	"github.com/josephjohncox/ductstream/internal/ddl"
	"github.com/josephjohncox/ductstream/internal/orchestrator"
	"github.com/josephjohncox/ductstream/internal/registry"
	"github.com/josephjohncox/ductstream/internal/runner"
	"github.com/josephjohncox/ductstream/internal/telemetry"
	"github.com/josephjohncox/ductstream/internal/workflow"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

// Run wires up core services. It will grow as implementations land.
func Run(ctx context.Context, cfg *config.Config) error {
	tracer := telemetry.Tracer(cfg.Telemetry.ServiceName)
	var engine workflow.Engine = workflow.NewNoopEngine()
	baseEngine := engine
	var checkpoints connector.CheckpointStore
	var registryStore registry.Store
	var registryCloser interface{ Close() }
	var dbosOrchestrator *orchestrator.DBOSOrchestrator
	if cfg.Postgres.DSN != "" {
		postgresEngine, err := workflow.NewPostgresEngine(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
		engine = postgresEngine
		baseEngine = postgresEngine

		checkpointStore, err := checkpoint.NewPostgresStore(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
		checkpoints = checkpointStore

		store, err := registry.NewPostgresStore(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
		registryStore = store
		registryCloser = store
	}

	factory := runner.Factory{}
	if registryStore != nil {
		factory.SchemaHook = &registry.Hook{
			Store:        registryStore,
			AutoApprove:  cfg.DDL.AutoApprove,
			GateApproval: cfg.DDL.Gate,
		}
	}

	if cfg.DDL.CatalogEnabled && cfg.Postgres.DSN != "" && registryStore != nil {
		pool, err := pgxpool.New(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
		scanner := &ddl.CatalogScanner{
			Pool:        pool,
			Registry:    registryStore,
			Schemas:     cfg.DDL.CatalogSchemas,
			AutoApprove: cfg.DDL.AutoApprove,
		}
		go func() {
			ticker := time.NewTicker(cfg.DDL.CatalogInterval)
			defer ticker.Stop()
			for {
				if err := scanner.RunOnce(ctx); err != nil {
					log.Printf("ddl catalog scan error: %v", err)
				}
				select {
				case <-ctx.Done():
					pool.Close()
					return
				case <-ticker.C:
				}
			}
		}()
	}

	if cfg.DBOS.Enabled {
		if cfg.Postgres.DSN == "" {
			return errors.New("dbos enabled but postgres dsn is not set")
		}
		maxEmptyReads := cfg.DBOS.MaxEmptyReads
		if cfg.DBOS.Schedule != "" && maxEmptyReads <= 0 {
			maxEmptyReads = 1
		}
		dbosRunner, err := orchestrator.NewDBOSOrchestrator(ctx, orchestrator.Config{
			AppName:       cfg.DBOS.AppName,
			DatabaseURL:   cfg.Postgres.DSN,
			Queue:         cfg.DBOS.Queue,
			Schedule:      cfg.DBOS.Schedule,
			MaxEmptyReads: maxEmptyReads,
			DefaultWire:   connector.WireFormat(cfg.Wire.DefaultFormat),
			StrictWire:    cfg.Wire.Enforce,
			Tracer:        tracer,
		}, baseEngine, checkpoints, factory)
		if err != nil {
			return err
		}
		dbosOrchestrator = dbosRunner
		engine = workflow.NewOrchestratedEngine(baseEngine, dbosRunner)
	}

	listener, err := net.Listen("tcp", cfg.API.GRPCListen)
	if err != nil {
		return err
	}

	server := apigrpc.New(engine, checkpoints, registryStore)
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
			if closer, ok := baseEngine.(interface{ Close() }); ok {
				closer.Close()
			}
			if closer, ok := checkpoints.(interface{ Close() }); ok {
				closer.Close()
			}
			if registryCloser != nil {
				registryCloser.Close()
			}
			if dbosOrchestrator != nil {
				dbosOrchestrator.Shutdown(30 * time.Second)
			}
			return nil
		}
		return ctx.Err()
	}
}
