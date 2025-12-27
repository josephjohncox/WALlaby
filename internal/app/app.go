package app

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
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
	"github.com/josephjohncox/ductstream/pkg/pgstream"
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
	var kubeDispatcher *orchestrator.KubernetesDispatcher
	var streamStore *pgstream.Store
	postgresDSN := cfg.Postgres.DSN
	if postgresDSN != "" {
		postgresEngine, err := workflow.NewPostgresEngine(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
		engine = postgresEngine
		baseEngine = postgresEngine

		store, err := registry.NewPostgresStore(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
		registryStore = store
		registryCloser = store

		streamStore, err = pgstream.NewStore(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
	}

	backend := resolveCheckpointBackend(cfg)
	switch backend {
	case "", "none":
	case "postgres":
		if postgresDSN == "" {
			return errors.New("checkpoint backend postgres requires DUCTSTREAM_POSTGRES_DSN")
		}
		checkpointStore, err := checkpoint.NewPostgresStore(ctx, postgresDSN)
		if err != nil {
			return err
		}
		checkpoints = checkpointStore
	case "sqlite":
		dsn := cfg.Checkpoints.DSN
		if dsn == "" {
			dsn = cfg.Checkpoints.Path
		}
		if dsn == "" {
			dsn = defaultCheckpointPath()
		}
		checkpointStore, err := checkpoint.NewSQLiteStore(ctx, dsn)
		if err != nil {
			return err
		}
		checkpoints = checkpointStore
	default:
		return errors.New("unsupported checkpoint backend: " + backend)
	}

	factory := runner.Factory{}
	if registryStore != nil {
		factory.SchemaHook = &registry.Hook{
			Store:        registryStore,
			AutoApprove:  cfg.DDL.AutoApprove,
			GateApproval: cfg.DDL.Gate,
			AutoApply:    cfg.DDL.AutoApply,
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
		if cfg.Kubernetes.Enabled {
			return errors.New("dbos and kubernetes dispatch cannot both be enabled")
		}
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
	if cfg.Kubernetes.Enabled {
		dispatcher, err := orchestrator.NewKubernetesDispatcher(ctx, orchestrator.KubernetesConfig{
			KubeconfigPath:     cfg.Kubernetes.KubeconfigPath,
			KubeContext:        cfg.Kubernetes.KubeContext,
			APIServer:          cfg.Kubernetes.APIServer,
			BearerToken:        cfg.Kubernetes.BearerToken,
			CAFile:             cfg.Kubernetes.CAFile,
			CAData:             cfg.Kubernetes.CAData,
			ClientCertFile:     cfg.Kubernetes.ClientCertFile,
			ClientKeyFile:      cfg.Kubernetes.ClientKeyFile,
			InsecureSkipTLS:    cfg.Kubernetes.InsecureSkipTLS,
			Namespace:          cfg.Kubernetes.Namespace,
			JobImage:           cfg.Kubernetes.JobImage,
			JobImagePullPolicy: cfg.Kubernetes.JobImagePullPolicy,
			JobServiceAccount:  cfg.Kubernetes.JobServiceAccount,
			JobNamePrefix:      cfg.Kubernetes.JobNamePrefix,
			JobTTLSeconds:      cfg.Kubernetes.JobTTLSeconds,
			JobBackoffLimit:    cfg.Kubernetes.JobBackoffLimit,
			MaxEmptyReads:      cfg.Kubernetes.MaxEmptyReads,
			JobLabels:          cfg.Kubernetes.JobLabels,
			JobAnnotations:     cfg.Kubernetes.JobAnnotations,
			JobCommand:         cfg.Kubernetes.JobCommand,
			JobArgs:            cfg.Kubernetes.JobArgs,
			JobEnv:             cfg.Kubernetes.JobEnv,
			JobEnvFrom:         cfg.Kubernetes.JobEnvFrom,
		})
		if err != nil {
			return err
		}
		kubeDispatcher = dispatcher
		engine = workflow.NewOrchestratedEngine(baseEngine, dispatcher)
	}

	listener, err := net.Listen("tcp", cfg.API.GRPCListen)
	if err != nil {
		return err
	}

	var dispatcher apigrpc.FlowDispatcher
	if dbosOrchestrator != nil {
		dispatcher = dbosOrchestrator
	}
	if kubeDispatcher != nil {
		dispatcher = kubeDispatcher
	}

	server := apigrpc.New(engine, dispatcher, checkpoints, registryStore, streamStore)
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
			if streamStore != nil {
				streamStore.Close()
			}
			if dbosOrchestrator != nil {
				dbosOrchestrator.Shutdown(30 * time.Second)
			}
			_ = kubeDispatcher
			return nil
		}
		return ctx.Err()
	}
}

func resolveCheckpointBackend(cfg *config.Config) string {
	backend := strings.ToLower(strings.TrimSpace(cfg.Checkpoints.Backend))
	if backend != "" {
		return backend
	}
	if cfg.Postgres.DSN != "" {
		return "postgres"
	}
	if cfg.Checkpoints.DSN != "" || cfg.Checkpoints.Path != "" {
		return "sqlite"
	}
	return ""
}

func defaultCheckpointPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "checkpoints.db"
	}
	return filepath.Join(home, ".ductstream", "checkpoints.db")
}
