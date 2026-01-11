package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	apigrpc "github.com/josephjohncox/wallaby/internal/api/grpc"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/config"
	"github.com/josephjohncox/wallaby/internal/ddl"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/orchestrator"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

// Run wires up core services. It will grow as implementations land.
func Run(ctx context.Context, cfg *config.Config) error {
	// Initialize telemetry provider
	telemetryProvider, err := telemetry.NewProvider(ctx, cfg.Telemetry)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = telemetryProvider.Shutdown(shutdownCtx)
	}()

	tracer := telemetry.Tracer(cfg.Telemetry.ServiceName)
	var engine workflow.Engine = workflow.NewNoopEngine()
	baseEngine := engine
	var checkpoints connector.CheckpointStore
	var registryStore registry.Store
	var registryCloser interface{ Close() }
	var ddlApplied func(context.Context, string, string, string) error
	var dbosOrchestrator *orchestrator.DBOSOrchestrator
	var kubeDispatcher *orchestrator.KubernetesDispatcher
	var streamStore *pgstream.Store
	var traceSink stream.TraceSink
	var traceClose func() error
	tracePath := cfg.Trace.Path
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
		ddlApplied = func(ctx context.Context, flowID string, lsn string, _ string) error {
			return registry.MarkDDLAppliedByLSN(ctx, registryStore, flowID, lsn)
		}

		streamStore, err = pgstream.NewStore(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
	}

	if cfg.DBOS.Enabled && tracePath != "" && strings.Contains(tracePath, "{flow_id}") {
		// DBOS will manage per-flow trace sinks.
	} else if tracePath != "" {
		tracePath = strings.ReplaceAll(tracePath, "{flow_id}", "server")
		// #nosec G304 -- path is configured by the operator.
		traceFile, err := os.Create(tracePath)
		if err != nil {
			return fmt.Errorf("open trace file: %w", err)
		}
		traceClose = traceFile.Close
		traceSink = stream.NewJSONTraceSink(traceFile)
	}
	if traceClose != nil {
		defer func() { _ = traceClose() }()
	}

	backend := resolveCheckpointBackend(cfg)
	switch backend {
	case "", "none":
	case "postgres":
		if postgresDSN == "" {
			return errors.New("checkpoint backend postgres requires WALLABY_POSTGRES_DSN")
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
		defaults := flow.DDLPolicyDefaults{
			Gate:        cfg.DDL.Gate,
			AutoApprove: cfg.DDL.AutoApprove,
			AutoApply:   cfg.DDL.AutoApply,
		}
		factory.SchemaHookForFlow = func(f flow.Flow) replication.SchemaHook {
			policy := f.Config.DDL.Resolve(defaults)
			return &registry.Hook{
				Store:        registryStore,
				FlowID:       f.ID,
				AutoApprove:  policy.AutoApprove,
				GateApproval: policy.Gate,
				AutoApply:    policy.AutoApply,
			}
		}
		factory.SchemaHook = &registry.Hook{
			Store:        registryStore,
			AutoApprove:  defaults.AutoApprove,
			GateApproval: defaults.Gate,
			AutoApply:    defaults.AutoApply,
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
			MaxRetries:    cfg.DBOS.MaxRetries,
			MaxRetriesSet: cfg.DBOS.MaxRetriesSet,
			DefaultWire:   connector.WireFormat(cfg.Wire.DefaultFormat),
			StrictWire:    cfg.Wire.Enforce,
			Tracer:        tracer,
			Meters:        telemetryProvider.Meters(),
			DDLApplied:    ddlApplied,
			TraceSink:     traceSink,
			TracePath:     tracePath,
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

	server := apigrpc.New(engine, dispatcher, checkpoints, registryStore, streamStore, cfg.API.GRPCReflection)
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
	return filepath.Join(home, ".wallaby", "checkpoints.db")
}
