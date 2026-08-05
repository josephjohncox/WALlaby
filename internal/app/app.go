package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // pprof is gated by config.
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	apigrpc "github.com/josephjohncox/wallaby/internal/api/grpc"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/config"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/ddl"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/orchestrator"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/schemabaseline"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

// Run wires up core services. It will grow as implementations land.
func Run(ctx context.Context, cfg *config.Config) error {
	telemetryProvider, err := telemetry.NewProvider(ctx, cfg.Telemetry)
	if err != nil {
		return err
	}

	cleanupFns := make([]func(), 0, 10)
	addCleanup := func(fn func()) {
		if fn != nil {
			cleanupFns = append(cleanupFns, fn)
		}
	}
	defer func() {
		for i := len(cleanupFns) - 1; i >= 0; i-- {
			cleanupFns[i]()
		}
	}()
	addCleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = telemetryProvider.Shutdown(shutdownCtx)
	})

	if cfg.Profiling.Enabled {
		pprofServer := &http.Server{
			Addr:              cfg.Profiling.Listen,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			log.Printf("pprof server listening on %s", cfg.Profiling.Listen)
			if err := pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("pprof server error: %v", err)
			}
		}()
		addCleanup(func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = pprofServer.Shutdown(shutdownCtx)
		})
	}

	tracer := telemetry.Tracer(cfg.Telemetry.ServiceName)
	var baseEngine workflow.LifecycleStore
	var engine workflow.ControlEngine
	var checkpoints connector.CheckpointStore
	var registryStore registry.Store
	var ddlExecutions stream.DDLExecutionStore
	var dbosOrchestrator *orchestrator.DBOSOrchestrator
	var kubeDispatcher *orchestrator.KubernetesDispatcher
	var streamStore *pgstream.Store
	var traceSink stream.TraceSink
	var traceClose func() error
	tracePath := cfg.Trace.Path
	postgresDSN := cfg.Postgres.DSN
	var controlPool *pgxpool.Pool
	var authorityStore authority.Store
	var deliveryCoordinator *delivery.Coordinator
	var schemaBaselines connector.ManagedSchemaBaselineStore
	if postgresDSN != "" {
		control, err := controlstore.New(ctx, postgresDSN)
		if err != nil {
			return fmt.Errorf("start shared control store: %w", err)
		}
		addCleanup(control.Close)
		controlPool = control.Pool()
		if err := controlplane.ApplyMigrations(ctx, controlPool); err != nil {
			return fmt.Errorf("migrate shared control store: %w", err)
		}
		authorityStore, err = authority.NewPostgresStore(controlPool)
		if err != nil {
			return fmt.Errorf("start authority store: %w", err)
		}
		deliveryCoordinator, err = delivery.NewCoordinator(ctx, controlPool)
		if err != nil {
			return fmt.Errorf("start delivery coordinator: %w", err)
		}
		schemaBaselines, err = schemabaseline.NewStore(controlPool)
		if err != nil {
			return fmt.Errorf("start managed schema-baseline store: %w", err)
		}
	}
	if cfg.Workflow.Store == "memory" && (cfg.DBOS.Enabled || cfg.Kubernetes.Enabled) {
		return errors.New("memory workflow store cannot be used with DBOS or Kubernetes dispatch")
	}
	switch cfg.Workflow.Store {
	case "postgres":
		if controlPool == nil {
			return errors.New("postgres workflow store requires WALLABY_POSTGRES_DSN")
		}
		postgresEngine, err := workflow.NewPostgresEngineWithPool(ctx, controlPool)
		if err != nil {
			return err
		}
		baseEngine = postgresEngine
		addCleanup(postgresEngine.Close)
	case "memory":
		baseEngine = workflow.NewMemoryEngine()
	default:
		return fmt.Errorf("unsupported workflow store: %s", cfg.Workflow.Store)
	}
	if controlPool != nil {
		store, err := registry.NewPostgresStoreWithPool(ctx, controlPool)
		if err != nil {
			return err
		}
		registryStore = store
		ddlExecutions = store
		addCleanup(store.Close)

		streamStore, err = pgstream.NewStore(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
		addCleanup(func() {
			streamStore.Close()
		})
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
		addCleanup(func() {
			_ = traceClose()
		})
	}

	backend := resolveCheckpointBackend(cfg)
	switch backend {
	case "", "none":
	case "postgres":
		if postgresDSN == "" {
			return errors.New("checkpoint backend postgres requires WALLABY_POSTGRES_DSN")
		}
		checkpointStore, err := checkpoint.NewPostgresStoreWithPool(ctx, controlPool)
		if err != nil {
			return err
		}
		checkpoints = checkpointStore
		addCleanup(func() {
			checkpointStore.Close()
		})
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
		addCleanup(func() {
			if err := checkpointStore.Close(); err != nil {
				log.Printf("close checkpoint store: %v", err)
			}
		})
	default:
		return errors.New("unsupported checkpoint backend: " + backend)
	}

	ddlDefaults := flow.DDLPolicyDefaults{
		Gate:        cfg.DDL.Gate,
		AutoApprove: cfg.DDL.AutoApprove,
		AutoApply:   cfg.DDL.AutoApply,
	}
	factory := runner.Factory{
		Meters: telemetryProvider.Meters(), ManagedControl: controlPool, ManagedAuthority: authorityStore,
	}
	if registryStore != nil {
		factory.SchemaHookForFlow = func(f flow.Flow) replication.SchemaHook {
			policy := flow.ResolveDDLPolicy(f.Config.DDL, &ddlDefaults)
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
			AutoApprove:  ddlDefaults.AutoApprove,
			GateApproval: ddlDefaults.Gate,
			AutoApply:    ddlDefaults.AutoApply,
		}
	}

	if cfg.DDL.CatalogEnabled && cfg.Postgres.DSN != "" && registryStore != nil {
		catalogRegistry, ok := registryStore.(ddl.CatalogRegistry)
		if !ok {
			return errors.New("configured DDL registry does not support atomic catalog changes")
		}
		pool, err := pgxpool.New(ctx, cfg.Postgres.DSN)
		if err != nil {
			return err
		}
		scanner := &ddl.CatalogScanner{
			Pool:        pool,
			Registry:    catalogRegistry,
			Schemas:     cfg.DDL.CatalogSchemas,
			AutoApprove: cfg.DDL.AutoApprove,
		}
		cleanupCatalogScanner := startCatalogScanner(ctx, scanner, cfg.DDL.CatalogInterval, pool)
		addCleanup(func() {
			cleanupCatalogScanner()
		})
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
			AppName:           cfg.DBOS.AppName,
			DatabaseURL:       cfg.Postgres.DSN,
			Queue:             cfg.DBOS.Queue,
			Schedule:          cfg.DBOS.Schedule,
			MaxEmptyReads:     maxEmptyReads,
			MaxRetries:        cfg.DBOS.MaxRetries,
			MaxRetriesSet:     cfg.DBOS.MaxRetriesSet,
			DefaultWire:       connector.WireFormat(cfg.Wire.DefaultFormat),
			StrictWire:        cfg.Wire.Enforce,
			Tracer:            tracer,
			Meters:            telemetryProvider.Meters(),
			DDLExecutions:     ddlExecutions,
			DDLPolicyDefaults: &ddlDefaults,
			TraceSink:         traceSink,
			TracePath:         tracePath,
			Authority:         authorityStore,
			Deliveries:        deliveryCoordinator,
			SchemaBaselines:   schemaBaselines,
			Artifacts:         runner.NewArtifactLogFactory(controlPool, cfg.Artifacts, cfg.Iceberg),
		}, baseEngine, checkpoints, factory)
		if err != nil {
			return err
		}
		dbosOrchestrator = dbosRunner
		addCleanup(func() {
			dbosOrchestrator.Shutdown(30 * time.Second)
		})
	}
	if cfg.Kubernetes.Enabled {
		dispatcher, err := orchestrator.NewKubernetesDispatcher(ctx, orchestrator.KubernetesConfig{
			KubeconfigPath:                  cfg.Kubernetes.KubeconfigPath,
			KubeContext:                     cfg.Kubernetes.KubeContext,
			APIServer:                       cfg.Kubernetes.APIServer,
			BearerToken:                     cfg.Kubernetes.BearerToken,
			CAFile:                          cfg.Kubernetes.CAFile,
			CAData:                          cfg.Kubernetes.CAData,
			ClientCertFile:                  cfg.Kubernetes.ClientCertFile,
			ClientKeyFile:                   cfg.Kubernetes.ClientKeyFile,
			InsecureSkipTLS:                 cfg.Kubernetes.InsecureSkipTLS,
			Namespace:                       cfg.Kubernetes.Namespace,
			JobImage:                        cfg.Kubernetes.JobImage,
			JobImagePullPolicy:              cfg.Kubernetes.JobImagePullPolicy,
			JobServiceAccount:               cfg.Kubernetes.JobServiceAccount,
			JobAutomountServiceAccountToken: cfg.Kubernetes.JobAutomountServiceAccountToken,
			JobNamePrefix:                   cfg.Kubernetes.JobNamePrefix,
			JobTTLSeconds:                   cfg.Kubernetes.JobTTLSeconds,
			JobBackoffLimit:                 cfg.Kubernetes.JobBackoffLimit,
			MaxEmptyReads:                   cfg.Kubernetes.MaxEmptyReads,
			JobLabels:                       cfg.Kubernetes.JobLabels,
			JobAnnotations:                  cfg.Kubernetes.JobAnnotations,
			JobCommand:                      cfg.Kubernetes.JobCommand,
			JobArgs:                         cfg.Kubernetes.JobArgs,
			JobEnv:                          cfg.Kubernetes.JobEnv,
			JobEnvFrom:                      cfg.Kubernetes.JobEnvFrom,
		})
		if err != nil {
			return err
		}
		kubeDispatcher = dispatcher
	}

	var lifecycleDispatcher workflow.Dispatcher = workflow.PassiveDispatcher{}
	if dbosOrchestrator != nil {
		lifecycleDispatcher = dbosOrchestrator
	}
	if kubeDispatcher != nil {
		lifecycleDispatcher = kubeDispatcher
	}
	var resourceCleaners []workflow.SourceResourceCleaner
	if cleanupAuthority, ok := authorityStore.(authority.CleanupStore); ok {
		resourceCleaners = append(resourceCleaners, runner.ManagedSourceCleanup{Factory: factory, Authority: cleanupAuthority})
	}
	orchestratedEngine := workflow.NewOrchestratedEngine(baseEngine, lifecycleDispatcher, telemetryProvider.Meters(), resourceCleaners...)
	engine = orchestratedEngine
	// Reconciliation is started before the server accepts lifecycle requests.
	go orchestratedEngine.RunReconciler(ctx, time.Second)

	listener, err := net.Listen("tcp", cfg.API.GRPCListen)
	if err != nil {
		return err
	}

	var dispatcher apigrpc.RunOnceDispatcher
	if dbosOrchestrator != nil {
		dispatcher = dbosOrchestrator
	}
	if kubeDispatcher != nil {
		dispatcher = kubeDispatcher
	}

	server := apigrpc.New(engine, dispatcher, checkpoints, registryStore, streamStore, cfg.API.GRPCReflection, telemetryProvider.Meters())
	addCleanup(func() {
		_ = listener.Close()
	})
	addCleanup(func() {
		server.Stop()
	})
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.Canceled) {
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

func startCatalogScanner(ctx context.Context, scanner *ddl.CatalogScanner, interval time.Duration, pool *pgxpool.Pool) func() {
	catalogCtx, cancel := context.WithCancel(ctx)
	catalogDone := make(chan struct{})

	go func() {
		defer close(catalogDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if err := scanner.RunOnce(catalogCtx); err != nil {
				log.Printf("ddl catalog scan error: %v", err)
			}
			select {
			case <-catalogCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return func() {
		cancel()
		select {
		case <-catalogDone:
		case <-time.After(2 * time.Second):
			log.Printf("ddl catalog scanner did not stop within timeout")
		}
		pool.Close()
	}
}

func defaultCheckpointPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "checkpoints.db"
	}
	return filepath.Join(home, ".wallaby", "checkpoints.db")
}
