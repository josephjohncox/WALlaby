package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // pprof is gated by config.
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/cli"
	"github.com/josephjohncox/wallaby/internal/config"
	"github.com/josephjohncox/wallaby/internal/controlplane"
	"github.com/josephjohncox/wallaby/internal/controlstore"
	"github.com/josephjohncox/wallaby/internal/delivery"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"github.com/spf13/cobra"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	command := newWallabyWorkerCommand()
	if err := command.Execute(); err != nil {
		return fmt.Errorf("execute wallaby worker command: %w", err)
	}
	return nil
}

func newWallabyWorkerCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "wallaby-worker",
		Short:        "Run a single Wallaby flow worker",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWallabyWorker(cmd)
		},
	}
	command.PersistentFlags().String("config", "", "path to config file")
	command.Flags().String("flow-id", "", "flow id to run")
	command.Flags().Int64("generation", 0, "expected lifecycle generation (0 resolves current generation for standalone runs)")
	command.Flags().String("execution-backend", "worker", "authoritative execution backend")
	command.Flags().String("execution-id", "", "authoritative execution id (injected by managed dispatchers)")
	command.Flags().Int("max-empty-reads", 0, "stop after N empty reads (0 = continuous)")
	command.Flags().String("mode", connector.SourceModeCDC, "source mode: cdc or backfill")
	command.Flags().String("tables", "", "comma-separated tables for backfill (schema.table)")
	command.Flags().String("schemas", "", "comma-separated schemas for backfill")
	command.Flags().String("start-lsn", "", "override start LSN for replay")
	command.Flags().Int("snapshot-workers", 0, "parallel workers for backfill snapshots")
	command.Flags().String("partition-column", "", "partition column for backfill hashing")
	command.Flags().Int("partition-count", 0, "partition count per table for backfill hashing")
	command.Flags().Bool("resolve-staging", false, "resolve destination staging tables after batch/backfill runs")
	command.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		if err := initWallabyWorkerConfig(cmd); err != nil {
			return fmt.Errorf("initialize wallaby worker config: %w", err)
		}
		return nil
	}
	command.InitDefaultCompletionCmd()
	return command
}

func initWallabyWorkerConfig(cmd *cobra.Command) error {
	if err := cli.InitViperFromCommand(cmd, cli.ViperConfig{
		EnvPrefix:        "WALLABY_WORKER",
		ConfigEnvVar:     "WALLABY_WORKER_CONFIG",
		ConfigName:       "wallaby-worker",
		ConfigType:       "yaml",
		ConfigSearchPath: nil,
	}); err != nil {
		return fmt.Errorf("initialize worker viper config: %w", err)
	}
	return nil
}

func runWallabyWorker(cmd *cobra.Command) error {
	configPath := cli.ResolveStringFlag(cmd, "config")
	flowID := cli.ResolveStringFlag(cmd, "flow-id")
	generation, err := cmd.Flags().GetInt64("generation")
	if err != nil {
		return err
	}
	executionBackend := cli.ResolveStringFlag(cmd, "execution-backend")
	executionID := cli.ResolveStringFlag(cmd, "execution-id")
	if executionBackend == "" {
		executionBackend = "worker"
	}
	if executionBackend == "kubernetes" && executionID == "" {
		return errors.New("kubernetes execution backend requires an exact execution-id")
	}
	maxEmptyReads := cli.ResolveIntFlag(cmd, "max-empty-reads")
	mode := cli.ResolveStringFlag(cmd, "mode")
	tables := cli.ResolveStringFlag(cmd, "tables")
	schemas := cli.ResolveStringFlag(cmd, "schemas")
	startLSN := cli.ResolveStringFlag(cmd, "start-lsn")
	snapshotWorkers := cli.ResolveIntFlag(cmd, "snapshot-workers")
	partitionColumn := cli.ResolveStringFlag(cmd, "partition-column")
	partitionCount := cli.ResolveIntFlag(cmd, "partition-count")
	resolveStaging := cli.ResolveBoolFlag(cmd, "resolve-staging")

	if flowID == "" {
		return errors.New("flow-id is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if cfg.Postgres.DSN == "" {
		return errors.New("WALLABY_POSTGRES_DSN is required to run a flow worker")
	}

	telemetryProvider, err := telemetry.NewProvider(ctx, cfg.Telemetry)
	if err != nil {
		return fmt.Errorf("init telemetry: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = telemetryProvider.Shutdown(shutdownCtx)
	}()

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
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = pprofServer.Shutdown(shutdownCtx)
		}()
	}

	tracer := telemetry.Tracer(cfg.Telemetry.ServiceName)

	control, err := controlstore.New(ctx, cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("start shared control store: %w", err)
	}
	defer control.Close()
	controlPool := control.Pool()
	if err := controlplane.ApplyMigrations(ctx, controlPool); err != nil {
		return fmt.Errorf("migrate shared control store: %w", err)
	}

	engine, err := workflow.NewPostgresEngineWithPool(ctx, controlPool)
	if err != nil {
		return fmt.Errorf("start workflow engine: %w", err)
	}
	defer engine.Close()

	checkpoints, err := checkpoint.NewPostgresStoreWithPool(ctx, controlPool)
	if err != nil {
		return fmt.Errorf("start checkpoint store: %w", err)
	}
	defer checkpoints.Close()

	authorityStore, err := authority.NewPostgresStore(controlPool)
	if err != nil {
		return fmt.Errorf("start authority store: %w", err)
	}
	deliveryCoordinator, err := delivery.NewCoordinator(ctx, controlPool)
	if err != nil {
		return fmt.Errorf("start delivery coordinator: %w", err)
	}

	registryStore, err := registry.NewPostgresStoreWithPool(ctx, controlPool)
	if err != nil {
		return fmt.Errorf("start registry store: %w", err)
	}
	defer registryStore.Close()

	flowDef, err := engine.Get(ctx, flowID)
	if err != nil {
		return fmt.Errorf("load flow: %w", err)
	}

	flowSource := flowDef.Source
	if flowSource.Options != nil {
		flowSource.Options = copyStringMap(flowDef.Source.Options)
	}

	if maxEmptyReads > 0 {
		if flowSource.Options == nil {
			flowSource.Options = map[string]string{}
		}
		if flowSource.Options["emit_empty"] == "" {
			flowSource.Options["emit_empty"] = "true"
		}
	}
	mode, err = connector.NormalizeSourceMode(mode)
	if err != nil {
		return err
	}

	if mode != connector.SourceModeCDC {
		if flowSource.Options == nil {
			flowSource.Options = map[string]string{}
		}
		flowSource.Options["mode"] = mode
		if tables != "" {
			flowSource.Options["tables"] = tables
		}
		if schemas != "" {
			flowSource.Options["schemas"] = schemas
		}
		if snapshotWorkers > 0 {
			flowSource.Options["snapshot_workers"] = fmt.Sprintf("%d", snapshotWorkers)
		}
		if partitionColumn != "" {
			flowSource.Options["partition_column"] = partitionColumn
		}
		if partitionCount > 0 {
			flowSource.Options["partition_count"] = fmt.Sprintf("%d", partitionCount)
		}
	}
	if startLSN != "" {
		if flowSource.Options == nil {
			flowSource.Options = map[string]string{}
		}
		flowSource.Options["start_lsn"] = startLSN
	}

	runFlow := flowDef
	runFlow.Source = flowSource
	if flowDef.WireFormat == "" && cfg.Wire.DefaultFormat != "" {
		runFlow.WireFormat = connector.WireFormat(cfg.Wire.DefaultFormat)
	}

	defaults := flow.DDLPolicyDefaults{
		Gate:        cfg.DDL.Gate,
		AutoApprove: cfg.DDL.AutoApprove,
		AutoApply:   cfg.DDL.AutoApply,
	}
	factory := runner.Factory{
		ManagedControl:   controlPool,
		ManagedAuthority: authorityStore,
		SchemaHookForFlow: func(f flow.Flow) replication.SchemaHook {
			policy := f.Config.DDL.Resolve(defaults)
			return &registry.Hook{
				Store:        registryStore,
				FlowID:       f.ID,
				AutoApprove:  policy.AutoApprove,
				GateApproval: policy.Gate,
				AutoApply:    policy.AutoApply,
			}
		},
		Meters: telemetryProvider.Meters(),
		SchemaHook: &registry.Hook{
			Store:        registryStore,
			AutoApprove:  defaults.AutoApprove,
			GateApproval: defaults.Gate,
			AutoApply:    defaults.AutoApply,
		},
	}

	source, err := factory.SourceForFlow(runFlow)
	if err != nil {
		return fmt.Errorf("build source: %w", err)
	}
	destinations, err := factory.DestinationsForFlow(runFlow)
	if err != nil {
		return fmt.Errorf("build destinations: %w", err)
	}

	flowRunner := runner.FlowRunner{
		Engine:             engine,
		Checkpoints:        checkpoints,
		Tracer:             tracer,
		Meters:             telemetryProvider.Meters(),
		StrictWire:         cfg.Wire.Enforce,
		MaxEmpty:           maxEmptyReads,
		ResolveStaging:     resolveStaging,
		ExecutionBackend:   executionBackend,
		ExecutionID:        executionID,
		ExpectedGeneration: generation,
		Authority:          authorityStore,
		Deliveries:         deliveryCoordinator,
	}
	if cfg.Trace.Path != "" {
		tracePath := strings.ReplaceAll(cfg.Trace.Path, "{flow_id}", flowDef.ID)
		// #nosec G304 -- trace path is configured by the operator.
		traceFile, err := os.Create(tracePath)
		if err != nil {
			return fmt.Errorf("open trace file: %w", err)
		}
		defer func() { _ = traceFile.Close() }()
		flowRunner.TraceSink = stream.NewJSONTraceSink(traceFile)
	}
	flowRunner.DDLExecutions = registryStore
	if flowRunner.WireFormat == "" && cfg.Wire.DefaultFormat != "" {
		flowRunner.WireFormat = connector.WireFormat(cfg.Wire.DefaultFormat)
	}

	if err := flowRunner.Run(ctx, runFlow, source, destinations); err != nil {
		return fmt.Errorf("run flow: %w", err)
	}
	return nil
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
