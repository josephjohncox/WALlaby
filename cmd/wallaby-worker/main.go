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

	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/config"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	command := newWallabyWorkerCommand()
	return command.Execute()
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
	command.Flags().Int("max-empty-reads", 0, "stop after N empty reads (0 = continuous)")
	command.Flags().String("mode", "cdc", "source mode: cdc or backfill")
	command.Flags().String("tables", "", "comma-separated tables for backfill (schema.table)")
	command.Flags().String("schemas", "", "comma-separated schemas for backfill")
	command.Flags().String("start-lsn", "", "override start LSN for replay")
	command.Flags().Int("snapshot-workers", 0, "parallel workers for backfill snapshots")
	command.Flags().String("partition-column", "", "partition column for backfill hashing")
	command.Flags().Int("partition-count", 0, "partition count per table for backfill hashing")
	command.Flags().Bool("resolve-staging", false, "resolve destination staging tables after batch/backfill runs")
	command.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		return initWallabyWorkerConfig(cmd)
	}
	command.InitDefaultCompletionCmd()
	return command
}

func initWallabyWorkerConfig(cmd *cobra.Command) error {
	configFlags := cmd.Flags()
	if cmd.Root() != nil && cmd.Root().PersistentFlags().Lookup("config") != nil {
		configFlags = cmd.Root().PersistentFlags()
	}
	configPath, err := configFlags.GetString("config")
	if err != nil {
		return fmt.Errorf("read config flag: %w", err)
	}

	viper.Reset()
	viper.SetEnvPrefix("WALLABY_WORKER")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else if envPath := os.Getenv("WALLABY_WORKER_CONFIG"); envPath != "" {
		viper.SetConfigFile(envPath)
	} else {
		viper.SetConfigName("wallaby-worker")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
	}

	if err := viper.ReadInConfig(); err != nil {
		var missing viper.ConfigFileNotFoundError
		if !errors.As(err, &missing) {
			return fmt.Errorf("read config: %w", err)
		}
	}

	return nil
}

func resolveStringFlag(cmd *cobra.Command, key string) string {
	value, err := cmd.Flags().GetString(key)
	if err != nil {
		return ""
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetString(key)
	}
	return value
}

func resolveIntFlag(cmd *cobra.Command, key string) int {
	value, err := cmd.Flags().GetInt(key)
	if err != nil {
		return 0
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetInt(key)
	}
	return value
}

func resolveBoolFlag(cmd *cobra.Command, key string) bool {
	value, err := cmd.Flags().GetBool(key)
	if err != nil {
		return false
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetBool(key)
	}
	return value
}

func runWallabyWorker(cmd *cobra.Command) error {
	configPath := resolveStringFlag(cmd, "config")
	flowID := resolveStringFlag(cmd, "flow-id")
	maxEmptyReads := resolveIntFlag(cmd, "max-empty-reads")
	mode := resolveStringFlag(cmd, "mode")
	tables := resolveStringFlag(cmd, "tables")
	schemas := resolveStringFlag(cmd, "schemas")
	startLSN := resolveStringFlag(cmd, "start-lsn")
	snapshotWorkers := resolveIntFlag(cmd, "snapshot-workers")
	partitionColumn := resolveStringFlag(cmd, "partition-column")
	partitionCount := resolveIntFlag(cmd, "partition-count")
	resolveStaging := resolveBoolFlag(cmd, "resolve-staging")

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

	engine, err := workflow.NewPostgresEngine(ctx, cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("start workflow engine: %w", err)
	}
	defer engine.Close()

	checkpoints, err := checkpoint.NewPostgresStore(ctx, cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("start checkpoint store: %w", err)
	}
	defer checkpoints.Close()

	registryStore, err := registry.NewPostgresStore(ctx, cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("start registry store: %w", err)
	}
	defer registryStore.Close()

	flowDef, err := engine.Get(ctx, flowID)
	if err != nil {
		return fmt.Errorf("load flow: %w", err)
	}

	if flowDef.WireFormat == "" && cfg.Wire.DefaultFormat != "" {
		flowDef.WireFormat = connector.WireFormat(cfg.Wire.DefaultFormat)
	}

	if maxEmptyReads > 0 {
		if flowDef.Source.Options == nil {
			flowDef.Source.Options = map[string]string{}
		}
		if flowDef.Source.Options["emit_empty"] == "" {
			flowDef.Source.Options["emit_empty"] = "true"
		}
	}
	if mode != "" && mode != "cdc" {
		if flowDef.Source.Options == nil {
			flowDef.Source.Options = map[string]string{}
		}
		flowDef.Source.Options["mode"] = mode
		if tables != "" {
			flowDef.Source.Options["tables"] = tables
		}
		if schemas != "" {
			flowDef.Source.Options["schemas"] = schemas
		}
		if snapshotWorkers > 0 {
			flowDef.Source.Options["snapshot_workers"] = fmt.Sprintf("%d", snapshotWorkers)
		}
		if partitionColumn != "" {
			flowDef.Source.Options["partition_column"] = partitionColumn
		}
		if partitionCount > 0 {
			flowDef.Source.Options["partition_count"] = fmt.Sprintf("%d", partitionCount)
		}
	}
	if startLSN != "" {
		if flowDef.Source.Options == nil {
			flowDef.Source.Options = map[string]string{}
		}
		flowDef.Source.Options["start_lsn"] = startLSN
	}

	defaults := flow.DDLPolicyDefaults{
		Gate:        cfg.DDL.Gate,
		AutoApprove: cfg.DDL.AutoApprove,
		AutoApply:   cfg.DDL.AutoApply,
	}
	factory := runner.Factory{
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
		SchemaHook: &registry.Hook{
			Store:        registryStore,
			AutoApprove:  defaults.AutoApprove,
			GateApproval: defaults.Gate,
			AutoApply:    defaults.AutoApply,
		},
	}

	source, err := factory.SourceForFlow(flowDef)
	if err != nil {
		return fmt.Errorf("build source: %w", err)
	}
	destinations, err := factory.DestinationsForFlow(flowDef)
	if err != nil {
		return fmt.Errorf("build destinations: %w", err)
	}

	flowRunner := runner.FlowRunner{
		Engine:         engine,
		Checkpoints:    checkpoints,
		Tracer:         tracer,
		Meters:         telemetryProvider.Meters(),
		StrictWire:     cfg.Wire.Enforce,
		MaxEmpty:       maxEmptyReads,
		ResolveStaging: resolveStaging,
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
	if registryStore != nil {
		flowRunner.DDLApplied = func(ctx context.Context, flowID string, lsn string, _ string) error {
			return registry.MarkDDLAppliedByLSN(ctx, registryStore, flowID, lsn)
		}
	}
	if flowRunner.WireFormat == "" && cfg.Wire.DefaultFormat != "" {
		flowRunner.WireFormat = connector.WireFormat(cfg.Wire.DefaultFormat)
	}

	if err := flowRunner.Run(ctx, flowDef, source, destinations); err != nil {
		return fmt.Errorf("run flow: %w", err)
	}
	return nil
}
