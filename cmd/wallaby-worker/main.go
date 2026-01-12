package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

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
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	var (
		configPath      string
		flowID          string
		maxEmptyReads   int
		mode            string
		tables          string
		schemas         string
		startLSN        string
		snapshotWorkers int
		partitionColumn string
		partitionCount  int
		resolveStaging  bool
	)
	flag.StringVar(&configPath, "config", "", "path to config file")
	flag.StringVar(&flowID, "flow-id", "", "flow id to run")
	flag.IntVar(&maxEmptyReads, "max-empty-reads", 0, "stop after N empty reads (0 = continuous)")
	flag.StringVar(&mode, "mode", "cdc", "source mode: cdc or backfill")
	flag.StringVar(&tables, "tables", "", "comma-separated tables for backfill (schema.table)")
	flag.StringVar(&schemas, "schemas", "", "comma-separated schemas for backfill")
	flag.StringVar(&startLSN, "start-lsn", "", "override start LSN for replay")
	flag.IntVar(&snapshotWorkers, "snapshot-workers", 0, "parallel workers for backfill snapshots")
	flag.StringVar(&partitionColumn, "partition-column", "", "partition column for backfill hashing")
	flag.IntVar(&partitionCount, "partition-count", 0, "partition count per table for backfill hashing")
	flag.BoolVar(&resolveStaging, "resolve-staging", false, "resolve destination staging tables after batch/backfill runs")
	flag.Parse()

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
	destinations, err := factory.Destinations(flowDef.Destinations)
	if err != nil {
		return fmt.Errorf("build destinations: %w", err)
	}

	flowRunner := runner.FlowRunner{
		Engine:         engine,
		Checkpoints:    checkpoints,
		Tracer:         tracer,
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
