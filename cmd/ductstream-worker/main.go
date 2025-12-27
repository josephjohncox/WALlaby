package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/josephjohncox/ductstream/internal/checkpoint"
	"github.com/josephjohncox/ductstream/internal/config"
	"github.com/josephjohncox/ductstream/internal/registry"
	"github.com/josephjohncox/ductstream/internal/runner"
	"github.com/josephjohncox/ductstream/internal/telemetry"
	"github.com/josephjohncox/ductstream/internal/workflow"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

func main() {
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
	flag.Parse()

	if flowID == "" {
		log.Fatal("flow-id is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if cfg.Postgres.DSN == "" {
		log.Fatal("DUCTSTREAM_POSTGRES_DSN is required to run a flow worker")
	}

	tracer := telemetry.Tracer(cfg.Telemetry.ServiceName)

	engine, err := workflow.NewPostgresEngine(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatalf("start workflow engine: %v", err)
	}
	defer engine.Close()

	checkpoints, err := checkpoint.NewPostgresStore(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatalf("start checkpoint store: %v", err)
	}
	defer checkpoints.Close()

	registryStore, err := registry.NewPostgresStore(ctx, cfg.Postgres.DSN)
	if err != nil {
		log.Fatalf("start registry store: %v", err)
	}
	defer registryStore.Close()

	flowDef, err := engine.Get(ctx, flowID)
	if err != nil {
		log.Fatalf("load flow: %v", err)
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

	factory := runner.Factory{
		SchemaHook: &registry.Hook{
			Store:        registryStore,
			AutoApprove:  cfg.DDL.AutoApprove,
			GateApproval: cfg.DDL.Gate,
			AutoApply:    cfg.DDL.AutoApply,
		},
	}

	source, err := factory.Source(flowDef.Source)
	if err != nil {
		log.Fatalf("build source: %v", err)
	}
	destinations, err := factory.Destinations(flowDef.Destinations)
	if err != nil {
		log.Fatalf("build destinations: %v", err)
	}

	flowRunner := runner.FlowRunner{
		Engine:      engine,
		Checkpoints: checkpoints,
		Tracer:      tracer,
		StrictWire:  cfg.Wire.Enforce,
		MaxEmpty:    maxEmptyReads,
	}
	if flowRunner.WireFormat == "" && cfg.Wire.DefaultFormat != "" {
		flowRunner.WireFormat = connector.WireFormat(cfg.Wire.DefaultFormat)
	}

	if err := flowRunner.Run(ctx, flowDef, source, destinations); err != nil {
		log.Fatalf("run flow: %v", err)
	}
}
