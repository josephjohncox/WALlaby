package main

import (
	"context"
	"flag"
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
		configPath    string
		flowID        string
		maxEmptyReads int
	)
	flag.StringVar(&configPath, "config", "", "path to config file")
	flag.StringVar(&flowID, "flow-id", "", "flow id to run")
	flag.IntVar(&maxEmptyReads, "max-empty-reads", 0, "stop after N empty reads (0 = continuous)")
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

	factory := runner.Factory{
		SchemaHook: &registry.Hook{
			Store:        registryStore,
			AutoApprove:  cfg.DDL.AutoApprove,
			GateApproval: cfg.DDL.Gate,
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
