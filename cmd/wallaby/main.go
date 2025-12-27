package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/josephjohncox/wallaby/internal/app"
	"github.com/josephjohncox/wallaby/internal/config"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to config file")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if err := app.Run(ctx, cfg); err != nil {
		log.Fatalf("wallaby stopped: %v", err)
	}
}
