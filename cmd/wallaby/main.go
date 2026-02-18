package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/josephjohncox/wallaby/internal/app"
	"github.com/josephjohncox/wallaby/internal/cli"
	"github.com/josephjohncox/wallaby/internal/config"
	"github.com/spf13/cobra"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	command := newWallabyCommand()
	return command.Execute()
}

func newWallabyCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "wallaby",
		Short:        "Run the Wallaby service",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWallaby(cmd)
		},
	}
	command.PersistentFlags().String("config", "", "path to config file")
	command.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		return initWallabyConfig(cmd)
	}
	command.InitDefaultCompletionCmd()
	return command
}

func initWallabyConfig(cmd *cobra.Command) error {
	return cli.InitViperFromCommand(cmd, cli.ViperConfig{
		EnvPrefix:        "WALLABY",
		ConfigEnvVar:     "WALLABY_CONFIG",
		ConfigName:       "wallaby",
		ConfigType:       "yaml",
		ConfigSearchPath: nil,
	})
}

func runWallaby(cmd *cobra.Command) error {
	configPath := cli.ResolveStringFlag(cmd, "config")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if err := app.Run(ctx, cfg); err != nil {
		return fmt.Errorf("wallaby stopped: %w", err)
	}
	return nil
}
