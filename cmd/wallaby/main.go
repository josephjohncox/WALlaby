package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/josephjohncox/wallaby/internal/app"
	"github.com/josephjohncox/wallaby/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	configFlags := cmd.Flags()
	if cmd.Root() != nil && cmd.Root().PersistentFlags().Lookup("config") != nil {
		configFlags = cmd.Root().PersistentFlags()
	}
	configPath, err := configFlags.GetString("config")
	if err != nil {
		return fmt.Errorf("read config flag: %w", err)
	}

	viper.Reset()
	viper.SetEnvPrefix("WALLABY")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else if envPath := os.Getenv("WALLABY_CONFIG"); envPath != "" {
		viper.SetConfigFile(envPath)
	} else {
		viper.SetConfigName("wallaby")
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

func runWallaby(cmd *cobra.Command) error {
	configPath := resolveStringFlag(cmd, "config")
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
