package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/josephjohncox/wallaby/tools/specaction"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

const cliVersion = "0.0.0-dev"

func run() error {
	command := newWallabySpeccheckCommand()
	command.SetArgs(os.Args[1:])
	return command.Execute()
}

func newWallabySpeccheckCommand() *cobra.Command {
	command := &cobra.Command{
		Use:                "wallaby-speccheck",
		Short:              "Run spec action static checker",
		Version:            cliVersion,
		SilenceUsage:       true,
		FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSpeccheck(args)
		},
	}
	command.PersistentFlags().String("config", "", "path to config file")
	command.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		return initSpeccheckConfig(cmd)
	}
	command.AddCommand(&cobra.Command{
		Use:     "version",
		Short:   "show version",
		Aliases: []string{"v"},
		Args:    cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			fmt.Printf("wallaby-speccheck %s\n", cliVersion)
			return nil
		},
	})
	command.InitDefaultCompletionCmd()
	return command
}

func initSpeccheckConfig(cmd *cobra.Command) error {
	configFlags := cmd.Flags()
	if cmd.Root() != nil && cmd.Root().PersistentFlags().Lookup("config") != nil {
		configFlags = cmd.Root().PersistentFlags()
	}
	configPath, err := configFlags.GetString("config")
	if err != nil {
		return err
	}

	viper.Reset()
	viper.SetEnvPrefix("WALLABY_SPECCHECK")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	if configPath != "" {
		viper.SetConfigFile(configPath)
	}
	if err := viper.ReadInConfig(); err != nil {
		var missing viper.ConfigFileNotFoundError
		if !errors.As(err, &missing) {
			return err
		}
	}
	return nil
}

func runSpeccheck(args []string) error {
	// Preserve analyzer-native flags, but allow common options through viper.
	enhancedArgs := append([]string{"wallaby-speccheck"}, args...)
	if viper.IsSet("manifest") && !containsArg(enhancedArgs, "--manifest", "-manifest", "-specaction.manifest") {
		enhancedArgs = append([]string{"wallaby-speccheck", "-manifest", viper.GetString("manifest")}, enhancedArgs[1:]...)
	}
	if viper.IsSet("specaction.manifest") && !containsArg(enhancedArgs, "--manifest", "-manifest", "-specaction.manifest") {
		enhancedArgs = append([]string{"wallaby-speccheck", "-manifest", viper.GetString("specaction.manifest")}, enhancedArgs[1:]...)
	}
	if viper.IsSet("verbose") && !containsArg(enhancedArgs, "--verbose", "-verbose", "--specaction.verbose", "-specaction.verbose") {
		enhancedArgs = append([]string{"wallaby-speccheck", "-verbose"}, enhancedArgs[1:]...)
	}
	if viper.IsSet("specaction.verbose") && !containsArg(enhancedArgs, "--verbose", "-verbose", "--specaction.verbose", "-specaction.verbose") {
		enhancedArgs = append([]string{"wallaby-speccheck", "-specaction.verbose"}, enhancedArgs[1:]...)
	}
	if mode := strings.TrimSpace(viper.GetString("verbose-mode")); mode != "" && !containsArg(enhancedArgs, "--verbose-mode", "-verbose-mode", "--specaction.verbose-mode", "-specaction.verbose-mode") {
		enhancedArgs = append([]string{"wallaby-speccheck", "-verbose-mode", mode}, enhancedArgs[1:]...)
	}
	if mode := strings.TrimSpace(viper.GetString("specaction.verbose-mode")); mode != "" && !containsArg(enhancedArgs, "--verbose-mode", "-verbose-mode", "--specaction.verbose-mode", "-specaction.verbose-mode") {
		enhancedArgs = append([]string{"wallaby-speccheck", "-specaction.verbose-mode", mode}, enhancedArgs[1:]...)
	}
	previousArgs := os.Args
	defer func() {
		os.Args = previousArgs
	}()
	os.Args = enhancedArgs
	singlechecker.Main(specaction.Analyzer)
	return nil
}

func containsArg(args []string, names ...string) bool {
	for _, arg := range args {
		for _, name := range names {
			if arg == name || strings.HasPrefix(arg, name+"=") {
				return true
			}
		}
	}
	return false
}
