package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// ViperConfig defines command-level viper bootstrap settings.
type ViperConfig struct {
	EnvPrefix        string
	ConfigEnvVar     string
	ConfigName       string
	ConfigType       string
	ConfigSearchPath []string
}

// InitViperFromCommand initializes viper with env/cmd precedence for a cobra command.
//
// The command is expected to expose a "config" flag either on itself or an ancestor.
func InitViperFromCommand(cmd *cobra.Command, cfg ViperConfig) error {
	configFlags := cmd.Flags()
	if cmd.Root() != nil && cmd.Root().PersistentFlags().Lookup("config") != nil {
		configFlags = cmd.Root().PersistentFlags()
	}
	configPath := ""
	if configFlags.Lookup("config") != nil {
		var err error
		configPath, err = configFlags.GetString("config")
		if err != nil {
			return fmt.Errorf("read config flag: %w", err)
		}
	}

	viper.Reset()
	viper.SetEnvPrefix(cfg.EnvPrefix)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	configPathConfigured := false
	if configPath != "" {
		viper.SetConfigFile(configPath)
		configPathConfigured = true
	} else if cfg.ConfigEnvVar != "" {
		if envPath := os.Getenv(cfg.ConfigEnvVar); envPath != "" {
			viper.SetConfigFile(envPath)
			configPathConfigured = true
		}
	}

	if !configPathConfigured && cfg.ConfigName != "" {
		cfgType := strings.TrimSpace(cfg.ConfigType)
		if cfgType == "" {
			cfgType = "yaml"
		}
		viper.SetConfigName(cfg.ConfigName)
		viper.SetConfigType(cfgType)
		viper.AddConfigPath(".")
		for _, path := range cfg.ConfigSearchPath {
			if trimmed := strings.TrimSpace(path); trimmed != "" {
				viper.AddConfigPath(trimmed)
			}
		}
	}

	if configPathConfigured || cfg.ConfigName != "" {
		if err := viper.ReadInConfig(); err != nil {
			var missing viper.ConfigFileNotFoundError
			if !errors.As(err, &missing) {
				return fmt.Errorf("read config: %w", err)
			}
		}
	}
	return nil
}

func ResolveStringFlag(cmd *cobra.Command, key string) string {
	value, err := cmd.Flags().GetString(key)
	if err != nil {
		return ""
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetString(key)
	}
	return value
}

func ResolveIntFlag(cmd *cobra.Command, key string) int {
	value, err := cmd.Flags().GetInt(key)
	if err != nil {
		return 0
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetInt(key)
	}
	return value
}

func ResolveInt64Flag(cmd *cobra.Command, key string) int64 {
	value, err := cmd.Flags().GetInt64(key)
	if err != nil {
		return 0
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetInt64(key)
	}
	return value
}

func ResolveBoolFlag(cmd *cobra.Command, key string) bool {
	value, err := cmd.Flags().GetBool(key)
	if err != nil {
		return false
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		return viper.GetBool(key)
	}
	return value
}

func ResolveStringFlagValue(cmd *cobra.Command, key string) (*string, error) {
	value, err := cmd.Flags().GetString(key)
	if err != nil {
		return nil, err
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		value = viper.GetString(key)
	}
	return &value, nil
}

func ResolveIntFlagValue(cmd *cobra.Command, key string) (*int, error) {
	value, err := cmd.Flags().GetInt(key)
	if err != nil {
		return nil, err
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		value = viper.GetInt(key)
	}
	return &value, nil
}

func ResolveInt64FlagValue(cmd *cobra.Command, key string) (*int64, error) {
	value, err := cmd.Flags().GetInt64(key)
	if err != nil {
		return nil, err
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		value = viper.GetInt64(key)
	}
	return &value, nil
}

func ResolveBoolFlagValue(cmd *cobra.Command, key string) (*bool, error) {
	value, err := cmd.Flags().GetBool(key)
	if err != nil {
		return nil, err
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		value = viper.GetBool(key)
	}
	return &value, nil
}

func ResolveFloat64FlagValue(cmd *cobra.Command, key string) (*float64, error) {
	value, err := cmd.Flags().GetFloat64(key)
	if err != nil {
		return nil, err
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		value = viper.GetFloat64(key)
	}
	return &value, nil
}

func ResolveDurationFlagValue(cmd *cobra.Command, key string) (*time.Duration, error) {
	value, err := cmd.Flags().GetDuration(key)
	if err != nil {
		return nil, err
	}
	if f := cmd.Flags().Lookup(key); f == nil || (!f.Changed && viper.IsSet(key)) {
		value = viper.GetDuration(key)
	}
	return &value, nil
}
