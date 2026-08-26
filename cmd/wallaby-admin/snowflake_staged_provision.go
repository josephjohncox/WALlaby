package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	snowflakedest "github.com/josephjohncox/wallaby/connectors/destinations/snowflake"
	_ "github.com/snowflakedb/gosnowflake" // Explicit owner-only administration command.
	"github.com/spf13/cobra"
)

func addSnowflakeStagedProvisionCommand(root *cobra.Command) {
	snowflakeCommand := &cobra.Command{Use: "snowflake", Short: "manage Snowflake owner operations"}
	stagedCommand := &cobra.Command{Use: "staged", Short: "manage staged COPY owner operations"}
	provisionCommand := &cobra.Command{Use: "provision", Short: "inspect and reconcile staged catalog provisioning"}
	add := func(name, short string, needAttempt bool, run func(context.Context, *sql.DB, snowflakedest.ManagedStagedProvisionSpec, string, int64) (any, error)) *cobra.Command {
		var specPath, ownerDSN, attemptID string
		var epoch int64
		command := &cobra.Command{Use: name, Short: short, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
			if strings.TrimSpace(ownerDSN) == "" {
				return errors.New("--owner-dsn is required and is never persisted")
			}
			if needAttempt && (strings.TrimSpace(attemptID) == "" || epoch <= 0) {
				return errors.New("--attempt-id and positive --epoch are required")
			}
			spec, err := loadManagedStagedProvisionSpec(specPath)
			if err != nil {
				return err
			}
			db, err := sql.Open("snowflake", ownerDSN)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()
			ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Minute)
			defer cancel()
			if err := db.PingContext(ctx); err != nil {
				return fmt.Errorf("connect Snowflake owner session: %w", err)
			}
			result, err := run(ctx, db, spec, attemptID, epoch)
			if err != nil {
				return err
			}
			if result == nil {
				return nil
			}
			encoder := json.NewEncoder(cmd.OutOrStdout())
			encoder.SetIndent("", "  ")
			return encoder.Encode(result)
		}}
		command.Flags().StringVar(&specPath, "spec", "", "path to strict non-secret staged provision JSON")
		command.Flags().StringVar(&ownerDSN, "owner-dsn", "", "ephemeral Snowflake owner DSN; never persisted")
		if needAttempt {
			command.Flags().StringVar(&attemptID, "attempt-id", "", "lowercase provision attempt UUID")
			command.Flags().Int64Var(&epoch, "epoch", 0, "expected durable provision epoch")
		}
		_ = command.MarkFlagRequired("spec")
		_ = command.MarkFlagRequired("owner-dsn")
		provisionCommand.AddCommand(command)
		return command
	}
	bootstrap := add("bootstrap", "install current staged auxiliary objects and first catalog authority", false, func(ctx context.Context, db *sql.DB, spec snowflakedest.ManagedStagedProvisionSpec, _ string, _ int64) (any, error) {
		return snowflakedest.BootstrapManagedStagedProvision(ctx, db, spec)
	})
	bootstrap.Aliases = []string{"install"}
	add("inspect", "inspect durable and live staged catalog identity", false, func(ctx context.Context, db *sql.DB, spec snowflakedest.ManagedStagedProvisionSpec, _ string, _ int64) (any, error) {
		return snowflakedest.InspectManagedStagedProvision(ctx, db, spec)
	})
	add("start", "start an exclusive owner provision attempt", true, func(ctx context.Context, db *sql.DB, spec snowflakedest.ManagedStagedProvisionSpec, attempt string, epoch int64) (any, error) {
		if err := snowflakedest.BeginManagedStagedProvision(ctx, db, spec, attempt, epoch); err != nil {
			return nil, err
		}
		return snowflakedest.InspectManagedStagedProvision(ctx, db, spec)
	})
	add("resume", "validate live catalog and finish the exact attempt", true, func(ctx context.Context, db *sql.DB, spec snowflakedest.ManagedStagedProvisionSpec, attempt string, epoch int64) (any, error) {
		return snowflakedest.ResumeManagedStagedProvision(ctx, db, spec, attempt, epoch)
	})
	add("abort", "abort the exact unfinished provision attempt", true, func(ctx context.Context, db *sql.DB, spec snowflakedest.ManagedStagedProvisionSpec, attempt string, epoch int64) (any, error) {
		return nil, snowflakedest.AbortManagedStagedProvision(ctx, db, spec, attempt, epoch)
	})
	stagedCommand.AddCommand(provisionCommand)
	snowflakeCommand.AddCommand(stagedCommand)
	root.AddCommand(snowflakeCommand)
}

func loadManagedStagedProvisionSpec(path string) (snowflakedest.ManagedStagedProvisionSpec, error) {
	if strings.TrimSpace(path) == "" {
		return snowflakedest.ManagedStagedProvisionSpec{}, errors.New("--spec is required")
	}
	// #nosec G304 -- the operator explicitly supplies the local non-secret specification path.
	file, err := os.Open(path)
	if err != nil {
		return snowflakedest.ManagedStagedProvisionSpec{}, err
	}
	defer func() { _ = file.Close() }()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var spec snowflakedest.ManagedStagedProvisionSpec
	if err := decoder.Decode(&spec); err != nil {
		return snowflakedest.ManagedStagedProvisionSpec{}, fmt.Errorf("decode staged provision spec: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return snowflakedest.ManagedStagedProvisionSpec{}, errors.New("staged provision spec contains trailing JSON")
	}
	return spec, nil
}
