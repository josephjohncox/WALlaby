package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/josephjohncox/wallaby/connectors/sources/postgres"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/pkg/certify"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

const cliVersion = "0.0.0-dev"

func main() {
	if err := run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	command := newAdminCommand()
	parsedArgs := []string{}
	if len(args) > 1 {
		parsedArgs = args[1:]
	}
	command.SetArgs(parsedArgs)
	return command.Execute()
}

func newAdminCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "wallaby-admin",
		Short:        "Wallaby admin CLI",
		Version:      cliVersion,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			usage()
			return nil
		},
	}

	command.PersistentFlags().String("config", "", "path to wallaby-admin config file")
	command.PersistentPreRunE = func(cmd *cobra.Command, _ []string) error {
		return initAdminConfig(cmd)
	}

	addLeaf := func(parent *cobra.Command, name, short string, addFlags func(*cobra.Command), runFn func(*cobra.Command, []string) error) {
		cmd := &cobra.Command{
			Use:   name,
			Short: short,
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				return runWithConfig(cmd, runFn, args)
			},
		}
		if addFlags != nil {
			addFlags(cmd)
		}
		parent.AddCommand(cmd)
	}

	addLeaf(command, "check", "check local flow configuration or endpoint connectivity", addCheckFlags, runCheck)
	addLeaf(command, "certify", "run data certification for postgres sources and destinations", addCertifyFlags, runCertify)

	ddlCommand := &cobra.Command{
		Use:   "ddl",
		Short: "manage DDL history and approval",
		RunE: func(*cobra.Command, []string) error {
			ddlUsage()
			return nil
		},
	}
	addLeaf(ddlCommand, "list", "list DDL changes", addDDLListFlags, ddlList)
	addLeaf(ddlCommand, "history", "show DDL event history", addDDLHistoryFlags, ddlHistory)
	addLeaf(ddlCommand, "show", "show a DDL event", addDDLShowFlags, ddlShow)
	addLeaf(ddlCommand, "approve", "approve a DDL event", addDDLApproveFlags, ddlApprove)
	addLeaf(ddlCommand, "reject", "reject a DDL event", addDDLRejectFlags, ddlReject)
	addLeaf(ddlCommand, "apply", "apply a DDL event", addDDLApplyFlags, ddlApply)
	command.AddCommand(ddlCommand)

	streamCommand := &cobra.Command{
		Use:   "stream",
		Short: "administer stream operations",
		RunE: func(*cobra.Command, []string) error {
			streamUsage()
			return nil
		},
	}
	addLeaf(streamCommand, "replay", "replay stream events", addStreamReplayFlags, streamReplay)
	addLeaf(streamCommand, "pull", "pull stream events", addStreamPullFlags, streamPull)
	addLeaf(streamCommand, "ack", "ack stream records", addStreamAckFlags, streamAck)
	command.AddCommand(streamCommand)

	slotCommand := &cobra.Command{
		Use:   "slot",
		Short: "manage logical replication slots",
		RunE: func(*cobra.Command, []string) error {
			slotUsage()
			return nil
		},
	}
	addLeaf(slotCommand, "list", "list replication slots", addSlotListFlags, slotList)
	addLeaf(slotCommand, "show", "show replication slots", addSlotShowFlags, slotShow)
	addLeaf(slotCommand, "drop", "drop replication slots", addSlotDropFlags, slotDrop)
	command.AddCommand(slotCommand)

	publicationCommand := &cobra.Command{
		Use:   "publication",
		Short: "manage source publications",
		RunE: func(*cobra.Command, []string) error {
			publicationUsage()
			return nil
		},
	}
	addLeaf(publicationCommand, "list", "list source publications", addPublicationListFlags, publicationList)
	addLeaf(publicationCommand, "add", "add a publication", addPublicationAddFlags, publicationAdd)
	addLeaf(publicationCommand, "remove", "remove a publication", addPublicationRemoveFlags, publicationRemove)
	addLeaf(publicationCommand, "sync", "sync publication membership", addPublicationSyncFlags, publicationSync)
	addLeaf(publicationCommand, "scrape", "scrape publication metadata", addPublicationScrapeFlags, publicationScrape)
	command.AddCommand(publicationCommand)

	flowCommand := &cobra.Command{
		Use:   "flow",
		Short: "manage replication flows",
		RunE: func(*cobra.Command, []string) error {
			flowUsage()
			return nil
		},
	}
	addLeaf(flowCommand, "create", "create flow", addFlowCreateFlags, flowCreate)
	addLeaf(flowCommand, "update", "update flow", addFlowUpdateFlags, flowUpdate)
	addLeaf(flowCommand, "reconfigure", "reconfigure flow", addFlowReconfigureFlags, flowReconfigure)
	addLeaf(flowCommand, "start", "start flow", addFlowStartFlags, flowStart)
	addLeaf(flowCommand, "run-once", "run flow once", addFlowRunOnceFlags, flowRunOnce)
	addLeaf(flowCommand, "stop", "stop flow", addFlowStopFlags, flowStop)
	addLeaf(flowCommand, "resume", "resume flow", addFlowResumeFlags, flowResume)
	addLeaf(flowCommand, "cleanup", "cleanup flow", addFlowCleanupFlags, flowCleanup)
	addLeaf(flowCommand, "resolve-staging", "resolve staging", addFlowResolveStagingFlags, flowResolveStaging)
	addLeaf(flowCommand, "get", "get flow", addFlowGetFlags, flowGet)
	addLeaf(flowCommand, "list", "list flows", addFlowListFlags, flowList)
	addLeaf(flowCommand, "delete", "delete flow", addFlowDeleteFlags, flowDelete)
	addLeaf(flowCommand, "wait", "wait for flow state", addFlowWaitFlags, flowWait)
	addLeaf(flowCommand, "check", "check flow definition", addFlowCheckFlags, flowCheck)
	addLeaf(flowCommand, "plan", "preview flow plan", addFlowPlanFlags, flowPlan)
	addLeaf(flowCommand, "dry-run", "dry-run flow update", addFlowDryRunFlags, flowDryRun)
	addLeaf(flowCommand, "validate", "validate flow definition", addFlowValidateFlags, flowValidate)
	command.AddCommand(flowCommand)

	command.InitDefaultCompletionCmd()

	command.AddCommand(&cobra.Command{
		Use:     "version",
		Short:   "show version",
		Args:    cobra.NoArgs,
		Aliases: []string{"v"},
		RunE: func(*cobra.Command, []string) error {
			return runVersion()
		},
	})
	return command
}

func initAdminConfig(cmd *cobra.Command) error {
	configFlags := cmd.Flags()
	if cmd.Root() != nil && cmd.Root().PersistentFlags().Lookup("config") != nil {
		configFlags = cmd.Root().PersistentFlags()
	}
	configPath, err := configFlags.GetString("config")
	if err != nil {
		return fmt.Errorf("read config flag: %w", err)
	}

	viper.Reset()
	viper.SetEnvPrefix("WALLABY_ADMIN")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else if envPath := os.Getenv("WALLABY_ADMIN_CONFIG"); envPath != "" {
		viper.SetConfigFile(envPath)
	} else {
		viper.SetConfigName("wallaby-admin")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		if home, err := os.UserHomeDir(); err == nil {
			viper.AddConfigPath(filepath.Join(home, ".config", "wallaby"))
		}
	}

	if err := viper.ReadInConfig(); err != nil {
		var missing viper.ConfigFileNotFoundError
		if !errors.As(err, &missing) {
			return fmt.Errorf("read config: %w", err)
		}
	}
	return nil
}

func runWithConfig(cmd *cobra.Command, fn func(*cobra.Command, []string) error, args []string) error {
	if viper.IsSet("endpoint") && cmd.Flags().Lookup("endpoint") != nil && !cmd.Flags().Changed("endpoint") {
		if err := cmd.Flags().Set("endpoint", viper.GetString("endpoint")); err != nil {
			return fmt.Errorf("set endpoint from config: %w", err)
		}
	}
	if viper.IsSet("insecure") && cmd.Flags().Lookup("insecure") != nil && !cmd.Flags().Changed("insecure") {
		if err := cmd.Flags().Set("insecure", fmt.Sprintf("%t", viper.GetBool("insecure"))); err != nil {
			return fmt.Errorf("set insecure from config: %w", err)
		}
	}
	return fn(cmd, args)
}

func addAdminConnectionFlags(cmd *cobra.Command, defaultEndpoint string) {
	cmd.Flags().String("endpoint", defaultEndpoint, "admin endpoint host:port")
	cmd.Flags().Bool("insecure", true, "use insecure gRPC")
}

func addJSONOutputFlags(cmd *cobra.Command, withYAML bool) {
	cmd.Flags().Bool("json", false, "output JSON for scripting")
	if withYAML {
		cmd.Flags().Bool("yaml", false, "output YAML for scripting")
	}
	cmd.Flags().Bool("pretty", false, "pretty-print JSON output")
}

func addCheckFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "")
	cmd.Flags().String("flow-id", "", "flow id to validate")
	cmd.Flags().String("file", "", "flow config JSON file to validate")
	cmd.Flags().Bool("connectivity", false, "probe destination connectivity")
	cmd.Flags().Duration("timeout", 5*time.Second, "connection timeout")
	addJSONOutputFlags(cmd, true)
}

func addDDLHistoryFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	cmd.Flags().String("status", "all", "status filter: pending|approved|rejected|applied|all")
	addJSONOutputFlags(cmd, true)
}

func addSlotListFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	cmd.Flags().String("dsn", "", "postgres source dsn")
	cmd.Flags().String("slot", "", "optional slot filter")
	addJSONOutputFlags(cmd, false)
	addAWSIAMFlags(cmd)
}

func addSlotShowFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	cmd.Flags().String("dsn", "", "postgres source dsn")
	cmd.Flags().String("slot", "", "slot name (optional if using -flow-id)")
	addJSONOutputFlags(cmd, false)
	addAWSIAMFlags(cmd)
}

func addSlotDropFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	cmd.Flags().String("dsn", "", "postgres source dsn")
	cmd.Flags().String("slot", "", "slot name (optional if using -flow-id)")
	cmd.Flags().Bool("if-exists", true, "succeed even when slot is missing")
	addJSONOutputFlags(cmd, false)
	addAWSIAMFlags(cmd)
}

func addDDLListFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("status", "pending", "status filter: pending|approved|rejected|applied|all")
	cmd.Flags().String("flow-id", "", "filter by flow id")
	addJSONOutputFlags(cmd, false)
}

func addDDLShowFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().Int64("id", 0, "DDL event ID")
	cmd.Flags().String("status", "all", "status filter: pending|approved|rejected|applied|all")
	cmd.Flags().String("flow-id", "", "optional filter by flow id")
	addJSONOutputFlags(cmd, true)
}

func addDDLApproveFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().Int64("id", 0, "DDL event ID")
}

func addDDLRejectFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().Int64("id", 0, "DDL event ID")
}

func addDDLApplyFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().Int64("id", 0, "DDL event ID")
}

func addStreamReplayFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("stream", "", "stream name")
	cmd.Flags().String("group", "", "consumer group")
	cmd.Flags().String("from-lsn", "", "replay events from this LSN (optional)")
	cmd.Flags().String("since", "", "replay events since RFC3339 time (optional)")
}

func addCertifyFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id to load source/destination DSNs")
	cmd.Flags().String("destination", "", "destination name (required when flow has multiple destinations)")
	cmd.Flags().String("source-dsn", "", "source Postgres DSN (overrides flow)")
	cmd.Flags().String("dest-dsn", "", "destination Postgres DSN (overrides flow)")
	cmd.Flags().String("table", "", "table name (schema.table)")
	cmd.Flags().String("tables", "", "comma-separated table list (schema.table)")
	cmd.Flags().String("primary-keys", "", "comma-separated primary key columns (optional)")
	cmd.Flags().String("columns", "", "comma-separated columns to hash (optional)")
	cmd.Flags().Float64("sample-rate", 1, "sample rate 0..1 (uses deterministic PK hash)")
	cmd.Flags().Int("sample-limit", 0, "max rows to hash per table")
	addJSONOutputFlags(cmd, false)
	addAWSIAMFlags(cmd)

	var sourceOpts keyValueFlag
	cmd.Flags().Var(&sourceOpts, "source-opt", "source option key=value (repeatable)")
	var destOpts keyValueFlag
	cmd.Flags().Var(&destOpts, "dest-opt", "destination option key=value (repeatable)")
}

func addFlowCreateFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("file", "", "flow config JSON file")
	cmd.Flags().Bool("start", false, "start flow immediately")
	addJSONOutputFlags(cmd, false)
}

func addFlowUpdateFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("file", "", "flow config JSON file")
	cmd.Flags().String("flow-id", "", "flow id override")
	cmd.Flags().Bool("pause", false, "pause flow before update")
	cmd.Flags().Bool("resume", false, "resume flow after update")
	addJSONOutputFlags(cmd, false)
}

func addFlowReconfigureFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("file", "", "flow config JSON file")
	cmd.Flags().String("flow-id", "", "flow id override")
	cmd.Flags().Bool("pause", true, "pause flow before reconfigure")
	cmd.Flags().Bool("resume", true, "resume flow after reconfigure")
	cmd.Flags().Bool("sync-publication", false, "sync publication tables after update")
	addJSONOutputFlags(cmd, false)
}

func addFlowRunOnceFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	addJSONOutputFlags(cmd, false)
}

func addFlowStartFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	addJSONOutputFlags(cmd, false)
}

func addFlowStopFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	addJSONOutputFlags(cmd, false)
}

func addFlowResumeFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	addJSONOutputFlags(cmd, false)
}

func addFlowCleanupFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	cmd.Flags().Bool("drop-slot", true, "drop replication slot")
	cmd.Flags().Bool("drop-publication", false, "drop publication")
	cmd.Flags().Bool("drop-source-state", true, "delete source state row")
	addJSONOutputFlags(cmd, false)
}

func addStreamPullFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("stream", "", "stream name")
	cmd.Flags().String("group", "", "consumer group")
	cmd.Flags().Int("max", 10, "max messages to pull")
	cmd.Flags().Int("visibility", 30, "visibility timeout seconds")
	cmd.Flags().String("consumer", "", "consumer id")
	addJSONOutputFlags(cmd, false)
}

func addStreamAckFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("stream", "", "stream name")
	cmd.Flags().String("group", "", "consumer group")
	cmd.Flags().String("ids", "", "comma-separated message ids")
}

func addPublicationListFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	cmd.Flags().String("dsn", "", "postgres source dsn")
	cmd.Flags().String("publication", "", "publication name")
	addJSONOutputFlags(cmd, false)
	addAWSIAMFlags(cmd)
}

func addPublicationAddFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	cmd.Flags().String("dsn", "", "postgres source dsn")
	cmd.Flags().String("publication", "", "publication name")
	cmd.Flags().String("tables", "", "comma-separated schema.table list")
	addAWSIAMFlags(cmd)
}

func addPublicationRemoveFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	cmd.Flags().String("dsn", "", "postgres source dsn")
	cmd.Flags().String("publication", "", "publication name")
	cmd.Flags().String("tables", "", "comma-separated schema.table list")
	addAWSIAMFlags(cmd)
}

func addPublicationSyncFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	cmd.Flags().String("dsn", "", "postgres source dsn override")
	cmd.Flags().String("publication", "", "publication name override")
	cmd.Flags().String("tables", "", "comma-separated schema.table list")
	cmd.Flags().String("schemas", "", "comma-separated schemas for auto-discovery")
	cmd.Flags().String("mode", "add", "sync mode: add or sync")
	cmd.Flags().Bool("pause", true, "pause flow before sync")
	cmd.Flags().Bool("resume", true, "resume flow after sync")
	cmd.Flags().Bool("snapshot", false, "run backfill for newly added tables")
	cmd.Flags().Int("snapshot-workers", 0, "parallel workers for backfill snapshots")
	cmd.Flags().String("partition-column", "", "partition column for backfill hashing")
	cmd.Flags().Int("partition-count", 0, "partition count per table for backfill hashing")
	cmd.Flags().String("snapshot-state-backend", "", "snapshot state backend (postgres|file|none)")
	cmd.Flags().String("snapshot-state-path", "", "snapshot state path (file backend)")
	addAWSIAMFlags(cmd)
}

func addPublicationScrapeFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	cmd.Flags().String("dsn", "", "postgres source dsn")
	cmd.Flags().String("publication", "", "publication name")
	cmd.Flags().String("schemas", "", "comma-separated schemas")
	cmd.Flags().Bool("apply", false, "add newly discovered tables to publication")
	addAWSIAMFlags(cmd)
}

func addFlowResolveStagingFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	cmd.Flags().String("tables", "", "comma-separated tables (schema.table)")
	cmd.Flags().String("schemas", "", "comma-separated schemas to resolve")
	cmd.Flags().String("dest", "", "destination name (optional)")
}

func addFlowListFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().Int("page-size", 0, "max flows per request (0 = server default)")
	cmd.Flags().String("page-token", "", "pagination token")
	cmd.Flags().String("state", "", "optional state filter: created|running|paused|stopping|failed")
	addJSONOutputFlags(cmd, false)
}

func addFlowGetFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	addJSONOutputFlags(cmd, false)
}

func addFlowDeleteFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	addJSONOutputFlags(cmd, false)
}

func addFlowWaitFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "localhost:8080")
	cmd.Flags().String("flow-id", "", "flow id")
	cmd.Flags().String("state", "", "target state: created|running|paused|stopping|failed")
	cmd.Flags().Duration("timeout", 60*time.Second, "max time to wait")
	cmd.Flags().Duration("interval", 2*time.Second, "poll interval")
	addJSONOutputFlags(cmd, false)
}

func addFlowValidateFlags(cmd *cobra.Command) {
	cmd.Flags().String("file", "", "flow config JSON file")
	addJSONOutputFlags(cmd, false)
}

func addFlowDryRunFlags(cmd *cobra.Command) {
	cmd.Flags().String("file", "", "flow config JSON file")
	addJSONOutputFlags(cmd, false)
}

func addFlowPlanFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "")
	cmd.Flags().String("file", "", "flow config JSON file")
	addJSONOutputFlags(cmd, false)
}

func addFlowCheckFlags(cmd *cobra.Command) {
	addAdminConnectionFlags(cmd, "")
	cmd.Flags().String("file", "", "flow config JSON file")
	addJSONOutputFlags(cmd, false)
}

func stringFlag(cmd *cobra.Command, name string) (*string, error) {
	v, err := cmd.Flags().GetString(name)
	if err != nil {
		return nil, fmt.Errorf("read --%s: %w", name, err)
	}
	return &v, nil
}

func boolFlag(cmd *cobra.Command, name string) (*bool, error) {
	v, err := cmd.Flags().GetBool(name)
	if err != nil {
		return nil, fmt.Errorf("read --%s: %w", name, err)
	}
	return &v, nil
}

func durationFlag(cmd *cobra.Command, name string) (*time.Duration, error) {
	v, err := cmd.Flags().GetDuration(name)
	if err != nil {
		return nil, fmt.Errorf("read --%s: %w", name, err)
	}
	return &v, nil
}

func intFlag(cmd *cobra.Command, name string) (*int, error) {
	v, err := cmd.Flags().GetInt(name)
	if err != nil {
		return nil, fmt.Errorf("read --%s: %w", name, err)
	}
	return &v, nil
}

func int64Flag(cmd *cobra.Command, name string) (*int64, error) {
	v, err := cmd.Flags().GetInt64(name)
	if err != nil {
		return nil, fmt.Errorf("read --%s: %w", name, err)
	}
	return &v, nil
}

func float64Flag(cmd *cobra.Command, name string) (*float64, error) {
	v, err := cmd.Flags().GetFloat64(name)
	if err != nil {
		return nil, fmt.Errorf("read --%s: %w", name, err)
	}
	return &v, nil
}

func keyValueFlagValue(cmd *cobra.Command, name string) (*keyValueFlag, error) {
	f := cmd.Flags().Lookup(name)
	if f == nil {
		return nil, fmt.Errorf("read --%s: missing flag", name)
	}
	kv, ok := f.Value.(*keyValueFlag)
	if !ok {
		return nil, fmt.Errorf("read --%s: invalid flag type", name)
	}
	return kv, nil
}

func awsIAMOptions(cmd *cobra.Command) (*awsIAMFlags, error) {
	enabled, err := boolFlag(cmd, "aws-rds-iam")
	if err != nil {
		return nil, err
	}
	region, err := stringFlag(cmd, "aws-region")
	if err != nil {
		return nil, err
	}
	profile, err := stringFlag(cmd, "aws-profile")
	if err != nil {
		return nil, err
	}
	roleARN, err := stringFlag(cmd, "aws-role-arn")
	if err != nil {
		return nil, err
	}
	roleSessionName, err := stringFlag(cmd, "aws-role-session-name")
	if err != nil {
		return nil, err
	}
	roleExternalID, err := stringFlag(cmd, "aws-role-external-id")
	if err != nil {
		return nil, err
	}
	endpoint, err := stringFlag(cmd, "aws-endpoint")
	if err != nil {
		return nil, err
	}
	return &awsIAMFlags{
		enabled:         enabled,
		region:          region,
		profile:         profile,
		roleARN:         roleARN,
		roleSessionName: roleSessionName,
		roleExternalID:  roleExternalID,
		endpoint:        endpoint,
	}, nil
}

func renderTextTable(headers []string, rows [][]string) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	header := make(table.Row, len(headers))
	for i, value := range headers {
		header[i] = value
	}
	t.AppendHeader(header)
	for _, rowValues := range rows {
		row := make(table.Row, len(rowValues))
		for i, value := range rowValues {
			row[i] = value
		}
		t.AppendRow(row)
	}
	t.Render()
}

func runVersion() error {
	fmt.Printf("wallaby-admin %s\n", cliVersion)
	return nil
}

var adminFileSystem = afero.NewOsFs()

func readFile(path string) ([]byte, error) {
	return afero.ReadFile(adminFileSystem, path)
}

func runCheck(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	configPath, err := stringFlag(cmd, "file")
	if err != nil {
		return err
	}
	checkConnectivity, err := boolFlag(cmd, "connectivity")
	if err != nil {
		return err
	}
	connectTimeout, err := durationFlag(cmd, "timeout")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	yamlOutput, err := boolFlag(cmd, "yaml")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	type checkArgs struct {
		Endpoint   string `validate:"required_without_all=FlowID ConfigPath"`
		FlowID     string `validate:"required_without_all=Endpoint ConfigPath"`
		ConfigPath string `validate:"required_without_all=Endpoint FlowID"`
	}

	validation := validator.New()
	if err := validation.Struct(checkArgs{
		Endpoint:   *endpoint,
		FlowID:     *flowID,
		ConfigPath: *configPath,
	}); err != nil {
		return errors.New("-flow-id, -file, or -endpoint is required")
	}
	if *flowID == "" && *configPath == "" && *endpoint == "" {
		return errors.New("-flow-id, -file, or -endpoint is required")
	}
	if *flowID != "" && *endpoint == "" {
		return errors.New("-endpoint is required when -flow-id is set")
	}
	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput && *yamlOutput {
		return errors.New("use either -json or -yaml")
	}

	result := checkResult{
		CheckedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}

	ctx, cancel := context.WithTimeout(context.Background(), *connectTimeout)
	defer cancel()

	var endpoints []*wallabypb.Endpoint
	var flow *wallabypb.Flow

	if *endpoint != "" {
		client, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			result.AdminReachable = false
			result.Endpoint = *endpoint
			result.EndpointError = err.Error()
			if *flowID != "" {
				return fmt.Errorf("admin endpoint check: %w", err)
			}
		} else {
			result.Endpoint = *endpoint
			result.AdminReachable = true
			defer func() { _ = closeConn() }()
			if *flowID != "" {
				resp, err := client.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: *flowID})
				if err != nil {
					return fmt.Errorf("get flow: %w", err)
				}
				flow = resp
			}
		}
	}

	if *flowID != "" && flow == nil {
		return errors.New("flow load failed")
	}
	if flow != nil {
		result.FlowID = flow.Id
		result.FlowName = flow.Name
		endpoints = append(endpoints, flow.Source)
		endpoints = append(endpoints, flow.Destinations...)
	}

	if *configPath != "" {
		fsys := afero.NewOsFs()
		payload, err := afero.ReadFile(fsys, *configPath)
		if err != nil {
			return fmt.Errorf("read flow config: %w", err)
		}
		var cfg flowConfig
		if err := json.Unmarshal(payload, &cfg); err != nil {
			return fmt.Errorf("parse flow config: %w", err)
		}
		if _, err := flowConfigToProto(cfg); err != nil {
			return fmt.Errorf("flow config: %w", err)
		}
		result.ConfigFile = *configPath

		pbFlow, err := flowConfigToProto(cfg)
		if err != nil {
			return fmt.Errorf("flow config: %w", err)
		}
		// Keep config file details in the output even if flow-id mode was used.
		if result.FlowID == "" {
			result.FlowID = pbFlow.Id
		}
		if result.FlowName == "" {
			result.FlowName = pbFlow.Name
		}
		cfgEndpoints := make([]*wallabypb.Endpoint, 0, len(pbFlow.Destinations)+1)
		if pbFlow.Source != nil {
			cfgEndpoints = append(cfgEndpoints, pbFlow.Source)
		}
		cfgEndpoints = append(cfgEndpoints, pbFlow.Destinations...)
		endpoints = append(endpoints, cfgEndpoints...)
	}

	if len(endpoints) == 0 {
		result.AdminReachable = result.AdminReachable || *endpoint == ""
		if *endpoint == "" {
			result.Endpoint = "offline"
		}
	}

	if result.Endpoint == "" {
		result.Endpoint = "offline"
	}

	for idx, item := range endpoints {
		if item == nil {
			continue
		}
		isSource := flow != nil && flow.Source != nil && item == flow.Source
		if !isSource && flow == nil && idx == 0 {
			isSource = true
		}
		record := checkEndpointResult(item.Name, endpointTypeFromProto(item.Type), isSource, item.Options, *checkConnectivity, *connectTimeout)
		if record.Source {
			result.Source = record
		} else {
			result.Destinations = append(result.Destinations, record)
		}
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(result); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	if *yamlOutput {
		out, err := yaml.Marshal(result)
		if err != nil {
			return fmt.Errorf("encode yaml: %w", err)
		}
		_, err = os.Stdout.Write(out)
		return err
	}

	fmt.Printf("check ok: flow=%q endpoint=%q admin=%t\n", result.FlowID, result.Endpoint, result.AdminReachable)
	if len(result.Source.Name) > 0 {
		fmt.Printf("source: %s (%s) reachable=%t\n", result.Source.Name, result.Source.Type, result.Source.Reachable)
	}
	for _, dest := range result.Destinations {
		fmt.Printf("destination: %s (%s) reachable=%t\n", dest.Name, dest.Type, dest.Reachable)
	}
	return nil
}

func checkEndpointResult(name string, endpointType connector.EndpointType, isSource bool, options map[string]string, checkConnectivity bool, checkTimeout time.Duration) endpointCheckResult {
	result := endpointCheckResult{
		Name:      name,
		Type:      string(endpointType),
		Source:    isSource,
		Checked:   false,
		Reachable: true,
	}
	if !checkConnectivity {
		return result
	}

	result.Checked = true
	start := time.Now()
	if err := checkEndpointConnectivity(endpointType, options, checkTimeout); err != nil {
		result.Reachable = false
		result.Error = err.Error()
		return result
	}
	result.Reachable = true
	result.LatencyMs = time.Since(start).Milliseconds()
	return result
}

func checkEndpointConnectivity(endpointType connector.EndpointType, options map[string]string, timeout time.Duration) error {
	switch endpointType {
	case connector.EndpointPostgres, connector.EndpointPGStream:
		dsn := strings.TrimSpace(optionValue(options, "dsn"))
		if dsn == "" {
			return errors.New("dsn is required for connectivity check")
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("connect postgres: %w", err)
		}
		defer func() {
			_ = conn.Close(context.Background())
		}()
		var one int
		if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
			return fmt.Errorf("postgres query: %w", err)
		}
		return nil

	case connector.EndpointKafka:
		brokers := strings.TrimSpace(optionValue(options, "brokers"))
		if brokers == "" {
			return errors.New("brokers is required for connectivity check")
		}
		target, _, err := firstCSVValue(brokers)
		if err != nil {
			return err
		}
		if err := checkNetworkConnectivity(target, timeout); err != nil {
			return fmt.Errorf("connect kafka broker: %w", err)
		}
		return nil

	case connector.EndpointHTTP:
		target := firstOption(options, []string{"url", "endpoint", "target", "server"}, "")
		if target == "" {
			return errors.New("url is required for connectivity check")
		}
		return checkHTTPConnectivity(target, timeout)

	case connector.EndpointGRPC, connector.EndpointS3, connector.EndpointSnowflake, connector.EndpointSnowpipe, connector.EndpointParquet, connector.EndpointDuckLake, connector.EndpointDuckDB, connector.EndpointBufStream, connector.EndpointClickHouse, connector.EndpointProto:
		target := firstOption(options, []string{"url", "endpoint", "target", "address", "addr", "host", "dsn"}, "")
		if target == "" {
			return fmt.Errorf("connectivity target is required for endpoint type %q", endpointType)
		}
		if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
			return checkHTTPConnectivity(target, timeout)
		}
		return checkNetworkConnectivity(target, timeout)
	default:
		return fmt.Errorf("connectivity check unsupported for endpoint type %q", endpointType)
	}
}

func checkHTTPConnectivity(target string, timeout time.Duration) error {
	urlValue := strings.TrimSpace(target)
	if urlValue == "" {
		return errors.New("url is required")
	}
	if !strings.Contains(urlValue, "://") {
		urlValue = "http://" + urlValue
	}
	parsed, err := url.Parse(urlValue)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	if parsed.Host == "" {
		return errors.New("url must include host")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "HEAD", parsed.String(), nil)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("http check: %w", err)
	}
	_ = resp.Body.Close()
	return nil
}

func checkNetworkConnectivity(target string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
	if err != nil {
		return fmt.Errorf("tcp dial: %w", err)
	}
	_ = conn.Close()
	return nil
}

func firstCSVValue(value string) (string, bool, error) {
	pieces := parseCSVValue(value)
	if len(pieces) == 0 {
		return "", false, errors.New("no endpoint specified")
	}
	return pieces[0], true, nil
}

func firstOption(options map[string]string, keys []string, fallback string) string {
	for _, key := range keys {
		if val := strings.TrimSpace(optionValue(options, key)); val != "" {
			return val
		}
	}
	if fallback != "" {
		return fallback
	}
	return ""
}

func optionValue(options map[string]string, key string) string {
	if len(options) == 0 {
		return ""
	}
	return strings.TrimSpace(options[key])
}

func ddlHistory(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	status, err := stringFlag(cmd, "status")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	yamlOutput, err := boolFlag(cmd, "yaml")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	if strings.TrimSpace(*flowID) == "" {
		return errors.New("-flow-id is required")
	}

	client, closeConn, err := ddlClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ListDDL(ctx, &wallabypb.ListDDLRequest{
		Status: *status,
		FlowId: *flowID,
	})
	if err != nil {
		return fmt.Errorf("list ddl: %w", err)
	}
	if *prettyOutput {
		*jsonOutput = true
	}
	out := ddlHistoryOutput{
		FlowID: *flowID,
		Status: *status,
		Count:  len(resp.Events),
		Events: ddlListEvents(resp.Events),
	}

	if *yamlOutput {
		if *jsonOutput {
			return errors.New("cannot use -json and -yaml together")
		}
		yamlPayload, err := yaml.Marshal(out)
		if err != nil {
			return fmt.Errorf("encode yaml: %w", err)
		}
		_, err = os.Stdout.Write(yamlPayload)
		return err
	}

	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	if len(out.Events) == 0 {
		fmt.Printf("No ddl history for flow %s.\n", *flowID)
		return nil
	}

	fmt.Printf("DDL history for flow=%s status=%s:\n", *flowID, *status)
	for _, event := range out.Events {
		line := fmt.Sprintf("%-6d %-10s %-18s %s", event.ID, event.Status, event.LSN, truncate(ddlListRecordSummary(event), 140))
		fmt.Println(line)
	}
	return nil
}

func ddlListRecordSummary(event ddlListRecord) string {
	if event.DDL != "" {
		return event.DDL
	}
	if event.PlanJSON != "" {
		return "plan:" + event.PlanJSON
	}
	return "(no ddl)"
}

func slotUsage() {
	fmt.Println("Slot subcommands:")
	fmt.Println("  wallaby-admin slot list -flow-id <id> [-slot <name>] [--json|--pretty]")
	fmt.Println("  wallaby-admin slot list -dsn <dsn> [-slot <name>] [-aws-* flags] [--json|--pretty]")
	fmt.Println("  wallaby-admin slot show -flow-id <id> [-slot <name>] [--json|--pretty]")
	fmt.Println("  wallaby-admin slot show -dsn <dsn> -slot <name> [-aws-* flags] [--json|--pretty]")
	fmt.Println("  wallaby-admin slot drop -flow-id <id> [-slot <name>] [-if-exists] [--json|--pretty]")
	fmt.Println("  wallaby-admin slot drop -dsn <dsn> -slot <name> [-if-exists] [-aws-* flags]")
	fmt.Println("  IAM flags: -aws-rds-iam -aws-region <region> -aws-profile <profile> -aws-role-arn <arn>")
	os.Exit(1)
}

func slotList(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	dsn, err := stringFlag(cmd, "dsn")
	if err != nil {
		return err
	}
	slot, err := stringFlag(cmd, "slot")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	awsFlags, err := awsIAMOptions(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg, err := resolveSlotConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, "", false, awsFlags.options())
	if err != nil {
		return err
	}

	var records []slotListRecord
	slotName := strings.TrimSpace(*slot)
	if cfg.flow != nil {
		flowSvc, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			return err
		}
		defer func() { _ = closeConn() }()

		if slotName == "" {
			resp, err := flowSvc.ListReplicationSlots(ctx, &wallabypb.ListReplicationSlotsRequest{
				FlowId:  cfg.flow.Id,
				Dsn:     cfg.dsn,
				Slot:    slotName,
				Options: cfg.options,
			})
			if err != nil {
				return fmt.Errorf("list replication slots: %w", err)
			}
			for _, item := range resp.Slots {
				records = append(records, slotListRecordFromProto(item))
			}
		} else {
			resp, err := flowSvc.GetReplicationSlot(ctx, &wallabypb.GetReplicationSlotRequest{
				FlowId:  cfg.flow.Id,
				Dsn:     cfg.dsn,
				Slot:    slotName,
				Options: cfg.options,
			})
			if err != nil {
				return fmt.Errorf("get replication slot: %w", err)
			}
			records = append(records, slotListRecordFromProto(resp.Slot))
		}
	} else {
		if slotName == "" {
			slots, err := postgres.ListReplicationSlots(ctx, cfg.dsn, cfg.options)
			if err != nil {
				return err
			}
			for _, item := range slots {
				records = append(records, slotListRecordFromInfo(item))
			}
		} else {
			item, ok, err := postgres.GetReplicationSlot(ctx, cfg.dsn, slotName, cfg.options)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("slot %q not found", slotName)
			}
			records = append(records, slotListRecordFromInfo(item))
		}
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := slotListOutput{
			Count: len(records),
			Slots: records,
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	if len(records) == 0 {
		fmt.Println("No replication slots found.")
		return nil
	}

	rows := make([][]string, 0, len(records))
	for _, item := range records {
		pid := "n/a"
		if item.ActivePID != nil {
			pid = fmt.Sprintf("%d", *item.ActivePID)
		}
		rows = append(rows, []string{
			item.SlotName,
			item.Plugin,
			item.SlotType,
			item.Database,
			fmt.Sprintf("%t", item.Active),
			pid,
			fmt.Sprintf("%t", item.Temporary),
			item.WalStatus,
			item.RestartLSN,
		})
	}
	renderTextTable([]string{
		"SLOT",
		"PLUGIN",
		"TYPE",
		"DB",
		"ACTIVE",
		"ACTIVE_PID",
		"TEMP",
		"WAL_STATUS",
		"RESTART_LSN",
	}, rows)
	return nil
}

func slotShow(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	dsn, err := stringFlag(cmd, "dsn")
	if err != nil {
		return err
	}
	slot, err := stringFlag(cmd, "slot")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	awsFlags, err := awsIAMOptions(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg, err := resolveSlotConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *slot, true, awsFlags.options())
	if err != nil {
		return err
	}

	var record slotListRecord
	if cfg.flow != nil {
		flowSvc, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			return err
		}
		defer func() { _ = closeConn() }()
		resp, err := flowSvc.GetReplicationSlot(ctx, &wallabypb.GetReplicationSlotRequest{
			FlowId:  cfg.flow.Id,
			Dsn:     cfg.dsn,
			Slot:    cfg.slot,
			Options: cfg.options,
		})
		if err != nil {
			return fmt.Errorf("get replication slot: %w", err)
		}
		record = slotListRecordFromProto(resp.Slot)
	} else {
		item, ok, err := postgres.GetReplicationSlot(ctx, cfg.dsn, cfg.slot, cfg.options)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("slot %q not found", cfg.slot)
		}
		record = slotListRecordFromInfo(item)
	}
	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(record); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	pid := "n/a"
	if record.ActivePID != nil {
		pid = fmt.Sprintf("%d", *record.ActivePID)
	}
	fmt.Printf("slot=%s plugin=%s type=%s db=%s active=%t active_pid=%s temp=%t wal_status=%s restart_lsn=%s confirmed_flush_lsn=%s\n",
		record.SlotName, record.Plugin, record.SlotType, record.Database, record.Active, pid, record.Temporary, record.WalStatus, record.RestartLSN, record.ConfirmedFlushLSN)
	return nil
}

func slotDrop(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	dsn, err := stringFlag(cmd, "dsn")
	if err != nil {
		return err
	}
	slot, err := stringFlag(cmd, "slot")
	if err != nil {
		return err
	}
	ifExists, err := boolFlag(cmd, "if-exists")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	awsFlags, err := awsIAMOptions(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg, err := resolveSlotConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *slot, true, awsFlags.options())
	if err != nil {
		return err
	}

	var found bool
	var dropped bool
	if cfg.flow != nil {
		flowSvc, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			return err
		}
		defer func() { _ = closeConn() }()
		resp, err := flowSvc.DropReplicationSlot(ctx, &wallabypb.DropReplicationSlotRequest{
			FlowId:   cfg.flow.Id,
			Dsn:      cfg.dsn,
			Slot:     cfg.slot,
			IfExists: *ifExists,
			Options:  cfg.options,
		})
		if err != nil {
			return fmt.Errorf("drop replication slot: %w", err)
		}
		found = resp.Found
		dropped = resp.Dropped
	} else {
		_, ok, getErr := postgres.GetReplicationSlot(ctx, cfg.dsn, cfg.slot, cfg.options)
		if getErr != nil {
			return getErr
		}
		if !*ifExists && !ok {
			return fmt.Errorf("slot %q not found", cfg.slot)
		}
		if err := postgres.DropReplicationSlot(ctx, cfg.dsn, cfg.slot, cfg.options); err != nil {
			return fmt.Errorf("drop replication slot: %w", err)
		}
		found = true
		dropped = true
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := slotDropOutput{
			SlotName: cfg.slot,
			Dropped:  dropped,
			Found:    found,
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	if dropped && found {
		fmt.Printf("Dropped slot %s\n", cfg.slot)
		return nil
	}
	if *ifExists {
		fmt.Printf("Slot %s was already absent\n", cfg.slot)
	} else {
		fmt.Printf("Dropped slot %s\n", cfg.slot)
	}
	return nil
}

func ddlList(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	status, err := stringFlag(cmd, "status")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	client, closeConn, err := ddlClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var events []*wallabypb.DDLEvent
	if *status == "pending" {
		resp, err := client.ListPendingDDL(ctx, &wallabypb.ListPendingDDLRequest{FlowId: *flowID})
		if err != nil {
			return fmt.Errorf("list pending ddl: %w", err)
		}
		events = resp.Events
	} else {
		resp, err := client.ListDDL(ctx, &wallabypb.ListDDLRequest{Status: *status, FlowId: *flowID})
		if err != nil {
			return fmt.Errorf("list ddl: %w", err)
		}
		events = resp.Events
	}

	if *prettyOutput {
		*jsonOutput = true
	}

	if *jsonOutput {
		out := ddlListOutput{
			Status:  *status,
			Count:   len(events),
			Records: ddlListEvents(events),
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	if len(events) == 0 {
		fmt.Println("No DDL events found.")
		return nil
	}

	rows := make([][]string, 0, len(events))
	for _, event := range events {
		flagged := statusFlag(event.Status)
		summary := ddlSummary(event)
		if flagged != "" {
			summary = fmt.Sprintf("%s [%s]", summary, flagged)
		}
		rows = append(rows, []string{
			fmt.Sprintf("%d", event.Id),
			event.Status,
			event.Lsn,
			event.FlowId,
			summary,
		})
	}
	renderTextTable([]string{"ID", "STATUS", "LSN", "FLOW", "DDL/PLAN"}, rows)
	return nil
}

func ddlShow(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	id, err := int64Flag(cmd, "id")
	if err != nil {
		return err
	}
	status, err := stringFlag(cmd, "status")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	if *id == 0 {
		return errors.New("-id is required")
	}

	client, closeConn, err := ddlClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	event, err := loadDDLByID(ctx, client, *id, *status, *flowID)
	if err != nil {
		return err
	}
	return printDDLEvent(event, jsonOutput, prettyOutput)
}

func loadDDLByID(ctx context.Context, client wallabypb.DDLServiceClient, id int64, status string, flowID string) (*wallabypb.DDLEvent, error) {
	requestedStatus := strings.TrimSpace(strings.ToLower(status))
	if requestedStatus == "" {
		requestedStatus = "all"
	}

	switch requestedStatus {
	case "pending":
		resp, err := client.ListPendingDDL(ctx, &wallabypb.ListPendingDDLRequest{FlowId: flowID})
		if err != nil {
			return nil, fmt.Errorf("list pending ddl: %w", err)
		}
		if event := findDDLEventByID(resp.Events, id, flowID); event != nil {
			return event, nil
		}
		return nil, fmt.Errorf("ddl event %d not found", id)
	case "approved", "rejected", "applied", "all", "":
		resp, err := client.ListDDL(ctx, &wallabypb.ListDDLRequest{Status: requestedStatus, FlowId: flowID})
		if err != nil {
			return nil, fmt.Errorf("list ddl: %w", err)
		}
		if event := findDDLEventByID(resp.Events, id, flowID); event != nil {
			return event, nil
		}
		return nil, fmt.Errorf("ddl event %d not found", id)
	default:
		return nil, fmt.Errorf("invalid status %q", status)
	}
}

func findDDLEventByID(events []*wallabypb.DDLEvent, id int64, flowID string) *wallabypb.DDLEvent {
	for _, event := range events {
		if event == nil || event.Id != id {
			continue
		}
		if flowID != "" && event.FlowId != flowID {
			continue
		}
		return event
	}
	return nil
}

func printDDLEvent(event *wallabypb.DDLEvent, jsonOutput, prettyOutput *bool) error {
	if event == nil {
		return errors.New("nil ddl event")
	}
	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := ddlListRecord{
			ID:        event.Id,
			FlowID:    event.FlowId,
			Status:    event.Status,
			LSN:       event.Lsn,
			DDL:       event.Ddl,
			PlanJSON:  event.PlanJson,
			CreatedAt: formatProtoTime(event.CreatedAt),
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("DDL %d status=%s flow=%s lsn=%s created=%s\n", event.Id, event.Status, event.FlowId, event.Lsn, formatProtoTime(event.CreatedAt))
	if event.Ddl != "" {
		fmt.Printf("%s\n", event.Ddl)
	}
	if event.PlanJson != "" {
		fmt.Printf("plan=%s\n", event.PlanJson)
	}
	return nil
}

func ddlApprove(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	id, err := int64Flag(cmd, "id")
	if err != nil {
		return err
	}

	if *id == 0 {
		return errors.New("-id is required")
	}

	client, closeConn, err := ddlClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ApproveDDL(ctx, &wallabypb.ApproveDDLRequest{Id: *id})
	if err != nil {
		return fmt.Errorf("approve ddl: %w", err)
	}
	fmt.Printf("Approved DDL %d (status=%s)\n", resp.Event.Id, resp.Event.Status)
	return nil
}

func ddlReject(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	id, err := int64Flag(cmd, "id")
	if err != nil {
		return err
	}

	if *id == 0 {
		return errors.New("-id is required")
	}

	client, closeConn, err := ddlClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.RejectDDL(ctx, &wallabypb.RejectDDLRequest{Id: *id})
	if err != nil {
		return fmt.Errorf("reject ddl: %w", err)
	}
	fmt.Printf("Rejected DDL %d (status=%s)\n", resp.Event.Id, resp.Event.Status)
	return nil
}

func ddlApply(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	id, err := int64Flag(cmd, "id")
	if err != nil {
		return err
	}

	if *id == 0 {
		return errors.New("-id is required")
	}

	client, closeConn, err := ddlClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.MarkDDLApplied(ctx, &wallabypb.MarkDDLAppliedRequest{Id: *id})
	if err != nil {
		return fmt.Errorf("mark ddl applied: %w", err)
	}
	fmt.Printf("Marked DDL %d applied (status=%s)\n", resp.Event.Id, resp.Event.Status)
	return nil
}

func ddlClient(endpoint string, insecureConn bool) (wallabypb.DDLServiceClient, func() error, error) {
	if endpoint == "" {
		return nil, nil, errors.New("endpoint is required")
	}
	if !insecureConn {
		return nil, nil, errors.New("secure grpc is not configured")
	}

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("connect: %w", err)
	}
	return wallabypb.NewDDLServiceClient(conn), conn.Close, nil
}

func ddlSummary(event *wallabypb.DDLEvent) string {
	if event == nil {
		return ""
	}
	if event.Ddl != "" {
		return event.Ddl
	}
	if event.PlanJson != "" {
		return "plan:" + truncate(event.PlanJson, 120)
	}
	return "(no ddl)"
}

func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max-3] + "..."
}

func formatProtoTime(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().Format(time.RFC3339Nano)
}

func statusFlag(status string) string {
	switch strings.ToLower(status) {
	case "rejected", "failed":
		return "FAILED"
	default:
		return ""
	}
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  wallaby-admin slot <list|show|drop> [flags]")
	fmt.Println("  wallaby-admin ddl <list|history|show|approve|reject|apply> [flags]")
	fmt.Println("  wallaby-admin check [flags]")
	fmt.Println("  wallaby-admin stream <replay|pull|ack> [flags]")
	fmt.Println("  wallaby-admin publication <list|add|remove|sync|scrape> [flags]")
	fmt.Println("  wallaby-admin flow <create|update|reconfigure|start|run-once|stop|resume|cleanup|resolve-staging|get|list|delete|wait|validate|check|plan|dry-run> [flags]")
	fmt.Println("  wallaby-admin completion [bash|zsh|fish|powershell|powershell_core]")
	fmt.Println("  wallaby-admin version")
	fmt.Println("  wallaby-admin certify [flags]")
	os.Exit(1)
}

func ddlUsage() {
	fmt.Println("DDL subcommands:")
	fmt.Println("  wallaby-admin ddl list -status pending|approved|rejected|applied|all [-flow-id <flow_id>]")
	fmt.Println("  wallaby-admin ddl history -flow-id <flow_id> [--json|--pretty|--yaml]")
	fmt.Println("  wallaby-admin ddl show -id <event_id> [-status pending|approved|rejected|applied|all] [-flow-id <flow_id>] [--json|--pretty]")
	fmt.Println("  wallaby-admin ddl approve -id <event_id>")
	fmt.Println("  wallaby-admin ddl reject -id <event_id>")
	fmt.Println("  wallaby-admin ddl apply -id <event_id>")
	os.Exit(1)
}

func streamReplay(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	stream, err := stringFlag(cmd, "stream")
	if err != nil {
		return err
	}
	consumerGroup, err := stringFlag(cmd, "group")
	if err != nil {
		return err
	}
	fromLSN, err := stringFlag(cmd, "from-lsn")
	if err != nil {
		return err
	}
	since, err := stringFlag(cmd, "since")
	if err != nil {
		return err
	}

	if *stream == "" || *consumerGroup == "" {
		return errors.New("-stream and -group are required")
	}

	client, closeConn, err := streamClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	var sinceTS *timestamppb.Timestamp
	if *since != "" {
		parsed, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			return fmt.Errorf("parse since: %w", err)
		}
		sinceTS = timestamppb.New(parsed)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Replay(ctx, &wallabypb.StreamReplayRequest{
		Stream:        *stream,
		ConsumerGroup: *consumerGroup,
		FromLsn:       *fromLSN,
		Since:         sinceTS,
	})
	if err != nil {
		return fmt.Errorf("replay stream: %w", err)
	}

	fmt.Printf("Reset %d deliveries for stream %s group %s\n", resp.Reset_, *stream, *consumerGroup)
	return nil
}

func streamUsage() {
	fmt.Println("Stream subcommands:")
	fmt.Println("  wallaby-admin stream replay -stream <name> -group <name> [-from-lsn <lsn>] [-since <rfc3339>]")
	fmt.Println("  wallaby-admin stream pull -stream <name> -group <name> [-max 10] [-visibility 30] [-consumer <id>] [--json|--pretty]")
	fmt.Println("  wallaby-admin stream ack -stream <name> -group <name> -ids 1,2,3")
	os.Exit(1)
}

func runCertify(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	destName, err := stringFlag(cmd, "destination")
	if err != nil {
		return err
	}
	sourceDSN, err := stringFlag(cmd, "source-dsn")
	if err != nil {
		return err
	}
	destDSN, err := stringFlag(cmd, "dest-dsn")
	if err != nil {
		return err
	}
	table, err := stringFlag(cmd, "table")
	if err != nil {
		return err
	}
	tables, err := stringFlag(cmd, "tables")
	if err != nil {
		return err
	}
	primaryKeys, err := stringFlag(cmd, "primary-keys")
	if err != nil {
		return err
	}
	columns, err := stringFlag(cmd, "columns")
	if err != nil {
		return err
	}
	sampleRate, err := float64Flag(cmd, "sample-rate")
	if err != nil {
		return err
	}
	sampleLimit, err := intFlag(cmd, "sample-limit")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	sourceOpts, err := keyValueFlagValue(cmd, "source-opt")
	if err != nil {
		return err
	}
	destOpts, err := keyValueFlagValue(cmd, "dest-opt")
	if err != nil {
		return err
	}

	tableList := parseCSVValue(*tables)
	if *table != "" {
		if len(tableList) > 0 {
			return errors.New("-table and -tables are mutually exclusive")
		}
		tableList = []string{*table}
	}
	if len(tableList) == 0 {
		return errors.New("-table or -tables is required")
	}

	var sourceOptions map[string]string
	var destOptions map[string]string
	if *flowID != "" {
		client, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			return err
		}
		defer func() { _ = closeConn() }()

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		resp, err := client.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: *flowID})
		if err != nil {
			return fmt.Errorf("get flow: %w", err)
		}
		sourceOptions = copyStringMap(resp.Source.Options)
		if *sourceDSN == "" {
			*sourceDSN = strings.TrimSpace(resp.Source.Options["dsn"])
		}

		destSpec, err := selectDestination(resp.Destinations, *destName)
		if err != nil {
			return err
		}
		if destSpec.Type != wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES {
			return fmt.Errorf("destination %q is not postgres", destSpec.Name)
		}
		destOptions = copyStringMap(destSpec.Options)
		if *destDSN == "" {
			*destDSN = strings.TrimSpace(destSpec.Options["dsn"])
		}
	}

	if *sourceDSN == "" || *destDSN == "" {
		return errors.New("source-dsn and dest-dsn are required")
	}
	sourceOptions = mergeOptions(sourceOptions, sourceOpts.values)
	destOptions = mergeOptions(destOptions, destOpts.values)

	opts := certify.TableCertOptions{
		SampleRate:  *sampleRate,
		SampleLimit: *sampleLimit,
		PrimaryKeys: parseCSVValue(*primaryKeys),
		Columns:     parseCSVValue(*columns),
	}
	if len(opts.PrimaryKeys) > 0 && len(tableList) > 1 {
		return errors.New("-primary-keys can only be used with a single table")
	}
	if len(opts.Columns) > 0 && len(tableList) > 1 {
		return errors.New("-columns can only be used with a single table")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	reports := make([]certify.TableCertReport, 0, len(tableList))
	overallMatch := true
	for _, name := range tableList {
		report, err := certify.CertifyPostgresTable(ctx, *sourceDSN, sourceOptions, *destDSN, destOptions, name, opts)
		if err != nil {
			return err
		}
		reports = append(reports, report)
		overallMatch = overallMatch && report.Match
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := map[string]any{
			"match":  overallMatch,
			"tables": reports,
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	for _, report := range reports {
		fmt.Printf("%s match=%t rows=%d hash=%s\n", report.Table, report.Match, report.Source.Rows, report.Source.Hash)
	}
	if !overallMatch {
		return errors.New("certificate mismatch")
	}
	return nil
}

func flowUsage() {
	fmt.Println("Flow subcommands:")
	fmt.Println("  wallaby-admin flow create -file <path> [-start] [--json|--pretty]")
	fmt.Println("  wallaby-admin flow update -file <path> [-flow-id <id>] [-pause] [-resume] [--json|--pretty]")
	fmt.Println("  wallaby-admin flow reconfigure -file <path> [-flow-id <id>] [-pause] [-resume] [-sync-publication] [--json|--pretty]")
	fmt.Println("  wallaby-admin flow start -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow run-once -flow-id <id>")
	fmt.Println("  wallaby-admin flow get -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow list [-page-size 100] [-page-token <token>] [-state created|running|paused|stopping|failed] [--json|--pretty]")
	fmt.Println("  wallaby-admin flow plan -file <path> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow dry-run -file <path> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow check -file <path> [-endpoint <addr>] [--json|--pretty]")
	fmt.Println("  wallaby-admin flow delete -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow wait -flow-id <id> -state <state> [-timeout 60s] [-interval 2s] [--json|--pretty]")
	fmt.Println("  wallaby-admin flow validate -file <path> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow stop -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow resume -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow cleanup -flow-id <id> [-drop-slot] [-drop-publication] [-drop-source-state] [--json|--pretty]")
	fmt.Println("  wallaby-admin flow resolve-staging -flow-id <id> [-tables schema.table,...] [-schemas public,...] [-dest <name>]")
	os.Exit(1)
}

func flowCreate(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	path, err := stringFlag(cmd, "file")
	if err != nil {
		return err
	}
	start, err := boolFlag(cmd, "start")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	if *path == "" {
		return errors.New("-file is required")
	}

	payload, err := readFile(*path)
	if err != nil {
		return fmt.Errorf("read flow file: %w", err)
	}

	var cfg flowConfig
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return fmt.Errorf("parse flow json: %w", err)
	}

	pbFlow, err := flowConfigToProto(cfg)
	if err != nil {
		return fmt.Errorf("flow config: %w", err)
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.CreateFlow(ctx, &wallabypb.CreateFlowRequest{
		Flow:             pbFlow,
		StartImmediately: *start,
	})
	if err != nil {
		return fmt.Errorf("create flow: %w", err)
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := map[string]any{
			"id":          resp.Id,
			"name":        resp.Name,
			"state":       resp.State.String(),
			"wire_format": resp.WireFormat.String(),
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Created flow %s (state=%s)\n", resp.Id, resp.State.String())
	return nil
}

func flowUpdate(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	path, err := stringFlag(cmd, "file")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	pause, err := boolFlag(cmd, "pause")
	if err != nil {
		return err
	}
	resume, err := boolFlag(cmd, "resume")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	if *path == "" {
		return errors.New("-file is required")
	}

	payload, err := readFile(*path)
	if err != nil {
		return fmt.Errorf("read flow file: %w", err)
	}

	var cfg flowConfig
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return fmt.Errorf("parse flow json: %w", err)
	}
	if *flowID != "" {
		cfg.ID = *flowID
	}
	if cfg.ID == "" {
		return errors.New("flow id is required (provide in file or -flow-id)")
	}

	pbFlow, err := flowConfigToProto(cfg)
	if err != nil {
		return fmt.Errorf("flow config: %w", err)
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if *pause {
		if _, err := client.StopFlow(ctx, &wallabypb.StopFlowRequest{FlowId: cfg.ID}); err != nil {
			log.Printf("pause flow: %v", err)
		}
	}

	resp, err := client.UpdateFlow(ctx, &wallabypb.UpdateFlowRequest{Flow: pbFlow})
	if err != nil {
		return fmt.Errorf("update flow: %w", err)
	}

	if *resume {
		if _, err := client.ResumeFlow(ctx, &wallabypb.ResumeFlowRequest{FlowId: cfg.ID}); err != nil {
			log.Printf("resume flow: %v", err)
		}
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := map[string]any{
			"id":          resp.Id,
			"name":        resp.Name,
			"state":       resp.State.String(),
			"wire_format": resp.WireFormat.String(),
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Updated flow %s (state=%s)\n", resp.Id, resp.State.String())
	return nil
}

func flowReconfigure(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	path, err := stringFlag(cmd, "file")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	pause, err := boolFlag(cmd, "pause")
	if err != nil {
		return err
	}
	resume, err := boolFlag(cmd, "resume")
	if err != nil {
		return err
	}
	syncPublication, err := boolFlag(cmd, "sync-publication")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	if *path == "" {
		return errors.New("-file is required")
	}

	payload, err := readFile(*path)
	if err != nil {
		return fmt.Errorf("read flow file: %w", err)
	}

	var cfg flowConfig
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return fmt.Errorf("parse flow json: %w", err)
	}
	if *flowID != "" {
		cfg.ID = *flowID
	}
	if cfg.ID == "" {
		return errors.New("flow id is required (provide in file or -flow-id)")
	}

	pbFlow, err := flowConfigToProto(cfg)
	if err != nil {
		return fmt.Errorf("flow config: %w", err)
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.ReconfigureFlow(ctx, &wallabypb.ReconfigureFlowRequest{
		Flow:            pbFlow,
		PauseFirst:      proto.Bool(*pause),
		ResumeAfter:     proto.Bool(*resume),
		SyncPublication: proto.Bool(*syncPublication),
	})
	if err != nil {
		return fmt.Errorf("reconfigure flow: %w", err)
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := map[string]any{
			"id":          resp.Id,
			"name":        resp.Name,
			"state":       resp.State.String(),
			"wire_format": resp.WireFormat.String(),
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Reconfigured flow %s (state=%s)\n", resp.Id, resp.State.String())
	return nil
}

func flowRunOnce(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	if *flowID == "" {
		return errors.New("-flow-id is required")
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.RunFlowOnce(ctx, &wallabypb.RunFlowOnceRequest{FlowId: *flowID})
	if err != nil {
		return fmt.Errorf("run flow once: %w", err)
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := map[string]any{
			"flow_id":    *flowID,
			"dispatched": resp.Dispatched,
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Dispatched flow %s\n", *flowID)
	return nil
}

func flowStart(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	if *flowID == "" {
		return errors.New("-flow-id is required")
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.StartFlow(ctx, &wallabypb.StartFlowRequest{FlowId: *flowID})
	if err != nil {
		return fmt.Errorf("start flow: %w", err)
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := map[string]any{
			"id":    resp.Id,
			"state": resp.State.String(),
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Started flow %s (state=%s)\n", resp.Id, resp.State.String())
	return nil
}

func flowStop(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	if *flowID == "" {
		return errors.New("-flow-id is required")
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.StopFlow(ctx, &wallabypb.StopFlowRequest{FlowId: *flowID})
	if err != nil {
		return fmt.Errorf("stop flow: %w", err)
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := map[string]any{
			"id":    resp.Id,
			"state": resp.State.String(),
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Stopped flow %s (state=%s)\n", resp.Id, resp.State.String())
	return nil
}

func flowResume(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	if *flowID == "" {
		return errors.New("-flow-id is required")
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.ResumeFlow(ctx, &wallabypb.ResumeFlowRequest{FlowId: *flowID})
	if err != nil {
		return fmt.Errorf("resume flow: %w", err)
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := map[string]any{
			"id":    resp.Id,
			"state": resp.State.String(),
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Resumed flow %s (state=%s)\n", resp.Id, resp.State.String())
	return nil
}

func flowCleanup(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	dropSlot, err := boolFlag(cmd, "drop-slot")
	if err != nil {
		return err
	}
	dropPublication, err := boolFlag(cmd, "drop-publication")
	if err != nil {
		return err
	}
	dropState, err := boolFlag(cmd, "drop-source-state")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	if *flowID == "" {
		return errors.New("-flow-id is required")
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.CleanupFlow(ctx, &wallabypb.CleanupFlowRequest{
		FlowId:          *flowID,
		DropSlot:        proto.Bool(*dropSlot),
		DropPublication: proto.Bool(*dropPublication),
		DropSourceState: proto.Bool(*dropState),
	})
	if err != nil {
		return fmt.Errorf("cleanup flow: %w", err)
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := map[string]any{
			"flow_id": *flowID,
			"cleaned": resp.Cleaned,
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Cleaned flow %s\n", *flowID)
	return nil
}

func streamClient(endpoint string, insecureConn bool) (wallabypb.StreamServiceClient, func() error, error) {
	if endpoint == "" {
		return nil, nil, errors.New("endpoint is required")
	}
	if !insecureConn {
		return nil, nil, errors.New("secure grpc is not configured")
	}

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("connect: %w", err)
	}
	return wallabypb.NewStreamServiceClient(conn), conn.Close, nil
}

func streamPull(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	stream, err := stringFlag(cmd, "stream")
	if err != nil {
		return err
	}
	consumerGroup, err := stringFlag(cmd, "group")
	if err != nil {
		return err
	}
	max, err := intFlag(cmd, "max")
	if err != nil {
		return err
	}
	visibility, err := intFlag(cmd, "visibility")
	if err != nil {
		return err
	}
	consumerID, err := stringFlag(cmd, "consumer")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	if *stream == "" || *consumerGroup == "" {
		return errors.New("-stream and -group are required")
	}

	client, closeConn, err := streamClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	maxMessages, err := int32Arg("max", *max)
	if err != nil {
		return err
	}
	visibilitySeconds, err := int32Arg("visibility", *visibility)
	if err != nil {
		return err
	}

	resp, err := client.Pull(ctx, &wallabypb.StreamPullRequest{
		Stream:                   *stream,
		ConsumerGroup:            *consumerGroup,
		MaxMessages:              maxMessages,
		VisibilityTimeoutSeconds: visibilitySeconds,
		ConsumerId:               *consumerID,
	})
	if err != nil {
		return fmt.Errorf("pull stream: %w", err)
	}

	if *prettyOutput {
		*jsonOutput = true
	}

	if *jsonOutput {
		out := streamPullOutput{
			Stream:  *stream,
			Group:   *consumerGroup,
			Count:   len(resp.Messages),
			Records: streamPullMessages(resp.Messages),
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	if len(resp.Messages) == 0 {
		fmt.Println("No messages available.")
		return nil
	}

	fmt.Printf("Pulled %d messages:\n", len(resp.Messages))
	for _, msg := range resp.Messages {
		fmt.Printf("- id=%d table=%s lsn=%s bytes=%d\n", msg.Id, msg.Table, msg.Lsn, len(msg.Payload))
	}
	return nil
}

func streamAck(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	stream, err := stringFlag(cmd, "stream")
	if err != nil {
		return err
	}
	consumerGroup, err := stringFlag(cmd, "group")
	if err != nil {
		return err
	}
	ids, err := stringFlag(cmd, "ids")
	if err != nil {
		return err
	}

	if *stream == "" || *consumerGroup == "" {
		return errors.New("-stream and -group are required")
	}
	if *ids == "" {
		return errors.New("-ids is required")
	}

	parsed, err := parseIDs(*ids)
	if err != nil {
		return fmt.Errorf("parse ids: %w", err)
	}

	client, closeConn, err := streamClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Ack(ctx, &wallabypb.StreamAckRequest{
		Stream:        *stream,
		ConsumerGroup: *consumerGroup,
		Ids:           parsed,
	})
	if err != nil {
		return fmt.Errorf("ack stream: %w", err)
	}

	fmt.Printf("Acked %d messages\n", resp.Acked)
	return nil
}

func parseIDs(value string) ([]int64, error) {
	parts := strings.Split(value, ",")
	out := make([]int64, 0, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		var id int64
		if _, err := fmt.Sscanf(item, "%d", &id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	if len(out) == 0 {
		return nil, errors.New("no ids provided")
	}
	return out, nil
}

func int32Arg(name string, value int) (int32, error) {
	if value < 0 || value > math.MaxInt32 {
		return 0, fmt.Errorf("%s out of range: %d", name, value)
	}
	// #nosec G115 -- bounds checked above.
	return int32(value), nil
}

func publicationUsage() {
	fmt.Println("Publication subcommands:")
	fmt.Println("  wallaby-admin publication list -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin publication add -flow-id <id> -tables schema.table,...")
	fmt.Println("  wallaby-admin publication remove -flow-id <id> -tables schema.table,...")
	fmt.Println("  wallaby-admin publication sync -flow-id <id> [-tables ...] [-schemas ...] [-mode add|sync] [-pause] [-resume] [-snapshot]")
	fmt.Println("  wallaby-admin publication scrape -flow-id <id> -schemas public,app [-apply]")
	fmt.Println("  IAM flags: -aws-rds-iam -aws-region <region> -aws-profile <profile> -aws-role-arn <arn>")
	os.Exit(1)
}

func publicationList(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	dsn, err := stringFlag(cmd, "dsn")
	if err != nil {
		return err
	}
	publication, err := stringFlag(cmd, "publication")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	awsFlags, err := awsIAMOptions(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		return err
	}

	var tables []string
	if cfg.flow != nil {
		flowSvc, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			return err
		}
		defer func() { _ = closeConn() }()
		resp, err := flowSvc.ListPublicationTables(ctx, &wallabypb.ListPublicationTablesRequest{
			FlowId:      cfg.flow.Id,
			Dsn:         cfg.dsn,
			Publication: cfg.publication,
			Options:     cfg.options,
		})
		if err != nil {
			return fmt.Errorf("list publication tables: %w", err)
		}
		tables = resp.Tables
	} else {
		tables, err = postgres.ListPublicationTables(ctx, cfg.dsn, cfg.publication, cfg.options)
		if err != nil {
			return fmt.Errorf("list publication tables: %w", err)
		}
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := map[string]any{
			"publication": cfg.publication,
			"count":       len(tables),
			"tables":      tables,
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	if len(tables) == 0 {
		fmt.Println("No tables in publication.")
		return nil
	}
	fmt.Printf("Publication %s tables:\n", cfg.publication)
	for _, table := range tables {
		fmt.Printf("- %s\n", table)
	}
	return nil
}

func publicationAdd(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	dsn, err := stringFlag(cmd, "dsn")
	if err != nil {
		return err
	}
	publication, err := stringFlag(cmd, "publication")
	if err != nil {
		return err
	}
	tables, err := stringFlag(cmd, "tables")
	if err != nil {
		return err
	}
	awsFlags, err := awsIAMOptions(cmd)
	if err != nil {
		return err
	}

	if *tables == "" {
		return errors.New("-tables is required")
	}
	tableList := parseCSVValue(*tables)
	if len(tableList) == 0 {
		return errors.New("no tables provided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		return err
	}

	if cfg.flow != nil {
		flowSvc, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			return err
		}
		defer func() { _ = closeConn() }()
		if _, err := flowSvc.AddPublicationTables(ctx, &wallabypb.AddPublicationTablesRequest{
			FlowId:      cfg.flow.Id,
			Dsn:         cfg.dsn,
			Publication: cfg.publication,
			Tables:      tableList,
			Options:     cfg.options,
		}); err != nil {
			return fmt.Errorf("add publication tables: %w", err)
		}
	} else {
		if err := postgres.AddPublicationTables(ctx, cfg.dsn, cfg.publication, tableList, cfg.options); err != nil {
			return fmt.Errorf("add publication tables: %w", err)
		}
	}
	fmt.Printf("Added %d tables to publication %s\n", len(tableList), cfg.publication)
	return nil
}

func publicationRemove(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	dsn, err := stringFlag(cmd, "dsn")
	if err != nil {
		return err
	}
	publication, err := stringFlag(cmd, "publication")
	if err != nil {
		return err
	}
	tables, err := stringFlag(cmd, "tables")
	if err != nil {
		return err
	}
	awsFlags, err := awsIAMOptions(cmd)
	if err != nil {
		return err
	}

	if *tables == "" {
		return errors.New("-tables is required")
	}
	tableList := parseCSVValue(*tables)
	if len(tableList) == 0 {
		return errors.New("no tables provided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		return err
	}

	if cfg.flow != nil {
		flowSvc, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			return err
		}
		defer func() { _ = closeConn() }()
		if _, err := flowSvc.DropPublicationTables(ctx, &wallabypb.DropPublicationTablesRequest{
			FlowId:      cfg.flow.Id,
			Dsn:         cfg.dsn,
			Publication: cfg.publication,
			Tables:      tableList,
			Options:     cfg.options,
		}); err != nil {
			return fmt.Errorf("drop publication tables: %w", err)
		}
	} else {
		if err := postgres.DropPublicationTables(ctx, cfg.dsn, cfg.publication, tableList, cfg.options); err != nil {
			return fmt.Errorf("drop publication tables: %w", err)
		}
	}
	fmt.Printf("Removed %d tables from publication %s\n", len(tableList), cfg.publication)
	return nil
}

func publicationSync(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	dsn, err := stringFlag(cmd, "dsn")
	if err != nil {
		return err
	}
	publication, err := stringFlag(cmd, "publication")
	if err != nil {
		return err
	}
	tables, err := stringFlag(cmd, "tables")
	if err != nil {
		return err
	}
	schemas, err := stringFlag(cmd, "schemas")
	if err != nil {
		return err
	}
	mode, err := stringFlag(cmd, "mode")
	if err != nil {
		return err
	}
	pause, err := boolFlag(cmd, "pause")
	if err != nil {
		return err
	}
	resume, err := boolFlag(cmd, "resume")
	if err != nil {
		return err
	}
	snapshot, err := boolFlag(cmd, "snapshot")
	if err != nil {
		return err
	}
	snapshotWorkers, err := intFlag(cmd, "snapshot-workers")
	if err != nil {
		return err
	}
	partitionColumn, err := stringFlag(cmd, "partition-column")
	if err != nil {
		return err
	}
	partitionCount, err := intFlag(cmd, "partition-count")
	if err != nil {
		return err
	}
	snapshotStateBackend, err := stringFlag(cmd, "snapshot-state-backend")
	if err != nil {
		return err
	}
	snapshotStatePath, err := stringFlag(cmd, "snapshot-state-path")
	if err != nil {
		return err
	}
	awsFlags, err := awsIAMOptions(cmd)
	if err != nil {
		return err
	}

	if *flowID == "" {
		return errors.New("-flow-id is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		return err
	}
	if cfg.flow == nil {
		return errors.New("flow is required for publication sync")
	}

	desired, err := resolveDesiredTables(ctx, cfg, cfg.options, *tables, *schemas)
	if err != nil {
		return err
	}
	if len(desired) == 0 {
		return errors.New("no tables provided for sync")
	}

	flowSvc, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	if *pause {
		if _, err := flowSvc.StopFlow(ctx, &wallabypb.StopFlowRequest{FlowId: cfg.flow.Id}); err != nil {
			log.Printf("pause flow: %v", err)
		}
	}

	resp, err := flowSvc.SyncPublicationTables(ctx, &wallabypb.SyncPublicationTablesRequest{
		FlowId:      cfg.flow.Id,
		Dsn:         cfg.dsn,
		Publication: cfg.publication,
		Tables:      desired,
		Mode:        *mode,
		Options:     cfg.options,
	})
	if err != nil {
		return fmt.Errorf("sync publication: %w", err)
	}

	added := resp.Added
	removed := resp.Removed

	if *snapshot && len(added) > 0 {
		if err := runBackfill(context.Background(), cfg.flow, added, *snapshotWorkers, *partitionColumn, *partitionCount, *snapshotStateBackend, *snapshotStatePath); err != nil {
			return fmt.Errorf("backfill new tables: %w", err)
		}
	}

	if *resume {
		if _, err := flowSvc.ResumeFlow(ctx, &wallabypb.ResumeFlowRequest{FlowId: cfg.flow.Id}); err != nil {
			log.Printf("resume flow: %v", err)
		}
	}

	fmt.Printf("Synced publication %s (added=%d removed=%d)\n", cfg.publication, len(added), len(removed))
	return nil
}

func publicationScrape(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	dsn, err := stringFlag(cmd, "dsn")
	if err != nil {
		return err
	}
	publication, err := stringFlag(cmd, "publication")
	if err != nil {
		return err
	}
	schemas, err := stringFlag(cmd, "schemas")
	if err != nil {
		return err
	}
	apply, err := boolFlag(cmd, "apply")
	if err != nil {
		return err
	}
	awsFlags, err := awsIAMOptions(cmd)
	if err != nil {
		return err
	}

	if *schemas == "" {
		return errors.New("-schemas is required")
	}
	schemaList := parseCSVValue(*schemas)
	if len(schemaList) == 0 {
		return errors.New("no schemas provided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		return err
	}

	var missing []string
	var discoveredTables []string
	if cfg.flow != nil {
		flowSvc, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			return err
		}
		defer func() { _ = closeConn() }()
		resp, err := flowSvc.ScrapePublicationTables(ctx, &wallabypb.ScrapePublicationTablesRequest{
			FlowId:      cfg.flow.Id,
			Dsn:         cfg.dsn,
			Publication: cfg.publication,
			Schemas:     schemaList,
			Apply:       *apply,
			Options:     cfg.options,
		})
		if err != nil {
			return fmt.Errorf("scrape publication tables: %w", err)
		}
		discoveredTables = resp.DiscoveredTables
		missing = resp.MissingTables
	} else {
		discoveredTables, err = postgres.ScrapeTables(ctx, cfg.dsn, schemaList, cfg.options)
		if err != nil {
			return fmt.Errorf("scrape tables: %w", err)
		}
		current, err := postgres.ListPublicationTables(ctx, cfg.dsn, cfg.publication, cfg.options)
		if err != nil {
			return fmt.Errorf("list publication tables: %w", err)
		}

		currentSet := make(map[string]struct{}, len(current))
		for _, table := range current {
			currentSet[strings.ToLower(table)] = struct{}{}
		}
		missing = make([]string, 0)
		for _, table := range discoveredTables {
			if _, ok := currentSet[strings.ToLower(table)]; !ok {
				missing = append(missing, table)
			}
		}
	}

	if *apply && len(missing) > 0 {
		if cfg.flow == nil {
			if err := postgres.AddPublicationTables(ctx, cfg.dsn, cfg.publication, missing, cfg.options); err != nil {
				return fmt.Errorf("add publication tables: %w", err)
			}
		}
		fmt.Printf("Added %d tables to publication %s\n", len(missing), cfg.publication)
		return nil
	}

	if len(missing) == 0 {
		fmt.Println("No new tables found.")
		return nil
	}
	fmt.Printf("New tables not in publication %s:\n", cfg.publication)
	for _, table := range missing {
		fmt.Printf("- %s\n", table)
	}
	return nil
}

func flowResolveStaging(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	tables, err := stringFlag(cmd, "tables")
	if err != nil {
		return err
	}
	schemas, err := stringFlag(cmd, "schemas")
	if err != nil {
		return err
	}
	destName, err := stringFlag(cmd, "dest")
	if err != nil {
		return err
	}

	if *flowID == "" {
		return errors.New("-flow-id is required")
	}

	ctx := context.Background()
	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	flowResp, err := client.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: *flowID})
	if err != nil {
		return fmt.Errorf("load flow: %w", err)
	}
	model, err := flowFromProto(flowResp)
	if err != nil {
		return fmt.Errorf("parse flow: %w", err)
	}

	tableList, err := resolveFlowTables(ctx, model, *tables, *schemas)
	if err != nil {
		return fmt.Errorf("resolve tables: %w", err)
	}
	if len(tableList) == 0 {
		return errors.New("no tables resolved")
	}
	schemaList, err := parseSchemaTables(tableList)
	if err != nil {
		return fmt.Errorf("parse tables: %w", err)
	}

	factory := runner.Factory{}
	destinations, err := factory.Destinations(model.Destinations)
	if err != nil {
		return fmt.Errorf("build destinations: %w", err)
	}

	resolved := 0
	for _, dest := range destinations {
		if *destName != "" && dest.Spec.Name != *destName {
			continue
		}
		if dest.Spec.Options == nil {
			dest.Spec.Options = map[string]string{}
		}
		if dest.Spec.Options["flow_id"] == "" {
			dest.Spec.Options["flow_id"] = model.ID
		}
		if err := dest.Dest.Open(ctx, dest.Spec); err != nil {
			return fmt.Errorf("open destination %s: %w", dest.Spec.Name, err)
		}

		if resolver, ok := dest.Dest.(stream.StagingResolverFor); ok {
			if err := resolver.ResolveStagingFor(ctx, schemaList); err != nil {
				_ = dest.Dest.Close(ctx)
				return fmt.Errorf("resolve staging for %s: %w", dest.Spec.Name, err)
			}
			resolved++
		} else if resolver, ok := dest.Dest.(stream.StagingResolver); ok {
			if err := resolver.ResolveStaging(ctx); err != nil {
				_ = dest.Dest.Close(ctx)
				return fmt.Errorf("resolve staging for %s: %w", dest.Spec.Name, err)
			}
			resolved++
		}

		if err := dest.Dest.Close(ctx); err != nil {
			return fmt.Errorf("close destination %s: %w", dest.Spec.Name, err)
		}
	}

	fmt.Printf("Resolved staging for %d destinations\n", resolved)
	return nil
}

func flowList(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	pageSize, err := intFlag(cmd, "page-size")
	if err != nil {
		return err
	}
	pageToken, err := stringFlag(cmd, "page-token")
	if err != nil {
		return err
	}
	state, err := stringFlag(cmd, "state")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	filterState := wallabypb.FlowState_FLOW_STATE_UNSPECIFIED
	if strings.TrimSpace(*state) != "" {
		parsed, err := parseFlowState(*state)
		if err != nil {
			return err
		}
		filterState = parsed
	}

	req := &wallabypb.ListFlowsRequest{PageSize: 0, PageToken: *pageToken}
	if *pageSize > 0 {
		if value, ok := safeInt32(*pageSize); ok {
			req.PageSize = value
		} else {
			return fmt.Errorf("invalid page-size: %d", *pageSize)
		}
	}

	resp, err := client.ListFlows(ctx, req)
	if err != nil {
		return fmt.Errorf("list flows: %w", err)
	}
	flows := make([]flowSummary, 0, len(resp.Flows))
	for _, item := range resp.Flows {
		if item == nil {
			continue
		}
		if filterState != wallabypb.FlowState_FLOW_STATE_UNSPECIFIED && item.State != filterState {
			continue
		}
		flows = append(flows, flowSummaryFromProto(item))
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := flowListOutput{
			Count:         len(flows),
			NextPageToken: resp.NextPageToken,
			Flows:         flows,
		}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	if len(flows) == 0 {
		if filterState != wallabypb.FlowState_FLOW_STATE_UNSPECIFIED {
			fmt.Printf("No flows in state %q.\n", flowStateName(filterState))
		} else {
			fmt.Println("No flows found.")
		}
		return nil
	}
	rows := make([][]string, 0, len(flows))
	for _, item := range flows {
		rows = append(rows, []string{
			item.ID,
			item.State,
			item.Source.Name,
			fmt.Sprintf("%d", len(item.Destinations)),
			item.Name,
		})
	}
	renderTextTable([]string{"ID", "STATE", "SOURCE", "DST", "NAME"}, rows)
	return nil
}

func flowGet(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	if *flowID == "" {
		return errors.New("-flow-id is required")
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: *flowID})
	if err != nil {
		return fmt.Errorf("get flow: %w", err)
	}

	output := flowDetailFromProto(resp)
	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(output); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Flow: %s\n", output.ID)
	fmt.Printf("  Name:    %s\n", output.Name)
	fmt.Printf("  State:   %s\n", output.State)
	fmt.Printf("  Source:  %s (%s)\n", output.Source.Name, output.Source.Type)
	fmt.Printf("  Wire:    %s\n", output.WireFormat)
	fmt.Printf("  Destinations: %d\n", len(output.Destinations))
	for _, dest := range output.Destinations {
		fmt.Printf("  - %s (%s)\n", dest.Name, dest.Type)
	}
	return nil
}

func flowDelete(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	if *flowID == "" {
		return errors.New("-flow-id is required")
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.DeleteFlow(ctx, &wallabypb.DeleteFlowRequest{FlowId: *flowID})
	if err != nil {
		return fmt.Errorf("delete flow: %w", err)
	}
	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		out := flowDeleteOutput{FlowID: *flowID, Deleted: resp.Deleted}
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}
	fmt.Printf("Deleted flow %s\n", *flowID)
	return nil
}

func flowWait(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	flowID, err := stringFlag(cmd, "flow-id")
	if err != nil {
		return err
	}
	targetState, err := stringFlag(cmd, "state")
	if err != nil {
		return err
	}
	timeout, err := durationFlag(cmd, "timeout")
	if err != nil {
		return err
	}
	interval, err := durationFlag(cmd, "interval")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}

	if *flowID == "" {
		return errors.New("-flow-id is required")
	}
	if *targetState == "" {
		return errors.New("-state is required")
	}
	if *interval <= 0 {
		return errors.New("-interval must be greater than 0")
	}
	if *timeout <= 0 {
		return errors.New("-timeout must be greater than 0")
	}

	target, err := parseFlowState(*targetState)
	if err != nil {
		return err
	}

	client, closeConn, err := flowClient(*endpoint, *insecureConn)
	if err != nil {
		return err
	}
	defer func() { _ = closeConn() }()

	deadline := time.Now().Add(*timeout)
	start := time.Now()
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: *flowID})
		cancel()
		if err != nil {
			return fmt.Errorf("wait flow: %w", err)
		}
		if resp.State == target {
			out := flowWaitOutput{
				FlowID:    *flowID,
				StateRaw:  int32(resp.State),
				State:     flowStateName(resp.State),
				Name:      resp.Name,
				Elapsed:   time.Since(start).String(),
				StateName: flowStateName(resp.State),
			}
			if *prettyOutput {
				*jsonOutput = true
			}
			if *jsonOutput {
				enc := json.NewEncoder(os.Stdout)
				if *prettyOutput {
					enc.SetIndent("", "  ")
				}
				if err := enc.Encode(out); err != nil {
					return fmt.Errorf("encode json: %w", err)
				}
				return nil
			}
			fmt.Printf("Flow %s reached state %s\n", *flowID, out.State)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for flow %s to reach %s", *flowID, flowStateName(target))
		}
		<-ticker.C
	}
}

func flowValidate(cmd *cobra.Command, _ []string) error {
	path, err := stringFlag(cmd, "file")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	if *path == "" {
		return errors.New("-file is required")
	}

	payload, err := readFile(*path)
	if err != nil {
		return fmt.Errorf("read flow file: %w", err)
	}

	var cfg flowConfig
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return fmt.Errorf("parse flow json: %w", err)
	}
	if cfg.Name == "" {
		return errors.New("flow name is required")
	}
	if cfg.Source.Type == "" {
		return errors.New("source.type is required")
	}
	if len(cfg.Destinations) == 0 {
		return errors.New("at least one destination is required")
	}

	_, err = flowConfigToProto(cfg)
	if err != nil {
		return fmt.Errorf("validate flow config: %w", err)
	}

	if *prettyOutput {
		*jsonOutput = true
	}
	out := flowValidateOutput{
		Valid:            true,
		ID:               cfg.ID,
		Name:             cfg.Name,
		Source:           cfg.Source,
		DestinationCount: len(cfg.Destinations),
	}
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Flow config is valid: %s (%s -> %d destination)\n", out.Name, out.Source.Type, out.DestinationCount)
	return nil
}

func flowDryRun(cmd *cobra.Command, _ []string) error {
	path, err := stringFlag(cmd, "file")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	if *path == "" {
		return errors.New("-file is required")
	}

	payload, err := readFile(*path)
	if err != nil {
		return fmt.Errorf("read flow file: %w", err)
	}

	var cfg flowConfig
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return fmt.Errorf("parse flow json: %w", err)
	}

	pbFlow, err := flowConfigToProto(cfg)
	if err != nil {
		return fmt.Errorf("flow config: %w", err)
	}

	out := flowDetailFromProto(pbFlow)
	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Flow dry-run: %s -> %s (%s destination)\n", out.Name, out.State, out.WireFormat)
	fmt.Printf("Source: %s (%s)\n", out.Source.Name, out.Source.Type)
	for _, dest := range out.Destinations {
		fmt.Printf("Destination: %s (%s)\n", dest.Name, dest.Type)
	}
	return nil
}

func flowPlan(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	path, err := stringFlag(cmd, "file")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	if *path == "" {
		return errors.New("-file is required")
	}

	payload, err := readFile(*path)
	if err != nil {
		return fmt.Errorf("read flow file: %w", err)
	}
	var cfg flowConfig
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return fmt.Errorf("parse flow json: %w", err)
	}

	pbFlow, err := flowConfigToProto(cfg)
	if err != nil {
		return fmt.Errorf("flow config: %w", err)
	}
	plan := flowPlanOutput{
		FlowID:      pbFlow.Id,
		FlowName:    pbFlow.Name,
		Desired:     flowDetailFromProto(pbFlow),
		Compared:    false,
		ChangeCount: 0,
	}
	if *endpoint != "" {
		client, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			return err
		}
		defer func() { _ = closeConn() }()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: pbFlow.Id})
		if err == nil && resp != nil {
			current := flowDetailFromProto(resp)
			plan.Current = &current
			plan.Compared = true
			plan.Changes = append(plan.Changes, flowPlanDiff("name", current.Name, plan.Desired.Name)...)
			plan.Changes = append(plan.Changes, flowPlanDiff("wire_format", current.WireFormat, plan.Desired.WireFormat)...)
			plan.Changes = append(plan.Changes, flowPlanDiffInt("state_raw", int(current.StateRaw), int(plan.Desired.StateRaw))...)
			plan.Changes = append(plan.Changes, compareFlowEndpointList("destinations", current.Destinations, plan.Desired.Destinations)...)
			plan.ChangeCount = len(plan.Changes)
		} else if err != nil {
			return fmt.Errorf("load flow for plan: %w", err)
		}
	}

	plan.ChangeCount = len(plan.Changes)

	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(plan); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Flow plan for %q (%s)\n", plan.FlowName, plan.FlowID)
	if !plan.Compared {
		fmt.Println("No existing flow provided for diff; planning from local config only.")
		return nil
	}
	if plan.ChangeCount == 0 {
		fmt.Println("No changes detected.")
		return nil
	}
	for _, change := range plan.Changes {
		fmt.Printf("- %s: %s -> %s\n", change.Path, change.Before, change.After)
	}
	return nil
}

func flowPlanDiff(field, before, after string) []flowPlanChange {
	if before == after {
		return nil
	}
	return []flowPlanChange{{Path: field, Before: before, After: after}}
}

func flowPlanDiffInt(field string, before, after int) []flowPlanChange {
	if before == after {
		return nil
	}
	return []flowPlanChange{{Path: field, Before: fmt.Sprintf("%d", before), After: fmt.Sprintf("%d", after)}}
}

func compareFlowEndpointList(field string, before, after []flowEndpointInfoDetail) []flowPlanChange {
	beforeEncoded, err := json.Marshal(before)
	if err != nil {
		return []flowPlanChange{{Path: field, Before: "(invalid)", After: "(invalid)"}}
	}
	afterEncoded, err := json.Marshal(after)
	if err != nil {
		return []flowPlanChange{{Path: field, Before: "(invalid)", After: "(invalid)"}}
	}
	if string(beforeEncoded) == string(afterEncoded) {
		return nil
	}
	return []flowPlanChange{
		{
			Path:   field,
			Before: string(beforeEncoded),
			After:  string(afterEncoded),
		},
	}
}

func flowCheck(cmd *cobra.Command, _ []string) error {
	endpoint, err := stringFlag(cmd, "endpoint")
	if err != nil {
		return err
	}
	insecureConn, err := boolFlag(cmd, "insecure")
	if err != nil {
		return err
	}
	path, err := stringFlag(cmd, "file")
	if err != nil {
		return err
	}
	jsonOutput, err := boolFlag(cmd, "json")
	if err != nil {
		return err
	}
	prettyOutput, err := boolFlag(cmd, "pretty")
	if err != nil {
		return err
	}
	if *path == "" {
		return errors.New("-file is required")
	}

	payload, err := readFile(*path)
	if err != nil {
		return fmt.Errorf("read flow file: %w", err)
	}

	var cfg flowConfig
	if err := json.Unmarshal(payload, &cfg); err != nil {
		return fmt.Errorf("parse flow json: %w", err)
	}
	if cfg.Name == "" {
		return errors.New("flow name is required")
	}
	if cfg.Source.Type == "" {
		return errors.New("source.type is required")
	}
	if len(cfg.Destinations) == 0 {
		return errors.New("at least one destination is required")
	}

	if _, err := flowConfigToProto(cfg); err != nil {
		return fmt.Errorf("validate flow config: %w", err)
	}

	var endpointMsg string
	if *endpoint != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, closeConn, err := flowClient(*endpoint, *insecureConn)
		if err != nil {
			return fmt.Errorf("flow endpoint check: %w", err)
		}
		defer func() { _ = closeConn() }()
		_, err = client.ListFlows(ctx, &wallabypb.ListFlowsRequest{PageSize: 1})
		if err != nil {
			return fmt.Errorf("flow endpoint check: %w", err)
		}
		endpointMsg = *endpoint
	}

	out := map[string]any{
		"valid":              true,
		"name":               cfg.Name,
		"flow_id":            cfg.ID,
		"source_type":        cfg.Source.Type,
		"destination_count":  len(cfg.Destinations),
		"endpoint_checked":   *endpoint != "",
		"endpoint_reachable": true,
		"endpoint":           endpointMsg,
	}
	if *prettyOutput {
		*jsonOutput = true
	}
	if *jsonOutput {
		enc := json.NewEncoder(os.Stdout)
		if *prettyOutput {
			enc.SetIndent("", "  ")
		}
		if err := enc.Encode(out); err != nil {
			return fmt.Errorf("encode json: %w", err)
		}
		return nil
	}

	fmt.Printf("Flow config is valid: %s (%d destination)\n", cfg.Name, len(cfg.Destinations))
	if *endpoint != "" {
		fmt.Printf("Endpoint reachable: %s\n", endpointMsg)
	}
	return nil
}

func flowSummaryFromProto(pbFlow *wallabypb.Flow) flowSummary {
	if pbFlow == nil {
		return flowSummary{}
	}

	item := flowSummary{
		ID:          pbFlow.Id,
		Name:        pbFlow.Name,
		State:       flowStateName(pbFlow.State),
		StateRaw:    int32(pbFlow.State),
		WireFormat:  wireFormatName(pbFlow.WireFormat),
		Parallelism: pbFlow.Parallelism,
	}
	if pbFlow.Source != nil {
		item.Source = flowEndpointInfoFromProto(pbFlow.Source)
	}
	for _, dest := range pbFlow.Destinations {
		item.Destinations = append(item.Destinations, flowEndpointInfoFromProto(dest))
	}
	return item
}

func flowDetailFromProto(pbFlow *wallabypb.Flow) flowDetail {
	if pbFlow == nil {
		return flowDetail{}
	}

	item := flowDetail{
		ID:          pbFlow.Id,
		Name:        pbFlow.Name,
		State:       flowStateName(pbFlow.State),
		StateRaw:    int32(pbFlow.State),
		Source:      flowEndpointInfoDetailFromProto(pbFlow.Source),
		WireFormat:  wireFormatName(pbFlow.WireFormat),
		Parallelism: pbFlow.Parallelism,
	}
	if pbFlow.Config != nil {
		item.Config = flowConfigInfo{
			AckPolicy:                       ackPolicyName(pbFlow.Config.AckPolicy),
			PrimaryDestination:              pbFlow.Config.PrimaryDestination,
			FailureMode:                     failureModeName(pbFlow.Config.FailureMode),
			GiveUpPolicy:                    giveUpPolicyName(pbFlow.Config.GiveUpPolicy),
			SchemaRegistrySubject:           pbFlow.Config.SchemaRegistrySubject,
			SchemaRegistryProtoTypesSubject: pbFlow.Config.SchemaRegistryProtoTypesSubject,
			SchemaRegistrySubjectMode:       pbFlow.Config.SchemaRegistrySubjectMode,
		}
	}
	for _, dest := range pbFlow.Destinations {
		item.Destinations = append(item.Destinations, flowEndpointInfoDetailFromProto(dest))
	}
	return item
}

func flowEndpointInfoFromProto(endpoint *wallabypb.Endpoint) flowEndpointInfo {
	if endpoint == nil {
		return flowEndpointInfo{}
	}
	return flowEndpointInfo{
		Name:    endpoint.Name,
		Type:    strings.TrimPrefix(strings.ToLower(endpoint.Type.String()), "endpoint_type_"),
		TypeRaw: int32(endpoint.Type),
	}
}

func flowEndpointInfoDetailFromProto(endpoint *wallabypb.Endpoint) flowEndpointInfoDetail {
	if endpoint == nil {
		return flowEndpointInfoDetail{}
	}
	return flowEndpointInfoDetail{
		Name:    endpoint.Name,
		Type:    strings.TrimPrefix(strings.ToLower(endpoint.Type.String()), "endpoint_type_"),
		TypeRaw: int32(endpoint.Type),
		Options: endpoint.Options,
	}
}

func wireFormatName(format wallabypb.WireFormat) string {
	switch format {
	case wallabypb.WireFormat_WIRE_FORMAT_ARROW:
		return "arrow"
	case wallabypb.WireFormat_WIRE_FORMAT_PARQUET:
		return "parquet"
	case wallabypb.WireFormat_WIRE_FORMAT_PROTO:
		return "proto"
	case wallabypb.WireFormat_WIRE_FORMAT_AVRO:
		return "avro"
	case wallabypb.WireFormat_WIRE_FORMAT_JSON:
		return "json"
	default:
		return "unspecified"
	}
}

func ackPolicyName(policy wallabypb.AckPolicy) string {
	switch policy {
	case wallabypb.AckPolicy_ACK_POLICY_ALL:
		return "all"
	case wallabypb.AckPolicy_ACK_POLICY_PRIMARY:
		return "primary"
	default:
		return "unspecified"
	}
}

func failureModeName(mode wallabypb.FailureMode) string {
	switch mode {
	case wallabypb.FailureMode_FAILURE_MODE_HOLD_SLOT:
		return "hold_slot"
	case wallabypb.FailureMode_FAILURE_MODE_DROP_SLOT:
		return "drop_slot"
	default:
		return "unspecified"
	}
}

func giveUpPolicyName(policy wallabypb.GiveUpPolicy) string {
	switch policy {
	case wallabypb.GiveUpPolicy_GIVE_UP_POLICY_NEVER:
		return "never"
	case wallabypb.GiveUpPolicy_GIVE_UP_POLICY_ON_RETRY_EXHAUSTION:
		return "on_retry_exhaustion"
	default:
		return "unspecified"
	}
}

func parseFlowState(value string) (wallabypb.FlowState, error) {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "created", "flow_state_created":
		return wallabypb.FlowState_FLOW_STATE_CREATED, nil
	case "running", "flow_state_running":
		return wallabypb.FlowState_FLOW_STATE_RUNNING, nil
	case "paused", "flow_state_paused":
		return wallabypb.FlowState_FLOW_STATE_PAUSED, nil
	case "stopping", "flow_state_stopping":
		return wallabypb.FlowState_FLOW_STATE_STOPPING, nil
	case "failed", "flow_state_failed":
		return wallabypb.FlowState_FLOW_STATE_FAILED, nil
	default:
		return wallabypb.FlowState_FLOW_STATE_UNSPECIFIED, fmt.Errorf("invalid state %q", value)
	}
}

func flowStateName(state wallabypb.FlowState) string {
	switch state {
	case wallabypb.FlowState_FLOW_STATE_CREATED:
		return "created"
	case wallabypb.FlowState_FLOW_STATE_RUNNING:
		return "running"
	case wallabypb.FlowState_FLOW_STATE_PAUSED:
		return "paused"
	case wallabypb.FlowState_FLOW_STATE_STOPPING:
		return "stopping"
	case wallabypb.FlowState_FLOW_STATE_FAILED:
		return "failed"
	default:
		return "unspecified"
	}
}

func safeInt32(value int) (int32, bool) {
	if value <= 0 {
		return 0, false
	}
	return int32(value), true
}

type flowListOutput struct {
	Count         int           `json:"count"`
	NextPageToken string        `json:"next_page_token,omitempty"`
	Flows         []flowSummary `json:"flows"`
}

type flowSummary struct {
	ID           string             `json:"id"`
	Name         string             `json:"name"`
	State        string             `json:"state"`
	StateRaw     int32              `json:"state_raw"`
	WireFormat   string             `json:"wire_format"`
	Parallelism  int32              `json:"parallelism"`
	Source       flowEndpointInfo   `json:"source"`
	Destinations []flowEndpointInfo `json:"destinations"`
}

type flowDetail struct {
	ID           string                   `json:"id"`
	Name         string                   `json:"name"`
	State        string                   `json:"state"`
	StateRaw     int32                    `json:"state_raw"`
	Source       flowEndpointInfoDetail   `json:"source"`
	Destinations []flowEndpointInfoDetail `json:"destinations"`
	WireFormat   string                   `json:"wire_format"`
	Parallelism  int32                    `json:"parallelism"`
	Config       flowConfigInfo           `json:"config"`
}

type flowEndpointInfo struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	TypeRaw int32  `json:"type_raw"`
}

type flowEndpointInfoDetail struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	TypeRaw int32             `json:"type_raw"`
	Options map[string]string `json:"options,omitempty"`
}

type flowConfigInfo struct {
	AckPolicy                       string `json:"ack_policy,omitempty"`
	PrimaryDestination              string `json:"primary_destination,omitempty"`
	FailureMode                     string `json:"failure_mode,omitempty"`
	GiveUpPolicy                    string `json:"give_up_policy,omitempty"`
	SchemaRegistrySubject           string `json:"schema_registry_subject,omitempty"`
	SchemaRegistryProtoTypesSubject string `json:"schema_registry_proto_types_subject,omitempty"`
	SchemaRegistrySubjectMode       string `json:"schema_registry_subject_mode,omitempty"`
}

type flowDeleteOutput struct {
	FlowID  string `json:"flow_id"`
	Deleted bool   `json:"deleted"`
}

type flowWaitOutput struct {
	FlowID    string `json:"flow_id"`
	Name      string `json:"name"`
	StateRaw  int32  `json:"state_raw"`
	State     string `json:"state"`
	StateName string `json:"state_name"`
	Elapsed   string `json:"elapsed"`
}

type checkResult struct {
	CheckedAt         string                `json:"checked_at"`
	Endpoint          string                `json:"endpoint"`
	AdminReachable    bool                  `json:"admin_reachable"`
	EndpointError     string                `json:"endpoint_error,omitempty"`
	FlowID            string                `json:"flow_id,omitempty"`
	FlowName          string                `json:"flow_name,omitempty"`
	ConfigFile        string                `json:"config_file,omitempty"`
	Source            endpointCheckResult   `json:"source"`
	Destinations      []endpointCheckResult `json:"destinations"`
	EndpointReachable bool                  `json:"endpoint_reachable,omitempty"`
	EndpointChecked   bool                  `json:"endpoint_checked,omitempty"`
}

type endpointCheckResult struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Source    bool   `json:"source"`
	Checked   bool   `json:"checked"`
	Reachable bool   `json:"reachable"`
	Error     string `json:"error,omitempty"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
}

type ddlHistoryOutput struct {
	FlowID string          `json:"flow_id"`
	Status string          `json:"status"`
	Count  int             `json:"count"`
	Events []ddlListRecord `json:"events"`
}

type flowPlanOutput struct {
	FlowID      string           `json:"flow_id"`
	FlowName    string           `json:"flow_name"`
	Compared    bool             `json:"compared"`
	ChangeCount int              `json:"change_count"`
	Current     *flowDetail      `json:"current,omitempty"`
	Desired     flowDetail       `json:"desired"`
	Changes     []flowPlanChange `json:"changes"`
}

type flowPlanChange struct {
	Path   string `json:"path"`
	Before string `json:"before"`
	After  string `json:"after"`
}

type slotListOutput struct {
	Count int              `json:"count"`
	Slots []slotListRecord `json:"slots"`
}

type slotListRecord struct {
	SlotName          string `json:"slot_name"`
	Plugin            string `json:"plugin"`
	SlotType          string `json:"slot_type"`
	Database          string `json:"database"`
	Active            bool   `json:"active"`
	ActivePID         *int32 `json:"active_pid,omitempty"`
	Temporary         bool   `json:"temporary"`
	WalStatus         string `json:"wal_status"`
	RestartLSN        string `json:"restart_lsn,omitempty"`
	ConfirmedFlushLSN string `json:"confirmed_flush_lsn,omitempty"`
}

type slotDropOutput struct {
	SlotName string `json:"slot"`
	Found    bool   `json:"found"`
	Dropped  bool   `json:"dropped"`
}

type publicationConfig struct {
	dsn         string
	publication string
	flow        *wallabypb.Flow
	options     map[string]string
}

type slotConfig struct {
	dsn     string
	slot    string
	options map[string]string
	flow    *wallabypb.Flow
}

type flowValidateOutput struct {
	Valid            bool           `json:"valid"`
	ID               string         `json:"id"`
	Name             string         `json:"name"`
	Source           endpointConfig `json:"source"`
	DestinationCount int            `json:"destination_count"`
}

func resolvePublicationConfig(ctx context.Context, endpoint string, insecureConn bool, flowID, dsn, publication string, options map[string]string) (publicationConfig, error) {
	cfg := publicationConfig{dsn: dsn, publication: publication, options: options}
	if flowID == "" {
		if cfg.dsn == "" || cfg.publication == "" {
			return cfg, errors.New("flow-id or dsn/publication is required")
		}
		return cfg, nil
	}

	client, closeConn, err := flowClient(endpoint, insecureConn)
	if err != nil {
		return cfg, err
	}
	defer func() { _ = closeConn() }()

	flowResp, err := client.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: flowID})
	if err != nil {
		return cfg, fmt.Errorf("load flow: %w", err)
	}
	cfg.flow = flowResp
	if cfg.dsn == "" {
		cfg.dsn = flowResp.Source.Options["dsn"]
	}
	if cfg.publication == "" {
		cfg.publication = flowResp.Source.Options["publication"]
	}
	cfg.options = mergeOptionMaps(flowResp.Source.Options, cfg.options)
	if cfg.dsn == "" || cfg.publication == "" {
		return cfg, errors.New("source dsn/publication not found on flow")
	}
	return cfg, nil
}

func resolveSlotConfig(ctx context.Context, endpoint string, insecureConn bool, flowID, dsn, slot string, requireSlot bool, options map[string]string) (slotConfig, error) {
	cfg := slotConfig{dsn: dsn, slot: slot, options: options}
	if flowID == "" {
		if cfg.dsn == "" {
			return cfg, errors.New("flow-id or dsn is required")
		}
		if requireSlot && cfg.slot == "" {
			return cfg, errors.New("slot is required when -flow-id is not provided")
		}
		return cfg, nil
	}

	client, closeConn, err := flowClient(endpoint, insecureConn)
	if err != nil {
		return cfg, err
	}
	defer func() { _ = closeConn() }()

	flowResp, err := client.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: flowID})
	if err != nil {
		return cfg, fmt.Errorf("load flow: %w", err)
	}
	if flowResp.Source == nil {
		return cfg, errors.New("flow has no source")
	}
	if flowResp.Source.Type != wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES {
		return cfg, errors.New("flow source is not postgres")
	}
	cfg.options = mergeOptionMaps(flowResp.Source.Options, cfg.options)
	if cfg.dsn == "" {
		cfg.dsn = flowResp.Source.Options["dsn"]
	}
	cfg.flow = flowResp
	if cfg.slot == "" {
		cfg.slot = flowResp.Source.Options["slot"]
	}
	if cfg.dsn == "" {
		return cfg, errors.New("source dsn not found on flow")
	}
	if requireSlot && cfg.slot == "" {
		return cfg, errors.New("source slot not found on flow")
	}
	return cfg, nil
}

func slotListRecordFromInfo(info postgres.ReplicationSlotInfo) slotListRecord {
	return slotListRecord{
		SlotName:          info.SlotName,
		Plugin:            info.Plugin,
		SlotType:          info.SlotType,
		Database:          info.Database,
		Active:            info.Active,
		ActivePID:         info.ActivePID,
		Temporary:         info.Temporary,
		WalStatus:         info.WalStatus,
		RestartLSN:        info.RestartLSN,
		ConfirmedFlushLSN: info.ConfirmedLSN,
	}
}

func slotListRecordFromProto(info *wallabypb.ReplicationSlotInfo) slotListRecord {
	if info == nil {
		return slotListRecord{}
	}
	var activePID *int32
	if info.ActivePidPresent {
		value := info.ActivePid
		activePID = &value
	}
	return slotListRecord{
		SlotName:          info.SlotName,
		Plugin:            info.Plugin,
		SlotType:          info.SlotType,
		Database:          info.Database,
		Active:            info.Active,
		ActivePID:         activePID,
		Temporary:         info.Temporary,
		WalStatus:         info.WalStatus,
		RestartLSN:        info.RestartLsn,
		ConfirmedFlushLSN: info.ConfirmedFlushLsn,
	}
}

func resolveDesiredTables(ctx context.Context, cfg publicationConfig, options map[string]string, tables, schemas string) ([]string, error) {
	if tables != "" {
		return parseCSVValue(tables), nil
	}
	if schemas != "" {
		list := parseCSVValue(schemas)
		if len(list) == 0 {
			return nil, errors.New("no schemas provided")
		}
		return postgres.ScrapeTables(ctx, cfg.dsn, list, options)
	}
	if cfg.flow != nil {
		if value := cfg.flow.Source.Options["publication_tables"]; value != "" {
			return parseCSVValue(value), nil
		}
		if value := cfg.flow.Source.Options["tables"]; value != "" {
			return parseCSVValue(value), nil
		}
		if value := cfg.flow.Source.Options["publication_schemas"]; value != "" {
			return postgres.ScrapeTables(ctx, cfg.dsn, parseCSVValue(value), options)
		}
		if value := cfg.flow.Source.Options["schemas"]; value != "" {
			return postgres.ScrapeTables(ctx, cfg.dsn, parseCSVValue(value), options)
		}
	}
	return nil, errors.New("no tables or schemas specified")
}

func resolveFlowTables(ctx context.Context, model flow.Flow, tables, schemas string) ([]string, error) {
	if tables != "" {
		return parseCSVValue(tables), nil
	}
	if schemas != "" {
		schemaList := parseCSVValue(schemas)
		if len(schemaList) == 0 {
			return nil, errors.New("no schemas provided")
		}
		return scrapeFlowTables(ctx, model, schemaList)
	}
	if model.Source.Options != nil {
		if value := model.Source.Options["tables"]; value != "" {
			return parseCSVValue(value), nil
		}
		if value := model.Source.Options["schemas"]; value != "" {
			return scrapeFlowTables(ctx, model, parseCSVValue(value))
		}
		if value := model.Source.Options["publication_tables"]; value != "" {
			return parseCSVValue(value), nil
		}
		if value := model.Source.Options["publication_schemas"]; value != "" {
			return scrapeFlowTables(ctx, model, parseCSVValue(value))
		}
	}
	return nil, errors.New("no tables or schemas specified")
}

func scrapeFlowTables(ctx context.Context, model flow.Flow, schemas []string) ([]string, error) {
	if len(schemas) == 0 {
		return nil, errors.New("no schemas provided")
	}
	if model.Source.Options == nil {
		return nil, errors.New("source options missing dsn")
	}
	dsn := model.Source.Options["dsn"]
	if dsn == "" {
		return nil, errors.New("source dsn missing")
	}
	return postgres.ScrapeTables(ctx, dsn, schemas, model.Source.Options)
}

type awsIAMFlags struct {
	enabled         *bool
	region          *string
	profile         *string
	roleARN         *string
	roleSessionName *string
	roleExternalID  *string
	endpoint        *string
}

func addAWSIAMFlags(cmd *cobra.Command) {
	fs := cmd.Flags()
	fs.Bool("aws-rds-iam", false, "use AWS RDS IAM authentication")
	fs.String("aws-region", "", "AWS region for RDS IAM authentication")
	fs.String("aws-profile", "", "AWS profile for RDS IAM authentication")
	fs.String("aws-role-arn", "", "AWS role ARN to assume for RDS IAM")
	fs.String("aws-role-session-name", "", "AWS role session name for RDS IAM")
	fs.String("aws-role-external-id", "", "AWS role external id for RDS IAM")
	fs.String("aws-endpoint", "", "AWS endpoint override for RDS IAM")
}

func (f *awsIAMFlags) options() map[string]string {
	if f == nil {
		return nil
	}
	enabled := false
	options := map[string]string{}
	if f.enabled != nil && *f.enabled {
		enabled = true
	}
	if f.region != nil && strings.TrimSpace(*f.region) != "" {
		options["aws_region"] = strings.TrimSpace(*f.region)
		enabled = true
	}
	if f.profile != nil && strings.TrimSpace(*f.profile) != "" {
		options["aws_profile"] = strings.TrimSpace(*f.profile)
		enabled = true
	}
	if f.roleARN != nil && strings.TrimSpace(*f.roleARN) != "" {
		options["aws_role_arn"] = strings.TrimSpace(*f.roleARN)
		enabled = true
	}
	if f.roleSessionName != nil && strings.TrimSpace(*f.roleSessionName) != "" {
		options["aws_role_session_name"] = strings.TrimSpace(*f.roleSessionName)
		enabled = true
	}
	if f.roleExternalID != nil && strings.TrimSpace(*f.roleExternalID) != "" {
		options["aws_role_external_id"] = strings.TrimSpace(*f.roleExternalID)
		enabled = true
	}
	if f.endpoint != nil && strings.TrimSpace(*f.endpoint) != "" {
		options["aws_endpoint"] = strings.TrimSpace(*f.endpoint)
		enabled = true
	}
	if !enabled {
		return nil
	}
	options["aws_rds_iam"] = "true"
	return options
}

func mergeOptionMaps(base map[string]string, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}

func parseSchemaTables(tables []string) ([]connector.Schema, error) {
	out := make([]connector.Schema, 0, len(tables))
	for _, table := range tables {
		parts := strings.SplitN(table, ".", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("table %q must be schema.table", table)
		}
		out = append(out, connector.Schema{
			Name:      parts[1],
			Namespace: parts[0],
		})
	}
	return out, nil
}

func runBackfill(ctx context.Context, flowPB *wallabypb.Flow, tables []string, workers int, partitionColumn string, partitionCount int, snapshotStateBackend string, snapshotStatePath string) error {
	model, err := flowFromProto(flowPB)
	if err != nil {
		return err
	}
	if model.Source.Options == nil {
		model.Source.Options = map[string]string{}
	}
	model.Source.Options["mode"] = "backfill"
	model.Source.Options["tables"] = strings.Join(tables, ",")
	if workers > 0 {
		model.Source.Options["snapshot_workers"] = fmt.Sprintf("%d", workers)
	}
	if partitionColumn != "" {
		model.Source.Options["partition_column"] = partitionColumn
	}
	if partitionCount > 0 {
		model.Source.Options["partition_count"] = fmt.Sprintf("%d", partitionCount)
	}
	if snapshotStateBackend != "" {
		model.Source.Options["snapshot_state_backend"] = snapshotStateBackend
	}
	if snapshotStatePath != "" {
		model.Source.Options["snapshot_state_path"] = snapshotStatePath
	}

	factory := runner.Factory{}
	source, err := factory.SourceForFlow(model)
	if err != nil {
		return err
	}
	destinations, err := factory.Destinations(model.Destinations)
	if err != nil {
		return err
	}

	runner := stream.Runner{
		Source:       source,
		SourceSpec:   model.Source,
		Destinations: destinations,
		FlowID:       model.ID,
		WireFormat:   model.WireFormat,
		Parallelism:  model.Parallelism,
	}
	if model.Config.AckPolicy != "" {
		runner.AckPolicy = model.Config.AckPolicy
	}
	if model.Config.PrimaryDestination != "" {
		runner.PrimaryDestination = model.Config.PrimaryDestination
	}
	if model.Config.FailureMode != "" {
		runner.FailureMode = model.Config.FailureMode
	}
	if model.Config.GiveUpPolicy != "" {
		runner.GiveUpPolicy = model.Config.GiveUpPolicy
	}
	return runner.Run(ctx)
}

func flowClient(endpoint string, insecureConn bool) (wallabypb.FlowServiceClient, func() error, error) {
	if endpoint == "" {
		return nil, nil, errors.New("endpoint is required")
	}
	if !insecureConn {
		return nil, nil, errors.New("secure grpc is not configured")
	}

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("connect: %w", err)
	}
	return wallabypb.NewFlowServiceClient(conn), conn.Close, nil
}

func flowFromProto(pb *wallabypb.Flow) (flow.Flow, error) {
	if pb == nil {
		return flow.Flow{}, errors.New("flow is required")
	}
	source, err := endpointFromProto(pb.Source)
	if err != nil {
		return flow.Flow{}, err
	}

	destinations := make([]connector.Spec, 0, len(pb.Destinations))
	for _, dest := range pb.Destinations {
		spec, err := endpointFromProto(dest)
		if err != nil {
			return flow.Flow{}, err
		}
		destinations = append(destinations, spec)
	}

	return flow.Flow{
		ID:           pb.Id,
		Name:         pb.Name,
		Source:       source,
		Destinations: destinations,
		WireFormat:   wireFormatFromProto(pb.WireFormat),
		Parallelism:  int(pb.Parallelism),
		Config:       flowConfigFromProto(pb.Config),
	}, nil
}

func flowConfigFromProto(cfg *wallabypb.FlowConfig) flow.Config {
	if cfg == nil {
		return flow.Config{}
	}
	return flow.Config{
		AckPolicy:          ackPolicyFromProto(cfg.AckPolicy),
		PrimaryDestination: cfg.PrimaryDestination,
		FailureMode:        failureModeFromProto(cfg.FailureMode),
		GiveUpPolicy:       giveUpPolicyFromProto(cfg.GiveUpPolicy),
	}
}

func ackPolicyFromProto(policy wallabypb.AckPolicy) stream.AckPolicy {
	switch policy {
	case wallabypb.AckPolicy_ACK_POLICY_ALL:
		return stream.AckPolicyAll
	case wallabypb.AckPolicy_ACK_POLICY_PRIMARY:
		return stream.AckPolicyPrimary
	default:
		return ""
	}
}

func failureModeFromProto(mode wallabypb.FailureMode) stream.FailureMode {
	switch mode {
	case wallabypb.FailureMode_FAILURE_MODE_HOLD_SLOT:
		return stream.FailureModeHoldSlot
	case wallabypb.FailureMode_FAILURE_MODE_DROP_SLOT:
		return stream.FailureModeDropSlot
	default:
		return ""
	}
}

func giveUpPolicyFromProto(policy wallabypb.GiveUpPolicy) stream.GiveUpPolicy {
	switch policy {
	case wallabypb.GiveUpPolicy_GIVE_UP_POLICY_NEVER:
		return stream.GiveUpPolicyNever
	case wallabypb.GiveUpPolicy_GIVE_UP_POLICY_ON_RETRY_EXHAUSTION:
		return stream.GiveUpPolicyOnRetryExhaustion
	default:
		return ""
	}
}

func endpointFromProto(endpoint *wallabypb.Endpoint) (connector.Spec, error) {
	if endpoint == nil {
		return connector.Spec{}, errors.New("endpoint is required")
	}
	if endpoint.Type == wallabypb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED {
		return connector.Spec{}, errors.New("endpoint type is required")
	}
	return connector.Spec{
		Name:    endpoint.Name,
		Type:    endpointTypeFromProto(endpoint.Type),
		Options: endpoint.Options,
	}, nil
}

func endpointTypeFromProto(t wallabypb.EndpointType) connector.EndpointType {
	switch t {
	case wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES:
		return connector.EndpointPostgres
	case wallabypb.EndpointType_ENDPOINT_TYPE_SNOWFLAKE:
		return connector.EndpointSnowflake
	case wallabypb.EndpointType_ENDPOINT_TYPE_S3:
		return connector.EndpointS3
	case wallabypb.EndpointType_ENDPOINT_TYPE_KAFKA:
		return connector.EndpointKafka
	case wallabypb.EndpointType_ENDPOINT_TYPE_HTTP:
		return connector.EndpointHTTP
	case wallabypb.EndpointType_ENDPOINT_TYPE_GRPC:
		return connector.EndpointGRPC
	case wallabypb.EndpointType_ENDPOINT_TYPE_PROTO:
		return connector.EndpointProto
	case wallabypb.EndpointType_ENDPOINT_TYPE_PGSTREAM:
		return connector.EndpointPGStream
	case wallabypb.EndpointType_ENDPOINT_TYPE_SNOWPIPE:
		return connector.EndpointSnowpipe
	case wallabypb.EndpointType_ENDPOINT_TYPE_PARQUET:
		return connector.EndpointParquet
	case wallabypb.EndpointType_ENDPOINT_TYPE_DUCKDB:
		return connector.EndpointDuckDB
	case wallabypb.EndpointType_ENDPOINT_TYPE_DUCKLAKE:
		return connector.EndpointDuckLake
	case wallabypb.EndpointType_ENDPOINT_TYPE_BUFSTREAM:
		return connector.EndpointBufStream
	case wallabypb.EndpointType_ENDPOINT_TYPE_CLICKHOUSE:
		return connector.EndpointClickHouse
	default:
		return ""
	}
}

func wireFormatFromProto(format wallabypb.WireFormat) connector.WireFormat {
	switch format {
	case wallabypb.WireFormat_WIRE_FORMAT_ARROW:
		return connector.WireFormatArrow
	case wallabypb.WireFormat_WIRE_FORMAT_PARQUET:
		return connector.WireFormatParquet
	case wallabypb.WireFormat_WIRE_FORMAT_PROTO:
		return connector.WireFormatProto
	case wallabypb.WireFormat_WIRE_FORMAT_AVRO:
		return connector.WireFormatAvro
	case wallabypb.WireFormat_WIRE_FORMAT_JSON:
		return connector.WireFormatJSON
	default:
		return ""
	}
}

func parseCSVValue(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trim := strings.TrimSpace(part)
		if trim != "" {
			out = append(out, trim)
		}
	}
	return out
}

type keyValueFlag struct {
	values map[string]string
}

func (k *keyValueFlag) String() string {
	if k == nil || len(k.values) == 0 {
		return ""
	}
	return fmt.Sprintf("%d entries", len(k.values))
}

func (k *keyValueFlag) Set(value string) error {
	parts := strings.SplitN(value, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("expected key=value, got %q", value)
	}
	if k.values == nil {
		k.values = map[string]string{}
	}
	key := strings.TrimSpace(parts[0])
	if key == "" {
		return fmt.Errorf("empty key in %q", value)
	}
	k.values[key] = strings.TrimSpace(parts[1])
	return nil
}

func (k *keyValueFlag) Type() string {
	return "key=value"
}

func copyStringMap(values map[string]string) map[string]string {
	if values == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(values))
	for k, v := range values {
		out[k] = v
	}
	return out
}

func mergeOptions(base map[string]string, overrides map[string]string) map[string]string {
	if base == nil {
		base = map[string]string{}
	}
	if len(overrides) == 0 {
		return base
	}
	out := copyStringMap(base)
	for key, value := range overrides {
		out[key] = value
	}
	return out
}

func selectDestination(destinations []*wallabypb.Endpoint, name string) (*wallabypb.Endpoint, error) {
	if len(destinations) == 0 {
		return nil, errors.New("flow has no destinations")
	}
	if name == "" {
		if len(destinations) == 1 {
			return destinations[0], nil
		}
		return nil, errors.New("destination name is required when flow has multiple destinations")
	}
	for _, dest := range destinations {
		if dest.GetName() == name {
			return dest, nil
		}
	}
	return nil, fmt.Errorf("destination %q not found", name)
}

type flowConfig struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	WireFormat   string            `json:"wire_format"`
	Parallelism  int32             `json:"parallelism"`
	Config       flowRuntimeConfig `json:"config,omitempty"`
	Source       endpointConfig    `json:"source"`
	Destinations []endpointConfig  `json:"destinations"`
}

type endpointConfig struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	Options map[string]string `json:"options"`
}

type flowRuntimeConfig struct {
	AckPolicy                       string `json:"ack_policy"`
	PrimaryDestination              string `json:"primary_destination"`
	FailureMode                     string `json:"failure_mode"`
	GiveUpPolicy                    string `json:"give_up_policy"`
	SchemaRegistrySubject           string `json:"schema_registry_subject,omitempty"`
	SchemaRegistryProtoTypesSubject string `json:"schema_registry_proto_types_subject,omitempty"`
	SchemaRegistrySubjectMode       string `json:"schema_registry_subject_mode,omitempty"`
}

func flowConfigToProto(cfg flowConfig) (*wallabypb.Flow, error) {
	source, err := endpointConfigToProto(cfg.Source)
	if err != nil {
		return nil, err
	}
	destinations := make([]*wallabypb.Endpoint, 0, len(cfg.Destinations))
	for _, dest := range cfg.Destinations {
		pb, err := endpointConfigToProto(dest)
		if err != nil {
			return nil, err
		}
		destinations = append(destinations, pb)
	}

	return &wallabypb.Flow{
		Id:           cfg.ID,
		Name:         cfg.Name,
		Source:       source,
		Destinations: destinations,
		WireFormat:   wireFormatToProto(cfg.WireFormat),
		Parallelism:  cfg.Parallelism,
		Config:       flowRuntimeConfigToProto(cfg.Config),
	}, nil
}

func flowRuntimeConfigToProto(cfg flowRuntimeConfig) *wallabypb.FlowConfig {
	if cfg == (flowRuntimeConfig{}) {
		return nil
	}
	return &wallabypb.FlowConfig{
		AckPolicy:                       ackPolicyStringToProto(cfg.AckPolicy),
		PrimaryDestination:              cfg.PrimaryDestination,
		FailureMode:                     failureModeStringToProto(cfg.FailureMode),
		GiveUpPolicy:                    giveUpPolicyStringToProto(cfg.GiveUpPolicy),
		SchemaRegistrySubject:           cfg.SchemaRegistrySubject,
		SchemaRegistryProtoTypesSubject: cfg.SchemaRegistryProtoTypesSubject,
		SchemaRegistrySubjectMode:       cfg.SchemaRegistrySubjectMode,
	}
}

func endpointConfigToProto(cfg endpointConfig) (*wallabypb.Endpoint, error) {
	endpointType := endpointTypeToProto(cfg.Type)
	if endpointType == wallabypb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED {
		return nil, fmt.Errorf("endpoint type %q is unsupported", cfg.Type)
	}
	return &wallabypb.Endpoint{
		Name:    cfg.Name,
		Type:    endpointType,
		Options: cfg.Options,
	}, nil
}

func endpointTypeToProto(value string) wallabypb.EndpointType {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "postgres":
		return wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES
	case "snowflake":
		return wallabypb.EndpointType_ENDPOINT_TYPE_SNOWFLAKE
	case "s3":
		return wallabypb.EndpointType_ENDPOINT_TYPE_S3
	case "kafka":
		return wallabypb.EndpointType_ENDPOINT_TYPE_KAFKA
	case "http":
		return wallabypb.EndpointType_ENDPOINT_TYPE_HTTP
	case "grpc":
		return wallabypb.EndpointType_ENDPOINT_TYPE_GRPC
	case "proto":
		return wallabypb.EndpointType_ENDPOINT_TYPE_PROTO
	case "pgstream":
		return wallabypb.EndpointType_ENDPOINT_TYPE_PGSTREAM
	case "snowpipe":
		return wallabypb.EndpointType_ENDPOINT_TYPE_SNOWPIPE
	case "parquet":
		return wallabypb.EndpointType_ENDPOINT_TYPE_PARQUET
	case "duckdb":
		return wallabypb.EndpointType_ENDPOINT_TYPE_DUCKDB
	case "ducklake":
		return wallabypb.EndpointType_ENDPOINT_TYPE_DUCKLAKE
	case "bufstream":
		return wallabypb.EndpointType_ENDPOINT_TYPE_BUFSTREAM
	case "clickhouse":
		return wallabypb.EndpointType_ENDPOINT_TYPE_CLICKHOUSE
	default:
		return wallabypb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED
	}
}

func ackPolicyStringToProto(value string) wallabypb.AckPolicy {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "all":
		return wallabypb.AckPolicy_ACK_POLICY_ALL
	case "primary":
		return wallabypb.AckPolicy_ACK_POLICY_PRIMARY
	default:
		return wallabypb.AckPolicy_ACK_POLICY_UNSPECIFIED
	}
}

func failureModeStringToProto(value string) wallabypb.FailureMode {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "hold_slot", "hold":
		return wallabypb.FailureMode_FAILURE_MODE_HOLD_SLOT
	case "drop_slot", "drop":
		return wallabypb.FailureMode_FAILURE_MODE_DROP_SLOT
	default:
		return wallabypb.FailureMode_FAILURE_MODE_UNSPECIFIED
	}
}

func giveUpPolicyStringToProto(value string) wallabypb.GiveUpPolicy {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "never":
		return wallabypb.GiveUpPolicy_GIVE_UP_POLICY_NEVER
	case "on_retry_exhaustion", "on_retry", "retry_exhaustion":
		return wallabypb.GiveUpPolicy_GIVE_UP_POLICY_ON_RETRY_EXHAUSTION
	default:
		return wallabypb.GiveUpPolicy_GIVE_UP_POLICY_UNSPECIFIED
	}
}

func wireFormatToProto(value string) wallabypb.WireFormat {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "arrow":
		return wallabypb.WireFormat_WIRE_FORMAT_ARROW
	case "parquet":
		return wallabypb.WireFormat_WIRE_FORMAT_PARQUET
	case "proto":
		return wallabypb.WireFormat_WIRE_FORMAT_PROTO
	case "avro":
		return wallabypb.WireFormat_WIRE_FORMAT_AVRO
	case "json":
		return wallabypb.WireFormat_WIRE_FORMAT_JSON
	default:
		return wallabypb.WireFormat_WIRE_FORMAT_UNSPECIFIED
	}
}

type streamPullOutput struct {
	Stream  string              `json:"stream"`
	Group   string              `json:"consumer_group"`
	Count   int                 `json:"count"`
	Records []streamPullMessage `json:"messages"`
}

type streamPullMessage struct {
	ID              int64  `json:"id"`
	Namespace       string `json:"namespace"`
	Table           string `json:"table"`
	LSN             string `json:"lsn"`
	WireFormat      string `json:"wire_format"`
	PayloadBase64   string `json:"payload_base64"`
	RegistrySubject string `json:"registry_subject,omitempty"`
	RegistryID      string `json:"registry_id,omitempty"`
	RegistryVersion int32  `json:"registry_version,omitempty"`
}

func streamPullMessages(messages []*wallabypb.StreamMessage) []streamPullMessage {
	out := make([]streamPullMessage, 0, len(messages))
	for _, msg := range messages {
		payload := base64.StdEncoding.EncodeToString(msg.Payload)
		out = append(out, streamPullMessage{
			ID:              msg.Id,
			Namespace:       msg.Namespace,
			Table:           msg.Table,
			LSN:             msg.Lsn,
			WireFormat:      msg.WireFormat,
			PayloadBase64:   payload,
			RegistrySubject: msg.RegistrySubject,
			RegistryID:      msg.RegistryId,
			RegistryVersion: msg.RegistryVersion,
		})
	}
	return out
}

type ddlListOutput struct {
	Status  string          `json:"status"`
	Count   int             `json:"count"`
	Records []ddlListRecord `json:"events"`
}

type ddlListRecord struct {
	ID        int64  `json:"id"`
	FlowID    string `json:"flow_id,omitempty"`
	Status    string `json:"status"`
	LSN       string `json:"lsn"`
	DDL       string `json:"ddl,omitempty"`
	PlanJSON  string `json:"plan_json,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}

func ddlListEvents(events []*wallabypb.DDLEvent) []ddlListRecord {
	out := make([]ddlListRecord, 0, len(events))
	for _, event := range events {
		record := ddlListRecord{
			ID:       event.Id,
			FlowID:   event.FlowId,
			Status:   event.Status,
			LSN:      event.Lsn,
			DDL:      event.Ddl,
			PlanJSON: event.PlanJson,
		}
		if event.CreatedAt != nil {
			record.CreatedAt = event.CreatedAt.AsTime().Format(time.RFC3339Nano)
		}
		out = append(out, record)
	}
	return out
}
