package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/connectors/sources/postgres"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/pkg/certify"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	if err := run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	if len(args) < 2 {
		usage()
		return nil
	}

	switch args[1] {
	case "ddl":
		return runDDL(args[2:])
	case "stream":
		return runStream(args[2:])
	case "publication":
		return runPublication(args[2:])
	case "flow":
		return runFlow(args[2:])
	case "certify":
		return runCertify(args[2:])
	default:
		usage()
		return nil
	}
}

func runDDL(args []string) error {
	if len(args) < 1 {
		ddlUsage()
	}

	sub := args[0]
	switch sub {
	case "list":
		return ddlList(args[1:])
	case "approve":
		return ddlApprove(args[1:])
	case "reject":
		return ddlReject(args[1:])
	case "apply":
		return ddlApply(args[1:])
	default:
		ddlUsage()
	}
	return nil
}

func ddlList(args []string) error {
	fs := flag.NewFlagSet("ddl list", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	status := fs.String("status", "pending", "status filter: pending|approved|rejected|applied|all")
	flowID := fs.String("flow-id", "", "filter by flow id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
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

	fmt.Printf("%-6s %-10s %-18s %-12s %-s\n", "ID", "STATUS", "LSN", "FLOW", "DDL/PLAN")
	for _, event := range events {
		flagged := statusFlag(event.Status)
		line := fmt.Sprintf("%-6d %-10s %-18s %-12s %-s", event.Id, event.Status, event.Lsn, event.FlowId, ddlSummary(event))
		if flagged != "" {
			line = fmt.Sprintf("%s [%s]", line, flagged)
		}
		fmt.Println(line)
	}
	return nil
}

func ddlApprove(args []string) error {
	fs := flag.NewFlagSet("ddl approve", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	id := fs.Int64("id", 0, "DDL event ID")
	if err := fs.Parse(args); err != nil {
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

func ddlReject(args []string) error {
	fs := flag.NewFlagSet("ddl reject", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	id := fs.Int64("id", 0, "DDL event ID")
	if err := fs.Parse(args); err != nil {
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

func ddlApply(args []string) error {
	fs := flag.NewFlagSet("ddl apply", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	id := fs.Int64("id", 0, "DDL event ID")
	if err := fs.Parse(args); err != nil {
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
	fmt.Println("  wallaby-admin ddl <list|approve|reject|apply> [flags]")
	fmt.Println("  wallaby-admin stream <replay|pull|ack> [flags]")
	fmt.Println("  wallaby-admin publication <list|add|remove|sync|scrape> [flags]")
	fmt.Println("  wallaby-admin flow <create|update|reconfigure|start|run-once|stop|resume|cleanup|resolve-staging> [flags]")
	fmt.Println("  wallaby-admin certify [flags]")
	os.Exit(1)
}

func ddlUsage() {
	fmt.Println("DDL subcommands:")
	fmt.Println("  wallaby-admin ddl list -status pending|approved|rejected|applied|all [-flow-id <flow_id>]")
	fmt.Println("  wallaby-admin ddl approve -id <event_id>")
	fmt.Println("  wallaby-admin ddl reject -id <event_id>")
	fmt.Println("  wallaby-admin ddl apply -id <event_id>")
	os.Exit(1)
}

func runStream(args []string) error {
	if len(args) < 1 {
		streamUsage()
	}
	switch args[0] {
	case "replay":
		return streamReplay(args[1:])
	case "pull":
		return streamPull(args[1:])
	case "ack":
		return streamAck(args[1:])
	default:
		streamUsage()
	}
	return nil
}

func streamReplay(args []string) error {
	fs := flag.NewFlagSet("stream replay", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	stream := fs.String("stream", "", "stream name")
	consumerGroup := fs.String("group", "", "consumer group")
	fromLSN := fs.String("from-lsn", "", "replay events from this LSN (optional)")
	since := fs.String("since", "", "replay events since RFC3339 time (optional)")
	if err := fs.Parse(args); err != nil {
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

func runFlow(args []string) error {
	if len(args) < 1 {
		flowUsage()
	}
	switch args[0] {
	case "create":
		return flowCreate(args[1:])
	case "update":
		return flowUpdate(args[1:])
	case "reconfigure":
		return flowReconfigure(args[1:])
	case "start":
		return flowStart(args[1:])
	case "run-once":
		return flowRunOnce(args[1:])
	case "stop":
		return flowStop(args[1:])
	case "resume":
		return flowResume(args[1:])
	case "cleanup":
		return flowCleanup(args[1:])
	case "resolve-staging":
		return flowResolveStaging(args[1:])
	default:
		flowUsage()
	}
	return nil
}

func runCertify(args []string) error {
	fs := flag.NewFlagSet("certify", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id to load source/destination DSNs")
	destName := fs.String("destination", "", "destination name (required when flow has multiple destinations)")
	sourceDSN := fs.String("source-dsn", "", "source Postgres DSN (overrides flow)")
	destDSN := fs.String("dest-dsn", "", "destination Postgres DSN (overrides flow)")
	table := fs.String("table", "", "table name (schema.table)")
	tables := fs.String("tables", "", "comma-separated table list (schema.table)")
	primaryKeys := fs.String("primary-keys", "", "comma-separated primary key columns (optional)")
	columns := fs.String("columns", "", "comma-separated columns to hash (optional)")
	sampleRate := fs.Float64("sample-rate", 1, "sample rate 0..1 (uses deterministic PK hash)")
	sampleLimit := fs.Int("sample-limit", 0, "max rows to hash per table")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")

	var sourceOpts keyValueFlag
	var destOpts keyValueFlag
	fs.Var(&sourceOpts, "source-opt", "source option key=value (repeatable)")
	fs.Var(&destOpts, "dest-opt", "destination option key=value (repeatable)")

	if err := fs.Parse(args); err != nil {
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
	fmt.Println("  wallaby-admin flow stop -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow resume -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow cleanup -flow-id <id> [-drop-slot] [-drop-publication] [-drop-source-state] [--json|--pretty]")
	fmt.Println("  wallaby-admin flow resolve-staging -flow-id <id> [-tables schema.table,...] [-schemas public,...] [-dest <name>]")
	os.Exit(1)
}

func flowCreate(args []string) error {
	fs := flag.NewFlagSet("flow create", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	path := fs.String("file", "", "flow config JSON file")
	start := fs.Bool("start", false, "start flow immediately")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *path == "" {
		return errors.New("-file is required")
	}

	payload, err := os.ReadFile(*path)
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

func flowUpdate(args []string) error {
	fs := flag.NewFlagSet("flow update", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	path := fs.String("file", "", "flow config JSON file")
	flowID := fs.String("flow-id", "", "flow id override")
	pause := fs.Bool("pause", false, "pause flow before update")
	resume := fs.Bool("resume", false, "resume flow after update")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *path == "" {
		return errors.New("-file is required")
	}

	payload, err := os.ReadFile(*path)
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

func flowReconfigure(args []string) error {
	fs := flag.NewFlagSet("flow reconfigure", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	path := fs.String("file", "", "flow config JSON file")
	flowID := fs.String("flow-id", "", "flow id override")
	pause := fs.Bool("pause", true, "pause flow before reconfigure")
	resume := fs.Bool("resume", true, "resume flow after reconfigure")
	syncPublication := fs.Bool("sync-publication", false, "sync publication tables after update")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *path == "" {
		return errors.New("-file is required")
	}

	payload, err := os.ReadFile(*path)
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

func flowRunOnce(args []string) error {
	fs := flag.NewFlagSet("flow run-once", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
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

func flowStart(args []string) error {
	fs := flag.NewFlagSet("flow start", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
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

func flowStop(args []string) error {
	fs := flag.NewFlagSet("flow stop", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
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

func flowResume(args []string) error {
	fs := flag.NewFlagSet("flow resume", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
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

func flowCleanup(args []string) error {
	fs := flag.NewFlagSet("flow cleanup", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	dropSlot := fs.Bool("drop-slot", true, "drop replication slot")
	dropPublication := fs.Bool("drop-publication", false, "drop publication")
	dropState := fs.Bool("drop-source-state", true, "delete source state row")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
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

func streamPull(args []string) error {
	fs := flag.NewFlagSet("stream pull", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	stream := fs.String("stream", "", "stream name")
	consumerGroup := fs.String("group", "", "consumer group")
	max := fs.Int("max", 10, "max messages to pull")
	visibility := fs.Int("visibility", 30, "visibility timeout seconds")
	consumerID := fs.String("consumer", "", "consumer id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	if err := fs.Parse(args); err != nil {
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

func streamAck(args []string) error {
	fs := flag.NewFlagSet("stream ack", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	stream := fs.String("stream", "", "stream name")
	consumerGroup := fs.String("group", "", "consumer group")
	ids := fs.String("ids", "", "comma-separated message ids")
	if err := fs.Parse(args); err != nil {
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

func runPublication(args []string) error {
	if len(args) < 1 {
		publicationUsage()
	}

	switch args[0] {
	case "list":
		return publicationList(args[1:])
	case "add":
		return publicationAdd(args[1:])
	case "remove":
		return publicationRemove(args[1:])
	case "sync":
		return publicationSync(args[1:])
	case "scrape":
		return publicationScrape(args[1:])
	default:
		publicationUsage()
	}
	return nil
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

func publicationList(args []string) error {
	fs := flag.NewFlagSet("publication list", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	dsn := fs.String("dsn", "", "postgres source dsn")
	publication := fs.String("publication", "", "publication name")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	awsFlags := addAWSIAMFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		return err
	}

	tables, err := postgres.ListPublicationTables(ctx, cfg.dsn, cfg.publication, cfg.options)
	if err != nil {
		return fmt.Errorf("list publication tables: %w", err)
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

func publicationAdd(args []string) error {
	fs := flag.NewFlagSet("publication add", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	dsn := fs.String("dsn", "", "postgres source dsn")
	publication := fs.String("publication", "", "publication name")
	tables := fs.String("tables", "", "comma-separated schema.table list")
	awsFlags := addAWSIAMFlags(fs)
	if err := fs.Parse(args); err != nil {
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

	if err := postgres.AddPublicationTables(ctx, cfg.dsn, cfg.publication, tableList, cfg.options); err != nil {
		return fmt.Errorf("add publication tables: %w", err)
	}
	fmt.Printf("Added %d tables to publication %s\n", len(tableList), cfg.publication)
	return nil
}

func publicationRemove(args []string) error {
	fs := flag.NewFlagSet("publication remove", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	dsn := fs.String("dsn", "", "postgres source dsn")
	publication := fs.String("publication", "", "publication name")
	tables := fs.String("tables", "", "comma-separated schema.table list")
	awsFlags := addAWSIAMFlags(fs)
	if err := fs.Parse(args); err != nil {
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

	if err := postgres.DropPublicationTables(ctx, cfg.dsn, cfg.publication, tableList, cfg.options); err != nil {
		return fmt.Errorf("drop publication tables: %w", err)
	}
	fmt.Printf("Removed %d tables from publication %s\n", len(tableList), cfg.publication)
	return nil
}

func publicationSync(args []string) error {
	fs := flag.NewFlagSet("publication sync", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	dsn := fs.String("dsn", "", "postgres source dsn override")
	publication := fs.String("publication", "", "publication name override")
	tables := fs.String("tables", "", "comma-separated schema.table list")
	schemas := fs.String("schemas", "", "comma-separated schemas for auto-discovery")
	mode := fs.String("mode", "add", "sync mode: add or sync")
	pause := fs.Bool("pause", true, "pause flow before sync")
	resume := fs.Bool("resume", true, "resume flow after sync")
	snapshot := fs.Bool("snapshot", false, "run backfill for newly added tables")
	snapshotWorkers := fs.Int("snapshot-workers", 0, "parallel workers for backfill snapshots")
	partitionColumn := fs.String("partition-column", "", "partition column for backfill hashing")
	partitionCount := fs.Int("partition-count", 0, "partition count per table for backfill hashing")
	snapshotStateBackend := fs.String("snapshot-state-backend", "", "snapshot state backend (postgres|file|none)")
	snapshotStatePath := fs.String("snapshot-state-path", "", "snapshot state path (file backend)")
	awsFlags := addAWSIAMFlags(fs)
	if err := fs.Parse(args); err != nil {
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

	added, removed, err := postgres.SyncPublicationTables(ctx, cfg.dsn, cfg.publication, desired, *mode, cfg.options)
	if err != nil {
		return fmt.Errorf("sync publication: %w", err)
	}

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

func publicationScrape(args []string) error {
	fs := flag.NewFlagSet("publication scrape", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	dsn := fs.String("dsn", "", "postgres source dsn")
	publication := fs.String("publication", "", "publication name")
	schemas := fs.String("schemas", "", "comma-separated schemas")
	apply := fs.Bool("apply", false, "add newly discovered tables to publication")
	awsFlags := addAWSIAMFlags(fs)
	if err := fs.Parse(args); err != nil {
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

	allTables, err := postgres.ScrapeTables(ctx, cfg.dsn, schemaList, cfg.options)
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
	var missing []string
	for _, table := range allTables {
		if _, ok := currentSet[strings.ToLower(table)]; !ok {
			missing = append(missing, table)
		}
	}

	if *apply && len(missing) > 0 {
		if err := postgres.AddPublicationTables(ctx, cfg.dsn, cfg.publication, missing, cfg.options); err != nil {
			return fmt.Errorf("add publication tables: %w", err)
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

func flowResolveStaging(args []string) error {
	fs := flag.NewFlagSet("flow resolve-staging", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	tables := fs.String("tables", "", "comma-separated tables (schema.table)")
	schemas := fs.String("schemas", "", "comma-separated schemas to resolve")
	destName := fs.String("dest", "", "destination name (optional)")
	if err := fs.Parse(args); err != nil {
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

type publicationConfig struct {
	dsn         string
	publication string
	flow        *wallabypb.Flow
	options     map[string]string
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

func addAWSIAMFlags(fs *flag.FlagSet) *awsIAMFlags {
	return &awsIAMFlags{
		enabled:         fs.Bool("aws-rds-iam", false, "use AWS RDS IAM authentication"),
		region:          fs.String("aws-region", "", "AWS region for RDS IAM authentication"),
		profile:         fs.String("aws-profile", "", "AWS profile for RDS IAM authentication"),
		roleARN:         fs.String("aws-role-arn", "", "AWS role ARN to assume for RDS IAM"),
		roleSessionName: fs.String("aws-role-session-name", "", "AWS role session name for RDS IAM"),
		roleExternalID:  fs.String("aws-role-external-id", "", "AWS role external id for RDS IAM"),
		endpoint:        fs.String("aws-endpoint", "", "AWS endpoint override for RDS IAM"),
	}
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
