package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/connectors/sources/postgres"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/internal/runner"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	switch os.Args[1] {
	case "ddl":
		runDDL(os.Args[2:])
	case "stream":
		runStream(os.Args[2:])
	case "publication":
		runPublication(os.Args[2:])
	case "flow":
		runFlow(os.Args[2:])
	default:
		usage()
	}
}

func runDDL(args []string) {
	if len(args) < 1 {
		ddlUsage()
	}

	sub := args[0]
	switch sub {
	case "list":
		ddlList(args[1:])
	case "approve":
		ddlApprove(args[1:])
	case "reject":
		ddlReject(args[1:])
	case "apply":
		ddlApply(args[1:])
	default:
		ddlUsage()
	}
}

func ddlList(args []string) {
	fs := flag.NewFlagSet("ddl list", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	status := fs.String("status", "pending", "status filter: pending|approved|rejected|applied|all")
	flowID := fs.String("flow-id", "", "filter by flow id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	fs.Parse(args)

	client, closeConn := ddlClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var events []*wallabypb.DDLEvent
	if *status == "pending" {
		resp, err := client.ListPendingDDL(ctx, &wallabypb.ListPendingDDLRequest{FlowId: *flowID})
		if err != nil {
			log.Fatalf("list pending ddl: %v", err)
		}
		events = resp.Events
	} else {
		resp, err := client.ListDDL(ctx, &wallabypb.ListDDLRequest{Status: *status, FlowId: *flowID})
		if err != nil {
			log.Fatalf("list ddl: %v", err)
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
			log.Fatalf("encode json: %v", err)
		}
		return
	}

	if len(events) == 0 {
		fmt.Println("No DDL events found.")
		return
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
}

func ddlApprove(args []string) {
	fs := flag.NewFlagSet("ddl approve", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	id := fs.Int64("id", 0, "DDL event ID")
	fs.Parse(args)

	if *id == 0 {
		log.Fatal("-id is required")
	}

	client, closeConn := ddlClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ApproveDDL(ctx, &wallabypb.ApproveDDLRequest{Id: *id})
	if err != nil {
		log.Fatalf("approve ddl: %v", err)
	}
	fmt.Printf("Approved DDL %d (status=%s)\n", resp.Event.Id, resp.Event.Status)
}

func ddlReject(args []string) {
	fs := flag.NewFlagSet("ddl reject", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	id := fs.Int64("id", 0, "DDL event ID")
	fs.Parse(args)

	if *id == 0 {
		log.Fatal("-id is required")
	}

	client, closeConn := ddlClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.RejectDDL(ctx, &wallabypb.RejectDDLRequest{Id: *id})
	if err != nil {
		log.Fatalf("reject ddl: %v", err)
	}
	fmt.Printf("Rejected DDL %d (status=%s)\n", resp.Event.Id, resp.Event.Status)
}

func ddlApply(args []string) {
	fs := flag.NewFlagSet("ddl apply", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	id := fs.Int64("id", 0, "DDL event ID")
	fs.Parse(args)

	if *id == 0 {
		log.Fatal("-id is required")
	}

	client, closeConn := ddlClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.MarkDDLApplied(ctx, &wallabypb.MarkDDLAppliedRequest{Id: *id})
	if err != nil {
		log.Fatalf("mark ddl applied: %v", err)
	}
	fmt.Printf("Marked DDL %d applied (status=%s)\n", resp.Event.Id, resp.Event.Status)
}

func ddlClientOrExit(endpoint string, insecureConn bool) (wallabypb.DDLServiceClient, func()) {
	if endpoint == "" {
		log.Fatal("endpoint is required")
	}
	if !insecureConn {
		log.Fatal("secure grpc is not configured")
	}

	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	return wallabypb.NewDDLServiceClient(conn), func() { _ = conn.Close() }
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
	fmt.Println("  wallaby-admin stream <replay> [flags]")
	fmt.Println("  wallaby-admin publication <list|add|remove|sync|scrape> [flags]")
	fmt.Println("  wallaby-admin flow <create|run-once|resolve-staging> [flags]")
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

func runStream(args []string) {
	if len(args) < 1 {
		streamUsage()
	}
	switch args[0] {
	case "replay":
		streamReplay(args[1:])
	case "pull":
		streamPull(args[1:])
	case "ack":
		streamAck(args[1:])
	default:
		streamUsage()
	}
}

func streamReplay(args []string) {
	fs := flag.NewFlagSet("stream replay", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	stream := fs.String("stream", "", "stream name")
	consumerGroup := fs.String("group", "", "consumer group")
	fromLSN := fs.String("from-lsn", "", "replay events from this LSN (optional)")
	since := fs.String("since", "", "replay events since RFC3339 time (optional)")
	fs.Parse(args)

	if *stream == "" || *consumerGroup == "" {
		log.Fatal("-stream and -group are required")
	}

	client, closeConn := streamClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	var sinceTS *timestamppb.Timestamp
	if *since != "" {
		parsed, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			log.Fatalf("parse since: %v", err)
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
		log.Fatalf("replay stream: %v", err)
	}

	fmt.Printf("Reset %d deliveries for stream %s group %s\n", resp.Reset_, *stream, *consumerGroup)
}

func streamUsage() {
	fmt.Println("Stream subcommands:")
	fmt.Println("  wallaby-admin stream replay -stream <name> -group <name> [-from-lsn <lsn>] [-since <rfc3339>]")
	fmt.Println("  wallaby-admin stream pull -stream <name> -group <name> [-max 10] [-visibility 30] [-consumer <id>] [--json|--pretty]")
	fmt.Println("  wallaby-admin stream ack -stream <name> -group <name> -ids 1,2,3")
	os.Exit(1)
}

func runFlow(args []string) {
	if len(args) < 1 {
		flowUsage()
	}
	switch args[0] {
	case "create":
		flowCreate(args[1:])
	case "start":
		flowStart(args[1:])
	case "run-once":
		flowRunOnce(args[1:])
	case "stop":
		flowStop(args[1:])
	case "resume":
		flowResume(args[1:])
	case "resolve-staging":
		flowResolveStaging(args[1:])
	default:
		flowUsage()
	}
}

func flowUsage() {
	fmt.Println("Flow subcommands:")
	fmt.Println("  wallaby-admin flow create -file <path> [-start] [--json|--pretty]")
	fmt.Println("  wallaby-admin flow start -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow run-once -flow-id <id>")
	fmt.Println("  wallaby-admin flow stop -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow resume -flow-id <id> [--json|--pretty]")
	fmt.Println("  wallaby-admin flow resolve-staging -flow-id <id> [-tables schema.table,...] [-schemas public,...] [-dest <name>]")
	os.Exit(1)
}

func flowCreate(args []string) {
	fs := flag.NewFlagSet("flow create", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	path := fs.String("file", "", "flow config JSON file")
	start := fs.Bool("start", false, "start flow immediately")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	fs.Parse(args)

	if *path == "" {
		log.Fatal("-file is required")
	}

	payload, err := os.ReadFile(*path)
	if err != nil {
		log.Fatalf("read flow file: %v", err)
	}

	var cfg flowConfig
	if err := json.Unmarshal(payload, &cfg); err != nil {
		log.Fatalf("parse flow json: %v", err)
	}

	pbFlow, err := flowConfigToProto(cfg)
	if err != nil {
		log.Fatalf("flow config: %v", err)
	}

	client, closeConn := flowClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.CreateFlow(ctx, &wallabypb.CreateFlowRequest{
		Flow:             pbFlow,
		StartImmediately: *start,
	})
	if err != nil {
		log.Fatalf("create flow: %v", err)
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
			log.Fatalf("encode json: %v", err)
		}
		return
	}

	fmt.Printf("Created flow %s (state=%s)\n", resp.Id, resp.State.String())
}

func flowRunOnce(args []string) {
	fs := flag.NewFlagSet("flow run-once", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	fs.Parse(args)

	if *flowID == "" {
		log.Fatal("-flow-id is required")
	}

	client, closeConn := flowClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.RunFlowOnce(ctx, &wallabypb.RunFlowOnceRequest{FlowId: *flowID})
	if err != nil {
		log.Fatalf("run flow once: %v", err)
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
			log.Fatalf("encode json: %v", err)
		}
		return
	}

	fmt.Printf("Dispatched flow %s\n", *flowID)
}

func flowStart(args []string) {
	fs := flag.NewFlagSet("flow start", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	fs.Parse(args)

	if *flowID == "" {
		log.Fatal("-flow-id is required")
	}

	client, closeConn := flowClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.StartFlow(ctx, &wallabypb.StartFlowRequest{FlowId: *flowID})
	if err != nil {
		log.Fatalf("start flow: %v", err)
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
			log.Fatalf("encode json: %v", err)
		}
		return
	}

	fmt.Printf("Started flow %s (state=%s)\n", resp.Id, resp.State.String())
}

func flowStop(args []string) {
	fs := flag.NewFlagSet("flow stop", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	fs.Parse(args)

	if *flowID == "" {
		log.Fatal("-flow-id is required")
	}

	client, closeConn := flowClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.StopFlow(ctx, &wallabypb.StopFlowRequest{FlowId: *flowID})
	if err != nil {
		log.Fatalf("stop flow: %v", err)
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
			log.Fatalf("encode json: %v", err)
		}
		return
	}

	fmt.Printf("Stopped flow %s (state=%s)\n", resp.Id, resp.State.String())
}

func flowResume(args []string) {
	fs := flag.NewFlagSet("flow resume", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	fs.Parse(args)

	if *flowID == "" {
		log.Fatal("-flow-id is required")
	}

	client, closeConn := flowClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.ResumeFlow(ctx, &wallabypb.ResumeFlowRequest{FlowId: *flowID})
	if err != nil {
		log.Fatalf("resume flow: %v", err)
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
			log.Fatalf("encode json: %v", err)
		}
		return
	}

	fmt.Printf("Resumed flow %s (state=%s)\n", resp.Id, resp.State.String())
}

func streamClientOrExit(endpoint string, insecureConn bool) (wallabypb.StreamServiceClient, func()) {
	if endpoint == "" {
		log.Fatal("endpoint is required")
	}
	if !insecureConn {
		log.Fatal("secure grpc is not configured")
	}

	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	return wallabypb.NewStreamServiceClient(conn), func() { _ = conn.Close() }
}

func streamPull(args []string) {
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
	fs.Parse(args)

	if *stream == "" || *consumerGroup == "" {
		log.Fatal("-stream and -group are required")
	}

	client, closeConn := streamClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Pull(ctx, &wallabypb.StreamPullRequest{
		Stream:                   *stream,
		ConsumerGroup:            *consumerGroup,
		MaxMessages:              int32(*max),
		VisibilityTimeoutSeconds: int32(*visibility),
		ConsumerId:               *consumerID,
	})
	if err != nil {
		log.Fatalf("pull stream: %v", err)
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
			log.Fatalf("encode json: %v", err)
		}
		return
	}

	if len(resp.Messages) == 0 {
		fmt.Println("No messages available.")
		return
	}

	fmt.Printf("Pulled %d messages:\n", len(resp.Messages))
	for _, msg := range resp.Messages {
		fmt.Printf("- id=%d table=%s lsn=%s bytes=%d\n", msg.Id, msg.Table, msg.Lsn, len(msg.Payload))
	}
}

func streamAck(args []string) {
	fs := flag.NewFlagSet("stream ack", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	stream := fs.String("stream", "", "stream name")
	consumerGroup := fs.String("group", "", "consumer group")
	ids := fs.String("ids", "", "comma-separated message ids")
	fs.Parse(args)

	if *stream == "" || *consumerGroup == "" {
		log.Fatal("-stream and -group are required")
	}
	if *ids == "" {
		log.Fatal("-ids is required")
	}

	parsed, err := parseIDs(*ids)
	if err != nil {
		log.Fatalf("parse ids: %v", err)
	}

	client, closeConn := streamClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Ack(ctx, &wallabypb.StreamAckRequest{
		Stream:        *stream,
		ConsumerGroup: *consumerGroup,
		Ids:           parsed,
	})
	if err != nil {
		log.Fatalf("ack stream: %v", err)
	}

	fmt.Printf("Acked %d messages\n", resp.Acked)
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

func runPublication(args []string) {
	if len(args) < 1 {
		publicationUsage()
	}

	switch args[0] {
	case "list":
		publicationList(args[1:])
	case "add":
		publicationAdd(args[1:])
	case "remove":
		publicationRemove(args[1:])
	case "sync":
		publicationSync(args[1:])
	case "scrape":
		publicationScrape(args[1:])
	default:
		publicationUsage()
	}
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

func publicationList(args []string) {
	fs := flag.NewFlagSet("publication list", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	dsn := fs.String("dsn", "", "postgres source dsn")
	publication := fs.String("publication", "", "publication name")
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	awsFlags := addAWSIAMFlags(fs)
	fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		log.Fatal(err)
	}

	tables, err := postgres.ListPublicationTables(ctx, cfg.dsn, cfg.publication, cfg.options)
	if err != nil {
		log.Fatalf("list publication tables: %v", err)
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
			log.Fatalf("encode json: %v", err)
		}
		return
	}

	if len(tables) == 0 {
		fmt.Println("No tables in publication.")
		return
	}
	fmt.Printf("Publication %s tables:\n", cfg.publication)
	for _, table := range tables {
		fmt.Printf("- %s\n", table)
	}
}

func publicationAdd(args []string) {
	fs := flag.NewFlagSet("publication add", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	dsn := fs.String("dsn", "", "postgres source dsn")
	publication := fs.String("publication", "", "publication name")
	tables := fs.String("tables", "", "comma-separated schema.table list")
	awsFlags := addAWSIAMFlags(fs)
	fs.Parse(args)

	if *tables == "" {
		log.Fatal("-tables is required")
	}
	tableList := parseCSVValue(*tables)
	if len(tableList) == 0 {
		log.Fatal("no tables provided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		log.Fatal(err)
	}

	if err := postgres.AddPublicationTables(ctx, cfg.dsn, cfg.publication, tableList, cfg.options); err != nil {
		log.Fatalf("add publication tables: %v", err)
	}
	fmt.Printf("Added %d tables to publication %s\n", len(tableList), cfg.publication)
}

func publicationRemove(args []string) {
	fs := flag.NewFlagSet("publication remove", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	dsn := fs.String("dsn", "", "postgres source dsn")
	publication := fs.String("publication", "", "publication name")
	tables := fs.String("tables", "", "comma-separated schema.table list")
	awsFlags := addAWSIAMFlags(fs)
	fs.Parse(args)

	if *tables == "" {
		log.Fatal("-tables is required")
	}
	tableList := parseCSVValue(*tables)
	if len(tableList) == 0 {
		log.Fatal("no tables provided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		log.Fatal(err)
	}

	if err := postgres.DropPublicationTables(ctx, cfg.dsn, cfg.publication, tableList, cfg.options); err != nil {
		log.Fatalf("drop publication tables: %v", err)
	}
	fmt.Printf("Removed %d tables from publication %s\n", len(tableList), cfg.publication)
}

func publicationSync(args []string) {
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
	fs.Parse(args)

	if *flowID == "" {
		log.Fatal("-flow-id is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		log.Fatal(err)
	}
	if cfg.flow == nil {
		log.Fatal("flow is required for publication sync")
	}

	desired, err := resolveDesiredTables(ctx, cfg, cfg.options, *tables, *schemas)
	if err != nil {
		log.Fatal(err)
	}
	if len(desired) == 0 {
		log.Fatal("no tables provided for sync")
	}

	flowClient, closeConn := flowClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	if *pause {
		if _, err := flowClient.StopFlow(ctx, &wallabypb.StopFlowRequest{FlowId: cfg.flow.Id}); err != nil {
			log.Printf("pause flow: %v", err)
		}
	}

	added, removed, err := postgres.SyncPublicationTables(ctx, cfg.dsn, cfg.publication, desired, *mode, cfg.options)
	if err != nil {
		log.Fatalf("sync publication: %v", err)
	}

	if *snapshot && len(added) > 0 {
		if err := runBackfill(context.Background(), cfg.flow, added, *snapshotWorkers, *partitionColumn, *partitionCount, *snapshotStateBackend, *snapshotStatePath); err != nil {
			log.Fatalf("backfill new tables: %v", err)
		}
	}

	if *resume {
		if _, err := flowClient.ResumeFlow(ctx, &wallabypb.ResumeFlowRequest{FlowId: cfg.flow.Id}); err != nil {
			log.Printf("resume flow: %v", err)
		}
	}

	fmt.Printf("Synced publication %s (added=%d removed=%d)\n", cfg.publication, len(added), len(removed))
}

func publicationScrape(args []string) {
	fs := flag.NewFlagSet("publication scrape", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id (optional if -dsn and -publication provided)")
	dsn := fs.String("dsn", "", "postgres source dsn")
	publication := fs.String("publication", "", "publication name")
	schemas := fs.String("schemas", "", "comma-separated schemas")
	apply := fs.Bool("apply", false, "add newly discovered tables to publication")
	awsFlags := addAWSIAMFlags(fs)
	fs.Parse(args)

	if *schemas == "" {
		log.Fatal("-schemas is required")
	}
	schemaList := parseCSVValue(*schemas)
	if len(schemaList) == 0 {
		log.Fatal("no schemas provided")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication, awsFlags.options())
	if err != nil {
		log.Fatal(err)
	}

	allTables, err := postgres.ScrapeTables(ctx, cfg.dsn, schemaList, cfg.options)
	if err != nil {
		log.Fatalf("scrape tables: %v", err)
	}
	current, err := postgres.ListPublicationTables(ctx, cfg.dsn, cfg.publication, cfg.options)
	if err != nil {
		log.Fatalf("list publication tables: %v", err)
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
			log.Fatalf("add publication tables: %v", err)
		}
		fmt.Printf("Added %d tables to publication %s\n", len(missing), cfg.publication)
		return
	}

	if len(missing) == 0 {
		fmt.Println("No new tables found.")
		return
	}
	fmt.Printf("New tables not in publication %s:\n", cfg.publication)
	for _, table := range missing {
		fmt.Printf("- %s\n", table)
	}
}

func flowResolveStaging(args []string) {
	fs := flag.NewFlagSet("flow resolve-staging", flag.ExitOnError)
	endpoint := fs.String("endpoint", "localhost:8080", "gRPC endpoint host:port")
	insecureConn := fs.Bool("insecure", true, "use insecure gRPC")
	flowID := fs.String("flow-id", "", "flow id")
	tables := fs.String("tables", "", "comma-separated tables (schema.table)")
	schemas := fs.String("schemas", "", "comma-separated schemas to resolve")
	destName := fs.String("dest", "", "destination name (optional)")
	fs.Parse(args)

	if *flowID == "" {
		log.Fatal("-flow-id is required")
	}

	ctx := context.Background()
	client, closeConn := flowClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	flowResp, err := client.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: *flowID})
	if err != nil {
		log.Fatalf("load flow: %v", err)
	}
	model, err := flowFromProto(flowResp)
	if err != nil {
		log.Fatalf("parse flow: %v", err)
	}

	tableList, err := resolveFlowTables(ctx, model, *tables, *schemas)
	if err != nil {
		log.Fatalf("resolve tables: %v", err)
	}
	if len(tableList) == 0 {
		log.Fatal("no tables resolved")
	}
	schemaList, err := parseSchemaTables(tableList)
	if err != nil {
		log.Fatalf("parse tables: %v", err)
	}

	factory := runner.Factory{}
	destinations, err := factory.Destinations(model.Destinations)
	if err != nil {
		log.Fatalf("build destinations: %v", err)
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
			log.Fatalf("open destination %s: %v", dest.Spec.Name, err)
		}

		if resolver, ok := dest.Dest.(stream.StagingResolverFor); ok {
			if err := resolver.ResolveStagingFor(ctx, schemaList); err != nil {
				_ = dest.Dest.Close(ctx)
				log.Fatalf("resolve staging for %s: %v", dest.Spec.Name, err)
			}
			resolved++
		} else if resolver, ok := dest.Dest.(stream.StagingResolver); ok {
			if err := resolver.ResolveStaging(ctx); err != nil {
				_ = dest.Dest.Close(ctx)
				log.Fatalf("resolve staging for %s: %v", dest.Spec.Name, err)
			}
			resolved++
		}

		if err := dest.Dest.Close(ctx); err != nil {
			log.Fatalf("close destination %s: %v", dest.Spec.Name, err)
		}
	}

	fmt.Printf("Resolved staging for %d destinations\n", resolved)
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

	client, closeConn := flowClientOrExit(endpoint, insecureConn)
	defer closeConn()

	flowResp, err := client.GetFlow(ctx, &wallabypb.GetFlowRequest{FlowId: flowID})
	if err != nil {
		return cfg, err
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

func flowClientOrExit(endpoint string, insecureConn bool) (wallabypb.FlowServiceClient, func()) {
	if endpoint == "" {
		log.Fatal("endpoint is required")
	}
	if !insecureConn {
		log.Fatal("secure grpc is not configured")
	}

	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	return wallabypb.NewFlowServiceClient(conn), func() { _ = conn.Close() }
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
	AckPolicy          string `json:"ack_policy"`
	PrimaryDestination string `json:"primary_destination"`
	FailureMode        string `json:"failure_mode"`
	GiveUpPolicy       string `json:"give_up_policy"`
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
		AckPolicy:          ackPolicyStringToProto(cfg.AckPolicy),
		PrimaryDestination: cfg.PrimaryDestination,
		FailureMode:        failureModeStringToProto(cfg.FailureMode),
		GiveUpPolicy:       giveUpPolicyStringToProto(cfg.GiveUpPolicy),
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
	ID            int64  `json:"id"`
	Namespace     string `json:"namespace"`
	Table         string `json:"table"`
	LSN           string `json:"lsn"`
	WireFormat    string `json:"wire_format"`
	PayloadBase64 string `json:"payload_base64"`
}

func streamPullMessages(messages []*wallabypb.StreamMessage) []streamPullMessage {
	out := make([]streamPullMessage, 0, len(messages))
	for _, msg := range messages {
		payload := base64.StdEncoding.EncodeToString(msg.Payload)
		out = append(out, streamPullMessage{
			ID:            msg.Id,
			Namespace:     msg.Namespace,
			Table:         msg.Table,
			LSN:           msg.Lsn,
			WireFormat:    msg.WireFormat,
			PayloadBase64: payload,
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
