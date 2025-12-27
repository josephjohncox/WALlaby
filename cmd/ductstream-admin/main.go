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

	"github.com/josephjohncox/ductstream/connectors/sources/postgres"
	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
	"github.com/josephjohncox/ductstream/internal/flow"
	"github.com/josephjohncox/ductstream/internal/runner"
	"github.com/josephjohncox/ductstream/pkg/connector"
	"github.com/josephjohncox/ductstream/pkg/stream"
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
	jsonOutput := fs.Bool("json", false, "output JSON for scripting")
	prettyOutput := fs.Bool("pretty", false, "pretty-print JSON output")
	fs.Parse(args)

	client, closeConn := ddlClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var events []*ductstreampb.DDLEvent
	if *status == "pending" {
		resp, err := client.ListPendingDDL(ctx, &ductstreampb.ListPendingDDLRequest{})
		if err != nil {
			log.Fatalf("list pending ddl: %v", err)
		}
		events = resp.Events
	} else {
		resp, err := client.ListDDL(ctx, &ductstreampb.ListDDLRequest{Status: *status})
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

	fmt.Printf("%-6s %-10s %-18s %-s\n", "ID", "STATUS", "LSN", "DDL/PLAN")
	for _, event := range events {
		flagged := statusFlag(event.Status)
		line := fmt.Sprintf("%-6d %-10s %-18s %-s", event.Id, event.Status, event.Lsn, ddlSummary(event))
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

	resp, err := client.ApproveDDL(ctx, &ductstreampb.ApproveDDLRequest{Id: *id})
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

	resp, err := client.RejectDDL(ctx, &ductstreampb.RejectDDLRequest{Id: *id})
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

	resp, err := client.MarkDDLApplied(ctx, &ductstreampb.MarkDDLAppliedRequest{Id: *id})
	if err != nil {
		log.Fatalf("mark ddl applied: %v", err)
	}
	fmt.Printf("Marked DDL %d applied (status=%s)\n", resp.Event.Id, resp.Event.Status)
}

func ddlClientOrExit(endpoint string, insecureConn bool) (ductstreampb.DDLServiceClient, func()) {
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
	return ductstreampb.NewDDLServiceClient(conn), func() { _ = conn.Close() }
}

func ddlSummary(event *ductstreampb.DDLEvent) string {
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
	fmt.Println("  ductstream-admin ddl <list|approve|reject|apply> [flags]")
	fmt.Println("  ductstream-admin stream <replay> [flags]")
	fmt.Println("  ductstream-admin publication <list|add|remove|sync|scrape> [flags]")
	os.Exit(1)
}

func ddlUsage() {
	fmt.Println("DDL subcommands:")
	fmt.Println("  ductstream-admin ddl list -status pending|approved|rejected|applied|all")
	fmt.Println("  ductstream-admin ddl approve -id <event_id>")
	fmt.Println("  ductstream-admin ddl reject -id <event_id>")
	fmt.Println("  ductstream-admin ddl apply -id <event_id>")
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

	resp, err := client.Replay(ctx, &ductstreampb.StreamReplayRequest{
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
	fmt.Println("  ductstream-admin stream replay -stream <name> -group <name> [-from-lsn <lsn>] [-since <rfc3339>]")
	fmt.Println("  ductstream-admin stream pull -stream <name> -group <name> [-max 10] [-visibility 30] [-consumer <id>] [--json|--pretty]")
	fmt.Println("  ductstream-admin stream ack -stream <name> -group <name> -ids 1,2,3")
	os.Exit(1)
}

func streamClientOrExit(endpoint string, insecureConn bool) (ductstreampb.StreamServiceClient, func()) {
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
	return ductstreampb.NewStreamServiceClient(conn), func() { _ = conn.Close() }
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

	resp, err := client.Pull(ctx, &ductstreampb.StreamPullRequest{
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

	resp, err := client.Ack(ctx, &ductstreampb.StreamAckRequest{
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
	fmt.Println("  ductstream-admin publication list -flow-id <id> [--json|--pretty]")
	fmt.Println("  ductstream-admin publication add -flow-id <id> -tables schema.table,...")
	fmt.Println("  ductstream-admin publication remove -flow-id <id> -tables schema.table,...")
	fmt.Println("  ductstream-admin publication sync -flow-id <id> [-tables ...] [-schemas ...] [-mode add|sync] [-pause] [-resume] [-snapshot]")
	fmt.Println("  ductstream-admin publication scrape -flow-id <id> -schemas public,app [-apply]")
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
	fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication)
	if err != nil {
		log.Fatal(err)
	}

	tables, err := postgres.ListPublicationTables(ctx, cfg.dsn, cfg.publication)
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

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication)
	if err != nil {
		log.Fatal(err)
	}

	if err := postgres.AddPublicationTables(ctx, cfg.dsn, cfg.publication, tableList); err != nil {
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

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication)
	if err != nil {
		log.Fatal(err)
	}

	if err := postgres.DropPublicationTables(ctx, cfg.dsn, cfg.publication, tableList); err != nil {
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
	fs.Parse(args)

	if *flowID == "" {
		log.Fatal("-flow-id is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication)
	if err != nil {
		log.Fatal(err)
	}
	if cfg.flow == nil {
		log.Fatal("flow is required for publication sync")
	}

	desired, err := resolveDesiredTables(ctx, cfg, *tables, *schemas)
	if err != nil {
		log.Fatal(err)
	}
	if len(desired) == 0 {
		log.Fatal("no tables provided for sync")
	}

	flowClient, closeConn := flowClientOrExit(*endpoint, *insecureConn)
	defer closeConn()

	if *pause {
		if _, err := flowClient.StopFlow(ctx, &ductstreampb.StopFlowRequest{FlowId: cfg.flow.Id}); err != nil {
			log.Printf("pause flow: %v", err)
		}
	}

	added, removed, err := postgres.SyncPublicationTables(ctx, cfg.dsn, cfg.publication, desired, *mode)
	if err != nil {
		log.Fatalf("sync publication: %v", err)
	}

	if *snapshot && len(added) > 0 {
		if err := runBackfill(context.Background(), cfg.flow, added, *snapshotWorkers, *partitionColumn, *partitionCount, *snapshotStateBackend, *snapshotStatePath); err != nil {
			log.Fatalf("backfill new tables: %v", err)
		}
	}

	if *resume {
		if _, err := flowClient.ResumeFlow(ctx, &ductstreampb.ResumeFlowRequest{FlowId: cfg.flow.Id}); err != nil {
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

	cfg, err := resolvePublicationConfig(ctx, *endpoint, *insecureConn, *flowID, *dsn, *publication)
	if err != nil {
		log.Fatal(err)
	}

	allTables, err := postgres.ScrapeTables(ctx, cfg.dsn, schemaList)
	if err != nil {
		log.Fatalf("scrape tables: %v", err)
	}
	current, err := postgres.ListPublicationTables(ctx, cfg.dsn, cfg.publication)
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
		if err := postgres.AddPublicationTables(ctx, cfg.dsn, cfg.publication, missing); err != nil {
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

type publicationConfig struct {
	dsn         string
	publication string
	flow        *ductstreampb.Flow
}

func resolvePublicationConfig(ctx context.Context, endpoint string, insecureConn bool, flowID, dsn, publication string) (publicationConfig, error) {
	cfg := publicationConfig{dsn: dsn, publication: publication}
	if flowID == "" {
		if cfg.dsn == "" || cfg.publication == "" {
			return cfg, errors.New("flow-id or dsn/publication is required")
		}
		return cfg, nil
	}

	client, closeConn := flowClientOrExit(endpoint, insecureConn)
	defer closeConn()

	flowResp, err := client.GetFlow(ctx, &ductstreampb.GetFlowRequest{FlowId: flowID})
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
	if cfg.dsn == "" || cfg.publication == "" {
		return cfg, errors.New("source dsn/publication not found on flow")
	}
	return cfg, nil
}

func resolveDesiredTables(ctx context.Context, cfg publicationConfig, tables, schemas string) ([]string, error) {
	if tables != "" {
		return parseCSVValue(tables), nil
	}
	if schemas != "" {
		list := parseCSVValue(schemas)
		if len(list) == 0 {
			return nil, errors.New("no schemas provided")
		}
		return postgres.ScrapeTables(ctx, cfg.dsn, list)
	}
	if cfg.flow != nil {
		if value := cfg.flow.Source.Options["publication_tables"]; value != "" {
			return parseCSVValue(value), nil
		}
		if value := cfg.flow.Source.Options["tables"]; value != "" {
			return parseCSVValue(value), nil
		}
		if value := cfg.flow.Source.Options["publication_schemas"]; value != "" {
			return postgres.ScrapeTables(ctx, cfg.dsn, parseCSVValue(value))
		}
		if value := cfg.flow.Source.Options["schemas"]; value != "" {
			return postgres.ScrapeTables(ctx, cfg.dsn, parseCSVValue(value))
		}
	}
	return nil, errors.New("no tables or schemas specified")
}

func runBackfill(ctx context.Context, flowPB *ductstreampb.Flow, tables []string, workers int, partitionColumn string, partitionCount int, snapshotStateBackend string, snapshotStatePath string) error {
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
	source, err := factory.Source(model.Source)
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
		WireFormat:   model.WireFormat,
		Parallelism:  model.Parallelism,
	}
	return runner.Run(ctx)
}

func flowClientOrExit(endpoint string, insecureConn bool) (ductstreampb.FlowServiceClient, func()) {
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
	return ductstreampb.NewFlowServiceClient(conn), func() { _ = conn.Close() }
}

func flowFromProto(pb *ductstreampb.Flow) (flow.Flow, error) {
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
	}, nil
}

func endpointFromProto(endpoint *ductstreampb.Endpoint) (connector.Spec, error) {
	if endpoint == nil {
		return connector.Spec{}, errors.New("endpoint is required")
	}
	if endpoint.Type == ductstreampb.EndpointType_ENDPOINT_TYPE_UNSPECIFIED {
		return connector.Spec{}, errors.New("endpoint type is required")
	}
	return connector.Spec{
		Name:    endpoint.Name,
		Type:    endpointTypeFromProto(endpoint.Type),
		Options: endpoint.Options,
	}, nil
}

func endpointTypeFromProto(t ductstreampb.EndpointType) connector.EndpointType {
	switch t {
	case ductstreampb.EndpointType_ENDPOINT_TYPE_POSTGRES:
		return connector.EndpointPostgres
	case ductstreampb.EndpointType_ENDPOINT_TYPE_SNOWFLAKE:
		return connector.EndpointSnowflake
	case ductstreampb.EndpointType_ENDPOINT_TYPE_S3:
		return connector.EndpointS3
	case ductstreampb.EndpointType_ENDPOINT_TYPE_KAFKA:
		return connector.EndpointKafka
	case ductstreampb.EndpointType_ENDPOINT_TYPE_HTTP:
		return connector.EndpointHTTP
	case ductstreampb.EndpointType_ENDPOINT_TYPE_GRPC:
		return connector.EndpointGRPC
	case ductstreampb.EndpointType_ENDPOINT_TYPE_PROTO:
		return connector.EndpointProto
	case ductstreampb.EndpointType_ENDPOINT_TYPE_PGSTREAM:
		return connector.EndpointPGStream
	case ductstreampb.EndpointType_ENDPOINT_TYPE_SNOWPIPE:
		return connector.EndpointSnowpipe
	case ductstreampb.EndpointType_ENDPOINT_TYPE_PARQUET:
		return connector.EndpointParquet
	case ductstreampb.EndpointType_ENDPOINT_TYPE_DUCKDB:
		return connector.EndpointDuckDB
	case ductstreampb.EndpointType_ENDPOINT_TYPE_BUFSTREAM:
		return connector.EndpointBufStream
	case ductstreampb.EndpointType_ENDPOINT_TYPE_CLICKHOUSE:
		return connector.EndpointClickHouse
	default:
		return ""
	}
}

func wireFormatFromProto(format ductstreampb.WireFormat) connector.WireFormat {
	switch format {
	case ductstreampb.WireFormat_WIRE_FORMAT_ARROW:
		return connector.WireFormatArrow
	case ductstreampb.WireFormat_WIRE_FORMAT_PARQUET:
		return connector.WireFormatParquet
	case ductstreampb.WireFormat_WIRE_FORMAT_PROTO:
		return connector.WireFormatProto
	case ductstreampb.WireFormat_WIRE_FORMAT_AVRO:
		return connector.WireFormatAvro
	case ductstreampb.WireFormat_WIRE_FORMAT_JSON:
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

func streamPullMessages(messages []*ductstreampb.StreamMessage) []streamPullMessage {
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
	Status    string `json:"status"`
	LSN       string `json:"lsn"`
	DDL       string `json:"ddl,omitempty"`
	PlanJSON  string `json:"plan_json,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}

func ddlListEvents(events []*ductstreampb.DDLEvent) []ddlListRecord {
	out := make([]ddlListRecord, 0, len(events))
	for _, event := range events {
		record := ddlListRecord{
			ID:       event.Id,
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
