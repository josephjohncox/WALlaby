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

	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
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
