package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	ductstreampb "github.com/josephjohncox/ductstream/gen/go/ductstream/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	switch os.Args[1] {
	case "ddl":
		runDDL(os.Args[2:])
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
