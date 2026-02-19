package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	pgsource "github.com/josephjohncox/wallaby/connectors/sources/postgres"
	apigrpc "github.com/josephjohncox/wallaby/internal/api/grpc"
	"github.com/josephjohncox/wallaby/internal/registry"
	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/internal/workflow"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
)

type noopDispatcher struct{}

func (noopDispatcher) EnqueueFlow(context.Context, string) error { return nil }

type recordingDispatcher struct {
	ch chan string
}

func (d *recordingDispatcher) EnqueueFlow(_ context.Context, flowID string) error {
	select {
	case d.ch <- flowID:
	default:
	}
	return nil
}

func TestCLIIntegrationDDLList(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	dbName := "wallaby_cli_" + suffix
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
		t.Fatalf("create cli database: %v", err)
	}
	defer func() {
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName))
	}()

	dbDSN, err := dsnWithDatabase(baseDSN, dbName)
	if err != nil {
		t.Fatalf("build cli dsn: %v", err)
	}

	store, err := registry.NewPostgresStore(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create registry store: %v", err)
	}
	defer store.Close()

	plan := schema.Plan{
		Changes: []schema.Change{{
			Type:      schema.ChangeAddColumn,
			Namespace: "public",
			Table:     "widgets",
			Column:    "extra",
			ToType:    "text",
		}},
	}
	eventDDL := `ALTER TABLE "public"."widgets" ADD COLUMN "extra" text`
	eventID, err := store.RecordDDL(ctx, "flow-cli", eventDDL, plan, "0/0", registry.StatusPending)
	if err != nil {
		t.Fatalf("record ddl: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(workflow.NewNoopEngine(), noopDispatcher{}, nil, store, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "ddl", "list", "--json")
	if err != nil {
		t.Fatalf("wallaby-admin ddl list: %v\n%s", err, output)
	}

	var resp struct {
		Status string `json:"status"`
		Count  int    `json:"count"`
		Events []struct {
			ID     int64  `json:"id"`
			FlowID string `json:"flow_id"`
			DDL    string `json:"ddl"`
		} `json:"events"`
		Records []struct {
			ID     int64  `json:"id"`
			FlowID string `json:"flow_id"`
			DDL    string `json:"ddl"`
		} `json:"records"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		t.Fatalf("decode cli output: %v\n%s", err, output)
	}
	if resp.Count == 0 {
		t.Fatalf("expected ddl event, got count=0: %s", output)
	}
	events := resp.Events
	if len(events) == 0 {
		events = resp.Records
	}
	found := false
	for _, record := range events {
		if record.ID == eventID && record.DDL == eventDDL && record.FlowID == "flow-cli" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected ddl event %d in output: %s", eventID, output)
	}

}

func TestCLIIntegrationDDLShow(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName := "wallaby_cli_show_" + fmt.Sprintf("%d", time.Now().UnixNano())
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
		t.Fatalf("create cli database: %v", err)
	}
	defer func() {
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName))
	}()

	dbDSN, err := dsnWithDatabase(baseDSN, dbName)
	if err != nil {
		t.Fatalf("build cli dsn: %v", err)
	}

	store, err := registry.NewPostgresStore(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create registry store: %v", err)
	}
	defer store.Close()

	plan := schema.Plan{
		Changes: []schema.Change{{
			Type:      schema.ChangeAddColumn,
			Namespace: "public",
			Table:     "widgets",
			Column:    "extra",
			ToType:    "text",
		}},
	}
	eventID, err := store.RecordDDL(ctx, "flow-cli-show", `ALTER TABLE "public"."widgets" ADD COLUMN "extra" text`, plan, "0/0", registry.StatusPending)
	if err != nil {
		t.Fatalf("record ddl: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(workflow.NewNoopEngine(), noopDispatcher{}, nil, store, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "ddl", "show", "--id", fmt.Sprintf("%d", eventID), "--json")
	if err != nil {
		t.Fatalf("wallaby-admin ddl show: %v\n%s", err, output)
	}

	var resp struct {
		ID    int64  `json:"id"`
		Flow  string `json:"flow_id"`
		DDL   string `json:"ddl"`
		LSN   string `json:"lsn"`
		State string `json:"status"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		t.Fatalf("decode ddl show output: %v\n%s", err, output)
	}
	if resp.ID != eventID {
		t.Fatalf("expected ddl id %d, got %d", eventID, resp.ID)
	}
	if resp.Flow != "flow-cli-show" {
		t.Fatalf("unexpected flow id: %s", resp.Flow)
	}
	if resp.DDL == "" {
		t.Fatalf("expected ddl sql, got empty output: %s", output)
	}
	if resp.LSN == "" || resp.State == "" {
		t.Fatalf("expected populated status and lsn, got %s / %s", resp.State, resp.LSN)
	}
}

func TestCLIIntegrationDDLApproveRejectApply(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName := "wallaby_cli_ddl_" + fmt.Sprintf("%d", time.Now().UnixNano())
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
		t.Fatalf("create cli database: %v", err)
	}
	defer func() {
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName))
	}()

	dbDSN, err := dsnWithDatabase(baseDSN, dbName)
	if err != nil {
		t.Fatalf("build cli dsn: %v", err)
	}

	store, err := registry.NewPostgresStore(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create registry store: %v", err)
	}
	defer store.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(workflow.NewNoopEngine(), noopDispatcher{}, nil, store, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	plan := schema.Plan{Changes: []schema.Change{{Type: schema.ChangeAddColumn, Namespace: "public", Table: "widgets", Column: "extra", ToType: "text"}}}
	eventID, err := store.RecordDDL(ctx, "flow-cli", `ALTER TABLE "public"."widgets" ADD COLUMN "extra" text`, plan, "0/0", registry.StatusPending)
	if err != nil {
		t.Fatalf("record ddl: %v", err)
	}

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "ddl", "approve", "--id", fmt.Sprintf("%d", eventID)); err != nil {
		t.Fatalf("wallaby-admin ddl approve: %v", err)
	}
	approved, err := store.GetDDL(ctx, eventID)
	if err != nil {
		t.Fatalf("get ddl: %v", err)
	}
	if approved.Status != registry.StatusApproved {
		t.Fatalf("expected approved status, got %s", approved.Status)
	}

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "ddl", "apply", "--id", fmt.Sprintf("%d", eventID)); err != nil {
		t.Fatalf("wallaby-admin ddl apply: %v", err)
	}
	applied, err := store.GetDDL(ctx, eventID)
	if err != nil {
		t.Fatalf("get ddl: %v", err)
	}
	if applied.Status != registry.StatusApplied {
		t.Fatalf("expected applied status, got %s", applied.Status)
	}

	rejectID, err := store.RecordDDL(ctx, "flow-cli", `ALTER TABLE "public"."widgets" ADD COLUMN "rejected" text`, plan, "0/1", registry.StatusPending)
	if err != nil {
		t.Fatalf("record ddl: %v", err)
	}
	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "ddl", "reject", "--id", fmt.Sprintf("%d", rejectID)); err != nil {
		t.Fatalf("wallaby-admin ddl reject: %v", err)
	}
	rejected, err := store.GetDDL(ctx, rejectID)
	if err != nil {
		t.Fatalf("get ddl: %v", err)
	}
	if rejected.Status != registry.StatusRejected {
		t.Fatalf("expected rejected status, got %s", rejected.Status)
	}
}

func TestCLIIntegrationDDLHistory(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName := "wallaby_cli_history_" + fmt.Sprintf("%d", time.Now().UnixNano())
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
		t.Fatalf("create cli database: %v", err)
	}
	defer func() {
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", dbName))
	}()

	dbDSN, err := dsnWithDatabase(baseDSN, dbName)
	if err != nil {
		t.Fatalf("build cli dsn: %v", err)
	}

	store, err := registry.NewPostgresStore(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create registry store: %v", err)
	}
	defer store.Close()

	plan := schema.Plan{
		Changes: []schema.Change{{
			Type:      schema.ChangeAddColumn,
			Namespace: "public",
			Table:     "widgets",
			Column:    "history",
			ToType:    "text",
		}},
	}
	if _, err := store.RecordDDL(ctx, "flow-cli-history", `ALTER TABLE "public"."widgets" ADD COLUMN "history" text`, plan, "0/0", registry.StatusPending); err != nil {
		t.Fatalf("record ddl: %v", err)
	}
	if _, err := store.RecordDDL(ctx, "flow-cli-history", `ALTER TABLE "public"."widgets" DROP COLUMN "history"`, schema.Plan{}, "0/1", registry.StatusApproved); err != nil {
		t.Fatalf("record ddl: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(workflow.NewNoopEngine(), noopDispatcher{}, nil, store, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "ddl", "history", "--flow-id", "flow-cli-history", "--json")
	if err != nil {
		t.Fatalf("wallaby-admin ddl history: %v\n%s", err, output)
	}

	var resp struct {
		FlowID string `json:"flow_id"`
		Count  int    `json:"count"`
		Events []struct {
			ID     int64  `json:"id"`
			FlowID string `json:"flow_id"`
			Status string `json:"status"`
			DDL    string `json:"ddl"`
		} `json:"events"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		t.Fatalf("decode ddl history output: %v\n%s", err, output)
	}
	if resp.FlowID != "flow-cli-history" {
		t.Fatalf("unexpected flow_id in history output: %s", output)
	}
	if resp.Count != len(resp.Events) || resp.Count < 2 {
		t.Fatalf("expected at least 2 ddl history events, got %d events in %s", resp.Count, output)
	}
	if len(resp.Events[0].Status) == 0 || len(resp.Events[1].Status) == 0 {
		t.Fatalf("expected statuses in history payload: %s", output)
	}
}

func TestCLIIntegrationCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	output, err := runWallabyAdmin(ctx, "", "completion", "bash")
	if err != nil {
		t.Fatalf("wallaby-admin completion bash: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "wallaby-admin") || !strings.Contains(string(output), "bash completion") {
		t.Fatalf("expected bash completion script in output: %s", output)
	}
}

func TestCLIIntegrationFlowPlan(t *testing.T) {
	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "cli-flow-plan",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "plan-src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": testPostgresAppDSN(t),
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "plan-dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	output, err := runWallabyAdmin(ctx, "", "flow", "plan", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow plan: %v\n%s", err, output)
	}
	var resp struct {
		FlowName string `json:"flow_name"`
		Desired  struct {
			Name  string `json:"name"`
			State string `json:"state"`
		} `json:"desired"`
		ChangeCount int  `json:"change_count"`
		Compared    bool `json:"compared"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		t.Fatalf("decode flow plan output: %v\n%s", err, output)
	}
	if resp.FlowName != "cli-flow-plan" {
		t.Fatalf("expected plan flow_name cli-flow-plan, got %s", resp.FlowName)
	}
	if resp.Desired.Name != "cli-flow-plan" {
		t.Fatalf("expected desired.name cli-flow-plan, got %s", resp.Desired.Name)
	}
	if resp.Compared {
		t.Fatalf("did not expect compared=true without endpoint")
	}
	if resp.ChangeCount != 0 {
		t.Fatalf("expected no changes without compare endpoint, got %d", resp.ChangeCount)
	}
}

func TestCLIIntegrationCheckCommand(t *testing.T) {
	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "cli-flow-check",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "check-src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": testPostgresAppDSN(t),
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "check-dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	output, err := runWallabyAdmin(ctx, "", "check", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin check: %v\n%s", err, output)
	}
	var resp struct {
		AdminReachable bool   `json:"admin_reachable"`
		FlowName       string `json:"flow_name"`
		ConfigFile     string `json:"config_file"`
		Source         struct {
			Name      string `json:"name"`
			Checked   bool   `json:"checked"`
			Reachable bool   `json:"reachable"`
		} `json:"source"`
		Destinations []struct {
			Name      string `json:"name"`
			Checked   bool   `json:"checked"`
			Reachable bool   `json:"reachable"`
		} `json:"destinations"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		t.Fatalf("decode check output: %v\n%s", err, output)
	}
	if resp.FlowName != "cli-flow-check" {
		t.Fatalf("expected flow_name cli-flow-check, got %s", resp.FlowName)
	}
	if resp.ConfigFile == "" {
		t.Fatalf("expected config_file to be populated: %s", output)
	}
	if resp.AdminReachable {
		t.Fatalf("expected offline check to mark admin as unreachable without endpoint: %s", output)
	}
	if len(resp.Destinations) != 1 {
		t.Fatalf("expected one destination check result: %s", output)
	}
	if resp.Source.Name != "check-src" {
		t.Fatalf("expected check source name check-src, got %s", resp.Source.Name)
	}
	if resp.Source.Checked {
		t.Fatalf("expected connectivity checks to be off by default")
	}
	if len(resp.Destinations) == 0 {
		t.Fatalf("expected destination checks to be present: %s", output)
	}
	if resp.Destinations[0].Checked {
		t.Fatalf("expected connectivity checks to be off by default")
	}
	if resp.Destinations[0].Name != "check-dest" {
		t.Fatalf("expected destination name check-dest, got %s", resp.Destinations[0].Name)
	}
}

func TestCLIIntegrationStreamPullAck(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName, dbDSN := createTempDatabase(t, ctx, adminPool, "wallaby_stream")
	defer dropDatabase(t, adminPool, dbName)

	store, err := pgstream.NewStore(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create stream store: %v", err)
	}
	defer store.Close()

	streamName := "orders"
	if err := store.Enqueue(ctx, streamName, []pgstream.Message{
		{
			Stream:     streamName,
			Namespace:  "public",
			Table:      "orders",
			LSN:        "0/1",
			WireFormat: "json",
			Payload:    []byte(`{"id":1}`),
		},
	}); err != nil {
		t.Fatalf("enqueue stream message: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(workflow.NewNoopEngine(), noopDispatcher{}, nil, nil, store, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "stream", "pull", "--stream", streamName, "--group", "g1", "--max", "1", "--json")
	if err != nil {
		t.Fatalf("wallaby-admin stream pull: %v\n%s", err, output)
	}

	var resp struct {
		Count   int `json:"count"`
		Records []struct {
			ID int64 `json:"id"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		t.Fatalf("decode stream pull output: %v\n%s", err, output)
	}
	if resp.Count == 0 || len(resp.Records) == 0 {
		t.Fatalf("expected stream messages, got: %s", output)
	}
	msgID := resp.Records[0].ID

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "stream", "ack", "--stream", streamName, "--group", "g1", "--ids", fmt.Sprintf("%d", msgID)); err != nil {
		t.Fatalf("wallaby-admin stream ack: %v", err)
	}

	checkPool, err := pgxpool.New(ctx, dbDSN)
	if err != nil {
		t.Fatalf("connect stream db: %v", err)
	}
	defer checkPool.Close()

	var status string
	if err := checkPool.QueryRow(ctx, "SELECT status FROM stream_deliveries WHERE event_id = $1 AND consumer_group = $2", msgID, "g1").Scan(&status); err != nil {
		t.Fatalf("read delivery status: %v", err)
	}
	if status != pgstream.DeliveryStatusAcked {
		t.Fatalf("expected acked status, got %s", status)
	}
}

func TestCLIIntegrationFlowCreateRunOnce(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName, dbDSN := createTempDatabase(t, ctx, adminPool, "wallaby_flow")
	defer dropDatabase(t, adminPool, dbName)

	engine, err := workflow.NewPostgresEngine(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer engine.Close()

	dispatcher := &recordingDispatcher{ch: make(chan string, 1)}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(engine, dispatcher, nil, nil, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "cli-flow",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": testPostgresAppDSN(t),
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow create: %v\n%s", err, output)
	}

	var createResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &createResp); err != nil {
		t.Fatalf("decode flow create output: %v\n%s", err, output)
	}
	if createResp.ID == "" {
		t.Fatalf("expected flow id, got: %s", output)
	}

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "run-once", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow run-once: %v\n%s", err, output)
	}

	var runResp struct {
		FlowID     string `json:"flow_id"`
		Dispatched bool   `json:"dispatched"`
	}
	if err := json.Unmarshal(output, &runResp); err != nil {
		t.Fatalf("decode flow run-once output: %v\n%s", err, output)
	}
	if !runResp.Dispatched {
		t.Fatalf("expected dispatched=true, got: %s", output)
	}

	select {
	case got := <-dispatcher.ch:
		if got != createResp.ID {
			t.Fatalf("expected dispatched flow %s, got %s", createResp.ID, got)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("dispatcher did not receive flow id")
	}
}

func TestCLIIntegrationFlowListGetDeleteWaitValidate(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName, dbDSN := createTempDatabase(t, ctx, adminPool, "wallaby_flow_list_get")
	defer dropDatabase(t, adminPool, dbName)

	engine, err := workflow.NewPostgresEngine(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer engine.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "cli-flow-list-get-delete-wait",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": testPostgresAppDSN(t),
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow create: %v\n%s", err, output)
	}
	var createResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &createResp); err != nil {
		t.Fatalf("decode flow create output: %v\n%s", err, output)
	}
	if createResp.ID == "" {
		t.Fatalf("expected flow id, got: %s", output)
	}

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "list", "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow list: %v\n%s", err, output)
	}
	var listResp struct {
		Count int `json:"count"`
		Flows []struct {
			ID       string `json:"id"`
			Name     string `json:"name"`
			State    string `json:"state"`
			StateRaw int32  `json:"state_raw"`
			Source   struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"source"`
			Destinations []struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"destinations"`
		} `json:"flows"`
	}
	if err := json.Unmarshal(output, &listResp); err != nil {
		t.Fatalf("decode flow list output: %v\n%s", err, output)
	}
	if listResp.Count == 0 || len(listResp.Flows) == 0 {
		t.Fatalf("expected flow list result, got %s", output)
	}

	var flowState string
	var found bool
	for _, item := range listResp.Flows {
		if item.ID != createResp.ID {
			continue
		}
		found = true
		flowState = item.State
		if item.Name != "cli-flow-list-get-delete-wait" {
			t.Fatalf("unexpected flow name: %s", item.Name)
		}
		if item.Source.Name == "" || item.Source.Type == "" {
			t.Fatalf("missing source summary in flow list: %s", output)
		}
		if item.StateRaw == 0 {
			t.Fatalf("flow state raw should be set: %s", output)
		}
		if len(item.Destinations) != 1 || item.Destinations[0].Name == "" {
			t.Fatalf("missing destination summary in flow list: %s", output)
		}
		break
	}
	if !found {
		t.Fatalf("flow %q not found in list output: %s", createResp.ID, output)
	}

	listFilter := fmt.Sprintf("--state=%s", flowState)
	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "list", listFilter, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow list with state filter: %v\n%s", err, output)
	}
	var filtered struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(output, &filtered); err != nil {
		t.Fatalf("decode filtered flow list output: %v\n%s", err, output)
	}
	if filtered.Count == 0 {
		t.Fatalf("expected non-empty flow list after state filter: %s", output)
	}

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "get", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow get: %v\n%s", err, output)
	}
	var getResp struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		State    string `json:"state"`
		StateRaw int32  `json:"state_raw"`
		Source   struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"source"`
		Destinations []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"destinations"`
		WireFormat string `json:"wire_format"`
	}
	if err := json.Unmarshal(output, &getResp); err != nil {
		t.Fatalf("decode flow get output: %v\n%s", err, output)
	}
	if getResp.ID != createResp.ID {
		t.Fatalf("expected flow id %s, got %s", createResp.ID, getResp.ID)
	}
	if getResp.Name != "cli-flow-list-get-delete-wait" {
		t.Fatalf("expected flow name cli-flow-list-get-delete-wait, got %s", getResp.Name)
	}
	if getResp.StateRaw == 0 {
		t.Fatalf("expected non-zero flow state, got: %s", output)
	}
	if getResp.Source.Name == "" || getResp.Source.Type == "" {
		t.Fatalf("missing source info in flow get: %s", output)
	}
	if len(getResp.Destinations) != 1 || getResp.Destinations[0].Name == "" {
		t.Fatalf("missing destination in flow get: %s", output)
	}
	if getResp.WireFormat != "json" {
		t.Fatalf("expected wire_format json, got %s", getResp.WireFormat)
	}
	if flowState == "" {
		flowState = getResp.State
	}

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "wait", "--flow-id", createResp.ID, "--state", flowState, "--timeout", "2s", "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow wait: %v\n%s", err, output)
	}
	var waitResp struct {
		FlowID   string `json:"flow_id"`
		State    string `json:"state"`
		StateRaw int32  `json:"state_raw"`
	}
	if err := json.Unmarshal(output, &waitResp); err != nil {
		t.Fatalf("decode flow wait output: %v\n%s", err, output)
	}
	if waitResp.FlowID != createResp.ID {
		t.Fatalf("flow wait returned wrong flow id: %s", waitResp.FlowID)
	}
	if waitResp.State != flowState {
		t.Fatalf("expected flow wait state %s, got %s", flowState, waitResp.State)
	}
	if waitResp.StateRaw == 0 {
		t.Fatalf("expected non-zero state raw in wait output: %s", output)
	}

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "delete", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow delete: %v\n%s", err, output)
	}
	var deleteResp struct {
		FlowID  string `json:"flow_id"`
		Deleted bool   `json:"deleted"`
	}
	if err := json.Unmarshal(output, &deleteResp); err != nil {
		t.Fatalf("decode flow delete output: %v\n%s", err, output)
	}
	if !deleteResp.Deleted {
		t.Fatalf("expected deleted=true, got %s", output)
	}
	if deleteResp.FlowID != "" && deleteResp.FlowID != createResp.ID {
		t.Fatalf("expected flow delete output id %s, got %s", createResp.ID, deleteResp.FlowID)
	}

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "list", "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow list after delete: %v\n%s", err, output)
	}
	var afterDelete struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(output, &afterDelete); err != nil {
		t.Fatalf("decode flow list output after delete: %v\n%s", err, output)
	}
	if afterDelete.Count != 0 {
		t.Fatalf("expected no flows after delete, got %d", afterDelete.Count)
	}

	output, err = runWallabyAdmin(ctx, "", "flow", "validate", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow validate: %v\n%s", err, output)
	}
	var validateResp struct {
		Valid            bool                  `json:"valid"`
		Name             string                `json:"name"`
		DestinationCount int                   `json:"destination_count"`
		Source           endpointConfigPayload `json:"source"`
	}
	if err := json.Unmarshal(output, &validateResp); err != nil {
		t.Fatalf("decode flow validate output: %v\n%s", err, output)
	}
	if !validateResp.Valid {
		t.Fatalf("expected flow config to be valid: %s", output)
	}
	if validateResp.Name != "cli-flow-list-get-delete-wait" {
		t.Fatalf("expected validated name cli-flow-list-get-delete-wait, got %s", validateResp.Name)
	}
	if validateResp.Source.Name == "" {
		t.Fatalf("expected validated source name, got %v", output)
	}
	if validateResp.DestinationCount != 1 {
		t.Fatalf("expected destination count 1, got %d", validateResp.DestinationCount)
	}
}

func TestCLIIntegrationFlowDryRunCheck(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_ = baseDSN

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "cli-flow-dry-run-check",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": testPostgresAppDSN(t),
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, "", "flow", "dry-run", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow dry-run: %v\n%s", err, output)
	}
	var dryRunResp struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Source struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"source"`
		Destinations []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"destinations"`
	}
	if err := json.Unmarshal(output, &dryRunResp); err != nil {
		t.Fatalf("decode flow dry-run output: %v\n%s", err, output)
	}
	if dryRunResp.Name != "cli-flow-dry-run-check" {
		t.Fatalf("expected dry-run name cli-flow-dry-run-check, got %q", dryRunResp.Name)
	}
	if dryRunResp.Source.Name == "" || dryRunResp.Source.Type == "" {
		t.Fatalf("expected source in dry-run output: %s", output)
	}
	if len(dryRunResp.Destinations) != 1 {
		t.Fatalf("expected 1 destination in dry-run output, got %d", len(dryRunResp.Destinations))
	}

	output, err = runWallabyAdmin(ctx, "", "flow", "check", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow check: %v\n%s", err, output)
	}
	var checkResp struct {
		Valid            bool   `json:"valid"`
		Name             string `json:"name"`
		SourceType       string `json:"source_type"`
		DestinationCount int    `json:"destination_count"`
	}
	if err := json.Unmarshal(output, &checkResp); err != nil {
		t.Fatalf("decode flow check output: %v\n%s", err, output)
	}
	if !checkResp.Valid {
		t.Fatalf("expected flow check valid=true: %s", output)
	}
	if checkResp.Name != "cli-flow-dry-run-check" {
		t.Fatalf("expected checked name cli-flow-dry-run-check, got %q", checkResp.Name)
	}
	if checkResp.SourceType != "postgres" {
		t.Fatalf("expected source_type postgres, got %q", checkResp.SourceType)
	}
	if checkResp.DestinationCount != 1 {
		t.Fatalf("expected destination_count 1, got %d", checkResp.DestinationCount)
	}
}

func TestCLIIntegrationFlowUpdate(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName, dbDSN := createTempDatabase(t, ctx, adminPool, "wallaby_flow_update")
	defer dropDatabase(t, adminPool, dbName)

	engine, err := workflow.NewPostgresEngine(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer engine.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "update-flow",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": testPostgresAppDSN(t),
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow create: %v\n%s", err, output)
	}

	var createResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &createResp); err != nil {
		t.Fatalf("decode flow create output: %v\n%s", err, output)
	}
	if createResp.ID == "" {
		t.Fatalf("expected flow id, got: %s", output)
	}

	updatePath := writeFlowConfig(t, flowConfigPayload{
		ID:         createResp.ID,
		Name:       "update-flow",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": testPostgresAppDSN(t),
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders_v2",
				},
			},
		},
	})

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "update", "--file", updatePath, "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow update: %v\n%s", err, output)
	}

	var updateResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &updateResp); err != nil {
		t.Fatalf("decode flow update output: %v\n%s", err, output)
	}
	if updateResp.ID != createResp.ID {
		t.Fatalf("expected updated id %s, got %s", createResp.ID, updateResp.ID)
	}

	updated, err := engine.Get(ctx, createResp.ID)
	if err != nil {
		t.Fatalf("get updated flow: %v", err)
	}
	if len(updated.Destinations) != 1 {
		t.Fatalf("expected 1 destination, got %d", len(updated.Destinations))
	}
	if updated.Destinations[0].Options["stream"] != "orders_v2" {
		t.Fatalf("expected updated stream option, got %v", updated.Destinations[0].Options)
	}
}

func TestCLIIntegrationFlowReconfigure(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName, dbDSN := createTempDatabase(t, ctx, adminPool, "wallaby_flow_reconfigure")
	defer dropDatabase(t, adminPool, dbName)

	engine, err := workflow.NewPostgresEngine(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer engine.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "reconfigure-flow",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": baseDSN,
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow create: %v\n%s", err, output)
	}

	var createResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &createResp); err != nil {
		t.Fatalf("decode flow create output: %v\n%s", err, output)
	}
	if createResp.ID == "" {
		t.Fatalf("expected flow id, got: %s", output)
	}

	updatePath := writeFlowConfig(t, flowConfigPayload{
		ID:         createResp.ID,
		Name:       "reconfigure-flow",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": baseDSN,
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders_v2",
				},
			},
		},
	})

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "reconfigure", "--file", updatePath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow reconfigure: %v\n%s", err, output)
	}

	var resp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		t.Fatalf("decode flow reconfigure output: %v\n%s", err, output)
	}
	if resp.ID != createResp.ID {
		t.Fatalf("expected flow id %s, got %s", createResp.ID, resp.ID)
	}
}

func TestCLIIntegrationFlowCleanup(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName, dbDSN := createTempDatabase(t, ctx, adminPool, "wallaby_flow_cleanup")
	defer dropDatabase(t, adminPool, dbName)

	engine, err := workflow.NewPostgresEngine(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer engine.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "cleanup-flow",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn":         baseDSN,
				"slot":        "cleanup_slot",
				"publication": "cleanup_pub",
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow create: %v\n%s", err, output)
	}

	var createResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &createResp); err != nil {
		t.Fatalf("decode flow create output: %v\n%s", err, output)
	}
	if createResp.ID == "" {
		t.Fatalf("expected flow id, got: %s", output)
	}

	slotName := "cleanup_slot"
	pubName := "cleanup_pub"
	tableName := fmt.Sprintf("cleanup_table_%d", time.Now().UnixNano())
	tableIdent := pgx.Identifier{"public", tableName}.Sanitize()

	if err := pgsource.DropReplicationSlot(ctx, baseDSN, slotName, nil); err != nil {
		t.Fatalf("drop replication slot: %v", err)
	}
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int primary key)", tableIdent)); err != nil {
		t.Fatalf("create cleanup table: %v", err)
	}
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{pubName}.Sanitize())); err != nil {
		t.Fatalf("drop publication: %v", err)
	}
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgx.Identifier{pubName}.Sanitize(), tableIdent)); err != nil {
		t.Fatalf("create publication: %v", err)
	}
	if _, err := adminPool.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slotName); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42710" {
			t.Fatalf("create replication slot: %v", err)
		}
	}
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS wallaby"); err != nil {
		t.Fatalf("ensure source state schema: %v", err)
	}
	if _, err := adminPool.Exec(ctx, `CREATE TABLE IF NOT EXISTS wallaby.source_state (
  id TEXT PRIMARY KEY,
  source_name TEXT,
  slot_name TEXT NOT NULL,
  publication_name TEXT NOT NULL,
  state TEXT NOT NULL,
  options JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_lsn TEXT,
  last_ack_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`); err != nil {
		t.Fatalf("ensure source state table: %v", err)
	}
	if _, err := adminPool.Exec(ctx, `INSERT INTO wallaby.source_state (id, source_name, slot_name, publication_name, state, options, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, now(), now())
ON CONFLICT (id) DO UPDATE SET slot_name = EXCLUDED.slot_name, publication_name = EXCLUDED.publication_name, state = EXCLUDED.state, updated_at = now()`,
		"src", "src", slotName, pubName, "running"); err != nil {
		t.Fatalf("seed source state: %v", err)
	}

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "cleanup", "--flow-id", createResp.ID, "--drop-publication", "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow cleanup: %v\n%s", err, output)
	}

	var resp struct {
		Cleaned bool `json:"cleaned"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		t.Fatalf("decode flow cleanup output: %v\n%s", err, output)
	}
	if !resp.Cleaned {
		t.Fatalf("expected cleaned=true, got %s", output)
	}

	var slotCount int
	if err := adminPool.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&slotCount); err != nil {
		t.Fatalf("check slot: %v", err)
	}
	if slotCount != 0 {
		t.Fatalf("expected slot to be dropped, still present")
	}
	var pubCount int
	if err := adminPool.QueryRow(ctx, "SELECT count(*) FROM pg_publication WHERE pubname = $1", pubName).Scan(&pubCount); err != nil {
		t.Fatalf("check publication: %v", err)
	}
	if pubCount != 0 {
		t.Fatalf("expected publication to be dropped, still present")
	}
	var stateCount int
	if err := adminPool.QueryRow(ctx, "SELECT count(*) FROM wallaby.source_state WHERE id = $1", "src").Scan(&stateCount); err != nil {
		t.Fatalf("check source state: %v", err)
	}
	if stateCount != 0 {
		t.Fatalf("expected source state to be deleted, still present")
	}
}

func TestCLIIntegrationSlotCommands(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName, dbDSN := createTempDatabase(t, ctx, adminPool, "wallaby_cli_slot")
	defer dropDatabase(t, adminPool, dbName)

	engine, err := workflow.NewPostgresEngine(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer engine.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	slotName := "cli_slot_cmd"
	pubName := "cli_slot_pub"
	tableName := fmt.Sprintf("slot_table_%d", time.Now().UnixNano())
	tableIdent := pgx.Identifier{"public", tableName}.Sanitize()

	if err := pgsource.DropReplicationSlot(ctx, baseDSN, slotName, nil); err != nil {
		t.Fatalf("drop replication slot: %v", err)
	}
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int primary key)", tableIdent)); err != nil {
		t.Fatalf("create slot table: %v", err)
	}
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{pubName}.Sanitize())); err != nil {
		t.Fatalf("drop publication: %v", err)
	}
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pgx.Identifier{pubName}.Sanitize(), tableIdent)); err != nil {
		t.Fatalf("create publication: %v", err)
	}
	if _, err := adminPool.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slotName); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42710" {
			t.Fatalf("create replication slot: %v", err)
		}
	}

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "slot-command-flow",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn":         baseDSN,
				"slot":        slotName,
				"publication": pubName,
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow create: %v\n%s", err, output)
	}
	var createResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &createResp); err != nil {
		t.Fatalf("decode flow create output: %v\n%s", err, output)
	}
	if createResp.ID == "" {
		t.Fatalf("expected flow id, got: %s", output)
	}

	listOutput, err := runWallabyAdmin(ctx, listener.Addr().String(), "slot", "list", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin slot list: %v\n%s", err, listOutput)
	}
	var listResp struct {
		Count int `json:"count"`
		Slots []struct {
			SlotName string `json:"slot_name"`
		} `json:"slots"`
	}
	if err := json.Unmarshal(listOutput, &listResp); err != nil {
		t.Fatalf("decode slot list output: %v\n%s", err, listOutput)
	}
	if listResp.Count == 0 {
		t.Fatalf("expected at least one slot, got 0")
	}
	found := false
	for _, item := range listResp.Slots {
		if item.SlotName == slotName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected slot %q in list output: %v", slotName, listOutput)
	}

	filterOutput, err := runWallabyAdmin(ctx, listener.Addr().String(), "slot", "list", "--flow-id", createResp.ID, "--slot", slotName, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin slot list filtered: %v\n%s", err, filterOutput)
	}
	var filterResp struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(filterOutput, &filterResp); err != nil {
		t.Fatalf("decode filtered slot list output: %v\n%s", err, filterOutput)
	}
	if filterResp.Count != 1 {
		t.Fatalf("expected filtered slot count 1, got %d", filterResp.Count)
	}

	showOutput, err := runWallabyAdmin(ctx, listener.Addr().String(), "slot", "show", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin slot show: %v\n%s", err, showOutput)
	}
	var showResp struct {
		SlotName string `json:"slot_name"`
	}
	if err := json.Unmarshal(showOutput, &showResp); err != nil {
		t.Fatalf("decode slot show output: %v\n%s", err, showOutput)
	}
	if showResp.SlotName != slotName {
		t.Fatalf("expected slot %q, got %q", slotName, showResp.SlotName)
	}

	dropOutput, err := runWallabyAdmin(ctx, listener.Addr().String(), "slot", "drop", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin slot drop: %v\n%s", err, dropOutput)
	}
	var dropResp struct {
		SlotName string `json:"slot"`
		Dropped  bool   `json:"dropped"`
	}
	if err := json.Unmarshal(dropOutput, &dropResp); err != nil {
		t.Fatalf("decode slot drop output: %v\n%s", err, dropOutput)
	}
	if !dropResp.Dropped || dropResp.SlotName != slotName {
		t.Fatalf("expected dropped slot %q response, got %#v", slotName, dropResp)
	}

	var slotCount int
	if err := adminPool.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&slotCount); err != nil {
		t.Fatalf("check slot removed: %v", err)
	}
	if slotCount != 0 {
		t.Fatalf("expected slot to be dropped, still present")
	}
}

func TestCLIIntegrationFlowStartStopResume(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName, dbDSN := createTempDatabase(t, ctx, adminPool, "wallaby_flow_state")
	defer dropDatabase(t, adminPool, dbName)

	engine, err := workflow.NewPostgresEngine(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer engine.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "cli-flow-state",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": testPostgresAppDSN(t),
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow create: %v\n%s", err, output)
	}
	var createResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &createResp); err != nil {
		t.Fatalf("decode flow create output: %v\n%s", err, output)
	}
	if createResp.ID == "" {
		t.Fatalf("expected flow id, got: %s", output)
	}

	type stateResp struct {
		ID    string `json:"id"`
		State string `json:"state"`
	}

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "start", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow start: %v\n%s", err, output)
	}
	var startResp stateResp
	if err := json.Unmarshal(output, &startResp); err != nil {
		t.Fatalf("decode flow start output: %v\n%s", err, output)
	}
	if startResp.State != "FLOW_STATE_RUNNING" {
		t.Fatalf("expected running, got %s", startResp.State)
	}

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "stop", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow stop: %v\n%s", err, output)
	}
	var stopResp stateResp
	if err := json.Unmarshal(output, &stopResp); err != nil {
		t.Fatalf("decode flow stop output: %v\n%s", err, output)
	}
	if stopResp.State != "FLOW_STATE_PAUSED" {
		t.Fatalf("expected paused, got %s", stopResp.State)
	}

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "resume", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow resume: %v\n%s", err, output)
	}
	var resumeResp stateResp
	if err := json.Unmarshal(output, &resumeResp); err != nil {
		t.Fatalf("decode flow resume output: %v\n%s", err, output)
	}
	if resumeResp.State != "FLOW_STATE_RUNNING" {
		t.Fatalf("expected running, got %s", resumeResp.State)
	}
}

func TestCLIIntegrationFlowCreateStartFlag(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	dbName, dbDSN := createTempDatabase(t, ctx, adminPool, "wallaby_flow_start_flag")
	defer dropDatabase(t, adminPool, dbName)

	engine, err := workflow.NewPostgresEngine(ctx, dbDSN)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer engine.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "cli-flow-start",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn": testPostgresAppDSN(t),
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "--file", configPath, "--start", "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow create -start: %v\n%s", err, output)
	}

	var resp struct {
		ID    string `json:"id"`
		State string `json:"state"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		t.Fatalf("decode flow create output: %v\n%s", err, output)
	}
	if resp.State != "FLOW_STATE_RUNNING" {
		t.Fatalf("expected running, got %s", resp.State)
	}
}

func TestCLIIntegrationPublicationSync(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	srcDB, srcDSN := createTempDatabase(t, ctx, adminPool, "wallaby_pub_src")
	defer dropDatabase(t, adminPool, srcDB)

	engineDB, engineDSN := createTempDatabase(t, ctx, adminPool, "wallaby_pub_engine")
	defer dropDatabase(t, adminPool, engineDB)

	srcPool, err := pgxpool.New(ctx, srcDSN)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcPool.Close()

	if _, err := srcPool.Exec(ctx, `CREATE TABLE public.alpha (id int primary key)`); err != nil {
		t.Fatalf("create table alpha: %v", err)
	}
	if _, err := srcPool.Exec(ctx, `CREATE TABLE public.beta (id int primary key)`); err != nil {
		t.Fatalf("create table beta: %v", err)
	}
	pubName := "wallaby_pub_" + fmt.Sprintf("%d", time.Now().UnixNano())
	if _, err := srcPool.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE public.alpha`, pubName)); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	engine, err := workflow.NewPostgresEngine(ctx, engineDSN)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer engine.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "pub-flow",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn":         srcDSN,
				"publication": pubName,
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow create: %v\n%s", err, output)
	}
	var createResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &createResp); err != nil {
		t.Fatalf("decode flow create output: %v\n%s", err, output)
	}
	if createResp.ID == "" {
		t.Fatalf("expected flow id, got: %s", output)
	}

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "publication", "sync", "--flow-id", createResp.ID, "--tables", "public.beta", "--mode", "add"); err != nil {
		t.Fatalf("wallaby-admin publication sync: %v", err)
	}

	tables, err := pgsource.ListPublicationTables(ctx, srcDSN, pubName, nil)
	if err != nil {
		t.Fatalf("list publication tables: %v", err)
	}

	found := false
	for _, table := range tables {
		if strings.EqualFold(table, "public.beta") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected public.beta in publication, got %v", tables)
	}
}

func TestCLIIntegrationPublicationRemoteCommands(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	srcDB, srcDSN := createTempDatabase(t, ctx, adminPool, "wallaby_pub_remote_cmd")
	defer dropDatabase(t, adminPool, srcDB)

	srcPool, err := pgxpool.New(ctx, srcDSN)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcPool.Close()

	if _, err := srcPool.Exec(ctx, `CREATE TABLE public.alpha (id int primary key)`); err != nil {
		t.Fatalf("create table alpha: %v", err)
	}
	if _, err := srcPool.Exec(ctx, `CREATE TABLE public.beta (id int primary key)`); err != nil {
		t.Fatalf("create table beta: %v", err)
	}
	if _, err := srcPool.Exec(ctx, `CREATE TABLE public.gamma (id int primary key)`); err != nil {
		t.Fatalf("create table gamma: %v", err)
	}

	pubName := "wallaby_pub_remote_" + fmt.Sprintf("%d", time.Now().UnixNano())
	if _, err := srcPool.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE public.alpha`, pgx.Identifier{pubName}.Sanitize())); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	engine, err := workflow.NewPostgresEngine(ctx, srcDSN)
	if err != nil {
		t.Fatalf("create workflow engine: %v", err)
	}
	defer engine.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	defer listener.Close()

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false, nil)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	configPath := writeFlowConfig(t, flowConfigPayload{
		Name:       "pub-remote-command-flow",
		WireFormat: "json",
		Source: endpointConfigPayload{
			Name: "src",
			Type: "postgres",
			Options: map[string]string{
				"dsn":         srcDSN,
				"publication": pubName,
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    testPostgresAppDSN(t),
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "--file", configPath, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin flow create: %v\n%s", err, output)
	}
	var createResp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(output, &createResp); err != nil {
		t.Fatalf("decode flow create output: %v\n%s", err, output)
	}
	if createResp.ID == "" {
		t.Fatalf("expected flow id, got: %s", output)
	}

	listOutput, err := runWallabyAdmin(ctx, listener.Addr().String(), "publication", "list", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables := parsePublicationTables(t, listOutput)
	if !containsTable(tables, "public.alpha") {
		t.Fatalf("expected public.alpha, got %v", tables)
	}

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "publication", "add", "--flow-id", createResp.ID, "--tables", "public.beta"); err != nil {
		t.Fatalf("wallaby-admin publication add: %v", err)
	}
	listOutput, err = runWallabyAdmin(ctx, listener.Addr().String(), "publication", "list", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables = parsePublicationTables(t, listOutput)
	if !containsTable(tables, "public.beta") {
		t.Fatalf("expected public.beta, got %v", tables)
	}

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "publication", "remove", "--flow-id", createResp.ID, "--tables", "public.alpha"); err != nil {
		t.Fatalf("wallaby-admin publication remove: %v", err)
	}
	listOutput, err = runWallabyAdmin(ctx, listener.Addr().String(), "publication", "list", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables = parsePublicationTables(t, listOutput)
	if containsTable(tables, "public.alpha") {
		t.Fatalf("expected public.alpha removed, got %v", tables)
	}

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "publication", "scrape", "--flow-id", createResp.ID, "--schemas", "public", "--apply"); err != nil {
		t.Fatalf("wallaby-admin publication scrape: %v", err)
	}
	listOutput, err = runWallabyAdmin(ctx, listener.Addr().String(), "publication", "list", "--flow-id", createResp.ID, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables = parsePublicationTables(t, listOutput)
	if !containsTable(tables, "public.gamma") {
		t.Fatalf("expected public.gamma after scrape, got %v", tables)
	}
}

func TestCLIIntegrationPublicationListAddRemoveScrape(t *testing.T) {
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer adminPool.Close()

	srcDB, srcDSN := createTempDatabase(t, ctx, adminPool, "wallaby_pub_list")
	defer dropDatabase(t, adminPool, srcDB)

	srcPool, err := pgxpool.New(ctx, srcDSN)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer srcPool.Close()

	if _, err := srcPool.Exec(ctx, `CREATE TABLE public.alpha (id int primary key)`); err != nil {
		t.Fatalf("create table alpha: %v", err)
	}
	if _, err := srcPool.Exec(ctx, `CREATE TABLE public.beta (id int primary key)`); err != nil {
		t.Fatalf("create table beta: %v", err)
	}
	if _, err := srcPool.Exec(ctx, `CREATE TABLE public.gamma (id int primary key)`); err != nil {
		t.Fatalf("create table gamma: %v", err)
	}
	pubName := "wallaby_pub_" + fmt.Sprintf("%d", time.Now().UnixNano())
	if _, err := srcPool.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE public.alpha`, pubName)); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	listOutput, err := runWallabyAdmin(ctx, "unused:0", "publication", "list", "--dsn", srcDSN, "--publication", pubName, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables := parsePublicationTables(t, listOutput)
	if !containsTable(tables, "public.alpha") {
		t.Fatalf("expected public.alpha, got %v", tables)
	}

	if _, err := runWallabyAdmin(ctx, "unused:0", "publication", "add", "--dsn", srcDSN, "--publication", pubName, "--tables", "public.beta"); err != nil {
		t.Fatalf("wallaby-admin publication add: %v", err)
	}
	listOutput, err = runWallabyAdmin(ctx, "unused:0", "publication", "list", "--dsn", srcDSN, "--publication", pubName, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables = parsePublicationTables(t, listOutput)
	if !containsTable(tables, "public.beta") {
		t.Fatalf("expected public.beta, got %v", tables)
	}

	if _, err := runWallabyAdmin(ctx, "unused:0", "publication", "remove", "--dsn", srcDSN, "--publication", pubName, "--tables", "public.alpha"); err != nil {
		t.Fatalf("wallaby-admin publication remove: %v", err)
	}
	listOutput, err = runWallabyAdmin(ctx, "unused:0", "publication", "list", "--dsn", srcDSN, "--publication", pubName, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables = parsePublicationTables(t, listOutput)
	if containsTable(tables, "public.alpha") {
		t.Fatalf("expected public.alpha removed, got %v", tables)
	}

	if _, err := runWallabyAdmin(ctx, "unused:0", "publication", "scrape", "--dsn", srcDSN, "--publication", pubName, "--schemas", "public", "--apply"); err != nil {
		t.Fatalf("wallaby-admin publication scrape: %v", err)
	}
	listOutput, err = runWallabyAdmin(ctx, "unused:0", "publication", "list", "--dsn", srcDSN, "--publication", pubName, "--json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables = parsePublicationTables(t, listOutput)
	if !containsTable(tables, "public.gamma") {
		t.Fatalf("expected public.gamma after scrape, got %v", tables)
	}
}

func runWallabyAdmin(ctx context.Context, endpoint string, args ...string) ([]byte, error) {
	root, err := moduleRoot()
	if err != nil {
		return nil, err
	}
	cmdArgs := append([]string{"run", "./cmd/wallaby-admin"}, args...)
	if endpoint != "" {
		cmdArgs = append(cmdArgs, "--endpoint", endpoint, "--insecure")
	}
	cmd := exec.CommandContext(ctx, "go", cmdArgs...)
	cmd.Dir = root
	cmd.Env = os.Environ()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	output := stdout.Bytes()
	debugLog := strings.TrimSpace(os.Getenv("WALLABY_TEST_CLI_LOG")) == "1"
	if debugLog {
		if len(stderr.Bytes()) > 0 {
			fmt.Fprintf(os.Stderr, "\n[wallaby-admin %s stderr]\n%s\n", strings.Join(args, " "), stderr.String())
		}
	}
	if err != nil && len(stderr.Bytes()) > 0 {
		return append(output, stderr.Bytes()...), err
	}
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_CLI_LOG")) == "1" {
		fmt.Fprintf(os.Stderr, "\n[wallaby-admin %s]\n%s\n", strings.Join(args, " "), string(output))
	}
	return output, err
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for i := 0; i < 10; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("go.mod not found from %s", dir)
}

func createTempDatabase(t *testing.T, ctx context.Context, admin *pgxpool.Pool, prefix string) (string, string) {
	t.Helper()
	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", name)); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	dsn, err := dsnWithDatabase(os.Getenv("TEST_PG_DSN"), name)
	if err != nil {
		t.Fatalf("build database dsn: %v", err)
	}
	return name, dsn
}

func testPostgresAppDSN(t *testing.T) string {
	t.Helper()
	baseDSN := strings.TrimSpace(os.Getenv("TEST_PG_DSN"))
	if baseDSN == "" {
		t.Skip("TEST_PG_DSN not set")
	}
	dsn, err := dsnWithDatabase(baseDSN, "app")
	if err != nil {
		t.Fatalf("build test postgres app dsn: %v", err)
	}
	return dsn
}

func dropDatabase(t *testing.T, admin *pgxpool.Pool, name string) {
	t.Helper()
	_, _ = admin.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", name))
}

func waitForTCP(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", addr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func parsePublicationTables(t *testing.T, output []byte) []string {
	t.Helper()
	var resp struct {
		Count  int      `json:"count"`
		Tables []string `json:"tables"`
	}
	if err := json.Unmarshal(output, &resp); err != nil {
		t.Fatalf("decode publication list output: %v\n%s", err, output)
	}
	if resp.Count == 0 {
		return nil
	}
	return resp.Tables
}

func containsTable(tables []string, value string) bool {
	for _, table := range tables {
		if strings.EqualFold(table, value) {
			return true
		}
	}
	return false
}

type flowConfigPayload struct {
	ID           string                  `json:"id,omitempty"`
	Name         string                  `json:"name"`
	WireFormat   string                  `json:"wire_format"`
	Parallelism  int32                   `json:"parallelism,omitempty"`
	Source       endpointConfigPayload   `json:"source"`
	Destinations []endpointConfigPayload `json:"destinations"`
}

type endpointConfigPayload struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	Options map[string]string `json:"options"`
}

func writeFlowConfig(t *testing.T, cfg flowConfigPayload) string {
	t.Helper()
	file, err := os.CreateTemp("", "wallaby-flow-*.json")
	if err != nil {
		t.Fatalf("create flow config: %v", err)
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cfg); err != nil {
		t.Fatalf("encode flow config: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Remove(file.Name())
	})
	return file.Name()
}
