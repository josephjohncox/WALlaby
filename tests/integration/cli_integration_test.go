package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

	server := apigrpc.New(workflow.NewNoopEngine(), noopDispatcher{}, nil, store, nil, false)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "ddl", "list", "-json")
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

	server := apigrpc.New(workflow.NewNoopEngine(), noopDispatcher{}, nil, store, nil, false)
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

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "ddl", "approve", "-id", fmt.Sprintf("%d", eventID)); err != nil {
		t.Fatalf("wallaby-admin ddl approve: %v", err)
	}
	approved, err := store.GetDDL(ctx, eventID)
	if err != nil {
		t.Fatalf("get ddl: %v", err)
	}
	if approved.Status != registry.StatusApproved {
		t.Fatalf("expected approved status, got %s", approved.Status)
	}

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "ddl", "apply", "-id", fmt.Sprintf("%d", eventID)); err != nil {
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
	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "ddl", "reject", "-id", fmt.Sprintf("%d", rejectID)); err != nil {
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

	server := apigrpc.New(workflow.NewNoopEngine(), noopDispatcher{}, nil, nil, store, false)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()
	waitForTCP(t, listener.Addr().String(), 2*time.Second)

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "stream", "pull", "-stream", streamName, "-group", "g1", "-max", "1", "-json")
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

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "stream", "ack", "-stream", streamName, "-group", "g1", "-ids", fmt.Sprintf("%d", msgID)); err != nil {
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

	server := apigrpc.New(engine, dispatcher, nil, nil, nil, false)
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
				"dsn": "postgres://user:pass@localhost:5432/app?sslmode=disable",
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    "postgres://user:pass@localhost:5432/app?sslmode=disable",
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "-file", configPath, "-json")
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

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "run-once", "-flow-id", createResp.ID, "-json")
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

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false)
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
				"dsn": "postgres://user:pass@localhost:5432/app?sslmode=disable",
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    "postgres://user:pass@localhost:5432/app?sslmode=disable",
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "-file", configPath, "-json")
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

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "start", "-flow-id", createResp.ID, "-json")
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

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "stop", "-flow-id", createResp.ID, "-json")
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

	output, err = runWallabyAdmin(ctx, listener.Addr().String(), "flow", "resume", "-flow-id", createResp.ID, "-json")
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

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false)
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
				"dsn": "postgres://user:pass@localhost:5432/app?sslmode=disable",
			},
		},
		Destinations: []endpointConfigPayload{
			{
				Name: "dest",
				Type: "pgstream",
				Options: map[string]string{
					"dsn":    "postgres://user:pass@localhost:5432/app?sslmode=disable",
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "-file", configPath, "-start", "-json")
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

	server := apigrpc.New(engine, noopDispatcher{}, nil, nil, nil, false)
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
					"dsn":    "postgres://user:pass@localhost:5432/app?sslmode=disable",
					"stream": "orders",
				},
			},
		},
	})

	output, err := runWallabyAdmin(ctx, listener.Addr().String(), "flow", "create", "-file", configPath, "-json")
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

	if _, err := runWallabyAdmin(ctx, listener.Addr().String(), "publication", "sync", "-flow-id", createResp.ID, "-tables", "public.beta", "-mode", "add"); err != nil {
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

	listOutput, err := runWallabyAdmin(ctx, "unused:0", "publication", "list", "-dsn", srcDSN, "-publication", pubName, "-json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables := parsePublicationTables(t, listOutput)
	if !containsTable(tables, "public.alpha") {
		t.Fatalf("expected public.alpha, got %v", tables)
	}

	if _, err := runWallabyAdmin(ctx, "unused:0", "publication", "add", "-dsn", srcDSN, "-publication", pubName, "-tables", "public.beta"); err != nil {
		t.Fatalf("wallaby-admin publication add: %v", err)
	}
	listOutput, err = runWallabyAdmin(ctx, "unused:0", "publication", "list", "-dsn", srcDSN, "-publication", pubName, "-json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables = parsePublicationTables(t, listOutput)
	if !containsTable(tables, "public.beta") {
		t.Fatalf("expected public.beta, got %v", tables)
	}

	if _, err := runWallabyAdmin(ctx, "unused:0", "publication", "remove", "-dsn", srcDSN, "-publication", pubName, "-tables", "public.alpha"); err != nil {
		t.Fatalf("wallaby-admin publication remove: %v", err)
	}
	listOutput, err = runWallabyAdmin(ctx, "unused:0", "publication", "list", "-dsn", srcDSN, "-publication", pubName, "-json")
	if err != nil {
		t.Fatalf("wallaby-admin publication list: %v\n%s", err, listOutput)
	}
	tables = parsePublicationTables(t, listOutput)
	if containsTable(tables, "public.alpha") {
		t.Fatalf("expected public.alpha removed, got %v", tables)
	}

	if _, err := runWallabyAdmin(ctx, "unused:0", "publication", "scrape", "-dsn", srcDSN, "-publication", pubName, "-schemas", "public", "-apply"); err != nil {
		t.Fatalf("wallaby-admin publication scrape: %v", err)
	}
	listOutput, err = runWallabyAdmin(ctx, "unused:0", "publication", "list", "-dsn", srcDSN, "-publication", pubName, "-json")
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
	cmdArgs = append(cmdArgs, "-endpoint", endpoint, "-insecure")
	cmd := exec.CommandContext(ctx, "go", cmdArgs...)
	cmd.Dir = root
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
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
