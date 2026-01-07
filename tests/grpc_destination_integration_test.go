package tests

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	grpcdest "github.com/josephjohncox/wallaby/connectors/destinations/grpc"
	"github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ingestRecorder struct {
	wallabypb.UnimplementedIngestServiceServer
	mu       sync.Mutex
	requests []*wallabypb.IngestBatchRequest
	meta     []metadata.MD
}

func (r *ingestRecorder) IngestBatch(ctx context.Context, req *wallabypb.IngestBatchRequest) (*wallabypb.IngestBatchResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	r.mu.Lock()
	r.requests = append(r.requests, req)
	r.meta = append(r.meta, md)
	r.mu.Unlock()
	return &wallabypb.IngestBatchResponse{Accepted: true}, nil
}

func TestGRPCDestinationModes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	recorder := &ingestRecorder{}
	server := grpc.NewServer()
	wallabypb.RegisterIngestServiceServer(server, recorder)
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.GracefulStop()

	target := listener.Addr().String()

	t.Run("wire", func(t *testing.T) {
		recorder.mu.Lock()
		recorder.requests = nil
		recorder.meta = nil
		recorder.mu.Unlock()

		dest := &grpcdest.Destination{}
		spec := connector.Spec{
			Name: "grpc-wire",
			Type: connector.EndpointGRPC,
			Options: map[string]string{
				"endpoint": target,
				"format":   "json",
			},
		}
		if err := dest.Open(ctx, spec); err != nil {
			t.Fatalf("open dest: %v", err)
		}
		defer dest.Close(ctx)

		schema := connector.Schema{Name: "items"}
		batch := connector.Batch{Records: []connector.Record{
			{Table: "items", Operation: connector.OpInsert, After: map[string]any{"id": 1}},
			{Table: "items", Operation: connector.OpInsert, After: map[string]any{"id": 2}},
		}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "1"}}
		if err := dest.Write(ctx, batch); err != nil {
			t.Fatalf("write: %v", err)
		}

		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		if len(recorder.requests) != 1 {
			t.Fatalf("expected 1 request, got %d", len(recorder.requests))
		}
		req := recorder.requests[0]
		if req.WireFormat != wallabypb.WireFormat_WIRE_FORMAT_JSON {
			t.Fatalf("unexpected wire format: %v", req.WireFormat)
		}
		if len(req.Payload) == 0 {
			t.Fatalf("expected payload")
		}
		if mode := recorder.meta[0].Get("x-wallaby-payload-mode"); len(mode) == 0 || mode[0] != "wire" {
			t.Fatalf("expected payload_mode=wire, got %v", mode)
		}
	})

	t.Run("record_json", func(t *testing.T) {
		recorder.mu.Lock()
		recorder.requests = nil
		recorder.meta = nil
		recorder.mu.Unlock()

		dest := &grpcdest.Destination{}
		spec := connector.Spec{
			Name: "grpc-record",
			Type: connector.EndpointGRPC,
			Options: map[string]string{
				"endpoint":     target,
				"payload_mode": "record_json",
			},
		}
		if err := dest.Open(ctx, spec); err != nil {
			t.Fatalf("open dest: %v", err)
		}
		defer dest.Close(ctx)

		schema := connector.Schema{Name: "items"}
		batch := connector.Batch{Records: []connector.Record{
			{Table: "items", Operation: connector.OpInsert, After: map[string]any{"id": 1}},
			{Table: "items", Operation: connector.OpInsert, After: map[string]any{"id": 2}},
		}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "2"}}
		if err := dest.Write(ctx, batch); err != nil {
			t.Fatalf("write: %v", err)
		}

		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		if len(recorder.requests) != 2 {
			t.Fatalf("expected 2 requests, got %d", len(recorder.requests))
		}
		for i, req := range recorder.requests {
			var payload map[string]any
			if err := json.Unmarshal(req.Payload, &payload); err != nil {
				t.Fatalf("decode payload %d: %v", i, err)
			}
			if payload["table"] != "items" {
				t.Fatalf("unexpected table %v", payload["table"])
			}
			if mode := recorder.meta[i].Get("x-wallaby-payload-mode"); len(mode) == 0 || mode[0] != "record_json" {
				t.Fatalf("expected payload_mode=record_json, got %v", mode)
			}
		}
	})

	t.Run("wal", func(t *testing.T) {
		recorder.mu.Lock()
		recorder.requests = nil
		recorder.meta = nil
		recorder.mu.Unlock()

		dest := &grpcdest.Destination{}
		spec := connector.Spec{
			Name: "grpc-wal",
			Type: connector.EndpointGRPC,
			Options: map[string]string{
				"endpoint":     target,
				"payload_mode": "wal",
			},
		}
		if err := dest.Open(ctx, spec); err != nil {
			t.Fatalf("open dest: %v", err)
		}
		defer dest.Close(ctx)

		schema := connector.Schema{Name: "items"}
		wal := []byte("wal-bytes")
		batch := connector.Batch{Records: []connector.Record{
			{Table: "items", Operation: connector.OpInsert, Payload: wal},
		}, Schema: schema, Checkpoint: connector.Checkpoint{LSN: "3"}}
		if err := dest.Write(ctx, batch); err != nil {
			t.Fatalf("write: %v", err)
		}

		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		if len(recorder.requests) != 1 {
			t.Fatalf("expected 1 request, got %d", len(recorder.requests))
		}
		if string(recorder.requests[0].Payload) != string(wal) {
			t.Fatalf("unexpected wal payload")
		}
		if mode := recorder.meta[0].Get("x-wallaby-payload-mode"); len(mode) == 0 || mode[0] != "wal" {
			t.Fatalf("expected payload_mode=wal, got %v", mode)
		}
	})
}
