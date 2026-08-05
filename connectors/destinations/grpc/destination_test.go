package grpc

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/pkg/connector"
	gogrpc "google.golang.org/grpc"
)

type recordingIngestClient struct {
	requests []*wallabypb.IngestBatchRequest
}

func (c *recordingIngestClient) IngestBatch(_ context.Context, request *wallabypb.IngestBatchRequest, _ ...gogrpc.CallOption) (*wallabypb.IngestBatchResponse, error) {
	c.requests = append(c.requests, request)
	return &wallabypb.IngestBatchResponse{Accepted: true}, nil
}

func TestWriteSendsMappedAppendRequest(t *testing.T) {
	client := &recordingIngestClient{}
	destination := &Destination{client: client, payloadMode: payloadModeRecordJSON, timeout: time.Second, flowID: "flow-1", destination: "sink"}
	batch := connector.Batch{
		Schema:      connector.Schema{Namespace: "mapped", Name: "events", Columns: []connector.Column{{Name: "event_id", Type: "int8"}}},
		Checkpoint:  connector.Checkpoint{LSN: "0/20"},
		WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
		Records:     []connector.Record{{Table: "events", Operation: connector.OpInsert, SchemaVersion: 3, After: map[string]any{"event_id": float64(7)}}},
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(client.requests) != 1 {
		t.Fatalf("ingest requests=%d", len(client.requests))
	}
	request := client.requests[0]
	if request.FlowId != "flow-1" || request.Destination != "sink" || request.WireFormat != wallabypb.WireFormat_WIRE_FORMAT_JSON || request.Checkpoint.GetLsn() != "0/20" {
		t.Fatalf("request=%+v", request)
	}
	var payload struct {
		Table     string         `json:"table"`
		Operation string         `json:"operation"`
		After     map[string]any `json:"after"`
	}
	if err := json.Unmarshal(request.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Table != "events" || payload.Operation != "insert" || payload.After["event_id"] != float64(7) {
		t.Fatalf("payload=%+v", payload)
	}
}
