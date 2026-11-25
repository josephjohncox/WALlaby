package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	iggydest "github.com/josephjohncox/wallaby/connectors/destinations/iggy"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestIggyDestinationWireFormats(t *testing.T) {
	address := os.Getenv("WALLABY_TEST_IGGY_ADDRESS")
	if address == "" {
		t.Skip("WALLABY_TEST_IGGY_ADDRESS not set")
	}
	username, password := os.Getenv("WALLABY_TEST_IGGY_USERNAME"), os.Getenv("WALLABY_TEST_IGGY_PASSWORD")
	if username == "" || password == "" {
		t.Fatal("Iggy integration requires explicit WALLABY_TEST_IGGY_USERNAME and WALLABY_TEST_IGGY_PASSWORD")
	}
	tlsEnabled := true
	if raw := os.Getenv("WALLABY_TEST_IGGY_TLS"); raw != "" {
		var err error
		tlsEnabled, err = strconv.ParseBool(raw)
		if err != nil {
			t.Fatal("invalid WALLABY_TEST_IGGY_TLS")
		}
	}
	options := []tcp.Option{tcp.WithServerAddress(address), tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials(username, password))}
	if tlsEnabled {
		options = append(options, tcp.WithTLS(tcp.WithTLSCAFile(os.Getenv("WALLABY_TEST_IGGY_TLS_CA_FILE")), tcp.WithTLSDomain(os.Getenv("WALLABY_TEST_IGGY_TLS_SERVER_NAME"))))
	}
	admin, err := client.NewIggyClient(client.WithTcp(options...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := admin.Connect(ctx); err != nil {
		t.Fatalf("connect to native Iggy server: %v", err)
	}
	stream := fmt.Sprintf("wallaby_test_%d", time.Now().UnixNano())
	if _, err := admin.CreateStream(ctx, stream, nil); err != nil {
		t.Fatalf("create isolated Iggy stream: %v", err)
	}
	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := admin.DeleteStream(cleanupCtx, streamID); err != nil {
			t.Errorf("clean up isolated Iggy stream: %v", err)
		}
	})
	for _, format := range []string{"json", "arrow", "avro", "proto"} {
		t.Run(format, func(t *testing.T) {
			durability := iggcon.DurabilityPersisted
			if _, err := admin.CreateTopic(ctx, streamID, format, &iggcon.TopicOptions{Durability: &durability}); err != nil {
				t.Fatal(err)
			}
			spec := connector.RuntimeSpec{Type: connector.EndpointIggy, Options: map[string]string{
				"address": address, "stream": stream, "topic": format, "partition_id": "0", "format": format,
				"username": username, "password": password, "tls": strconv.FormatBool(tlsEnabled),
			}}
			if tlsEnabled {
				if ca := os.Getenv("WALLABY_TEST_IGGY_TLS_CA_FILE"); ca != "" {
					spec.Options["tls_ca_file"] = ca
				}
				if name := os.Getenv("WALLABY_TEST_IGGY_TLS_SERVER_NAME"); name != "" {
					spec.Options["tls_server_name"] = name
				}
			}
			destination := &iggydest.Destination{}
			if err := destination.Open(ctx, spec); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = destination.Close(context.Background()) })
			batch := connector.Batch{
				Schema:     connector.Schema{Name: "orders", Namespace: "public", Version: 1, Columns: []connector.Column{{Name: "id", Type: "int8"}, {Name: "status", Type: "text"}}},
				Records:    []connector.Record{{Table: "public.orders", Operation: connector.OpInsert, SchemaVersion: 1, After: map[string]any{"id": int64(1), "status": "paid"}}},
				Checkpoint: connector.Checkpoint{LSN: "0/1"},
			}
			if err := destination.Write(ctx, batch); err != nil {
				t.Fatal(err)
			}
			topicID, err := iggcon.NewIdentifier(format)
			if err != nil {
				t.Fatal(err)
			}
			polled, err := admin.PollMessages(ctx, streamID, topicID, 0, iggcon.DefaultConsumer(), iggcon.OffsetPollingStrategy(0), iggcon.WaitPollOptions(5*time.Second))
			if err != nil || len(polled.Messages) != 1 {
				t.Fatalf("native Iggy readback error=%v", err)
			}
			message := polled.Messages[0]
			switch format {
			case "json":
				var got connector.Batch
				if err := json.Unmarshal(message.Payload, &got); err != nil || len(got.Records) != 1 || got.Records[0].After["status"] != "paid" {
					t.Fatalf("JSON readback err=%v batch=%+v", err, got)
				}
			case "arrow":
				assertArrowPayload(t, message.Payload)
			case "avro":
				assertAvroPayload(t, message.Payload)
			case "proto":
				assertProtoPayload(t, message.Payload)
			}
			headers, err := iggcon.DeserializeHeaders(message.UserHeaders)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, header := range headers {
				if string(header.Key.Value) == "wallaby-format" && string(header.Value.Value) == format {
					found = true
				}
			}
			if !found {
				t.Fatal("native Iggy message lost its format header")
			}
		})
	}
}
