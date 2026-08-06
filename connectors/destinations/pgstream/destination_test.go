package pgstream

import (
	"context"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	storepkg "github.com/josephjohncox/wallaby/pkg/pgstream"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	"github.com/josephjohncox/wallaby/pkg/wire"
)

type recordingMessageStore struct {
	stream   string
	messages []storepkg.Message
}

func (s *recordingMessageStore) Enqueue(_ context.Context, stream string, messages []storepkg.Message) error {
	s.stream = stream
	s.messages = append(s.messages, messages...)
	return nil
}
func (*recordingMessageStore) Close() {}

func TestOpenRejectsRegistryOptionsBeforeStoreCreation(t *testing.T) {
	for key, value := range map[string]string{
		schemaregistry.OptRegistryTimeout:        "soon",
		schemaregistry.OptRegistryApicurioCompat: "yes",
	} {
		t.Run(key, func(t *testing.T) {
			err := (&Destination{}).Open(context.Background(), connector.Spec{Options: map[string]string{key: value}})
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("Open() error = %v", err)
			}
			if strings.Contains(err.Error(), "dsn is required") {
				t.Fatalf("registry config was not parsed first: %v", err)
			}
		})
	}
}

func TestWriteEnqueuesMappedAppendMessage(t *testing.T) {
	store := &recordingMessageStore{}
	destination := &Destination{store: store, stream: "flow-events", codec: &wire.JSONCodec{}}
	batch := connector.Batch{
		Schema:      connector.Schema{Namespace: "mapped", Name: "events", Columns: []connector.Column{{Name: "event_id", Type: "int8"}}},
		Checkpoint:  connector.Checkpoint{LSN: "0/30"},
		WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
		Records:     []connector.Record{{Table: "events", Operation: connector.OpInsert, After: map[string]any{"event_id": int64(7)}}},
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if store.stream != "flow-events" || len(store.messages) != 1 {
		t.Fatalf("stream=%q messages=%d", store.stream, len(store.messages))
	}
	message := store.messages[0]
	if message.Namespace != "mapped" || message.Table != "events" || message.LSN != "0/30" || message.WireFormat != connector.WireFormatJSON {
		t.Fatalf("message=%+v", message)
	}
	if payload := string(message.Payload); !strings.Contains(payload, `"event_id":7`) || !strings.Contains(payload, `"Namespace":"mapped"`) {
		t.Fatalf("payload=%s", payload)
	}
}
