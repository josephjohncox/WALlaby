package redpanda

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaBackedProfilesProduceCommittedAppendRecords(t *testing.T) {
	profiles := []struct {
		id            connector.CapabilityProfileID
		transactional bool
		lossy         bool
	}{
		{id: CapabilityProfileBase},
		{id: CapabilityProfileTransactionalOnly, transactional: true},
		{id: CapabilityProfileLossyOnly, lossy: true},
		{id: CapabilityProfileTransactionalLossy, transactional: true, lossy: true},
	}
	for _, profile := range profiles {
		t.Run(string(profile.id), func(t *testing.T) {
			topic := "events-" + string(profile.id)
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
			defer cluster.Close()
			options := map[string]string{
				"brokers": strings.Join(cluster.ListenAddrs(), ","), "topic": topic,
				"format": "json", "message_mode": "record",
				"transactional_producer": strconv.FormatBool(profile.transactional),
				"allow_oversize_skip":    strconv.FormatBool(profile.lossy),
			}
			if profile.transactional {
				options["transactional_id"] = "wallaby-" + string(profile.id)
			}
			spec := connector.Spec{Name: "redpanda", Type: connector.EndpointRedpanda, Options: options}
			destination := &Destination{}
			if err := destination.Open(context.Background(), spec); err != nil {
				t.Fatal(err)
			}
			defer destination.Close(context.Background())
			capabilities, err := destination.CapabilitiesFor(spec)
			if err != nil {
				t.Fatal(err)
			}
			want := exactRedpandaCapabilities(profile.transactional, profile.lossy)
			if !reflect.DeepEqual(capabilities, want) {
				t.Fatalf("capabilities=\n%+v\nwant=\n%+v", capabilities, want)
			}
			batch := connector.Batch{
				Schema:     connector.Schema{Namespace: "mapped", Name: "events", Columns: []connector.Column{{Name: "event_id", Type: "int8"}}},
				Checkpoint: connector.Checkpoint{LSN: "0/50"}, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
				Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, SourcePosition: "0/50", After: map[string]any{"event_id": int64(7)}}},
			}
			if err := destination.Write(context.Background(), batch); err != nil {
				t.Fatal(err)
			}
			consumer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fetches := consumer.PollRecords(ctx, 1)
			if errs := fetches.Errors(); len(errs) > 0 {
				t.Fatal(errs[0])
			}
			if fetches.NumRecords() != 1 {
				t.Fatalf("committed records=%d", fetches.NumRecords())
			}
			record := fetches.Records()[0]
			if record.Topic != topic || !strings.Contains(string(record.Value), `"event_id":7`) || headerValue(record.Headers, "wallaby-table") != "events" || headerValue(record.Headers, "wallaby-op") != "insert" || headerValue(record.Headers, "wallaby-lsn") != "0/50" {
				t.Fatalf("record=%+v value=%s", record, record.Value)
			}
			upsert := connector.TableWritePolicy{Mode: connector.ResolvedWriteUpsert, KeyColumns: []string{"event_id"}}
			if err := stream.ValidateDestinationTablePolicy(stream.DestinationConfig{Spec: spec, Dest: destination}, upsert); err == nil {
				t.Fatal("upsert passed pre-I/O validation")
			}
		})
	}
}

func TestKafkaBackedProfilesEnforceOversizePolicy(t *testing.T) {
	for _, test := range []struct {
		name      string
		policy    string
		wantError bool
	}{{name: "base", policy: "false", wantError: true}, {name: "lossy-only", policy: "true"}} {
		t.Run(test.name, func(t *testing.T) {
			topic := "oversize-" + test.name
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
			defer cluster.Close()
			spec := connector.Spec{Name: "redpanda", Type: connector.EndpointRedpanda, Options: map[string]string{
				"brokers": strings.Join(cluster.ListenAddrs(), ","), "topic": topic, "format": "json", "message_mode": "record",
				"max_record_bytes": "1", "allow_oversize_skip": test.policy,
			}}
			destination := &Destination{}
			if err := destination.Open(context.Background(), spec); err != nil {
				t.Fatal(err)
			}
			defer destination.Close(context.Background())
			batch := connector.Batch{Schema: connector.Schema{Name: "events"}, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}, Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, After: map[string]any{"payload": strings.Repeat("x", 128)}}}}
			err := destination.Write(context.Background(), batch)
			if (err != nil) != test.wantError {
				t.Fatalf("Write error=%v wantError=%t", err, test.wantError)
			}
		})
	}
}

func exactRedpandaCapabilities(transactional, lossy bool) connector.Capabilities {
	return connector.Capabilities{
		Support:               connector.SupportExperimental,
		Evidence:              connector.ContractEvidence{Restart: false, Replay: false, SchemaEvolution: false, Integration: false},
		Delivery:              connector.DeliverySemantics{TransactionalBatch: transactional, IdempotentReplay: false, ReplaySafe: false, ExecutesDDL: false, Lossy: lossy},
		TableWrites:           connector.TableWriteSemantics{Append: true, Upsert: false, ExplicitKey: false, WatermarkGuard: false},
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsBulkLoad:      false,
		SupportsTypeMapping:   true,
		SupportedWireFormats:  []connector.WireFormat{connector.WireFormatArrow, connector.WireFormatAvro, connector.WireFormatProto, connector.WireFormatJSON},
	}
}

func TestCapabilityProfileClassifierRejectsInvalidBooleansAndTransactions(t *testing.T) {
	tests := []map[string]string{
		{"transactional_producer": "yes"},
		{"allow_oversize_skip": "1"},
		{"transactional_producer": "true"},
		{"transactional_id": "unclassified-transaction"},
	}
	for _, options := range tests {
		if _, err := (&Destination{}).ClassifyCapabilityProfile(connector.Spec{Options: options}); err == nil {
			t.Fatalf("options=%v unexpectedly classified", options)
		}
	}
}

func headerValue(headers []kgo.RecordHeader, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
