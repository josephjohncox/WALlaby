package kafka

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	"github.com/josephjohncox/wallaby/pkg/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

type recordingProducer struct {
	records    []*kgo.Record
	begins     int
	ends       []kgo.TransactionEndTry
	closeCalls int
}

func (p *recordingProducer) BeginTransaction() error {
	p.begins++
	return nil
}
func (p *recordingProducer) EndTransaction(_ context.Context, mode kgo.TransactionEndTry) error {
	p.ends = append(p.ends, mode)
	return nil
}
func (p *recordingProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	p.records = append(p.records, records...)
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i].Record = record
	}
	return results
}
func (p *recordingProducer) Close() { p.closeCalls++ }

type closeTrackingRegistry struct {
	closeCalls int
	closeErr   error
}

func (*closeTrackingRegistry) Register(context.Context, schemaregistry.RegisterRequest) (schemaregistry.RegisterResult, error) {
	return schemaregistry.RegisterResult{}, nil
}

func (r *closeTrackingRegistry) Close() error {
	r.closeCalls++
	return r.closeErr
}

func TestOpenRejectsRegistryOptionsBeforeClientCreation(t *testing.T) {
	for key, value := range map[string]string{
		schemaregistry.OptRegistryTimeout:        "soon",
		schemaregistry.OptRegistryApicurioCompat: "yes",
	} {
		t.Run(key, func(t *testing.T) {
			clientCalls := 0
			registryCalls := 0
			factories := destinationFactories{
				newClient: func(...kgo.Opt) (producer, error) {
					clientCalls++
					return &recordingProducer{}, nil
				},
				newRegistry: func(context.Context, schemaregistry.Config) (schemaregistry.Registry, error) {
					registryCalls++
					return nil, nil
				},
			}
			err := (&Destination{}).open(context.Background(), connector.Spec{Type: connector.EndpointKafka, Options: map[string]string{
				optBrokers: "localhost:9092",
				optTopic:   "events",
				optFormat:  string(connector.WireFormatJSON),
				key:        value,
			}}, factories)
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("open() error = %v", err)
			}
			if clientCalls != 0 || registryCalls != 0 {
				t.Fatalf("side effects before config error: client=%d registry=%d", clientCalls, registryCalls)
			}
		})
	}
}

func TestOpenRegistryFailureClosesLocalResources(t *testing.T) {
	client := &recordingProducer{}
	closeErr := errors.New("registry close failed")
	registry := &closeTrackingRegistry{closeErr: closeErr}
	registryErr := errors.New("registry creation failed")
	factories := destinationFactories{
		newClient: func(...kgo.Opt) (producer, error) { return client, nil },
		newRegistry: func(context.Context, schemaregistry.Config) (schemaregistry.Registry, error) {
			return registry, registryErr
		},
	}
	destination := &Destination{}
	err := destination.open(context.Background(), connector.Spec{Type: connector.EndpointKafka, Options: map[string]string{
		optBrokers: "localhost:9092",
		optTopic:   "events",
		optFormat:  string(connector.WireFormatAvro),
	}}, factories)
	if !errors.Is(err, registryErr) || !errors.Is(err, closeErr) {
		t.Fatalf("open() error = %v", err)
	}
	if client.closeCalls != 1 || registry.closeCalls != 1 {
		t.Fatalf("cleanup calls: client=%d registry=%d", client.closeCalls, registry.closeCalls)
	}
	if destination.client != nil || destination.registry != nil {
		t.Fatalf("destination retained failed resources: client=%v registry=%v", destination.client, destination.registry)
	}
}

func TestAppendTransportBuildsKafkaRecord(t *testing.T) {
	producer := &recordingProducer{}
	destination := &Destination{client: producer, topic: "mapped-events", codec: &wire.JSONCodec{}, messageMode: "batch", keyMode: "hash"}
	batch := connector.Batch{
		Schema:      connector.Schema{Namespace: "mapped", Name: "events", Columns: []connector.Column{{Name: "event_id", Type: "int8"}}},
		Checkpoint:  connector.Checkpoint{LSN: "0/40"},
		WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend},
		Records:     []connector.Record{{Table: "events", Operation: connector.OpInsert, After: map[string]any{"event_id": int64(7)}}},
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(producer.records) != 1 {
		t.Fatalf("records=%d", len(producer.records))
	}
	record := producer.records[0]
	if record.Topic != "mapped-events" || !strings.Contains(string(record.Value), `"event_id":7`) || !strings.Contains(string(record.Value), `"Namespace":"mapped"`) {
		t.Fatalf("record topic=%q key=%x value=%s", record.Topic, record.Key, record.Value)
	}
}

func TestClosedCapabilityProfilesDeclareEveryField(t *testing.T) {
	tests := []struct {
		id      connector.CapabilityProfileID
		options map[string]string
		want    connector.Capabilities
	}{
		{id: CapabilityProfileBase, options: map[string]string{optTransactionalProducer: "false", optAllowOversizeSkip: "false"}, want: exactKafkaCapabilities(false, false)},
		{id: CapabilityProfileTransactionalOnly, options: map[string]string{optTransactionalProducer: "true", optTxnID: "test-transaction", optAllowOversizeSkip: "false"}, want: exactKafkaCapabilities(true, false)},
		{id: CapabilityProfileLossyOnly, options: map[string]string{optTransactionalProducer: "false", optAllowOversizeSkip: "true"}, want: exactKafkaCapabilities(false, true)},
		{id: CapabilityProfileTransactionalLossy, options: map[string]string{optTransactionalProducer: "true", optTxnID: "test-transaction", optAllowOversizeSkip: "true"}, want: exactKafkaCapabilities(true, true)},
	}
	destination := &Destination{}
	for _, test := range tests {
		t.Run(string(test.id), func(t *testing.T) {
			spec := connector.Spec{Type: connector.EndpointKafka, Options: test.options}
			profileID, err := destination.ClassifyCapabilityProfile(spec)
			if err != nil {
				t.Fatal(err)
			}
			if profileID != test.id {
				t.Fatalf("profile=%q want=%q", profileID, test.id)
			}
			capabilities, err := destination.CapabilitiesFor(spec)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(capabilities, test.want) {
				t.Fatalf("capabilities=\n%+v\nwant=\n%+v", capabilities, test.want)
			}
		})
	}
}

func exactKafkaCapabilities(transactional, lossy bool) connector.Capabilities {
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
		{optTransactionalProducer: "TRUE"},
		{optAllowOversizeSkip: "1"},
		{optTransactionalProducer: "true"},
		{optTxnID: "unclassified-transaction"},
	}
	for _, options := range tests {
		if _, err := (&Destination{}).ClassifyCapabilityProfile(connector.Spec{Options: options}); err == nil {
			t.Fatalf("options=%v unexpectedly classified", options)
		}
	}
}

func TestCapabilitiesForDeliveryOptions(t *testing.T) {
	spec := connector.Spec{Options: map[string]string{optTransactionalProducer: "true", optTxnID: "wallaby-flow", optAllowOversizeSkip: "true"}}
	producer := &recordingProducer{}
	destination := &Destination{client: producer, topic: "events", codec: &wire.JSONCodec{}, transactional: true, oversizePolicy: "drop", messageMode: "batch"}
	capabilities, err := destination.CapabilitiesFor(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.Delivery.TransactionalBatch || !capabilities.Delivery.Lossy || capabilities.Delivery.IdempotentReplay || capabilities.Delivery.ReplaySafe {
		t.Fatalf("configured Kafka capabilities=%+v", capabilities.Delivery)
	}
	batch := connector.Batch{Schema: connector.Schema{Name: "events"}, WritePolicy: connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}, Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, After: map[string]any{"id": int64(1)}}}}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if producer.begins != 1 || len(producer.ends) != 1 || producer.ends[0] != kgo.TryCommit || len(producer.records) != 1 {
		t.Fatalf("transactional transport begins=%d ends=%v records=%d", producer.begins, producer.ends, len(producer.records))
	}

	lossyProducer := &recordingProducer{}
	lossy := &Destination{client: lossyProducer, topic: "events", codec: &wire.JSONCodec{}, oversizePolicy: "drop", messageMode: "batch", maxRecordSize: 1}
	if err := lossy.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(lossyProducer.records) != 0 {
		t.Fatalf("lossy oversize transport produced %d records", len(lossyProducer.records))
	}
}
