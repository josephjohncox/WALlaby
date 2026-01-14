package tests

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/hamba/avro/v2/ocf"
	kafkadest "github.com/josephjohncox/wallaby/connectors/destinations/kafka"
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func TestKafkaDestinationJSON(t *testing.T) {
	brokersRaw := strings.TrimSpace(os.Getenv("WALLABY_TEST_KAFKA_BROKERS"))
	if brokersRaw == "" {
		t.Skip("WALLABY_TEST_KAFKA_BROKERS not set")
	}
	brokers := splitCSV(brokersRaw)
	if len(brokers) == 0 {
		t.Skip("no kafka brokers configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	topic := fmt.Sprintf("wallaby_test_%d", time.Now().UnixNano())
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("create kafka client: %v", err)
	}
	defer adminClient.Close()

	admin := kadm.NewClient(adminClient)
	if err := ensureKafkaTopic(ctx, admin, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	dest := &kafkadest.Destination{}
	spec := connector.Spec{
		Name: "kafka-test",
		Type: connector.EndpointKafka,
		Options: map[string]string{
			"brokers": brokersRaw,
			"topic":   topic,
			"format":  "json",
			"acks":    "all",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	now := time.Now().UTC()
	batch := connector.Batch{
		Schema: connector.Schema{
			Name:      "orders",
			Namespace: "public",
			Version:   1,
			Columns: []connector.Column{
				{Name: "id", Type: "int8"},
				{Name: "status", Type: "text"},
			},
		},
		Checkpoint: connector.Checkpoint{LSN: "0/1", Timestamp: now},
		WireFormat: connector.WireFormatJSON,
		Records: []connector.Record{
			{
				Table:         "public.orders",
				Operation:     connector.OpInsert,
				SchemaVersion: 1,
				Key:           []byte(`{"id":1}`),
				After: map[string]any{
					"id":     1,
					"status": "paid",
				},
				Timestamp: now,
			},
		},
	}

	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write kafka batch: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create kafka consumer: %v", err)
	}
	defer consumer.Close()

	record, err := waitForKafkaRecord(ctx, consumer, topic)
	if err != nil {
		t.Fatalf("consume kafka record: %v", err)
	}

	var got connector.Batch
	if err := json.Unmarshal(record.Value, &got); err != nil {
		t.Fatalf("decode kafka payload: %v", err)
	}
	if len(got.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got.Records))
	}
	if got.Schema.Name != "orders" || got.Schema.Namespace != "public" {
		t.Fatalf("unexpected schema: %+v", got.Schema)
	}
	if got.Records[0].Operation != connector.OpInsert {
		t.Fatalf("unexpected operation: %s", got.Records[0].Operation)
	}

	if headerValue(record.Headers, "wallaby-format") != "json" {
		t.Fatalf("missing wallaby-format header: %v", record.Headers)
	}
	if headerValue(record.Headers, "wallaby-schema") != "orders" {
		t.Fatalf("missing wallaby-schema header: %v", record.Headers)
	}
}

func TestKafkaDestinationWireFormats(t *testing.T) {
	brokersRaw := strings.TrimSpace(os.Getenv("WALLABY_TEST_KAFKA_BROKERS"))
	if brokersRaw == "" {
		t.Skip("WALLABY_TEST_KAFKA_BROKERS not set")
	}
	brokers := splitCSV(brokersRaw)
	if len(brokers) == 0 {
		t.Skip("no kafka brokers configured")
	}

	tests := []struct {
		name   string
		format string
		check  func(t *testing.T, payload []byte)
	}{
		{name: "avro", format: "avro", check: assertAvroPayload},
		{name: "arrow", format: "arrow", check: assertArrowPayload},
		{name: "proto", format: "proto", check: assertProtoPayload},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			topic := fmt.Sprintf("wallaby_test_%s_%d", tc.name, time.Now().UnixNano())
			adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
			if err != nil {
				t.Fatalf("create kafka client: %v", err)
			}
			defer adminClient.Close()

			admin := kadm.NewClient(adminClient)
			if err := ensureKafkaTopic(ctx, admin, topic); err != nil {
				t.Fatalf("create topic: %v", err)
			}

			dest := &kafkadest.Destination{}
			spec := connector.Spec{
				Name: "kafka-test-" + tc.name,
				Type: connector.EndpointKafka,
				Options: map[string]string{
					"brokers": brokersRaw,
					"topic":   topic,
					"format":  tc.format,
					"acks":    "all",
				},
			}
			if err := dest.Open(ctx, spec); err != nil {
				t.Fatalf("open destination: %v", err)
			}
			defer dest.Close(ctx)

			batch := sampleBatch(wireFormatFor(tc.format))
			if err := dest.Write(ctx, batch); err != nil {
				t.Fatalf("write kafka batch: %v", err)
			}

			consumer, err := kgo.NewClient(
				kgo.SeedBrokers(brokers...),
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			if err != nil {
				t.Fatalf("create kafka consumer: %v", err)
			}
			defer consumer.Close()

			record, err := waitForKafkaRecord(ctx, consumer, topic)
			if err != nil {
				t.Fatalf("consume kafka record: %v", err)
			}

			if headerValue(record.Headers, "wallaby-format") != tc.format {
				t.Fatalf("expected wallaby-format %s, got %s", tc.format, headerValue(record.Headers, "wallaby-format"))
			}
			tc.check(t, record.Value)
		})
	}
}

func TestKafkaDestinationRecordModeHeaders(t *testing.T) {
	brokersRaw := strings.TrimSpace(os.Getenv("WALLABY_TEST_KAFKA_BROKERS"))
	if brokersRaw == "" {
		t.Skip("WALLABY_TEST_KAFKA_BROKERS not set")
	}
	brokers := splitCSV(brokersRaw)
	if len(brokers) == 0 {
		t.Skip("no kafka brokers configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	topic := fmt.Sprintf("wallaby_test_record_%d", time.Now().UnixNano())
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("create kafka client: %v", err)
	}
	defer adminClient.Close()

	admin := kadm.NewClient(adminClient)
	if err := ensureKafkaTopic(ctx, admin, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	dest := &kafkadest.Destination{}
	spec := connector.Spec{
		Name: "kafka-test-record",
		Type: connector.EndpointKafka,
		Options: map[string]string{
			"brokers":            brokersRaw,
			"topic":              topic,
			"format":             "json",
			"acks":               "all",
			"message_mode":       "record",
			"key_mode":           "hash",
			"transaction_header": "wallaby-transaction-id",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	now := time.Now().UTC()
	batch := connector.Batch{
		Schema: connector.Schema{
			Name:      "orders",
			Namespace: "public",
			Version:   1,
			Columns: []connector.Column{
				{Name: "id", Type: "int8"},
				{Name: "status", Type: "text"},
			},
		},
		Checkpoint: connector.Checkpoint{LSN: "0/42", Timestamp: now},
		WireFormat: connector.WireFormatJSON,
		Records: []connector.Record{
			{
				Table:         "public.orders",
				Operation:     connector.OpInsert,
				SchemaVersion: 1,
				Key:           []byte(`{"id":42}`),
				After: map[string]any{
					"id":     42,
					"status": "paid",
				},
				Timestamp: now,
			},
		},
	}

	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write kafka batch: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create kafka consumer: %v", err)
	}
	defer consumer.Close()

	record, err := waitForKafkaRecord(ctx, consumer, topic)
	if err != nil {
		t.Fatalf("consume kafka record: %v", err)
	}

	if headerValue(record.Headers, "wallaby-table") != "public.orders" {
		t.Fatalf("missing wallaby-table header: %v", record.Headers)
	}
	if headerValue(record.Headers, "wallaby-op") != string(connector.OpInsert) {
		t.Fatalf("missing wallaby-op header: %v", record.Headers)
	}
	if headerValue(record.Headers, "wallaby-lsn") != "0/42" {
		t.Fatalf("missing wallaby-lsn header: %v", record.Headers)
	}
	if headerValue(record.Headers, "wallaby-transaction-id") != "0/42" {
		t.Fatalf("missing transaction header: %v", record.Headers)
	}
	if len(record.Key) == 0 {
		t.Fatalf("expected deterministic record key")
	}
	expectedHash := hashString(string(batch.Records[0].Key))
	if headerValue(record.Headers, "wallaby-key-hash") != expectedHash {
		t.Fatalf("expected key hash %s, got %s", expectedHash, headerValue(record.Headers, "wallaby-key-hash"))
	}
}

func TestKafkaDestinationSchemaRegistryHeaders(t *testing.T) {
	brokersRaw := strings.TrimSpace(os.Getenv("WALLABY_TEST_KAFKA_BROKERS"))
	if brokersRaw == "" {
		t.Skip("WALLABY_TEST_KAFKA_BROKERS not set")
	}
	brokers := splitCSV(brokersRaw)
	if len(brokers) == 0 {
		t.Skip("no kafka brokers configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	topic := fmt.Sprintf("wallaby_test_registry_%d", time.Now().UnixNano())
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("create kafka client: %v", err)
	}
	defer adminClient.Close()

	admin := kadm.NewClient(adminClient)
	if err := ensureKafkaTopic(ctx, admin, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	dest := &kafkadest.Destination{}
	spec := connector.Spec{
		Name: "kafka-test-registry",
		Type: connector.EndpointKafka,
		Options: map[string]string{
			"brokers":                      brokersRaw,
			"topic":                        topic,
			"format":                       "avro",
			"acks":                         "all",
			"schema_registry":              "local",
			"schema_registry_subject_mode": "topic",
		},
	}
	if err := dest.Open(ctx, spec); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer dest.Close(ctx)

	batch := sampleBatch(connector.WireFormatAvro)
	if err := dest.Write(ctx, batch); err != nil {
		t.Fatalf("write kafka batch: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("create kafka consumer: %v", err)
	}
	defer consumer.Close()

	record, err := waitForKafkaRecord(ctx, consumer, topic)
	if err != nil {
		t.Fatalf("consume kafka record: %v", err)
	}

	if headerValue(record.Headers, "wallaby-registry-subject") == "" {
		t.Fatalf("missing wallaby-registry-subject header")
	}
	if headerValue(record.Headers, "wallaby-registry-id") == "" {
		t.Fatalf("missing wallaby-registry-id header")
	}
}

func waitForKafkaRecord(ctx context.Context, client *kgo.Client, topic string) (*kgo.Record, error) {
	for {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return nil, err
		}
		var record *kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Topic == topic && record == nil {
				record = r
			}
		})
		if record != nil {
			return record, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

func headerValue(headers []kgo.RecordHeader, key string) string {
	for _, header := range headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value)
		}
	}
	return ""
}

func sampleBatch(format connector.WireFormat) connector.Batch {
	now := time.Now().UTC()
	return connector.Batch{
		Schema: connector.Schema{
			Name:      "orders",
			Namespace: "public",
			Version:   1,
			Columns: []connector.Column{
				{Name: "id", Type: "int8"},
				{Name: "status", Type: "text"},
			},
		},
		Checkpoint: connector.Checkpoint{LSN: "0/1", Timestamp: now},
		WireFormat: format,
		Records: []connector.Record{
			{
				Table:         "public.orders",
				Operation:     connector.OpInsert,
				SchemaVersion: 1,
				Key:           []byte(`{"id":1}`),
				After: map[string]any{
					"id":     int64(1),
					"status": "paid",
				},
				Timestamp: now,
			},
		},
	}
}

func wireFormatFor(name string) connector.WireFormat {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "avro":
		return connector.WireFormatAvro
	case "arrow":
		return connector.WireFormatArrow
	case "proto":
		return connector.WireFormatProto
	default:
		return connector.WireFormatJSON
	}
}

func assertAvroPayload(t *testing.T, payload []byte) {
	t.Helper()
	decoder, err := ocf.NewDecoder(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("avro decoder: %v", err)
	}
	if !decoder.HasNext() {
		t.Fatalf("avro decoder has no records")
	}
	row := map[string]any{}
	if err := decoder.Decode(&row); err != nil {
		t.Fatalf("decode avro: %v", err)
	}
	if fmt.Sprint(row["__op"]) != "insert" {
		t.Fatalf("unexpected __op: %v", row["__op"])
	}
	if fmt.Sprint(row["__table"]) != "public.orders" {
		t.Fatalf("unexpected __table: %v", row["__table"])
	}
	if fmt.Sprint(row["__namespace"]) != "public" {
		t.Fatalf("unexpected __namespace: %v", row["__namespace"])
	}
	if asInt64(row["__schema_version"]) != 1 {
		t.Fatalf("unexpected __schema_version: %v", row["__schema_version"])
	}
	if asInt64(row["id"]) != 1 {
		t.Fatalf("unexpected id: %v", row["id"])
	}
	if fmt.Sprint(row["status"]) != "paid" {
		t.Fatalf("unexpected status: %v", row["status"])
	}
}

func assertArrowPayload(t *testing.T, payload []byte) {
	t.Helper()
	reader, err := ipc.NewReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("arrow reader: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		if err := reader.Err(); err != nil {
			t.Fatalf("arrow reader error: %v", err)
		}
		t.Fatalf("no arrow records")
	}

	rec := reader.RecordBatch()
	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
	idx := fieldIndex(rec, "__op")
	if idx < 0 {
		t.Fatalf("missing __op column")
	}
	opCol := rec.Column(idx).(*array.String)
	if opCol.Value(0) != "insert" {
		t.Fatalf("unexpected __op: %s", opCol.Value(0))
	}
	tableIdx := fieldIndex(rec, "__table")
	if tableIdx < 0 {
		t.Fatalf("missing __table column")
	}
	tableCol := rec.Column(tableIdx).(*array.String)
	if tableCol.Value(0) != "public.orders" {
		t.Fatalf("unexpected __table: %s", tableCol.Value(0))
	}
	namespaceIdx := fieldIndex(rec, "__namespace")
	if namespaceIdx < 0 {
		t.Fatalf("missing __namespace column")
	}
	namespaceCol := rec.Column(namespaceIdx).(*array.String)
	if namespaceCol.Value(0) != "public" {
		t.Fatalf("unexpected __namespace: %s", namespaceCol.Value(0))
	}
	idIdx := fieldIndex(rec, "id")
	if idIdx < 0 {
		t.Fatalf("missing id column")
	}
	idCol := rec.Column(idIdx).(*array.Int64)
	if idCol.Value(0) != 1 {
		t.Fatalf("unexpected id: %d", idCol.Value(0))
	}
	statusIdx := fieldIndex(rec, "status")
	if statusIdx < 0 {
		t.Fatalf("missing status column")
	}
	statusCol := rec.Column(statusIdx).(*array.String)
	if statusCol.Value(0) != "paid" {
		t.Fatalf("unexpected status: %s", statusCol.Value(0))
	}
}

func assertProtoPayload(t *testing.T, payload []byte) {
	t.Helper()
	var batch wallabypb.Batch
	if err := proto.Unmarshal(payload, &batch); err != nil {
		t.Fatalf("decode proto: %v", err)
	}
	if batch.Schema == nil || batch.Schema.Name != "orders" || batch.Schema.Namespace != "public" {
		t.Fatalf("unexpected schema: %+v", batch.Schema)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(batch.Records))
	}
	record := batch.Records[0]
	if record.Operation != "insert" {
		t.Fatalf("unexpected operation: %s", record.Operation)
	}
	var after map[string]any
	if err := json.Unmarshal(record.AfterJson, &after); err != nil {
		t.Fatalf("decode after json: %v", err)
	}
	if asInt64(after["id"]) != 1 {
		t.Fatalf("unexpected id: %v", after["id"])
	}
	if fmt.Sprint(after["status"]) != "paid" {
		t.Fatalf("unexpected status: %v", after["status"])
	}
}

func fieldIndex(rec arrow.RecordBatch, name string) int {
	for idx, field := range rec.Schema().Fields() {
		if field.Name == name {
			return idx
		}
	}
	return -1
}

func asInt64(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case uint64:
		return int64(v)
	case uint32:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	default:
		return 0
	}
}

func ensureKafkaTopic(ctx context.Context, admin *kadm.Client, topic string) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
		if err == nil || strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trim := strings.TrimSpace(part)
		if trim != "" {
			out = append(out, trim)
		}
	}
	return out
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
