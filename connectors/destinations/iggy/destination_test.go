package iggy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/wire"
)

type fakeProducer struct {
	connectErr  error
	sendErr     error
	resolveErr  error
	abandonErr  error
	abandoned   []uint64
	closeErr    error
	connects    int
	closes      int
	messages    []iggcon.IggyMessage
	stream      iggcon.Identifier
	topic       iggcon.Identifier
	partition   iggcon.Partitioning
	resolved    []uint64
	hadDeadline bool
}

func (p *fakeProducer) Connect(ctx context.Context) error {
	p.connects++
	_, p.hadDeadline = ctx.Deadline()
	return p.connectErr
}
func (p *fakeProducer) SendMessages(_ context.Context, stream, topic iggcon.Identifier, partition iggcon.Partitioning, messages []iggcon.IggyMessage) (*iggcon.SendMessagesResponse, error) {
	p.stream, p.topic, p.partition = stream, topic, partition
	p.messages = append(p.messages, messages...)
	// Empty placements are a valid acknowledgement, not a reason to resend.
	return &iggcon.SendMessagesResponse{}, p.sendErr
}
func (p *fakeProducer) ResolveOperation(_ context.Context, id uint64) error {
	p.resolved = append(p.resolved, id)
	return p.resolveErr
}
func (p *fakeProducer) AbandonOperation(_ context.Context, id uint64) error {
	p.abandoned = append(p.abandoned, id)
	return p.abandonErr
}
func (p *fakeProducer) Close() error { p.closes++; return p.closeErr }

func testSpec() connector.RuntimeSpec {
	return connector.RuntimeSpec{Type: connector.EndpointIggy, Options: map[string]string{
		"address": "localhost:8090", "stream": "cdc", "topic": "orders",
		"username": "test-user", "password": "test-password", "format": "json",
	}}
}

func testBatch() connector.Batch {
	return connector.Batch{
		Schema:     connector.Schema{Name: "orders", Namespace: "public", Version: 1, Columns: []connector.Column{{Name: "id", Type: "int8"}}},
		Checkpoint: connector.Checkpoint{LSN: "0/123"},
		Records:    []connector.Record{{Table: "public.orders", Operation: connector.OpInsert, After: map[string]any{"id": int64(42)}}},
	}
}

func openFake(t *testing.T, spec connector.RuntimeSpec, p *fakeProducer) *Destination {
	t.Helper()
	d := &Destination{}
	if err := d.open(context.Background(), spec, func(destinationConfig) (producer, error) { return p, nil }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close(context.Background()) })
	return d
}

func TestInvalidConfigurationFailsBeforeClientConstruction(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"address", ""}, {"address", "iggy://user:secret@localhost:8090"},
		{"address", "localhost:0"}, {"address", "localhost:65536"},
		{"address", "localhost:abc"}, {"stream", ""}, {"topic", ""},
		{"stream", strings.Repeat("x", 256)}, {"topic", strings.Repeat("x", 256)},
		{"password", ""}, {"username", ""}, {"token", "conflicting-token"},
		{"timeout", "0s"}, {"timeout", "-1s"}, {"timeout", "tomorrow"},
		{"partition_id", "-1"}, {"partition_id", "4294967296"},
		{"max_message_bytes", "0"}, {"max_message_bytes", "64000001"},
		{"format", "parquet"}, {"format", "unknown"}, {"tls", "sometimes"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			spec := testSpec()
			spec.Options[tc.key] = tc.value
			called := false
			err := (&Destination{}).open(context.Background(), spec, func(destinationConfig) (producer, error) {
				called = true
				return &fakeProducer{}, nil
			})
			if err == nil || called {
				t.Fatalf("err=%v client constructed=%v", err, called)
			}
		})
	}
}

func TestPartitionIDUsesFullUint32Range(t *testing.T) {
	for _, test := range []struct {
		value string
		want  uint32
	}{
		{value: "0", want: 0},
		{value: "2147483648", want: 2147483648},
		{value: "4294967295", want: 4294967295},
	} {
		t.Run(test.value, func(t *testing.T) {
			spec := testSpec()
			spec.Options["partition_id"] = test.value
			cfg, err := parseConfig(spec)
			if err != nil || cfg.partition != test.want {
				t.Fatalf("partition=%d err=%v, want %d", cfg.partition, err, test.want)
			}
		})
	}
}

func TestTLSAndAuthenticationConfiguration(t *testing.T) {
	for _, token := range []bool{false, true} {
		spec := testSpec()
		spec.Options["tls_ca_file"] = "ca.pem"
		spec.Options["tls_server_name"] = "iggy.example.test"
		if token {
			delete(spec.Options, "username")
			delete(spec.Options, "password")
			spec.Options["token"] = "test-token"
		}
		p := &fakeProducer{}
		d := &Destination{}
		err := d.open(context.Background(), spec, func(cfg destinationConfig) (producer, error) {
			if !cfg.tls || cfg.tlsCAFile != "ca.pem" || cfg.tlsServerName != "iggy.example.test" || (cfg.token != "") != token {
				t.Fatalf("wrong TLS or authentication selection (token=%v)", token)
			}
			return p, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !p.hadDeadline || d.config.password != "" || d.config.token != "" || d.config.username != "" {
			t.Fatal("missing connect deadline or adapter retained credentials")
		}
		if err := d.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		spec.Options["tls"] = "false"
		if _, err := parseConfig(spec); err == nil {
			t.Fatal("TLS-only options silently accepted on plaintext connection")
		}
	}
}

func TestOpenFailureClosesClientAndAllowsRetry(t *testing.T) {
	failure := errors.New("connection refused")
	p := &fakeProducer{connectErr: failure}
	d := &Destination{}
	factory := func(destinationConfig) (producer, error) { return p, nil }
	if err := d.open(context.Background(), testSpec(), factory); !errors.Is(err, failure) || p.closes != 1 || d.client != nil {
		t.Fatalf("err=%v closes=%d client=%v", err, p.closes, d.client)
	}
	p.connectErr = nil
	if err := d.open(context.Background(), testSpec(), factory); err != nil {
		t.Fatal(err)
	}
	if err := d.open(context.Background(), testSpec(), factory); err == nil || p.connects != 2 {
		t.Fatal("double open leaked or replaced the existing client")
	}
	_ = d.Close(context.Background())
	_ = d.Close(context.Background())
	if p.closes != 2 {
		t.Fatalf("close is not idempotent: calls=%d", p.closes)
	}
	if err := d.Write(context.Background(), testBatch()); err == nil {
		t.Fatal("write after close succeeded")
	}
}

func TestConstructorFailureClosesPartiallyCreatedClient(t *testing.T) {
	failure := errors.New("create failed")
	p := &fakeProducer{}
	err := (&Destination{}).open(context.Background(), testSpec(), func(destinationConfig) (producer, error) { return p, failure })
	if !errors.Is(err, failure) || p.closes != 1 || p.connects != 0 {
		t.Fatalf("error=%v closes=%d connects=%d", err, p.closes, p.connects)
	}
}

func TestWriteWireFormatsAndRouting(t *testing.T) {
	for _, format := range []string{"json", "arrow", "avro", "proto"} {
		t.Run(format, func(t *testing.T) {
			spec := testSpec()
			spec.Options["format"], spec.Options["partition_id"] = format, "0"
			p := &fakeProducer{}
			d := openFake(t, spec, p)
			batch := testBatch()
			if err := d.Write(context.Background(), batch); err != nil {
				t.Fatal(err)
			}
			codec, err := wire.NewCodec(format)
			if err != nil {
				t.Fatal(err)
			}
			want, err := codec.Encode(batch)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.messages) != 1 {
				t.Fatal("did not send exactly one complete encoded batch")
			}
			if format == "avro" {
				// OCF sync markers are random; compare decoded records instead
				// of requiring a second encoding to be byte-identical.
				decoder, err := ocf.NewDecoder(bytes.NewReader(p.messages[0].Payload))
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				for decoder.HasNext() {
					var row map[string]any
					if err := decoder.Decode(&row); err != nil {
						t.Fatal(err)
					}
					if row["id"] != int64(42) || row["__op"] != "insert" {
						t.Fatalf("avro row=%v", row)
					}
					count++
				}
				if decoder.Error() != nil || count != len(batch.Records) {
					t.Fatalf("avro records=%d err=%v", count, decoder.Error())
				}
			} else if !bytes.Equal(p.messages[0].Payload, want) {
				t.Fatal("encoded batch changed before sending")
			}
			if string(p.stream.Value()) != "cdc" || string(p.topic.Value()) != "orders" || p.partition.Kind != iggcon.PartitionIdKind || binary.LittleEndian.Uint32(p.partition.Value) != 0 {
				t.Fatal("incorrect stream, topic or zero-based partition")
			}
			headers, err := iggcon.DeserializeHeaders(p.messages[0].UserHeaders)
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]string{}
			for _, h := range headers {
				got[string(h.Key.Value)] = string(h.Value.Value)
			}
			hash, err := connector.BatchContentHash(batch)
			if err != nil {
				t.Fatal(err)
			}
			if !maps.Equal(got, map[string]string{"wallaby-format": format, "wallaby-content-type": codec.ContentType(), "wallaby-batch-hash": hash}) {
				t.Fatalf("headers=%v", got)
			}
			if err := d.Write(context.Background(), batch); err != nil || len(p.messages) != 2 || p.messages[0].Header.Id != p.messages[1].Header.Id {
				t.Fatalf("message identity changed on replay: err=%v", err)
			}
		})
	}
}

func TestOversizeAndEmptyBatchesNeverPublish(t *testing.T) {
	spec := testSpec()
	spec.Options["max_message_bytes"] = "1"
	p := &fakeProducer{}
	d := openFake(t, spec, p)
	if err := d.Write(context.Background(), connector.Batch{}); err != nil {
		t.Fatal(err)
	}
	if err := d.Write(context.Background(), testBatch()); err == nil || !strings.Contains(err.Error(), "max_message_bytes") {
		t.Fatalf("oversized batch error=%v", err)
	}
	if len(p.messages) != 0 {
		t.Fatal("empty or oversized batch was sent")
	}
}

func TestUncertainSendResolvesSameOperationWithoutResending(t *testing.T) {
	p := &fakeProducer{sendErr: ierror.OperationUncertain{OperationID: 42}}
	d := openFake(t, testSpec(), p)
	batch := testBatch()
	if err := d.Write(context.Background(), batch); err == nil {
		t.Fatal("uncertain send was acknowledged")
	}
	changed := testBatch()
	changed.Checkpoint.LSN = "0/124"
	if err := d.Write(context.Background(), changed); err == nil || len(p.resolved) != 0 {
		t.Fatal("a different batch overtook an uncertain send")
	}
	p.resolveErr = ierror.OperationUncertain{OperationID: 42}
	if err := d.Write(context.Background(), batch); err == nil {
		t.Fatal("uncertain resolution was acknowledged")
	}
	p.resolveErr = nil
	// Observation timestamps do not alter the logical batch identity.
	batch.Checkpoint.Timestamp = time.Now()
	if err := d.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if len(p.messages) != 1 || !reflect.DeepEqual(p.resolved, []uint64{42, 42}) || d.pending != nil {
		t.Fatalf("sends=%d resolved=%v pending=%v", len(p.messages), p.resolved, d.pending)
	}
}

func TestSendErrorsAndCancellationAreNotAcknowledged(t *testing.T) {
	failure := errors.New("send rejected")
	p := &fakeProducer{sendErr: failure}
	d := openFake(t, testSpec(), p)
	if err := d.Write(context.Background(), testBatch()); !errors.Is(err, failure) {
		t.Fatalf("send error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Write(ctx, testBatch()); !errors.Is(err, context.Canceled) || len(p.messages) != 1 {
		t.Fatalf("cancelled write err=%v sends=%d", err, len(p.messages))
	}
}

func TestCapabilitiesDoNotClaimReplaySafetyOrDDLExecution(t *testing.T) {
	d := &Destination{}
	caps := d.Capabilities()
	if err := caps.ValidateSupport(); err != nil {
		t.Fatal(err)
	}
	if caps.Support != connector.SupportExperimental || caps.Delivery != (connector.DeliverySemantics{}) || !caps.TableWrites.Append || caps.TableWrites.Upsert {
		t.Fatalf("overstated capabilities: %+v", caps)
	}
	if err := d.ApplyDDL(context.Background(), connector.Schema{}, connector.Record{}); err == nil {
		t.Fatal("DDL execution silently succeeded")
	}
}
