// Package iggy delivers CDC batches through Apache Iggy's native TCP protocol.
package iggy

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/josephjohncox/wallaby/internal/options"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/wire"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
)

var (
	tracer = otel.Tracer("wallaby/connectors/iggy")
	meter  = otel.Meter("wallaby/connectors/iggy")
	// These fixed, valid instrument names cannot fail validation.
	acknowledgedMessages, _ = meter.Int64Counter("wallaby.iggy.messages.acknowledged", metric.WithDescription("CDC batch messages acknowledged by Iggy"))
	writeErrors, _          = meter.Int64Counter("wallaby.iggy.write.errors", metric.WithDescription("Failed Iggy write or uncertainty-resolution attempts"))
)

type producer interface {
	Connect(context.Context) error
	SendMessages(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Partitioning, []iggcon.IggyMessage) (*iggcon.SendMessagesResponse, error)
	ResolveOperation(context.Context, uint64) error
	AbandonOperation(context.Context, uint64) error
	Close() error
}

type destinationConfig struct {
	address       string
	stream        iggcon.Identifier
	topic         iggcon.Identifier
	partition     uint32
	username      string
	password      string
	token         string
	tls           bool
	tlsCAFile     string
	tlsServerName string
	timeout       time.Duration
	maxMessage    int
	codec         wire.Codec
}

type pendingWrite struct {
	hash        string
	operationID uint64
}

// Destination sends one encoded batch per message to a pre-provisioned stream,
// topic and explicit (zero-based) partition. It does not provision broker state.
// A process restart can duplicate a committed message; message IDs do not make
// this connector replay-safe or suitable for primary-acknowledgement outboxes.
// Calls are serialized so an uncertain operation cannot be overtaken.
type Destination struct {
	mu      sync.Mutex
	client  producer
	config  destinationConfig
	pending *pendingWrite
}

func (d *Destination) Open(ctx context.Context, spec connector.RuntimeSpec) error {
	return d.open(ctx, spec, newProducer)
}

func (d *Destination) open(ctx context.Context, spec connector.RuntimeSpec, factory func(destinationConfig) (producer, error)) (err error) {
	ctx, span := tracer.Start(ctx, "iggy.Open")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, "iggy open failed")
		}
		span.End()
	}()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client != nil {
		return errors.New("iggy destination is already open")
	}
	cfg, err := parseConfig(spec)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	cli, err := factory(cfg)
	if err != nil {
		if cli != nil {
			err = errors.Join(err, cli.Close())
		}
		return fmt.Errorf("create iggy client: %w", err)
	}
	if cli == nil {
		return errors.New("create iggy client: nil producer")
	}
	if err := cli.Connect(ctx); err != nil {
		return errors.Join(fmt.Errorf("connect and authenticate iggy client: %w", err), cli.Close())
	}
	// Credentials belong only to the SDK's session, not to retained adapter state.
	cfg.username, cfg.password, cfg.token = "", "", ""
	d.client, d.config, d.pending = cli, cfg, nil
	return nil
}

// ValidateSpec validates without connecting or provisioning broker resources.
func (*Destination) ValidateSpec(spec connector.RuntimeSpec) error {
	_, err := parseConfig(spec)
	return err
}

func parseConfig(spec connector.RuntimeSpec) (destinationConfig, error) {
	decoder := options.NewDecoder("iggy options", spec.Options)
	partitionText := decoder.String("partition_id", "0")
	cfg := destinationConfig{
		address:       decoder.String("address", ""),
		username:      decoder.Raw("username", ""),
		password:      decoder.Raw("password", ""),
		token:         decoder.Raw("token", ""),
		tls:           decoder.Bool("tls", true),
		tlsCAFile:     decoder.String("tls_ca_file", ""),
		tlsServerName: decoder.String("tls_server_name", ""),
		timeout:       decoder.Duration("timeout", 10*time.Second),
		maxMessage:    decoder.Int("max_message_bytes", 1_000_000),
	}
	if err := decoder.Err(); err != nil {
		return destinationConfig{}, err
	}
	host, port, err := net.SplitHostPort(cfg.address)
	if err != nil || host == "" || strings.ContainsAny(host, "/@?# \t\r\n") {
		return destinationConfig{}, errors.New("iggy address must be host:port without credentials or a URL scheme")
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return destinationConfig{}, errors.New("iggy address port must be in 1..65535")
	}
	if cfg.token != "" {
		if cfg.username != "" || cfg.password != "" {
			return destinationConfig{}, errors.New("iggy token and username/password are mutually exclusive")
		}
	} else if cfg.username == "" || cfg.password == "" {
		return destinationConfig{}, errors.New("iggy requires a token or both username and password")
	}
	if !cfg.tls && (cfg.tlsCAFile != "" || cfg.tlsServerName != "") {
		return destinationConfig{}, errors.New("iggy TLS options require tls=true")
	}
	if cfg.timeout <= 0 || cfg.timeout > 30*time.Second {
		return destinationConfig{}, errors.New("iggy timeout must be positive and at most 30s (SDK operation limit)")
	}
	partition, err := strconv.ParseUint(partitionText, 10, 32)
	if err != nil {
		return destinationConfig{}, errors.New("iggy partition_id must be in 0..4294967295")
	}
	cfg.partition = uint32(partition)
	if cfg.maxMessage <= 0 || cfg.maxMessage > iggcon.MaxPayloadSize {
		return destinationConfig{}, fmt.Errorf("iggy max_message_bytes must be in 1..%d", iggcon.MaxPayloadSize)
	}
	cfg.stream, err = iggcon.NewIdentifier(decoder.String("stream", ""))
	if err != nil {
		return destinationConfig{}, errors.New("iggy stream name must contain 1..255 bytes")
	}
	cfg.topic, err = iggcon.NewIdentifier(decoder.String("topic", ""))
	if err != nil {
		return destinationConfig{}, errors.New("iggy topic name must contain 1..255 bytes")
	}
	cfg.codec, err = wire.NewCodec(spec.Options["format"])
	if err != nil {
		return destinationConfig{}, err
	}
	if cfg.codec.Name() == connector.WireFormatParquet {
		return destinationConfig{}, errors.New("iggy supports arrow, avro, proto and json wire formats, not parquet")
	}
	return cfg, nil
}

func newProducer(cfg destinationConfig) (producer, error) {
	auth := tcp.NewUsernamePasswordCredentials(cfg.username, cfg.password)
	if cfg.token != "" {
		auth = tcp.NewPersonalAccessTokenCredentials(cfg.token)
	}
	opts := []tcp.Option{tcp.WithServerAddress(cfg.address), tcp.WithAutoLogin(auth)}
	if cfg.tls {
		// Certificate validation cannot be disabled by connector configuration.
		opts = append(opts, tcp.WithTLS(tcp.WithTLSValidateCertificate(true), tcp.WithTLSCAFile(cfg.tlsCAFile), tcp.WithTLSDomain(cfg.tlsServerName)))
	}
	return client.NewIggyClient(client.WithTcp(opts...))
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) (err error) {
	ctx, span := tracer.Start(ctx, "iggy.Write")
	defer func() {
		if err != nil {
			// SDK/codec error text can contain source data. Keep detailed
			// errors on the caller boundary, not in exported telemetry.
			span.SetStatus(codes.Error, "iggy write failed")
			writeErrors.Add(ctx, 1)
		}
		span.End()
	}()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil {
		return errors.New("iggy destination is not initialized")
	}
	ctx, cancel := context.WithTimeout(ctx, d.config.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	hash, err := connector.BatchContentHash(batch)
	if err != nil {
		return err
	}
	if d.pending != nil {
		if hash != d.pending.hash {
			return errors.New("iggy has an uncertain write; retry the same batch before sending another")
		}
		// Resolve the SDK's retained request identity. A new SendMessages here
		// could append the same batch again or be rejected by the blocked lane.
		if err := d.client.ResolveOperation(ctx, d.pending.operationID); err != nil {
			if permitsFreshSend(err) {
				// Explicit at-least-once policy: a lost receipt or an allowlisted
				// refusal can be retried as a new send. Abandon only releases local
				// ownership; it does NOT prove that the old send had no effect.
				abandonErr := d.client.AbandonOperation(ctx, d.pending.operationID)
				if abandonErr != nil && !errors.Is(abandonErr, ierror.ErrOperationNotRetained) {
					return errors.Join(fmt.Errorf("resolve uncertain iggy send: %w", err), abandonErr)
				}
				d.pending = nil
				return fmt.Errorf("iggy recovery identity ended; next retry sends at least once and may duplicate: %w", err)
			}
			return fmt.Errorf("resolve uncertain iggy send: %w", err)
		}
		d.pending = nil
		acknowledgedMessages.Add(ctx, 1)
		return nil
	}
	if len(batch.Records) == 0 {
		return nil
	}
	payload, err := d.config.codec.Encode(batch)
	if err != nil {
		return fmt.Errorf("encode iggy batch: %w", err)
	}
	if len(payload) > d.config.maxMessage {
		return fmt.Errorf("iggy encoded batch exceeds max_message_bytes: %d > %d; reduce source batch_size", len(payload), d.config.maxMessage)
	}
	idBytes, err := hex.DecodeString(hash)
	if err != nil {
		return fmt.Errorf("decode iggy batch identity: %w", err)
	}
	var id [16]byte
	copy(id[:], idBytes)
	message, err := iggcon.NewIggyMessage(payload, iggcon.WithID(id), iggcon.WithUserHeaders([]iggcon.HeaderEntry{
		stringHeader("wallaby-format", string(d.config.codec.Name())),
		stringHeader("wallaby-content-type", d.config.codec.ContentType()),
		stringHeader("wallaby-batch-hash", hash),
	}))
	if err != nil {
		return fmt.Errorf("create iggy message: %w", err)
	}
	_, err = d.client.SendMessages(ctx, d.config.stream, d.config.topic, iggcon.PartitionId(d.config.partition), []iggcon.IggyMessage{message})
	if err != nil {
		var uncertain ierror.OperationUncertain
		if errors.As(err, &uncertain) {
			d.pending = &pendingWrite{hash: hash, operationID: uncertain.OperationID}
		}
		return fmt.Errorf("send iggy batch: %w", err)
	}
	// An empty placement list is valid SDK success. Do not resend based on it.
	acknowledgedMessages.Add(ctx, 1)
	return nil
}

func permitsFreshSend(err error) bool {
	return errors.Is(err, ierror.ErrOperationNotRetained) || errors.Is(err, ierror.ErrUnauthorized) ||
		errors.Is(err, ierror.ErrStreamNameNotFound) || errors.Is(err, ierror.ErrStreamIdNotFound) ||
		errors.Is(err, ierror.ErrTopicNameNotFound) || errors.Is(err, ierror.ErrTopicIdNotFound) || errors.Is(err, ierror.ErrPartitionNotFound)
}

func stringHeader(key, value string) iggcon.HeaderEntry {
	return iggcon.HeaderEntry{
		Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(key)},
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
	}
}

func (*Destination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return errors.New("iggy does not execute DDL; DDL can only be transported as CDC events")
}

func (*Destination) TypeMappings() map[string]string { return nil }

func (d *Destination) Close(ctx context.Context) (err error) {
	_, span := tracer.Start(ctx, "iggy.Close")
	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, "iggy close failed")
		}
		span.End()
	}()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil {
		return nil
	}
	err = d.client.Close()
	d.client, d.pending, d.config = nil, nil, destinationConfig{}
	return err
}

func (*Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		Support:               connector.SupportExperimental,
		TableWrites:           connector.TableWriteSemantics{Append: true},
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsTypeMapping:   true,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatArrow, connector.WireFormatAvro,
			connector.WireFormatProto, connector.WireFormatJSON,
		},
	}
}

var _ connector.Destination = (*Destination)(nil)
