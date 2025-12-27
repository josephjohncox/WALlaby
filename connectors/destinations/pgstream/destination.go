package pgstream

import (
	"context"
	"errors"
	"fmt"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
	"github.com/josephjohncox/wallaby/pkg/wire"
)

const (
	optDSN    = "dsn"
	optStream = "stream"
	optFormat = "format"
)

// Destination writes change events into a Postgres-backed stream.
type Destination struct {
	spec   connector.Spec
	store  *pgstream.Store
	stream string
	codec  wire.Codec
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	d.spec = spec
	dsn := spec.Options[optDSN]
	if dsn == "" {
		return errors.New("pgstream dsn is required")
	}
	streamName := spec.Options[optStream]
	if streamName == "" {
		streamName = spec.Name
	}
	if streamName == "" {
		return errors.New("pgstream stream name is required")
	}
	d.stream = streamName

	format := spec.Options[optFormat]
	if format == "" {
		format = string(connector.WireFormatJSON)
	}
	codec, err := wire.NewCodec(format)
	if err != nil {
		return err
	}
	d.codec = codec

	store, err := pgstream.NewStore(ctx, dsn)
	if err != nil {
		return err
	}
	d.store = store

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.store == nil {
		return errors.New("pgstream destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}

	messages := make([]pgstream.Message, 0, len(batch.Records))
	for _, record := range batch.Records {
		payloadBatch := connector.Batch{
			Records:    []connector.Record{record},
			Schema:     batch.Schema,
			Checkpoint: batch.Checkpoint,
			WireFormat: batch.WireFormat,
		}
		payload, err := d.codec.Encode(payloadBatch)
		if err != nil {
			return fmt.Errorf("encode stream payload: %w", err)
		}
		if len(payload) == 0 {
			continue
		}

		messages = append(messages, pgstream.Message{
			Stream:     d.stream,
			Namespace:  batch.Schema.Namespace,
			Table:      record.Table,
			LSN:        batch.Checkpoint.LSN,
			WireFormat: d.codec.Name(),
			Payload:    payload,
		})
	}

	return d.store.Enqueue(ctx, d.stream, messages)
}

func (d *Destination) ApplyDDL(_ context.Context, _ connector.Schema, _ connector.Record) error {
	return nil
}

func (d *Destination) TypeMappings() map[string]string { return nil }

func (d *Destination) Close(_ context.Context) error {
	if d.store != nil {
		d.store.Close()
	}
	return nil
}

func (d *Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsBulkLoad:      true,
		SupportsTypeMapping:   true,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatArrow,
			connector.WireFormatParquet,
			connector.WireFormatAvro,
			connector.WireFormatProto,
			connector.WireFormatJSON,
		},
	}
}
