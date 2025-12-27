package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/josephjohncox/ductstream/internal/replication"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

const (
	optDSN            = "dsn"
	optSlot           = "slot"
	optPublication    = "publication"
	optStartLSN       = "start_lsn"
	optBatchSize      = "batch_size"
	optBatchTimeout   = "batch_timeout"
	optStatusInterval = "status_interval"
	optCreateSlot     = "create_slot"
	optFormat         = "format"
	optEmitEmpty      = "emit_empty"
)

// Source implements Postgres logical replication as a connector.Source.
type Source struct {
	spec         connector.Spec
	stream       *replication.PostgresStream
	changes      <-chan replication.Change
	batchSize    int
	batchTimeout time.Duration
	slot         string
	publication  string
	wireFormat   connector.WireFormat
	emitEmpty    bool
	SchemaHook   replication.SchemaHook
}

func (s *Source) Open(ctx context.Context, spec connector.Spec) error {
	s.spec = spec

	dsn, ok := spec.Options[optDSN]
	if !ok || dsn == "" {
		return errors.New("postgres dsn is required")
	}

	s.slot = spec.Options[optSlot]
	if s.slot == "" {
		return errors.New("replication slot is required")
	}

	s.publication = spec.Options[optPublication]
	if s.publication == "" {
		return errors.New("publication is required")
	}

	s.batchSize = parseInt(spec.Options[optBatchSize], 100)
	s.batchTimeout = parseDuration(spec.Options[optBatchTimeout], 1*time.Second)
	statusInterval := parseDuration(spec.Options[optStatusInterval], 10*time.Second)
	s.wireFormat = connector.WireFormat(spec.Options[optFormat])
	if s.wireFormat == "" {
		s.wireFormat = connector.WireFormatArrow
	}
	s.emitEmpty = parseBool(spec.Options[optEmitEmpty], false)

	opts := []replication.PostgresStreamOption{
		replication.WithStatusInterval(statusInterval),
	}
	if s.SchemaHook != nil {
		opts = append(opts, replication.WithSchemaHook(s.SchemaHook))
	}
	if startLSN := spec.Options[optStartLSN]; startLSN != "" {
		lsn, err := pglogrepl.ParseLSN(startLSN)
		if err != nil {
			return fmt.Errorf("parse start_lsn: %w", err)
		}
		opts = append(opts, replication.WithStartLSN(lsn))
	}
	if createSlot := parseBool(spec.Options[optCreateSlot], true); !createSlot {
		opts = append(opts, replication.WithCreateSlot(false))
	}

	s.stream = replication.NewPostgresStream(dsn, opts...)
	changes, err := s.stream.Start(ctx, s.slot, s.publication)
	if err != nil {
		return err
	}
	s.changes = changes

	return nil
}

func (s *Source) Read(ctx context.Context) (connector.Batch, error) {
	if s.changes == nil {
		return connector.Batch{}, errors.New("source not started")
	}

	var records []connector.Record
	var schema connector.Schema
	var checkpoint connector.Checkpoint

	timer := time.NewTimer(s.batchTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return connector.Batch{}, ctx.Err()
		case <-timer.C:
			if len(records) == 0 {
				if s.emitEmpty {
					return connector.Batch{
						Records:    nil,
						Schema:     schema,
						Checkpoint: checkpoint,
						WireFormat: s.wireFormat,
					}, nil
				}
				timer.Reset(s.batchTimeout)
				continue
			}
			return connector.Batch{
				Records:    records,
				Schema:     schema,
				Checkpoint: checkpoint,
				WireFormat: s.wireFormat,
			}, nil
		case change, ok := <-s.changes:
			if !ok {
				return connector.Batch{}, io.EOF
			}
			if change.Record != nil {
				records = append(records, *change.Record)
			}
			if change.SchemaDef != nil {
				schema = *change.SchemaDef
			}
			checkpoint = connector.Checkpoint{
				LSN:       change.LSN.String(),
				Timestamp: time.Now().UTC(),
			}

			if len(records) >= s.batchSize {
				return connector.Batch{
					Records:    records,
					Schema:     schema,
					Checkpoint: checkpoint,
					WireFormat: s.wireFormat,
				}, nil
			}
		}
	}
}

func (s *Source) Ack(_ context.Context, checkpoint connector.Checkpoint) error {
	if s.stream == nil {
		return nil
	}
	if checkpoint.LSN == "" {
		return nil
	}
	lsn, err := pglogrepl.ParseLSN(checkpoint.LSN)
	if err != nil {
		return fmt.Errorf("parse checkpoint lsn: %w", err)
	}
	s.stream.Ack(lsn)
	return nil
}

func (s *Source) Close(ctx context.Context) error {
	if s.stream == nil {
		return nil
	}
	return s.stream.Stop(ctx)
}

func (s *Source) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsBulkLoad:      false,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatArrow,
			connector.WireFormatParquet,
			connector.WireFormatAvro,
			connector.WireFormatProto,
			connector.WireFormatJSON,
		},
	}
}

func parseInt(raw string, fallback int) int {
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func parseDuration(raw string, fallback time.Duration) time.Duration {
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return value
}

func parseBool(raw string, fallback bool) bool {
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}
