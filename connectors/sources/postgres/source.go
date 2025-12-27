package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	optDSN                 = "dsn"
	optSlot                = "slot"
	optPublication         = "publication"
	optStartLSN            = "start_lsn"
	optBatchSize           = "batch_size"
	optBatchTimeout        = "batch_timeout"
	optStatusInterval      = "status_interval"
	optCreateSlot          = "create_slot"
	optFormat              = "format"
	optEmitEmpty           = "emit_empty"
	optEnsurePublication   = "ensure_publication"
	optValidateSettings    = "validate_replication"
	optPublicationTables   = "publication_tables"
	optPublicationSchemas  = "publication_schemas"
	optSyncPublication     = "sync_publication"
	optSyncPublicationMode = "sync_publication_mode"
	optResolveTypes        = "resolve_types"
	optEnsureState         = "ensure_state"
	optStateSchema         = "state_schema"
	optStateTable          = "state_table"
	optFlowID              = "flow_id"
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
	stateStore   *sourceStateStore
	stateID      string
	typeResolver *pgTypeResolver
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

	ensurePublication := parseBool(spec.Options[optEnsurePublication], true)
	validateSettings := parseBool(spec.Options[optValidateSettings], true)
	publicationTables := parseCSV(spec.Options[optPublicationTables])
	if len(publicationTables) == 0 {
		publicationTables = parseCSV(spec.Options[optTables])
	}
	publicationSchemas := parseCSV(spec.Options[optPublicationSchemas])
	if len(publicationTables) == 0 && len(publicationSchemas) > 0 {
		tables, err := ScrapeTables(ctx, dsn, publicationSchemas)
		if err != nil {
			return err
		}
		publicationTables = tables
	}
	if ensurePublication || validateSettings {
		if err := ensureReplication(ctx, dsn, s.publication, publicationTables, ensurePublication, validateSettings); err != nil {
			return err
		}
	}

	if parseBool(spec.Options[optSyncPublication], false) {
		desired := publicationTables
		if len(desired) > 0 {
			mode := spec.Options[optSyncPublicationMode]
			if mode == "" {
				mode = "add"
			}
			if _, _, err := SyncPublicationTables(ctx, dsn, s.publication, desired, mode); err != nil {
				return err
			}
		}
	}

	if parseBool(spec.Options[optEnsureState], true) {
		stateSchema := spec.Options[optStateSchema]
		if stateSchema == "" {
			stateSchema = "wallaby"
		}
		stateTable := spec.Options[optStateTable]
		if stateTable == "" {
			stateTable = "source_state"
		}
		store, err := newSourceStateStore(ctx, dsn, stateSchema, stateTable)
		if err != nil {
			return err
		}
		s.stateStore = store
		s.stateID = sourceStateID(spec, s.slot)
	}

	opts := []replication.PostgresStreamOption{
		replication.WithStatusInterval(statusInterval),
	}
	if s.SchemaHook != nil {
		opts = append(opts, replication.WithSchemaHook(s.SchemaHook))
	}
	if parseBool(spec.Options[optResolveTypes], true) {
		resolver, err := newTypeResolver(ctx, dsn)
		if err != nil {
			return err
		}
		s.typeResolver = resolver
		opts = append(opts, replication.WithTypeResolver(resolver))
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
		if s.stateStore != nil {
			s.stateStore.Close()
			s.stateStore = nil
		}
		if s.typeResolver != nil {
			s.typeResolver.Close()
			s.typeResolver = nil
		}
		return err
	}
	s.changes = changes

	if s.stateStore != nil {
		err := s.stateStore.Upsert(ctx, sourceState{
			ID:          s.stateID,
			SourceName:  spec.Name,
			Slot:        s.slot,
			Publication: s.publication,
			State:       "running",
			Options:     sanitizeOptions(spec.Options),
		})
		if err != nil {
			_ = s.stream.Stop(ctx)
			s.stateStore.Close()
			s.stateStore = nil
			return err
		}
	}

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

func (s *Source) Ack(ctx context.Context, checkpoint connector.Checkpoint) error {
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
	if s.stateStore != nil {
		if err := s.stateStore.RecordAck(ctx, s.stateID, checkpoint.LSN); err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) Close(ctx context.Context) error {
	if s.stream == nil {
		return nil
	}
	err := s.stream.Stop(ctx)
	if s.stateStore != nil {
		_ = s.stateStore.UpdateState(ctx, s.stateID, "stopped")
		s.stateStore.Close()
		s.stateStore = nil
	}
	if s.typeResolver != nil {
		s.typeResolver.Close()
		s.typeResolver = nil
	}
	return err
}

func (s *Source) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsBulkLoad:      false,
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
