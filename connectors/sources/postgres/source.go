package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/flowctx"
	postgrescodec "github.com/josephjohncox/wallaby/internal/postgres"
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
	optCaptureDDL          = "capture_ddl"
	optDDLTriggerSchema    = "ddl_trigger_schema"
	optDDLTriggerName      = "ddl_trigger_name"
	optDDLMessagePrefix    = "ddl_message_prefix"
	optToastFetch          = "toast_fetch"
	optToastCacheSize      = "toast_cache_size"
	optAWSRDSIAM           = "aws_rds_iam"
	optAWSRegion           = "aws_region"
	optAWSProfile          = "aws_profile"
	optAWSRoleARN          = "aws_role_arn"
	optAWSRoleSessionName  = "aws_role_session_name"
	optAWSRoleExternalID   = "aws_role_external_id"
	optAWSEndpoint         = "aws_endpoint"
)

// Source implements Postgres logical replication as a connector.Source.
type Source struct {
	spec         connector.Spec
	dsn          string
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
	toastFetch   string
	toastPool    *pgxpool.Pool
	toastCache   *toastCache
}

func (s *Source) Open(ctx context.Context, spec connector.Spec) error {
	s.spec = spec

	dsn, ok := spec.Options[optDSN]
	if !ok || dsn == "" {
		return errors.New("postgres dsn is required")
	}
	s.dsn = dsn

	if flowID := strings.TrimSpace(spec.Options[optFlowID]); flowID != "" {
		ctx = flowctx.ContextWithFlowID(ctx, flowID)
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
	captureDDL := parseBool(spec.Options[optCaptureDDL], false)
	ddlSchema := strings.TrimSpace(spec.Options[optDDLTriggerSchema])
	ddlTrigger := strings.TrimSpace(spec.Options[optDDLTriggerName])
	ddlPrefix := strings.TrimSpace(spec.Options[optDDLMessagePrefix])
	publicationTables := parseCSV(spec.Options[optPublicationTables])
	if len(publicationTables) == 0 {
		publicationTables = parseCSV(spec.Options[optTables])
	}
	publicationSchemas := parseCSV(spec.Options[optPublicationSchemas])
	if len(publicationTables) == 0 && len(publicationSchemas) > 0 {
		tables, err := ScrapeTables(ctx, dsn, publicationSchemas, spec.Options)
		if err != nil {
			return err
		}
		publicationTables = tables
	}
	if ensurePublication || validateSettings || captureDDL {
		if err := ensureReplication(ctx, dsn, spec.Options, s.publication, publicationTables, ensurePublication, validateSettings, captureDDL, ddlSchema, ddlTrigger, ddlPrefix); err != nil {
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
			if _, _, err := SyncPublicationTables(ctx, dsn, s.publication, desired, mode, spec.Options); err != nil {
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
		store, err := newSourceStateStore(ctx, dsn, stateSchema, stateTable, spec.Options)
		if err != nil {
			return err
		}
		s.stateStore = store
		s.stateID = sourceStateID(spec, s.slot)
	}

	toastFetch := strings.ToLower(strings.TrimSpace(spec.Options[optToastFetch]))
	if toastFetch == "" {
		toastFetch = toastFetchOff
	}
	switch toastFetch {
	case toastFetchOff, toastFetchSource, toastFetchCache, toastFetchFull:
	default:
		return fmt.Errorf("unsupported toast_fetch %q", toastFetch)
	}
	s.toastFetch = toastFetch
	if s.toastFetch == toastFetchSource || s.toastFetch == toastFetchFull {
		pool, err := newPool(ctx, dsn, spec.Options)
		if err != nil {
			return err
		}
		s.toastPool = pool
	}
	if s.toastFetch == toastFetchCache {
		cacheSize := parseInt(spec.Options[optToastCacheSize], 10000)
		if cacheSize > 0 {
			s.toastCache = newToastCache(cacheSize)
		}
	}

	iamProvider, err := postgrescodec.NewRDSIAMTokenProvider(ctx, dsn, spec.Options)
	if err != nil {
		return err
	}

	opts := []replication.PostgresStreamOption{
		replication.WithStatusInterval(statusInterval),
	}
	if iamProvider != nil {
		opts = append(opts, replication.WithConnConfigFunc(iamProvider.ApplyToConnConfig))
	}
	if s.SchemaHook != nil {
		opts = append(opts, replication.WithSchemaHook(s.SchemaHook))
	}
	if parseBool(spec.Options[optResolveTypes], true) {
		resolver, err := newTypeResolver(ctx, dsn, spec.Options)
		if err != nil {
			return err
		}
		s.typeResolver = resolver
		opts = append(opts, replication.WithTypeResolver(resolver))
	}
	if ddlPrefix != "" {
		opts = append(opts, replication.WithDDLMessagePrefix(ddlPrefix))
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
	if captureDDL {
		opts = append(opts, replication.WithPluginArgs([]string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", s.publication),
			"messages 'true'",
		}))
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
				if s.stream != nil {
					if err := s.stream.Err(); err != nil {
						return connector.Batch{}, err
					}
				}
				return connector.Batch{}, io.EOF
			}
			if change.Record != nil {
				if err := s.handleToast(ctx, change, change.Record); err != nil {
					return connector.Batch{}, err
				}
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
	s.stream = nil
	if s.stateStore != nil {
		_ = s.stateStore.UpdateState(ctx, s.stateID, "stopped")
		s.stateStore.Close()
		s.stateStore = nil
	}
	if s.typeResolver != nil {
		s.typeResolver.Close()
		s.typeResolver = nil
	}
	if s.toastPool != nil {
		s.toastPool.Close()
		s.toastPool = nil
	}
	return err
}

// DropSlot drops the replication slot for this source.
func (s *Source) DropSlot(ctx context.Context) error {
	if s.slot == "" {
		return nil
	}
	if s.stream != nil {
		_ = s.stream.Stop(ctx)
		s.stream = nil
	}
	if s.dsn == "" {
		return errors.New("postgres dsn is required")
	}

	pool, err := newPool(ctx, s.dsn, s.spec.Options)
	if err != nil {
		return err
	}
	defer pool.Close()

	_, err = pool.Exec(ctx, "SELECT pg_drop_replication_slot($1)", s.slot)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42704" {
			err = nil
		}
	}
	if err != nil {
		return err
	}
	if s.stateStore != nil {
		if updateErr := s.stateStore.UpdateState(ctx, s.stateID, "dropped"); updateErr != nil {
			return updateErr
		}
	}
	return nil
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
