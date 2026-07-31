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
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/flowctx"
	postgrescodec "github.com/josephjohncox/wallaby/internal/postgres"
	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	optManaged             = "managed"
	optMaxTxnRecords       = "max_transaction_records"
	optMaxTxnBytes         = "max_transaction_bytes"
	optStreamingTxns       = "streaming_transactions"
	optSourceSystemID      = "source_system_identifier"
	optSourceLineageID     = "source_lineage_id"
	optPublicationRevision = "publication_revision"
	optManagedProfile      = "managed_profile"
	optSchemaBaselines     = connector.ManagedSchemaBaselinesMetadataKey
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
	spec                 connector.Spec
	dsn                  string
	stream               *replication.PostgresStream
	changes              <-chan replication.Change
	batchSize            int
	batchTimeout         time.Duration
	slot                 string
	publication          string
	wireFormat           connector.WireFormat
	emitEmpty            bool
	SchemaHook           replication.SchemaHook
	stateStore           *sourceStateStore
	stateID              string
	typeResolver         *pgTypeResolver
	toastFetch           string
	toastPool            *pgxpool.Pool
	toastCache           *toastCache
	lagPool              *pgxpool.Pool
	sourceLineage        string
	managedPostgresMajor int
	pendingChange        *replication.Change
	Meters               *telemetry.Meters
	ManagedControl       *pgxpool.Pool
	ManagedAuthority     authority.Store
	BootstrapHooks       bootstrap.Hooks
}

type changeBatchIdentity struct {
	control       bool
	namespace     string
	table         string
	schemaVersion int64
}

func identityForChange(change replication.Change) (changeBatchIdentity, bool) {
	if change.Record == nil {
		return changeBatchIdentity{}, false
	}

	record := change.Record
	identity := changeBatchIdentity{
		control:       record.Operation == connector.OpDDL || record.DDL != "" || len(record.DDLPlan) > 0,
		namespace:     change.Schema,
		table:         record.Table,
		schemaVersion: record.SchemaVersion,
	}
	if identity.table == "" {
		identity.table = change.Table
	}
	if change.SchemaDef != nil {
		identity.namespace = change.SchemaDef.Namespace
		identity.table = change.SchemaDef.Name
		identity.schemaVersion = change.SchemaDef.Version
	}
	return identity, true
}

func changeEndsBatch(current changeBatchIdentity, change replication.Change) bool {
	next, ok := identityForChange(change)
	if ok {
		return current != next
	}
	if change.SchemaDef == nil {
		return false
	}
	return current.namespace != change.SchemaDef.Namespace ||
		current.table != change.SchemaDef.Name ||
		current.schemaVersion != change.SchemaDef.Version
}

func (s *Source) Open(ctx context.Context, spec connector.Spec) error {
	s.spec = spec
	opened := false
	defer func() {
		if !opened {
			_ = s.closeResources(context.WithoutCancel(ctx), false)
		}
	}()

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
	managed := connector.IsManagedSourceSpec(spec)
	if managed {
		for _, option := range []string{optCreateSlot, optEnsureState, optEnsurePublication, optSyncPublication} {
			raw, present := spec.Options[option]
			if !present || parseBool(raw, true) {
				return fmt.Errorf("managed PostgreSQL Source.Open requires explicit %s=false; source-resource mutation is allowed only inside fenced bootstrap", option)
			}
		}
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
			mode, err := NormalizeSyncPublicationMode(spec.Options[optSyncPublicationMode])
			if err != nil {
				return err
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

	streamingTransactions := parseBool(spec.Options[optStreamingTxns], managed)
	s.sourceLineage = strings.TrimSpace(spec.Options[optSourceLineageID])
	maxTransactionRecords := parseInt(spec.Options[optMaxTxnRecords], 1_000_000)
	maxTransactionBytes := parseInt(spec.Options[optMaxTxnBytes], 256<<20)
	if maxTransactionRecords <= 0 || maxTransactionBytes <= 0 {
		return errors.New("max_transaction_records and max_transaction_bytes must be positive")
	}
	opts := []replication.PostgresStreamOption{
		replication.WithStatusInterval(statusInterval),
		replication.WithRequireAuthorizedStart(managed),
		replication.WithExpectedSystemID(spec.Options[optSourceSystemID]),
		replication.WithTransactionLimits(maxTransactionRecords, int64(maxTransactionBytes)),
		replication.WithStreamingTransactions(streamingTransactions),
	}
	if iamProvider != nil {
		opts = append(opts, replication.WithConnConfigFunc(iamProvider.ApplyToConnConfig))
	}
	if s.SchemaHook != nil {
		opts = append(opts, replication.WithSchemaHook(s.SchemaHook))
	}
	if managed {
		baselines, err := connector.DecodeManagedSchemaBaselines(spec.Options[optSchemaBaselines])
		if err != nil {
			return err
		}
		opts = append(opts, replication.WithSchemaBaselines(baselines))
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
	if captureDDL {
		opts = append(opts, replication.WithEmitPlanDDL(false))
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
		protocolVersion := 1
		if streamingTransactions {
			protocolVersion = 2
		}
		pluginArgs := []string{
			fmt.Sprintf("proto_version '%d'", protocolVersion),
			fmt.Sprintf("publication_names '%s'", s.publication),
			"messages 'true'",
		}
		if streamingTransactions {
			pluginArgs = append(pluginArgs, "streaming 'on'")
		}
		opts = append(opts, replication.WithPluginArgs(pluginArgs))
	}

	lagPool, err := newPool(ctx, dsn, spec.Options)
	if err != nil {
		return fmt.Errorf("create lag pool: %w", err)
	}
	s.lagPool = lagPool
	s.managedPostgresMajor, err = validateManagedPostgresServerVersion(ctx, lagPool, spec.Options[optManagedProfile])
	if err != nil {
		return err
	}
	if managed {
		expectedRevision := strings.TrimSpace(spec.Options[optPublicationRevision])
		actualRevision, err := PublicationFingerprint(ctx, lagPool, s.publication)
		if err != nil {
			return err
		}
		if actualRevision != expectedRevision {
			return fmt.Errorf("managed publication revision %s does not match configured %s", actualRevision, expectedRevision)
		}
	}

	if s.Meters != nil {
		opts = append(opts, replication.WithProtocolErrorReporter(s.recordProtocolError))
	}

	s.stream = replication.NewPostgresStream(dsn, opts...)
	changes, err := s.stream.Start(ctx, s.slot, s.publication)
	if err != nil {
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
			return err
		}
	}

	opened = true
	return nil
}

func (s *Source) recordProtocolError(ctx context.Context, errorType string) {
	if s.Meters == nil || errorType == "" {
		return
	}
	s.Meters.RecordError(ctx, "replication_protocol_"+errorType)
}

func (s *Source) Read(ctx context.Context) (connector.Batch, error) {
	if s.changes == nil {
		return connector.Batch{}, errors.New("source not started")
	}

	tracer := otel.Tracer("wallaby/source/postgres")
	var records []connector.Record
	var schema connector.Schema
	var checkpoint connector.Checkpoint
	var identity changeBatchIdentity
	identitySet := false
	var transactionID uint32
	transactionSet := false

	timer := time.NewTimer(s.batchTimeout)
	defer timer.Stop()

	_, waitSpan := tracer.Start(ctx, "source.wait")
	var processSpan trace.Span
	finishBatch := func() connector.Batch {
		if processSpan != nil {
			processSpan.SetAttributes(attribute.Int("records", len(records)))
			processSpan.End()
		} else {
			waitSpan.End()
		}
		return connector.Batch{
			Records:    records,
			Schema:     schema,
			Checkpoint: checkpoint,
			WireFormat: s.wireFormat,
		}
	}

	for {
		var change replication.Change
		var ok bool
		if s.pendingChange != nil {
			change = *s.pendingChange
			s.pendingChange = nil
			ok = true
		} else {
			select {
			case <-ctx.Done():
				waitSpan.End()
				if processSpan != nil {
					processSpan.End()
				}
				return connector.Batch{}, ctx.Err()
			case <-timer.C:
				if len(records) == 0 {
					// Always surface observed WAL progress, even when every change was
					// filtered. emit_empty_batches controls only positionless polling
					// heartbeats; suppressing a durable position would retain WAL forever.
					if checkpoint.LSN != "" || s.emitEmpty {
						return finishBatch(), nil
					}
					waitSpan.SetAttributes(attribute.Bool("timeout", true))
					timer.Reset(s.batchTimeout)
					continue
				}
				return finishBatch(), nil
			case change, ok = <-s.changes:
			}
		}

		if !ok {
			if len(records) > 0 || checkpoint.LSN != "" {
				return finishBatch(), nil
			}
			waitSpan.End()
			if processSpan != nil {
				processSpan.End()
			}
			if s.stream != nil {
				if err := s.stream.Err(); err != nil {
					return connector.Batch{}, err
				}
			}
			return connector.Batch{}, io.EOF
		}

		if transactionSet && change.TransactionID != 0 && change.TransactionID != transactionID {
			deferred := change
			s.pendingChange = &deferred
			return finishBatch(), nil
		}
		if identitySet && changeEndsBatch(identity, change) {
			deferred := change
			s.pendingChange = &deferred
			return finishBatch(), nil
		}
		if !transactionSet && change.TransactionID != 0 {
			transactionID = change.TransactionID
			transactionSet = true
		}
		if processSpan == nil {
			waitSpan.End()
			_, processSpan = tracer.Start(ctx, "source.process")
		}
		if change.Record != nil {
			if !identitySet {
				identity, identitySet = identityForChange(change)
			}
			record := *change.Record
			if record.SourcePosition == "" {
				record.SourcePosition = change.LSN.String()
			}
			if err := s.handleToast(ctx, change, &record); err != nil {
				processSpan.End()
				return connector.Batch{}, err
			}
			records = append(records, record)
		}
		if change.SchemaDef != nil {
			schema = *change.SchemaDef
		}
		// A committed PostgreSQL transaction may be split into table-scoped
		// compatibility batches. Only its final fragment carries the
		// transaction-end checkpoint, so an intermediate fragment can never
		// authorize source feedback beyond work not yet delivered.
		if change.TransactionID == 0 || change.TransactionFinal {
			checkpoint = connector.Checkpoint{
				LSN:       change.LSN.String(),
				Timestamp: time.Now().UTC(),
			}
		}

		if len(records) >= s.batchSize {
			return finishBatch(), nil
		}
	}
}

// ReadTransaction returns one complete committed PostgreSQL transaction. It
// preserves source order and groups only adjacent records with the same
// table/schema identity into compatibility batches.
func (s *Source) ReadTransaction(ctx context.Context) (connector.SourceTransaction, error) {
	if s.changes == nil {
		return connector.SourceTransaction{}, errors.New("source not started")
	}

	var transaction connector.SourceTransaction
	transaction.SourceLineageID = s.sourceLineage
	var currentIdentity changeBatchIdentity
	identitySet := false
	var fragment *connector.TransactionFragment

	for {
		change, err := s.nextTransactionChange(ctx)
		if err != nil {
			return connector.SourceTransaction{}, err
		}
		if transaction.TransactionID == 0 {
			transaction.TransactionID = change.TransactionID
			transaction.BeginLSN = change.TransactionBeginLSN.String()
			transaction.CommitLSN = change.TransactionCommitLSN.String()
			transaction.EndLSN = change.TransactionEndLSN.String()
		}
		if change.TransactionID != 0 && transaction.TransactionID != change.TransactionID {
			return connector.SourceTransaction{}, fmt.Errorf(
				"source transaction changed from xid %d to %d before final fragment",
				transaction.TransactionID,
				change.TransactionID,
			)
		}

		if change.Record != nil {
			identity, ok := identityForChange(change)
			if !ok {
				return connector.SourceTransaction{}, errors.New("postgres change record has no batch identity")
			}
			if !identitySet || currentIdentity != identity {
				transaction.Fragments = append(transaction.Fragments, connector.TransactionFragment{
					Ordinal: uint64(len(transaction.Fragments)),
					Batch: connector.Batch{
						WireFormat: s.wireFormat,
					},
				})
				fragment = &transaction.Fragments[len(transaction.Fragments)-1]
				currentIdentity = identity
				identitySet = true
			}
			record := *change.Record
			if record.SourcePosition == "" {
				record.SourcePosition = change.LSN.String()
			}
			if err := s.handleToast(ctx, change, &record); err != nil {
				return connector.SourceTransaction{}, err
			}
			fragment.Batch.Records = append(fragment.Batch.Records, record)
			if change.SchemaDef != nil {
				fragment.Batch.Schema = *change.SchemaDef
			}
		}

		if change.TransactionFinal || change.TransactionID == 0 {
			endLSN := change.TransactionEndLSN
			if endLSN == 0 {
				endLSN = change.LSN
			}
			transaction.EndLSN = endLSN.String()
			transaction.Checkpoint = connector.Checkpoint{
				LSN:       endLSN.String(),
				Timestamp: time.Now().UTC(),
			}
			return transaction, nil
		}
	}
}

func (s *Source) nextTransactionChange(ctx context.Context) (replication.Change, error) {
	if s.pendingChange != nil {
		change := *s.pendingChange
		s.pendingChange = nil
		return change, nil
	}
	select {
	case <-ctx.Done():
		return replication.Change{}, ctx.Err()
	case change, ok := <-s.changes:
		if ok {
			return change, nil
		}
		if s.stream != nil {
			if err := s.stream.Err(); err != nil {
				return replication.Change{}, err
			}
		}
		return replication.Change{}, io.EOF
	}
}

// InitialCheckpoint returns the exact validated start used by the replication
// stream. It is available only after Open succeeds.
func (s *Source) InitialCheckpoint() (connector.Checkpoint, bool) {
	if s.stream == nil {
		return connector.Checkpoint{}, false
	}
	lsn := s.stream.InitialLSN()
	if lsn == 0 {
		return connector.Checkpoint{}, false
	}
	return connector.Checkpoint{LSN: lsn.String()}, true
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
	if s.stateStore != nil {
		if err := s.stateStore.RecordAck(ctx, s.stateID, checkpoint.LSN); err != nil {
			return err
		}
	}
	s.stream.Ack(lsn)
	return nil
}

// AckWithEvidence sends feedback immediately and waits until PostgreSQL exposes
// the exact confirmed_flush_lsn for this logical slot.
func (s *Source) AckWithEvidence(ctx context.Context, checkpoint connector.Checkpoint) (connector.SourceFlushEvidence, error) {
	if s.stream == nil || s.lagPool == nil {
		return connector.SourceFlushEvidence{}, errors.New("postgres source feedback evidence requires an open stream and catalog pool")
	}
	target, err := pglogrepl.ParseLSN(checkpoint.LSN)
	if err != nil {
		return connector.SourceFlushEvidence{}, fmt.Errorf("parse source feedback checkpoint: %w", err)
	}
	if err := s.stream.AckWithEvidence(ctx, target); err != nil {
		return connector.SourceFlushEvidence{}, err
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var observed *string
		if err := s.lagPool.QueryRow(ctx, `
SELECT confirmed_flush_lsn::text
FROM pg_catalog.pg_replication_slots
WHERE slot_name=$1`, s.slot).Scan(&observed); err != nil {
			return connector.SourceFlushEvidence{}, fmt.Errorf("observe source slot flush: %w", err)
		}
		if observed != nil && strings.TrimSpace(*observed) != "" {
			observedLSN, err := pglogrepl.ParseLSN(*observed)
			if err != nil {
				return connector.SourceFlushEvidence{}, fmt.Errorf("parse observed source slot flush: %w", err)
			}
			if observedLSN >= target {
				if s.stateStore != nil {
					if err := s.stateStore.RecordAck(ctx, s.stateID, observedLSN.String()); err != nil {
						return connector.SourceFlushEvidence{}, err
					}
				}
				return connector.SourceFlushEvidence{ObservedFlushLSN: observedLSN.String()}, nil
			}
		}
		select {
		case <-ctx.Done():
			return connector.SourceFlushEvidence{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Source) Close(ctx context.Context) error {
	return s.closeResources(ctx, true)
}

func (s *Source) closeResources(ctx context.Context, updateState bool) error {
	var stopErr error
	if s.stream != nil {
		stopErr = s.stream.Stop(ctx)
		s.stream = nil
	}
	if s.stateStore != nil {
		if updateState {
			_ = s.stateStore.UpdateState(ctx, s.stateID, "stopped")
		}
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
	if s.lagPool != nil {
		s.lagPool.Close()
		s.lagPool = nil
	}
	s.changes = nil
	s.pendingChange = nil
	return stopErr
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
		Support: connector.SupportExperimental,
		Evidence: connector.ContractEvidence{
			SchemaEvolution: true,
		},
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

// ReplicationLag returns the current replication lag in bytes for this source's slot.
func (s *Source) ReplicationLag(ctx context.Context) (string, int64, error) {
	if s.lagPool == nil {
		return s.slot, 0, errors.New("lag pool not initialized")
	}

	var lagBytes int64
	err := s.lagPool.QueryRow(ctx, `
		SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
		FROM pg_replication_slots
		WHERE slot_name = $1
	`, s.slot).Scan(&lagBytes)
	if err != nil {
		return s.slot, 0, fmt.Errorf("query replication lag: %w", err)
	}

	return s.slot, lagBytes, nil
}

// ManagedPostgresMajor reports the live source major admitted during Open.
func (s *Source) ManagedPostgresMajor() int {
	return s.managedPostgresMajor
}

func validateManagedPostgresServerVersion(ctx context.Context, pool *pgxpool.Pool, profileName string) (int, error) {
	profileName = strings.TrimSpace(profileName)
	if profileName == "" {
		return 0, nil
	}
	if profileName != connector.ManagedProfilePostgresToPostgresV1 {
		return 0, fmt.Errorf("unsupported PostgreSQL managed profile %q", profileName)
	}
	var raw string
	if err := pool.QueryRow(ctx, "SHOW server_version_num").Scan(&raw); err != nil {
		return 0, fmt.Errorf("read PostgreSQL server version for managed profile: %w", err)
	}
	versionNumber, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse PostgreSQL server_version_num %q: %w", raw, err)
	}
	major := versionNumber / 10000
	if !connector.PostgresToPostgresV1Profile().SupportsPostgresVersion(major) {
		return 0, fmt.Errorf("managed profile %s does not admit PostgreSQL %d", profileName, major)
	}
	return major, nil
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
