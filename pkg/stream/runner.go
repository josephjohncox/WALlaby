package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/josephjohncox/wallaby/internal/checkpoint"
	"github.com/josephjohncox/wallaby/internal/replication"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/spec"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultRetryAttempts = 3
	defaultRetryBackoff  = 50 * time.Millisecond
)

var (
	ddlGatedCounter metric.Int64Counter
	ddlGatedOnce    sync.Once
)

// DestinationConfig binds a destination to its spec.
type DestinationConfig struct {
	Spec               connector.Spec
	Dest               connector.Destination
	Projector          Projector
	MappingFingerprint string
}

// StagingResolver is implemented by destinations that can resolve staging tables.
type StagingResolver interface {
	ResolveStaging(ctx context.Context) error
}

// StagingResolverFor lets destinations resolve staging tables for known schemas.
type StagingResolverFor interface {
	ResolveStagingFor(ctx context.Context, schemas []connector.Schema) error
}

// Runner streams data from a source to destinations.
type Runner struct {
	Source              connector.Source
	SourceSpec          connector.Spec
	Destinations        []DestinationConfig
	Checkpoints         connector.CheckpointStore
	CheckpointOutbox    connector.CheckpointOutboxStore
	FlowID              string
	ResolveStaging      bool
	Tracer              trace.Tracer
	Meters              *telemetry.Meters
	BatchTimeout        time.Duration
	MaxEmptyReads       int
	WireFormat          connector.WireFormat
	StrictFormat        bool
	Parallelism         int
	AckPolicy           AckPolicy
	PrimaryDestination  string
	RequireDDLExecution bool
	FailureMode         FailureMode
	GiveUpPolicy        GiveUpPolicy
	DDLExecutions       DDLExecutionStore
	TraceSink           TraceSink
	RunFence            *connector.RunFence
	DeliveryCoordinator ManagedDeliveryCoordinator
	ArtifactLog         ManagedArtifactLog
}

// Run executes the streaming loop until context cancellation or error. It requires
// a stable flow ID and durable checkpoint storage before acknowledging the source.
func (r *Runner) Run(ctx context.Context) (retErr error) {
	if r.Source == nil {
		return errors.New("source is required")
	}
	if len(r.Destinations) == 0 {
		return errors.New("at least one destination is required")
	}
	if r.FlowID == "" {
		return errors.New("a non-empty flow id is required for durable checkpoints")
	}
	if r.effectiveAckPolicy() != AckPolicyPrimary && r.Checkpoints == nil {
		return errors.New("a durable checkpoint store is required before source acknowledgement")
	}
	if err := ValidateDestinationContracts(
		r.Destinations,
		r.effectiveAckPolicy(),
		r.PrimaryDestination,
		r.RequireDDLExecution,
	); err != nil {
		return fmt.Errorf("validate destination contracts: %w", err)
	}
	if r.RequireDDLExecution && r.DDLExecutions == nil {
		return errors.New("automatic DDL execution requires durable execution receipt storage")
	}
	if r.effectiveAckPolicy() == AckPolicyMaterialized && !r.managed() {
		return errors.New("ack_policy=materialized requires managed PostgreSQL transactional execution")
	}
	if r.managed() {
		if r.RunFence == nil || r.DeliveryCoordinator == nil {
			return errors.New("managed execution requires a run fence and delivery coordinator")
		}
		if r.effectiveAckPolicy() != AckPolicyAll && r.effectiveAckPolicy() != AckPolicyMaterialized {
			return errors.New("managed PostgreSQL execution requires ack_policy=all or materialized")
		}
		if len(r.Destinations) != 1 {
			return errors.New("managed PostgreSQL execution currently requires exactly one destination revision")
		}
		if _, ok := r.Source.(connector.TransactionalSource); !ok {
			return errors.New("managed PostgreSQL execution requires a transactional source")
		}
		if _, ok := r.Source.(connector.FlushEvidenceSource); !ok {
			return errors.New("managed PostgreSQL execution requires observed source flush evidence")
		}
		if r.effectiveAckPolicy() == AckPolicyMaterialized {
			if r.ArtifactLog == nil {
				return errors.New("materialized acknowledgement requires the PostgreSQL-authoritative artifact log")
			}
		} else if _, ok := r.Destinations[0].Dest.(connector.ManagedTransactionDestination); !ok {
			return errors.New("managed execution requires a full-transaction reconcilable destination driver")
		}
		if _, ok := r.Checkpoints.(checkpoint.FencedStore); !ok {
			return errors.New("managed execution requires a generation-fenced PostgreSQL checkpoint store")
		}
	}

	defer func() {
		if retErr == nil {
			return
		}
		if r.effectiveFailureMode() != FailureModeDropSlot {
			return
		}
		if errors.Is(retErr, context.Canceled) || errors.Is(retErr, context.DeadlineExceeded) {
			return
		}
		if dropper, ok := r.Source.(connector.SlotDropper); ok {
			if err := dropper.DropSlot(ctx); err != nil {
				retErr = fmt.Errorf("%w (drop slot failed: %s)", retErr, err.Error())
			}
		}
	}()

	tracer := r.Tracer
	if tracer == nil {
		tracer = otel.Tracer("wallaby/stream")
	}
	if err := r.normalizeWireFormat(); err != nil {
		return err
	}

	ackPolicy := r.effectiveAckPolicy()
	checkpointStore := r.Checkpoints
	var outbox connector.CheckpointOutboxStore
	var primary DestinationConfig
	var secondary []DestinationConfig
	if ackPolicy == AckPolicyPrimary {
		if r.FlowID == "" {
			return errors.New("primary acknowledgement requires a non-empty flow id")
		}
		outbox = r.CheckpointOutbox
		if outbox == nil {
			return errors.New("primary acknowledgement requires a durable checkpoint store with atomic outbox support")
		}
		checkpointStore = outbox
		var err error
		primary, secondary, err = r.partitionDestinations()
		if err != nil {
			return err
		}
	}
	if r.FlowID != "" {
		if r.SourceSpec.Options == nil {
			r.SourceSpec.Options = map[string]string{}
		}
		if r.SourceSpec.Options["flow_id"] == "" {
			r.SourceSpec.Options["flow_id"] = r.FlowID
		}
		for i := range r.Destinations {
			spec := r.Destinations[i].Spec
			if spec.Options == nil {
				spec.Options = map[string]string{}
			}
			if spec.Options["flow_id"] == "" {
				spec.Options["flow_id"] = r.FlowID
			}
			r.Destinations[i].Spec = spec
		}
	}

	var restoredCheckpoint *connector.Checkpoint
	var restoredArtifactGrant *connector.AckGrant
	ackRestoredCheckpoint := false
	explicitStartLSN := ""
	if r.SourceSpec.Options != nil {
		explicitStartLSN = r.SourceSpec.Options["start_lsn"]
	}
	if checkpointStore != nil && r.FlowID != "" {
		var cp connector.Checkpoint
		var err error
		if r.managed() {
			cp, err = checkpointStore.(checkpoint.FencedStore).GetFenced(ctx, *r.RunFence)
		} else {
			cp, err = checkpointStore.Get(ctx, r.FlowID)
		}
		switch {
		case err == nil:
			restoredCheckpoint = &cp
			ackRestoredCheckpoint = explicitStartLSN == "" || checkpointPositionsEqual(explicitStartLSN, cp.LSN)
			if cp.LSN != "" {
				if r.SourceSpec.Options == nil {
					r.SourceSpec.Options = map[string]string{}
				}
				if explicitStartLSN == "" {
					r.SourceSpec.Options["start_lsn"] = cp.LSN
				}
			}
		case errors.Is(err, connector.ErrCheckpointNotFound):
			// A new flow has no restore position yet.
		default:
			return fmt.Errorf("restore checkpoint: %w", err)
		}
	}

	openedDestinations := make([]connector.Destination, 0, len(r.Destinations))
	defer func() {
		for index := len(openedDestinations) - 1; index >= 0; index-- {
			_ = openedDestinations[index].Close(ctx)
		}
	}()
	openDestinations := func() error {
		if len(openedDestinations) != 0 {
			return nil
		}
		for _, destination := range r.Destinations {
			if destination.Dest == nil {
				return errors.New("destination is required")
			}
			if err := destination.Dest.Open(ctx, destination.Spec); err != nil {
				return fmt.Errorf("open destination %s: %w", destination.Spec.Name, err)
			}
			openedDestinations = append(openedDestinations, destination.Dest)
		}
		return nil
	}

	if r.managed() && r.effectiveAckPolicy() == AckPolicyMaterialized {
		if err := r.ArtifactLog.Recover(ctx, *r.RunFence); err != nil {
			return fmt.Errorf("recover canonical artifact publication: %w", err)
		}
		if err := r.ArtifactLog.WaitForReadAdmission(ctx, *r.RunFence); err != nil {
			return fmt.Errorf("restore canonical artifact backpressure: %w", err)
		}
	}

	bootstrapMode := "never"
	if r.managed() {
		bootstrapMode = strings.ToLower(strings.TrimSpace(r.SourceSpec.Options["bootstrap"]))
		if bootstrapMode == "" {
			bootstrapMode = "auto"
		}
	}
	if r.managed() && bootstrapMode != "never" {
		if err := openDestinations(); err != nil {
			return err
		}
		bootstrapSource, ok := r.Source.(connector.ManagedBootstrapSource)
		if !ok {
			return errors.New("managed bootstrap source contract is missing")
		}
		bootstrapDestination, ok := r.Destinations[0].Dest.(connector.ManagedBootstrapDestination)
		if !ok {
			return errors.New("managed bootstrap destination contract is missing")
		}
		destinationRevisionID := strings.TrimSpace(r.Destinations[0].Spec.Options["destination_revision_id"])
		bootstrapProjector, ok := r.Destinations[0].Projector.(connector.ManagedBootstrapProjector)
		if !ok || strings.TrimSpace(bootstrapProjector.Fingerprint()) == "" {
			return errors.New("managed bootstrap requires a typed projection bound to a durable mapping fingerprint")
		}
		bootstrapResult, err := bootstrapSource.PrepareManagedBootstrap(ctx, *r.RunFence, r.SourceSpec, destinationRevisionID, bootstrapProjector, bootstrapDestination)
		if err != nil {
			return fmt.Errorf("prepare managed bootstrap: %w", err)
		}
		if r.SourceSpec.Options == nil {
			r.SourceSpec.Options = map[string]string{}
		}
		for key, value := range bootstrapResult.SourceOptions {
			r.SourceSpec.Options[key] = value
		}
		if bootstrapResult.CheckpointValid {
			if restoredCheckpoint != nil && !checkpointPositionsEqual(restoredCheckpoint.LSN, bootstrapResult.Checkpoint.LSN) {
				return fmt.Errorf("%w: restored checkpoint %s differs from bootstrap handoff %s", connector.ErrDeliveryConflict, restoredCheckpoint.LSN, bootstrapResult.Checkpoint.LSN)
			}
			checkpoint := bootstrapResult.Checkpoint
			restoredCheckpoint = &checkpoint
			ackRestoredCheckpoint = true
		}
	}

	if r.managed() {
		if r.SourceSpec.Options == nil {
			r.SourceSpec.Options = map[string]string{}
		}
		// Snapshot bootstrap performs all resource mutation before source Open.
		// The Snowflake SQL clean-start path is the sole exception: Source.Open
		// creates and roots one deterministic slot while holding the RunFence.
		allowSnowflakeSourceCut := bootstrapMode == "never" && restoredCheckpoint == nil &&
			strings.TrimSpace(r.SourceSpec.Options["managed_profile"]) == connector.ManagedProfilePostgresToSnowflakeSQLV1 &&
			strings.EqualFold(strings.TrimSpace(r.SourceSpec.Options["create_slot"]), "true")
		for _, option := range []string{"ensure_state", "ensure_publication", "sync_publication"} {
			r.SourceSpec.Options[option] = "false"
		}
		if !allowSnowflakeSourceCut {
			r.SourceSpec.Options["create_slot"] = "false"
		}
		if restoredCheckpoint != nil && restoredCheckpoint.Metadata != nil {
			if baselines := restoredCheckpoint.Metadata[connector.ManagedSchemaBaselinesMetadataKey]; baselines != "" {
				r.SourceSpec.Options[connector.ManagedSchemaBaselinesMetadataKey] = baselines
			}
		}
	}

	if r.managed() && r.effectiveAckPolicy() == AckPolicyMaterialized && restoredCheckpoint != nil && strings.TrimSpace(restoredCheckpoint.Metadata["artifact_publication_id"]) != "" {
		grant, err := r.ArtifactLog.RestoreCheckpoint(ctx, *r.RunFence, *restoredCheckpoint)
		if err != nil {
			return fmt.Errorf("restore canonical artifact checkpoint: %w", err)
		}
		restoredArtifactGrant = &grant
	}

	if err := r.openSource(ctx); err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() { _ = r.Source.Close(ctx) }()

	if r.managed() {
		if restoredCheckpoint == nil {
			initialSource, ok := r.Source.(connector.InitialCheckpointSource)
			if !ok {
				return errors.New("managed source does not expose its validated initial checkpoint")
			}
			initial, ok := initialSource.InitialCheckpoint()
			if !ok {
				return errors.New("managed source did not expose a validated initial checkpoint after open")
			}
			if r.effectiveAckPolicy() == AckPolicyMaterialized {
				restoredCheckpoint = &initial
				ackRestoredCheckpoint = true
			} else {
				grant, err := r.DeliveryCoordinator.AuthorizeAck(ctx, *r.RunFence, initial)
				if err != nil {
					return fmt.Errorf("persist managed initial checkpoint: %w", err)
				}
				restoredCheckpoint = &grant.Checkpoint
				ackRestoredCheckpoint = true
			}
		}
		if restoredCheckpoint != nil && ackRestoredCheckpoint {
			positionID, err := connector.CheckpointPositionID(*restoredCheckpoint)
			if err != nil {
				return fmt.Errorf("identify restored managed checkpoint: %w", err)
			}
			grant := connector.AckGrant{Checkpoint: *restoredCheckpoint, PositionID: positionID}
			if r.effectiveAckPolicy() == AckPolicyMaterialized {
				if strings.TrimSpace(restoredCheckpoint.Metadata["artifact_publication_id"]) == "" {
					grant, err = r.ArtifactLog.Append(ctx, *r.RunFence, connector.SourceTransaction{
						SourceLineageID: r.SourceSpec.Options["source_lineage_id"],
						TransactionID:   ^uint32(0),
						BeginLSN:        restoredCheckpoint.LSN,
						CommitLSN:       restoredCheckpoint.LSN,
						EndLSN:          restoredCheckpoint.LSN,
						Checkpoint:      *restoredCheckpoint,
					})
					if err != nil {
						return fmt.Errorf("materialize managed startup checkpoint: %w", err)
					}
					restoredCheckpoint = &grant.Checkpoint
				} else {
					if restoredArtifactGrant == nil {
						return errors.New("restored artifact checkpoint was not validated before source open")
					}
					grant = *restoredArtifactGrant
				}
			}
			if err := r.ackManagedGrant(ctx, grant); err != nil {
				return fmt.Errorf("restore managed source feedback: %w", err)
			}
		}
	}

	if r.effectiveAckPolicy() != AckPolicyMaterialized {
		if err := openDestinations(); err != nil {
			return err
		}
	}

	if r.managed() {
		switch strings.TrimSpace(r.SourceSpec.Options["managed_profile"]) {
		case connector.ManagedProfilePostgresToPostgresV1:
			sourceVersion, sourceOK := r.Source.(connector.ManagedPostgresVersionProvider)
			destinationVersion, destinationOK := r.Destinations[0].Dest.(connector.ManagedPostgresVersionProvider)
			if !sourceOK || !destinationOK {
				return errors.New("named managed PostgreSQL profile requires live endpoint version evidence")
			}
			if err := validateManagedPostgresMajorPair(sourceVersion.ManagedPostgresMajor(), destinationVersion.ManagedPostgresMajor()); err != nil {
				return err
			}
		case connector.ManagedProfilePostgresToClickHouseAppendV1:
			sourceVersion, sourceOK := r.Source.(connector.ManagedPostgresVersionProvider)
			destinationVersion, destinationOK := r.Destinations[0].Dest.(connector.ManagedClickHouseVersionProvider)
			if !sourceOK || !destinationOK {
				return errors.New("named managed ClickHouse profile requires live endpoint version evidence")
			}
			if err := validateManagedClickHouseVersionPair(sourceVersion.ManagedPostgresMajor(), destinationVersion.ManagedClickHouseVersion()); err != nil {
				return err
			}
		case connector.ManagedProfilePostgresToSnowflakeSQLV1:
			sourceVersion, sourceOK := r.Source.(connector.ManagedPostgresVersionProvider)
			sourcePublication, publicationOK := r.Source.(connector.ManagedPostgresPublicationProvider)
			destinationVersion, destinationOK := r.Destinations[0].Dest.(connector.ManagedSnowflakeVersionProvider)
			destinationSchema, schemaOK := r.Destinations[0].Dest.(connector.ManagedSourceSchemaValidator)
			destinationScope, scopeOK := r.Destinations[0].Dest.(connector.ManagedFlowScopeValidator)
			if !sourceOK || !publicationOK || !destinationOK || !schemaOK || !scopeOK {
				return errors.New("named managed Snowflake profile requires live endpoint version, publication, schema, and flow-scope evidence")
			}
			if err := validateManagedSnowflakeVersionPair(
				sourceVersion.ManagedPostgresMajor(),
				destinationVersion.ManagedSnowflakeVersion(),
				r.Destinations[0].Spec.Options["managed_snowflake_version"],
			); err != nil {
				return err
			}
			if err := validateManagedSnowflakePublicationRelation(
				sourcePublication.ManagedPostgresPublicationTables(),
				r.Destinations[0].Spec.Options["managed_source_schema"],
				r.Destinations[0].Spec.Options["managed_source_table"],
			); err != nil {
				return err
			}
			publicationSchemas := sourcePublication.ManagedPostgresPublicationSchemas()
			if len(publicationSchemas) != 1 {
				return fmt.Errorf("managed profile %s requires one live PostgreSQL publication schema, got %d", connector.ManagedProfilePostgresToSnowflakeSQLV1, len(publicationSchemas))
			}
			projector, ok := r.Destinations[0].Projector.(connector.ManagedBootstrapProjector)
			if !ok {
				return errors.New("managed Snowflake requires a typed destination projector")
			}
			projected, policy, included, err := projector.ProjectBootstrapSchema(publicationSchemas[0])
			if err != nil {
				return fmt.Errorf("project live managed Snowflake source schema: %w", err)
			}
			if !included || policy.Mode != connector.ResolvedWriteUpsert || policy.WatermarkColumn != "" {
				return errors.New("managed Snowflake live source projection must be included upsert without watermark")
			}
			if err := destinationSchema.ValidateManagedSourceSchema(projected); err != nil {
				return err
			}
			if err := destinationScope.ValidateManagedFlowScope(ctx, r.RunFence.FlowID, r.RunFence.FlowIncarnationID.String()); err != nil {
				return err
			}
		}
		return r.runManaged(ctx, restoredCheckpoint)
	}

	var secondaryQueues []*secondaryQueue
	if ackPolicy == AckPolicyPrimary {
		var err error
		secondaryQueues, err = r.restoreSecondaryQueues(ctx, outbox, secondary)
		if err != nil {
			return err
		}
		if err := r.flushSecondaryQueues(ctx, secondaryQueues); err != nil {
			return fmt.Errorf("drain restored primary-ack outbox: %w", err)
		}
	}

	if restoredCheckpoint != nil {
		r.emitCheckpointTrace(ctx, "restore_checkpoint", *restoredCheckpoint, "", spec.ActionNone, nil)
	}
	if restoredCheckpoint != nil && ackRestoredCheckpoint {
		if err := r.Source.Ack(ctx, *restoredCheckpoint); err != nil {
			r.emitCheckpointTrace(ctx, "restore_ack_error", *restoredCheckpoint, "", spec.ActionNone, err)
			r.Meters.RecordError(ctx, "source_restore_ack")
			return fmt.Errorf("ack restored checkpoint: %w", err)
		}
		r.emitCheckpointTrace(ctx, "restore_ack", *restoredCheckpoint, "", spec.ActionRestoreAck, nil)
	}

	emptyReads := 0
	readFailures := 0
	var positionlessPrimaryFragments []connector.Batch
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(secondaryQueues) > 0 {
			if _, err := r.drainSecondaryQueues(ctx, secondaryQueues); err != nil {
				return err
			}
		}

		batchStart := time.Now()
		batchCtx, span := tracer.Start(ctx, "stream.batch",
			trace.WithNewRoot(),
			trace.WithAttributes(
				attribute.String("flow.id", r.FlowID),
				attribute.String("source.type", string(r.SourceSpec.Type)),
			),
		)

		readStart := time.Now()
		readCtx, readSpan := tracer.Start(batchCtx, "source.read")
		batch, err := r.Source.Read(readCtx)
		readSpan.End()
		if r.Meters != nil {
			r.Meters.RecordSourceReadLatency(ctx, float64(time.Since(readStart).Milliseconds()))
			if lagProvider, ok := r.Source.(connector.ReplicationLagProvider); ok {
				if slot, lagBytes, lagErr := lagProvider.ReplicationLag(ctx); lagErr == nil {
					r.Meters.RecordSourceLag(ctx, slot, lagBytes)
				}
			}
		}
		if err != nil {
			if errors.Is(err, connector.ErrDDLApprovalRequired) {
				r.handleDDLGate(batchCtx, span, err)
				span.End()
				return err
			}
			if errors.Is(err, io.EOF) && r.isBackfill() {
				span.End()
				if len(secondaryQueues) > 0 {
					if err := r.flushSecondaryQueues(ctx, secondaryQueues); err != nil {
						return err
					}
				}
				if r.ResolveStaging {
					if err := r.resolveStaging(batchCtx); err != nil {
						return err
					}
				}
				return nil
			}
			r.emitTrace(batchCtx, "read_error", "", "", spec.ActionReadFail, err)
			r.Meters.RecordError(ctx, "source_read")
			span.RecordError(err)
			span.End()
			readFailures++
			if r.shouldGiveUp(readFailures) {
				return err
			}
			if err := r.sleepRetry(ctx); err != nil {
				return err
			}
			continue
		}
		if err := connector.ValidateBatch(batch); err != nil {
			validationErr := fmt.Errorf("validate source batch: %w", err)
			r.emitTrace(batchCtx, "read_error", batch.Checkpoint.LSN, "", spec.ActionReadFail, validationErr)
			r.Meters.RecordError(ctx, "source_batch_validation")
			span.RecordError(validationErr)
			span.End()
			return validationErr
		}
		readFailures = 0
		_, durablePositionErr := connector.CheckpointPositionID(batch.Checkpoint)
		hasDurablePosition := durablePositionErr == nil
		positionlessPostgres := r.SourceSpec.Type == connector.EndpointPostgres && strings.TrimSpace(batch.Checkpoint.LSN) == "" && !isControlCheckpoint(batch.Checkpoint) && batch.Checkpoint.Metadata["mode"] != connector.SourceModeBackfill
		if len(batch.Records) > 0 && (!hasDurablePosition || positionlessPostgres) {
			if r.SourceSpec.Type != connector.EndpointPostgres {
				span.End()
				return errors.New("source emitted records without a durable checkpoint")
			}
			// PostgreSQL compatibility reads may split one committed transaction
			// into table-scoped fragments. Deliver intermediate fragments but do
			// not checkpoint or acknowledge until the final fragment carries the
			// transaction-end LSN. Primary acknowledgement writes the primary now
			// and retains source batches in memory so all secondary fragments enter
			// the authoritative outbox with the final checkpoint.
			targets := r.Destinations
			if ackPolicy == AckPolicyPrimary {
				targets = []DestinationConfig{primary}
			}
			if err := r.writeWithRetry(batchCtx, batch, targets); err != nil {
				span.RecordError(err)
				span.End()
				return err
			}
			if ackPolicy == AckPolicyPrimary {
				positionlessPrimaryFragments = append(positionlessPrimaryFragments, batch)
			}
			r.Meters.RecordBatch(ctx, r.FlowID, int64(len(batch.Records)), float64(time.Since(batchStart).Milliseconds()))
			span.End()
			continue
		}
		if len(batch.Records) == 0 && !hasDurablePosition {
			// Sources may emit an empty heartbeat before they have a durable
			// position. It carries no data and must not be traced, persisted, or
			// acknowledged as a checkpoint. Empty batches with a source position
			// are durable progress and must advance the slot.
			emptyReads++
			span.End()
			if r.MaxEmptyReads > 0 && emptyReads >= r.MaxEmptyReads {
				if len(secondaryQueues) > 0 {
					if err := r.flushSecondaryQueues(ctx, secondaryQueues); err != nil {
						return err
					}
				}
				if r.ResolveStaging && r.isBackfill() {
					if err := r.resolveStaging(batchCtx); err != nil {
						return err
					}
				}
				return nil
			}
			continue
		}

		tracePosition, positionErr := connector.CheckpointPositionID(batch.Checkpoint)
		if positionErr != nil {
			span.RecordError(positionErr)
			span.End()
			return fmt.Errorf("identify checkpoint: %w", positionErr)
		}
		if len(ddlRecordsInBatch(batch)) > 0 {
			r.emitCheckpointTrace(batchCtx, "read", batch.Checkpoint, tracePosition, spec.ActionReadDDL, nil)
		} else {
			r.emitCheckpointTrace(batchCtx, "read", batch.Checkpoint, tracePosition, spec.ActionReadBatch, nil)
		}
		if r.WireFormat != "" {
			batch.WireFormat = r.WireFormat
		}

		if len(batch.Records) == 0 {
			r.emitCheckpointTrace(batchCtx, "deliver", batch.Checkpoint, tracePosition, spec.ActionDeliver, nil)
			emitControlCheckpoint := isControlCheckpoint(batch.Checkpoint)
			var err error
			if ackPolicy == AckPolicyPrimary {
				outboxBatches := primaryAckOutboxBatches(batch, positionlessPrimaryFragments, tracePosition)
				err = r.ackPrimaryAndOutbox(batchCtx, outbox, batch, positionlessPrimaryFragments, secondary, tracePosition, emitControlCheckpoint)
				if err == nil {
					positionlessPrimaryFragments = nil
					enqueuePrimaryAckBatches(secondaryQueues, outboxBatches)
					_, err = r.drainSecondaryQueues(batchCtx, secondaryQueues)
				}
			} else {
				err = r.ackAndCheckpoint(batchCtx, batch.Checkpoint, tracePosition, emitControlCheckpoint)
			}
			if err != nil {
				span.RecordError(err)
				span.End()
				return err
			}
			span.End()
			continue
		}
		emptyReads = 0

		span.SetAttributes(
			attribute.Int("batch.records", len(batch.Records)),
			attribute.String("batch.schema", batch.Schema.Name),
		)

		ddlRecords := ddlRecordsInBatch(batch)

		if ackPolicy == AckPolicyPrimary {
			writeStart := time.Now()
			if err := r.writeWithRetry(batchCtx, batch, []DestinationConfig{primary}); err != nil {
				r.emitTrace(batchCtx, "write_error", batch.Checkpoint.LSN, "", spec.ActionWriteFail, err)
				r.Meters.RecordError(ctx, "destination_write")
				span.RecordError(err)
				span.End()
				return err
			}
			r.Meters.RecordDestinationWrite(ctx, r.FlowID, float64(time.Since(writeStart).Milliseconds()))
			r.emitCheckpointTrace(batchCtx, "deliver", batch.Checkpoint, tracePosition, spec.ActionDeliver, nil)
			outboxBatches := primaryAckOutboxBatches(batch, positionlessPrimaryFragments, tracePosition)
			if err := r.ackPrimaryAndOutbox(batchCtx, outbox, batch, positionlessPrimaryFragments, secondary, tracePosition, false); err != nil {
				span.RecordError(err)
				span.End()
				return err
			}
			positionlessPrimaryFragments = nil
			enqueuePrimaryAckBatches(secondaryQueues, outboxBatches)
			if _, err := r.drainSecondaryQueues(batchCtx, secondaryQueues); err != nil {
				span.RecordError(err)
				span.End()
				return err
			}
			r.Meters.RecordBatch(ctx, r.FlowID, int64(len(batch.Records)), float64(time.Since(batchStart).Milliseconds()))
			span.End()
			continue
		}

		writeStart := time.Now()
		writeCtx, writeSpan := tracer.Start(batchCtx, "destination.write",
			trace.WithAttributes(
				attribute.Int("destinations.count", len(r.Destinations)),
			),
		)
		if err := r.writeWithRetry(writeCtx, batch, r.Destinations); err != nil {
			r.emitTrace(batchCtx, "write_error", batch.Checkpoint.LSN, "", spec.ActionWriteFail, err)
			r.Meters.RecordError(ctx, "destination_write")
			writeSpan.RecordError(err)
			writeSpan.End()
			span.RecordError(err)
			span.End()
			return err
		}
		writeLatencyMs := float64(time.Since(writeStart).Milliseconds())
		writeSpan.SetAttributes(attribute.Float64("latency_ms", writeLatencyMs))
		writeSpan.End()
		r.Meters.RecordDestinationWrite(ctx, r.FlowID, float64(time.Since(writeStart).Milliseconds()))
		span.SetAttributes(
			attribute.Float64("destination.write_latency_ms", writeLatencyMs),
		)
		r.emitDDLAppliedTrace(batchCtx, batch.Checkpoint, ddlRecords)
		r.emitCheckpointTrace(batchCtx, "deliver", batch.Checkpoint, tracePosition, spec.ActionDeliver, nil)
		if err := r.ackAndCheckpoint(batchCtx, batch.Checkpoint, tracePosition, false); err != nil {
			span.RecordError(err)
			span.End()
			return err
		}

		batchLatencyMs := float64(time.Since(batchStart).Milliseconds())
		r.Meters.RecordBatch(ctx, r.FlowID, int64(len(batch.Records)), batchLatencyMs)
		span.SetAttributes(
			attribute.Float64("batch.latency_ms", batchLatencyMs),
		)
		span.End()
	}
}

// ManagedProfileEnabled reports whether this runner will use the fenced,
// full-transaction managed execution path. A named profile implies managed
// execution even when the legacy managed=true option is omitted.
func validateManagedPostgresMajorPair(sourceMajor, destinationMajor int) error {
	if sourceMajor <= 0 || destinationMajor <= 0 {
		return errors.New("named managed PostgreSQL profile requires positive live server majors")
	}
	if connector.PostgresToPostgresV1Profile().SameMajorOnly && sourceMajor != destinationMajor {
		return fmt.Errorf("managed PostgreSQL profile requires matching source and destination majors; got %d and %d", sourceMajor, destinationMajor)
	}
	return nil
}

func validateManagedClickHouseVersionPair(sourceMajor int, clickHouseVersion string) error {
	profile := connector.PostgresToClickHouseAppendV1Profile()
	if !profile.SupportsPostgresVersion(sourceMajor) {
		return fmt.Errorf("managed profile %s does not admit PostgreSQL %d", profile.Name, sourceMajor)
	}
	if !profile.SupportsClickHouseVersion(clickHouseVersion) {
		return fmt.Errorf("managed profile %s does not admit ClickHouse %s", profile.Name, clickHouseVersion)
	}
	return nil
}

func validateManagedSnowflakePublicationRelation(publicationTables []string, sourceSchema, sourceTable string) error {
	expectedRelation := strings.TrimSpace(sourceSchema) + "." + strings.TrimSpace(sourceTable)
	if len(publicationTables) != 1 || publicationTables[0] != expectedRelation {
		return fmt.Errorf("managed profile %s requires live PostgreSQL publication relation [%s], got %v", connector.ManagedProfilePostgresToSnowflakeSQLV1, expectedRelation, publicationTables)
	}
	return nil
}

func validateManagedSnowflakeVersionPair(sourceMajor int, snowflakeVersion, exactPin string) error {
	profile := connector.PostgresToSnowflakeSQLV1Profile()
	if !profile.SupportsPostgresVersion(sourceMajor) {
		return fmt.Errorf("managed profile %s does not admit PostgreSQL %d", profile.Name, sourceMajor)
	}
	snowflakeVersion = strings.TrimSpace(snowflakeVersion)
	exactPin = strings.TrimSpace(exactPin)
	if snowflakeVersion == "" || exactPin == "" || snowflakeVersion != exactPin {
		return fmt.Errorf("managed profile %s requires Snowflake CURRENT_VERSION()=%q to equal exact runtime pin %q", profile.Name, snowflakeVersion, exactPin)
	}
	return nil
}

func (r *Runner) ManagedProfileEnabled() bool {
	return r.managed()
}

func (r *Runner) managed() bool {
	return connector.IsManagedSourceSpec(r.SourceSpec)
}

func (r *Runner) openSource(ctx context.Context) error {
	for {
		err := r.Source.Open(ctx, r.SourceSpec)
		if err == nil {
			return nil
		}
		if !r.managed() || !errors.Is(err, replication.ErrReplicationSlotActive) {
			return err
		}
		if r.Meters != nil {
			r.Meters.RecordError(ctx, "source_slot_active")
		}
		if err := r.sleepRetry(ctx); err != nil {
			return err
		}
	}
}

func (r *Runner) runManaged(ctx context.Context, restored *connector.Checkpoint) error {
	source := r.Source.(connector.TransactionalSource)
	fence := *r.RunFence
	materialized := r.effectiveAckPolicy() == AckPolicyMaterialized
	var destination DestinationConfig
	var driver connector.ManagedTransactionDestination
	coordinator := r.DeliveryCoordinator
	if !materialized {
		destination = r.Destinations[0]
		driver = destination.Dest.(connector.ManagedTransactionDestination)
	}
	managedMetadata := map[string]string{}
	if restored != nil {
		for key, value := range restored.Metadata {
			managedMetadata[key] = value
		}
	}

	for {
		if materialized {
			// Durable backlog is restored before source open and rechecked before
			// every subsequent read. No in-memory batch is needed to enforce it.
			if err := r.ArtifactLog.WaitForReadAdmission(ctx, fence); err != nil {
				return fmt.Errorf("wait for canonical artifact backlog: %w", err)
			}
		}
		transaction, err := source.ReadTransaction(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read managed source transaction: %w", err)
		}
		if err := transaction.Validate(); err != nil {
			return fmt.Errorf("validate managed source transaction: %w", err)
		}
		transaction.Checkpoint.Metadata, err = connector.MergeManagedSchemaBaselines(managedMetadata, transaction)
		if err != nil {
			return fmt.Errorf("merge managed schema baselines: %w", err)
		}
		// Materialized Iceberg projection is performed exactly once inside the
		// projection-bound canonical v2 artifact runtime. Ordinary managed delivery
		// projects before transaction identity, DDL, intent creation, and I/O.
		if !materialized && destination.Projector != nil {
			projected, _, projectErr := destination.Projector.ProjectTransaction(transaction)
			if projectErr != nil {
				return fmt.Errorf("project managed destination %s: %w", destination.Spec.Name, projectErr)
			}
			transaction = projected
		}
		expectedLineage := strings.TrimSpace(r.SourceSpec.Options["source_lineage_id"])
		if transaction.SourceLineageID == "" || transaction.SourceLineageID != expectedLineage {
			return fmt.Errorf("managed source transaction lineage %q does not match configured %q", transaction.SourceLineageID, expectedLineage)
		}
		positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
		if err != nil {
			return fmt.Errorf("identify managed delivery: %w", err)
		}
		readAction := spec.ActionReadBatch
		if sourceTransactionContainsDDL(transaction) {
			readAction = spec.ActionReadDDL
		}
		r.emitCheckpointTrace(ctx, "read", transaction.Checkpoint, positionID, readAction, nil)

		if materialized {
			grant, err := r.ArtifactLog.Append(ctx, fence, transaction)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("publish canonical source transaction: %w", err)
			}
			r.emitCheckpointTrace(ctx, "deliver", grant.Checkpoint, grant.PositionID, spec.ActionDeliver, nil)
			r.emitCheckpointTrace(ctx, "checkpoint", grant.Checkpoint, grant.PositionID, spec.ActionPersistCheckpoint, nil)
			// Append has committed the immutable roots, delivery rows, quota,
			// checkpoint, and ACK intent. Source feedback is strictly later.
			if err := r.ackManagedGrant(ctx, grant); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("ack materialized source transaction: %w", err)
			}
			managedMetadata = grant.Checkpoint.Metadata
			continue
		}

		if len(transaction.Fragments) == 0 {
			grant, err := r.DeliveryCoordinator.AuthorizeAck(ctx, fence, transaction.Checkpoint)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("authorize empty managed source transaction ack: %w", err)
			}
			r.emitCheckpointTrace(ctx, "deliver", grant.Checkpoint, grant.PositionID, spec.ActionDeliver, nil)
			r.emitCheckpointTrace(ctx, "checkpoint", grant.Checkpoint, grant.PositionID, spec.ActionPersistCheckpoint, nil)
			if err := r.ackManagedGrant(ctx, grant); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("ack empty managed source transaction: %w", err)
			}
			managedMetadata = grant.Checkpoint.Metadata
			continue
		}
		contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
		if err != nil {
			return fmt.Errorf("identify managed source transaction: %w", err)
		}
		destinationRevisionID := strings.TrimSpace(destination.Spec.Options["destination_revision_id"])
		if destinationRevisionID == "" {
			return errors.New("managed destination_revision_id is required")
		}
		intent := connector.DeliveryIntent{
			FlowID:                fence.FlowID,
			FlowIncarnationID:     fence.FlowIncarnationID.String(),
			SourceLineageID:       transaction.SourceLineageID,
			Generation:            fence.Generation,
			AcquisitionID:         fence.AcquisitionID.String(),
			LeaseEpoch:            fence.LeaseEpoch,
			DestinationRevisionID: destinationRevisionID,
			LogicalBatchID:        logicalBatchID,
			PositionID:            positionID,
			ContentHash:           contentHash,
		}
		grant, err := coordinator.DeliverTransaction(ctx, fence, intent, transaction, driver)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("deliver managed source transaction: %w", err)
		}
		r.emitCheckpointTrace(ctx, "deliver", grant.Checkpoint, grant.PositionID, spec.ActionDeliver, nil)
		r.emitCheckpointTrace(ctx, "checkpoint", grant.Checkpoint, grant.PositionID, spec.ActionPersistCheckpoint, nil)
		if err := r.ackManagedGrant(ctx, grant); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("ack managed source transaction: %w", err)
		}
		managedMetadata = grant.Checkpoint.Metadata
	}
}

func (r *Runner) ackManagedGrant(ctx context.Context, grant connector.AckGrant) error {
	fence := *r.RunFence
	source := r.Source.(connector.FlushEvidenceSource)
	if err := r.DeliveryCoordinator.CommitSourceFeedback(ctx, fence, grant, source); err != nil {
		return fmt.Errorf("commit managed source feedback: %w", err)
	}
	r.emitCheckpointTrace(ctx, "source_flush", grant.Checkpoint, grant.PositionID, spec.ActionAck, nil)
	r.emitCheckpointTrace(ctx, "ack", grant.Checkpoint, grant.PositionID, spec.ActionAck, nil)
	return nil
}

func sourceTransactionContainsDDL(transaction connector.SourceTransaction) bool {
	for _, fragment := range transaction.Fragments {
		if len(ddlRecordsInBatch(fragment.Batch)) > 0 {
			return true
		}
	}
	return false
}

func checkpointPositionsEqual(left, right string) bool {
	if left == "" || right == "" {
		return left == right
	}
	cmp, err := connector.CompareCheckpointLSN(left, right)
	return err == nil && cmp == 0
}

type primaryAckOutboxBatch struct {
	batch      connector.Batch
	positionID string
}

func primaryAckOutboxBatches(final connector.Batch, preceding []connector.Batch, tracePosition string) []primaryAckOutboxBatch {
	items := make([]primaryAckOutboxBatch, 0, len(preceding)+1)
	for index, fragment := range preceding {
		fragment.Checkpoint = final.Checkpoint
		items = append(items, primaryAckOutboxBatch{batch: fragment, positionID: fmt.Sprintf("%s/fragment/%06d", tracePosition, index)})
	}
	if len(final.Records) > 0 {
		items = append(items, primaryAckOutboxBatch{batch: final, positionID: tracePosition})
	}
	return items
}

func enqueuePrimaryAckBatches(queues []*secondaryQueue, items []primaryAckOutboxBatch) {
	for _, item := range items {
		pending := newPendingBatch(item.batch, ddlRecordsInBatch(item.batch), len(queues), item.positionID)
		for _, queue := range queues {
			queue.pending = append(queue.pending, pending)
		}
	}
}

func (r *Runner) ackPrimaryAndOutbox(ctx context.Context, outbox connector.OutboxStore, batch connector.Batch, preceding []connector.Batch, secondary []DestinationConfig, tracePosition string, emitControlCheckpoint bool) error {
	batches := primaryAckOutboxBatches(batch, preceding, tracePosition)
	entries := make([]connector.OutboxEntry, 0, len(secondary)*len(batches))
	createdAt := time.Now().UTC()
	for _, item := range batches {
		for _, destination := range secondary {
			entries = append(entries, connector.OutboxEntry{
				FlowID: r.FlowID, Destination: destination.Spec.Name, PositionID: item.positionID,
				ProjectionFingerprint: destination.MappingFingerprint, Batch: item.batch, CreatedAt: createdAt,
			})
		}
		createdAt = createdAt.Add(time.Nanosecond)
	}
	if err := outbox.PersistCheckpointAndOutbox(ctx, r.FlowID, batch.Checkpoint, entries); err != nil {
		r.emitCheckpointTrace(ctx, "checkpoint_error", batch.Checkpoint, tracePosition, spec.ActionCheckpointFail, err)
		r.Meters.RecordError(ctx, "checkpoint_outbox_persist")
		return fmt.Errorf("persist checkpoint and primary-ack outbox: %w", err)
	}
	r.emitCheckpointTrace(ctx, "checkpoint", batch.Checkpoint, tracePosition, spec.ActionPersistCheckpoint, nil)
	r.Meters.RecordCheckpoint(ctx, r.FlowID)
	if err := r.Source.Ack(ctx, batch.Checkpoint); err != nil {
		r.emitCheckpointTrace(ctx, "ack_error", batch.Checkpoint, tracePosition, spec.ActionNone, err)
		r.Meters.RecordError(ctx, "source_ack")
		return fmt.Errorf("ack source: %w", err)
	}
	r.emitCheckpointTrace(ctx, "ack", batch.Checkpoint, tracePosition, spec.ActionAck, nil)
	if emitControlCheckpoint {
		r.emitCheckpointTrace(ctx, "control_checkpoint", batch.Checkpoint, tracePosition, spec.ActionNone, nil)
	}
	return nil
}

func (r *Runner) ackAndCheckpoint(ctx context.Context, checkpoint connector.Checkpoint, tracePosition string, emitControlCheckpoint bool) error {
	if r.Checkpoints != nil && r.FlowID != "" && shouldPersistCheckpoint(checkpoint) {
		if err := r.Checkpoints.Put(ctx, r.FlowID, checkpoint); err != nil {
			r.emitCheckpointTrace(ctx, "checkpoint_error", checkpoint, tracePosition, spec.ActionCheckpointFail, err)
			r.Meters.RecordError(ctx, "checkpoint_persist")
			return fmt.Errorf("persist checkpoint: %w", err)
		}
		r.emitCheckpointTrace(ctx, "checkpoint", checkpoint, tracePosition, spec.ActionPersistCheckpoint, nil)
		r.Meters.RecordCheckpoint(ctx, r.FlowID)
	}
	if err := r.Source.Ack(ctx, checkpoint); err != nil {
		r.emitCheckpointTrace(ctx, "ack_error", checkpoint, tracePosition, spec.ActionNone, err)
		r.Meters.RecordError(ctx, "source_ack")
		return fmt.Errorf("ack source: %w", err)
	}
	r.emitCheckpointTrace(ctx, "ack", checkpoint, tracePosition, spec.ActionAck, nil)
	if emitControlCheckpoint {
		r.emitCheckpointTrace(ctx, "control_checkpoint", checkpoint, tracePosition, spec.ActionNone, nil)
	}
	return nil
}

func (r *Runner) resolveStaging(ctx context.Context) error {
	for _, dest := range r.Destinations {
		if resolver, ok := dest.Dest.(StagingResolver); ok {
			if err := resolver.ResolveStaging(ctx); err != nil {
				return fmt.Errorf("resolve staging for %s: %w", dest.Spec.Name, err)
			}
		}
	}
	return nil
}

func (r *Runner) isBackfill() bool {
	if r.SourceSpec.Options == nil {
		return false
	}
	return r.SourceSpec.Options["mode"] == "backfill"
}

func (r *Runner) effectiveAckPolicy() AckPolicy {
	if r.AckPolicy == "" {
		return AckPolicyAll
	}
	return r.AckPolicy
}

func (r *Runner) effectiveGiveUpPolicy() GiveUpPolicy {
	if r.GiveUpPolicy == "" {
		return GiveUpPolicyOnRetryExhaustion
	}
	return r.GiveUpPolicy
}

func (r *Runner) effectiveFailureMode() FailureMode {
	if r.FailureMode == "" {
		return FailureModeHoldSlot
	}
	return r.FailureMode
}

func (r *Runner) retryLimit() int {
	return defaultRetryAttempts
}

func (r *Runner) retryBackoff() time.Duration {
	return defaultRetryBackoff
}

func (r *Runner) shouldGiveUp(attempts int) bool {
	if r.effectiveGiveUpPolicy() == GiveUpPolicyNever {
		return false
	}
	return attempts >= r.retryLimit()
}

func (r *Runner) sleepRetry(ctx context.Context) error {
	backoff := r.retryBackoff()
	if backoff <= 0 {
		return nil
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Runner) partitionDestinations() (DestinationConfig, []DestinationConfig, error) {
	if len(r.Destinations) == 0 {
		return DestinationConfig{}, nil, errors.New("at least one destination is required")
	}
	seen := make(map[string]struct{}, len(r.Destinations))
	for _, destination := range r.Destinations {
		if destination.Spec.Name == "" {
			return DestinationConfig{}, nil, errors.New("primary acknowledgement requires a stable name for every destination")
		}
		if _, duplicate := seen[destination.Spec.Name]; duplicate {
			return DestinationConfig{}, nil, fmt.Errorf("duplicate destination identity %q", destination.Spec.Name)
		}
		seen[destination.Spec.Name] = struct{}{}
	}
	if r.PrimaryDestination == "" {
		return r.Destinations[0], append([]DestinationConfig(nil), r.Destinations[1:]...), nil
	}
	for i, dest := range r.Destinations {
		if dest.Spec.Name == r.PrimaryDestination {
			secondary := make([]DestinationConfig, 0, len(r.Destinations)-1)
			secondary = append(secondary, r.Destinations[:i]...)
			secondary = append(secondary, r.Destinations[i+1:]...)
			return dest, secondary, nil
		}
	}
	return DestinationConfig{}, nil, fmt.Errorf("primary destination %q not found", r.PrimaryDestination)
}

func (r *Runner) writeWithRetry(ctx context.Context, batch connector.Batch, dests []DestinationConfig) error {
	if err := r.completeVacuousDDLPositions(ctx, batch); err != nil {
		return err
	}
	remaining := append([]DestinationConfig(nil), dests...)
	attempts := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		result := r.attemptDestinations(ctx, batch, remaining)
		if result.err == nil {
			return nil
		}
		remaining = result.remaining
		attempts++
		if r.shouldGiveUp(attempts) {
			return result.err
		}
		if err := r.sleepRetry(ctx); err != nil {
			return err
		}
	}
}

func (r *Runner) restoreSecondaryQueues(ctx context.Context, outbox connector.OutboxStore, secondary []DestinationConfig) ([]*secondaryQueue, error) {
	queues := make([]*secondaryQueue, 0, len(secondary))
	byDestination := make(map[string]*secondaryQueue, len(secondary))
	for _, destination := range secondary {
		if destination.Spec.Name == "" {
			return nil, errors.New("primary-ack secondary destination name is required")
		}
		if _, duplicate := byDestination[destination.Spec.Name]; duplicate {
			return nil, fmt.Errorf("duplicate secondary destination identity %q", destination.Spec.Name)
		}
		queue := &secondaryQueue{dest: destination, outbox: outbox}
		queues = append(queues, queue)
		byDestination[destination.Spec.Name] = queue
	}
	entries, err := outbox.ListOutbox(ctx, r.FlowID)
	if err != nil {
		return nil, fmt.Errorf("restore primary-ack outbox: %w", err)
	}
	groups := make(map[string]*pendingBatch)
	for _, entry := range entries {
		queue, ok := byDestination[entry.Destination]
		if !ok {
			return nil, fmt.Errorf("primary-ack outbox contains destination %q not configured as a secondary for flow %q; restore or explicitly reconcile that destination", entry.Destination, r.FlowID)
		}
		positionID, err := connector.CheckpointPositionID(entry.Batch.Checkpoint)
		if err != nil {
			return nil, fmt.Errorf("validate restored outbox entry for %s: %w", entry.Destination, err)
		}
		positionMatches := entry.PositionID == positionID || strings.HasPrefix(entry.PositionID, positionID+"/fragment/")
		if entry.FlowID != r.FlowID || !positionMatches {
			return nil, fmt.Errorf("invalid restored outbox identity flow=%q destination=%q position=%q batch_position=%q", entry.FlowID, entry.Destination, entry.PositionID, positionID)
		}
		if entry.BatchHash == "" {
			return nil, fmt.Errorf("restored outbox entry for %s at %s has no durable batch hash", entry.Destination, entry.PositionID)
		}
		if entry.ProjectionFingerprint == "" || entry.ProjectionFingerprint != queue.dest.MappingFingerprint {
			return nil, fmt.Errorf("restored outbox entry for %s at %s has projection fingerprint %q, configured %q", entry.Destination, entry.PositionID, entry.ProjectionFingerprint, queue.dest.MappingFingerprint)
		}
		pending := groups[entry.PositionID]
		if pending == nil {
			pending = newPendingBatch(entry.Batch, ddlRecordsInBatch(entry.Batch), 0, entry.PositionID)
			pending.batchHash = entry.BatchHash
			groups[entry.PositionID] = pending
		} else if pending.batchHash != entry.BatchHash {
			return nil, fmt.Errorf("primary-ack outbox position %q contains different batches across destinations", entry.PositionID)
		}
		pending.remaining++
		queue.pending = append(queue.pending, pending)
	}
	return queues, nil
}

func (r *Runner) drainSecondaryQueues(ctx context.Context, queues []*secondaryQueue) (bool, error) {
	progressed := false
	for _, queue := range queues {
		for len(queue.pending) > 0 {
			pending := queue.pending[0]
			if err := r.writeDestination(ctx, queue.dest, pending.batch); err != nil {
				pending.bumpAttempt(queue.dest.Spec.Name)
				if r.shouldGiveUp(pending.attempts[queue.dest.Spec.Name]) {
					return progressed, err
				}
				break
			}
			if pending.remaining == 1 && len(pending.ddlRecords) > 0 {
				r.emitDDLAppliedTrace(ctx, pending.batch.Checkpoint, pending.ddlRecords)
			}
			if err := queue.outbox.DeleteOutbox(ctx, r.FlowID, queue.dest.Spec.Name, pending.positionID); err != nil {
				return progressed, fmt.Errorf("delete delivered primary-ack outbox entry for %s: %w", queue.dest.Spec.Name, err)
			}
			queue.pending = queue.pending[1:]
			pending.remaining--
			progressed = true
		}
	}
	return progressed, nil
}

func (r *Runner) flushSecondaryQueues(ctx context.Context, queues []*secondaryQueue) error {
	for {
		empty := true
		for _, queue := range queues {
			if len(queue.pending) > 0 {
				empty = false
				break
			}
		}
		if empty {
			return nil
		}
		progressed, err := r.drainSecondaryQueues(ctx, queues)
		if err != nil {
			return err
		}
		if progressed {
			continue
		}
		if r.effectiveGiveUpPolicy() != GiveUpPolicyNever {
			return fmt.Errorf("secondary destinations failed to catch up")
		}
		if err := r.sleepRetry(ctx); err != nil {
			return err
		}
	}
}

type pendingBatch struct {
	batch      connector.Batch
	ddlRecords []connector.Record
	remaining  int
	positionID string
	batchHash  string
	attempts   map[string]int
}

func newPendingBatch(batch connector.Batch, ddlRecords []connector.Record, remaining int, positionID string) *pendingBatch {
	return &pendingBatch{
		batch:      batch,
		ddlRecords: ddlRecords,
		remaining:  remaining,
		positionID: positionID,
		attempts:   make(map[string]int),
	}
}

func (p *pendingBatch) bumpAttempt(dest string) {
	p.attempts[dest]++
}

type secondaryQueue struct {
	dest    DestinationConfig
	outbox  connector.OutboxStore
	pending []*pendingBatch
}

func (r *Runner) normalizeWireFormat() error {
	if r.WireFormat == "" {
		return nil
	}
	if r.SourceSpec.Options == nil {
		r.SourceSpec.Options = map[string]string{}
	}
	if srcFormat := r.SourceSpec.Options["format"]; srcFormat != "" && connector.WireFormat(srcFormat) != r.WireFormat {
		if r.StrictFormat {
			return fmt.Errorf("source format %s does not match flow format %s", srcFormat, r.WireFormat)
		}
	} else if r.SourceSpec.Options["format"] == "" {
		r.SourceSpec.Options["format"] = string(r.WireFormat)
	}

	for i := range r.Destinations {
		spec := r.Destinations[i].Spec
		if spec.Options == nil {
			spec.Options = map[string]string{}
		}
		if destFormat := spec.Options["format"]; destFormat != "" && connector.WireFormat(destFormat) != r.WireFormat {
			if r.StrictFormat {
				return fmt.Errorf("destination %s format %s does not match flow format %s", spec.Name, destFormat, r.WireFormat)
			}
		} else if spec.Options["format"] == "" {
			spec.Options["format"] = string(r.WireFormat)
		}
		r.Destinations[i].Spec = spec
	}

	return nil
}

func isControlCheckpoint(cp connector.Checkpoint) bool {
	if cp.Metadata == nil {
		return false
	}
	if cp.Metadata["mode"] == "backfill" {
		return true
	}
	if cp.Metadata["done"] == "true" {
		return true
	}
	if cp.Metadata["control"] == "true" {
		return true
	}
	return false
}

func shouldPersistCheckpoint(cp connector.Checkpoint) bool {
	return cp.LSN != "" || len(cp.Metadata) > 0
}

type destinationAttemptResult struct {
	remaining []DestinationConfig
	err       error
}

func (r *Runner) attemptDestinations(ctx context.Context, batch connector.Batch, dests []DestinationConfig) destinationAttemptResult {
	if len(dests) == 0 {
		return destinationAttemptResult{}
	}
	if err := ctx.Err(); err != nil {
		return destinationAttemptResult{remaining: append([]DestinationConfig(nil), dests...), err: err}
	}

	parallelism := r.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism == 1 || len(dests) == 1 {
		for index, dest := range dests {
			if err := ctx.Err(); err != nil {
				return destinationAttemptResult{
					remaining: append([]DestinationConfig(nil), dests[index:]...),
					err:       err,
				}
			}
			if err := r.writeDestination(ctx, dest, batch); err != nil {
				return destinationAttemptResult{
					remaining: append([]DestinationConfig(nil), dests[index:]...),
					err:       err,
				}
			}
		}
		return destinationAttemptResult{}
	}

	if parallelism > len(dests) {
		parallelism = len(dests)
	}

	sem := make(chan struct{}, parallelism)
	attemptErrors := make([]error, len(dests))
	var wg sync.WaitGroup
	for index, dest := range dests {
		wg.Add(1)
		go func(index int, dest DestinationConfig) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				attemptErrors[index] = ctx.Err()
				return
			}
			if err := ctx.Err(); err != nil {
				attemptErrors[index] = err
				return
			}
			attemptErrors[index] = r.writeDestination(ctx, dest, batch)
		}(index, dest)
	}
	wg.Wait()

	remaining := make([]DestinationConfig, 0, len(dests))
	failures := make([]error, 0, len(dests))
	for index, err := range attemptErrors {
		if err == nil {
			continue
		}
		remaining = append(remaining, dests[index])
		failures = append(failures, err)
	}
	if len(failures) == 0 {
		return destinationAttemptResult{}
	}
	return destinationAttemptResult{remaining: remaining, err: errors.Join(failures...)}
}

func ddlRecordsInBatch(batch connector.Batch) []connector.Record {
	if len(batch.Records) == 0 {
		return nil
	}
	records := make([]connector.Record, 0)
	for _, record := range batch.Records {
		if record.Operation == connector.OpDDL || record.DDL != "" || len(record.DDLPlan) > 0 {
			records = append(records, record)
		}
	}
	return records
}

func (r *Runner) emitDDLAppliedTrace(ctx context.Context, checkpoint connector.Checkpoint, records []connector.Record) {
	if len(records) == 0 || checkpoint.LSN == "" {
		return
	}
	for _, record := range records {
		ddlText := record.DDL
		if ddlText == "" && len(record.DDLPlan) > 0 {
			ddlText = string(record.DDLPlan)
		}
		if r.TraceSink != nil {
			r.TraceSink.Emit(ctx, TraceEvent{
				Kind:       "ddl_applied",
				Spec:       spec.SpecCDCFlow,
				SpecAction: spec.ActionApplyDDL,
				LSN:        ddlRecordPosition(record, checkpoint),
				FlowID:     r.FlowID,
				DDL:        ddlText,
			})
		}
	}
}

func (r *Runner) handleDDLGate(ctx context.Context, span trace.Span, err error) {
	if span == nil {
		span = trace.SpanFromContext(ctx)
	}
	gate, _ := connector.AsDDLGate(err)
	attrs := []attribute.KeyValue{
		attribute.Bool("ddl.gated", true),
	}
	if r.FlowID != "" {
		attrs = append(attrs, attribute.String("flow.id", r.FlowID))
	}
	if gate != nil {
		if gate.FlowID != "" {
			attrs = append(attrs, attribute.String("ddl.flow_id", gate.FlowID))
		}
		if gate.LSN != "" {
			attrs = append(attrs, attribute.String("ddl.lsn", gate.LSN))
		}
		if gate.Status != "" {
			attrs = append(attrs, attribute.String("ddl.status", gate.Status))
		}
		if gate.EventID != 0 {
			attrs = append(attrs, attribute.Int64("ddl.event_id", gate.EventID))
		}
	}
	span.AddEvent("ddl.gated", trace.WithAttributes(attrs...))

	r.emitDDLGateTrace(ctx, gate, err)
	r.emitDDLGateMetric(ctx, gate)

	if gate != nil {
		log.Printf("ddl gate: flow=%s event_id=%d status=%s lsn=%s", r.FlowID, gate.EventID, gate.Status, gate.LSN)
		return
	}
	log.Printf("ddl gate: flow=%s error=%v", r.FlowID, err)
}

func (r *Runner) emitDDLGateMetric(ctx context.Context, gate *connector.DDLGateError) {
	counter := ddlGatedMetric()
	if counter == nil {
		return
	}
	attrs := []attribute.KeyValue{}
	if r.FlowID != "" {
		attrs = append(attrs, attribute.String("flow.id", r.FlowID))
	}
	if gate != nil && gate.Status != "" {
		attrs = append(attrs, attribute.String("ddl.status", gate.Status))
	}
	counter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (r *Runner) emitDDLGateTrace(ctx context.Context, gate *connector.DDLGateError, err error) {
	if r.TraceSink == nil {
		return
	}
	event := TraceEvent{
		Kind:       "ddl_gate",
		Spec:       spec.SpecCDCFlow,
		SpecAction: spec.ActionPause,
		FlowID:     r.FlowID,
	}
	if err != nil {
		event.Error = err.Error()
	}
	if gate != nil {
		if gate.FlowID != "" {
			event.FlowID = gate.FlowID
		}
		event.LSN = gate.LSN
		event.DDL = gate.DDL
		event.Detail = gate.PlanJSON
		event.EventID = gate.EventID
	}
	r.TraceSink.Emit(ctx, event)
}

func ddlGatedMetric() metric.Int64Counter {
	ddlGatedOnce.Do(func() {
		meter := otel.Meter("wallaby/stream")
		counter, err := meter.Int64Counter(
			"wallaby.ddl.gated_total",
			metric.WithDescription("Number of times a flow was gated on DDL approval."),
			metric.WithUnit("1"),
		)
		if err == nil {
			ddlGatedCounter = counter
		}
	})
	return ddlGatedCounter
}

func (r *Runner) writeDestination(ctx context.Context, dest DestinationConfig, sourceBatch connector.Batch) error {
	batch := sourceBatch
	if dest.Projector != nil {
		projected, decision, err := dest.Projector.ProjectBatch(sourceBatch)
		if err != nil {
			return fmt.Errorf("project destination %s: %w", dest.Spec.Name, err)
		}
		if decision == ProjectionFiltered {
			return nil
		}
		batch = projected
	}
	if err := ValidateDestinationTablePolicy(dest, batch.WritePolicy); err != nil {
		return err
	}
	expectedDestinations := map[string][]string{}
	capabilities, err := connector.ResolveDestinationCapabilities(dest.Dest, dest.Spec)
	if err != nil {
		return fmt.Errorf("destination %s capability profile: %w", dest.Spec.Name, err)
	}
	if !r.RequireDDLExecution || !capabilities.ExecutesDDL() {
		return r.writeDestinationLocked(ctx, dest, batch, expectedDestinations)
	}
	expectedDestinations, err = r.ddlExecutionDestinations(sourceBatch)
	if err != nil {
		return err
	}
	if r.DDLExecutions == nil {
		return errors.New("automatic DDL execution requires durable execution receipt storage")
	}
	if err := validateDDLRecordPositions(batch); err != nil {
		return err
	}
	if !batchContainsDDL(batch) {
		return r.writeDestinationLocked(ctx, dest, batch, expectedDestinations)
	}
	return r.DDLExecutions.WithDDLExecutionLock(
		ctx,
		r.FlowID,
		dest.Spec.Name,
		func() error { return r.writeDestinationLocked(ctx, dest, batch, expectedDestinations) },
	)
}

func batchContainsDDL(batch connector.Batch) bool {
	for _, record := range batch.Records {
		if record.Operation == connector.OpDDL || record.DDL != "" || len(record.DDLPlan) > 0 {
			return true
		}
	}
	return false
}

func (r *Runner) writeDestinationLocked(ctx context.Context, dest DestinationConfig, batch connector.Batch, expectedDestinations map[string][]string) error {
	type ddlExecution struct {
		position string
		ddl      string
	}
	var executedDDL []ddlExecution
	if len(batch.Records) > 0 {
		capabilities, err := connector.ResolveDestinationCapabilities(dest.Dest, dest.Spec)
		if err != nil {
			return fmt.Errorf("destination %s capability profile: %w", dest.Spec.Name, err)
		}
		if r.RequireDDLExecution && capabilities.ExecutesDDL() {
			if err := validateDDLRecordPositions(batch); err != nil {
				return err
			}
			if r.DDLExecutions == nil {
				return errors.New("automatic DDL execution requires durable execution receipt storage")
			}
			for _, record := range batch.Records {
				if record.Operation != connector.OpDDL && record.DDL == "" && len(record.DDLPlan) == 0 {
					continue
				}
				position := ddlRecordPosition(record, batch.Checkpoint)
				expected, ok := expectedDestinations[position]
				if !ok {
					return fmt.Errorf("DDL source position %s has no projected destination manifest", position)
				}
				state, err := r.DDLExecutions.PrepareDDLExecution(ctx, r.FlowID, position, dest.Spec.Name, expected)
				if err != nil {
					return fmt.Errorf("prepare ddl execution destination %s: %w", dest.Spec.Name, err)
				}
				if state == connector.DDLExecutionComplete {
					continue
				}
				applyDDL := state == connector.DDLExecutionNew
				if state == connector.DDLExecutionRetry {
					reconciler, ok := dest.Dest.(connector.DDLReconciler)
					if !ok {
						return fmt.Errorf("reconcile ddl destination %s: %w", dest.Spec.Name, connector.ErrDDLReconciliationRequired)
					}
					result, reconcileErr := reconciler.ReconcileDDL(ctx, batch.Schema, record)
					if reconcileErr != nil {
						return fmt.Errorf("reconcile ddl destination %s: %w", dest.Spec.Name, reconcileErr)
					}
					switch result {
					case connector.DDLReconcileApplied:
						r.emitTrace(ctx, "ddl_reconciled", position, dest.Spec.Name, spec.ActionApplyDDL, nil)
					case connector.DDLReconcileNotApplied:
						applyDDL = true
					default:
						return fmt.Errorf("reconcile ddl destination %s: %w", dest.Spec.Name, connector.ErrDDLReconciliationIndeterminate)
					}
				} else if !state.Valid() {
					return fmt.Errorf("prepare ddl execution destination %s returned invalid state %d", dest.Spec.Name, state)
				}
				if applyDDL {
					if err := dest.Dest.ApplyDDL(ctx, batch.Schema, record); err != nil {
						r.emitTrace(ctx, "ddl_error", position, dest.Spec.Name, spec.ActionNone, err)
						return fmt.Errorf("apply ddl destination %s: %w", dest.Spec.Name, err)
					}
					r.Meters.RecordDestinationDDL(ctx, string(dest.Spec.Type))
				}
				ddlText := record.DDL
				if ddlText == "" && len(record.DDLPlan) > 0 {
					ddlText = string(record.DDLPlan)
				}
				executedDDL = append(executedDDL, ddlExecution{position: position, ddl: ddlText})
			}
		}
	}

	destBatch := batch
	baseMappings := dest.Dest.TypeMappings()
	if transformed, ok, err := transformBatchForDestination(batch, dest.Spec, baseMappings); err != nil {
		return fmt.Errorf("transform destination %s: %w", dest.Spec.Name, err)
	} else if ok {
		destBatch = transformed
	}

	if err := dest.Dest.Write(ctx, destBatch); err != nil {
		r.emitTrace(ctx, "write_error", batch.Checkpoint.LSN, dest.Spec.Name, spec.ActionWriteFail, err)
		return fmt.Errorf("write destination %s: %w", dest.Spec.Name, err)
	}
	for _, execution := range executedDDL {
		expected, ok := expectedDestinations[execution.position]
		if !ok {
			return fmt.Errorf("DDL source position %s has no projected destination manifest", execution.position)
		}
		if err := r.DDLExecutions.RecordDDLExecution(
			ctx,
			r.FlowID,
			execution.position,
			execution.ddl,
			dest.Spec.Name,
			expected,
		); err != nil {
			return fmt.Errorf("persist ddl receipt destination %s: %w", dest.Spec.Name, err)
		}
	}
	r.emitTrace(ctx, "write", batch.Checkpoint.LSN, dest.Spec.Name, spec.ActionNone, nil)
	r.Meters.RecordDestinationWriteCount(ctx, string(dest.Spec.Type))
	return nil
}

func (r *Runner) emitCheckpointTrace(ctx context.Context, kind string, checkpoint connector.Checkpoint, positionID string, specAction spec.Action, err error) {
	if positionID == "" {
		var positionErr error
		positionID, positionErr = connector.CheckpointPositionID(checkpoint)
		if positionErr != nil {
			return
		}
	}
	if checkpoint.LSN != "" {
		r.emitTracePosition(ctx, kind, positionID, "", "", specAction, err)
		return
	}
	r.emitTracePosition(ctx, kind, "", positionID, "", specAction, err)
}

func validateDDLRecordPositions(batch connector.Batch) error {
	positions := make(map[string]struct{})
	for _, record := range ddlRecordsInBatch(batch) {
		position := ddlRecordPosition(record, batch.Checkpoint)
		if strings.TrimSpace(position) == "" {
			return errors.New("DDL record requires a durable source position")
		}
		if _, duplicate := positions[position]; duplicate {
			return fmt.Errorf("multiple DDL records share source position %q", position)
		}
		positions[position] = struct{}{}
	}
	return nil
}

func ddlRecordPosition(record connector.Record, checkpoint connector.Checkpoint) string {
	if record.SourcePosition != "" {
		return record.SourcePosition
	}
	return checkpoint.LSN
}

func (r *Runner) completeVacuousDDLPositions(ctx context.Context, sourceBatch connector.Batch) error {
	if !r.RequireDDLExecution || len(ddlRecordsInBatch(sourceBatch)) == 0 {
		return nil
	}
	if r.DDLExecutions == nil {
		return errors.New("automatic DDL execution requires durable execution receipt storage")
	}
	manifests, err := r.ddlExecutionDestinations(sourceBatch)
	if err != nil {
		return err
	}
	for _, record := range ddlRecordsInBatch(sourceBatch) {
		position := ddlRecordPosition(record, sourceBatch.Checkpoint)
		if len(manifests[position]) != 0 {
			continue
		}
		ddlText := record.DDL
		if ddlText == "" {
			ddlText = string(record.DDLPlan)
		}
		if err := r.DDLExecutions.RecordVacuousDDLExecution(ctx, r.FlowID, position, ddlText); err != nil {
			return fmt.Errorf("complete vacuous DDL position %s: %w", position, err)
		}
	}
	return nil
}

func (r *Runner) ddlExecutionDestinations(sourceBatch connector.Batch) (map[string][]string, error) {
	destinations := make(map[string][]string)
	seen := make(map[string]map[string]struct{})
	for _, destination := range r.Destinations {
		if destination.Dest == nil || destination.Spec.Name == "" {
			continue
		}
		capabilities, err := connector.ResolveDestinationCapabilities(destination.Dest, destination.Spec)
		if err != nil {
			return nil, fmt.Errorf("destination %s capability profile: %w", destination.Spec.Name, err)
		}
		if !capabilities.ExecutesDDL() {
			continue
		}
		projected := sourceBatch
		if destination.Projector != nil {
			var decision ProjectionDecision
			var err error
			projected, decision, err = destination.Projector.ProjectBatch(sourceBatch)
			if err != nil {
				return nil, fmt.Errorf("project DDL receipt destination %s: %w", destination.Spec.Name, err)
			}
			if decision == ProjectionFiltered {
				continue
			}
		}
		for _, record := range ddlRecordsInBatch(projected) {
			position := ddlRecordPosition(record, projected.Checkpoint)
			if strings.TrimSpace(position) == "" {
				return nil, fmt.Errorf("projected DDL destination %s has no durable source position", destination.Spec.Name)
			}
			if seen[position] == nil {
				seen[position] = make(map[string]struct{})
			}
			if _, duplicate := seen[position][destination.Spec.Name]; duplicate {
				continue
			}
			seen[position][destination.Spec.Name] = struct{}{}
			destinations[position] = append(destinations[position], destination.Spec.Name)
		}
	}
	return destinations, nil
}

func (r *Runner) emitTrace(ctx context.Context, kind, lsn, destination string, specAction spec.Action, err error) {
	r.emitTracePosition(ctx, kind, lsn, "", destination, specAction, err)
}

func (r *Runner) emitTracePosition(ctx context.Context, kind, lsn, position, destination string, specAction spec.Action, err error) {
	if r.TraceSink == nil {
		return
	}
	if lsn == "" && position == "" {
		switch kind {
		case "read", "deliver", "ack", "ack_error", "checkpoint", "restore_checkpoint", "restore_ack", "restore_ack_error", "write", "write_error", "ddl_error", "control_checkpoint":
			return
		}
	}
	event := TraceEvent{
		Kind:        kind,
		Spec:        spec.SpecCDCFlow,
		SpecAction:  specAction,
		LSN:         lsn,
		Position:    position,
		FlowID:      r.FlowID,
		Managed:     r.managed(),
		Destination: destination,
	}
	if err != nil {
		event.Error = err.Error()
	}
	r.TraceSink.Emit(ctx, event)
}
