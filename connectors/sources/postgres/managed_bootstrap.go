package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/bootstrap"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"golang.org/x/sync/errgroup"
)

const (
	defaultManagedSnapshotWorkers = 4
	maxManagedSnapshotWorkers     = 16
	defaultManagedSnapshotTables  = 64
	defaultManagedSnapshotRetries = 3
)

// CleanupManagedResources retires exact owned source resources under terminal
// lifecycle authority. It constructs a fresh source connection so cleanup is
// recoverable by the control-plane process even when the worker was killed.
func (s *Source) CleanupManagedResources(ctx context.Context, fence connector.CleanupFence, spec connector.RuntimeSpec) (retErr error) {
	if err := fence.Validate(); err != nil {
		return err
	}
	if s.ManagedControl == nil {
		return errors.New("managed PostgreSQL cleanup requires shared control PostgreSQL")
	}
	dsn := strings.TrimSpace(spec.Options[optDSN])
	if dsn == "" {
		return errors.New("managed PostgreSQL cleanup source dsn is required")
	}
	ctx, endSpan := telemetry.StartBootstrapSpan(ctx, "cleanup", fence.FlowID, "", "", fence.Generation)
	defer func() { endSpan(retErr) }()
	sourcePool, err := newPool(ctx, dsn, spec.Options)
	if err != nil {
		return err
	}
	defer sourcePool.Close()
	coordinator, err := bootstrap.NewBootstrapper(ctx, s.ManagedControl, dsn, sourcePool, s.BootstrapHooks)
	if err != nil {
		return err
	}
	return coordinator.CleanupOwnedResources(ctx, fence)
}

// PrepareManagedBootstrap runs or recovers the slot-anchored snapshot before
// the ordinary logical replication source is opened. The destination has
// already been opened by the stream runner.
func (s *Source) PrepareManagedBootstrap(ctx context.Context, fence connector.RunFence, spec connector.RuntimeSpec, destinationRevisionID string, projector connector.ManagedBootstrapProjector, driver connector.ManagedBootstrapDestination) (connector.ManagedBootstrapResult, error) {
	if err := fence.Validate(); err != nil {
		return connector.ManagedBootstrapResult{}, err
	}
	if s.ManagedControl == nil || s.ManagedAuthority == nil {
		return connector.ManagedBootstrapResult{}, errors.New("managed PostgreSQL source requires shared control PostgreSQL and authority")
	}
	if driver == nil || projector == nil || strings.TrimSpace(projector.Fingerprint()) == "" || strings.TrimSpace(destinationRevisionID) == "" {
		return connector.ManagedBootstrapResult{}, errors.New("managed bootstrap destination revision, projector, and driver are required")
	}
	mode := strings.ToLower(strings.TrimSpace(spec.Options["bootstrap"]))
	if mode == "" {
		mode = "auto"
	}
	if mode == "never" {
		return connector.ManagedBootstrapResult{}, nil
	}
	if mode != "auto" && mode != "required" {
		return connector.ManagedBootstrapResult{}, fmt.Errorf("unsupported managed bootstrap mode %q", mode)
	}
	dsn := strings.TrimSpace(spec.Options[optDSN])
	if dsn == "" {
		return connector.ManagedBootstrapResult{}, errors.New("managed bootstrap source dsn is required")
	}
	maxSourceConns := parseInt(spec.Options["pool_max_conns"], 4)
	if maxSourceConns < 2 {
		return connector.ManagedBootstrapResult{}, errors.New("managed bootstrap requires pool_max_conns>=2 (one schema barrier plus at least one snapshot importer)")
	}
	sourcePool, err := newPool(ctx, dsn, spec.Options)
	if err != nil {
		return connector.ManagedBootstrapResult{}, err
	}
	defer sourcePool.Close()
	if _, err := validateManagedPostgresServerVersion(ctx, sourcePool, spec.Options[optManagedProfile]); err != nil {
		return connector.ManagedBootstrapResult{}, err
	}
	coordinator, err := bootstrap.NewBootstrapper(ctx, s.ManagedControl, dsn, sourcePool, s.BootstrapHooks)
	if err != nil {
		return connector.ManagedBootstrapResult{}, err
	}

	recoveryCtx, endRecoverySpan := telemetry.StartBootstrapSpan(ctx, "recovery", fence.FlowID, "", "", fence.Generation)
	latest, phase, err := coordinator.LoadLatest(recoveryCtx, fence)
	endRecoverySpan(err)
	switch {
	case err == nil:
		switch phase {
		case "streaming":
			checkpoint, err := loadManagedAuthoritativeCheckpoint(ctx, s.ManagedControl, fence)
			if err != nil {
				return connector.ManagedBootstrapResult{}, err
			}
			return managedBootstrapResult(latest, checkpoint), nil
		case "published":
			checkpoint, err := handoffRecoveredBootstrapPublication(ctx, coordinator, fence, latest, "handoff recovered published bootstrap")
			if err != nil {
				return connector.ManagedBootstrapResult{}, err
			}
			return managedBootstrapResult(latest, checkpoint), nil
		case "snapshotting", "exporting", "abandoning":
			telemetry.RecordBootstrapEvent(ctx, "exporter_lost")
			// A replacement cannot import a snapshot whose exporter process is
			// gone. It may, however, reconcile an atomic destination publication
			// marker committed before the old owner recorded its control receipt.
			tables, schemaErr := coordinator.LoadDeliveryContracts(ctx, fence, latest)
			if latest.SourceLineageID != "" {
				if schemaErr != nil {
					return connector.ManagedBootstrapResult{}, recoverableBootstrapPublicationError("load schemas for publication recovery", schemaErr)
				}
				intent := managedBootstrapIntent(fence, latest, destinationRevisionID)
				reconciler, ok := driver.(connector.ManagedBootstrapPublicationReconciler)
				if !ok {
					return connector.ManagedBootstrapResult{}, recoverableBootstrapPublicationError("reconcile destination bootstrap publication", errors.New("managed bootstrap destination cannot reconcile publication after exporter loss"))
				}
				disposition, evidence, reconcileErr := reconciler.ReconcileBootstrapPublication(ctx, intent)
				if reconcileErr != nil {
					return connector.ManagedBootstrapResult{}, recoverableBootstrapPublicationError("reconcile destination bootstrap publication", reconcileErr)
				}
				switch disposition {
				case connector.DeliveryApplied:
					if evidence.ContentHash != latest.ManifestHash || strings.TrimSpace(evidence.ExternalID) == "" {
						return connector.ManagedBootstrapResult{}, recoverableBootstrapPublicationError("validate recovered destination publication evidence", fmt.Errorf("%w: recovered bootstrap publication evidence mismatch", connector.ErrDeliveryConflict))
					}
					checkpoint, err := finalizeRecoveredBootstrapPublication(ctx, coordinator, fence, latest, destinationRevisionID, evidence.ContentHash, uuid.New())
					if err != nil {
						return connector.ManagedBootstrapResult{}, err
					}
					return managedBootstrapResult(latest, checkpoint), nil
				case connector.DeliveryNotApplied:
					if err := driver.AbandonBootstrap(ctx, intent, tables); err != nil {
						return connector.ManagedBootstrapResult{}, err
					}
				case connector.DeliveryIndeterminate:
					return connector.ManagedBootstrapResult{}, fmt.Errorf("%w: bootstrap publication cannot be reconciled", connector.ErrDeliveryIndeterminate)
				default:
					return connector.ManagedBootstrapResult{}, fmt.Errorf("unknown bootstrap publication disposition %d", disposition)
				}
			}
			// No destination publication marker exists. Abandon the entire old
			// generation; persisted cursors never cross into its replacement.
			if err := coordinator.Abandon(ctx, fence, latest, "exporter session unavailable during recovery"); err != nil {
				return connector.ManagedBootstrapResult{}, errors.Join(schemaErr, err)
			}
		case "abandoned":
		default:
			return connector.ManagedBootstrapResult{}, fmt.Errorf("unknown bootstrap phase %q", phase)
		}
	case errors.Is(err, pgx.ErrNoRows):
		var checkpointExists bool
		if err := s.ManagedControl.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM authoritative_checkpoints WHERE flow_incarnation_id=$1)`, fence.FlowIncarnationID).Scan(&checkpointExists); err != nil {
			return connector.ManagedBootstrapResult{}, err
		}
		if checkpointExists {
			if mode == "required" {
				return connector.ManagedBootstrapResult{}, errors.New("bootstrap=required rejects an authoritative checkpoint without a completed bootstrap generation")
			}
			return connector.ManagedBootstrapResult{}, nil
		}
	default:
		return connector.ManagedBootstrapResult{}, err
	}

	restartLimit := parseInt(spec.Options["bootstrap_restart_limit"], defaultManagedSnapshotRetries)
	if restartLimit < 1 || restartLimit > 32 {
		return connector.ManagedBootstrapResult{}, fmt.Errorf("bootstrap_restart_limit must be between 1 and 32, got %d", restartLimit)
	}
	var lastErr error
	for attempt := 1; attempt <= restartLimit; attempt++ {
		result, retry, err := s.runManagedBootstrapGeneration(ctx, coordinator, sourcePool, fence, spec, destinationRevisionID, projector, driver)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !retry {
			return connector.ManagedBootstrapResult{}, err
		}
		telemetry.RecordBootstrapEvent(ctx, "generation_restarted")
	}
	return connector.ManagedBootstrapResult{}, fmt.Errorf("managed bootstrap exhausted %d full-generation restarts: %w", restartLimit, lastErr)
}

func (s *Source) runManagedBootstrapGeneration(ctx context.Context, coordinator *bootstrap.Bootstrapper, sourcePool *pgxpool.Pool, fence authority.RunFence, spec connector.RuntimeSpec, destinationRevisionID string, projector connector.ManagedBootstrapProjector, driver connector.ManagedBootstrapDestination) (result connector.ManagedBootstrapResult, retry bool, retErr error) {
	publication := strings.TrimSpace(spec.Options[optPublication])
	if publication == "" {
		publication = managedPublicationName(fence)
	}
	maxTables := parseInt(spec.Options["snapshot_max_tables"], defaultManagedSnapshotTables)
	if maxTables < 1 || maxTables > 256 {
		return connector.ManagedBootstrapResult{}, false, fmt.Errorf("snapshot_max_tables must be between 1 and 256, got %d", maxTables)
	}
	// Acquire the DDL/control barrier before publication creation and slot
	// creation. This makes the publication visible in the logical decoding
	// snapshot while preventing relation/schema changes across task planning.
	barrier, err := sourcePool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return connector.ManagedBootstrapResult{}, false, err
	}
	defer func() { _ = barrier.Rollback(context.WithoutCancel(ctx)) }()
	tasks, relations, err := discoverManagedSnapshotTasks(ctx, barrier, spec, projector, maxTables)
	if err != nil {
		return connector.ManagedBootstrapResult{}, false, err
	}
	tasks, relations, projectedTables, err := filterManagedSnapshotTasks(tasks, relations, projector)
	if err != nil {
		return connector.ManagedBootstrapResult{}, false, err
	}
	for _, relation := range relations {
		// pg_relation_size opens the relation with AccessShareLock and keeps the
		// lock until this transaction releases the barrier after handoff.
		if err := barrier.QueryRow(ctx, `SELECT pg_catalog.pg_relation_size($1::oid)`, relation.OID).Scan(new(int64)); err != nil {
			return connector.ManagedBootstrapResult{}, false, fmt.Errorf("lock bootstrap relation %d: %w", relation.OID, err)
		}
	}
	manifestDigest := managedManifestHash(tasks)
	if manifestDigest.err != nil {
		return connector.ManagedBootstrapResult{}, false, manifestDigest.err
	}
	manifestHash := manifestDigest.value
	var sourceSystem, databaseName string
	if err := barrier.QueryRow(ctx, `SELECT system_identifier::text,current_database() FROM pg_catalog.pg_control_system()`).Scan(&sourceSystem, &databaseName); err != nil {
		return connector.ManagedBootstrapResult{}, false, err
	}
	publicationRevision := bootstrap.ExpectedPublicationRevision(publication, relations)
	resource, err := coordinator.EnsurePublication(ctx, fence, bootstrap.ExportedSnapshot{
		SourceSystem: sourceSystem, DatabaseName: databaseName,
	}, publication, publicationRevision, relations, parseBool(spec.Options[optEnsurePublication], true))
	if err != nil {
		return connector.ManagedBootstrapResult{}, false, err
	}
	selectionHash := managedSelectionHash(spec, projector.Fingerprint())
	session, err := coordinator.Start(ctx, fence, resource.Name, selectionHash)
	if err != nil {
		latest, phase, loadErr := coordinator.LoadLatest(context.WithoutCancel(ctx), fence)
		if loadErr == nil && (phase == "snapshotting" || phase == "abandoning") {
			abandonErr := coordinator.Abandon(context.WithoutCancel(ctx), fence, latest, "exporter lost during bootstrap start")
			return connector.ManagedBootstrapResult{}, true, errors.Join(err, abandonErr)
		}
		return connector.ManagedBootstrapResult{}, false, errors.Join(err, loadErr)
	}
	ctx, endSpan := telemetry.StartBootstrapSpan(ctx, "generation", fence.FlowID, session.Snapshot.BootstrapID.String(), "", session.Snapshot.BootstrapGeneration)
	defer func() { endSpan(retErr) }()
	telemetry.RecordBootstrapEvent(ctx, "generation_started")
	var bootstrapTables []connector.BootstrapTable
	publicationAttempted := false
	defer func() {
		_ = session.Close(context.WithoutCancel(ctx))
		if retErr == nil {
			return
		}
		if len(bootstrapTables) > 0 && !publicationAttempted {
			intent := managedBootstrapIntent(fence, session.Snapshot, destinationRevisionID)
			_ = driver.AbandonBootstrap(context.WithoutCancel(ctx), intent, bootstrapTables)
		}
		if !publicationAttempted {
			if abandonErr := coordinator.Abandon(context.WithoutCancel(ctx), fence, session.Snapshot, retErr.Error()); abandonErr != nil {
				retErr = errors.Join(retErr, abandonErr)
			}
		}
	}()
	if session.Snapshot.SourceSystem != sourceSystem || session.Snapshot.DatabaseName != databaseName {
		return connector.ManagedBootstrapResult{}, false, errors.New("source identity changed between publication preparation and slot creation")
	}
	verification, err := coordinator.ImportSnapshot(ctx, fence, session)
	if err != nil {
		return connector.ManagedBootstrapResult{}, true, err
	}
	verifiedTasks, verifiedRelations, err := discoverManagedSnapshotTasks(ctx, verification, spec, projector, maxTables)
	if err != nil {
		_ = verification.Rollback(context.WithoutCancel(ctx))
		return connector.ManagedBootstrapResult{}, !session.Alive(), err
	}
	verifiedTasks, verifiedRelations, verifiedProjectedTables, err := filterManagedSnapshotTasks(verifiedTasks, verifiedRelations, projector)
	if err != nil {
		_ = verification.Rollback(context.WithoutCancel(ctx))
		return connector.ManagedBootstrapResult{}, false, err
	}
	verifiedDigest := managedManifestHash(verifiedTasks)
	if verifiedDigest.err != nil {
		_ = verification.Rollback(context.WithoutCancel(ctx))
		return connector.ManagedBootstrapResult{}, false, verifiedDigest.err
	}
	verifiedHash := verifiedDigest.value
	if verifiedHash != manifestHash || !samePublicationRelations(relations, verifiedRelations) || !reflect.DeepEqual(projectedTables, verifiedProjectedTables) {
		_ = verification.Rollback(context.WithoutCancel(ctx))
		return connector.ManagedBootstrapResult{}, false, errors.New("schema/control barrier changed across the slot consistent point")
	}
	if err := verification.Rollback(ctx); err != nil {
		return connector.ManagedBootstrapResult{}, false, err
	}
	session.Snapshot, err = coordinator.AttachPublication(ctx, fence, session.Snapshot, resource)
	if err != nil {
		return connector.ManagedBootstrapResult{}, false, err
	}
	session.Snapshot, err = coordinator.FreezeManifest(ctx, fence, session.Snapshot, strings.TrimSpace(spec.Options[optSourceLineageID]), manifestHash, resource.Revision, tasks)
	if err != nil {
		return connector.ManagedBootstrapResult{}, false, err
	}
	bootstrapTables = projectedTables
	for index := range bootstrapTables {
		bootstrapTables[index].SourcePosition = session.Snapshot.ConsistentLSN.String()
	}
	bootstrapIntent := managedBootstrapIntent(fence, session.Snapshot, destinationRevisionID)
	if err := driver.PrepareBootstrap(ctx, bootstrapIntent, bootstrapTables); err != nil {
		return connector.ManagedBootstrapResult{}, false, err
	}
	workers := parseInt(spec.Options["snapshot_workers"], defaultManagedSnapshotWorkers)
	if workers < 1 || workers > maxManagedSnapshotWorkers {
		return connector.ManagedBootstrapResult{}, false, fmt.Errorf("snapshot_workers must be between 1 and %d, got %d", maxManagedSnapshotWorkers, workers)
	}
	availableImporters := int(sourcePool.Config().MaxConns) - 1
	if workers > availableImporters {
		workers = availableImporters
	}
	batchSize := parseInt(spec.Options[optBatchSize], 100)
	if batchSize < 1 || batchSize > 10000 {
		return connector.ManagedBootstrapResult{}, false, fmt.Errorf("bootstrap batch_size must be between 1 and 10000, got %d", batchSize)
	}
	claimLease := parseDuration(spec.Options["snapshot_claim_lease"], 30*time.Second)
	if claimLease <= 0 {
		return connector.ManagedBootstrapResult{}, false, errors.New("snapshot_claim_lease must be positive")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(workers)
	for _, task := range tasks {
		task := task
		group.Go(func() error {
			return s.runManagedSnapshotTask(groupCtx, coordinator, session, fence, task, destinationRevisionID, batchSize, claimLease, projector, driver)
		})
	}
	if err := group.Wait(); err != nil {
		return connector.ManagedBootstrapResult{}, !session.Alive() || isInvalidSnapshotError(err), err
	}
	// Once publication is attempted, neither source nor destination staging may
	// be abandoned without reconciliation proving the marker absent.
	publicationAttempted = true
	evidence, err := driver.PublishBootstrap(ctx, bootstrapIntent, bootstrapTables)
	if err != nil && errors.Is(err, connector.ErrDeliveryIndeterminate) {
		// PublishBootstrap is itself reconciliatory: a retry reads the marker
		// committed with the atomic target replacement.
		evidence, err = driver.PublishBootstrap(ctx, bootstrapIntent, bootstrapTables)
	}
	if err != nil {
		return connector.ManagedBootstrapResult{}, false, recoverableBootstrapPublicationError("publish destination bootstrap", err)
	}
	if evidence.ContentHash != session.Snapshot.ManifestHash || strings.TrimSpace(evidence.ExternalID) == "" {
		return connector.ManagedBootstrapResult{}, false, recoverableBootstrapPublicationError("validate destination publication evidence", fmt.Errorf("%w: bootstrap publication evidence mismatch", connector.ErrDeliveryConflict))
	}
	// Destination publication is an irreversible external fact. From this
	// point recovery must reconcile its immutable marker and must never abandon
	// the source slot merely because the control receipt/handoff is interrupted.
	if s.BootstrapHooks.AfterPublication != nil {
		if err := s.BootstrapHooks.AfterPublication(ctx, session.Snapshot); err != nil {
			return connector.ManagedBootstrapResult{}, false, recoverableBootstrapPublicationError("after destination publication", err)
		}
	}
	if err := coordinator.RecordPublication(ctx, fence, session.Snapshot, destinationRevisionID, evidence.ContentHash, uuid.New()); err != nil {
		return connector.ManagedBootstrapResult{}, false, recoverableBootstrapPublicationError("record destination publication", err)
	}
	if s.BootstrapHooks.AfterPublicationReceipt != nil {
		if err := s.BootstrapHooks.AfterPublicationReceipt(ctx, session.Snapshot); err != nil {
			return connector.ManagedBootstrapResult{}, false, recoverableBootstrapPublicationError("after publication receipt", err)
		}
	}
	checkpoint, err := coordinator.Handoff(ctx, fence, session.Snapshot)
	if err != nil {
		return connector.ManagedBootstrapResult{}, false, recoverableBootstrapPublicationError("handoff published bootstrap", err)
	}
	if err := barrier.Commit(ctx); err != nil {
		return connector.ManagedBootstrapResult{}, false, recoverableBootstrapPublicationError("release bootstrap schema barrier", err)
	}
	if err := session.Close(ctx); err != nil {
		return connector.ManagedBootstrapResult{}, false, recoverableBootstrapPublicationError("close published bootstrap session", err)
	}
	if s.BootstrapHooks.AfterHandoff != nil {
		if err := s.BootstrapHooks.AfterHandoff(ctx, session.Snapshot); err != nil {
			return connector.ManagedBootstrapResult{}, false, recoverableBootstrapPublicationError("after bootstrap handoff", err)
		}
	}
	result = managedBootstrapResult(session.Snapshot, checkpoint)
	return result, false, nil
}

type recoveredBootstrapPublicationHandoff interface {
	Handoff(context.Context, authority.RunFence, bootstrap.ExportedSnapshot) (connector.Checkpoint, error)
}

type recoveredBootstrapPublicationFinalizer interface {
	recoveredBootstrapPublicationHandoff
	RecordPublication(context.Context, authority.RunFence, bootstrap.ExportedSnapshot, string, string, uuid.UUID) error
}

func finalizeRecoveredBootstrapPublication(ctx context.Context, coordinator recoveredBootstrapPublicationFinalizer, fence authority.RunFence, snapshot bootstrap.ExportedSnapshot, destinationRevisionID, contentHash string, attemptID uuid.UUID) (connector.Checkpoint, error) {
	if err := coordinator.RecordPublication(ctx, fence, snapshot, destinationRevisionID, contentHash, attemptID); err != nil {
		return connector.Checkpoint{}, recoverableBootstrapPublicationError("record recovered destination publication", err)
	}
	return handoffRecoveredBootstrapPublication(ctx, coordinator, fence, snapshot, "handoff recovered destination publication")
}

func handoffRecoveredBootstrapPublication(ctx context.Context, coordinator recoveredBootstrapPublicationHandoff, fence authority.RunFence, snapshot bootstrap.ExportedSnapshot, stage string) (connector.Checkpoint, error) {
	checkpoint, err := coordinator.Handoff(ctx, fence, snapshot)
	if err != nil {
		return connector.Checkpoint{}, recoverableBootstrapPublicationError(stage, err)
	}
	return checkpoint, nil
}

func recoverableBootstrapPublicationError(stage string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %w", connector.ErrDeliveryIndeterminate, stage, err)
}

func (s *Source) runManagedSnapshotTask(ctx context.Context, coordinator *bootstrap.Bootstrapper, session *bootstrap.Session, fence authority.RunFence, task bootstrap.SnapshotTask, destinationRevisionID string, batchSize int, claimLease time.Duration, projector connector.ManagedBootstrapProjector, driver connector.ManagedBootstrapDestination) error {
	claim, err := s.ManagedAuthority.AcquireClaim(ctx, fence, authority.ClaimSnapshot, task.WorkID(session.Snapshot.BootstrapID), claimLease)
	if err != nil {
		return err
	}
	defer func() { _ = s.ManagedAuthority.ReleaseClaim(context.WithoutCancel(ctx), claim) }()
	return runWithRenewedSnapshotClaim(ctx, s.ManagedAuthority, claim, claimLease, func(claimCtx context.Context) error {
		return s.runClaimedManagedSnapshotTask(claimCtx, coordinator, session, fence, claim, task, destinationRevisionID, batchSize, projector, driver)
	})
}

func (s *Source) runClaimedManagedSnapshotTask(ctx context.Context, coordinator *bootstrap.Bootstrapper, session *bootstrap.Session, fence authority.RunFence, claim authority.ClaimFence, task bootstrap.SnapshotTask, destinationRevisionID string, batchSize int, projector connector.ManagedBootstrapProjector, driver connector.ManagedBootstrapDestination) (retErr error) {
	ctx, endSpan := telemetry.StartBootstrapSpan(ctx, "task", fence.FlowID, session.Snapshot.BootstrapID.String(), task.TaskID, session.Snapshot.BootstrapGeneration)
	defer func() { endSpan(retErr) }()
	var lastTransientErr error
	for taskAttempt := 0; taskAttempt < defaultManagedSnapshotRetries; taskAttempt++ {
		ordinal, cursor, complete, err := coordinator.TaskProgress(ctx, fence, session.Snapshot, task)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		tx, err := coordinator.ImportSnapshot(ctx, fence, session)
		if err != nil {
			return err
		}
		for {
			records, nextCursor, done, err := queryManagedSnapshotBatch(ctx, tx, task, cursor, batchSize)
			if err != nil {
				_ = tx.Rollback(context.WithoutCancel(ctx))
				wrapped := fmt.Errorf("query snapshot task %s batch: %w", task.WorkID(session.Snapshot.BootstrapID), err)
				if !isTransientManagedSnapshotError(err) {
					return wrapped
				}
				lastTransientErr = wrapped
				break
			}
			ordinal++
			checkpoint := connector.Checkpoint{
				LSN:      session.Snapshot.ConsistentLSN.String(),
				Metadata: map[string]string{"bootstrap_id": session.Snapshot.BootstrapID.String(), "task_id": task.TaskID, "batch_ordinal": fmt.Sprint(ordinal)},
			}
			sourceBatch := connector.Batch{Records: records, Schema: task.Schema, Checkpoint: checkpoint, WireFormat: connector.WireFormatArrow}
			batch, included, err := projector.ProjectBootstrapBatch(sourceBatch)
			if err != nil {
				_ = tx.Rollback(context.WithoutCancel(ctx))
				return fmt.Errorf("project bootstrap task %s: %w", task.WorkID(session.Snapshot.BootstrapID), err)
			}
			if !included {
				_ = tx.Rollback(context.WithoutCancel(ctx))
				return errors.New("frozen included bootstrap task was filtered during batch projection")
			}
			if err := coordinator.DeliverTaskBatch(ctx, claim, session.Snapshot, task, ordinal, nextCursor, done, destinationRevisionID, batch, driver); err != nil {
				_ = tx.Rollback(context.WithoutCancel(ctx))
				wrapped := fmt.Errorf("deliver snapshot task %s batch %d: %w", task.WorkID(session.Snapshot.BootstrapID), ordinal, err)
				if !isTransientManagedSnapshotError(err) {
					return wrapped
				}
				lastTransientErr = wrapped
				break
			}
			telemetry.RecordBootstrapProgress(ctx, len(records))
			cursor = nextCursor
			if done {
				return tx.Commit(ctx)
			}
		}
		if !session.Alive() {
			return errors.New("bootstrap exporter lost while snapshot task was active")
		}
	}
	return managedSnapshotRetriesExhausted(task.WorkID(session.Snapshot.BootstrapID), lastTransientErr)
}

func managedSnapshotRetriesExhausted(workID string, lastErr error) error {
	if lastErr == nil {
		return fmt.Errorf("snapshot task %s exhausted retries without a classified transient error", workID)
	}
	return fmt.Errorf("snapshot task %s exhausted transient retries: %w", workID, lastErr)
}

func isTransientManagedSnapshotError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, authority.ErrFenceRejected) || errors.Is(err, connector.ErrDeliveryConflict) || errors.Is(err, connector.ErrDeliveryIndeterminate) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, "08") {
			return true
		}
		switch pgErr.Code {
		case "40001", "40P01", "53300", "53400", "55P03", "57P01", "57P02", "57P03":
			return true
		default:
			return false
		}
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return false
	}
	var operationErr *net.OpError
	return errors.As(err, &operationErr)
}

func runWithRenewedSnapshotClaim(ctx context.Context, store authority.Store, claim authority.ClaimFence, lease time.Duration, work func(context.Context) error) error {
	claimCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	renewalErr := make(chan error, 1)
	go func() {
		interval := lease / 3
		if interval <= 0 {
			interval = time.Nanosecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-claimCtx.Done():
				renewalErr <- nil
				return
			case <-ticker.C:
				if err := store.RenewClaim(claimCtx, claim, lease); err != nil {
					telemetry.RecordBootstrapClaimRenewal(claimCtx, "failure")
					wrapped := fmt.Errorf("renew snapshot claim: %w", err)
					cancel(wrapped)
					renewalErr <- wrapped
					return
				}
				telemetry.RecordBootstrapClaimRenewal(claimCtx, "success")
			}
		}
	}()
	workErr := work(claimCtx)
	cancel(nil)
	renewErr := <-renewalErr
	if renewErr != nil {
		return renewErr
	}
	return workErr
}

func loadManagedSnowflakePublicationContract(ctx context.Context, pool *pgxpool.Pool, publication string) ([]string, []connector.Schema, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var serverEncoding, integerDatetimes string
	if err := tx.QueryRow(ctx, `SELECT current_setting('server_encoding'),current_setting('integer_datetimes')`).Scan(&serverEncoding, &integerDatetimes); err != nil {
		return nil, nil, fmt.Errorf("inspect managed Snowflake PostgreSQL encoding contract: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(serverEncoding), "UTF8") || !strings.EqualFold(strings.TrimSpace(integerDatetimes), "on") {
		return nil, nil, fmt.Errorf("managed Snowflake source requires server_encoding=UTF8 and integer_datetimes=on, got %s/%s", serverEncoding, integerDatetimes)
	}
	var allTables, publishInsert, publishUpdate, publishDelete, publishTruncate bool
	if err := tx.QueryRow(ctx, `SELECT puballtables,pubinsert,pubupdate,pubdelete,pubtruncate
FROM pg_catalog.pg_publication WHERE pubname=$1`, publication).Scan(
		&allTables, &publishInsert, &publishUpdate, &publishDelete, &publishTruncate,
	); err != nil {
		return nil, nil, err
	}
	if allTables {
		return nil, nil, fmt.Errorf("publication %s uses FOR ALL TABLES", publication)
	}
	if !publishInsert || !publishUpdate || !publishDelete || publishTruncate {
		return nil, nil, fmt.Errorf("managed Snowflake publication requires insert/update/delete and rejects truncate; got insert=%t update=%t delete=%t truncate=%t", publishInsert, publishUpdate, publishDelete, publishTruncate)
	}
	rows, err := tx.Query(ctx, `
SELECT c.oid,n.nspname,c.relname,c.relkind::text,c.relispartition,c.relreplident::text,
       pr.prattrs IS NOT NULL,pr.prqual IS NOT NULL
FROM pg_catalog.pg_publication p
JOIN pg_catalog.pg_publication_rel pr ON pr.prpubid=p.oid
JOIN pg_catalog.pg_class c ON c.oid=pr.prrelid
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE p.pubname=$1
ORDER BY n.nspname,c.relname`, publication)
	if err != nil {
		return nil, nil, err
	}
	var relations []bootstrap.PublicationRelation
	for rows.Next() {
		var relation bootstrap.PublicationRelation
		var replicaIdentity string
		var hasColumnList, hasRowFilter bool
		if err := rows.Scan(&relation.OID, &relation.Namespace, &relation.Table, &relation.RelationKind, &relation.IsPartition, &replicaIdentity, &hasColumnList, &hasRowFilter); err != nil {
			rows.Close()
			return nil, nil, err
		}
		if relation.RelationKind != "r" || relation.IsPartition {
			rows.Close()
			return nil, nil, fmt.Errorf("managed Snowflake profile rejects partitioned or partition relation %s.%s", relation.Namespace, relation.Table)
		}
		if replicaIdentity != "d" {
			rows.Close()
			return nil, nil, fmt.Errorf("managed Snowflake profile requires default primary-key replica identity on %s.%s", relation.Namespace, relation.Table)
		}
		if hasColumnList || hasRowFilter {
			rows.Close()
			return nil, nil, fmt.Errorf("managed Snowflake profile rejects publication column lists and row filters on %s.%s", relation.Namespace, relation.Table)
		}
		relations = append(relations, relation)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()
	if len(relations) == 0 {
		return nil, nil, errors.New("managed Snowflake source publication contains no relations")
	}
	tables := make([]string, 0, len(relations))
	schemas := make([]connector.Schema, 0, len(relations))
	for _, relation := range relations {
		schema, _, err := loadManagedSnapshotSchema(ctx, tx, relation)
		if err != nil {
			return nil, nil, err
		}
		tables = append(tables, pgx.Identifier{relation.Namespace, relation.Table}.Sanitize())
		schemas = append(schemas, schema)
	}
	return tables, schemas, nil
}

func discoverManagedSnapshotTasks(ctx context.Context, tx pgx.Tx, spec connector.RuntimeSpec, projector connector.ManagedBootstrapProjector, maxTables int) ([]bootstrap.SnapshotTask, []bootstrap.PublicationRelation, error) {
	requested := parseCSV(spec.Options[optPublicationTables])
	if len(requested) == 0 {
		requested = parseCSV(spec.Options["tables"])
	}
	var rows pgx.Rows
	var err error
	if len(requested) > 0 {
		rows, err = tx.Query(ctx, `
SELECT c.oid,n.nspname,c.relname,c.relkind::text,c.relispartition
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE c.oid = ANY($1::regclass[]) AND c.relkind IN ('r','p')
ORDER BY c.oid`, requested)
	} else {
		schemas, parseErr := parseIdentifierCSV(spec.Options[optPublicationSchemas])
		if parseErr != nil {
			return nil, nil, fmt.Errorf("parse publication_schemas: %w", parseErr)
		}
		if len(schemas) == 0 {
			schemas, parseErr = parseIdentifierCSV(spec.Options["schemas"])
			if parseErr != nil {
				return nil, nil, fmt.Errorf("parse schemas: %w", parseErr)
			}
		}
		if len(schemas) == 0 {
			schemas = []string{"public"}
		}
		rows, err = tx.Query(ctx, `
SELECT c.oid,n.nspname,c.relname,c.relkind::text,c.relispartition
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname=ANY($1::text[]) AND c.relkind IN ('r','p')
ORDER BY c.oid`, schemas)
	}
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var relations []bootstrap.PublicationRelation
	for rows.Next() {
		var relation bootstrap.PublicationRelation
		if err := rows.Scan(&relation.OID, &relation.Namespace, &relation.Table, &relation.RelationKind, &relation.IsPartition); err != nil {
			return nil, nil, err
		}
		included, err := admitManagedSnapshotRelation(projector, relation, len(relations), maxTables)
		if err != nil {
			return nil, nil, err
		}
		if !included {
			continue
		}
		relations = append(relations, relation)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	if len(relations) == 0 {
		return nil, nil, errors.New("managed bootstrap selected no source tables")
	}
	tasks := make([]bootstrap.SnapshotTask, 0, len(relations))
	for _, relation := range relations {
		schema, keys, err := loadManagedSnapshotSchema(ctx, tx, relation)
		if err != nil {
			return nil, nil, err
		}
		tasks = append(tasks, bootstrap.SnapshotTask{RelationID: relation.OID, TaskID: "full-table", Namespace: relation.Namespace, Table: relation.Table, Schema: schema, KeyColumns: keys})
	}
	return tasks, relations, nil
}

func admitManagedSnapshotRelation(projector connector.ManagedBootstrapProjector, relation bootstrap.PublicationRelation, selected, maxTables int) (bool, error) {
	included, err := projector.IncludeBootstrapRelation(relation.Namespace, relation.Table)
	if err != nil {
		return false, fmt.Errorf("select bootstrap relation %s.%s: %w", relation.Namespace, relation.Table, err)
	}
	if !included {
		return false, nil
	}
	if relation.RelationKind == "p" || relation.IsPartition {
		return false, fmt.Errorf("managed bootstrap does not support partitioned or partition relations: %s.%s", relation.Namespace, relation.Table)
	}
	if selected+1 > maxTables {
		return false, fmt.Errorf("managed bootstrap selected more than snapshot_max_tables=%d", maxTables)
	}
	return true, nil
}

func filterManagedSnapshotTasks(tasks []bootstrap.SnapshotTask, relations []bootstrap.PublicationRelation, projector connector.ManagedBootstrapProjector) ([]bootstrap.SnapshotTask, []bootstrap.PublicationRelation, []connector.BootstrapTable, error) {
	byOID := make(map[uint32]bootstrap.PublicationRelation, len(relations))
	for _, relation := range relations {
		byOID[relation.OID] = relation
	}
	filteredTasks := make([]bootstrap.SnapshotTask, 0, len(tasks))
	filteredRelations := make([]bootstrap.PublicationRelation, 0, len(tasks))
	tables := make([]connector.BootstrapTable, 0, len(tasks))
	for _, task := range tasks {
		mapped, policy, included, err := projector.ProjectBootstrapSchema(task.Schema)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("project bootstrap schema %s.%s: %w", task.Namespace, task.Table, err)
		}
		if !included {
			continue
		}
		relation, ok := byOID[task.RelationID]
		if !ok {
			return nil, nil, nil, fmt.Errorf("bootstrap task relation %d is absent from publication selection", task.RelationID)
		}
		task.Delivery = bootstrap.SnapshotDeliveryContract{
			Version:               bootstrap.SnapshotDeliveryContractV1,
			Schema:                mapped,
			WritePolicy:           policy,
			ProjectionFingerprint: projector.Fingerprint(),
		}
		if err := task.Delivery.Validate(); err != nil {
			return nil, nil, nil, fmt.Errorf("freeze bootstrap destination contract %s.%s: %w", task.Namespace, task.Table, err)
		}
		filteredTasks = append(filteredTasks, task)
		filteredRelations = append(filteredRelations, relation)
		tables = append(tables, connector.BootstrapTable{Schema: task.Delivery.Schema, WritePolicy: task.Delivery.WritePolicy})
	}
	if len(filteredTasks) == 0 {
		return nil, nil, nil, errors.New("table mapping excludes every managed bootstrap relation")
	}
	return filteredTasks, filteredRelations, tables, nil
}

func loadManagedSnapshotSchema(ctx context.Context, tx pgx.Tx, relation bootstrap.PublicationRelation) (connector.Schema, []string, error) {
	rows, err := tx.Query(ctx, `
SELECT a.attname,NOT a.attnotnull,format_type(a.atttypid,a.atttypmod),a.attgenerated::text,
       pg_get_expr(ad.adbin,ad.adrelid),tns.nspname,ext.extname
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
JOIN pg_catalog.pg_namespace tns ON tns.oid=t.typnamespace
LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid=a.attrelid AND ad.adnum=a.attnum
LEFT JOIN pg_catalog.pg_depend dep ON dep.classid='pg_type'::regclass AND dep.objid=t.oid AND dep.deptype='e'
LEFT JOIN pg_catalog.pg_extension ext ON ext.oid=dep.refobjid
WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped
ORDER BY a.attnum`, relation.OID)
	if err != nil {
		return connector.Schema{}, nil, err
	}
	var columns []connector.Column
	for rows.Next() {
		var name, dataType, generated, typeSchema string
		var nullable bool
		var expression, extension *string
		if err := rows.Scan(&name, &nullable, &dataType, &generated, &expression, &typeSchema, &extension); err != nil {
			rows.Close()
			return connector.Schema{}, nil, err
		}
		column := connector.Column{
			Name: name, Type: formatTypeName(typeSchema, dataType), Nullable: nullable, Generated: generated != "",
			TypeMetadata: map[string]string{"nullability_known": "true", "generated_known": "true"},
		}
		if expression != nil {
			column.Expression = *expression
		}
		if extension != nil && *extension != "" {
			column.TypeMetadata["extension"] = strings.ToLower(*extension)
		}
		columns = append(columns, column)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return connector.Schema{}, nil, err
	}
	keyRows, err := tx.Query(ctx, `
SELECT a.attname,i.indimmediate,i.indisvalid,i.indisready
FROM pg_catalog.pg_index i
JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS key(attnum,ord) ON key.ord <= i.indnkeyatts
JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=key.attnum
WHERE i.indrelid=$1 AND i.indisprimary
ORDER BY key.ord`, relation.OID)
	if err != nil {
		return connector.Schema{}, nil, err
	}
	defer keyRows.Close()
	var keys []string
	for keyRows.Next() {
		var key string
		var immediate, valid, ready bool
		if err := keyRows.Scan(&key, &immediate, &valid, &ready); err != nil {
			return connector.Schema{}, nil, err
		}
		if !immediate || !valid || !ready {
			return connector.Schema{}, nil, fmt.Errorf("managed bootstrap requires an immediate, valid, ready primary key on %s.%s", relation.Namespace, relation.Table)
		}
		keys = append(keys, key)
	}
	if err := keyRows.Err(); err != nil {
		return connector.Schema{}, nil, err
	}
	if len(keys) == 0 {
		return connector.Schema{}, nil, fmt.Errorf("managed bootstrap requires a primary key on %s.%s", relation.Namespace, relation.Table)
	}
	var replicaIdentity string
	if err := tx.QueryRow(ctx, `SELECT relreplident::text FROM pg_catalog.pg_class WHERE oid=$1`, relation.OID).Scan(&replicaIdentity); err != nil {
		return connector.Schema{}, nil, err
	}
	primary := make(map[string]int, len(keys))
	for ordinal, key := range keys {
		primary[key] = ordinal + 1
	}
	replicaColumns := make(map[string]struct{})
	switch replicaIdentity {
	case "d":
		for _, key := range keys {
			replicaColumns[key] = struct{}{}
		}
	case "i":
		rows, err := tx.Query(ctx, `SELECT a.attname FROM pg_catalog.pg_index i
JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,ord) ON k.ord<=i.indnkeyatts
JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
WHERE i.indrelid=$1 AND i.indisreplident ORDER BY k.ord`, relation.OID)
		if err != nil {
			return connector.Schema{}, nil, err
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return connector.Schema{}, nil, err
			}
			replicaColumns[name] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return connector.Schema{}, nil, err
		}
		rows.Close()
	case "f":
		for _, column := range columns {
			replicaColumns[column.Name] = struct{}{}
		}
	}
	for index := range columns {
		ordinal, ok := primary[columns[index].Name]
		if !ok {
			continue
		}
		if columns[index].TypeMetadata == nil {
			columns[index].TypeMetadata = map[string]string{}
		}
		columns[index].TypeMetadata["primary_key"] = "true"
		columns[index].TypeMetadata["primary_key_ordinal"] = strconv.Itoa(ordinal)
	}
	for index := range columns {
		if _, ok := replicaColumns[columns[index].Name]; ok {
			columns[index].TypeMetadata["replica_identity"] = "true"
		}
	}
	return connector.Schema{Name: relation.Table, Namespace: relation.Namespace, Version: 1, Columns: columns}, keys, nil
}

func queryManagedSnapshotBatch(ctx context.Context, tx pgx.Tx, task bootstrap.SnapshotTask, cursor []byte, limit int) ([]connector.Record, []byte, bool, error) {
	identifier := pgx.Identifier{task.Namespace, task.Table}.Sanitize()
	keys := make([]string, len(task.KeyColumns))
	for i, key := range task.KeyColumns {
		keys[i] = pgx.Identifier{key}.Sanitize()
	}
	args := make([]any, 0, len(keys)+1)
	where := ""
	if len(cursor) > 0 {
		values, err := decodeManagedSnapshotCursor(task, cursor)
		if err != nil {
			return nil, nil, false, err
		}
		placeholders := make([]string, len(keys))
		for i, value := range values {
			args = append(args, value)
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		where = fmt.Sprintf(" WHERE (%s) > (%s)", strings.Join(keys, ","), strings.Join(placeholders, ","))
	}
	args = append(args, limit)
	query := fmt.Sprintf("SELECT * FROM %s%s ORDER BY %s LIMIT $%d", identifier, where, strings.Join(keys, ","), len(args))
	// #nosec G201 -- table and key identifiers are quoted by pgx.Identifier;
	// cursor values and the limit remain protocol parameters.
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, false, err
	}
	defer rows.Close()
	records := make([]connector.Record, 0, limit)
	var nextCursor []byte
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, false, err
		}
		fields := rows.FieldDescriptions()
		row := make(map[string]any, len(fields))
		for i, field := range fields {
			row[field.Name] = values[i]
		}
		if err := connector.NormalizePostgresRecord(task.Schema, row); err != nil {
			return nil, nil, false, err
		}
		keyMap := make(map[string]any, len(task.KeyColumns))
		for _, key := range task.KeyColumns {
			keyMap[key] = row[key]
		}
		keyJSON, err := json.Marshal(keyMap)
		if err != nil {
			return nil, nil, false, err
		}
		nextCursor, err = encodeManagedSnapshotCursor(task, row)
		if err != nil {
			return nil, nil, false, err
		}
		records = append(records, connector.Record{Table: task.Table, Operation: connector.OpLoad, SchemaVersion: task.Schema.Version, Key: keyJSON, After: row, Timestamp: time.Unix(0, 0).UTC()})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, false, err
	}
	return records, nextCursor, len(records) < limit, nil
}

func samePublicationRelations(left, right []bootstrap.PublicationRelation) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

type managedManifestDigest struct {
	value string
	err   error
}

func managedManifestHash(tasks []bootstrap.SnapshotTask) managedManifestDigest {
	value, err := bootstrap.SnapshotManifestHash(tasks)
	return managedManifestDigest{value: value, err: err}
}

func managedBootstrapIntent(fence authority.RunFence, snapshot bootstrap.ExportedSnapshot, destinationRevisionID string) connector.BootstrapIntent {
	return connector.BootstrapIntent{
		FlowID: fence.FlowID, FlowIncarnationID: fence.FlowIncarnationID.String(), SourceLineageID: snapshot.SourceLineageID,
		BootstrapID: snapshot.BootstrapID.String(), BootstrapGeneration: snapshot.BootstrapGeneration,
		Generation: fence.Generation, AcquisitionID: fence.AcquisitionID.String(), LeaseEpoch: fence.LeaseEpoch,
		DestinationRevisionID: destinationRevisionID, ManifestHash: snapshot.ManifestHash,
	}
}

func managedBootstrapResult(snapshot bootstrap.ExportedSnapshot, checkpoint connector.Checkpoint) connector.ManagedBootstrapResult {
	return connector.ManagedBootstrapResult{
		SourceOptions: map[string]string{
			optSlot: snapshot.SlotName, optPublication: snapshot.Publication,
			optStartLSN: checkpoint.LSN, optCreateSlot: "false",
			optEnsurePublication: "false", optSyncPublication: "false", optEnsureState: "false",
			optPublicationRevision: snapshot.PublicationRevision,
		},
		Checkpoint: checkpoint, CheckpointValid: true,
	}
}

func loadManagedAuthoritativeCheckpoint(ctx context.Context, control *pgxpool.Pool, fence authority.RunFence) (connector.Checkpoint, error) {
	var lsn string
	var metadata []byte
	if err := control.QueryRow(ctx, `SELECT lsn,metadata FROM authoritative_checkpoints WHERE flow_incarnation_id=$1`, fence.FlowIncarnationID).Scan(&lsn, &metadata); err != nil {
		return connector.Checkpoint{}, fmt.Errorf("load completed bootstrap checkpoint: %w", err)
	}
	checkpoint := connector.Checkpoint{LSN: lsn}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &checkpoint.Metadata); err != nil {
			return connector.Checkpoint{}, fmt.Errorf("decode completed bootstrap checkpoint metadata: %w", err)
		}
	}
	return checkpoint, nil
}

func managedSelectionHash(spec connector.RuntimeSpec, projectionFingerprint string) string {
	keys := []string{optSourceSystemID, optSourceLineageID, optPublication, optPublicationTables, optPublicationSchemas, "tables", "schemas"}
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "projection_fingerprint=%s\n", projectionFingerprint)
	for _, key := range keys {
		_, _ = fmt.Fprintf(hash, "%s=%s\n", key, strings.TrimSpace(spec.Options[key]))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func managedPublicationName(fence authority.RunFence) string {
	digest := sha256.Sum256([]byte(fence.FlowID + "\x00" + fence.FlowIncarnationID.String()))
	return "wallaby_" + hex.EncodeToString(digest[:20])
}

func isInvalidSnapshotError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "invalid snapshot identifier") || strings.Contains(text, "exported snapshot") || strings.Contains(text, "exporter lost")
}

func (s *Source) beginManagedSnowflakeSourceCut(ctx context.Context) (_ pgx.Tx, resultErr error) {
	if s.ManagedControl == nil || s.managedFence == nil || s.lagPool == nil {
		return nil, errors.New("managed Snowflake source-cut creation requires bound PostgreSQL authority and control/catalog pools")
	}
	if _, err := bootstrap.NewBootstrapper(ctx, s.ManagedControl, s.dsn, s.lagPool, s.BootstrapHooks); err != nil {
		return nil, fmt.Errorf("initialize managed Snowflake source-cut authority: %w", err)
	}
	expectedSlot := bootstrap.GenerationSlotName(s.managedFence.FlowID, s.managedFence.FlowIncarnationID, 1)
	if s.slot != expectedSlot {
		return nil, fmt.Errorf("managed Snowflake clean start requires deterministic slot %s, got %s", expectedSlot, s.slot)
	}
	tx, err := s.ManagedControl.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin managed Snowflake source-cut authority: %w", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if err = authority.ValidateRunFence(ctx, tx, *s.managedFence); err != nil {
		return nil, err
	}
	var checkpointExists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM authoritative_checkpoints WHERE flow_incarnation_id=$1)`, s.managedFence.FlowIncarnationID).Scan(&checkpointExists); err != nil {
		return nil, fmt.Errorf("inspect managed Snowflake source-cut checkpoint: %w", err)
	}
	if checkpointExists {
		return nil, errors.New("managed Snowflake source-cut creation rejects an existing authoritative checkpoint")
	}
	if err = s.lagPool.QueryRow(ctx, `SELECT system_identifier::text,current_database() FROM pg_catalog.pg_control_system()`).Scan(&s.managedSourceSystem, &s.managedDatabase); err != nil {
		return nil, fmt.Errorf("identify managed Snowflake source cut: %w", err)
	}
	if expected := strings.TrimSpace(s.spec.Options[optSourceSystemID]); expected != s.managedSourceSystem {
		return nil, fmt.Errorf("managed Snowflake source system %s differs from configured %s", s.managedSourceSystem, expected)
	}
	var slotExists bool
	if err = s.lagPool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)`, s.slot).Scan(&slotExists); err != nil {
		return nil, fmt.Errorf("inspect managed Snowflake source-cut slot: %w", err)
	}
	if slotExists {
		return nil, fmt.Errorf("%w: managed Snowflake source-cut slot %s already exists without an authoritative checkpoint; ownership must be reconciled explicitly", connector.ErrDeliveryConflict, s.slot)
	}
	if len(s.managedPublicationSchemas) != 1 {
		return nil, fmt.Errorf("managed Snowflake source cut requires one admitted source relation, got %d", len(s.managedPublicationSchemas))
	}
	relation := s.managedPublicationSchemas[0]
	guard, err := s.lagPool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin managed Snowflake source-cut relation guard: %w", err)
	}
	guarded := false
	defer func() {
		if !guarded {
			_ = guard.Rollback(context.WithoutCancel(ctx))
		}
	}()
	qualified := pgx.Identifier{relation.Namespace, relation.Name}.Sanitize()
	if _, err = guard.Exec(ctx, "SET LOCAL lock_timeout = '60s'"); err != nil {
		return nil, fmt.Errorf("set managed Snowflake source-cut lock timeout: %w", err)
	}
	if _, err = guard.Exec(ctx, "SET LOCAL statement_timeout = '120s'"); err != nil {
		return nil, fmt.Errorf("set managed Snowflake source-cut statement timeout: %w", err)
	}
	if _, err = guard.Exec(ctx, "LOCK TABLE "+qualified+" IN SHARE MODE"); err != nil {
		return nil, fmt.Errorf("lock managed Snowflake clean-start relation: %w", err)
	}
	var sourceHasRows bool
	if err = guard.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+qualified+" LIMIT 1)").Scan(&sourceHasRows); err != nil {
		return nil, fmt.Errorf("inspect managed Snowflake clean-start relation: %w", err)
	}
	if sourceHasRows {
		return nil, errors.New("managed Snowflake clean start requires an empty PostgreSQL source relation")
	}
	s.managedSourceCutGuard = guard
	guarded = true
	return tx, nil
}

func (s *Source) releaseManagedSnowflakeSourceCutGuard(ctx context.Context) error {
	if s.managedSourceCutGuard == nil {
		return nil
	}
	guard := s.managedSourceCutGuard
	s.managedSourceCutGuard = nil
	if err := guard.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("release managed Snowflake source-cut relation guard: %w", err)
	}
	return nil
}

func (s *Source) dropUncommittedManagedSnowflakeSourceCut(ctx context.Context) error {
	if s.lagPool == nil || strings.TrimSpace(s.slot) == "" {
		return nil
	}
	var database, plugin, slotType string
	var active bool
	err := s.lagPool.QueryRow(ctx, `
SELECT database,plugin,slot_type,active
FROM pg_catalog.pg_replication_slots
WHERE slot_name=$1`, s.slot).Scan(&database, &plugin, &slotType, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect uncommitted managed Snowflake source cut: %w", err)
	}
	if database != s.managedDatabase || plugin != "pgoutput" || slotType != "logical" || active {
		return fmt.Errorf("%w: uncommitted managed Snowflake slot %s database=%s plugin=%s type=%s active=%t", connector.ErrDeliveryConflict, s.slot, database, plugin, slotType, active)
	}
	if _, err := s.lagPool.Exec(ctx, `SELECT pg_catalog.pg_drop_replication_slot($1)`, s.slot); err != nil {
		return fmt.Errorf("drop uncommitted managed Snowflake source cut: %w", err)
	}
	return nil
}

func persistManagedSnowflakeSourceCut(ctx context.Context, tx pgx.Tx, fence connector.RunFence, checkpoint connector.Checkpoint) error {
	positionID, err := connector.CheckpointPositionID(checkpoint)
	if err != nil {
		return err
	}
	metadata, err := json.Marshal(checkpoint.Metadata)
	if err != nil {
		return fmt.Errorf("encode managed Snowflake source-cut metadata: %w", err)
	}
	if len(metadata) == 0 || string(metadata) == "null" {
		metadata = []byte("{}")
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO authoritative_checkpoints (
  flow_incarnation_id,flow_id,generation,acquisition_id,lease_epoch,lsn,metadata
) VALUES ($1,$2,$3,$4,$5,$6,$7)`, fence.FlowIncarnationID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch, checkpoint.LSN, metadata); err != nil {
		return fmt.Errorf("persist managed Snowflake source-cut checkpoint: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO source_ack_intents (
  flow_incarnation_id,position_id,checkpoint_lsn,generation,acquisition_id,lease_epoch
) VALUES ($1,$2,$3,$4,$5,$6)`, fence.FlowIncarnationID, positionID, checkpoint.LSN, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch); err != nil {
		return fmt.Errorf("persist managed Snowflake source-cut ACK intent: %w", err)
	}
	return nil
}

func (s *Source) persistManagedSnowflakeSourceCutResource(ctx context.Context, tx pgx.Tx, fence connector.RunFence, checkpoint connector.Checkpoint) error {
	resourceID := uuid.New()
	operationID := uuid.New()
	digest := sha256.Sum256([]byte(s.managedSourceSystem + "\x00" + s.managedDatabase + "\x00" + s.slot + "\x00pgoutput"))
	revision := hex.EncodeToString(digest[:])
	if _, err := tx.Exec(ctx, `
INSERT INTO source_resources (
  flow_incarnation_id,resource_kind,resource_id,flow_id,generation,acquisition_id,lease_epoch,
  created_generation,created_acquisition_id,created_lease_epoch,
  source_system_id,database_name,physical_name,ownership,revision,state
) VALUES ($1,'slot',$2,$3,$4,$5,$6,$4,$5,$6,$7,$8,$9,'owned',$10,'ready')`,
		fence.FlowIncarnationID, resourceID, fence.FlowID, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch,
		s.managedSourceSystem, s.managedDatabase, s.slot, revision); err != nil {
		return fmt.Errorf("persist managed Snowflake source-cut slot: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO source_resource_operations (
  operation_id,flow_incarnation_id,resource_kind,resource_id,operation,desired_revision,
  generation,acquisition_id,lease_epoch,status,source_system_id,database_name,physical_name,
  external_evidence,completed_at
) VALUES ($1,$2,'slot',$3,'create',$4,$5,$6,$7,'applied',$8,$9,$10,
  jsonb_build_object('consistent_lsn',$11::text),clock_timestamp())`,
		operationID, fence.FlowIncarnationID, resourceID, revision, fence.Generation, fence.AcquisitionID, fence.LeaseEpoch,
		s.managedSourceSystem, s.managedDatabase, s.slot, checkpoint.LSN); err != nil {
		return fmt.Errorf("persist managed Snowflake source-cut slot operation: %w", err)
	}
	return nil
}

// BindRunFence threads the producer capability into the registry hook before
// logical replication can emit schema or DDL mutations.
func (s *Source) BindRunFence(fence connector.RunFence) error {
	if err := fence.Validate(); err != nil {
		return err
	}
	bound := fence
	s.managedFence = &bound
	if s.SchemaHook == nil {
		return nil
	}
	binder, ok := s.SchemaHook.(connector.RunFenceBinder)
	if !ok {
		return errors.New("managed PostgreSQL schema hook does not accept the acquired RunFence")
	}
	return binder.BindRunFence(fence)
}

var _ connector.ManagedBootstrapSource = (*Source)(nil)
var _ connector.ManagedSourceResourceCleaner = (*Source)(nil)
var _ connector.RunFenceBinder = (*Source)(nil)
