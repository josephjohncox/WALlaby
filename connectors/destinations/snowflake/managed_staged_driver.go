package snowflake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// StagedHooks exposes deterministic fault boundaries around the ambiguous
// PUT/COPY/receipt transitions of the staged COPY profile. Production callers
// leave every hook nil; the live recovery matrix injects response loss.
type StagedHooks struct {
	AfterPut      func() error
	AfterCopy     func() error
	BeforeReceipt func() error
	AfterReceipt  func() error
}

// stagedDriver orchestrates the staged COPY append protocol against a
// stageProtocol seam. All crash-window recovery lives here so it can be proven
// exhaustively with an in-memory protocol fake.
type stagedDriver struct {
	proto              stageProtocol
	cfg                stagedConfig
	catalogFingerprint string
	hooks              StagedHooks
	sleep              func(context.Context, time.Duration) error
}

func newStagedDriver(proto stageProtocol, cfg stagedConfig, catalogFingerprint string, hooks StagedHooks) *stagedDriver {
	return &stagedDriver{proto: proto, cfg: cfg, catalogFingerprint: catalogFingerprint, hooks: hooks}
}

func (p managedStagedPlan) loadReceiptKey() stagedReceiptKey {
	return stagedReceiptKey{
		flowIncarnationID: p.receipt.flowIncarnationID, destinationRevisionID: p.receipt.destinationRevisionID,
		logicalBatchID: p.receipt.logicalBatchID, sourceLineageID: p.receipt.sourceLineageID,
		positionID: p.receipt.positionID, externalID: p.receipt.externalID, kind: stagedReceiptKindLoad,
	}
}

// apply materializes one committed transaction as an immutable stage object,
// loads it fail-closed, verifies completion through load history, and records a
// durable receipt. Every step is idempotent so a replay after any crash window
// converges on exactly one load receipt.
func (d *stagedDriver) apply(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (evidence connector.DeliveryEvidence, resultErr error) {
	plan, err := planManagedStagedTransaction(d.cfg, intent, transaction)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	plan.catalogFingerprint = d.catalogFingerprint
	plan.receipt.catalogFingerprint = d.catalogFingerprint

	ctx, endSpan := telemetry.StartSnowflakeManagedSpan(ctx, "stage", plan.identity.externalID, intent.LogicalBatchID, int64(plan.rowCount), plan.encodedBytes)
	defer func() { endSpan(resultErr) }()
	if err := ctx.Err(); err != nil {
		return connector.DeliveryEvidence{}, err
	}

	if existing, found, lookupErr := d.proto.LookupReceipt(ctx, d.cfg, plan.loadReceiptKey()); lookupErr != nil {
		return connector.DeliveryEvidence{}, lookupErr
	} else if found {
		if err := validateStagedReceipt(plan.receipt, existing); err != nil {
			return connector.DeliveryEvidence{}, err
		}
		return connector.DeliveryEvidence{ExternalID: existing.externalID, ContentHash: existing.contentHash}, nil
	}

	if err := d.ensureStageObject(ctx, plan); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	entry, err := d.ensureLoaded(ctx, plan)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	plan.receipt.loadRowCount = entry.rowCount
	plan.receipt.loadStatus = stagedLoadStatusLoaded

	if hook := d.hooks.BeforeReceipt; hook != nil {
		if err := hook(); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("before staged Snowflake receipt: %w", err)
		}
	}
	receiptCtx, endReceipt := telemetry.StartSnowflakeManagedSpan(ctx, "receipt", plan.identity.externalID, intent.LogicalBatchID, 1, 0)
	insert, insertErr := d.proto.InsertReceipt(receiptCtx, d.cfg, plan.receipt)
	endReceipt(insertErr)
	if insertErr != nil {
		return connector.DeliveryEvidence{}, insertErr
	}
	if !insert.inserted {
		existing, found, lookupErr := d.proto.LookupReceipt(ctx, d.cfg, plan.loadReceiptKey())
		if lookupErr != nil {
			return connector.DeliveryEvidence{}, lookupErr
		}
		if !found {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: staged Snowflake receipt insert reported a duplicate that is not visible", connector.ErrDeliveryIndeterminate)
		}
		if err := validateStagedReceipt(plan.receipt, existing); err != nil {
			return connector.DeliveryEvidence{}, err
		}
		return connector.DeliveryEvidence{ExternalID: existing.externalID, ContentHash: existing.contentHash}, nil
	}
	if hook := d.hooks.AfterReceipt; hook != nil {
		if err := hook(); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: injected after staged Snowflake receipt: %w", connector.ErrDeliveryIndeterminate, err)
		}
	}
	return connector.DeliveryEvidence{ExternalID: plan.receipt.externalID, ContentHash: plan.receipt.contentHash}, nil
}

// ensureStageObject guarantees the deterministic path holds exactly the planned
// bytes, reconciling every PUT-uncertainty window and rejecting any wrong-byte
// collision fail-closed.
func (d *stagedDriver) ensureStageObject(ctx context.Context, plan managedStagedPlan) (resultErr error) {
	ctx, endSpan := telemetry.StartSnowflakeManagedSpan(ctx, "stage_put", plan.identity.externalID, plan.receipt.logicalBatchID, int64(plan.rowCount), int64(len(plan.fileBytes)))
	defer func() { endSpan(resultErr) }()
	stageRef := plan.copyPlan.stageRef
	path := plan.identity.relativePath
	stat, err := d.proto.StatObject(ctx, stageRef, path)
	if err != nil {
		return err
	}
	if stat.present {
		return d.verifyStagedBytes(ctx, stat, plan)
	}
	putErr := d.proto.PutObject(ctx, stageRef, path, plan.fileBytes, plan.fileMD5)
	if putErr != nil {
		if errors.Is(putErr, errStagedWrongByteCollision) {
			return fmt.Errorf("%w: %w", connector.ErrDeliveryConflict, putErr)
		}
		// A failed PUT may still have durably staged the object. Reconcile the
		// uncertainty by re-reading the stage before deciding.
		recheck, statErr := d.proto.StatObject(ctx, stageRef, path)
		if statErr != nil {
			return errors.Join(putErr, statErr)
		}
		if !recheck.present {
			return fmt.Errorf("%w: staged Snowflake PUT did not durably stage the object: %w", connector.ErrDeliveryIndeterminate, putErr)
		}
		return d.verifyStagedBytes(ctx, recheck, plan)
	}
	if hook := d.hooks.AfterPut; hook != nil {
		if err := hook(); err != nil {
			return fmt.Errorf("%w: injected after staged Snowflake PUT: %w", connector.ErrDeliveryIndeterminate, err)
		}
	}
	confirm, err := d.proto.StatObject(ctx, stageRef, path)
	if err != nil {
		return err
	}
	if !confirm.present {
		return fmt.Errorf("%w: staged Snowflake PUT reported success but the object is absent", connector.ErrDeliveryIndeterminate)
	}
	return d.verifyStagedBytes(ctx, confirm, plan)
}

// maxStagedEncryptionOverheadBytes is the allowance added to the planned
// plaintext size when bounding a download. Client-side stage encryption adds at
// most a small header plus one AES block of padding, so 64 KiB is a deliberately
// generous ceiling that can never reject a legitimate replay while still
// refusing to download an object that is not plausibly this batch.
const maxStagedEncryptionOverheadBytes = 64 << 10

// verifyStagedBytes proves the staged object equals the immutable plan before
// any load. It downloads on every attempt, including immediately after this
// process staged the bytes, because LIST MD5 semantics on client-side-encrypted
// internal stages are not a proven equality oracle. The cost is one extra
// bounded download per transaction.
func (d *stagedDriver) verifyStagedBytes(ctx context.Context, stat stageObjectStat, plan managedStagedPlan) (resultErr error) {
	ctx, endSpan := telemetry.StartSnowflakeManagedSpan(ctx, "stage_verify", plan.identity.externalID, plan.receipt.logicalBatchID, int64(plan.rowCount), int64(len(plan.fileBytes)))
	defer func() { endSpan(resultErr) }()
	if stat.sizeBytes < 0 {
		return fmt.Errorf("%w: Snowflake LIST reported a negative staged-object size", connector.ErrDeliveryIndeterminate)
	}
	if len(plan.fileBytes) > 0 && stat.sizeBytes == 0 {
		return fmt.Errorf("%w: Snowflake LIST omitted the staged-object size needed to bound GET", connector.ErrDeliveryIndeterminate)
	}
	maxStoredBytes := int64(len(plan.fileBytes)) + maxStagedEncryptionOverheadBytes
	if stat.sizeBytes > maxStoredBytes {
		return fmt.Errorf("%w: staged Snowflake object size=%d exceeds planned plaintext plus encryption bound=%d", connector.ErrDeliveryConflict, stat.sizeBytes, maxStoredBytes)
	}
	content, err := d.proto.GetObject(ctx, plan.copyPlan.stageRef, plan.identity.relativePath, len(plan.fileBytes))
	if errors.Is(err, errStagedPlaintextOversize) {
		return fmt.Errorf("%w: staged Snowflake object holds more plaintext than the planned bytes: %w", connector.ErrDeliveryConflict, err)
	}
	if err != nil {
		return fmt.Errorf("%w: Snowflake could not provide bounded plaintext byte-equality evidence: %w", connector.ErrDeliveryIndeterminate, err)
	}
	return assertStagedBytes(stat, content, plan)
}

// assertStagedBytes requires exact decrypted GET bytes. LIST MD5 remains an
// additional collision signal when Snowflake supplies it, but it is never the
// sole equality proof because encrypted-stage size and checksum semantics vary.
func assertStagedBytes(stat stageObjectStat, content []byte, plan managedStagedPlan) error {
	if stat.md5 != "" && !strings.EqualFold(stat.md5, plan.fileMD5) {
		return fmt.Errorf("%w: staged Snowflake object md5=%s does not match planned bytes md5=%s", connector.ErrDeliveryConflict, stat.md5, plan.fileMD5)
	}
	if !bytes.Equal(content, plan.fileBytes) {
		return fmt.Errorf("%w: staged Snowflake GET plaintext does not equal the planned bytes", connector.ErrDeliveryConflict)
	}
	return nil
}

// ensureLoaded runs the fail-closed COPY (or refreshes the auto-ingest pipe) and
// then proves completion through Snowflake load history. Auto-ingest can never
// acknowledge before a completed load is verifiable.
func (d *stagedDriver) ensureLoaded(ctx context.Context, plan managedStagedPlan) (stageLoadEntry, error) {
	if d.cfg.autoIngest {
		pipeRef := managedSnowflakeStagedQualified(d.cfg, d.cfg.pipe)
		if err := d.proto.RefreshPipe(ctx, pipeRef, plan.identity.relativePath); err != nil {
			return stageLoadEntry{}, err
		}
		return d.verifyLoadHistory(ctx, plan)
	}
	copyCtx, endCopy := telemetry.StartSnowflakeManagedSpan(ctx, "copy", plan.identity.externalID, plan.receipt.logicalBatchID, int64(plan.rowCount), int64(len(plan.fileBytes)))
	result, copyErr := d.proto.Copy(copyCtx, plan.copyPlan)
	endCopy(copyErr)
	if copyErr == nil && result.present {
		entry, conclusive, err := interpretStagedCopyResult(result, plan.rowCount)
		if err != nil {
			return stageLoadEntry{}, err
		}
		if conclusive {
			if hook := d.hooks.AfterCopy; hook != nil {
				if hookErr := hook(); hookErr != nil {
					return stageLoadEntry{}, fmt.Errorf("%w: injected after staged Snowflake COPY: %w", connector.ErrDeliveryIndeterminate, hookErr)
				}
			}
			return entry, nil
		}
	}
	// A lost or inconclusive COPY response is reconciled through durable load
	// history: the COPY may have committed even though the response was lost.
	return d.verifyLoadHistory(ctx, plan)
}

func interpretStagedCopyResult(result stageCopyResult, expectedRows int) (stageLoadEntry, bool, error) {
	switch normalizeStagedLoadStatus(result.status) {
	case stagedHistoryLoaded:
		if result.errorsSeen != 0 || result.rowsLoaded != expectedRows {
			return stageLoadEntry{}, false, fmt.Errorf("%w: %w (rows_loaded=%d want=%d errors=%d)", connector.ErrDeliveryConflict, errStagedPartialLoad, result.rowsLoaded, expectedRows, result.errorsSeen)
		}
		return stageLoadEntry{present: true, status: stagedHistoryLoaded, rowCount: result.rowsLoaded}, true, nil
	case stagedHistoryPartiallyLoaded, stagedHistoryLoadFailed:
		return stageLoadEntry{}, false, fmt.Errorf("%w: %w: %s", connector.ErrDeliveryConflict, errStagedPartialLoad, result.firstError)
	default:
		// An empty or "skipped" status (a re-COPY of an already-loaded file with
		// FORCE=FALSE) is inconclusive; the durable history is authoritative.
		return stageLoadEntry{}, false, nil
	}
}

func (d *stagedDriver) verifyLoadHistory(ctx context.Context, plan managedStagedPlan) (stageLoadEntry, error) {
	// The synchronous COPY path probes history once: COPY is authoritative and
	// history is only consulted to reconcile a lost response, so COPY_HISTORY
	// ingestion latency surfaces as ErrDeliveryIndeterminate and is retried by the
	// outer coordinator. Only the async auto-ingest path polls within the bound.
	attempts := 1
	interval := time.Duration(0)
	if d.cfg.autoIngest {
		attempts = d.cfg.loadVerifyAttempts
		interval = d.cfg.loadVerifyInterval
	}
	if attempts < 1 {
		attempts = 1
	}
	verifyCtx, endVerify := telemetry.StartSnowflakeManagedSpan(ctx, "verify", plan.identity.externalID, plan.receipt.logicalBatchID, int64(plan.rowCount), 0)
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			endVerify(err)
			return stageLoadEntry{}, err
		}
		entry, err := d.proto.LoadHistory(verifyCtx, plan.copyPlan.target, plan.identity.relativePath)
		if err != nil {
			endVerify(err)
			return stageLoadEntry{}, err
		}
		if entry.present {
			switch normalizeStagedLoadStatus(entry.status) {
			case stagedHistoryLoaded:
				if entry.errorCount != 0 || entry.rowCount != plan.rowCount {
					lastErr = fmt.Errorf("%w: %w (rows=%d want=%d errors=%d)", connector.ErrDeliveryConflict, errStagedPartialLoad, entry.rowCount, plan.rowCount, entry.errorCount)
					endVerify(lastErr)
					return stageLoadEntry{}, lastErr
				}
				endVerify(nil)
				return entry, nil
			case stagedHistoryPartiallyLoaded, stagedHistoryLoadFailed:
				lastErr = fmt.Errorf("%w: %w: %s", connector.ErrDeliveryConflict, errStagedPartialLoad, entry.firstError)
				endVerify(lastErr)
				return stageLoadEntry{}, lastErr
			case stagedHistoryLoadInProgress:
				// Snowpipe is still importing the file; keep polling within the bound.
			default:
				// An unrecognized status is treated as in-progress and re-polled; it is
				// never adopted as a completion.
			}
		}
		if attempt+1 < attempts {
			if err := d.sleepFor(ctx, interval); err != nil {
				endVerify(err)
				return stageLoadEntry{}, err
			}
		}
	}
	lastErr = fmt.Errorf("%w: %w", connector.ErrDeliveryIndeterminate, errStagedLoadNotVisible)
	endVerify(lastErr)
	return stageLoadEntry{}, lastErr
}

func (d *stagedDriver) sleepFor(ctx context.Context, interval time.Duration) error {
	if d.sleep != nil {
		return d.sleep(ctx, interval)
	}
	if interval <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// reconcile treats only one fully matching durable load receipt as applied. It
// is read-only: an absent receipt is NotApplied so a replay can converge, even
// when load history already shows the file, because the durable receipt plus the
// history together are the completion proof.
func (d *stagedDriver) reconcile(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if err := validateManagedSnowflakeIntentBounds(intent); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if intent.FlowID != d.cfg.flowID {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: delivery flow differs from admitted staged Snowflake flow", connector.ErrDeliveryConflict)
	}
	if intent.DestinationRevisionID != d.cfg.destinationRevision {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: delivery destination revision differs from admitted staged Snowflake revision", connector.ErrDeliveryConflict)
	}
	copyPlan := newStagedCopyPlan(d.cfg)
	planHash := stagedPlanHash(copyPlan)
	identity, err := newManagedStagedIdentity(d.cfg, intent, planHash, intent.ContentHash)
	if err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	expected := managedStagedReceipt{
		kind: stagedReceiptKindLoad, profileVersion: d.cfg.profile, flowID: intent.FlowID, flowIncarnationID: intent.FlowIncarnationID,
		sourceLineageID: intent.SourceLineageID, destinationRevisionID: intent.DestinationRevisionID,
		logicalBatchID: intent.LogicalBatchID, positionID: intent.PositionID, contentHash: intent.ContentHash,
		schemaContractHash: d.cfg.schemaContractHash, catalogFingerprint: d.catalogFingerprint,
		manifestHash: identity.manifestHash, externalID: identity.externalID, stageName: d.cfg.stage, stagePath: identity.relativePath,
	}
	reconcileCtx, endReconcile := telemetry.StartSnowflakeManagedSpan(ctx, "reconcile", identity.externalID, intent.LogicalBatchID, 0, 0)
	receipt, found, err := d.proto.LookupReceipt(reconcileCtx, d.cfg, stagedReceiptKey{
		flowIncarnationID: intent.FlowIncarnationID, destinationRevisionID: intent.DestinationRevisionID,
		logicalBatchID: intent.LogicalBatchID, sourceLineageID: intent.SourceLineageID,
		positionID: intent.PositionID, externalID: identity.externalID, kind: stagedReceiptKindLoad,
	})
	if err != nil {
		endReconcile(err)
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if !found {
		endReconcile(nil)
		return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
	}
	if err := validateStagedReceiptIdentity(expected, receipt); err != nil {
		endReconcile(err)
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	endReconcile(nil)
	return connector.DeliveryApplied, connector.DeliveryEvidence{ExternalID: receipt.externalID, ContentHash: receipt.contentHash}, nil
}

// cleanup performs one bounded pass of stage-object retention for a flow
// incarnation. It removes only objects whose fully loaded, durably recorded
// batch is older than the retention window and not yet released, then writes an
// idempotent release receipt so the removal is convergent.
func (d *stagedDriver) cleanup(ctx context.Context, flowIncarnationID string) (released int, resultErr error) {
	ctx, endSpan := telemetry.StartSnowflakeManagedSpan(ctx, "cleanup", d.cfg.stage, flowIncarnationID, 0, 0)
	defer func() { endSpan(resultErr) }()
	if strings.TrimSpace(flowIncarnationID) == "" {
		return 0, errors.New("staged Snowflake cleanup requires a flow incarnation")
	}
	candidates, err := d.proto.ListReleasableReceipts(ctx, d.cfg, flowIncarnationID, d.cfg.cleanupRetention, d.cfg.cleanupMaxObjects)
	if err != nil {
		return 0, err
	}
	stageRef := managedSnowflakeStagedQualified(d.cfg, d.cfg.stage)
	for _, receipt := range candidates {
		if receipt.kind != stagedReceiptKindLoad || receipt.loadStatus != stagedLoadStatusLoaded {
			continue
		}
		if err := d.proto.RemoveObject(ctx, stageRef, receipt.stagePath); err != nil {
			return released, err
		}
		release := stagedReleaseReceipt(receipt)
		if _, err := d.proto.InsertReceipt(ctx, d.cfg, release); err != nil {
			return released, err
		}
		released++
	}
	return released, nil
}

func stagedReleaseReceipt(load managedStagedReceipt) managedStagedReceipt {
	release := load
	release.kind = stagedReceiptKindRelease
	release.externalID = load.externalID + ":release"
	release.loadStatus = stagedLoadStatusReleased
	return release
}

func validateStagedReceipt(expected, actual managedStagedReceipt) error {
	if err := validateStagedReceiptIdentity(expected, actual); err != nil {
		return err
	}
	if expected.fileContentHash != actual.fileContentHash || expected.fileMD5 != actual.fileMD5 {
		return fmt.Errorf("%w: staged Snowflake receipt file identity differs", connector.ErrDeliveryConflict)
	}
	if expected.transactionID != actual.transactionID || expected.fragmentCount != actual.fragmentCount ||
		expected.recordCount != actual.recordCount || (actual.loadRowCount != 0 && expected.recordCount != actual.loadRowCount) {
		return fmt.Errorf("%w: staged Snowflake receipt transaction manifest differs", connector.ErrDeliveryConflict)
	}
	return nil
}

func validateStagedReceiptIdentity(expected, actual managedStagedReceipt) error {
	if actual.kind != stagedReceiptKindLoad {
		return fmt.Errorf("%w: staged Snowflake receipt kind %q is not a load receipt", connector.ErrDeliveryConflict, actual.kind)
	}
	if expected.profileVersion != actual.profileVersion || expected.flowID != actual.flowID ||
		expected.flowIncarnationID != actual.flowIncarnationID || expected.sourceLineageID != actual.sourceLineageID ||
		expected.destinationRevisionID != actual.destinationRevisionID || expected.logicalBatchID != actual.logicalBatchID ||
		expected.positionID != actual.positionID || expected.contentHash != actual.contentHash ||
		expected.schemaContractHash != actual.schemaContractHash || expected.catalogFingerprint != actual.catalogFingerprint ||
		expected.manifestHash != actual.manifestHash || expected.externalID != actual.externalID ||
		expected.stageName != actual.stageName || expected.stagePath != actual.stagePath {
		return fmt.Errorf("%w: staged Snowflake receipt identity or hash differs", connector.ErrDeliveryConflict)
	}
	return nil
}
