package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

var (
	_ connector.ManagedTransactionDestination = (*Destination)(nil)
	_ connector.ManagedTransactionPreparer    = (*Destination)(nil)
	_ connector.ManagedSourceSchemaValidator  = (*Destination)(nil)
)

type managedHooks struct {
	BeforeCommit func() error
	AfterCommit  func() error
}

func (d *Destination) managedHooksSnapshot() managedHooks {
	d.managedHooksMu.RLock()
	defer d.managedHooksMu.RUnlock()
	return d.managedHooks
}

// InitializeManagedDelivery verifies that Open established the exact authority
// required by the configured managed profile before any managed source I/O.
func (d *Destination) InitializeManagedDelivery(context.Context) error {
	if d.db == nil {
		return errors.New("managed Snowflake destination not initialized")
	}
	switch d.managedProfile {
	case connector.ManagedProfilePostgresToSnowflakeSQLV1:
		if strings.TrimSpace(d.managedConfig.destinationRevision) == "" || strings.TrimSpace(d.managedConfig.receiptsTable) == "" || strings.TrimSpace(d.managedConfig.schemaContractHash) == "" {
			return errors.New("managed Snowflake SQL receipt authority is not configured")
		}
	case connector.ManagedProfilePostgresToSnowflakeStagedAppendV1:
		if strings.TrimSpace(d.stagedConfig.destinationRevision) == "" || strings.TrimSpace(d.stagedConfig.receiptsTable) == "" || strings.TrimSpace(d.stagedConfig.schemaContractHash) == "" || strings.TrimSpace(d.stagedCatalogFingerprint) == "" {
			return errors.New("managed staged Snowflake receipt and catalog authority is not configured")
		}
	case connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1:
		if !ManagedStreamingTransportAvailable() {
			return ErrManagedStreamingTransportUnavailable
		}
		if strings.TrimSpace(d.streamConfig.destinationRevision) == "" || strings.TrimSpace(d.streamConfig.receiptsTable) == "" || strings.TrimSpace(d.streamConfig.channelStateTable) == "" || strings.TrimSpace(d.streamConfig.schemaContractHash) == "" || strings.TrimSpace(d.streamCatalogFingerprint) == "" {
			return errors.New("managed streaming Snowflake receipt, channel, and catalog authority is not configured")
		}
	default:
		return errors.New("managed Snowflake destination not initialized")
	}
	return nil
}

// Apply is intentionally unavailable because the Snowflake SQL profile can
// authorize only a complete source transaction and its atomic target receipt.
func (d *Destination) Apply(_ context.Context, intent connector.DeliveryIntent, _ connector.Batch) (connector.DeliveryEvidence, error) {
	if err := intent.Validate(); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	return connector.DeliveryEvidence{}, errors.New("managed Snowflake SQL profile requires ApplyTransaction")
}

// ValidateManagedSourceSchema compares the projected live pg_catalog relation
// with the immutable projected contract before the runner reads WAL.
func (d *Destination) ValidateManagedSourceSchema(schema connector.Schema) error {
	if d.db == nil {
		return errors.New("managed Snowflake destination not initialized")
	}
	var contract connector.Schema
	switch d.managedProfile {
	case connector.ManagedProfilePostgresToSnowflakeSQLV1:
		contract = d.managedConfig.schemaContract
	case connector.ManagedProfilePostgresToSnowflakeStagedAppendV1:
		contract = d.stagedConfig.schemaContract
	case connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1:
		contract = d.streamConfig.schemaContract
	default:
		return errors.New("managed Snowflake destination not initialized")
	}
	if err := validateManagedRuntimeSchema(contract, schema); err != nil {
		return fmt.Errorf("validate projected live PostgreSQL schema for managed Snowflake: %w", err)
	}
	expectedIdentity, err := managedIdentityColumns(contract)
	if err != nil {
		return err
	}
	actualIdentity, err := managedIdentityColumns(schema)
	if err != nil {
		return fmt.Errorf("validate projected live PostgreSQL primary key for managed Snowflake: %w", err)
	}
	if !slices.Equal(expectedIdentity, actualIdentity) {
		return fmt.Errorf("%w: live PostgreSQL primary-key order %v differs from configured order %v", errManagedSnowflakeSchemaNotReconciled, actualIdentity, expectedIdentity)
	}
	return nil
}

// ValidateTransaction proves the complete transaction through the same deep
// planning and catalog-validation seam used immediately before attempt prepare.
func (d *Destination) ValidateTransaction(ctx context.Context, transaction connector.SourceTransaction) error {
	contentHash, logicalBatchID, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return err
	}
	intent := connector.DeliveryIntent{
		FlowID: "validation", FlowIncarnationID: "validation", SourceLineageID: transaction.SourceLineageID,
		Generation: 1, AcquisitionID: "validation", LeaseEpoch: 1,
		DestinationRevisionID: d.managedConfig.destinationRevision,
		LogicalBatchID:        logicalBatchID, PositionID: transaction.Checkpoint.LSN, ContentHash: contentHash,
	}
	_, err = d.PrepareTransaction(ctx, intent, transaction)
	return err
}

type preparedManagedSnowflakeTransaction struct {
	destination *Destination
	intent      connector.DeliveryIntent
	plan        managedSnowflakePlan
}

// PrepareTransaction validates and retains one immutable SQL plan before the
// PostgreSQL coordinator creates a new destination attempt.
func (d *Destination) PrepareTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.PreparedManagedTransaction, error) {
	if d.managedProfile == connector.ManagedProfilePostgresToSnowflakeStagedAppendV1 {
		return d.prepareManagedStaged(ctx, intent, transaction)
	}
	if d.managedProfile == connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 {
		return d.prepareManagedStreaming(ctx, intent, transaction)
	}
	if d.db == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeSQLV1 {
		return nil, errors.New("managed Snowflake destination not initialized")
	}
	plan, err := planManagedSnowflakeTransaction(d.managedConfig, intent, transaction)
	if err != nil {
		return nil, err
	}
	conn, err := d.acquireManagedSnowflakeConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	catalog, err := d.loadManagedSnowflakeCatalogWith(ctx, conn)
	if err != nil {
		return nil, err
	}
	if err := validateManagedSnowflakeCatalog(d.managedConfig, catalog); err != nil {
		return nil, err
	}
	catalogFingerprint, err := managedSnowflakeCatalogFingerprint(catalog)
	if err != nil {
		return nil, err
	}
	if err := d.validateManagedSnowflakeReceiptScope(ctx, conn, intent.FlowIncarnationID); err != nil {
		return nil, err
	}
	plan.catalogFingerprint = catalogFingerprint
	plan.receipt.catalogFingerprint = catalogFingerprint
	return &preparedManagedSnowflakeTransaction{destination: d, intent: intent, plan: plan}, nil
}

// ApplyTransaction executes all source-ordered DML and the target receipt in
// one Snowflake transaction. Any COMMIT transport failure remains ambiguous.
func (d *Destination) ApplyTransaction(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	prepared, err := d.PrepareTransaction(ctx, intent, transaction)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	return prepared.Apply(ctx)
}

func (p *preparedManagedSnowflakeTransaction) Apply(ctx context.Context) (_ connector.DeliveryEvidence, resultErr error) {
	ctx, endTransaction := telemetry.StartSnowflakeManagedSpan(
		ctx, "transaction", p.plan.receipt.externalID, p.intent.LogicalBatchID,
		int64(p.plan.recordCount), p.plan.encodedBytes,
	)
	defer func() { endTransaction(resultErr) }()
	if err := ctx.Err(); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	conn, err := p.destination.acquireManagedSnowflakeConn(ctx)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	defer func() { _ = conn.Close() }()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return connector.DeliveryEvidence{}, fmt.Errorf("begin managed Snowflake transaction: %w", err)
	}
	transactionEnded := false
	mutated := false
	defer func() {
		if transactionEnded {
			return
		}
		rollbackErr := tx.Rollback()
		if rollbackErr == nil || errors.Is(rollbackErr, sql.ErrTxDone) {
			return
		}
		discardManagedSnowflakeConn(conn)
		if mutated {
			resultErr = errors.Join(resultErr, fmt.Errorf("%w: rollback managed Snowflake transaction: %w", connector.ErrDeliveryIndeterminate, rollbackErr))
		}
	}()

	transactionIdentity := ""
	if p.plan.catalogFingerprint != "" {
		transactionIdentity, err = managedSnowflakeCurrentTransaction(ctx, tx)
		if err != nil {
			return connector.DeliveryEvidence{}, err
		}
		catalog, catalogErr := p.destination.loadManagedSnowflakeCatalogWith(ctx, tx)
		if catalogErr != nil {
			return connector.DeliveryEvidence{}, catalogErr
		}
		if catalogErr := validateManagedSnowflakeCatalog(p.destination.managedConfig, catalog); catalogErr != nil {
			return connector.DeliveryEvidence{}, catalogErr
		}
		catalogFingerprint, catalogErr := managedSnowflakeCatalogFingerprint(catalog)
		if catalogErr != nil {
			return connector.DeliveryEvidence{}, catalogErr
		}
		if catalogFingerprint != p.plan.catalogFingerprint {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: managed Snowflake catalog changed between planning and apply", connector.ErrDeliveryConflict)
		}
		if err := validateManagedSnowflakeCurrentTransaction(ctx, tx, transactionIdentity); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("catalog inspection escaped managed Snowflake transaction: %w", err)
		}
	}

	if p.plan.catalogFingerprint != "" {
		if err := p.destination.validateManagedSnowflakeReceiptScope(ctx, tx, p.intent.FlowIncarnationID); err != nil {
			return connector.DeliveryEvidence{}, err
		}
	}
	existing, found, err := loadManagedSnowflakeReceipt(ctx, tx, p.destination.managedConfig, p.intent, p.plan.receipt.externalID)
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	if found {
		if err := validateManagedSnowflakeReceipt(p.plan.receipt, existing); err != nil {
			return connector.DeliveryEvidence{}, err
		}
		return connector.DeliveryEvidence{ExternalID: existing.externalID, ContentHash: existing.contentHash}, nil
	}

	// The receipt is inserted before any target DML. Its enforced hybrid-table
	// keys serialize stale owners; rollback removes it together with all DML.
	receiptCtx, endReceipt := telemetry.StartSnowflakeManagedSpan(ctx, "receipt", p.plan.receipt.externalID, p.intent.LogicalBatchID, 1, 0)
	receiptCtx, recordQueryID := managedSnowflakeQueryIDContext(receiptCtx)
	receiptResult, receiptErr := tx.ExecContext(receiptCtx, managedReceiptInsertSQL(p.destination.managedConfig), managedReceiptValues(p.plan.receipt)...)
	recordQueryID()
	if receiptErr != nil {
		endReceipt(receiptErr)
		return connector.DeliveryEvidence{}, fmt.Errorf("%w: insert managed Snowflake receipt: %w", connector.ErrDeliveryIndeterminate, receiptErr)
	}
	mutated = true
	receiptRows, receiptRowsErr := receiptResult.RowsAffected()
	if receiptRowsErr != nil || receiptRows != 1 {
		if receiptRowsErr == nil {
			receiptRowsErr = fmt.Errorf("managed Snowflake receipt insert affected %d rows, want exactly 1", receiptRows)
		}
		endReceipt(receiptRowsErr)
		return connector.DeliveryEvidence{}, receiptRowsErr
	}
	endReceipt(nil)

	for _, operation := range p.plan.operations {
		operationID := fmt.Sprintf("%s:%d:%d", p.plan.receipt.externalID, operation.identity.fragmentOrdinal, operation.identity.recordOrdinal)
		operationCtx, endOperation := telemetry.StartSnowflakeManagedSpan(ctx, "dml", operationID, p.intent.LogicalBatchID, 1, operation.bytes)
		operationCtx, recordQueryID := managedSnowflakeQueryIDContext(operationCtx)
		result, execErr := tx.ExecContext(operationCtx, operation.query, operation.args...)
		recordQueryID()
		if execErr != nil {
			endOperation(execErr)
			return connector.DeliveryEvidence{}, fmt.Errorf("execute managed Snowflake fragment %d record %d: %w", operation.identity.fragmentOrdinal, operation.identity.recordOrdinal, execErr)
		}
		rows, rowsErr := result.RowsAffected()
		if rowsErr != nil {
			endOperation(rowsErr)
			return connector.DeliveryEvidence{}, fmt.Errorf("read managed Snowflake DML cardinality: %w", rowsErr)
		}
		if rows != 1 {
			cardinalityErr := fmt.Errorf("managed Snowflake %s affected %d rows, want exactly 1", operation.identity.operation, rows)
			endOperation(cardinalityErr)
			return connector.DeliveryEvidence{}, cardinalityErr
		}
		endOperation(nil)
	}
	if p.plan.catalogFingerprint != "" {
		catalog, catalogErr := p.destination.loadManagedSnowflakeCatalogWith(ctx, tx)
		if catalogErr != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("reinspect managed Snowflake catalog after DML: %w", catalogErr)
		}
		if catalogErr := validateManagedSnowflakeCatalog(p.destination.managedConfig, catalog); catalogErr != nil {
			return connector.DeliveryEvidence{}, catalogErr
		}
		catalogFingerprint, catalogErr := managedSnowflakeCatalogFingerprint(catalog)
		if catalogErr != nil {
			return connector.DeliveryEvidence{}, catalogErr
		}
		if catalogFingerprint != p.plan.catalogFingerprint {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: managed Snowflake catalog changed during apply", connector.ErrDeliveryConflict)
		}
		if err := validateManagedSnowflakeCurrentTransaction(ctx, tx, transactionIdentity); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("managed Snowflake transaction ended before commit: %w", err)
		}
	}
	if hook := p.destination.managedHooksSnapshot().BeforeCommit; hook != nil {
		if err := hook(); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("before managed Snowflake commit: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		transactionEnded = true // database/sql marks Tx done before the driver call.
		discardManagedSnowflakeConn(conn)
		return connector.DeliveryEvidence{}, fmt.Errorf("%w: commit managed Snowflake logical batch %s: %w", connector.ErrDeliveryIndeterminate, p.intent.LogicalBatchID, err)
	}
	transactionEnded = true
	if hook := p.destination.managedHooksSnapshot().AfterCommit; hook != nil {
		if err := hook(); err != nil {
			return connector.DeliveryEvidence{}, fmt.Errorf("%w: injected after Snowflake commit: %w", connector.ErrDeliveryIndeterminate, err)
		}
	}
	return connector.DeliveryEvidence{ExternalID: p.plan.receipt.externalID, ContentHash: p.intent.ContentHash}, nil
}

// Reconcile treats only one fully matching target receipt as applied. Queries
// include every stable identity so a reused logical batch, position, or external
// marker fails as a conflict instead of appearing absent.
func (d *Destination) Reconcile(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if d.managedProfile == connector.ManagedProfilePostgresToSnowflakeStagedAppendV1 {
		return d.reconcileManagedStaged(ctx, intent)
	}
	if d.managedProfile == connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 {
		return d.reconcileManagedStreaming(ctx, intent)
	}
	if err := intent.Validate(); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if err := validateManagedSnowflakeIntentBounds(intent); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if d.db == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeSQLV1 {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.New("managed Snowflake destination not initialized")
	}
	if intent.FlowID != d.managedConfig.flowID {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: delivery flow differs from admitted Snowflake flow", connector.ErrDeliveryConflict)
	}
	if intent.DestinationRevisionID != d.managedConfig.destinationRevision {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf("%w: delivery destination revision differs from admitted Snowflake revision", connector.ErrDeliveryConflict)
	}
	expected := managedSnowflakeReceipt{
		profileVersion: d.managedConfig.profile, flowID: intent.FlowID, flowIncarnationID: intent.FlowIncarnationID,
		sourceLineageID: intent.SourceLineageID, destinationRevisionID: intent.DestinationRevisionID,
		logicalBatchID: intent.LogicalBatchID, positionID: intent.PositionID, contentHash: intent.ContentHash,
		schemaContractHash: d.managedConfig.schemaContractHash,
		manifestHash:       managedDestinationManifestHash(d.managedConfig, intent),
	}
	expected.externalID = "sf-marker:v1:" + expected.manifestHash
	reconcileCtx, endReconcile := telemetry.StartSnowflakeManagedSpan(ctx, "reconcile", expected.externalID, intent.LogicalBatchID, 0, 0)
	conn, err := d.acquireManagedSnowflakeConn(reconcileCtx)
	if err != nil {
		endReconcile(err)
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	defer func() { _ = conn.Close() }()
	if d.managedConfig.validateEveryConnection {
		catalog, catalogErr := d.loadManagedSnowflakeCatalogWith(reconcileCtx, conn)
		if catalogErr != nil {
			endReconcile(catalogErr)
			return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, catalogErr
		}
		if catalogErr := validateManagedSnowflakeCatalog(d.managedConfig, catalog); catalogErr != nil {
			endReconcile(catalogErr)
			return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, catalogErr
		}
		expected.catalogFingerprint, catalogErr = managedSnowflakeCatalogFingerprint(catalog)
		if catalogErr != nil {
			endReconcile(catalogErr)
			return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, catalogErr
		}
	}
	if d.managedConfig.validateEveryConnection {
		if err := d.validateManagedSnowflakeReceiptScope(reconcileCtx, conn, intent.FlowIncarnationID); err != nil {
			endReconcile(err)
			return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
		}
	}
	receipt, found, err := loadManagedSnowflakeReceipt(reconcileCtx, conn, d.managedConfig, intent, expected.externalID)
	if err != nil {
		endReconcile(err)
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if !found {
		endReconcile(nil)
		return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
	}
	if err := validateManagedSnowflakeReceiptIdentity(expected, receipt); err != nil {
		endReconcile(err)
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	endReconcile(nil)
	return connector.DeliveryApplied, connector.DeliveryEvidence{ExternalID: receipt.externalID, ContentHash: receipt.contentHash}, nil
}

// ValidateManagedFlowScope rejects object reuse across flow incarnations before
// the managed runner reads WAL or acknowledges any new source position.
func (d *Destination) ValidateManagedFlowScope(ctx context.Context, flowID, flowIncarnationID string) error {
	if d.managedProfile == connector.ManagedProfilePostgresToSnowflakeStagedAppendV1 {
		if d.db == nil {
			return errors.New("managed staged Snowflake destination is not open")
		}
		if flowID != d.stagedConfig.flowID || strings.TrimSpace(flowIncarnationID) == "" {
			return fmt.Errorf("%w: managed staged Snowflake flow scope differs from admitted configuration", connector.ErrDeliveryConflict)
		}
		return d.validateStagedSnowflakeReceiptScope(ctx, connector.DeliveryIntent{FlowIncarnationID: flowIncarnationID, LogicalBatchID: "scope-validation"})
	}
	if d.db == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeSQLV1 {
		return errors.New("managed Snowflake destination is not open")
	}
	if flowID != d.managedConfig.flowID || strings.TrimSpace(flowIncarnationID) == "" {
		return fmt.Errorf("%w: managed Snowflake flow scope differs from admitted configuration", connector.ErrDeliveryConflict)
	}
	conn, err := d.acquireManagedSnowflakeConn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return d.validateManagedSnowflakeReceiptScope(ctx, conn, flowIncarnationID)
}

func (d *Destination) validateManagedSnowflakeReceiptScope(ctx context.Context, queryer managedSnowflakeCatalogQueryer, flowIncarnationID string) error {
	flowIncarnationID = strings.TrimSpace(flowIncarnationID)
	if flowIncarnationID == "" {
		return errors.New("managed Snowflake flow incarnation is required")
	}
	d.managedScopeMu.Lock()
	defer d.managedScopeMu.Unlock()
	if d.managedFlowIncarnation != "" {
		if d.managedFlowIncarnation != flowIncarnationID {
			return fmt.Errorf("%w: managed Snowflake destination is already bound to flow incarnation %s", connector.ErrDeliveryConflict, d.managedFlowIncarnation)
		}
		return nil
	}
	var foreign int
	query := "SELECT COUNT(*) FROM " + managedSnowflakeQualifiedTable(d.managedConfig, d.managedConfig.receiptsTable) +
		" WHERE \"FLOW_ID\" <> ? OR \"FLOW_INCARNATION_ID\" <> ?"
	if err := queryer.QueryRowContext(ctx, query, d.managedConfig.flowID, flowIncarnationID).Scan(&foreign); err != nil {
		return fmt.Errorf("validate managed Snowflake receipt flow scope: %w", err)
	}
	if foreign != 0 {
		return fmt.Errorf("%w: managed Snowflake receipt table contains %d rows for another flow incarnation", connector.ErrDeliveryConflict, foreign)
	}
	d.managedFlowIncarnation = flowIncarnationID
	return nil
}

func managedSnowflakeCurrentTransaction(ctx context.Context, queryer managedSnowflakeCatalogQueryer) (string, error) {
	var identity any
	if err := queryer.QueryRowContext(ctx, "SELECT CURRENT_TRANSACTION()").Scan(&identity); err != nil {
		return "", fmt.Errorf("read managed Snowflake transaction identity: %w", err)
	}
	value := strings.TrimSpace(sqlValueString(identity))
	if value == "" {
		return "", errors.New("managed Snowflake transaction identity is NULL")
	}
	return value, nil
}

func validateManagedSnowflakeCurrentTransaction(ctx context.Context, queryer managedSnowflakeCatalogQueryer, expected string) error {
	actual, err := managedSnowflakeCurrentTransaction(ctx, queryer)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("managed Snowflake transaction identity changed from %s to %s", expected, actual)
	}
	return nil
}

type managedSnowflakeQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func loadManagedSnowflakeReceipt(ctx context.Context, queryer managedSnowflakeQueryer, cfg managedConfig, intent connector.DeliveryIntent, externalID string) (managedSnowflakeReceipt, bool, error) {
	queryCtx, recordQueryID := managedSnowflakeQueryIDContext(ctx)
	rows, err := queryer.QueryContext(queryCtx, managedReceiptLookupSQL(cfg),
		intent.FlowIncarnationID, intent.DestinationRevisionID, intent.LogicalBatchID,
		intent.FlowIncarnationID, intent.DestinationRevisionID, intent.SourceLineageID, intent.PositionID,
		externalID,
	)
	recordQueryID()
	if err != nil {
		return managedSnowflakeReceipt{}, false, fmt.Errorf("query managed Snowflake receipt: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var receipts []managedSnowflakeReceipt
	for rows.Next() {
		var receipt managedSnowflakeReceipt
		var transactionID int64
		if err := rows.Scan(
			&receipt.profileVersion, &receipt.flowID, &receipt.flowIncarnationID, &receipt.sourceLineageID,
			&receipt.destinationRevisionID, &receipt.logicalBatchID, &receipt.positionID, &receipt.contentHash,
			&receipt.schemaContractHash, &receipt.catalogFingerprint, &receipt.manifestHash, &receipt.externalID, &receipt.generation, &receipt.acquisitionID,
			&receipt.leaseEpoch, &transactionID, &receipt.fragmentCount, &receipt.recordCount,
		); err != nil {
			return managedSnowflakeReceipt{}, false, fmt.Errorf("scan managed Snowflake receipt: %w", err)
		}
		if transactionID < 0 || transactionID > int64(^uint32(0)) {
			return managedSnowflakeReceipt{}, false, errors.New("managed Snowflake receipt transaction ID is out of range")
		}
		receipt.transactionID = uint32(transactionID) // #nosec G115 -- range checked above.
		receipts = append(receipts, receipt)
		if len(receipts) > 1 {
			return managedSnowflakeReceipt{}, false, fmt.Errorf("%w: multiple managed Snowflake receipts match one delivery identity", connector.ErrDeliveryConflict)
		}
	}
	if err := rows.Err(); err != nil {
		return managedSnowflakeReceipt{}, false, fmt.Errorf("iterate managed Snowflake receipts: %w", err)
	}
	if len(receipts) == 0 {
		return managedSnowflakeReceipt{}, false, nil
	}
	return receipts[0], true, nil
}

func validateManagedSnowflakeReceipt(expected, actual managedSnowflakeReceipt) error {
	if err := validateManagedSnowflakeReceiptIdentity(expected, actual); err != nil {
		return err
	}
	// Generation, acquisition, and lease identify the external attempt that won.
	// A replacement PostgreSQL owner must adopt that attempt when every stable
	// transaction identity and manifest field matches.
	if expected.transactionID != actual.transactionID || expected.fragmentCount != actual.fragmentCount || expected.recordCount != actual.recordCount {
		return fmt.Errorf("%w: managed Snowflake receipt transaction manifest differs", connector.ErrDeliveryConflict)
	}
	return nil
}

func validateManagedSnowflakeReceiptIdentity(expected, actual managedSnowflakeReceipt) error {
	if expected.profileVersion != actual.profileVersion || expected.flowID != actual.flowID ||
		expected.flowIncarnationID != actual.flowIncarnationID || expected.sourceLineageID != actual.sourceLineageID ||
		expected.destinationRevisionID != actual.destinationRevisionID || expected.logicalBatchID != actual.logicalBatchID ||
		expected.positionID != actual.positionID || expected.contentHash != actual.contentHash ||
		expected.schemaContractHash != actual.schemaContractHash || expected.catalogFingerprint != actual.catalogFingerprint ||
		expected.manifestHash != actual.manifestHash || expected.externalID != actual.externalID {
		return fmt.Errorf("%w: managed Snowflake receipt identity or hash differs", connector.ErrDeliveryConflict)
	}
	return nil
}

func managedReceiptLookupColumns() []string {
	return []string{
		"PROFILE_VERSION", "FLOW_ID", "FLOW_INCARNATION_ID", "SOURCE_LINEAGE_ID",
		"DESTINATION_REVISION_ID", "LOGICAL_BATCH_ID", "POSITION_ID", "CONTENT_HASH",
		"SCHEMA_CONTRACT_HASH", "CATALOG_FINGERPRINT", "MANIFEST_HASH", "EXTERNAL_ID", "GENERATION", "ACQUISITION_ID", "LEASE_EPOCH",
		"TRANSACTION_ID", "FRAGMENT_COUNT", "RECORD_COUNT",
	}
}

func managedReceiptLookupSQL(cfg managedConfig) string {
	table := managedSnowflakeQualifiedTable(cfg, cfg.receiptsTable)
	return "SELECT " + quoteColumns(managedReceiptLookupColumns()) + " FROM " + table +
		" WHERE (\"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"LOGICAL_BATCH_ID\" = ?) OR " +
		"(\"FLOW_INCARNATION_ID\" = ? AND \"DESTINATION_REVISION_ID\" = ? AND \"SOURCE_LINEAGE_ID\" = ? AND \"POSITION_ID\" = ?) OR " +
		"\"EXTERNAL_ID\" = ?"
}

func managedReceiptInsertSQL(cfg managedConfig) string {
	columns := managedReceiptLookupColumns()
	return "INSERT INTO " + managedSnowflakeQualifiedTable(cfg, cfg.receiptsTable) +
		" (" + quoteColumns(columns) + ", \"COMMITTED_AT\") VALUES (" + placeholders(len(columns)) + ", CURRENT_TIMESTAMP())"
}

func managedReceiptValues(receipt managedSnowflakeReceipt) []any {
	return []any{
		receipt.profileVersion, receipt.flowID, receipt.flowIncarnationID, receipt.sourceLineageID,
		receipt.destinationRevisionID, receipt.logicalBatchID, receipt.positionID, receipt.contentHash,
		receipt.schemaContractHash, receipt.catalogFingerprint, receipt.manifestHash, receipt.externalID, receipt.generation, receipt.acquisitionID, receipt.leaseEpoch,
		int64(receipt.transactionID), receipt.fragmentCount, receipt.recordCount,
	}
}

func managedSnowflakeQueryIDContext(ctx context.Context) (context.Context, func()) {
	queryIDs := make(chan string, 1)
	queryCtx := gosnowflake.WithQueryIDChan(ctx, queryIDs)
	return queryCtx, func() {
		select {
		case queryID := <-queryIDs:
			telemetry.RecordSnowflakeQueryID(ctx, queryID)
		default:
		}
	}
}

func managedSnowflakeQualifiedTable(cfg managedConfig, table string) string {
	return strings.Join([]string{quoteIdent(cfg.database, '"'), quoteIdent(cfg.schema, '"'), quoteIdent(table, '"')}, ".")
}
