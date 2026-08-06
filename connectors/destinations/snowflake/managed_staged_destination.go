package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func (d *Destination) stagedHooksSnapshot() stagedHooks {
	d.stagedHooksMu.RLock()
	defer d.stagedHooksMu.RUnlock()
	return d.stagedHooks
}

func (d *Destination) stagedSessionShim(cfg stagedConfig) managedConfig {
	return managedConfig{
		profile: cfg.profile, flowID: cfg.flowID, account: cfg.account, database: cfg.database, schema: cfg.schema,
		ownerRole: cfg.ownerRole, executionRole: cfg.executionRole, warehouse: cfg.warehouse,
		snowflakeVersion: cfg.snowflakeVersion, destinationRevision: cfg.destinationRevision,
		statementTimeoutSeconds: cfg.statementTimeoutSeconds, hybridLockTimeoutSeconds: cfg.statementTimeoutSeconds,
		validateEveryConnection: true,
	}
}

func (d *Destination) openManagedStaged(ctx context.Context, dsn string, spec connector.Spec) (resultErr error) {
	ctx, endAdmission := telemetry.StartSnowflakeManagedSpan(ctx, "admission", "", "", 0, 0)
	defer func() { endAdmission(resultErr) }()
	cfg, err := stagedConfigFromSpec(dsn, spec)
	if err != nil {
		return err
	}
	d.managedScopeMu.Lock()
	d.managedFlowIncarnation = ""
	d.managedScopeMu.Unlock()
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("open managed staged Snowflake: %w", err)
	}
	db.SetMaxOpenConns(cfg.maxOpenConnections)
	db.SetMaxIdleConns(cfg.maxOpenConnections)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	opened := false
	defer func() {
		if !opened {
			_ = db.Close()
			d.db = nil
		}
	}()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping managed staged Snowflake: %w", err)
	}
	d.db = db
	d.stagedConfig = cfg
	d.managedConfig = d.stagedSessionShim(cfg)
	conn, err := d.acquireManagedSnowflakeConn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	catalog, err := d.loadManagedStagedCatalog(ctx, conn)
	if err != nil {
		return err
	}
	if err := validateManagedStagedCatalog(cfg, catalog); err != nil {
		return err
	}
	fingerprint, err := managedStagedCatalogFingerprint(catalog)
	if err != nil {
		return err
	}
	d.stagedCatalogFingerprint = fingerprint
	var foreignReceipts int
	// #nosec G202 -- all identifiers passed through strict unquoted-uppercase validation.
	if err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+managedSnowflakeStagedQualifiedTable(cfg, cfg.receiptsTable)+" WHERE \"PROFILE_VERSION\" <> ? OR \"FLOW_ID\" <> ? OR \"DESTINATION_REVISION_ID\" <> ? OR \"SCHEMA_CONTRACT_HASH\" <> ?",
		cfg.profile, cfg.flowID, cfg.destinationRevision, cfg.schemaContractHash,
	).Scan(&foreignReceipts); err != nil {
		return fmt.Errorf("validate managed staged Snowflake receipt ownership rows: %w", err)
	}
	if foreignReceipts != 0 {
		return fmt.Errorf("managed staged Snowflake receipt table contains %d rows owned by another profile, flow, destination revision, or schema contract", foreignReceipts)
	}
	var receiptRows int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+managedSnowflakeStagedQualifiedTable(cfg, cfg.receiptsTable)).Scan(&receiptRows); err != nil {
		return fmt.Errorf("count managed staged Snowflake receipts: %w", err)
	}
	var targetHasRows bool
	if err := conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+managedSnowflakeStagedQualifiedTable(cfg, cfg.table)+" LIMIT 1)").Scan(&targetHasRows); err != nil {
		return fmt.Errorf("inspect managed staged Snowflake clean-start target: %w", err)
	}
	if err := validateManagedStagedCleanStartState(receiptRows, targetHasRows); err != nil {
		return err
	}
	opened = true
	return nil
}

func validateManagedStagedCleanStartState(receiptRows int, targetHasRows bool) error {
	if receiptRows < 0 {
		return errors.New("managed staged Snowflake receipt count cannot be negative")
	}
	if receiptRows == 0 && targetHasRows {
		return errors.New("managed staged Snowflake clean start requires an empty append target when no managed receipts exist")
	}
	return nil
}

func (d *Destination) newStagedDriver() *stagedDriver {
	return newStagedDriver(newSQLStageProtocol(d.db), d.stagedConfig, d.stagedCatalogFingerprint, d.stagedHooksSnapshot())
}

type preparedManagedStagedTransaction struct {
	destination *Destination
	intent      connector.DeliveryIntent
	transaction connector.SourceTransaction
	plan        managedStagedPlan
}

func (d *Destination) prepareManagedStaged(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.PreparedManagedTransaction, error) {
	if d.db == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeStagedAppendV1 {
		return nil, errors.New("managed staged Snowflake destination not initialized")
	}
	plan, err := planManagedStagedTransaction(d.stagedConfig, intent, transaction)
	if err != nil {
		return nil, err
	}
	if err := d.validateStagedSnowflakeReceiptScope(ctx, intent); err != nil {
		return nil, err
	}
	plan.catalogFingerprint = d.stagedCatalogFingerprint
	plan.receipt.catalogFingerprint = d.stagedCatalogFingerprint
	return &preparedManagedStagedTransaction{destination: d, intent: intent, transaction: transaction, plan: plan}, nil
}

func (p *preparedManagedStagedTransaction) Apply(ctx context.Context) (connector.DeliveryEvidence, error) {
	driver := p.destination.newStagedDriver()
	return driver.apply(ctx, p.intent, p.transaction)
}

func (d *Destination) reconcileManagedStaged(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if d.db == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeStagedAppendV1 {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, errors.New("managed staged Snowflake destination not initialized")
	}
	if err := d.validateStagedSnowflakeReceiptScope(ctx, intent); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	return d.newStagedDriver().reconcile(ctx, intent)
}

// CleanupManagedStaged runs one bounded stage-object retention pass for the
// bound flow incarnation. The runner schedules it after acknowledged deliveries;
// it never removes an object whose delivery was not durably recorded.
func (d *Destination) CleanupManagedStaged(ctx context.Context, flowIncarnationID string) (int, error) {
	if d.db == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeStagedAppendV1 {
		return 0, errors.New("managed staged Snowflake destination not initialized")
	}
	if err := d.validateStagedSnowflakeReceiptScope(ctx, connector.DeliveryIntent{FlowIncarnationID: flowIncarnationID, LogicalBatchID: "scope-validation"}); err != nil {
		return 0, err
	}
	return d.newStagedDriver().cleanup(ctx, flowIncarnationID)
}

func (d *Destination) validateStagedSnowflakeReceiptScope(ctx context.Context, intent connector.DeliveryIntent) error {
	flowIncarnationID := strings.TrimSpace(intent.FlowIncarnationID)
	if flowIncarnationID == "" {
		return errors.New("managed staged Snowflake flow incarnation is required")
	}
	d.managedScopeMu.Lock()
	defer d.managedScopeMu.Unlock()
	if d.managedFlowIncarnation != "" {
		if d.managedFlowIncarnation != flowIncarnationID {
			return fmt.Errorf("%w: managed staged Snowflake destination is already bound to flow incarnation %s", connector.ErrDeliveryConflict, d.managedFlowIncarnation)
		}
		return nil
	}
	conn, err := d.acquireManagedSnowflakeConn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	var foreign int
	query := "SELECT COUNT(*) FROM " + managedSnowflakeStagedQualifiedTable(d.stagedConfig, d.stagedConfig.receiptsTable) +
		" WHERE \"FLOW_ID\" <> ? OR \"FLOW_INCARNATION_ID\" <> ?"
	if err := conn.QueryRowContext(ctx, query, d.stagedConfig.flowID, flowIncarnationID).Scan(&foreign); err != nil {
		return fmt.Errorf("validate managed staged Snowflake receipt flow scope: %w", err)
	}
	if foreign != 0 {
		return fmt.Errorf("%w: managed staged Snowflake receipt table contains %d rows for another flow incarnation", connector.ErrDeliveryConflict, foreign)
	}
	d.managedFlowIncarnation = flowIncarnationID
	return nil
}

func (d *Destination) capabilitiesForStaged(capabilities connector.Capabilities) connector.Capabilities {
	capabilities.Delivery.TransactionalBatch = false
	capabilities.Delivery.IdempotentReplay = true
	capabilities.Delivery.ReplaySafe = true
	capabilities.Delivery.ExecutesDDL = false
	return capabilities
}
