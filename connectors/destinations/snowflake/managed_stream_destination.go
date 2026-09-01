package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func (d *Destination) streamHooksSnapshot() streamingHooks {
	d.streamHooksMu.RLock()
	defer d.streamHooksMu.RUnlock()
	return d.streamHooks
}

func (d *Destination) streamSessionShim(cfg streamConfig) managedConfig {
	return managedConfig{
		profile: cfg.profile, flowID: cfg.flowID, account: cfg.account, database: cfg.database, schema: cfg.schema,
		ownerRole: cfg.ownerRole, executionRole: cfg.executionRole, warehouse: cfg.warehouse,
		snowflakeVersion: cfg.snowflakeVersion, destinationRevision: cfg.destinationRevision,
		statementTimeoutSeconds: cfg.statementTimeoutSeconds, hybridLockTimeoutSeconds: cfg.statementTimeoutSeconds,
		validateEveryConnection: true,
	}
}

// openManagedStreaming assembles the experimental REST transport only when the
// build tag and the deployment-owned Streaming capability are both present.
// The default build returns before SQL, HTTP, or JWT work.
func (d *Destination) openManagedStreaming(ctx context.Context, dsn string, spec connector.RuntimeSpec, factories destinationFactories) (resultErr error) {
	_, endAdmission := telemetry.StartSnowflakeManagedSpan(ctx, "admission", "", "", 0, 0)
	defer func() { endAdmission(resultErr) }()
	cfg, err := streamConfigFromSpec(dsn, spec)
	if err != nil {
		return err
	}
	if !ManagedStreamingTransportAvailable() {
		return fmt.Errorf("managed streaming Snowflake profile %s: %w", cfg.profile, ErrManagedStreamingTransportUnavailable)
	}
	streamingPolicy, err := d.deploymentPolicy.StreamingRESTPolicy()
	if err != nil {
		return err
	}
	if err := streamingPolicy.Admit(spec); err != nil {
		return err
	}
	if factories.openDB == nil {
		return errors.New("managed streaming Snowflake database factory is unavailable")
	}
	db, err := factories.openDB("snowflake", dsn)
	if err != nil {
		if db != nil {
			return errors.Join(fmt.Errorf("open managed streaming Snowflake: %w", err), db.Close())
		}
		return fmt.Errorf("open managed streaming Snowflake: %w", err)
	}
	db.SetMaxOpenConns(cfg.maxOpenConnections)
	db.SetMaxIdleConns(cfg.maxOpenConnections)
	db.SetConnMaxIdleTime(5 * time.Minute)
	db.SetConnMaxLifetime(30 * time.Minute)
	opened := false
	defer func() {
		if opened {
			return
		}
		resultErr = errors.Join(resultErr, db.Close())
		d.db = nil
		d.streamRuntimeProtocol = nil
		d.streamingPolicy = connector.SnowflakeStreamingRESTPolicy{}
	}()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping managed streaming Snowflake: %w", err)
	}
	d.db = db
	d.streamConfig = cfg
	d.managedConfig = d.streamSessionShim(cfg)
	openRuntime := factories.openStreamRuntime
	if openRuntime == nil {
		openRuntime = d.openManagedStreamRuntime
	}
	protocol, fingerprint, err := openRuntime(ctx, db, cfg, streamingPolicy)
	if err != nil {
		return err
	}
	if protocol == nil || strings.TrimSpace(fingerprint) == "" {
		return errors.New("managed streaming Snowflake runtime assembly is incomplete")
	}
	d.streamRuntimeProtocol = protocol
	d.streamCatalogFingerprint = fingerprint
	d.streamingPolicy = streamingPolicy
	d.managedScopeMu.Lock()
	d.managedFlowIncarnation = ""
	d.managedScopeMu.Unlock()
	opened = true
	return nil
}

func (d *Destination) openManagedStreamRuntime(ctx context.Context, db *sql.DB, cfg streamConfig, policy connector.SnowflakeStreamingRESTPolicy) (streamProtocol, string, error) {
	conn, err := d.acquireManagedSnowflakeConn(ctx)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = conn.Close() }()
	fingerprint, err := validateManagedStreamCatalog(ctx, conn, cfg)
	if err != nil {
		return nil, "", err
	}
	transport, err := newDeploymentStreamRESTTransport(policy, http.DefaultClient, time.Now, 55*time.Minute)
	if err != nil {
		return nil, "", err
	}
	return &composedStreamProtocol{streamTransport: transport, streamStateStore: newSQLStreamProtocol(db)}, fingerprint, nil
}

func (d *Destination) requireStreamingCapability() error {
	if !ManagedStreamingTransportAvailable() {
		return ErrManagedStreamingTransportUnavailable
	}
	if !d.streamingPolicy.Enabled() {
		return connector.ErrSnowflakeStreamingRESTDisabled
	}
	return d.streamingPolicy.Admit(d.spec)
}

func (d *Destination) newStreamDriver() *streamDriver {
	protocol := d.streamRuntimeProtocol
	if protocol == nil {
		protocol = newSQLStreamProtocol(d.db)
	}
	return newStreamDriver(protocol, d.streamConfig, d.streamCatalogFingerprint, d.streamHooksSnapshot())
}

type preparedManagedStreamTransaction struct {
	destination *Destination
	intent      connector.DeliveryIntent
	transaction connector.SourceTransaction
	plan        managedStreamPlan
}

func (d *Destination) prepareManagedStreaming(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.PreparedManagedTransaction, error) {
	if err := d.requireStreamingCapability(); err != nil {
		return nil, err
	}
	if d.db == nil || d.streamRuntimeProtocol == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 {
		return nil, streamingNotInitializedError()
	}
	plan, err := planManagedStreamTransaction(d.streamConfig, intent, transaction)
	if err != nil {
		return nil, err
	}
	if err := d.validateStreamingSnowflakeReceiptScope(ctx, intent); err != nil {
		return nil, err
	}
	plan.catalogFingerprint = d.streamCatalogFingerprint
	plan.receipt.catalogFingerprint = d.streamCatalogFingerprint
	return &preparedManagedStreamTransaction{destination: d, intent: intent, transaction: transaction, plan: plan}, nil
}

func (p *preparedManagedStreamTransaction) Apply(ctx context.Context) (connector.DeliveryEvidence, error) {
	if err := p.destination.requireStreamingCapability(); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	driver := p.destination.newStreamDriver()
	return driver.apply(ctx, p.intent, p.transaction)
}

func (d *Destination) reconcileManagedStreaming(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if err := d.requireStreamingCapability(); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if d.db == nil || d.streamRuntimeProtocol == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, streamingNotInitializedError()
	}
	if err := d.validateStreamingSnowflakeReceiptScope(ctx, intent); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	return d.newStreamDriver().reconcile(ctx, intent)
}

// CleanupManagedStreaming runs one bounded channel-state retention pass for the
// bound flow incarnation. The runner schedules it after acknowledged deliveries;
// it never releases a batch whose delivery was not durably recorded.
func (d *Destination) CleanupManagedStreaming(ctx context.Context, flowIncarnationID string) (int, error) {
	if err := d.requireStreamingCapability(); err != nil {
		return 0, err
	}
	if d.db == nil || d.streamRuntimeProtocol == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 {
		return 0, streamingNotInitializedError()
	}
	if err := d.validateStreamingSnowflakeReceiptScope(ctx, connector.DeliveryIntent{FlowIncarnationID: flowIncarnationID, LogicalBatchID: "scope-validation"}); err != nil {
		return 0, err
	}
	return d.newStreamDriver().cleanup(ctx, flowIncarnationID)
}

// streamingNotInitializedError explains why a managed streaming operation is
// unavailable: absent a reviewed high-performance append transport, admission
// fails closed and the destination is never initialized for delivery.
func streamingNotInitializedError() error {
	return fmt.Errorf("managed streaming Snowflake destination not initialized: %w", ErrManagedStreamingTransportUnavailable)
}

func (d *Destination) validateStreamingSnowflakeReceiptScope(ctx context.Context, intent connector.DeliveryIntent) error {
	flowIncarnationID := strings.TrimSpace(intent.FlowIncarnationID)
	if flowIncarnationID == "" {
		return errors.New("managed streaming Snowflake flow incarnation is required")
	}
	d.managedScopeMu.Lock()
	defer d.managedScopeMu.Unlock()
	if d.managedFlowIncarnation != "" {
		if d.managedFlowIncarnation != flowIncarnationID {
			return fmt.Errorf("%w: managed streaming Snowflake destination is already bound to flow incarnation %s", connector.ErrDeliveryConflict, d.managedFlowIncarnation)
		}
		return nil
	}
	conn, err := d.acquireManagedSnowflakeConn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	var foreign int
	query := "SELECT COUNT(*) FROM " + managedSnowflakeStreamQualifiedTable(d.streamConfig, d.streamConfig.receiptsTable) +
		" WHERE \"FLOW_ID\" <> ? OR \"FLOW_INCARNATION_ID\" <> ?"
	if err := conn.QueryRowContext(ctx, query, d.streamConfig.flowID, flowIncarnationID).Scan(&foreign); err != nil {
		return fmt.Errorf("validate managed streaming Snowflake receipt flow scope: %w", err)
	}
	if foreign != 0 {
		return fmt.Errorf("%w: managed streaming Snowflake receipt table contains %d rows for another flow incarnation", connector.ErrDeliveryConflict, foreign)
	}
	d.managedFlowIncarnation = flowIncarnationID
	return nil
}

func (d *Destination) capabilitiesForStreaming(capabilities connector.Capabilities) connector.Capabilities {
	capabilities.Delivery.TransactionalBatch = false
	capabilities.Delivery.IdempotentReplay = true
	capabilities.Delivery.ReplaySafe = true
	capabilities.Delivery.ExecutesDDL = false
	return capabilities
}
