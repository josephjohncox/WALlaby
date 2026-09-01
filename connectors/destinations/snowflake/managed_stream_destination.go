package snowflake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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

type streamRuntimeSnapshot struct {
	db                 *sql.DB
	protocol           streamProtocol
	cfg                streamConfig
	catalogFingerprint string
	policy             connector.SnowflakeStreamingRESTPolicy
}

// lockStreamRuntime returns one atomic runtime view and holds a read lock until
// the caller completes. Close takes the write lock and therefore cannot close
// the SQL pool or revoke the runtime while an operation uses this snapshot.
func (d *Destination) lockStreamRuntime() (streamRuntimeSnapshot, func(), error) {
	d.streamRuntimeMu.RLock()
	snapshot := streamRuntimeSnapshot{
		db: d.db, protocol: d.streamRuntimeProtocol, cfg: d.streamConfig,
		catalogFingerprint: d.streamCatalogFingerprint, policy: d.streamingPolicy,
	}
	if !ManagedStreamingTransportAvailable() || snapshot.db == nil || snapshot.protocol == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 || !snapshot.policy.Enabled() {
		d.streamRuntimeMu.RUnlock()
		return streamRuntimeSnapshot{}, nil, streamingNotInitializedError()
	}
	if err := snapshot.policy.Admit(d.spec); err != nil {
		d.streamRuntimeMu.RUnlock()
		return streamRuntimeSnapshot{}, nil, err
	}
	return snapshot, d.streamRuntimeMu.RUnlock, nil
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
		d.streamRuntimeMu.Lock()
		d.db = nil
		d.streamRuntimeProtocol = nil
		d.streamCatalogFingerprint = ""
		d.streamingPolicy = connector.SnowflakeStreamingRESTPolicy{}
		d.streamRuntimeMu.Unlock()
	}()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping managed streaming Snowflake: %w", err)
	}
	d.streamRuntimeMu.Lock()
	d.db = db
	d.streamConfig = cfg
	d.managedConfig = d.streamSessionShim(cfg)
	d.streamRuntimeMu.Unlock()
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
	d.streamRuntimeMu.Lock()
	d.streamRuntimeProtocol = protocol
	d.streamCatalogFingerprint = fingerprint
	d.streamingPolicy = streamingPolicy
	d.streamRuntimeMu.Unlock()
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
	catalogFingerprint, err := validateManagedStreamCatalog(ctx, conn, cfg)
	if err != nil {
		return nil, "", err
	}
	fingerprint, err := managedStreamRuntimeFingerprint(cfg, catalogFingerprint, policy)
	if err != nil {
		return nil, "", err
	}
	transport, err := newDeploymentStreamRESTTransport(policy, http.DefaultClient, time.Now, 55*time.Minute)
	if err != nil {
		return nil, "", err
	}
	return &composedStreamProtocol{streamTransport: transport, streamStateStore: newSQLStreamProtocol(db)}, fingerprint, nil
}

func managedStreamRuntimeFingerprint(cfg streamConfig, catalogFingerprint string, policy connector.SnowflakeStreamingRESTPolicy) (string, error) {
	policyFingerprint, err := policy.Fingerprint()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{cfg.configDigest, catalogFingerprint, policyFingerprint}, "\x00")))
	return hex.EncodeToString(digest[:]), nil
}

func (d *Destination) validateStreamRuntime(ctx context.Context, snapshot streamRuntimeSnapshot) error {
	conn, err := snapshot.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire managed streaming Snowflake validation session: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if err := d.validateManagedSnowflakeTransactions(ctx, conn); err != nil {
		return err
	}
	catalogFingerprint, err := validateManagedStreamCatalog(ctx, conn, snapshot.cfg)
	if err != nil {
		return err
	}
	fingerprint, err := managedStreamRuntimeFingerprint(snapshot.cfg, catalogFingerprint, snapshot.policy)
	if err != nil {
		return err
	}
	if fingerprint != snapshot.catalogFingerprint {
		return fmt.Errorf("%w: managed streaming Snowflake runtime or catalog fingerprint changed", connector.ErrDeliveryConflict)
	}
	return nil
}

func (d *Destination) newStreamDriver(snapshot streamRuntimeSnapshot) *streamDriver {
	return newStreamDriver(snapshot.protocol, snapshot.cfg, snapshot.catalogFingerprint, d.streamHooksSnapshot())
}

type preparedManagedStreamTransaction struct {
	destination *Destination
	intent      connector.DeliveryIntent
	transaction connector.SourceTransaction
	plan        managedStreamPlan
}

func (d *Destination) prepareManagedStreaming(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.PreparedManagedTransaction, error) {
	snapshot, unlock, err := d.lockStreamRuntime()
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := d.validateStreamRuntime(ctx, snapshot); err != nil {
		return nil, err
	}
	plan, err := planManagedStreamTransaction(snapshot.cfg, intent, transaction)
	if err != nil {
		return nil, err
	}
	if err := d.validateStreamingSnowflakeReceiptScope(ctx, intent); err != nil {
		return nil, err
	}
	plan.catalogFingerprint = snapshot.catalogFingerprint
	plan.receipt.catalogFingerprint = snapshot.catalogFingerprint
	return &preparedManagedStreamTransaction{destination: d, intent: intent, transaction: transaction, plan: plan}, nil
}

func (p *preparedManagedStreamTransaction) Apply(ctx context.Context) (connector.DeliveryEvidence, error) {
	snapshot, unlock, err := p.destination.lockStreamRuntime()
	if err != nil {
		return connector.DeliveryEvidence{}, err
	}
	defer unlock()
	if err := p.destination.validateStreamRuntime(ctx, snapshot); err != nil {
		return connector.DeliveryEvidence{}, err
	}
	return p.destination.newStreamDriver(snapshot).apply(ctx, p.intent, p.transaction)
}

func (d *Destination) reconcileManagedStreaming(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	snapshot, unlock, err := d.lockStreamRuntime()
	if err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	defer unlock()
	if err := d.validateStreamRuntime(ctx, snapshot); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	if err := d.validateStreamingSnowflakeReceiptScope(ctx, intent); err != nil {
		return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, err
	}
	return d.newStreamDriver(snapshot).reconcile(ctx, intent)
}

// CleanupManagedStreaming runs one bounded channel-state retention pass for the
// bound flow incarnation. The runner schedules it after acknowledged deliveries;
// it never releases a batch whose delivery was not durably recorded.
func (d *Destination) CleanupManagedStreaming(ctx context.Context, flowIncarnationID string) (int, error) {
	snapshot, unlock, err := d.lockStreamRuntime()
	if err != nil {
		return 0, err
	}
	defer unlock()
	if err := d.validateStreamRuntime(ctx, snapshot); err != nil {
		return 0, err
	}
	if err := d.validateStreamingSnowflakeReceiptScope(ctx, connector.DeliveryIntent{FlowIncarnationID: flowIncarnationID, LogicalBatchID: "scope-validation"}); err != nil {
		return 0, err
	}
	return d.newStreamDriver(snapshot).cleanup(ctx, flowIncarnationID)
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
