package snowflake

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func (d *Destination) streamHooksSnapshot() streamingHooks {
	d.streamHooksMu.RLock()
	defer d.streamHooksMu.RUnlock()
	return d.streamHooks
}

// openManagedStreaming admits the Snowpipe Streaming REST append profile. It
// performs the complete side-effect-free spec validation and then fails closed:
// no reviewed high-performance append transport is linked into this build, so
// starting delivery would require fabricating completion from local continuation
// and offset tokens. Rather than that theater, admission is refused before any
// network side effect. The deterministic recovery protocol is proven separately
// against the in-memory protocol fake and is promotion evidence only once a
// reviewed transport is linked and its live matrix passes.
func (d *Destination) openManagedStreaming(ctx context.Context, dsn string, spec connector.Spec) (resultErr error) {
	_, endAdmission := telemetry.StartSnowflakeManagedSpan(ctx, "admission", "", "", 0, 0)
	defer func() { endAdmission(resultErr) }()
	cfg, err := streamConfigFromSpec(dsn, spec)
	if err != nil {
		return err
	}
	d.streamConfig = cfg
	if !ManagedStreamingTransportAvailable() {
		return fmt.Errorf("managed streaming Snowflake profile %s: %w", cfg.profile, ErrManagedStreamingTransportUnavailable)
	}
	// Unreachable until a reviewed transport is linked; retained so the future
	// promotion path opens the session and validates the catalog here.
	return ErrManagedStreamingTransportUnavailable
}

func (d *Destination) newStreamDriver() *streamDriver {
	return newStreamDriver(newSQLStreamProtocol(d.db), d.streamConfig, d.streamCatalogFingerprint, d.streamHooksSnapshot())
}

type preparedManagedStreamTransaction struct {
	destination *Destination
	intent      connector.DeliveryIntent
	transaction connector.SourceTransaction
	plan        managedStreamPlan
}

func (d *Destination) prepareManagedStreaming(ctx context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.PreparedManagedTransaction, error) {
	if d.db == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 {
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
	driver := p.destination.newStreamDriver()
	return driver.apply(ctx, p.intent, p.transaction)
}

func (d *Destination) reconcileManagedStreaming(ctx context.Context, intent connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	if d.db == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 {
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
	if d.db == nil || d.managedProfile != connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1 {
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
