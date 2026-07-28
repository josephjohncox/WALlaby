package telemetry

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type durableMetricSet struct {
	once                   sync.Once
	fenceRejections        metric.Int64Counter
	leaseTakeovers         metric.Int64Counter
	deliveryOutcomes       metric.Int64Counter
	bootstrapEvents        metric.Int64Counter
	bootstrapRows          metric.Int64Counter
	bootstrapBatches       metric.Int64Counter
	bootstrapClaimRenewals metric.Int64Counter
	bootstrapPhaseDuration metric.Float64Histogram
	bootstrapExporterAge   metric.Float64Histogram
	artifactTransitions    metric.Int64Counter
	artifactBytes          metric.Int64Histogram
	consumerOutcomes       metric.Int64Counter
	gcOutcomes             metric.Int64Counter
	clickHouseOutcomes     metric.Int64Counter
	clickHouseRows         metric.Int64Histogram
	clickHouseBytes        metric.Int64Histogram
	clickHouseLatency      metric.Float64Histogram
	snowflakeOutcomes      metric.Int64Counter
	snowflakeRows          metric.Int64Histogram
	snowflakeBytes         metric.Int64Histogram
	snowflakeLatency       metric.Float64Histogram
	initErr                error
}

var durableMetrics = &durableMetricSet{}

func initDurableMetrics() bool {
	durableMetrics.once.Do(func() {
		meter := otel.Meter("wallaby/durable")
		errs := make([]error, 0, 8)
		var err error
		durableMetrics.fenceRejections, err = meter.Int64Counter("wallaby.fence.rejections")
		errs = append(errs, err)
		durableMetrics.leaseTakeovers, err = meter.Int64Counter("wallaby.lease.takeovers")
		errs = append(errs, err)
		durableMetrics.deliveryOutcomes, err = meter.Int64Counter("wallaby.delivery.outcomes")
		errs = append(errs, err)
		durableMetrics.bootstrapEvents, err = meter.Int64Counter("wallaby.bootstrap.events")
		errs = append(errs, err)
		durableMetrics.bootstrapRows, err = meter.Int64Counter("wallaby.bootstrap.rows")
		errs = append(errs, err)
		durableMetrics.bootstrapBatches, err = meter.Int64Counter("wallaby.bootstrap.batches")
		errs = append(errs, err)
		durableMetrics.bootstrapClaimRenewals, err = meter.Int64Counter("wallaby.bootstrap.claim.renewals")
		errs = append(errs, err)
		durableMetrics.bootstrapPhaseDuration, err = meter.Float64Histogram("wallaby.bootstrap.phase.duration", metric.WithUnit("s"))
		errs = append(errs, err)
		durableMetrics.bootstrapExporterAge, err = meter.Float64Histogram("wallaby.bootstrap.exporter.age", metric.WithUnit("s"))
		errs = append(errs, err)
		durableMetrics.artifactTransitions, err = meter.Int64Counter("wallaby.artifact.transitions")
		errs = append(errs, err)
		durableMetrics.artifactBytes, err = meter.Int64Histogram("wallaby.artifact.bytes")
		errs = append(errs, err)
		durableMetrics.consumerOutcomes, err = meter.Int64Counter("wallaby.artifact.consumer.outcomes")
		errs = append(errs, err)
		durableMetrics.gcOutcomes, err = meter.Int64Counter("wallaby.artifact.gc.outcomes")
		errs = append(errs, err)
		durableMetrics.clickHouseOutcomes, err = meter.Int64Counter("wallaby.clickhouse.managed.outcomes")
		errs = append(errs, err)
		durableMetrics.clickHouseRows, err = meter.Int64Histogram("wallaby.clickhouse.managed.rows")
		errs = append(errs, err)
		durableMetrics.clickHouseBytes, err = meter.Int64Histogram("wallaby.clickhouse.managed.bytes", metric.WithUnit("By"))
		errs = append(errs, err)
		durableMetrics.clickHouseLatency, err = meter.Float64Histogram("wallaby.clickhouse.managed.duration", metric.WithUnit("s"))
		errs = append(errs, err)
		durableMetrics.snowflakeOutcomes, err = meter.Int64Counter("wallaby.snowflake.managed.outcomes")
		errs = append(errs, err)
		durableMetrics.snowflakeRows, err = meter.Int64Histogram("wallaby.snowflake.managed.rows")
		errs = append(errs, err)
		durableMetrics.snowflakeBytes, err = meter.Int64Histogram("wallaby.snowflake.managed.bytes", metric.WithUnit("By"))
		errs = append(errs, err)
		durableMetrics.snowflakeLatency, err = meter.Float64Histogram("wallaby.snowflake.managed.duration", metric.WithUnit("s"))
		errs = append(errs, err)
		durableMetrics.initErr = errors.Join(errs...)
	})
	return durableMetrics.initErr == nil
}

func RecordFenceRejection(ctx context.Context, _ string) {
	if !initDurableMetrics() {
		return
	}
	durableMetrics.fenceRejections.Add(ctx, 1)
}

func RecordLeaseTakeover(ctx context.Context, _ string) {
	if !initDurableMetrics() {
		return
	}
	durableMetrics.leaseTakeovers.Add(ctx, 1)
}

func RecordDeliveryOutcome(ctx context.Context, outcome string) {
	if !initDurableMetrics() {
		return
	}
	durableMetrics.deliveryOutcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", boundedDeliveryOutcome(outcome))))
}

func boundedDeliveryOutcome(outcome string) string {
	switch outcome {
	case "attempt_prepared", "receipt_committed", "receipt_reused", "indeterminate", "apply_failed":
		return outcome
	default:
		return "other"
	}
}

func RecordBootstrapEvent(ctx context.Context, event string) {
	if !initDurableMetrics() {
		return
	}
	event = boundedBootstrapLabel(event, map[string]struct{}{
		"snapshot_exported": {}, "handoff_committed": {}, "generation_abandoned": {},
		"generation_started": {}, "generation_restarted": {}, "exporter_lost": {}, "cleanup": {},
	})
	durableMetrics.bootstrapEvents.Add(ctx, 1, metric.WithAttributes(attribute.String("event", event)))
}

func RecordBootstrapProgress(ctx context.Context, rows int) {
	if !initDurableMetrics() {
		return
	}
	durableMetrics.bootstrapBatches.Add(ctx, 1)
	if rows > 0 {
		durableMetrics.bootstrapRows.Add(ctx, int64(rows))
	}
}

func RecordBootstrapClaimRenewal(ctx context.Context, outcome string) {
	if !initDurableMetrics() {
		return
	}
	outcome = boundedBootstrapLabel(outcome, map[string]struct{}{"success": {}, "failure": {}})
	durableMetrics.bootstrapClaimRenewals.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

func RecordBootstrapExporterAge(ctx context.Context, age time.Duration, outcome string) {
	if !initDurableMetrics() {
		return
	}
	outcome = boundedBootstrapLabel(outcome, map[string]struct{}{"closed": {}, "lost": {}})
	durableMetrics.bootstrapExporterAge.Record(ctx, age.Seconds(), metric.WithAttributes(attribute.String("outcome", outcome)))
}

func StartBootstrapSpan(ctx context.Context, operation, flowID, bootstrapID, taskID string, generation int64) (context.Context, func(error)) {
	operation = boundedBootstrapLabel(operation, map[string]struct{}{
		"generation": {}, "task": {}, "publication": {}, "handoff": {}, "cleanup": {}, "recovery": {},
	})
	started := time.Now()
	ctx, span := otel.Tracer("wallaby/bootstrap").Start(ctx, "bootstrap."+operation, trace.WithAttributes(
		attribute.String("flow.id", flowID),
		attribute.String("bootstrap.id", bootstrapID),
		attribute.String("bootstrap.task.id", taskID),
		attribute.Int64("bootstrap.generation", generation),
	))
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		if initDurableMetrics() {
			durableMetrics.bootstrapPhaseDuration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(attribute.String("phase", operation)))
		}
	}
}

func boundedBootstrapLabel(value string, allowed map[string]struct{}) string {
	if _, ok := allowed[value]; ok {
		return value
	}
	return "other"
}

func RecordArtifactTransition(ctx context.Context, state string, bytes int64) {
	if !initDurableMetrics() {
		return
	}
	attrs := metric.WithAttributes(attribute.String("state", state))
	durableMetrics.artifactTransitions.Add(ctx, 1, attrs)
	if bytes > 0 {
		durableMetrics.artifactBytes.Record(ctx, bytes, attrs)
	}
}

func RecordArtifactConsumerOutcome(ctx context.Context, outcome string) {
	if !initDurableMetrics() {
		return
	}
	durableMetrics.consumerOutcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

func RecordArtifactGCOutcome(ctx context.Context, outcome string) {
	if !initDurableMetrics() {
		return
	}
	durableMetrics.gcOutcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// StartClickHouseManagedSpan correlates one native ClickHouse query with its
// immutable logical delivery. Query and batch identities are trace attributes,
// never metric labels.
func StartClickHouseManagedSpan(ctx context.Context, operation, queryID, logicalBatchID string, rows, bytes int64) (context.Context, func(error)) {
	operation = boundedClickHouseOperation(operation)
	started := time.Now()
	ctx, span := otel.Tracer("wallaby/clickhouse").Start(ctx, "clickhouse.managed."+operation, trace.WithAttributes(
		attribute.String("db.system", "clickhouse"),
		attribute.String("db.operation.name", "INSERT"),
		attribute.String("clickhouse.query.id", queryID),
		attribute.String("wallaby.logical_batch.id", logicalBatchID),
		attribute.Int64("db.operation.batch.size", rows),
	))
	return ctx, func(err error) {
		outcome := "success"
		if err != nil {
			outcome = "failure"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		if !initDurableMetrics() {
			return
		}
		attrs := metric.WithAttributes(attribute.String("operation", operation), attribute.String("outcome", outcome))
		durableMetrics.clickHouseOutcomes.Add(ctx, 1, attrs)
		if rows > 0 {
			durableMetrics.clickHouseRows.Record(ctx, rows, metric.WithAttributes(attribute.String("operation", operation)))
		}
		if bytes > 0 {
			durableMetrics.clickHouseBytes.Record(ctx, bytes, metric.WithAttributes(attribute.String("operation", operation)))
		}
		durableMetrics.clickHouseLatency.Record(ctx, time.Since(started).Seconds(), attrs)
	}
}

// StartSnowflakeManagedSpan records bounded operation/outcome metrics while
// retaining query and logical-batch identities only as trace correlation data.
func StartSnowflakeManagedSpan(ctx context.Context, operation, operationID, logicalBatchID string, rows, bytes int64) (context.Context, func(error)) {
	operation = boundedSnowflakeOperation(operation)
	started := time.Now()
	ctx, span := otel.Tracer("wallaby/snowflake").Start(ctx, "snowflake.managed."+operation, trace.WithAttributes(
		attribute.String("db.system", "snowflake"),
		attribute.String("db.operation.name", strings.ToUpper(operation)),
		attribute.String("wallaby.snowflake.operation.id", operationID),
		attribute.String("wallaby.logical_batch.id", logicalBatchID),
		attribute.Int64("db.operation.batch.size", rows),
	))
	return ctx, func(err error) {
		outcome := "success"
		if err != nil {
			outcome = "failure"
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		if !initDurableMetrics() {
			return
		}
		attrs := metric.WithAttributes(attribute.String("operation", operation), attribute.String("outcome", outcome))
		durableMetrics.snowflakeOutcomes.Add(ctx, 1, attrs)
		if rows > 0 {
			durableMetrics.snowflakeRows.Record(ctx, rows, metric.WithAttributes(attribute.String("operation", operation)))
		}
		if bytes > 0 {
			durableMetrics.snowflakeBytes.Record(ctx, bytes, metric.WithAttributes(attribute.String("operation", operation)))
		}
		durableMetrics.snowflakeLatency.Record(ctx, time.Since(started).Seconds(), attrs)
	}
}

// RecordSnowflakeQueryID adds the driver's server-issued query identifier to
// the current trace only. Query IDs must never become metric attributes.
func RecordSnowflakeQueryID(ctx context.Context, queryID string) {
	if queryID == "" {
		return
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("snowflake.query.id", queryID))
}

func boundedSnowflakeOperation(operation string) string {
	switch operation {
	case "dml", "receipt", "reconcile", "admission", "transaction",
		"stage", "stage_put", "copy", "verify", "cleanup":
		return operation
	default:
		return "other"
	}
}

func boundedClickHouseOperation(operation string) string {
	switch operation {
	case "fragment", "receipt", "reconcile", "admission":
		return operation
	default:
		return "other"
	}
}
