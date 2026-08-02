package telemetry

import (
	"context"
	"errors"
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
	initErr                error
}

var durableMetrics durableMetricSet

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
		durableMetrics.initErr = errors.Join(errs...)
	})
	return durableMetrics.initErr == nil
}

func RecordFenceRejection(ctx context.Context, flowID string) {
	if !initDurableMetrics() {
		return
	}
	durableMetrics.fenceRejections.Add(ctx, 1, metric.WithAttributes(attribute.String("flow.id", flowID)))
}

func RecordLeaseTakeover(ctx context.Context, flowID string) {
	if !initDurableMetrics() {
		return
	}
	durableMetrics.leaseTakeovers.Add(ctx, 1, metric.WithAttributes(attribute.String("flow.id", flowID)))
}

func RecordDeliveryOutcome(ctx context.Context, outcome string) {
	if !initDurableMetrics() {
		return
	}
	durableMetrics.deliveryOutcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
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
