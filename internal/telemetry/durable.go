package telemetry

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var durableMetrics struct {
	once                sync.Once
	fenceRejections     metric.Int64Counter
	leaseTakeovers      metric.Int64Counter
	deliveryOutcomes    metric.Int64Counter
	bootstrapEvents     metric.Int64Counter
	artifactTransitions metric.Int64Counter
	artifactBytes       metric.Int64Histogram
	consumerOutcomes    metric.Int64Counter
	gcOutcomes          metric.Int64Counter
	initErr             error
}

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
	durableMetrics.bootstrapEvents.Add(ctx, 1, metric.WithAttributes(attribute.String("event", event)))
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
