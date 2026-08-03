package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestBootstrapMetricLabelsAreBounded(t *testing.T) {
	allowed := map[string]struct{}{"success": {}, "failure": {}}
	if got := boundedBootstrapLabel("success", allowed); got != "success" {
		t.Fatalf("allowed label=%q", got)
	}
	if got := boundedBootstrapLabel("flow-123/bootstrap-456", allowed); got != "other" {
		t.Fatalf("unbounded identity leaked into metric label: %q", got)
	}
}

func TestPostgresManagedProfileMetrics(t *testing.T) {
	oldMeterProvider := otel.GetMeterProvider()
	oldMetrics := durableMetrics
	defer func() {
		otel.SetMeterProvider(oldMeterProvider)
		durableMetrics = oldMetrics
	}()

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(meterProvider)
	durableMetrics = &durableMetricSet{}
	ctx := context.Background()
	for _, outcome := range []string{"attempt_prepared", "receipt_committed", "receipt_reused", "indeterminate", "apply_failed", "flow-specific-unbounded-value"} {
		RecordDeliveryOutcome(ctx, outcome)
	}
	RecordFenceRejection(ctx, "unbounded-flow-identity")
	RecordLeaseTakeover(ctx, "another-unbounded-flow-identity")

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &metrics); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			sum, ok := measurement.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				if measurement.Name == "wallaby.delivery.outcomes" {
					if value, ok := point.Attributes.Value(attribute.Key("outcome")); ok {
						got[value.AsString()] = true
					}
				}
				if measurement.Name == "wallaby.fence.rejections" || measurement.Name == "wallaby.lease.takeovers" {
					if value, ok := point.Attributes.Value(attribute.Key("flow.id")); ok {
						t.Fatalf("unbounded flow.id=%q leaked into %s", value.AsString(), measurement.Name)
					}
				}
			}
		}
	}
	for _, outcome := range []string{"attempt_prepared", "receipt_committed", "receipt_reused", "indeterminate", "apply_failed", "other"} {
		if !got[outcome] {
			t.Fatalf("missing bounded managed delivery metric %q: %v", outcome, got)
		}
	}
	if got["flow-specific-unbounded-value"] {
		t.Fatalf("unbounded delivery outcome leaked into metric labels: %v", got)
	}
}

func TestBootstrapTelemetrySDKRecordsRequiredBoundedSignals(t *testing.T) {
	oldTracerProvider := otel.GetTracerProvider()
	oldMeterProvider := otel.GetMeterProvider()
	oldMetrics := durableMetrics
	defer func() {
		otel.SetTracerProvider(oldTracerProvider)
		otel.SetMeterProvider(oldMeterProvider)
		durableMetrics = oldMetrics
	}()

	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	otel.SetTracerProvider(tracerProvider)
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(meterProvider)
	durableMetrics = &durableMetricSet{}

	ctx := context.Background()
	for _, operation := range []string{"generation", "task", "recovery", "publication", "handoff", "cleanup"} {
		_, end := StartBootstrapSpan(ctx, operation, "flow-identity", "bootstrap-identity", "task-identity", 7)
		end(nil)
	}
	RecordBootstrapClaimRenewal(ctx, "success")
	RecordBootstrapEvent(ctx, "generation_restarted")
	RecordBootstrapEvent(ctx, "cleanup")

	spans := map[string]bool{}
	for _, span := range spanRecorder.Ended() {
		spans[span.Name()] = true
	}
	for _, name := range []string{"bootstrap.generation", "bootstrap.task", "bootstrap.recovery", "bootstrap.publication", "bootstrap.handoff", "bootstrap.cleanup"} {
		if !spans[name] {
			t.Fatalf("missing SDK-recorded span %q; got %v", name, spans)
		}
	}

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &metrics); err != nil {
		t.Fatal(err)
	}
	labels := map[string]map[string]bool{}
	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			sum, ok := measurement.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				for _, key := range []attribute.Key{"event", "outcome"} {
					if value, ok := point.Attributes.Value(key); ok {
						if labels[measurement.Name] == nil {
							labels[measurement.Name] = map[string]bool{}
						}
						labels[measurement.Name][value.AsString()] = true
					}
				}
			}
		}
	}
	if !labels["wallaby.bootstrap.claim.renewals"]["success"] {
		t.Fatalf("missing bounded claim-renewal metric: %v", labels)
	}
	if !labels["wallaby.bootstrap.events"]["generation_restarted"] || !labels["wallaby.bootstrap.events"]["cleanup"] {
		t.Fatalf("missing bounded restart/cleanup metrics: %v", labels)
	}
}
