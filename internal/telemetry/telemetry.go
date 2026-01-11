package telemetry

import (
	"context"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Provider manages OpenTelemetry providers and exporters.
type Provider struct {
	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
	meters         *Meters
}

// Meters holds all application metric instruments.
type Meters struct {
	// Performance metrics
	RecordsProcessed        metric.Int64Counter
	BatchesProcessed        metric.Int64Counter
	BatchLatency            metric.Float64Histogram
	DestinationWriteLatency metric.Float64Histogram
	RecordsPerBatch         metric.Int64Histogram

	// Operations metrics
	ErrorsTotal       metric.Int64Counter
	CheckpointCommits metric.Int64Counter
}

// NewProvider creates a new telemetry provider with metrics export.
func NewProvider(ctx context.Context, cfg config.TelemetryConfig) (*Provider, error) {
	if cfg.OTLPEndpoint == "" || cfg.MetricsExporter == "none" {
		return &Provider{meters: newNoopMeters()}, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, err
	}

	var exporter sdkmetric.Exporter
	protocol := strings.ToLower(cfg.OTLPProtocol)
	// Strip http:// or https:// scheme for gRPC endpoint
	endpoint := cfg.OTLPEndpoint
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	if protocol == "http/protobuf" || protocol == "http" {
		httpOpts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(endpoint),
		}
		if cfg.OTLPInsecure {
			httpOpts = append(httpOpts, otlpmetrichttp.WithInsecure())
		}
		exporter, err = otlpmetrichttp.New(ctx, httpOpts...)
	} else {
		// Default to gRPC
		grpcOpts := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithEndpoint(endpoint),
		}
		if cfg.OTLPInsecure {
			grpcOpts = append(grpcOpts, otlpmetricgrpc.WithInsecure())
		}
		exporter, err = otlpmetricgrpc.New(ctx, grpcOpts...)
	}
	if err != nil {
		return nil, err
	}

	interval := cfg.MetricsInterval
	if interval == 0 {
		interval = 30 * time.Second
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(interval)),
		),
	)

	otel.SetMeterProvider(meterProvider)

	meters, err := createMeters(meterProvider.Meter(cfg.ServiceName))
	if err != nil {
		_ = meterProvider.Shutdown(ctx)
		return nil, err
	}

	// Initialize tracer provider
	tracerProvider, err := newTracerProvider(ctx, cfg, res)
	if err != nil {
		_ = meterProvider.Shutdown(ctx)
		return nil, err
	}

	if tracerProvider != nil {
		otel.SetTracerProvider(tracerProvider)
	}

	return &Provider{
		meterProvider:  meterProvider,
		tracerProvider: tracerProvider,
		meters:         meters,
	}, nil
}

// latencyBuckets defines histogram buckets for latency metrics (in milliseconds).
var latencyBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

// countBuckets defines histogram buckets for count metrics (records per batch).
var countBuckets = []float64{1, 10, 50, 100, 500, 1000, 5000, 10000}

func createMeters(meter metric.Meter) (*Meters, error) {
	m := &Meters{}
	var err error

	// Performance counters
	m.RecordsProcessed, err = meter.Int64Counter("wallaby.records.processed",
		metric.WithDescription("Total number of records processed"),
		metric.WithUnit("{record}"))
	if err != nil {
		return nil, err
	}

	m.BatchesProcessed, err = meter.Int64Counter("wallaby.batches.processed",
		metric.WithDescription("Total number of batches processed"),
		metric.WithUnit("{batch}"))
	if err != nil {
		return nil, err
	}

	m.BatchLatency, err = meter.Float64Histogram("wallaby.batch.latency",
		metric.WithDescription("Batch processing latency"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(latencyBuckets...))
	if err != nil {
		return nil, err
	}

	m.DestinationWriteLatency, err = meter.Float64Histogram("wallaby.destination.write.latency",
		metric.WithDescription("Destination write latency"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(latencyBuckets...))
	if err != nil {
		return nil, err
	}

	m.RecordsPerBatch, err = meter.Int64Histogram("wallaby.batch.records",
		metric.WithDescription("Number of records per batch"),
		metric.WithUnit("{record}"),
		metric.WithExplicitBucketBoundaries(countBuckets...))
	if err != nil {
		return nil, err
	}

	// Operations counters
	m.ErrorsTotal, err = meter.Int64Counter("wallaby.errors.total",
		metric.WithDescription("Total number of errors by type"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, err
	}

	m.CheckpointCommits, err = meter.Int64Counter("wallaby.checkpoints.commits",
		metric.WithDescription("Total number of checkpoint commits"),
		metric.WithUnit("{commit}"))
	if err != nil {
		return nil, err
	}

	return m, nil
}

// newTracerProvider creates a new TracerProvider with OTLP export.
func newTracerProvider(ctx context.Context, cfg config.TelemetryConfig, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	if cfg.OTLPEndpoint == "" {
		return nil, nil
	}

	// Strip http:// or https:// scheme for gRPC endpoint
	endpoint := cfg.OTLPEndpoint
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	protocol := strings.ToLower(cfg.OTLPProtocol)

	var exporter sdktrace.SpanExporter
	var err error

	if protocol == "http/protobuf" || protocol == "http" {
		httpOpts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(endpoint),
		}
		if cfg.OTLPInsecure {
			httpOpts = append(httpOpts, otlptracehttp.WithInsecure())
		}
		exporter, err = otlptracehttp.New(ctx, httpOpts...)
	} else {
		// Default to gRPC
		grpcOpts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(endpoint),
		}
		if cfg.OTLPInsecure {
			grpcOpts = append(grpcOpts, otlptracegrpc.WithInsecure())
		}
		exporter, err = otlptracegrpc.New(ctx, grpcOpts...)
	}
	if err != nil {
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
	)

	return tracerProvider, nil
}

// newNoopMeters returns meters that do nothing (for when metrics are disabled).
func newNoopMeters() *Meters {
	meter := otel.Meter("noop")
	m := &Meters{}

	m.RecordsProcessed, _ = meter.Int64Counter("wallaby.records.processed")
	m.BatchesProcessed, _ = meter.Int64Counter("wallaby.batches.processed")
	m.BatchLatency, _ = meter.Float64Histogram("wallaby.batch.latency")
	m.DestinationWriteLatency, _ = meter.Float64Histogram("wallaby.destination.write.latency")
	m.RecordsPerBatch, _ = meter.Int64Histogram("wallaby.batch.records")
	m.ErrorsTotal, _ = meter.Int64Counter("wallaby.errors.total")
	m.CheckpointCommits, _ = meter.Int64Counter("wallaby.checkpoints.commits")

	return m
}

// Meters returns the metrics instruments.
func (p *Provider) Meters() *Meters {
	return p.meters
}

// Shutdown gracefully shuts down the telemetry provider.
func (p *Provider) Shutdown(ctx context.Context) error {
	var errs []error
	if p.tracerProvider != nil {
		if err := p.tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if p.meterProvider != nil {
		if err := p.meterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// TracerProvider returns the underlying TracerProvider.
func (p *Provider) TracerProvider() *sdktrace.TracerProvider {
	return p.tracerProvider
}

// Tracer returns a named tracer for the service.
func Tracer(service string) trace.Tracer {
	return otel.Tracer(service)
}

// --- Helper functions for recording metrics ---

// RecordError records an error metric with the given error type.
func (m *Meters) RecordError(ctx context.Context, errorType string) {
	if m == nil {
		return
	}
	m.ErrorsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("error_type", errorType)))
}

// RecordBatch records batch processing metrics.
func (m *Meters) RecordBatch(ctx context.Context, flowID string, recordCount int64, latencyMs float64) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("flow_id", flowID))
	m.RecordsProcessed.Add(ctx, recordCount, attrs)
	m.BatchesProcessed.Add(ctx, 1, attrs)
	m.BatchLatency.Record(ctx, latencyMs, attrs)
	m.RecordsPerBatch.Record(ctx, recordCount, attrs)
}

// RecordDestinationWrite records destination write latency.
func (m *Meters) RecordDestinationWrite(ctx context.Context, flowID string, latencyMs float64) {
	if m == nil {
		return
	}
	m.DestinationWriteLatency.Record(ctx, latencyMs, metric.WithAttributes(attribute.String("flow_id", flowID)))
}

// RecordCheckpoint records a checkpoint commit.
func (m *Meters) RecordCheckpoint(ctx context.Context, flowID string) {
	if m == nil {
		return
	}
	m.CheckpointCommits.Add(ctx, 1, metric.WithAttributes(attribute.String("flow_id", flowID)))
}
