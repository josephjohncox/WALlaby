// Package telemetry provides OpenTelemetry instrumentation for WALlaby.
package telemetry

import (
	"context"
	"errors"
	"fmt"
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
	"go.opentelemetry.io/otel/trace"
)

// ----------------------------------------------------------------------------
// Provider
// ----------------------------------------------------------------------------

// Provider manages OpenTelemetry providers and exporters.
type Provider struct {
	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
	meters         *Meters
}

// NewProvider creates a new telemetry provider with metrics and tracing export.
func NewProvider(ctx context.Context, cfg config.TelemetryConfig) (*Provider, error) {
	metricsExporter := strings.ToLower(strings.TrimSpace(cfg.MetricsExporter))
	tracesExporter := strings.ToLower(strings.TrimSpace(cfg.TracesExporter))
	metricsEnabled := cfg.OTLPEndpoint != "" && metricsExporter != "" && metricsExporter != "none"
	tracesEnabled := cfg.OTLPEndpoint != "" && tracesExporter != "" && tracesExporter != "none"

	if !metricsEnabled && !tracesEnabled {
		meters, err := newMeters(otel.Meter("noop"))
		if err != nil {
			return nil, err
		}
		return &Provider{meters: meters}, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, err
	}

	var meterProvider *sdkmetric.MeterProvider
	var tracerProvider *sdktrace.TracerProvider
	var meters *Meters

	if metricsEnabled {
		meterProvider, err = newMeterProvider(ctx, cfg, res)
		if err != nil {
			return nil, err
		}
		otel.SetMeterProvider(meterProvider)

		meters, err = newMeters(meterProvider.Meter(cfg.ServiceName))
		if err != nil {
			_ = meterProvider.Shutdown(ctx)
			return nil, err
		}
	} else {
		meters, err = newMeters(otel.Meter("noop"))
		if err != nil {
			return nil, err
		}
	}

	if tracesEnabled {
		tracerProvider, err = newTracerProvider(ctx, cfg, res)
		if err != nil {
			if meterProvider != nil {
				_ = meterProvider.Shutdown(ctx)
			}
			return nil, err
		}
		if tracerProvider != nil {
			otel.SetTracerProvider(tracerProvider)
		}
	}

	return &Provider{
		meterProvider:  meterProvider,
		tracerProvider: tracerProvider,
		meters:         meters,
	}, nil
}

// Meters returns the metrics instruments.
func (p *Provider) Meters() *Meters {
	return p.meters
}

// TracerProvider returns the underlying TracerProvider.
func (p *Provider) TracerProvider() *sdktrace.TracerProvider {
	return p.tracerProvider
}

// Shutdown gracefully shuts down all telemetry providers.
func (p *Provider) Shutdown(ctx context.Context) error {
	var err error
	if p.tracerProvider != nil {
		err = errors.Join(err, p.tracerProvider.Shutdown(ctx))
	}
	if p.meterProvider != nil {
		err = errors.Join(err, p.meterProvider.Shutdown(ctx))
	}
	return err
}

// Tracer returns a named tracer for the service.
func Tracer(service string) trace.Tracer {
	return otel.Tracer(service)
}

// ----------------------------------------------------------------------------
// Providers and Exporters
// ----------------------------------------------------------------------------

func newMeterProvider(ctx context.Context, cfg config.TelemetryConfig, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	exporter, err := newExporter(ctx, cfg, exporterMetric)
	if err != nil {
		return nil, err
	}

	interval := cfg.MetricsInterval
	if interval == 0 {
		interval = 30 * time.Second
	}

	return sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exporter.(sdkmetric.Exporter), sdkmetric.WithInterval(interval)),
		),
	), nil
}

func newTracerProvider(ctx context.Context, cfg config.TelemetryConfig, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	exporter, err := newExporter(ctx, cfg, exporterTrace)
	if err != nil {
		return nil, err
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter.(sdktrace.SpanExporter)),
	), nil
}

type exporterType int

const (
	exporterMetric exporterType = iota
	exporterTrace
)

func newExporter(ctx context.Context, cfg config.TelemetryConfig, typ exporterType) (any, error) {
	endpoint := strings.TrimPrefix(strings.TrimPrefix(cfg.OTLPEndpoint, "http://"), "https://")
	protocol := strings.ToLower(cfg.OTLPProtocol)
	useHTTP := protocol == "http/protobuf" || protocol == "http"

	switch {
	case typ == exporterMetric && useHTTP:
		opts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(endpoint)}
		if cfg.OTLPInsecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		return otlpmetrichttp.New(ctx, opts...)

	case typ == exporterMetric:
		opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(endpoint)}
		if cfg.OTLPInsecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		return otlpmetricgrpc.New(ctx, opts...)

	case typ == exporterTrace && useHTTP:
		opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(endpoint)}
		if cfg.OTLPInsecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		return otlptracehttp.New(ctx, opts...)

	default: // exporterTrace + gRPC
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
		if cfg.OTLPInsecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		return otlptracegrpc.New(ctx, opts...)
	}
}

// ----------------------------------------------------------------------------
// Meters
// ----------------------------------------------------------------------------

// Meters holds all application metric instruments.
type Meters struct {
	// Stream runner metrics
	RecordsProcessed        metric.Int64Counter
	BatchesProcessed        metric.Int64Counter
	BatchLatency            metric.Float64Histogram
	DestinationWriteLatency metric.Float64Histogram
	RecordsPerBatch         metric.Int64Histogram
	ErrorsTotal             metric.Int64Counter
	CheckpointCommits       metric.Int64Counter

	// gRPC metrics
	GRPCRequestsTotal  metric.Int64Counter
	GRPCRequestLatency metric.Float64Histogram
	GRPCErrorsTotal    metric.Int64Counter

	// Workflow metrics
	FlowsActive          metric.Int64UpDownCounter
	FlowStateTransitions metric.Int64Counter
	FlowCreateTotal      metric.Int64Counter

	// Checkpoint store metrics
	CheckpointGetLatency metric.Float64Histogram
	CheckpointPutLatency metric.Float64Histogram

	// Source metrics
	SourceReplicationLag metric.Int64Gauge
	SourceReadLatency    metric.Float64Histogram

	// Destination metrics
	DestinationWriteTotal metric.Int64Counter
	DestinationDDLApplied metric.Int64Counter
}

var (
	latencyBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
	countBuckets   = []float64{1, 10, 50, 100, 500, 1000, 5000, 10000}
)

func newMeters(meter metric.Meter) (*Meters, error) {
	var errs error

	setInt64Counter := func(name string, target *metric.Int64Counter, newMetric func() (metric.Int64Counter, error)) {
		value, err := newMetric()
		if err != nil {
			if errs == nil {
				errs = fmt.Errorf("create metric %q: %w", name, err)
			}
			return
		}
		*target = value
	}
	setFloat64Histogram := func(name string, target *metric.Float64Histogram, newMetric func() (metric.Float64Histogram, error)) {
		value, err := newMetric()
		if err != nil {
			if errs == nil {
				errs = fmt.Errorf("create metric %q: %w", name, err)
			}
			return
		}
		*target = value
	}
	setInt64UpDownCounter := func(name string, target *metric.Int64UpDownCounter, newMetric func() (metric.Int64UpDownCounter, error)) {
		value, err := newMetric()
		if err != nil {
			if errs == nil {
				errs = fmt.Errorf("create metric %q: %w", name, err)
			}
			return
		}
		*target = value
	}
	setInt64Gauge := func(name string, target *metric.Int64Gauge, newMetric func() (metric.Int64Gauge, error)) {
		value, err := newMetric()
		if err != nil {
			if errs == nil {
				errs = fmt.Errorf("create metric %q: %w", name, err)
			}
			return
		}
		*target = value
	}
	setInt64Histogram := func(name string, target *metric.Int64Histogram, newMetric func() (metric.Int64Histogram, error)) {
		value, err := newMetric()
		if err != nil {
			if errs == nil {
				errs = fmt.Errorf("create metric %q: %w", name, err)
			}
			return
		}
		*target = value
	}

	meters := &Meters{}

	// Stream runner
	setInt64Counter("wallaby.records.processed", &meters.RecordsProcessed, func() (metric.Int64Counter, error) {
		return meter.Int64Counter("wallaby.records.processed",
			metric.WithDescription("Total number of records processed"),
			metric.WithUnit("{record}"))
	})
	setInt64Counter("wallaby.batches.processed", &meters.BatchesProcessed, func() (metric.Int64Counter, error) {
		return meter.Int64Counter("wallaby.batches.processed",
			metric.WithDescription("Total number of batches processed"),
			metric.WithUnit("{batch}"))
	})
	setFloat64Histogram("wallaby.batch.latency", &meters.BatchLatency, func() (metric.Float64Histogram, error) {
		return meter.Float64Histogram("wallaby.batch.latency",
			metric.WithDescription("Batch processing latency"),
			metric.WithUnit("ms"),
			metric.WithExplicitBucketBoundaries(latencyBuckets...))
	})
	setFloat64Histogram("wallaby.destination.write.latency", &meters.DestinationWriteLatency, func() (metric.Float64Histogram, error) {
		return meter.Float64Histogram("wallaby.destination.write.latency",
			metric.WithDescription("Destination write latency"),
			metric.WithUnit("ms"),
			metric.WithExplicitBucketBoundaries(latencyBuckets...))
	})
	setInt64Histogram("wallaby.batch.records", &meters.RecordsPerBatch, func() (metric.Int64Histogram, error) {
		return meter.Int64Histogram("wallaby.batch.records",
			metric.WithDescription("Number of records per batch"),
			metric.WithUnit("{record}"),
			metric.WithExplicitBucketBoundaries(countBuckets...))
	})
	setInt64Counter("wallaby.errors.total", &meters.ErrorsTotal, func() (metric.Int64Counter, error) {
		return meter.Int64Counter("wallaby.errors.total",
			metric.WithDescription("Total number of errors by type"),
			metric.WithUnit("{error}"))
	})
	setInt64Counter("wallaby.checkpoints.commits", &meters.CheckpointCommits, func() (metric.Int64Counter, error) {
		return meter.Int64Counter("wallaby.checkpoints.commits",
			metric.WithDescription("Total number of checkpoint commits"),
			metric.WithUnit("{commit}"))
	})

	// gRPC
	setInt64Counter("wallaby.grpc.requests.total", &meters.GRPCRequestsTotal, func() (metric.Int64Counter, error) {
		return meter.Int64Counter("wallaby.grpc.requests.total",
			metric.WithDescription("Total number of gRPC requests"),
			metric.WithUnit("{request}"))
	})
	setFloat64Histogram("wallaby.grpc.request.latency", &meters.GRPCRequestLatency, func() (metric.Float64Histogram, error) {
		return meter.Float64Histogram("wallaby.grpc.request.latency",
			metric.WithDescription("gRPC request latency"),
			metric.WithUnit("ms"),
			metric.WithExplicitBucketBoundaries(latencyBuckets...))
	})
	setInt64Counter("wallaby.grpc.errors.total", &meters.GRPCErrorsTotal, func() (metric.Int64Counter, error) {
		return meter.Int64Counter("wallaby.grpc.errors.total",
			metric.WithDescription("Total number of gRPC errors"),
			metric.WithUnit("{error}"))
	})

	// Workflow
	setInt64UpDownCounter("wallaby.flows.active", &meters.FlowsActive, func() (metric.Int64UpDownCounter, error) {
		return meter.Int64UpDownCounter("wallaby.flows.active",
			metric.WithDescription("Number of currently active flows"),
			metric.WithUnit("{flow}"))
	})
	setInt64Counter("wallaby.flow.state.transitions", &meters.FlowStateTransitions, func() (metric.Int64Counter, error) {
		return meter.Int64Counter("wallaby.flow.state.transitions",
			metric.WithDescription("Total number of flow state transitions"),
			metric.WithUnit("{transition}"))
	})
	setInt64Counter("wallaby.flow.create.total", &meters.FlowCreateTotal, func() (metric.Int64Counter, error) {
		return meter.Int64Counter("wallaby.flow.create.total",
			metric.WithDescription("Total number of flows created"),
			metric.WithUnit("{flow}"))
	})

	// Checkpoint store
	setFloat64Histogram("wallaby.checkpoint.get.latency", &meters.CheckpointGetLatency, func() (metric.Float64Histogram, error) {
		return meter.Float64Histogram("wallaby.checkpoint.get.latency",
			metric.WithDescription("Checkpoint get latency"),
			metric.WithUnit("ms"),
			metric.WithExplicitBucketBoundaries(latencyBuckets...))
	})
	setFloat64Histogram("wallaby.checkpoint.put.latency", &meters.CheckpointPutLatency, func() (metric.Float64Histogram, error) {
		return meter.Float64Histogram("wallaby.checkpoint.put.latency",
			metric.WithDescription("Checkpoint put latency"),
			metric.WithUnit("ms"),
			metric.WithExplicitBucketBoundaries(latencyBuckets...))
	})

	// Source
	setInt64Gauge("wallaby.source.replication.lag", &meters.SourceReplicationLag, func() (metric.Int64Gauge, error) {
		return meter.Int64Gauge("wallaby.source.replication.lag",
			metric.WithDescription("Replication lag in bytes"),
			metric.WithUnit("By"))
	})
	setFloat64Histogram("wallaby.source.read.latency", &meters.SourceReadLatency, func() (metric.Float64Histogram, error) {
		return meter.Float64Histogram("wallaby.source.read.latency",
			metric.WithDescription("Source read latency"),
			metric.WithUnit("ms"),
			metric.WithExplicitBucketBoundaries(latencyBuckets...))
	})

	// Destination
	setInt64Counter("wallaby.destination.write.total", &meters.DestinationWriteTotal, func() (metric.Int64Counter, error) {
		return meter.Int64Counter("wallaby.destination.write.total",
			metric.WithDescription("Total number of destination writes"),
			metric.WithUnit("{write}"))
	})
	setInt64Counter("wallaby.destination.ddl.applied", &meters.DestinationDDLApplied, func() (metric.Int64Counter, error) {
		return meter.Int64Counter("wallaby.destination.ddl.applied",
			metric.WithDescription("Total number of DDL statements applied"),
			metric.WithUnit("{ddl}"))
	})

	if errs != nil {
		return nil, errs
	}
	return meters, nil
}

// ----------------------------------------------------------------------------
// Recording Methods
// ----------------------------------------------------------------------------

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

// RecordGRPCRequest records a gRPC request with method and status.
func (m *Meters) RecordGRPCRequest(ctx context.Context, method, status string) {
	if m == nil {
		return
	}
	m.GRPCRequestsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("method", method),
		attribute.String("status", status),
	))
}

// RecordGRPCLatency records gRPC request latency.
func (m *Meters) RecordGRPCLatency(ctx context.Context, method string, latencyMs float64) {
	if m == nil {
		return
	}
	m.GRPCRequestLatency.Record(ctx, latencyMs, metric.WithAttributes(attribute.String("method", method)))
}

// RecordGRPCError records a gRPC error with method and code.
func (m *Meters) RecordGRPCError(ctx context.Context, method, code string) {
	if m == nil {
		return
	}
	m.GRPCErrorsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("method", method),
		attribute.String("code", code),
	))
}

// RecordFlowActive adjusts the active flow count (delta: +1 or -1).
func (m *Meters) RecordFlowActive(ctx context.Context, delta int64) {
	if m == nil {
		return
	}
	m.FlowsActive.Add(ctx, delta)
}

// RecordFlowStateTransition records a flow state transition.
func (m *Meters) RecordFlowStateTransition(ctx context.Context, fromState, toState string) {
	if m == nil {
		return
	}
	m.FlowStateTransitions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("from_state", fromState),
		attribute.String("to_state", toState),
	))
}

// RecordFlowCreate records a flow creation.
func (m *Meters) RecordFlowCreate(ctx context.Context) {
	if m == nil {
		return
	}
	m.FlowCreateTotal.Add(ctx, 1)
}

// RecordCheckpointGet records checkpoint get latency.
func (m *Meters) RecordCheckpointGet(ctx context.Context, backend string, latencyMs float64) {
	if m == nil {
		return
	}
	m.CheckpointGetLatency.Record(ctx, latencyMs, metric.WithAttributes(attribute.String("backend", backend)))
}

// RecordCheckpointPut records checkpoint put latency.
func (m *Meters) RecordCheckpointPut(ctx context.Context, backend string, latencyMs float64) {
	if m == nil {
		return
	}
	m.CheckpointPutLatency.Record(ctx, latencyMs, metric.WithAttributes(attribute.String("backend", backend)))
}

// RecordSourceLag records replication lag for a slot.
func (m *Meters) RecordSourceLag(ctx context.Context, slot string, lagBytes int64) {
	if m == nil {
		return
	}
	m.SourceReplicationLag.Record(ctx, lagBytes, metric.WithAttributes(
		attribute.String("slot", slot),
	))
}

// RecordSourceReadLatency records source read latency.
func (m *Meters) RecordSourceReadLatency(ctx context.Context, latencyMs float64) {
	if m == nil {
		return
	}
	m.SourceReadLatency.Record(ctx, latencyMs)
}

// RecordDestinationWriteCount records a destination write.
func (m *Meters) RecordDestinationWriteCount(ctx context.Context, destType string) {
	if m == nil {
		return
	}
	m.DestinationWriteTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("type", destType)))
}

// RecordDestinationDDL records a DDL application.
func (m *Meters) RecordDestinationDDL(ctx context.Context, destType string) {
	if m == nil {
		return
	}
	m.DestinationDDLApplied.Add(ctx, 1, metric.WithAttributes(attribute.String("type", destType)))
}
