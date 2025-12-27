package telemetry

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Tracer returns a named tracer for the service.
func Tracer(service string) trace.Tracer {
	return otel.Tracer(service)
}
