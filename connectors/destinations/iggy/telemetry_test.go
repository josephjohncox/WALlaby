package iggy

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestWriteTelemetryDoesNotExportArbitraryErrorText(t *testing.T) {
	previous := tracer
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tracer = provider.Tracer("iggy-test")
	t.Cleanup(func() {
		tracer = previous
		_ = provider.Shutdown(context.Background())
	})
	failure := errors.New("private source value from an SDK or codec error")
	destination := openFake(t, testSpec(), &fakeProducer{sendErr: failure})
	if err := destination.Write(context.Background(), testBatch()); !errors.Is(err, failure) {
		t.Fatalf("caller lost the detailed error: %v", err)
	}
	for _, span := range recorder.Ended() {
		if span.Name() != "iggy.Write" {
			continue
		}
		if span.Status().Code != codes.Error || span.Status().Description != "iggy write failed" {
			t.Fatalf("missing sanitized error status: %+v", span.Status())
		}
		if len(span.Events()) != 0 || len(span.Attributes()) != 0 {
			t.Fatal("write span exported arbitrary SDK/codec details")
		}
		return
	}
	t.Fatal("Iggy write did not emit an OpenTelemetry span")
}
