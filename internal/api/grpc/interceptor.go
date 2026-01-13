package grpc

import (
	"context"
	"time"

	"github.com/josephjohncox/wallaby/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// MetricsInterceptor returns a gRPC unary server interceptor that records metrics.
func MetricsInterceptor(meters *telemetry.Meters) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		latencyMs := float64(time.Since(start).Milliseconds())
		method := info.FullMethod

		meters.RecordGRPCLatency(ctx, method, latencyMs)

		st, _ := status.FromError(err)
		statusCode := st.Code().String()
		meters.RecordGRPCRequest(ctx, method, statusCode)

		if err != nil {
			meters.RecordGRPCError(ctx, method, statusCode)
		}

		return resp, err
	}
}
