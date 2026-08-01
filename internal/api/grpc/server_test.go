package grpc

import (
	"context"
	"testing"

	"github.com/josephjohncox/wallaby/internal/workflow"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServerHealthContracts(t *testing.T) {
	t.Parallel()

	server := New(workflow.NewMemoryEngine(), nil, nil, nil, nil, false, nil)
	services := []string{"", HealthServiceStartup, HealthServiceReadiness, HealthServiceLiveness}
	for _, service := range services {
		response, err := server.health.Check(context.Background(), &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatalf("check %s: %v", service, err)
		}
		if response.Status != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("health %s = %s, want SERVING", service, response.Status)
		}
	}

	server.Stop()
	for _, service := range services {
		response, err := server.health.Check(context.Background(), &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatalf("check stopped %s: %v", service, err)
		}
		if response.Status != healthpb.HealthCheckResponse_NOT_SERVING {
			t.Fatalf("stopped health %s = %s, want NOT_SERVING", service, response.Status)
		}
	}
}
