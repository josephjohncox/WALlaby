package grpc

import (
	"context"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMarkDDLAppliedRequiresExecutionReceipts(t *testing.T) {
	t.Parallel()

	service := NewDDLService(nil)
	_, err := service.MarkDDLApplied(context.Background(), &wallabypb.MarkDDLAppliedRequest{Id: 1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("MarkDDLApplied() error=%v, want FailedPrecondition", err)
	}
}
