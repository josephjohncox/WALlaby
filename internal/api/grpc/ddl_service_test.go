package grpc

import (
	"reflect"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
)

func TestDDLServiceExposesOnlyCurrentControlPlaneTransitions(t *testing.T) {
	service := wallabypb.File_wallaby_v1_ddl_proto.Services().ByName("DDLService")
	if service == nil {
		t.Fatal("DDLService descriptor is missing")
	}
	methods := make([]string, service.Methods().Len())
	for index := range methods {
		methods[index] = string(service.Methods().Get(index).Name())
	}
	want := []string{"ListPendingDDL", "ListDDL", "ApproveDDL", "RejectDDL"}
	if !reflect.DeepEqual(methods, want) {
		t.Fatalf("DDLService methods=%v want=%v", methods, want)
	}
}
