package flow

import (
	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"google.golang.org/protobuf/proto"
)

// Clone returns a deep copy suitable for storage boundaries.
func Clone(in Flow) Flow {
	out := in
	out.Source = cloneEndpoint(in.Source)
	out.Destinations = make([]*wallabypb.Endpoint, len(in.Destinations))
	for index, destination := range in.Destinations {
		out.Destinations[index] = cloneEndpoint(destination)
	}
	out.Config.TableMappings = in.Config.TableMappings.Clone()
	out.Config.DDL.Gate = cloneBool(in.Config.DDL.Gate)
	out.Config.DDL.AutoApprove = cloneBool(in.Config.DDL.AutoApprove)
	out.Config.DDL.AutoApply = cloneBool(in.Config.DDL.AutoApply)
	return out
}

func cloneEndpoint(in *wallabypb.Endpoint) *wallabypb.Endpoint {
	if in == nil {
		return nil
	}
	return proto.Clone(in).(*wallabypb.Endpoint)
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
