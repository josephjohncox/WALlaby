package flow

import "google.golang.org/protobuf/proto"

// ExecutionIdentityEqual compares the persisted configuration that defines a
// flow execution incarnation. It is shared by durable and in-memory engines so
// their lifecycle fencing decisions cannot drift.
func ExecutionIdentityEqual(left, right Flow) (bool, error) {
	if left.WireFormat != right.WireFormat || !left.Config.TableMappings.Equal(right.Config.TableMappings) {
		return false, nil
	}
	if !proto.Equal(left.Source, right.Source) {
		return false, nil
	}
	if len(left.Destinations) != len(right.Destinations) {
		return false, nil
	}
	for index := range left.Destinations {
		if !proto.Equal(left.Destinations[index], right.Destinations[index]) {
			return false, nil
		}
	}
	return true, nil
}
