package flow

import "github.com/josephjohncox/wallaby/pkg/connector"

// Clone returns a deep copy suitable for storage boundaries.
func Clone(in Flow) Flow {
	out := in
	out.Source = cloneSpec(in.Source)
	out.Destinations = make([]connector.Spec, len(in.Destinations))
	for index, destination := range in.Destinations {
		out.Destinations[index] = cloneSpec(destination)
	}
	out.Config.TableMappings = in.Config.TableMappings.Clone()
	out.Config.DDL.Gate = cloneBool(in.Config.DDL.Gate)
	out.Config.DDL.AutoApprove = cloneBool(in.Config.DDL.AutoApprove)
	out.Config.DDL.AutoApply = cloneBool(in.Config.DDL.AutoApply)
	return out
}

func cloneSpec(in connector.Spec) connector.Spec {
	out := in
	if in.Options != nil {
		out.Options = make(map[string]string, len(in.Options))
		for key, value := range in.Options {
			out.Options[key] = value
		}
	}
	return out
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
