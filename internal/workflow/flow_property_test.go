package workflow

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"pgregory.net/rapid"
)

type stubRow struct {
	values []any
	Err    error
}

func (s stubRow) Scan(dest ...any) error {
	if s.Err != nil {
		return s.Err
	}
	if len(dest) != len(s.values) {
		return fmt.Errorf("scan mismatch")
	}
	for i, target := range dest {
		val := s.values[i]
		switch out := target.(type) {
		case *string:
			*out = val.(string)
		case *[]byte:
			*out = val.([]byte)
		case *int:
			*out = val.(int)
		case **string:
			if val == nil {
				*out = nil
				continue
			}
			str := val.(string)
			*out = &str
		default:
			return fmt.Errorf("unsupported scan type %T", target)
		}
	}
	return nil
}

func TestScanFlowRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		f := rapidFlow(t)

		sourceJSON, _ := json.Marshal(f.Source)
		destJSON, _ := json.Marshal(f.Destinations)
		configJSON, _ := json.Marshal(f.Config)
		if f.Config == (flow.Config{}) {
			configJSON = nil
		}

		var wireFormat any
		if f.WireFormat != "" {
			wireFormat = string(f.WireFormat)
		}

		row := stubRow{values: []any{
			f.ID,
			f.Name,
			sourceJSON,
			destJSON,
			string(f.State),
			wireFormat,
			f.Parallelism,
			configJSON,
		}}

		got, err := scanFlow(row)
		if err != nil {
			t.Fatalf("scan flow: %v", err)
		}

		if got.ID != f.ID || got.Name != f.Name {
			t.Fatalf("identity mismatch")
		}
		if got.State != f.State {
			t.Fatalf("state mismatch")
		}
		if got.WireFormat != f.WireFormat {
			t.Fatalf("wire format mismatch")
		}
		if f.Parallelism <= 0 {
			if got.Parallelism != 1 {
				t.Fatalf("expected default parallelism")
			}
		} else if got.Parallelism != f.Parallelism {
			t.Fatalf("parallelism mismatch")
		}
		if !specEqual(got.Source, f.Source) {
			t.Fatalf("source mismatch")
		}
		if len(got.Destinations) != len(f.Destinations) {
			t.Fatalf("destinations mismatch")
		}
		for i := range got.Destinations {
			if !specEqual(got.Destinations[i], f.Destinations[i]) {
				t.Fatalf("destination mismatch")
			}
		}
		if !got.Config.Equal(f.Config) {
			t.Fatalf("config mismatch")
		}
	})
}

func rapidFlow(t *rapid.T) flow.Flow {
	id := rapid.StringMatching(`[a-z]{4,8}`).Draw(t, "id")
	name := rapid.StringMatching(`[a-z]{0,8}`).Draw(t, "name")
	state := rapid.SampledFrom([]flow.State{flow.StateCreated, flow.StateRunning, flow.StatePaused, flow.StateStopping, flow.StateFailed}).Draw(t, "state")
	parallelism := rapid.IntRange(-1, 4).Draw(t, "parallelism")

	source := rapidSpec(t, "source")
	destCount := rapid.IntRange(1, 3).Draw(t, "destinations")
	dests := make([]connector.Spec, 0, destCount)
	for i := 0; i < destCount; i++ {
		dests = append(dests, rapidSpec(t, fmt.Sprintf("dest-%d", i)))
	}

	config := flow.Config{}
	if rapid.Bool().Draw(t, "config") {
		config = flow.Config{
			AckPolicy:                       rapid.SampledFrom([]stream.AckPolicy{stream.AckPolicyAll, stream.AckPolicyPrimary}).Draw(t, "ack"),
			PrimaryDestination:              rapid.StringMatching(`[a-z]{0,6}`).Draw(t, "primary"),
			FailureMode:                     rapid.SampledFrom([]stream.FailureMode{stream.FailureModeHoldSlot, stream.FailureModeDropSlot}).Draw(t, "failure"),
			GiveUpPolicy:                    rapid.SampledFrom([]stream.GiveUpPolicy{stream.GiveUpPolicyOnRetryExhaustion, stream.GiveUpPolicyNever}).Draw(t, "giveup"),
			SchemaRegistrySubject:           rapid.StringMatching(`[a-z]{0,6}`).Draw(t, "reg_subject"),
			SchemaRegistryProtoTypesSubject: rapid.StringMatching(`[a-z]{0,6}`).Draw(t, "reg_proto_subject"),
			SchemaRegistrySubjectMode:       rapid.SampledFrom([]string{"", "topic", "table", "topic_table"}).Draw(t, "reg_mode"),
			DDL: flow.DDLPolicy{
				Gate:        rapidBoolPtr(t, "ddl_gate"),
				AutoApprove: rapidBoolPtr(t, "ddl_auto_approve"),
				AutoApply:   rapidBoolPtr(t, "ddl_auto_apply"),
			},
		}
	}

	wireFormat := connector.WireFormat(rapid.SampledFrom([]string{"", string(connector.WireFormatJSON), string(connector.WireFormatProto)}).Draw(t, "wire"))

	return flow.Flow{
		ID:           id,
		Name:         name,
		Source:       source,
		Destinations: dests,
		State:        state,
		WireFormat:   wireFormat,
		Parallelism:  parallelism,
		Config:       config,
	}
}

func rapidSpec(t *rapid.T, prefix string) connector.Spec {
	options := map[string]string{}
	if rapid.Bool().Draw(t, prefix+"-opts") {
		options["opt"] = rapid.StringMatching(`[a-z]{0,6}`).Draw(t, prefix+"-opt")
	}
	endpoint := rapid.SampledFrom([]connector.EndpointType{
		connector.EndpointPostgres,
		connector.EndpointKafka,
		connector.EndpointHTTP,
		connector.EndpointGRPC,
	}).Draw(t, prefix+"-type")
	return connector.Spec{
		Name:    prefix,
		Type:    endpoint,
		Options: options,
	}
}

func specEqual(a, b connector.Spec) bool {
	if a.Name != b.Name || a.Type != b.Type {
		return false
	}
	if len(a.Options) != len(b.Options) {
		return false
	}
	for k, v := range a.Options {
		if b.Options[k] != v {
			return false
		}
	}
	return true
}

func rapidBoolPtr(t *rapid.T, name string) *bool {
	switch rapid.IntRange(0, 2).Draw(t, name) {
	case 0:
		return nil
	case 1:
		val := true
		return &val
	default:
		val := false
		return &val
	}
}
