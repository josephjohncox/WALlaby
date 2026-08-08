package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"google.golang.org/protobuf/proto"
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

func TestMarshalFlowConfigUsesStableSnakeCaseKeys(t *testing.T) {
	t.Parallel()
	definition := mappedTestFlow(flow.Flow{ID: "snake-case"})
	definition.Config.AckPolicy = stream.AckPolicyAll
	_, _, configJSON, err := marshalFlowFields(definition)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(configJSON)
	if !strings.Contains(encoded, `"table_mappings"`) || !strings.Contains(encoded, `"ack_policy"`) || strings.Contains(encoded, `"TableMappings"`) {
		t.Fatalf("persisted config JSON does not use stable snake_case keys: %s", encoded)
	}
}

func TestDecodeFlowRejectsPersistedDefinitionWithoutTableMappings(t *testing.T) {
	t.Parallel()
	var decoded flow.Flow
	definition := mappedTestFlow(flow.Flow{Source: workflowTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres}), Destinations: []*wallabypb.Endpoint{workflowTestDestination(connector.RuntimeSpec{Name: "destination", Type: connector.EndpointPostgres})}})
	source, destinations, _, err := marshalFlowFields(definition)
	if err != nil {
		t.Fatal(err)
	}
	config, _ := json.Marshal(flow.Config{})
	if err := decodeFlow(&decoded, source, destinations, config, string(flow.StateCreated), nil, 1); err == nil || !strings.Contains(err.Error(), "incompatible or missing table mappings") {
		t.Fatalf("decodeFlow() error=%v", err)
	}
}

func TestDecodeFlowRejectsLegacyUppercaseTableMappingsKey(t *testing.T) {
	t.Parallel()
	definition := mappedTestFlow(flow.Flow{ID: "legacy-uppercase", Source: workflowTestSource(connector.RuntimeSpec{Type: connector.EndpointPostgres})})
	source, destinations, _, err := marshalFlowFields(definition)
	if err != nil {
		t.Fatal(err)
	}
	mapping, _ := json.Marshal(definition.Config.TableMappings)
	config := []byte(`{"TableMappings":` + string(mapping) + `}`)
	var decoded flow.Flow
	if err := decodeFlow(&decoded, source, destinations, config, string(flow.StateCreated), nil, 1); err == nil || !strings.Contains(err.Error(), "incompatible or missing table mappings") {
		t.Fatalf("decodeFlow() legacy uppercase error=%v", err)
	}
}

func TestScanFlowRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		f := rapidFlow(t)

		sourceJSON, destJSON, configJSON, err := marshalFlowFields(f)
		if err != nil {
			t.Fatalf("marshal flow fields: %v", err)
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
		if !proto.Equal(got.Source, f.Source) {
			t.Fatalf("source mismatch")
		}
		if len(got.Destinations) != len(f.Destinations) {
			t.Fatalf("destinations mismatch")
		}
		for i := range got.Destinations {
			if !proto.Equal(got.Destinations[i], f.Destinations[i]) {
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
	state := rapid.SampledFrom([]flow.State{flow.StateCreated, flow.StateRunning, flow.StatePaused, flow.StateStopping, flow.StateStopped, flow.StateFailed}).Draw(t, "state")
	parallelism := rapid.IntRange(-1, 4).Draw(t, "parallelism")

	source := connector.RuntimeSpec{Name: "source", Type: connector.EndpointPostgres, Options: map[string]string{"mode": connector.SourceModeCDC}}
	if rapid.Bool().Draw(t, "source-dsn") {
		source.Options["dsn"] = "postgres://example/source"
	}
	destCount := rapid.IntRange(1, 3).Draw(t, "destinations")
	destinationSpecs := make([]connector.RuntimeSpec, 0, destCount)
	dests := make([]*wallabypb.Endpoint, 0, destCount)
	for i := 0; i < destCount; i++ {
		spec := rapidSpec(t, fmt.Sprintf("dest-%d", i))
		destinationSpecs = append(destinationSpecs, spec)
		dests = append(dests, workflowTestDestination(spec))
	}

	config := flow.Config{}
	if rapid.Bool().Draw(t, "config") {
		config = flow.Config{
			AckPolicy:          rapid.SampledFrom([]stream.AckPolicy{stream.AckPolicyAll, stream.AckPolicyPrimary}).Draw(t, "ack"),
			PrimaryDestination: rapid.StringMatching(`[a-z]{0,6}`).Draw(t, "primary"),
			FailureMode:        rapid.SampledFrom([]stream.FailureMode{stream.FailureModeHoldSlot, stream.FailureModeDropSlot}).Draw(t, "failure"),
			GiveUpPolicy:       rapid.SampledFrom([]stream.GiveUpPolicy{stream.GiveUpPolicyOnRetryExhaustion, stream.GiveUpPolicyNever}).Draw(t, "giveup"),
			DDL: flow.DDLPolicy{
				Gate:        rapidBoolPtr(t, "ddl_gate"),
				AutoApprove: rapidBoolPtr(t, "ddl_auto_approve"),
				AutoApply:   rapidBoolPtr(t, "ddl_auto_apply"),
			},
		}
	}
	config.TableMappings = flow.NewTableMappings(destinationSpecs)

	wireFormat := connector.WireFormat(rapid.SampledFrom([]string{"", string(connector.WireFormatJSON), string(connector.WireFormatProto)}).Draw(t, "wire"))

	return flow.Flow{
		ID:           id,
		Name:         name,
		Source:       workflowTestSource(source),
		Destinations: dests,
		State:        state,
		WireFormat:   wireFormat,
		Parallelism:  parallelism,
		Config:       config,
	}
}

func rapidSpec(t *rapid.T, prefix string) connector.RuntimeSpec {
	endpoint := rapid.SampledFrom([]connector.EndpointType{
		connector.EndpointPostgres,
		connector.EndpointKafka,
		connector.EndpointHTTP,
		connector.EndpointGRPC,
	}).Draw(t, prefix+"-type")
	options := map[string]string{}
	if rapid.Bool().Draw(t, prefix+"-opts") {
		switch endpoint {
		case connector.EndpointPostgres:
			options["dsn"] = "postgres://example/destination"
		case connector.EndpointKafka:
			options["topic"] = "events"
		case connector.EndpointHTTP:
			options["url"] = "https://example.test/events"
		case connector.EndpointGRPC:
			options["endpoint"] = "example.test:443"
		}
	}
	return connector.RuntimeSpec{Name: prefix, Type: endpoint, Options: options}
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
