package connector

import "testing"

func TestCapabilitiesValidateSupport(t *testing.T) {
	t.Parallel()

	complete := ContractEvidence{Restart: true, Replay: true, SchemaEvolution: true, Integration: true}
	tests := []struct {
		name    string
		caps    Capabilities
		wantErr bool
	}{
		{name: "experimental", caps: Capabilities{Support: SupportExperimental}},
		{name: "deprecated", caps: Capabilities{Support: SupportDeprecated}},
		{name: "placeholder", caps: Capabilities{Support: SupportPlaceholder}},
		{name: "maintained complete", caps: Capabilities{Support: SupportMaintained, Evidence: complete}},
		{name: "maintained incomplete", caps: Capabilities{Support: SupportMaintained}, wantErr: true},
		{name: "undeclared", caps: Capabilities{}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.caps.ValidateSupport()
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateSupport() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
