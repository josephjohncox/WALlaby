package connector

import "testing"

func TestIsManagedSourceSpecIncludesNamedProfiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec Spec
		want bool
	}{
		{name: "legacy managed", spec: Spec{Type: EndpointPostgres, Options: map[string]string{"managed": "true"}}, want: true},
		{name: "profile only", spec: Spec{Type: EndpointPostgres, Options: map[string]string{"managed_profile": "postgres_to_postgres_v1"}}, want: true},
		{name: "profile whitespace", spec: Spec{Type: EndpointPostgres, Options: map[string]string{"managed_profile": "  "}}},
		{name: "unmanaged postgres", spec: Spec{Type: EndpointPostgres, Options: map[string]string{"managed": "false"}}},
		{name: "non postgres", spec: Spec{Type: EndpointS3, Options: map[string]string{"managed_profile": "named"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsManagedSourceSpec(tt.spec); got != tt.want {
				t.Fatalf("IsManagedSourceSpec()=%t, want %t", got, tt.want)
			}
		})
	}
}
