package postgres

import (
	"strings"
	"testing"
)

func TestRDSIAMRequiresExplicitRegionAndRoleSessionName(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		options map[string]string
		wantErr string
	}{
		{
			name:    "region is never inferred",
			options: map[string]string{iamOptEnabled: "true"},
			wantErr: "aws_region is required",
		},
		{
			name: "role session name is never synthesized",
			options: map[string]string{
				iamOptEnabled: "true",
				iamOptRegion:  "us-east-1",
				iamOptRoleARN: "arn:aws:iam::123456789012:role/wallaby",
			},
			wantErr: "aws_role_session_name is required",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := rdsIAMConfigFromOptions(test.options)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error=%v, want substring %q", err, test.wantErr)
			}
		})
	}

	cfg, err := rdsIAMConfigFromOptions(map[string]string{
		iamOptEnabled:         "true",
		iamOptRegion:          "us-east-1",
		iamOptRoleARN:         "arn:aws:iam::123456789012:role/wallaby",
		iamOptRoleSessionName: "wallaby-prod",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Region != "us-east-1" || cfg.RoleSessionName != "wallaby-prod" {
		t.Fatalf("config=%+v", cfg)
	}
}
