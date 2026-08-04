package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMaterializedFlowConfigRoundTrip(t *testing.T) {
	input := flowRuntimeConfig{
		AckPolicy:       "materialized",
		Materialization: &flowMaterializationInfo{ProjectionID: artifactlog.ProjectionID},
	}
	pb := flowRuntimeConfigToProto(input)
	if pb == nil || pb.AckPolicy != wallabypb.AckPolicy_ACK_POLICY_MATERIALIZED || pb.Materialization == nil || pb.Materialization.ProjectionId != artifactlog.ProjectionID {
		t.Fatalf("flowRuntimeConfigToProto()=%+v", pb)
	}
	model := flowConfigFromProto(pb)
	if model.AckPolicy != stream.AckPolicyMaterialized || model.Materialization.ProjectionID != artifactlog.ProjectionID {
		t.Fatalf("flowConfigFromProto()=%+v", model)
	}
	detail := flowDetailFromProto(&wallabypb.Flow{Config: pb})
	if detail.Config.AckPolicy != "materialized" || detail.Config.Materialization == nil || detail.Config.Materialization.ProjectionID != artifactlog.ProjectionID {
		t.Fatalf("flowDetailFromProto().Config=%+v", detail.Config)
	}
}

func TestIcebergCLIEndpointRoundTrip(t *testing.T) {
	pb, err := endpointConfigToProto(endpointConfig{Name: "lake", Type: "iceberg", Options: map[string]string{"catalog_profile": "aws_s3_tables_v1"}})
	if err != nil {
		t.Fatal(err)
	}
	if pb.Type != wallabypb.EndpointType_ENDPOINT_TYPE_ICEBERG {
		t.Fatalf("proto type=%v", pb.Type)
	}
	model, err := endpointFromProto(pb)
	if err != nil {
		t.Fatal(err)
	}
	if model.Type != connector.EndpointIceberg {
		t.Fatalf("model type=%q", model.Type)
	}
	if endpointTypeFromProto(wallabypb.EndpointType_ENDPOINT_TYPE_ICEBERG) != connector.EndpointIceberg {
		t.Fatal("from-proto Iceberg conversion missing")
	}
}

func TestFlowPlanComparesCompleteDefinitionWithoutRuntimeState(t *testing.T) {
	before := flowDetail{Name: "flow", WireFormat: "arrow", Parallelism: 2, State: "running", StateRaw: int32(wallabypb.FlowState_FLOW_STATE_RUNNING), Source: flowEndpointInfoDetail{Name: "source", Type: "postgres", TypeRaw: int32(wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES), Options: map[string]string{"host": "old"}}, Destinations: []flowEndpointInfoDetail{{Name: "target", Type: "postgres", TypeRaw: int32(wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES)}}, Config: flowConfigInfo{AckPolicy: "all"}}
	after := before
	after.State = "unspecified"
	after.StateRaw = 0
	if changes := compareFlowDefinitions(before, after); len(changes) != 0 {
		t.Fatalf("runtime state produced false plan change: %+v", changes)
	}
	after = before
	after.Name = "renamed"
	assertSinglePlanPath(t, compareFlowDefinitions(before, after), "name")
	after = before
	after.WireFormat = "json"
	assertSinglePlanPath(t, compareFlowDefinitions(before, after), "wire_format")
	after = before
	after.Source.Options = map[string]string{"host": "new"}
	assertSinglePlanPath(t, compareFlowDefinitions(before, after), "source")
	after = before
	after.Parallelism = 3
	assertSinglePlanPath(t, compareFlowDefinitions(before, after), "parallelism")
	after = before
	after.Config.AckPolicy = "primary"
	assertSinglePlanPath(t, compareFlowDefinitions(before, after), "config")
	after = before
	after.Destinations = []flowEndpointInfoDetail{{Name: "other", Type: "postgres", TypeRaw: int32(wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES)}, before.Destinations[0]}
	assertSinglePlanPath(t, compareFlowDefinitions(before, after), "destinations")
}
func assertSinglePlanPath(t *testing.T, changes []flowPlanChange, path string) {
	t.Helper()
	if len(changes) != 1 || changes[0].Path != path {
		t.Fatalf("changes=%+v, want path %s", changes, path)
	}
}

func TestAdministrativeEndpointSanitizationAcrossOutputFormats(t *testing.T) {
	tests := []struct{ input, want string }{{"admin.example:9443", "admin.example:9443"}, {"https://user:password@admin.example:9443/private/path?token=secret#fragment", "https://admin.example:9443"}, {"https://[2001:db8::1]:9443/private?token=secret", "https://[2001:db8::1]:9443"}, {"https://[fe80::1%25eth0]:9443/private", redactedEndpointOption}, {"[fe80::1%eth0]:9443", redactedEndpointOption}, {"https://%65xample.com:9443/private", redactedEndpointOption}, {"https://admin.example:9443/\x01private", redactedEndpointOption}, {"\nadmin.example:9443", redactedEndpointOption}, {"user:password@admin.example:9443", redactedEndpointOption}, {"admin.example:9443/private", redactedEndpointOption}, {"://malformed-capability", redactedEndpointOption}}
	for _, tt := range tests {
		if got := sanitizeAdministrativeEndpoint(tt.input); got != tt.want {
			t.Errorf("sanitizeAdministrativeEndpoint(%q)=%q, want %q", tt.input, got, tt.want)
		}
	}
	raw := "https://admin-user:admin-password@admin.example:9443/private/path?token=admin-token#admin-fragment"
	for _, outputFlag := range []string{"", "--json", "--yaml"} {
		name := outputFlag
		if name == "" {
			name = "human"
		}
		t.Run(name, func(t *testing.T) {
			args := []string{"check", "--endpoint", raw}
			if outputFlag != "" {
				args = append(args, outputFlag)
			}
			command := newAdminCommand()
			command.SetArgs(args)
			output, err := captureAdminStdout(command.Execute)
			if err != nil {
				t.Fatal(err)
			}
			text := string(output)
			if !strings.Contains(text, "https://admin.example:9443") || strings.Contains(text, "admin-user") || strings.Contains(text, "admin-password") || strings.Contains(text, "private/path") || strings.Contains(text, "admin-token") || strings.Contains(text, "admin-fragment") {
				t.Fatalf("unsafe %s admin endpoint output: %s", name, text)
			}
		})
	}
}

func TestAdministrativeGRPCNormalizedErrorIsBounded(t *testing.T) {
	raw := "https://admin-user:admin-password@admin.example:9443/private/path?token=raw-token#raw-fragment"
	root := newAdminCommand()
	command, _, err := root.Find([]string{"flow", "get"})
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Flags().Set("endpoint", raw); err != nil {
		t.Fatal(err)
	}
	normalizedTransportMessage := "connection error: transport dial target admin-user:admin-password@admin.example:9443/private/path?token=normalized-token"
	got := runWithConfig(command, func(*cobra.Command, []string) error {
		return fmt.Errorf("get flow: %w", status.Error(codes.Unavailable, normalizedTransportMessage))
	}, nil)
	if got == nil {
		t.Fatal("expected bounded gRPC error")
	}
	text := got.Error()
	if text != "admin endpoint \"https://admin.example:9443\" rpc failed (class=grpc_status, grpc_status=Unavailable)" {
		t.Fatalf("unexpected bounded error: %s", text)
	}
	for _, secret := range []string{"admin-user", "admin-password", "private/path", "raw-token", "raw-fragment", "normalized-token", "connection error", "transport dial target"} {
		if strings.Contains(text, secret) {
			t.Fatalf("bounded error leaked %q: %s", secret, text)
		}
	}
}

func TestAdminRPCBoundaryUsesResolvedEndpointSourcesAndPrecedence(t *testing.T) {
	configRaw := "https://config-user:config-password@config.example:9443/config-path?token=config-token"
	envRaw := "https://env-user:env-password@env.example:9443/env-path?token=env-token"
	flagRaw := "https://flag-user:flag-password@flag.example:9443/flag-path?token=flag-token"
	tests := []struct{ name, env, explicit, want string }{{"config value", "", "", configRaw}, {"environment over config", envRaw, "", envRaw}, {"explicit flag over environment and config", envRaw, flagRaw, flagRaw}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			t.Setenv("WALLABY_ADMIN_ENDPOINT", tt.env)
			t.Setenv("WALLABY_ADMIN_CONFIG", "")
			configPath := filepath.Join(t.TempDir(), "admin.yaml")
			if err := os.WriteFile(configPath, []byte("endpoint: \""+configRaw+"\"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			root := newAdminCommand()
			command, _, err := root.Find([]string{"check"})
			if err != nil {
				t.Fatal(err)
			}
			if err := root.PersistentFlags().Set("config", configPath); err != nil {
				t.Fatal(err)
			}
			if tt.explicit != "" {
				if err := command.Flags().Set("endpoint", tt.explicit); err != nil {
					t.Fatal(err)
				}
			}
			if err := initAdminConfig(command); err != nil {
				t.Fatal(err)
			}
			resolved, err := stringFlag(command, "endpoint")
			if err != nil {
				t.Fatal(err)
			}
			if *resolved != tt.want {
				t.Fatalf("resolved endpoint=%q, want %q", *resolved, tt.want)
			}
			transport := status.Error(codes.Unavailable, "normalized connection target env-user:env-password@env.example/env-path?token=normalized-token")
			got := runWithConfig(command, func(*cobra.Command, []string) error { return transport }, nil)
			if got == nil {
				t.Fatal("expected bounded error")
			}
			text := got.Error()
			wantSafe := sanitizeAdministrativeEndpoint(tt.want)
			if !strings.Contains(text, wantSafe) || !strings.Contains(text, "class=grpc_status") || !strings.Contains(text, "grpc_status=Unavailable") {
				t.Fatalf("unexpected bounded error: %s", text)
			}
			for _, secret := range []string{"config-user", "config-password", "config-path", "config-token", "env-user", "env-password", "env-path", "env-token", "flag-user", "flag-password", "flag-path", "flag-token", "normalized-token", "normalized connection target"} {
				if strings.Contains(text, secret) {
					t.Fatalf("bounded error leaked %q: %s", secret, text)
				}
			}
		})
	}
}

func TestAdminRPCBoundaryUsesInheritedPersistentEndpointAndFailsClosed(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	raw := "https://persistent-user:persistent-password@persistent.example:9443/private?token=persistent-token"
	root := &cobra.Command{Use: "root"}
	root.PersistentFlags().String("endpoint", "", "admin endpoint")
	child := &cobra.Command{Use: "child"}
	root.AddCommand(child)
	if err := root.PersistentFlags().Set("endpoint", raw); err != nil {
		t.Fatal(err)
	}
	resolved, err := stringFlag(child, "endpoint")
	if err != nil {
		t.Fatal(err)
	}
	if *resolved != raw {
		t.Fatalf("inherited endpoint=%q, want %q", *resolved, raw)
	}
	rpcErr := status.Error(codes.DeadlineExceeded, "dial normalized persistent-password@persistent.example/private")
	got := runWithConfig(child, func(*cobra.Command, []string) error { return rpcErr }, nil)
	if got == nil || got.Error() != "admin endpoint \"https://persistent.example:9443\" rpc failed (class=grpc_status, grpc_status=DeadlineExceeded)" {
		t.Fatalf("unexpected inherited endpoint error: %v", got)
	}
	missing := &cobra.Command{Use: "missing"}
	generic := runWithConfig(missing, func(*cobra.Command, []string) error { return status.Error(codes.Unavailable, "raw normalized secret") }, nil)
	if generic == nil || generic.Error() != "admin endpoint \"[REDACTED]\" rpc failed (class=endpoint_resolution_failed)" {
		t.Fatalf("resolution failure was not generically bounded: %v", generic)
	}
}

func TestClickHouseTLSKeyFileRedactedAcrossOutputFormats(t *testing.T) {
	oldFS := adminFileSystem
	adminFileSystem = afero.NewMemMapFs()
	t.Cleanup(func() { adminFileSystem = oldFS })
	cfg := completeFlowFile()
	cfg.Destinations[0].Type = "clickhouse"
	cfg.Config.TableMappings.Destinations[0].Tables[0].Write.Mode = "append"
	cfg.Config.TableMappings.Destinations[0].Tables[0].Write.KeyColumns = nil
	cfg.Destinations[0].Options = map[string]string{"tls_key_file": "/var/run/secrets/clickhouse/client-private-key.pem", "tls_ca_file": "/etc/wallaby/clickhouse-ca.pem", "tls_cert_file": "/etc/wallaby/clickhouse-client.pem", "tls_server_name": "clickhouse.example"}
	payload, err := encodeDeterministic(cfg, "json")
	if err != nil {
		t.Fatal(err)
	}
	if err := afero.WriteFile(adminFileSystem, "clickhouse.json", payload, 0600); err != nil {
		t.Fatal(err)
	}
	for _, outputFlag := range []string{"", "--json", "--yaml"} {
		name := outputFlag
		if name == "" {
			name = "human"
		}
		t.Run(name, func(t *testing.T) {
			args := []string{"flow", "validate", "--file", "clickhouse.json"}
			if outputFlag != "" {
				args = append(args, outputFlag)
			}
			command := newAdminCommand()
			command.SetArgs(args)
			output, err := captureAdminStdout(command.Execute)
			if err != nil {
				t.Fatal(err)
			}
			text := string(output)
			for _, secretPath := range []string{"/var/run/secrets/clickhouse/client-private-key.pem", "/etc/wallaby/clickhouse-ca.pem", "/etc/wallaby/clickhouse-client.pem"} {
				if strings.Contains(text, secretPath) {
					t.Fatalf("%s output leaked local path %q: %s", name, secretPath, text)
				}
			}
			if outputFlag != "" {
				for _, key := range []string{"tls_key_file", "tls_ca_file", "tls_cert_file"} {
					if !strings.Contains(text, key) {
						t.Fatalf("%s output omitted audited key %q: %s", name, key, text)
					}
				}
				if strings.Count(text, redactedEndpointOption) < 3 {
					t.Fatalf("%s output did not redact credential and local TLS paths: %s", name, text)
				}
			}
		})
	}
}

func TestEndpointConnectivityErrorsDoNotEchoOptions(t *testing.T) {
	result := checkEndpointResult("webhook", connector.EndpointHTTP, false, map[string]string{"url": "http://%signed-capability"}, true, time.Second)
	if result.Reachable || result.Error != "http connectivity check failed" || strings.Contains(result.Error, "signed-capability") {
		t.Fatalf("unsafe connectivity result: %+v", result)
	}
}

func TestEndpointOptionClassificationAndSanitization(t *testing.T) {
	tests := []struct {
		name, key, value string
		class            endpointOptionValueClass
		want             string
	}{
		{"HTTP URL", "url", "https://hooks.example/signed/path?signature=secret", endpointOptionURL, "https://hooks.example/[REDACTED]"},
		{"webhook", "webhook_url", "https://hooks.slack.com/services/T/B/secret", endpointOptionURL, "https://hooks.slack.com/[REDACTED]"},
		{"catalog URI", "catalog_uri", "https://catalog.example/v1/config", endpointOptionURL, "https://catalog.example/[REDACTED]"},
		{"DuckLake catalog", "catalog", "postgres:dbname=ducklake user=wallaby password=secret", endpointOptionSensitive, redactedEndpointOption},
		{"DuckLake data path", "data_path", "s3://bucket/private/prefix?signature=x", endpointOptionURL, "s3://bucket/[REDACTED]"},
		{"catalog endpoint", "catalog_endpoint", "https://catalog.example:8443/api?sig=x", endpointOptionNetwork, "https://catalog.example:8443/[REDACTED]"},
		{"registry URL", "schema_registry_url", "https://registry.example/subjects?token=x", endpointOptionURL, "https://registry.example/[REDACTED]"},
		{"registry endpoint", "schema_registry_endpoint", "https://glue.example", endpointOptionNetwork, "https://glue.example"},
		{"S3 endpoint", "s3_endpoint", "https://s3.example/bucket?X-Amz-Signature=x", endpointOptionNetwork, "https://s3.example/[REDACTED]"},
		{"AWS endpoint", "aws_endpoint", "not a url", endpointOptionNetwork, redactedEndpointOption},
		{"gRPC endpoint", "endpoint", "https://grpc.example:8443/ingest?signature=x", endpointOptionNetwork, "https://grpc.example:8443/[REDACTED]"},
		{"gRPC host endpoint", "endpoint", "grpc.example:8443", endpointOptionNetwork, "grpc.example:8443"},
		{"gRPC address", "address", "grpc.example:443", endpointOptionNetwork, "grpc.example:443"},
		{"gRPC address capability", "address", "https://user:pass@grpc.example:443/service?sig=x", endpointOptionNetwork, "https://grpc.example:443/[REDACTED]"},
		{"Kafka brokers", "brokers", "kafka-1.example:9092, kafka-2.example:9092", endpointOptionNetwork, "kafka-1.example:9092,kafka-2.example:9092"},
		{"Kafka credential broker", "brokers", "user:pass@kafka.example:9092", endpointOptionNetwork, redactedEndpointOption},
		{"bootstrap servers", "bootstrap_servers", "boot-1.example:9092,boot-2.example:9092", endpointOptionNetwork, "boot-1.example:9092,boot-2.example:9092"},
		{"keeper address", "managed_keeper_address", "keeper.example:9181", endpointOptionNetwork, "keeper.example:9181"},
		{"TLS server name", "tls_server_name", "service.example", endpointOptionNetwork, "service.example"},
		{"managed TLS server name", "managed_replica_tls_server_name", "replica.example", endpointOptionNetwork, "replica.example"},
		{"server", "server", "service.example:443", endpointOptionNetwork, "service.example:443"},
		{"server capability", "server", "https://user:pass@service.example/private", endpointOptionNetwork, "https://service.example/[REDACTED]"},
		{"host", "host", "db.example:5432", endpointOptionNetwork, "db.example:5432"},
		{"malformed host", "host", "db.example:5432/private", endpointOptionNetwork, redactedEndpointOption},
		{"Postgres DSN", "dsn", "postgres://user:pass@db.example/db", endpointOptionSensitive, redactedEndpointOption},
		{"snapshot DSN", "snapshot_state_dsn", "postgres://user:pass@db.example/state", endpointOptionSensitive, redactedEndpointOption},
		{"registry DSN", "schema_registry_dsn", "postgres://user:pass@db.example/registry", endpointOptionSensitive, redactedEndpointOption},
		{"replica DSN", "managed_replica_dsn", "clickhouse://user:pass@replica.example/db", endpointOptionSensitive, redactedEndpointOption},
		{"registry password", "schema_registry_password", "secret", endpointOptionSensitive, redactedEndpointOption},
		{"registry token", "schema_registry_token", "secret", endpointOptionSensitive, redactedEndpointOption},
		{"S3 access key", "access_key", "secret", endpointOptionSensitive, redactedEndpointOption},
		{"S3 secret key", "secret_key", "secret", endpointOptionSensitive, redactedEndpointOption},
		{"raw headers", "headers", "Authorization: Bearer secret", endpointOptionSensitive, redactedEndpointOption},
		{"ClickHouse TLS key file", "tls_key_file", "/var/run/secrets/clickhouse/key.pem", endpointOptionSensitive, redactedEndpointOption},
		{"private key file", "client_private_key_file", "/var/run/secrets/client.pem", endpointOptionSensitive, redactedEndpointOption},
		{"password file", "database_password_file", "/var/run/secrets/password", endpointOptionSensitive, redactedEndpointOption},
		{"token file", "oauth_token_file", "/var/run/secrets/token", endpointOptionSensitive, redactedEndpointOption},
		{"secret file", "shared_secret_file", "/var/run/secrets/shared", endpointOptionSensitive, redactedEndpointOption},
		{"credentials file", "aws_credentials_file", "/var/run/secrets/aws", endpointOptionSensitive, redactedEndpointOption},
		{"TLS CA file", "tls_ca_file", "/etc/wallaby/ca.pem", endpointOptionSensitive, redactedEndpointOption},
		{"TLS cert file", "tls_cert_file", "/etc/wallaby/client.pem", endpointOptionSensitive, redactedEndpointOption},
		{"type mappings file", "type_mappings_file", "/etc/wallaby/types.json", endpointOptionSensitive, redactedEndpointOption},
		{"registry profile", "schema_registry_profile", "production", endpointOptionOrdinary, "production"},
		{"bootstrap mode", "bootstrap", "snapshot", endpointOptionOrdinary, "snapshot"},
		{"Kafka topic", "topic", "events", endpointOptionOrdinary, "events"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyEndpointOptionKey(tt.key); got != tt.class {
				t.Fatalf("classify %q=%d, want %d", tt.key, got, tt.class)
			}
			safe := redactFlowEndpoint(flowEndpointInfoDetail{Options: map[string]string{tt.key: tt.value}})
			if got := safe.Options[tt.key]; got != tt.want {
				t.Fatalf("render %q=%q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestFlowDetailAndPlanRedactSecretsWithoutMaskingComparison(t *testing.T) {
	pb := &wallabypb.Flow{Name: "flow", Source: &wallabypb.Endpoint{Name: "source", Type: wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES, Options: map[string]string{"host": "db.internal", "dsn": "postgres://user:dsn-secret@db.internal/db", "password": "password-secret", "oauth_client_secret": "oauth-secret", "aws_access_key_id": "access-secret", "endpoint_url": "https://catalog.internal/v1", "broker_url": "https://user:url-secret@broker.internal/v1", "webhook_url": "https://hooks.internal/v1?access_token=query-secret", "sasl_jaas_config": "password=jaas-secret", "oauth_token_endpoint": "https://identity.internal/token", "username": "visible-user", "headers": "{\"Authorization\":\"Bearer raw-secret\"}", "header": "QXV0aG9yaXphdGlvbjogQmVhcmVyIGVuY29kZWQtc2VjcmV0", "http_authorization_header": "Bearer http-secret", "grpc_authorization_header": "Basic grpc-secret", "http_auth_header": "Bearer http-short-secret", "grpc_auth_header": "Basic grpc-short-secret", "url": "https://hooks.slack.com/services/T000/B000/slack-path-secret", "github_webhook_url": "https://api.github.com/repos/org/repo/hooks/github-path-secret", "signed_url": "https://objects.example/bucket/object?signed=query-secret", "userinfo_uri": "https://url-user:url-password@example.internal/private", "plain_url": "https://plain.example:8443", "malformed_url": "://not-a-url", "endpoint": "https://endpoint.example/endpoint-path-secret", "uri": "https://uri.example/uri-path-secret", "webhook": "https://discord.example/api/webhooks/id/webhook-path-secret"}}, Destinations: []*wallabypb.Endpoint{{Name: "target", Type: wallabypb.EndpointType_ENDPOINT_TYPE_POSTGRES, Options: map[string]string{"token": "destination-token", "host": "sink.internal"}}}}
	raw := flowDetailForComparisonFromProto(pb)
	safe := flowDetailFromProto(pb)
	if safe.Destinations[0].Options["token"] != redactedEndpointOption || safe.Destinations[0].Options["host"] != "sink.internal" {
		t.Fatalf("destination options not safely rendered: %+v", safe.Destinations[0].Options)
	}
	if raw.Source.Options["password"] != "password-secret" {
		t.Fatalf("comparison input was redacted: %+v", raw.Source.Options)
	}
	for _, key := range []string{"dsn", "password", "oauth_client_secret", "aws_access_key_id", "malformed_url", "sasl_jaas_config", "headers", "header", "http_authorization_header", "grpc_authorization_header", "http_auth_header", "grpc_auth_header"} {
		if safe.Source.Options[key] != redactedEndpointOption {
			t.Fatalf("%s not redacted: %+v", key, safe.Source.Options)
		}
	}
	for _, key := range []string{"host", "username", "plain_url"} {
		if safe.Source.Options[key] != pb.Source.Options[key] {
			t.Fatalf("nonsecret %s hidden: %+v", key, safe.Source.Options)
		}
	}
	wantURLs := map[string]string{"endpoint_url": "https://catalog.internal/[REDACTED]", "broker_url": "https://broker.internal/[REDACTED]", "webhook_url": "https://hooks.internal/[REDACTED]", "oauth_token_endpoint": "https://identity.internal/[REDACTED]", "url": "https://hooks.slack.com/[REDACTED]", "github_webhook_url": "https://api.github.com/[REDACTED]", "signed_url": "https://objects.example/[REDACTED]", "userinfo_uri": "https://example.internal/[REDACTED]", "endpoint": "https://endpoint.example/[REDACTED]", "uri": "https://uri.example/[REDACTED]", "webhook": "https://discord.example/[REDACTED]"}
	for key, want := range wantURLs {
		if safe.Source.Options[key] != want {
			t.Fatalf("sanitized %s=%q, want %q", key, safe.Source.Options[key], want)
		}
	}
	changed := flowDetailForComparisonFromProto(pb)
	changed.Source.Options = map[string]string{}
	for key, value := range raw.Source.Options {
		changed.Source.Options[key] = value
	}
	changed.Source.Options["password"] = "new-password-secret"
	changes := compareFlowDefinitions(raw, changed)
	assertSinglePlanPath(t, changes, "source")
	encoded := changes[0].Before + changes[0].After
	if strings.Contains(encoded, "password-secret") || strings.Contains(encoded, "new-password-secret") || strings.Contains(encoded, "slack-path-secret") || strings.Contains(encoded, "github-path-secret") || strings.Contains(encoded, "query-secret") || strings.Contains(encoded, "url-user") || strings.Contains(encoded, "endpoint-path-secret") || strings.Contains(encoded, "uri-path-secret") || strings.Contains(encoded, "webhook-path-secret") || !strings.Contains(encoded, redactedEndpointOption) {
		t.Fatalf("plan change leaked secret: %s", encoded)
	}
}

func TestFlowValidateRedactsJSONAndYAMLInputsAndOutputs(t *testing.T) {
	oldFS := adminFileSystem
	adminFileSystem = afero.NewMemMapFs()
	t.Cleanup(func() { adminFileSystem = oldFS })
	cfg := completeFlowFile()
	cfg.Source.Options = map[string]string{"headers": "{\"Authorization\":\"Bearer source-secret\"}", "host": "source.internal", "webhook_url": "https://hooks.slack.com/services/T/B/validate-slack-secret", "plain_url": "https://plain.example:8443"}
	cfg.Destinations[0].Options = map[string]string{"grpc_authorization_header": "Basic destination-secret", "stream": "orders", "api_endpoint": "https://api.github.com/hooks/validate-github-secret?signature=signed-secret"}
	for _, inputFormat := range []string{"json", "yaml"} {
		input, err := encodeDeterministic(cfg, inputFormat)
		if err != nil {
			t.Fatal(err)
		}
		path := "flow." + inputFormat
		if err := afero.WriteFile(adminFileSystem, path, input, 0600); err != nil {
			t.Fatal(err)
		}
		for _, outputFlag := range []string{"--json", "--yaml"} {
			command := newAdminCommand()
			command.SetArgs([]string{"flow", "validate", "--file", path, outputFlag})
			output, err := captureAdminStdout(command.Execute)
			if err != nil {
				t.Fatal(err)
			}
			text := string(output)
			if strings.Contains(text, "source-secret") || strings.Contains(text, "destination-secret") || strings.Contains(text, "validate-slack-secret") || strings.Contains(text, "validate-github-secret") || strings.Contains(text, "signed-secret") || !strings.Contains(text, redactedEndpointOption) || !strings.Contains(text, "source.internal") || !strings.Contains(text, "https://hooks.slack.com/[REDACTED]") || !strings.Contains(text, "https://api.github.com/[REDACTED]") || !strings.Contains(text, "https://plain.example:8443") || !strings.Contains(text, "orders") || !strings.Contains(text, "destinations") {
				t.Fatalf("%s input %s output was unsafe or incomplete: %s", inputFormat, outputFlag, text)
			}
		}
	}
}
func captureAdminStdout(run func() error) ([]byte, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	old := os.Stdout
	os.Stdout = writer
	runErr := run()
	_ = writer.Close()
	os.Stdout = old
	payload, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if runErr != nil {
		return payload, runErr
	}
	return payload, readErr
}

func TestFlowPlanReportsMappingOnlyAndConfigOnlyChanges(t *testing.T) {
	beforeMappings := completeTestMappings()
	afterMappings := beforeMappings.Clone()
	afterMappings.Destinations[0].Tables[0].TargetTable = "renamed"
	before := flowConfigInfo{AckPolicy: "all", TableMappings: &beforeMappings}
	after := flowConfigInfo{AckPolicy: "all", TableMappings: &afterMappings}
	changes := compareFlowConfig(before, after)
	if len(changes) != 1 || changes[0].Path != "config.table_mappings" {
		t.Fatalf("mapping changes=%+v", changes)
	}
	after = before
	after.AckPolicy = "primary"
	changes = compareFlowConfig(before, after)
	if len(changes) != 1 || changes[0].Path != "config" {
		t.Fatalf("config changes=%+v", changes)
	}
}
