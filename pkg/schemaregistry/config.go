package schemaregistry

import (
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/internal/options"
)

const (
	OptRegistryType           = "schema_registry"
	OptRegistryURL            = "schema_registry_url"
	OptRegistryUsername       = "schema_registry_username"
	OptRegistryPassword       = "schema_registry_password"
	OptRegistryToken          = "schema_registry_token"
	OptRegistryDSN            = "schema_registry_dsn"
	OptRegistryLocalDirectory = "schema_registry_local_directory"
	OptRegistryTimeout        = "schema_registry_timeout"
	OptRegistryApicurioCompat = "schema_registry_apicurio_compat"
	OptRegistrySubjectMode    = "schema_registry_subject_mode"
	OptRegistrySubject        = "schema_registry_subject"
	OptRegistryProtoTypes     = "schema_registry_proto_types_subject"

	OptRegistryRegion       = "schema_registry_region"
	OptRegistryEndpoint     = "schema_registry_endpoint"
	OptRegistryProfile      = "schema_registry_profile"
	OptRegistryRoleARN      = "schema_registry_role_arn"
	OptRegistryGlueRegistry = "schema_registry_glue_registry"
	OptRegistryGlueSchema   = "schema_registry_glue_schema"
)

// Config defines schema registry connection settings.
type Config struct {
	Type           string
	URL            string
	Username       string
	Password       string
	Token          string
	DSN            string
	LocalDirectory string
	Timeout        time.Duration
	ApicurioCompat bool
	Region         string
	Endpoint       string
	Profile        string
	RoleARN        string
	GlueRegistry   string
	GlueSchema     string
}

// ConfigFromOptions strictly parses registry configuration from connector options.
func ConfigFromOptions(values map[string]string) (Config, error) {
	decoder := options.NewDecoder("schema registry options", values)
	cfg := Config{
		Type:           strings.ToLower(decoder.String(OptRegistryType, "")),
		URL:            decoder.String(OptRegistryURL, ""),
		Username:       decoder.String(OptRegistryUsername, ""),
		Password:       decoder.Raw(OptRegistryPassword, ""),
		Token:          decoder.String(OptRegistryToken, ""),
		DSN:            decoder.String(OptRegistryDSN, ""),
		LocalDirectory: decoder.String(OptRegistryLocalDirectory, ""),
		Timeout:        decoder.Duration(OptRegistryTimeout, 0),
		ApicurioCompat: decoder.Bool(OptRegistryApicurioCompat, true),
		Region:         decoder.String(OptRegistryRegion, ""),
		Endpoint:       decoder.String(OptRegistryEndpoint, ""),
		Profile:        decoder.String(OptRegistryProfile, ""),
		RoleARN:        decoder.String(OptRegistryRoleARN, ""),
		GlueRegistry:   decoder.String(OptRegistryGlueRegistry, ""),
		GlueSchema:     decoder.String(OptRegistryGlueSchema, ""),
	}
	if err := decoder.Err(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
