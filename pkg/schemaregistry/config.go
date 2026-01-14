package schemaregistry

import (
	"strconv"
	"strings"
	"time"
)

const (
	OptRegistryType           = "schema_registry"
	OptRegistryURL            = "schema_registry_url"
	OptRegistryUsername       = "schema_registry_username"
	OptRegistryPassword       = "schema_registry_password"
	OptRegistryToken          = "schema_registry_token"
	OptRegistryDSN            = "schema_registry_dsn"
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
	Timeout        time.Duration
	ApicurioCompat bool
	Region         string
	Endpoint       string
	Profile        string
	RoleARN        string
	GlueRegistry   string
	GlueSchema     string
}

// ConfigFromOptions parses registry configuration from connector options.
func ConfigFromOptions(options map[string]string) Config {
	cfg := Config{
		Type:           strings.ToLower(strings.TrimSpace(options[OptRegistryType])),
		URL:            strings.TrimSpace(options[OptRegistryURL]),
		Username:       strings.TrimSpace(options[OptRegistryUsername]),
		Password:       options[OptRegistryPassword],
		Token:          strings.TrimSpace(options[OptRegistryToken]),
		DSN:            strings.TrimSpace(options[OptRegistryDSN]),
		ApicurioCompat: parseBool(options[OptRegistryApicurioCompat], true),
		Region:         strings.TrimSpace(options[OptRegistryRegion]),
		Endpoint:       strings.TrimSpace(options[OptRegistryEndpoint]),
		Profile:        strings.TrimSpace(options[OptRegistryProfile]),
		RoleARN:        strings.TrimSpace(options[OptRegistryRoleARN]),
		GlueRegistry:   strings.TrimSpace(options[OptRegistryGlueRegistry]),
		GlueSchema:     strings.TrimSpace(options[OptRegistryGlueSchema]),
	}
	if cfg.Type == "" {
		if cfg.URL != "" {
			cfg.Type = "csr"
		} else if cfg.DSN != "" {
			cfg.Type = "postgres"
		}
	}
	if raw := strings.TrimSpace(options[OptRegistryTimeout]); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil {
			cfg.Timeout = parsed
		}
	}
	return cfg
}

func parseBool(raw string, fallback bool) bool {
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y":
		return true
	case "0", "false", "no", "n":
		return false
	default:
		parsed, err := strconv.ParseBool(raw)
		if err == nil {
			return parsed
		}
	}
	return fallback
}
