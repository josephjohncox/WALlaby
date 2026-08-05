package iceberg

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	CatalogProfileREST     = "rest"
	CatalogProfileS3Tables = "s3tables"
)

// Config is the non-secret catalog and projection contract. Authentication and
// TLS defaults are supplied by deployment configuration and may not be stored
// in publication rows or table-bucket objects.
type Config struct {
	Profile               string
	URI                   string
	Warehouse             string
	Prefix                string
	ControlTable          string
	DestinationRevisionID string
	MaxCommitRetries      int
	RequestTimeout        time.Duration
	ReconciliationHorizon time.Duration

	OAuthToken         string
	OAuthCredential    string
	OAuthScope         string
	OAuthURI           string
	Region             string
	SigningName        string
	ExpectedAWSRoleARN string
	SigV4              bool
	AllowHTTP          bool
	CAFile             string
	CAData             string
	ClientCertFile     string
	ClientKeyFile      string
	ServerName         string

	S3TablesTableBucketARN       string
	S3TablesConfigureMaintenance bool
	S3TablesMinSnapshotsToKeep   int32
	S3TablesMaxSnapshotAgeHours  int32

	// Optional S3-compatible object-storage settings for the Iceberg data and
	// metadata FileIO. These configure where the client reads and writes table
	// objects (for example MinIO in local emulation). Production Glue / S3
	// Tables deployments derive credentials from the AWS environment instead.
	// Access and secret keys are secrets and are never read from connector
	// options; endpoint and region are deployment-owned as well.
	S3Endpoint        string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Region          string
}

// ValidateFlowSpec validates the persisted, non-secret half of an Iceberg
// destination through the connector-wide pre-persistence contract.
func ValidateFlowSpec(spec connector.Spec) error {
	return connector.ValidatePersistedSpec(spec)
}

func catalogAuthenticationConfigured(cfg Config) bool {
	return cfg.SigV4 || cfg.OAuthToken != "" || cfg.OAuthCredential != "" || cfg.ClientKeyFile != ""
}

// ConfigFingerprint returns a non-secret identity for the effective catalog,
// target mapping, security mode, and behavior controls used by one destination
// revision. Credential rotation does not change the identity; changing the
// catalog or materialization behavior does.
func ConfigFingerprint(cfg Config) (string, error) {
	type fingerprint struct {
		Profile, URI, Warehouse, Prefix, ControlTable                 string
		Region, SigningName, ExpectedAWSRoleARN, OAuthScope, OAuthURI string
		S3TablesTableBucketARN, S3Endpoint, S3Region, ServerName      string
		MaxCommitRetries                                              int
		RequestTimeout, ReconciliationHorizon                         int64
		SigV4, AllowHTTP                                              bool
		OAuthToken, OAuthCredential, MTLS, S3StaticCredentials        bool
		S3TablesConfigureMaintenance                                  bool
		S3TablesMinSnapshotsToKeep, S3TablesMaxSnapshotAgeHours       int32
		CADataHash                                                    string
		CAFile, ClientCertFile, ClientKeyFile                         string
	}
	caHash := ""
	if cfg.CAData != "" {
		sum := sha256.Sum256([]byte(cfg.CAData))
		caHash = hex.EncodeToString(sum[:])
	}
	encoded, err := json.Marshal(fingerprint{
		Profile: strings.ToLower(strings.TrimSpace(cfg.Profile)), URI: strings.TrimSuffix(strings.TrimSpace(cfg.URI), "/"),
		Warehouse: strings.TrimSpace(cfg.Warehouse), Prefix: strings.Trim(strings.TrimSpace(cfg.Prefix), "/"),
		ControlTable: cfg.ControlTable,
		Region:       cfg.Region, SigningName: cfg.SigningName, ExpectedAWSRoleARN: cfg.ExpectedAWSRoleARN,
		OAuthScope: cfg.OAuthScope, OAuthURI: strings.TrimSuffix(strings.TrimSpace(cfg.OAuthURI), "/"),
		S3TablesTableBucketARN: cfg.S3TablesTableBucketARN, S3Endpoint: strings.TrimSuffix(strings.TrimSpace(cfg.S3Endpoint), "/"), S3Region: cfg.S3Region, ServerName: cfg.ServerName,
		MaxCommitRetries: cfg.MaxCommitRetries, RequestTimeout: int64(cfg.RequestTimeout), ReconciliationHorizon: int64(cfg.ReconciliationHorizon),
		SigV4: cfg.SigV4, AllowHTTP: cfg.AllowHTTP, OAuthToken: cfg.OAuthToken != "", OAuthCredential: cfg.OAuthCredential != "",
		MTLS: cfg.ClientCertFile != "" || cfg.ClientKeyFile != "", S3StaticCredentials: cfg.S3AccessKeyID != "" || cfg.S3SecretAccessKey != "",
		S3TablesConfigureMaintenance: cfg.S3TablesConfigureMaintenance, S3TablesMinSnapshotsToKeep: cfg.S3TablesMinSnapshotsToKeep,
		S3TablesMaxSnapshotAgeHours: cfg.S3TablesMaxSnapshotAgeHours, CADataHash: caHash, CAFile: cfg.CAFile,
		ClientCertFile: cfg.ClientCertFile, ClientKeyFile: cfg.ClientKeyFile,
	})
	if err != nil {
		return "", fmt.Errorf("encode effective Iceberg config fingerprint: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// ParseSpec merges flow target mapping over deployment defaults. Authenticated
// endpoint identity is deployment-bound: a flow may repeat an identical value
// for readability, but it cannot redirect deployment OAuth, mTLS, SigV4, or S3
// credentials to another catalog, region, warehouse, bucket, or object endpoint.
func ParseSpec(spec connector.Spec, defaults Config) (Config, error) {
	if err := ValidateFlowSpec(spec); err != nil {
		return Config{}, err
	}
	cfg := defaults
	option := func(key string) string { return strings.TrimSpace(spec.Options[key]) }
	set := func(key string, target *string) {
		if value := option(key); value != "" {
			*target = value
		}
	}
	if profile := strings.ToLower(option("catalog_profile")); profile != "" {
		if deploymentProfile := strings.ToLower(strings.TrimSpace(defaults.Profile)); deploymentProfile != "" && profile != deploymentProfile {
			return Config{}, fmt.Errorf("iceberg catalog_profile is deployment-bound to %q", deploymentProfile)
		}
		cfg.Profile = profile
	}
	set("control_table", &cfg.ControlTable)
	set("destination_revision_id", &cfg.DestinationRevisionID)

	if cfg.Profile == "" {
		cfg.Profile = CatalogProfileREST
	}
	cfg.Profile = strings.ToLower(cfg.Profile)
	if cfg.ControlTable == "" {
		cfg.ControlTable = "__wallaby_control"
	}
	if cfg.MaxCommitRetries == 0 {
		cfg.MaxCommitRetries = 4
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.ReconciliationHorizon == 0 {
		cfg.ReconciliationHorizon = 24 * time.Hour
	}
	if cfg.S3TablesMinSnapshotsToKeep == 0 {
		cfg.S3TablesMinSnapshotsToKeep = 100
	}
	if cfg.S3TablesMaxSnapshotAgeHours == 0 {
		cfg.S3TablesMaxSnapshotAgeHours = int32((cfg.ReconciliationHorizon + time.Hour - 1) / time.Hour) // #nosec G115 -- reconciliation horizon is a bounded positive operational setting.
	}
	if strings.TrimSpace(cfg.DestinationRevisionID) == "" {
		return Config{}, errors.New("iceberg destination_revision_id is required")
	}
	if cfg.Profile != CatalogProfileREST && cfg.Profile != CatalogProfileS3Tables {
		return Config{}, fmt.Errorf("unsupported Iceberg catalog_profile %q", cfg.Profile)
	}
	if strings.TrimSpace(cfg.URI) == "" {
		if cfg.Profile != CatalogProfileS3Tables || strings.TrimSpace(cfg.Region) == "" {
			return Config{}, errors.New("iceberg catalog URI is required")
		}
		cfg.URI = "https://glue." + cfg.Region + ".amazonaws.com/iceberg"
	}
	if cfg.Profile == CatalogProfileS3Tables && !cfg.AllowHTTP {
		expectedURI := "https://glue." + strings.TrimSpace(cfg.Region) + ".amazonaws.com/iceberg"
		if strings.TrimSuffix(strings.TrimSpace(cfg.URI), "/") != expectedURI {
			return Config{}, fmt.Errorf("S3 Tables requires the regional AWS Glue Iceberg endpoint %q", expectedURI)
		}
	}
	if strings.TrimSpace(cfg.Warehouse) == "" {
		return Config{}, errors.New("iceberg warehouse is required")
	}
	if cfg.MaxCommitRetries < 1 || cfg.RequestTimeout <= 0 || cfg.ReconciliationHorizon <= 0 {
		return Config{}, errors.New("iceberg retry, timeout, and reconciliation settings must be positive")
	}
	if cfg.Profile == CatalogProfileS3Tables {
		if strings.TrimSpace(cfg.Region) == "" || strings.TrimSpace(cfg.S3TablesTableBucketARN) == "" {
			return Config{}, errors.New("S3 Tables requires region and s3tables_table_bucket_arn")
		}
		if strings.TrimSpace(cfg.ExpectedAWSRoleARN) == "" {
			return Config{}, errors.New("S3 Tables requires expected_aws_role_arn")
		}
		cfg.SigV4 = true
		cfg.SigningName = "glue"
		if cfg.S3TablesMinSnapshotsToKeep < 2 || cfg.S3TablesMaxSnapshotAgeHours < 1 {
			return Config{}, errors.New("S3 Tables reconciliation requires at least two retained snapshots and a positive age horizon")
		}
	}
	return cfg, nil
}

func (c Config) target(mappedNamespace, mappedTable string) ([]string, error) {
	mappedNamespace = strings.TrimSpace(mappedNamespace)
	mappedTable = strings.TrimSpace(mappedTable)
	if mappedNamespace == "" || mappedTable == "" {
		return nil, errors.New("iceberg v2 mapped namespace and table are required")
	}
	return []string{mappedNamespace, mappedTable}, nil
}

func (c Config) controlTarget() ([]string, error) {
	namespace := "wallaby"
	if strings.TrimSpace(c.ControlTable) == "" {
		return nil, errors.New("iceberg control table is required")
	}
	return []string{namespace, c.ControlTable}, nil
}
