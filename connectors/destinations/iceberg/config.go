package iceberg

import (
	"errors"
	"fmt"
	"strconv"
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
	TargetNamespace       string
	TablePrefix           string
	FixedTable            string
	ControlTable          string
	DestinationRevisionID string
	MaxCommitRetries      int
	RequestTimeout        time.Duration
	ReconciliationHorizon time.Duration

	OAuthToken      string
	OAuthCredential string
	OAuthScope      string
	OAuthURI        string
	Region          string
	SigningName     string
	SigV4           bool
	AllowHTTP       bool
	CAFile          string
	CAData          string
	ClientCertFile  string
	ClientKeyFile   string
	ServerName      string

	S3TablesTableBucketARN       string
	S3TablesConfigureMaintenance bool
	S3TablesMinSnapshotsToKeep   int32
	S3TablesMaxSnapshotAgeHours  int32

	// Optional S3-compatible object-storage settings for the Iceberg data and
	// metadata FileIO. These configure where the client reads and writes table
	// objects (for example MinIO in local emulation). Production Glue / S3
	// Tables deployments derive credentials from the AWS environment instead.
	// Access and secret keys are secrets and are never read from connector
	// options; only the endpoint and region may be supplied as flow options.
	S3Endpoint        string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Region          string
}

// ParseSpec merges non-empty flow options over deployment defaults. Secrets are
// deliberately not accepted from connector options.
func ParseSpec(spec connector.Spec, defaults Config) (Config, error) {
	cfg := defaults
	option := func(key string) string { return strings.TrimSpace(spec.Options[key]) }
	set := func(key string, target *string) {
		if value := option(key); value != "" {
			*target = value
		}
	}
	set("catalog_profile", &cfg.Profile)
	set("uri", &cfg.URI)
	set("warehouse", &cfg.Warehouse)
	set("prefix", &cfg.Prefix)
	set("namespace", &cfg.TargetNamespace)
	set("table_prefix", &cfg.TablePrefix)
	set("table", &cfg.FixedTable)
	set("control_table", &cfg.ControlTable)
	set("destination_revision_id", &cfg.DestinationRevisionID)
	set("region", &cfg.Region)
	set("s3tables_table_bucket_arn", &cfg.S3TablesTableBucketARN)
	set("s3_endpoint", &cfg.S3Endpoint)
	set("s3_region", &cfg.S3Region)

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
	if raw := option("max_commit_retries"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 32 {
			return Config{}, fmt.Errorf("iceberg max_commit_retries must be between 1 and 32")
		}
		cfg.MaxCommitRetries = value
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
		cfg.SigV4 = true
		cfg.SigningName = "glue"
		if cfg.S3TablesMinSnapshotsToKeep < 2 || cfg.S3TablesMaxSnapshotAgeHours < 1 {
			return Config{}, errors.New("S3 Tables reconciliation requires at least two retained snapshots and a positive age horizon")
		}
	}
	return cfg, nil
}

func (c Config) target(sourceNamespace, sourceTable string) ([]string, error) {
	namespace := strings.TrimSpace(c.TargetNamespace)
	tableName := strings.TrimSpace(c.FixedTable)
	if tableName == "" {
		tableName = c.TablePrefix + sourceTable
		if namespace != "" && sourceNamespace != "" && namespace != sourceNamespace {
			tableName = sourceNamespace + "__" + tableName
		}
	}
	if namespace == "" {
		namespace = sourceNamespace
	}
	if namespace == "" || tableName == "" {
		return nil, errors.New("iceberg source namespace and table must map to a non-empty target")
	}
	return []string{namespace, tableName}, nil
}

func (c Config) controlTarget() ([]string, error) {
	namespace := strings.TrimSpace(c.TargetNamespace)
	if namespace == "" {
		namespace = "wallaby"
	}
	if strings.TrimSpace(c.ControlTable) == "" {
		return nil, errors.New("iceberg control table is required")
	}
	return []string{namespace, c.ControlTable}, nil
}
