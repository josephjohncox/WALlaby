package runner

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	icebergdest "github.com/josephjohncox/wallaby/connectors/destinations/iceberg"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/config"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

// ArtifactLogFactory builds one flow-incarnation-neutral publication adapter;
// the acquired RunFence supplied to each operation binds all authoritative
// rows to the exact incarnation and generation.
type ArtifactLogFactory func(context.Context, flow.Flow, []stream.DestinationConfig) (stream.ManagedArtifactLog, error)

// NewArtifactLogFactory converts deployment-level ordinary-S3 and catalog
// credentials into a lazy per-flow runtime. Iceberg specifications register
// asynchronous catalog consumers; non-Iceberg materialized flows retain the
// canonical-publication-only behavior.
func NewArtifactLogFactory(pool *pgxpool.Pool, cfg config.ArtifactConfig, icebergCfg config.IcebergConfig) ArtifactLogFactory {
	return func(ctx context.Context, f flow.Flow, destinations []stream.DestinationConfig) (stream.ManagedArtifactLog, error) {
		if pool == nil {
			return nil, errors.New("artifact publication requires the shared PostgreSQL control pool")
		}
		if strings.TrimSpace(cfg.Bucket) == "" {
			return nil, errors.New("artifact publication requires artifacts.bucket or WALLABY_ARTIFACT_BUCKET")
		}
		var icebergDefaults icebergdest.Config
		for _, destination := range destinations {
			if destination.Spec.Type != connector.EndpointIceberg {
				continue
			}
			var err error
			icebergDefaults, err = icebergDestinationConfig(icebergCfg)
			if err != nil {
				return nil, fmt.Errorf("configure Iceberg deployment defaults: %w", err)
			}
			break
		}
		objects, err := artifactlog.NewS3Store(ctx, artifactlog.S3Config{
			Bucket: cfg.Bucket, Region: cfg.Region, Endpoint: cfg.Endpoint,
			AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey, SessionToken: cfg.SessionToken,
			ForcePathStyle: cfg.ForcePathStyle,
		})
		if err != nil {
			return nil, err
		}
		catalogConsumers := make([]artifactlog.CatalogConsumerConfig, 0, 1)
		effectiveFingerprint := ""
		var projector stream.Projector
		mappingFingerprint := ""
		for _, destination := range destinations {
			if destination.Spec.Type != connector.EndpointIceberg {
				continue
			}
			parsed, err := icebergdest.ParseSpec(destination.Spec, icebergDefaults)
			if err != nil {
				return nil, fmt.Errorf("configure Iceberg artifact consumer: %w", err)
			}
			if effectiveFingerprint != "" {
				return nil, errors.New("artifact publication supports exactly one Iceberg destination revision")
			}
			if f.Config.Materialization.ProjectionID != artifactlog.ProjectionIDV2 {
				return nil, errors.New("iceberg materialization requires canonical_cdc_parquet_v2")
			}
			if destination.Projector == nil || destination.MappingFingerprint == "" || destination.Projector.Fingerprint() != destination.MappingFingerprint {
				return nil, errors.New("iceberg artifact factory requires the sole immutable destination projector and mapping fingerprint")
			}
			projector = destination.Projector
			mappingFingerprint = destination.MappingFingerprint
			effectiveFingerprint, err = icebergdest.ConfigFingerprint(parsed)
			if err != nil {
				return nil, fmt.Errorf("fingerprint effective Iceberg artifact consumer: %w", err)
			}
			var committer artifactlog.ChangelogCommitter
			switch parsed.Profile {
			case icebergdest.CatalogProfileREST:
				committer, err = icebergdest.NewRESTCommitter(ctx, objects, parsed)
			case icebergdest.CatalogProfileS3Tables:
				committer, err = icebergdest.NewS3TablesCommitter(ctx, objects, parsed)
			default:
				err = fmt.Errorf("unsupported Iceberg profile %q", parsed.Profile)
			}
			if err != nil {
				return nil, fmt.Errorf("start Iceberg artifact consumer: %w", err)
			}
			catalogConsumers = append(catalogConsumers, artifactlog.CatalogConsumerConfig{
				RevisionID: parsed.DestinationRevisionID, Committer: committer,
			})
		}
		return artifactlog.NewRuntime(ctx, pool, objects, artifactlog.RuntimeConfig{
			Stream: artifactlog.StreamConfig{
				ProjectionID: f.Config.Materialization.ProjectionID, MappingFingerprint: mappingFingerprint,
				HardRetainedBytes:        int64(cfg.HardRetainedBytes),
				BacklogCountHigh:         int64(cfg.BacklogBatchHigh),
				BacklogBytesHigh:         int64(cfg.BacklogBytesHigh),
				BacklogAgeHigh:           cfg.BacklogAgeHigh,
				BackpressurePollInterval: cfg.BackpressurePollInterval,
			},
			OrphanGrace: cfg.OrphanGrace,
			Retention:   cfg.Retention,
			GCInterval:  cfg.GCInterval,
			Consumers:   catalogConsumers, Projector: projector,
			DestinationFingerprint: effectiveFingerprint,
		})
	}
}

func icebergDestinationConfig(cfg config.IcebergConfig) (icebergdest.Config, error) {
	minSnapshots, err := checkedInt32Config("s3_tables_min_snapshots_to_keep", cfg.S3TablesMinSnapshotsToKeep)
	if err != nil {
		return icebergdest.Config{}, err
	}
	maxSnapshotAgeHours, err := checkedInt32Config("s3_tables_max_snapshot_age_hours", cfg.S3TablesMaxSnapshotAgeHours)
	if err != nil {
		return icebergdest.Config{}, err
	}
	return icebergdest.Config{
		Profile: cfg.Profile, URI: cfg.URI, Warehouse: cfg.Warehouse, Prefix: cfg.Prefix,
		ControlTable:     cfg.ControlTable,
		MaxCommitRetries: cfg.MaxCommitRetries, RequestTimeout: cfg.RequestTimeout,
		ReconciliationHorizon: cfg.ReconciliationHorizon,
		OAuthToken:            cfg.OAuthToken, OAuthCredential: cfg.OAuthCredential,
		OAuthScope: cfg.OAuthScope, OAuthURI: cfg.OAuthURI,
		Region: cfg.Region, SigningName: cfg.SigningName, ExpectedAWSRoleARN: cfg.ExpectedAWSRoleARN,
		SigV4:      cfg.SigV4,
		S3Endpoint: cfg.S3Endpoint, S3Region: cfg.S3Region,
		AllowHTTP: cfg.AllowHTTP, CAFile: cfg.CAFile, CAData: cfg.CAData,
		ClientCertFile: cfg.ClientCertFile, ClientKeyFile: cfg.ClientKeyFile,
		ServerName: cfg.ServerName, S3TablesTableBucketARN: cfg.S3TablesTableBucketARN,
		S3TablesConfigureMaintenance: cfg.S3TablesConfigureMaintenance,
		S3TablesMinSnapshotsToKeep:   minSnapshots,
		S3TablesMaxSnapshotAgeHours:  maxSnapshotAgeHours,
	}, nil
}

func checkedInt32Config(name string, value int) (int32, error) {
	parsed, err := strconv.ParseInt(strconv.Itoa(value), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s value %d exceeds int32 bounds: %w", name, value, err)
	}
	return int32(parsed), nil // #nosec G115 -- ParseInt with bitSize 32 guarantees the representable range.
}
