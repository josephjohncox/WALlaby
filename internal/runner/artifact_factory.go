package runner

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/artifactlog"
	"github.com/josephjohncox/wallaby/internal/config"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

// ArtifactLogFactory builds one flow-incarnation-neutral publication adapter;
// the acquired RunFence supplied to each operation binds all authoritative
// rows to the exact incarnation and generation.
type ArtifactLogFactory func(context.Context, flow.Flow, []stream.DestinationConfig) (stream.ManagedArtifactLog, error)

// NewArtifactLogFactory converts deployment-level S3 credentials and limits
// into a lazy per-flow factory. Non-materialized flows never invoke it. The
// production worker registers no catalog consumers until a restartable catalog
// runtime is shipped; materialized CDC therefore means canonical publication,
// not queued or committed downstream destination delivery.
func NewArtifactLogFactory(pool *pgxpool.Pool, cfg config.ArtifactConfig) ArtifactLogFactory {
	return func(ctx context.Context, _ flow.Flow, _ []stream.DestinationConfig) (stream.ManagedArtifactLog, error) {
		if pool == nil {
			return nil, errors.New("artifact publication requires the shared PostgreSQL control pool")
		}
		if strings.TrimSpace(cfg.Bucket) == "" {
			return nil, errors.New("artifact publication requires artifacts.bucket or WALLABY_ARTIFACT_BUCKET")
		}
		objects, err := artifactlog.NewS3Store(ctx, artifactlog.S3Config{
			Bucket: cfg.Bucket, Region: cfg.Region, Endpoint: cfg.Endpoint,
			AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey, SessionToken: cfg.SessionToken,
			ForcePathStyle: cfg.ForcePathStyle,
		})
		if err != nil {
			return nil, err
		}
		return artifactlog.NewRuntime(ctx, pool, objects, artifactlog.RuntimeConfig{
			Stream: artifactlog.StreamConfig{
				HardRetainedBytes:        int64(cfg.HardRetainedBytes),
				BacklogCountHigh:         int64(cfg.BacklogBatchHigh),
				BacklogBytesHigh:         int64(cfg.BacklogBytesHigh),
				BacklogAgeHigh:           cfg.BacklogAgeHigh,
				BackpressurePollInterval: cfg.BackpressurePollInterval,
			},
			OrphanGrace: cfg.OrphanGrace,
			Retention:   cfg.Retention,
			GCInterval:  cfg.GCInterval,
		})
	}
}
