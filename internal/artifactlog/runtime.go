package artifactlog

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/internal/telemetry"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

const recoverySweepLimit = 128

var ErrConsumerRetryable = errors.New("artifact catalog consumer failure is retryable")

// CatalogConsumerConfig binds one immutable destination revision to its
// append-only changelog committer.
type CatalogConsumerConfig struct {
	RevisionID string
	Committer  ChangelogCommitter
}

// RuntimeConfig combines the immutable publication contract with conservative
// orphan and rooted-retention maintenance. PostgreSQL remains the only source
// of quota, backlog, claim, and reachability state.
type RuntimeConfig struct {
	Stream                 StreamConfig
	Projector              stream.Projector
	OrphanGrace            time.Duration
	Retention              time.Duration
	GCInterval             time.Duration
	Consumers              []CatalogConsumerConfig
	DestinationFingerprint string
}

type runtimeConsumer struct {
	revisionID string
	consumer   *Consumer
}

// Runtime is the worker-facing deep module. It exposes publication and read
// admission while keeping S3 reconciliation, quota repair, catalog delivery,
// and mark/sweep local to internal/artifactlog.
type Runtime struct {
	publisher *Publisher
	collector *Collector
	consumers []runtimeConsumer
	config    RuntimeConfig
	lastGC    time.Time
}

// EffectiveDestinationFingerprint returns the deployment-merged non-secret
// catalog identity pinned to the PostgreSQL destination revision.
func (r *Runtime) EffectiveDestinationFingerprint() string {
	if r == nil {
		return ""
	}
	return r.config.DestinationFingerprint
}

func NewRuntime(ctx context.Context, pool *pgxpool.Pool, objects ObjectStore, config RuntimeConfig) (*Runtime, error) {
	if config.OrphanGrace <= 0 || config.Retention <= 0 || config.GCInterval <= 0 {
		return nil, errors.New("positive artifact orphan, retention, and GC intervals are required")
	}
	if config.Stream.ProjectionID == ProjectionIDV2 {
		if config.Projector == nil {
			return nil, errors.New("canonical v2 runtime requires the immutable destination projector")
		}
		if config.Stream.MappingFingerprint == "" || config.Projector.Fingerprint() != config.Stream.MappingFingerprint {
			return nil, errors.New("canonical v2 runtime projector fingerprint mismatch")
		}
	} else if config.Projector != nil || config.Stream.MappingFingerprint != "" {
		return nil, errors.New("canonical v1 runtime forbids logical projection")
	}
	consumerIDs := make([]string, 0, len(config.Consumers))
	seen := make(map[string]struct{}, len(config.Consumers))
	consumers := make([]runtimeConsumer, 0, len(config.Consumers))
	for _, candidate := range config.Consumers {
		if candidate.RevisionID == "" || candidate.Committer == nil {
			return nil, errors.New("artifact catalog consumers require revision ID and committer")
		}
		if _, exists := seen[candidate.RevisionID]; exists {
			return nil, errors.New("artifact catalog consumer revision IDs must be unique")
		}
		seen[candidate.RevisionID] = struct{}{}
		consumer, err := NewConsumer(pool, candidate.Committer)
		if err != nil {
			return nil, err
		}
		consumerIDs = append(consumerIDs, candidate.RevisionID)
		consumers = append(consumers, runtimeConsumer{revisionID: candidate.RevisionID, consumer: consumer})
	}
	if len(config.Consumers) > 0 && strings.TrimSpace(config.DestinationFingerprint) == "" {
		return nil, errors.New("artifact catalog consumers require a non-secret effective destination fingerprint")
	}
	config.Stream.Consumers = consumerIDs
	publisher, err := NewPublisher(ctx, pool, objects, config.Stream)
	if err != nil {
		return nil, err
	}
	collector, err := NewCollector(pool, objects)
	if err != nil {
		return nil, err
	}
	return &Runtime{publisher: publisher, collector: collector, consumers: consumers, config: config}, nil
}

func (r *Runtime) Recover(ctx context.Context, fence connector.RunFence) error {
	if err := r.publisher.Recover(ctx, fence); err != nil {
		return err
	}
	if err := r.consume(ctx, fence, recoverySweepLimit); err != nil {
		if !errors.Is(err, ErrConsumerRetryable) {
			return err
		}
		telemetry.RecordArtifactConsumerOutcome(ctx, "retry_deferred_during_recovery")
	}
	return r.maintain(ctx, fence, recoverySweepLimit)
}

func (r *Runtime) RestoreCheckpoint(ctx context.Context, fence connector.RunFence, checkpoint connector.Checkpoint) (connector.AckGrant, error) {
	return r.publisher.RestoreCheckpoint(ctx, fence, checkpoint)
}

func (r *Runtime) WaitForReadAdmission(ctx context.Context, fence connector.RunFence) error {
	for {
		consumerErr := r.consume(ctx, fence, 1)
		if r.lastGC.IsZero() || time.Since(r.lastGC) >= r.config.GCInterval {
			if err := r.maintain(ctx, fence, 2); err != nil {
				return err
			}
		}
		admissionErr := r.publisher.checkReadAdmission(ctx, fence)
		admitted, wait, err := resolveRuntimeReadAdmission(consumerErr, admissionErr, len(r.consumers) > 0)
		if err != nil {
			return err
		}
		if admitted {
			if consumerErr != nil {
				telemetry.RecordArtifactConsumerOutcome(ctx, "retry_deferred_below_watermark")
			}
			return nil
		}
		if wait {
			telemetry.RecordArtifactConsumerOutcome(ctx, "retry_blocked_at_watermark")
		}
		timer := time.NewTimer(r.config.Stream.BackpressurePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func resolveRuntimeReadAdmission(consumerErr, admissionErr error, hasConsumers bool) (admitted, wait bool, err error) {
	if consumerErr != nil && !errors.Is(consumerErr, ErrConsumerRetryable) {
		return false, false, consumerErr
	}
	if admissionErr == nil {
		return true, false, nil
	}
	if !errors.Is(admissionErr, ErrBackpressure) || !hasConsumers {
		return false, false, admissionErr
	}
	return false, true, nil
}

func (r *Runtime) Append(ctx context.Context, fence connector.RunFence, transaction connector.SourceTransaction) (connector.AckGrant, error) {
	if r.config.Stream.ProjectionID == ProjectionIDV2 {
		projected, _, err := r.config.Projector.ProjectTransaction(transaction)
		if err != nil {
			return connector.AckGrant{}, err
		}
		transaction = projected
	}
	return r.publisher.Append(ctx, fence, transaction)
}

func (r *Runtime) consume(ctx context.Context, fence authority.RunFence, limit int) error {
	for delivered := 0; delivered < limit; {
		progress := false
		for _, runtime := range r.consumers {
			consumed, err := runtime.consumer.ConsumeNext(ctx, fence, runtime.revisionID)
			if err != nil {
				return err
			}
			if consumed {
				progress = true
				delivered++
				if delivered >= limit {
					break
				}
			}
		}
		if !progress {
			return nil
		}
	}
	return nil
}

func (r *Runtime) maintain(ctx context.Context, fence connector.RunFence, limit int) error {
	defer func() { r.lastGC = time.Now() }()
	for swept := 0; swept < limit; swept++ {
		orphan, err := r.collector.CollectOne(ctx, fence, r.config.OrphanGrace)
		if err != nil {
			return err
		}
		retained, err := r.collector.CollectRetainedOne(ctx, fence, r.config.Retention)
		if err != nil {
			return err
		}
		if !orphan && !retained {
			return nil
		}
	}
	return nil
}
