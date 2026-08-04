package artifactlog

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const recoverySweepLimit = 128

// RuntimeConfig combines the immutable publication contract with conservative
// orphan and rooted-retention maintenance. PostgreSQL remains the only source
// of quota, backlog, claim, and reachability state.
type RuntimeConfig struct {
	Stream      StreamConfig
	OrphanGrace time.Duration
	Retention   time.Duration
	GCInterval  time.Duration
}

// Runtime is the worker-facing deep module. It exposes publication and read
// admission while keeping S3 reconciliation, quota repair, and mark/sweep
// local to internal/artifactlog.
type Runtime struct {
	publisher *Publisher
	collector *Collector
	config    RuntimeConfig
	lastGC    time.Time
}

func NewRuntime(ctx context.Context, pool *pgxpool.Pool, objects ObjectStore, config RuntimeConfig) (*Runtime, error) {
	if config.OrphanGrace <= 0 || config.Retention <= 0 || config.GCInterval <= 0 {
		return nil, errors.New("positive artifact orphan, retention, and GC intervals are required")
	}
	publisher, err := NewPublisher(ctx, pool, objects, config.Stream)
	if err != nil {
		return nil, err
	}
	collector, err := NewCollector(pool, objects)
	if err != nil {
		return nil, err
	}
	return &Runtime{publisher: publisher, collector: collector, config: config}, nil
}

func (r *Runtime) Recover(ctx context.Context, fence connector.RunFence) error {
	if err := r.publisher.Recover(ctx, fence); err != nil {
		return err
	}
	return r.maintain(ctx, fence, recoverySweepLimit)
}

func (r *Runtime) RestoreCheckpoint(ctx context.Context, fence connector.RunFence, checkpoint connector.Checkpoint) (connector.AckGrant, error) {
	return r.publisher.RestoreCheckpoint(ctx, fence, checkpoint)
}

func (r *Runtime) WaitForReadAdmission(ctx context.Context, fence connector.RunFence) error {
	if r.lastGC.IsZero() || time.Since(r.lastGC) >= r.config.GCInterval {
		if err := r.maintain(ctx, fence, 2); err != nil {
			return err
		}
	}
	return r.publisher.WaitForReadAdmission(ctx, fence)
}

func (r *Runtime) Append(ctx context.Context, fence connector.RunFence, transaction connector.SourceTransaction) (connector.AckGrant, error) {
	return r.publisher.Append(ctx, fence, transaction)
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
