package coordview

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type queryViewDrain struct {
	meta      *viewpb.QueryViewMeta
	onDrained func()
}

func (d queryViewDrain) key() qviews.QueryViewKey {
	return qviews.QueryViewKey{ShardID: qviews.NewShardIDFromQVMeta(d.meta), QueryViewVersion: qviews.FromProtoQueryViewVersion(d.meta.Version)}
}

// A drain runs outside the shard flush lane: a disconnected follower must not
// prevent preparation of its replacement. Its durable obligation holds only
// the old View's shared QNs, DataView and LoadInfo references.
type queryViewDrainTask struct {
	scheduler *DirtyViewFlushScheduler
	drain     queryViewDrain
	handle    nodescheduler.TaskHandle
	backoff   time.Duration
}

func (s *DirtyViewFlushScheduler) scheduleReplicaDrains(drains []queryViewDrain) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	for _, drain := range drains {
		key := drain.key()
		if s.drains[key] != nil {
			continue
		}
		task := &queryViewDrainTask{scheduler: s, drain: drain}
		s.drains[key] = task
		task.handle = s.nodeScheduler.Submit(task)
	}
	s.signalLocked()
}

func (t *queryViewDrainTask) Execute(ctx context.Context) error {
	s := t.scheduler
	if ctx.Err() != nil {
		return ctx.Err()
	}
	drained, err := s.replicaRetention.RetireWALQueryView(ctx, t.drain.meta)
	if err != nil || !drained {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			mlog.Warn(ctx, "cannot confirm WAL replica QueryView drain", mlog.String("view", t.drain.key().String()), mlog.Err(err))
		}
		t.backoff = min(5*time.Second, max(250*time.Millisecond, 2*t.backoff))
		return nodescheduler.DelayUntil(time.Now().Add(t.backoff))
	}
	// Keep the task registered while the callback submits its next effects so
	// Flush cannot observe an idle gap before shared-resource release is queued.
	t.drain.onDrained()
	s.mu.Lock()
	delete(s.drains, t.drain.key())
	s.signalLocked()
	s.mu.Unlock()
	return nil
}
