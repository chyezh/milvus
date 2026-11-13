package adaptor

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// replicaViewRetention owns control-plane obligations, independently of the
// read-only RecoveryStorage. Its observer never performs I/O under a View lock.
type replicaViewRetention struct {
	catalog   metastore.WALQueryViewRetentionCatalog
	runtime   *viewpb.StreamingWorkNode
	scheduler nodescheduler.Scheduler
	mu        sync.Mutex
	closed    bool
	views     map[qviews.QueryViewKey]bool // true: retained; false: locally drained
	releases  map[qviews.QueryViewKey]*replicaViewReleaseTask
}

func newReplicaViewRetention(catalog metastore.WALQueryViewRetentionCatalog, runtime *viewpb.StreamingWorkNode, scheduler nodescheduler.Scheduler) *replicaViewRetention {
	return &replicaViewRetention{
		catalog: catalog, runtime: runtime, scheduler: scheduler,
		views: make(map[qviews.QueryViewKey]bool), releases: make(map[qviews.QueryViewKey]*replicaViewReleaseTask),
	}
}

func replicaViewKey(meta *viewpb.QueryViewMeta) qviews.QueryViewKey {
	return qviews.QueryViewKey{ShardID: qviews.NewShardIDFromQVMeta(meta), QueryViewVersion: qviews.FromProtoQueryViewVersion(meta.Version)}
}

// Retain runs before a baseline or event can start lazy resource preparation.
// Already-owned targets do not re-enter admission when Up follows Preparing;
// the local state machine still observes the complete ordered intent stream.
func (r *replicaViewRetention) Retain(ctx context.Context, views []*viewpb.QueryViewOfShard) error {
	for _, view := range views {
		if view.Meta.State != viewpb.QueryViewState_QueryViewStatePreparing && view.Meta.State != viewpb.QueryViewState_QueryViewStateUp {
			continue
		}
		key := replicaViewKey(view.Meta)
		r.mu.Lock()
		retained, exists := r.views[key]
		closed := r.closed
		r.mu.Unlock()
		if closed || (exists && !retained) {
			return merr.WrapErrServiceUnavailable("WAL replica QueryView has already drained")
		}
		if retained {
			continue
		}
		if err := retry.Do(ctx, func() error { return r.catalog.RetainWALQueryView(ctx, r.runtime, view.Meta) }, retry.Attempts(3), retry.Sleep(10*time.Millisecond)); err != nil {
			return err
		}
		r.mu.Lock()
		retained, exists = r.views[key]
		if r.closed || (exists && !retained) {
			r.mu.Unlock()
			return merr.WrapErrServiceUnavailable("WAL replica QueryView drained during acquisition")
		}
		r.views[key] = true
		r.mu.Unlock()
	}
	return nil
}

// Report is called only by local resource lifecycle progress. Receiving Down
// or Dropped from the source is not sufficient: the local handler must finish
// its own query/resource drain and report Dropped first.
func (r *replicaViewRetention) Report(meta *viewpb.QueryViewMeta) {
	if meta.State != viewpb.QueryViewState_QueryViewStateDropped {
		return
	}
	key := replicaViewKey(meta)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.releases[key] != nil {
		return
	}
	if retained, exists := r.views[key]; exists && !retained {
		return
	}
	r.views[key] = false
	task := &replicaViewReleaseTask{owner: r, meta: proto.CloneOf(meta)}
	r.releases[key] = task
	task.handle = r.scheduler.Submit(task)
}

// Close joins release attempts before the owner releases the entire runtime.
// An interrupted individual release remains covered by the runtime obligation.
func (r *replicaViewRetention) Close() {
	r.mu.Lock()
	r.closed = true
	handles := make([]nodescheduler.TaskHandle, 0, len(r.releases))
	for _, task := range r.releases {
		handles = append(handles, task.handle)
	}
	r.mu.Unlock()
	for _, handle := range handles {
		handle.Cancel()
	}
	for _, handle := range handles {
		_ = handle.Wait(context.Background())
	}
}

type replicaViewReleaseTask struct {
	owner   *replicaViewRetention
	meta    *viewpb.QueryViewMeta
	handle  nodescheduler.TaskHandle
	backoff time.Duration
}

func (t *replicaViewReleaseTask) Execute(ctx context.Context) error {
	if err := t.owner.catalog.ReleaseWALQueryView(ctx, t.owner.runtime, t.meta); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		mlog.Warn(ctx, "failed to release drained WAL replica QueryView", mlog.String("view", replicaViewKey(t.meta).String()), mlog.Err(err))
		t.backoff = min(5*time.Second, max(250*time.Millisecond, 2*t.backoff))
		return nodescheduler.DelayUntil(time.Now().Add(t.backoff))
	}
	r := t.owner
	r.mu.Lock()
	delete(r.releases, replicaViewKey(t.meta))
	r.mu.Unlock()
	return nil
}
