package adaptor

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

var _ wal.ReplicaRefreshProvider = (*readOnlyReplica)(nil)

func (r *readOnlyReplica) ReplicaRefreshRequired() bool {
	return r.refresh.Load() || (r.dependencyRefresh != nil && r.dependencyRefresh())
}

// startFollowing is called once, before publishing the runtime. A transport
// attempt owns neither the installed state nor its query lifetime. The manager
// owns source changes and replacement baselines, including same-term eviction.
func (r *readOnlyReplica) startFollowing(client streamingpb.StreamingNodeReplicaServiceClient, primary types.PChannelInfo, opts ...grpc.CallOption) {
	r.followDone = make(chan struct{})
	go func() {
		defer close(r.followDone)
		delay := backoff.NewExponentialBackOff()
		delay.InitialInterval = 100 * time.Millisecond
		delay.MaxInterval = 5 * time.Second
		delay.MaxElapsedTime = 0
		delay.Reset()
		for r.availableCtx.Err() == nil {
			before := r.storage.Applied()
			err := r.storage.SubscribeAndApply(r.availableCtx, client, primary, opts...)
			if r.availableCtx.Err() != nil {
				return
			}
			if failure := r.storage.ApplyError(); failure != nil {
				// Apply may already have changed growing data. Such a runtime
				// cannot continue serving or retry this event on the same state.
				mlog.Warn(r.availableCtx, "WAL replica application failed", mlog.String("channel", r.channel.Name), mlog.Err(failure))
				r.cancel()
				return
			}
			var reset *walevent.ResetError
			if errors.As(err, &reset) {
				r.refresh.Store(true)
				return
			}
			if r.storage.Applied() != before {
				delay.Reset()
			}
			interval := delay.NextBackOff()
			mlog.Info(r.availableCtx, "reconnecting WAL replica event source", mlog.String("channel", r.channel.Name), mlog.Duration("retryAfter", interval), mlog.Err(err))
			timer := time.NewTimer(interval)
			select {
			case <-r.availableCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}
