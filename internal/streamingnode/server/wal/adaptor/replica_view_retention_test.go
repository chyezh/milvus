package adaptor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type replicaViewCatalogStub struct {
	metastore.WALQueryViewRetentionCatalog
}

func (*replicaViewCatalogStub) RetainWALQueryView(context.Context, *viewpb.StreamingWorkNode, *viewpb.QueryViewMeta) error {
	panic("mockey")
}

func (*replicaViewCatalogStub) ReleaseWALQueryView(context.Context, *viewpb.StreamingWorkNode, *viewpb.QueryViewMeta) error {
	panic("mockey")
}

func replicaRetentionView(version int64) *viewpb.QueryViewOfShard {
	return &viewpb.QueryViewOfShard{Meta: &viewpb.QueryViewMeta{
		CollectionId: 1, ReplicaId: 1, Vchannel: "p_1v0",
		Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: version},
		State:   viewpb.QueryViewState_QueryViewStatePreparing,
	}, StreamingNode: &viewpb.QueryViewOfStreamingNode{}}
}

func TestReplicaViewOwnershipPrecedesPreparationAndOutlivesTransport(t *testing.T) {
	mockey.PatchConvey("only real local Dropped releases the shared obligation", t, func() {
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		r := newReplicaViewRetention(&replicaViewCatalogStub{}, &viewpb.StreamingWorkNode{RuntimeId: 1}, scheduler)
		defer r.Close()
		retain := mockey.Mock((*replicaViewCatalogStub).RetainWALQueryView).Return(nil).Build()
		var releaseCalls atomic.Int32
		mockey.Mock((*replicaViewCatalogStub).ReleaseWALQueryView).To(func(_ *replicaViewCatalogStub, ctx context.Context, runtime *viewpb.StreamingWorkNode, meta *viewpb.QueryViewMeta) error {
			require.NoError(t, ctx.Err())
			require.EqualValues(t, 1, runtime.RuntimeId)
			require.EqualValues(t, 1, meta.Version.QueryVersion)
			if releaseCalls.Add(1) == 1 {
				return merr.ErrServiceUnavailable
			}
			return nil
		}).Build()
		view := replicaRetentionView(1)
		ctx, cancel := context.WithCancel(t.Context())
		require.NoError(t, r.Retain(ctx, []*viewpb.QueryViewOfShard{view}))
		view.Meta.State = viewpb.QueryViewState_QueryViewStateUp
		require.NoError(t, r.Retain(ctx, []*viewpb.QueryViewOfShard{view}))
		require.Equal(t, 1, retain.Times(), "Up after Preparing does not need new admission")
		cancel()
		view.Meta.State = viewpb.QueryViewState_QueryViewStateDown
		require.NoError(t, r.Retain(t.Context(), []*viewpb.QueryViewOfShard{view}))
		r.Report(view.Meta)
		require.Zero(t, releaseCalls.Load())
		view.Meta.State = viewpb.QueryViewState_QueryViewStateDropped
		r.Report(view.Meta)
		r.Report(view.Meta)
		require.Eventually(t, func() bool {
			r.mu.Lock()
			defer r.mu.Unlock()
			return len(r.releases) == 0
		}, 3*time.Second, time.Millisecond)
		require.EqualValues(t, 2, releaseCalls.Load(), "a failed release retries independently of transport cancellation")
		r.Report(view.Meta)
		view.Meta.State = viewpb.QueryViewState_QueryViewStateUp
		require.Error(t, r.Retain(t.Context(), []*viewpb.QueryViewOfShard{view}))
		r.Close()
		other := replicaRetentionView(2)
		require.Error(t, r.Retain(t.Context(), []*viewpb.QueryViewOfShard{other}))
		other.Meta.State = viewpb.QueryViewState_QueryViewStateDropped
		r.Report(other.Meta)
		require.Empty(t, r.releases)
	})
}

func TestReplicaViewAcquisitionFailuresAndConcurrentDrain(t *testing.T) {
	for _, scenario := range []string{"failure", "drain", "close"} {
		t.Run(scenario, func(t *testing.T) {
			mockey.PatchConvey("failed or raced acquisition never enables preparation", t, func() {
				scheduler := nodescheduler.New(1)
				defer scheduler.Close()
				r := newReplicaViewRetention(&replicaViewCatalogStub{}, &viewpb.StreamingWorkNode{RuntimeId: 1}, scheduler)
				defer r.Close()
				mockey.Mock((*replicaViewCatalogStub).ReleaseWALQueryView).Return(nil).Build()
				mockey.Mock((*replicaViewCatalogStub).RetainWALQueryView).To(func(_ *replicaViewCatalogStub, _ context.Context, _ *viewpb.StreamingWorkNode, meta *viewpb.QueryViewMeta) error {
					switch scenario {
					case "failure":
						return merr.ErrServiceUnavailable
					case "drain":
						drained := proto.CloneOf(meta)
						drained.State = viewpb.QueryViewState_QueryViewStateDropped
						r.Report(drained)
					case "close":
						r.Close()
					}
					return nil
				}).Build()
				view := replicaRetentionView(1)
				require.Error(t, r.Retain(t.Context(), []*viewpb.QueryViewOfShard{view}))
				r.mu.Lock()
				require.False(t, r.views[replicaViewKey(view.Meta)])
				r.mu.Unlock()
			})
		})
	}
}

func TestReplicaViewCloseJoinsCatalogRelease(t *testing.T) {
	mockey.PatchConvey("whole-runtime release cannot race unjoined local work", t, func() {
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		r := newReplicaViewRetention(&replicaViewCatalogStub{}, &viewpb.StreamingWorkNode{RuntimeId: 1}, scheduler)
		started, finished := make(chan struct{}), make(chan struct{})
		mockey.Mock((*replicaViewCatalogStub).ReleaseWALQueryView).To(func(_ *replicaViewCatalogStub, ctx context.Context, _ *viewpb.StreamingWorkNode, _ *viewpb.QueryViewMeta) error {
			close(started)
			<-ctx.Done()
			close(finished)
			return ctx.Err()
		}).Build()
		view := replicaRetentionView(1)
		view.Meta.State = viewpb.QueryViewState_QueryViewStateDropped
		r.Report(view.Meta)
		<-started
		r.Close()
		select {
		case <-finished:
		default:
			t.Fatal("release worker was not joined")
		}
	})
}

func TestReadOnlyReplicaRejectsUnprotectedQueryView(t *testing.T) {
	mockey.PatchConvey("baseline and live intents acquire shared ownership before local Apply", t, func() {
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		retention := newReplicaViewRetention(&replicaViewCatalogStub{}, &viewpb.StreamingWorkNode{RuntimeId: 1}, scheduler)
		defer retention.Close()
		mockey.Mock((*replicaViewCatalogStub).RetainWALQueryView).Return(merr.ErrServiceUnavailable).Build()
		cfg, snapshot := readOnlyReplicaBaseline()
		snapshot.QueryViews = []*viewpb.QueryViewOfShard{replicaRetentionView(1)}
		_, err := newReadOnlyReplica(t.Context(), cfg, snapshot, nil, nil, retention, recovery.WithNodeScheduler(scheduler))
		require.Error(t, err)
		snapshot.QueryViews = nil
		r, err := newReadOnlyReplica(t.Context(), cfg, snapshot, nil, nil, retention, recovery.WithNodeScheduler(scheduler))
		require.NoError(t, err)
		defer r.Close()
		err = r.storage.Apply(t.Context(), walevent.Event{Cursor: walevent.Cursor{Term: 7, Seq: 1}, Payload: walevent.QueryViewIntentBatch{Views: []*viewpb.QueryViewOfShard{replicaRetentionView(1)}}})
		require.Error(t, err)
		require.EqualValues(t, 0, r.storage.Applied().Seq)
		require.NoError(t, r.storage.ApplyError(), "failed acquisition must leave the old query prefix usable")
		var reset *walevent.ResetError
		require.ErrorAs(t, err, &reset)
		require.True(t, r.IsAvailable())
	})
}
