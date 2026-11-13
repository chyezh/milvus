package adaptor

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestReplicaFollowResumesAppliedPrefix(t *testing.T) {
	mockey.PatchConvey("transient failures retain the runtime and retry the applied cursor", t, func() {
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		cfg, snapshot := readOnlyReplicaBaseline()
		r, err := newReadOnlyReplica(t.Context(), cfg, snapshot, nil, nil, nil, recovery.WithNodeScheduler(scheduler))
		require.NoError(t, err)
		defer r.Close()
		var attempts atomic.Int64
		resumed := make(chan struct{})
		mockey.Mock((*recovery.ReadOnlyRecoveryStorage).SubscribeAndApply).To(func(storage *recovery.ReadOnlyRecoveryStorage, ctx context.Context, _ streamingpb.StreamingNodeReplicaServiceClient, _ types.PChannelInfo, _ ...grpc.CallOption) error {
			switch attempts.Add(1) {
			case 1:
				require.NoError(t, storage.Apply(ctx, walevent.Event{Cursor: walevent.Cursor{Term: 7, Seq: 1}, Payload: walevent.QueryViewIntentBatch{}}))
				return io.EOF
			case 2:
				require.Equal(t, walevent.Cursor{Term: 7, Seq: 1}, storage.Applied())
				return context.DeadlineExceeded
			default:
				require.NoError(t, storage.Apply(ctx, walevent.Event{Cursor: walevent.Cursor{Term: 7, Seq: 2}, Payload: walevent.QueryViewIntentBatch{}}))
				close(resumed)
				<-ctx.Done()
				return ctx.Err()
			}
		}).Build()
		manager := r.storage.VChannelManager()
		r.startFollowing(nil, cfg.Channel)
		select {
		case <-resumed:
		case <-time.After(5 * time.Second):
			t.Fatal("follower did not reconnect")
		}
		require.True(t, r.IsAvailable())
		require.False(t, r.ReplicaRefreshRequired())
		require.Same(t, manager, r.storage.VChannelManager())
		r.Close()
		require.EqualValues(t, 3, attempts.Load())
	})
}

func TestReplicaFollowResetPreservesQueriesButApplyFailureFences(t *testing.T) {
	for _, failApply := range []bool{false, true} {
		t.Run(map[bool]string{false: "history_evicted", true: "partial_apply"}[failApply], func(t *testing.T) {
			mockey.PatchConvey("reset and partial application have different query lifetimes", t, func() {
				scheduler := nodescheduler.New(1)
				defer scheduler.Close()
				cfg, snapshot := readOnlyReplicaBaseline()
				r, err := newReadOnlyReplica(t.Context(), cfg, snapshot, nil, nil, nil, recovery.WithNodeScheduler(scheduler))
				require.NoError(t, err)
				defer r.Close()
				attempt := mockey.Mock((*recovery.ReadOnlyRecoveryStorage).SubscribeAndApply).To(func(storage *recovery.ReadOnlyRecoveryStorage, ctx context.Context, _ streamingpb.StreamingNodeReplicaServiceClient, _ types.PChannelInfo, _ ...grpc.CallOption) error {
					if failApply {
						return storage.Apply(ctx, walevent.Event{Cursor: walevent.Cursor{Term: 7, Seq: 1}})
					}
					return walevent.NewResumeRequired(storage.Applied(), walevent.Cursor{Term: 7, Seq: 100})
				}).Build()
				r.startFollowing(nil, cfg.Channel)
				select {
				case <-r.followDone:
				case <-time.After(5 * time.Second):
					t.Fatal("follower kept retrying a terminal stream")
				}
				require.Equal(t, !failApply, r.IsAvailable())
				require.Equal(t, !failApply, r.ReplicaRefreshRequired())
				require.Equal(t, 1, attempt.Times())
				require.Equal(t, walevent.Cursor{Term: 7}, r.storage.Applied())
			})
		})
	}
}

func TestReplicaFollowCloseInterruptsBackoff(t *testing.T) {
	mockey.PatchConvey("closing the runtime stops reconnect backoff", t, func() {
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		cfg, snapshot := readOnlyReplicaBaseline()
		r, err := newReadOnlyReplica(t.Context(), cfg, snapshot, nil, nil, nil, recovery.WithNodeScheduler(scheduler))
		require.NoError(t, err)
		defer r.Close()
		attempted := make(chan struct{})
		attempt := mockey.Mock((*recovery.ReadOnlyRecoveryStorage).SubscribeAndApply).To(func(*recovery.ReadOnlyRecoveryStorage, context.Context, streamingpb.StreamingNodeReplicaServiceClient, types.PChannelInfo, ...grpc.CallOption) error {
			close(attempted)
			return io.EOF
		}).Build()
		r.startFollowing(nil, cfg.Channel)
		<-attempted
		r.Close()
		require.Equal(t, 1, attempt.Times())
	})
}
