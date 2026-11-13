package adaptor

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	replicaclient "github.com/milvus-io/milvus/internal/streamingnode/client/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func replicaOpenOption() *wal.OpenOption {
	channel := types.PChannelInfo{Name: "ro-replica", Term: 7, AccessMode: types.AccessModeRO}
	primary := channel
	primary.AccessMode = types.AccessModeRW
	return &wal.OpenOption{Channel: channel, Primary: &types.PChannelInfoAssigned{Channel: primary, Node: types.StreamingNodeInfo{ServerID: 11}}}
}

func TestReplicaOpenerOwnsRuntimeAfterComplete(t *testing.T) {
	for _, failure := range []string{"bootstrap", "recovery", "complete", "success"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("bootstrap must complete before an independently owned runtime is installed", t, func() {
				resource.InitForTest(t)
				o := NewOpenerAdaptor(nil).(*openerAdaptorImpl)
				defer o.Close()
				client := &replicaclient.Client{}
				created := mockey.Mock(replicaclient.NewClient).To(func(*clientv3.Client) *replicaclient.Client { return client }).Build()
				closed := mockey.Mock((*replicaclient.Client).Close).Return().Build()
				option := replicaOpenOption()
				mockey.Mock((*replicaclient.Client).ForPrimary).To(func(actual *replicaclient.Client, primary types.PChannelInfoAssigned) *replicaclient.Source {
					require.Same(t, client, actual)
					require.Equal(t, *option.Primary, primary)
					return &replicaclient.Source{}
				}).Build()
				_, snapshot := readOnlyReplicaBaseline()
				transfer := &replicaclient.Bootstrap{Snapshot: snapshot}
				mockey.Mock((*replicaclient.Source).Bootstrap).To(func(_ *replicaclient.Source, _ context.Context, cp *streamingpb.WALCheckpoint, _ ...grpc.CallOption) (*replicaclient.Bootstrap, error) {
					require.Nil(t, cp)
					if failure == "bootstrap" {
						return nil, io.ErrUnexpectedEOF
					}
					return transfer, nil
				}).Build()
				transferClosed := mockey.Mock((*replicaclient.Bootstrap).Close).Return().Build()
				completed := mockey.Mock((*replicaclient.Bootstrap).Complete).To(func(*replicaclient.Bootstrap, context.Context) error {
					require.Zero(t, o.walInstances.Len(), "private runtime is not installed before completion")
					if failure == "complete" {
						return io.ErrUnexpectedEOF
					}
					return nil
				}).Build()
				if failure == "recovery" {
					snapshot.Cursor.Term++
				}
				var following atomic.Bool
				mockey.Mock((*recovery.ReadOnlyRecoveryStorage).SubscribeAndApply).To(func(r *recovery.ReadOnlyRecoveryStorage, ctx context.Context, _ streamingpb.StreamingNodeReplicaServiceClient, primary types.PChannelInfo, _ ...grpc.CallOption) error {
					require.Equal(t, option.Primary.Channel, primary)
					require.Equal(t, walevent.Cursor{Term: 7}, r.Applied())
					following.Store(true)
					<-ctx.Done()
					return ctx.Err()
				}).Build()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				actual, err := o.Open(ctx, option)
				if failure != "success" {
					require.Error(t, err)
					require.Nil(t, actual)
					require.Zero(t, o.walInstances.Len())
					require.False(t, following.Load())
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, o.walInstances.Len())
					require.Eventually(t, following.Load, time.Second, time.Millisecond)
					cancel()
					require.True(t, actual.IsAvailable(), "assignment open context does not own local queries")
					require.Same(t, client, o.getReplicaClient())
					actual.Close()
					require.Zero(t, o.walInstances.Len())
					require.False(t, actual.IsAvailable())
				}
				require.Equal(t, 1, created.Times())
				require.Empty(t, o.openerCache, "RO must never acquire a backend writer")
				if failure == "bootstrap" {
					require.Zero(t, transferClosed.Times())
				} else {
					require.Equal(t, 1, transferClosed.Times())
				}
				if failure == "complete" || failure == "success" {
					require.Equal(t, 1, completed.Times())
				} else {
					require.Zero(t, completed.Times())
				}
				o.Close()
				require.Equal(t, 1, closed.Times())
			})
		})
	}
}

func TestReplicaOpenerRejectsInvalidAssignmentAndShutdown(t *testing.T) {
	resource.InitForTest(t)
	o := NewOpenerAdaptor(nil).(*openerAdaptorImpl)
	defer o.Close()
	for _, mutate := range []func(*wal.OpenOption){
		func(o *wal.OpenOption) { o.Primary = nil },
		func(o *wal.OpenOption) { o.Channel.AccessMode = types.AccessModeRW },
		func(o *wal.OpenOption) { o.Primary.Channel.AccessMode = types.AccessModeRO },
		func(o *wal.OpenOption) { o.Primary.Channel.Term++ },
		func(o *wal.OpenOption) { o.Primary.Channel.Name = "other" },
		func(o *wal.OpenOption) { o.Primary.Node.ServerID = 0 },
		func(o *wal.OpenOption) { o.Channel.Term = 0; o.Primary.Channel.Term = 0 },
	} {
		option := replicaOpenOption()
		mutate(option)
		actual, err := o.OpenReadOnlyReplica(t.Context(), option)
		require.Error(t, err)
		require.Nil(t, actual)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := o.OpenReadOnlyReplica(ctx, replicaOpenOption())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, o.replicaClient)
	_, err = o.Open(t.Context(), &wal.OpenOption{Channel: types.PChannelInfo{AccessMode: types.AccessModeRO}})
	require.Error(t, err, "RO service mode must never fall back to the obsolete raw ROWAL path")
	_, err = o.Open(t.Context(), &wal.OpenOption{Channel: types.PChannelInfo{AccessMode: types.AccessMode(-1)}})
	require.Error(t, err)
	o.Close()
	_, err = o.OpenReadOnlyReplica(t.Context(), replicaOpenOption())
	require.True(t, status.AsStreamingError(err).IsOnShutdown())
}
