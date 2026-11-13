package service

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type eventWAL struct{ wal.WAL }

func (*eventWAL) SubscribeWALEvents(context.Context, walevent.Cursor) (*walevent.Reader, error) {
	panic("mockey")
}

func TestWALReplicaGRPCResumeAndPrimaryTermChange(t *testing.T) {
	mockey.PatchConvey("real grpc delivery retains the RO runtime across attempts", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var active atomic.Pointer[walevent.Source]
		source := walevent.NewSource(7, 4, 1<<20, func(context.Context, walevent.Event) error { return nil })
		defer source.Close()
		active.Store(source)
		mockey.Mock((*replicaManager).GetAvailableWAL).To(func(_ *replicaManager, channel types.PChannelInfo) (wal.WAL, error) {
			if channel.Name != "p" {
				return nil, status.NewChannelNotExist(channel.Name)
			}
			return &eventWAL{}, nil
		}).Build()
		mockey.Mock((*eventWAL).SubscribeWALEvents).To(func(_ *eventWAL, _ context.Context, after walevent.Cursor) (*walevent.Reader, error) {
			return active.Load().Subscribe(after)
		}).Build()
		listener := bufconn.Listen(65536)
		server := grpc.NewServer()
		streamingpb.RegisterStreamingNodeReplicaServiceServer(server, NewReplicaService(&replicaManager{}))
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		defer listener.Close()
		conn, err := grpc.NewClient("passthrough:///wal-replica", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
		require.NoError(t, err)
		defer conn.Close()
		client := streamingpb.NewStreamingNodeReplicaServiceClient(conn)
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		channel := types.PChannelInfo{Name: "p", Term: 7}
		var intentCount atomic.Int64
		ro, err := recovery.NewReadOnlyRecoveryStorage(recovery.ReadOnlyConfig{
			Channel: channel, Applied: walevent.Cursor{Term: 7},
			Checkpoint: &recovery.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1, Term: 7},
			Modules:    vchannel.PChannelManagerConfig{Logger: mlog.With()},
		}, recovery.WithNodeScheduler(scheduler), recovery.WithQueryViewEventApplier(func(context.Context, []*viewpb.QueryViewOfShard) error { intentCount.Add(1); return nil }))
		require.NoError(t, err)
		defer ro.Close()
		runtime := ro.VChannelManager()
		attemptCtx, disconnect := context.WithCancel(ctx)
		finished := make(chan error, 1)
		go func() { finished <- ro.SubscribeAndApply(attemptCtx, client, channel) }()
		_, err = source.Publish(ctx, walevent.QueryViewIntentBatch{})
		require.NoError(t, err)
		require.Eventually(t, func() bool { return ro.Applied().Seq == 1 }, 5*time.Second, time.Millisecond)
		disconnect()
		require.Error(t, <-finished)
		for i := 0; i < 2; i++ {
			_, err = source.Publish(ctx, walevent.QueryViewIntentBatch{})
			require.NoError(t, err)
		}
		attemptCtx, disconnect = context.WithCancel(ctx)
		go func() { finished <- ro.SubscribeAndApply(attemptCtx, client, channel) }()
		require.Eventually(t, func() bool { return ro.Applied().Seq == 3 }, 5*time.Second, time.Millisecond)
		require.EqualValues(t, 3, intentCount.Load())
		disconnect()
		require.Error(t, <-finished)
		// Replacing a crashed primary does not mutate the local read runtime.
		source.Close()
		next := walevent.NewSource(8, 2, 1<<20, func(context.Context, walevent.Event) error { return nil })
		defer next.Close()
		active.Store(next)
		channel.Term = 8
		err = ro.SubscribeAndApply(ctx, client, channel)
		var reset *walevent.ResetError
		require.ErrorAs(t, err, &reset)
		require.Equal(t, walevent.Cursor{Term: 7, Seq: 3}, reset.Applied)
		require.Equal(t, walevent.Cursor{Term: 8}, reset.Requested)
		require.Same(t, runtime, ro.VChannelManager())
		require.Equal(t, walevent.Cursor{Term: 7, Seq: 3}, ro.Applied())
		// Missing history and malformed/unknown targets are explicit failures.
		for i := 0; i < 3; i++ {
			_, err = next.Publish(ctx, walevent.QueryViewIntentBatch{})
			require.NoError(t, err)
		}
		rpc, err := client.SubscribeWALEvents(ctx, &streamingpb.WALReplicaSubscribeRequest{Pchannel: types.NewProtoFromPChannelInfo(channel), After: (&walevent.Cursor{Term: 8}).IntoProto()})
		require.NoError(t, err)
		_, err = rpc.Recv()
		require.Error(t, err)
		for _, req := range []*streamingpb.WALReplicaSubscribeRequest{
			{}, {Pchannel: &streamingpb.PChannelInfo{Name: "missing", Term: 8}, After: (&walevent.Cursor{Term: 8}).IntoProto()},
		} {
			rpc, err = client.SubscribeWALEvents(ctx, req)
			require.NoError(t, err)
			_, err = rpc.Recv()
			require.Error(t, err)
		}
		// Source shutdown terminates a waiting stream without a business event.
		rpc, err = client.SubscribeWALEvents(ctx, &streamingpb.WALReplicaSubscribeRequest{Pchannel: types.NewProtoFromPChannelInfo(channel), After: (&walevent.Cursor{Term: 8, Seq: 3}).IntoProto()})
		require.NoError(t, err)
		_, err = next.Publish(ctx, walevent.WALEntry{Message: message.NewTimeTickMessageBuilderV1().WithAllVChannel().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).MustBuildMutable().WithTimeTick(10).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))})
		require.NoError(t, err)
		_, err = rpc.Recv()
		require.NoError(t, err)
		next.Close()
		_, err = rpc.Recv()
		require.Error(t, err)
	})
}

func TestReplicaEventStreamSendBudget(t *testing.T) {
	for _, outcome := range []string{"blocked", "idle", "send-error", "source-close", "canceled", "evicted"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			source := walevent.NewSource(7, 1, 1024, func(context.Context, walevent.Event) error { return nil })
			defer source.Close()
			reader, err := source.Subscribe(walevent.Cursor{Term: 7})
			require.NoError(t, err)
			defer reader.Close()
			if outcome != "idle" {
				_, err = source.Publish(ctx, walevent.QueryViewIntentBatch{})
				require.NoError(t, err)
			}
			if outcome == "evicted" {
				_, err = source.Publish(ctx, walevent.QueryViewIntentBatch{})
				require.NoError(t, err)
			}
			started, finished := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- serveReplicaEventReader(ctx, reader, 20*time.Millisecond, func(ctx context.Context, _ walevent.Event) error {
					defer close(finished)
					close(started)
					if outcome == "idle" || outcome == "send-error" {
						return merr.ErrServiceInternal
					}
					<-ctx.Done()
					return ctx.Err()
				})
			}()
			switch outcome {
			case "idle":
				select {
				case err := <-done:
					t.Fatalf("idle stream hit send deadline: %v", err)
				case <-time.After(60 * time.Millisecond):
				}
				_, err = source.Publish(ctx, walevent.QueryViewIntentBatch{})
				require.NoError(t, err)
			case "source-close":
				<-started
				source.Close()
			case "canceled":
				<-started
				cancel()
			}
			err = <-done
			switch outcome {
			case "idle", "send-error":
				require.ErrorIs(t, err, merr.ErrServiceInternal)
			case "canceled":
				require.ErrorIs(t, err, context.Canceled)
			case "blocked":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			case "evicted":
				var reset *walevent.ResetError
				require.ErrorAs(t, err, &reset)
			case "source-close":
				require.True(t, status.AsStreamingError(err).IsOnShutdown())
			}
			if outcome != "evicted" {
				select {
				case <-finished:
				case <-time.After(time.Second):
					t.Fatal("stream worker did not exit")
				}
			}
		})
	}
}
