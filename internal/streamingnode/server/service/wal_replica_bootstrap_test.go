package service

import (
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/client/connection"
	replicaclient "github.com/milvus-io/milvus/internal/streamingnode/client/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

type bootstrapWAL struct{ wal.WAL }

func (*bootstrapWAL) OpenReplicaBootstrap(context.Context, *streamingpb.WALCheckpoint) (wal.ReplicaBootstrap, error) {
	panic("mockey")
}

type bootstrapTransfer struct{ wal.ReplicaBootstrap }

func (*bootstrapTransfer) Snapshot() *streamingpb.WALReplicaSnapshot              { panic("mockey") }
func (*bootstrapTransfer) Next(context.Context) (message.ImmutableMessage, error) { panic("mockey") }
func (*bootstrapTransfer) Close()                                                 { panic("mockey") }

func bootstrapSnapshot() *streamingpb.WALReplicaSnapshot {
	return &streamingpb.WALReplicaSnapshot{
		Cursor:     &streamingpb.WALReplicaCursor{Term: 7, Seq: 2},
		Checkpoint: &streamingpb.WALCheckpoint{TimeTick: 1, MessageId: walimplstest.NewTestMessageID(1).IntoProto()},
		Observed:   &streamingpb.WALCheckpoint{TimeTick: 3, MessageId: walimplstest.NewTestMessageID(2).IntoProto()},
	}
}

func bootstrapTick(tick uint64, confirmed int64) message.ImmutableMessage {
	return message.NewTimeTickMessageBuilderV1().WithAllVChannel().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmed(walimplstest.NewTestMessageID(confirmed)).IntoImmutableMessage(walimplstest.NewTestMessageID(confirmed + 1))
}

func TestReplicaBootstrapGRPCRecovery(t *testing.T) {
	mockey.PatchConvey("bounded metadata and retained replay build a private RO runtime", t, func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		snapshot := bootstrapSnapshot()
		snapshot.QueryBarriers = map[string]uint64{strings.Repeat("v", 300000): 3}
		mockey.Mock((*replicaManager).GetAvailableWAL).Return(&bootstrapWAL{}, nil).Build()
		mockey.Mock((*bootstrapWAL).OpenReplicaBootstrap).Return(&bootstrapTransfer{}, nil).Build()
		mockey.Mock((*bootstrapTransfer).Snapshot).Return(snapshot).Build()
		var next atomic.Int64
		mockey.Mock((*bootstrapTransfer).Next).To(func(context.Context) (message.ImmutableMessage, error) {
			n := next.Add(1)
			return bootstrapTick(uint64(n+1), n), nil
		}).Build()
		var closed atomic.Bool
		mockey.Mock((*bootstrapTransfer).Close).To(func() { closed.Store(true) }).Build()
		listener := bufconn.Listen(65536)
		server := grpc.NewServer()
		streamingpb.RegisterStreamingNodeReplicaServiceServer(server, NewReplicaService(&replicaManager{}))
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		defer listener.Close()
		cc, err := grpc.NewClient("passthrough:///bootstrap", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
		require.NoError(t, err)
		conn := lazygrpc.NewConn(func(context.Context) (*grpc.ClientConn, error) { return cc, nil })
		defer conn.Close()
		mockey.Mock(connection.NewSession).Return(conn, (resolver.Builder)(nil)).Build()
		client := replicaclient.NewClient((*clientv3.Client)(nil))
		channel := types.PChannelInfo{Name: "p", Term: 7, AccessMode: types.AccessModeRW}
		source := client.ForPrimary(types.PChannelInfoAssigned{Channel: channel, Node: types.StreamingNodeInfo{ServerID: 42}})
		transfer, err := source.Bootstrap(ctx, nil)
		require.NoError(t, err)
		defer transfer.Close()
		require.True(t, proto.Equal(snapshot, transfer.Snapshot))
		scheduler := nodescheduler.New(1)
		defer scheduler.Close()
		ro, err := recovery.BootstrapReadOnlyRecoveryStorage(ctx, recovery.ReadOnlyConfig{Channel: channel, Modules: vchannel.PChannelManagerConfig{Logger: mlog.With()}}, transfer.Snapshot, transfer.Replay, recovery.WithNodeScheduler(scheduler))
		require.NoError(t, err)
		defer ro.Close()
		require.NoError(t, transfer.Complete(ctx))
		require.EqualValues(t, 2, next.Load())
		require.Eventually(t, closed.Load, time.Second, time.Millisecond)
		require.EqualValues(t, 2, ro.Applied().Seq)
	})
}

func TestReplicaBootstrapRejectsInvalidSource(t *testing.T) {
	for _, failure := range []string{"request", "missing", "unsupported", "open", "term", "snapshot", "interval", "replay", "nil", "order", "upper", "physical", "send", "cancel", "success"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("uncommitted transfer never reports completion", t, func() {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				snapshot := bootstrapSnapshot()
				req := &streamingpb.WALReplicaBootstrapRequest{Pchannel: &streamingpb.PChannelInfo{Name: "p", Term: 7}}
				var lookupErr, openErr, nextErr error
				var raw wal.WAL = &bootstrapWAL{}
				msg := bootstrapTick(3, 2)
				switch failure {
				case "request":
					req.Pchannel = nil
				case "missing":
					lookupErr = merr.WrapErrServiceUnavailable("missing")
				case "unsupported":
					raw = &eventWAL{}
				case "open":
					openErr = merr.WrapErrServiceUnavailable("pin rejected")
				case "term":
					snapshot.Cursor.Term = 8
				case "snapshot":
					snapshot.QueryBarriers = make(map[string]uint64)
					snapshot.QueryBarriers[strings.Repeat("v", 65<<20)] = 1
				case "interval":
					snapshot.Checkpoint.TimeTick = 4
				case "replay":
					nextErr = io.EOF
				case "nil":
					msg = nil
				case "order":
					msg = bootstrapTick(1, 1)
				case "upper":
					msg = bootstrapTick(4, 2)
				case "physical":
					msg = bootstrapTick(3, 1)
				case "cancel":
					cancel()
				}
				mockey.Mock((*replicaManager).GetAvailableWAL).Return(raw, lookupErr).Build()
				mockey.Mock((*bootstrapWAL).OpenReplicaBootstrap).Return(&bootstrapTransfer{}, openErr).Build()
				mockey.Mock((*bootstrapTransfer).Snapshot).Return(snapshot).Build()
				mockey.Mock((*bootstrapTransfer).Next).Return(msg, nextErr).Build()
				closeMock := mockey.Mock((*bootstrapTransfer).Close).Return().Build()
				complete := false
				service := NewReplicaService(&replicaManager{}).(*replicaServiceImpl)
				err := service.serveReplicaBootstrap(ctx, req, func(frame *streamingpb.WALReplicaBootstrapFrame) error {
					if failure == "send" {
						return io.ErrClosedPipe
					}
					complete = frame.GetComplete() != nil
					return nil
				})
				if failure == "success" {
					require.NoError(t, err)
					require.True(t, complete)
				} else {
					require.Error(t, err)
					require.False(t, complete)
				}
				if failure != "request" && failure != "missing" && failure != "unsupported" && failure != "open" {
					require.Equal(t, 1, closeMock.Times())
				}
			})
		})
	}
}

type bootstrapServerStream struct {
	streamingpb.StreamingNodeReplicaService_BootstrapWALReplicaServer
}

func (*bootstrapServerStream) Context() context.Context                         { panic("mockey") }
func (*bootstrapServerStream) Send(*streamingpb.WALReplicaBootstrapFrame) error { panic("mockey") }

func TestReplicaBootstrapAdmissionAndCancellation(t *testing.T) {
	mockey.PatchConvey("blocked transfers keep admission until their send exits", t, func() {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mockey.Mock((*bootstrapServerStream).Context).Return(ctx).Build()
		service := NewReplicaService(&replicaManager{}).(*replicaServiceImpl)
		for range cap(service.bootstrapSlots) {
			service.bootstrapSlots <- struct{}{}
		}
		require.Error(t, service.BootstrapWALReplica(nil, &bootstrapServerStream{}))
		for range cap(service.bootstrapSlots) {
			<-service.bootstrapSlots
		}
		// A failed provider request is also propagated through the RPC boundary.
		require.Error(t, service.BootstrapWALReplica(nil, &bootstrapServerStream{}))
		snapshot := bootstrapSnapshot()
		mockey.Mock((*replicaManager).GetAvailableWAL).Return(&bootstrapWAL{}, nil).Build()
		mockey.Mock((*bootstrapWAL).OpenReplicaBootstrap).Return(&bootstrapTransfer{}, nil).Build()
		mockey.Mock((*bootstrapTransfer).Snapshot).Return(snapshot).Build()
		closed := make(chan struct{})
		mockey.Mock((*bootstrapTransfer).Close).To(func() { close(closed) }).Build()
		sending, release := make(chan struct{}), make(chan struct{})
		mockey.Mock((*bootstrapServerStream).Send).To(func(*streamingpb.WALReplicaBootstrapFrame) error {
			close(sending)
			<-release
			return io.ErrClosedPipe
		}).Build()
		result := make(chan error, 1)
		go func() {
			result <- service.BootstrapWALReplica(&streamingpb.WALReplicaBootstrapRequest{Pchannel: &streamingpb.PChannelInfo{Name: "p", Term: 7}}, &bootstrapServerStream{})
		}()
		select {
		case <-sending:
		case <-time.After(time.Second):
			t.Fatal("send did not start")
		}
		cancel()
		select {
		case err := <-result:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("handler retained blocked send")
		}
		require.Len(t, service.bootstrapSlots, 1)
		close(release)
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("bootstrap protection was not released")
		}
		require.Eventually(t, func() bool { return len(service.bootstrapSlots) == 0 }, time.Second, time.Millisecond)
	})
}
