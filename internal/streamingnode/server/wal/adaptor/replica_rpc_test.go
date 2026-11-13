package adaptor_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestReplicaSubscriptionFromRealWAL(t *testing.T) {
	walimplstest.Reset()
	initResourceForTest(t)
	message.RegisterDefaultWALName(message.WALNameTest)
	opener, err := registry.MustGetBuilder(message.WALNameTest, redo.NewInterceptorBuilder(), lock.NewInterceptorBuilder(),
		replicate.NewInterceptorBuilder(), timetick.NewInterceptorBuilder(), shard.NewInterceptorBuilder()).Build()
	require.NoError(t, err)
	defer opener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	channel := types.PChannelInfo{Name: "replica-rpc", Term: 1}
	w, err := opener.Open(ctx, &wal.OpenOption{Channel: channel, DisableFlusher: true})
	require.NoError(t, err)
	closePrimary := sync.OnceFunc(w.Close)
	defer closePrimary()
	created, err := w.Append(ctx, message.NewCreateCollectionMessageBuilderV1().WithVChannel("replica-rpc_2v0").
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 2}).
		WithBody(&msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: "baseline", Version: 1}}).MustBuildMutable())
	require.NoError(t, err)
	resource.Resource().TimeTickInspector().TriggerSync(channel, false)
	provider := adaptor.RecoveryStorageForTest(w).(interface {
		CutReplicaSnapshot(context.Context) (*streamingpb.WALReplicaSnapshot, *walevent.Reader, error)
	})
	var snapshot *streamingpb.WALReplicaSnapshot
	require.Eventually(t, func() bool {
		var reader *walevent.Reader
		snapshot, reader, err = provider.CutReplicaSnapshot(ctx)
		require.NoError(t, err)
		reader.Close()
		return snapshot.GetObserved().GetTimeTick() >= created.TimeTick
	}, 5*time.Second, time.Millisecond)
	// The source exposes no tail in its metadata image. Use the real ordered
	// WAL scanner to rebuild an already nonempty channel before subscribing.
	scanner, err := w.Read(ctx, wal.ReadOption{DeliverPolicy: options.DeliverPolicyStartFrom(message.MustUnmarshalMessageID(snapshot.Checkpoint.MessageId)), IgnorePauseConsumption: true})
	require.NoError(t, err)
	defer scanner.Close()
	manager := mockey.Mock((*schemaRPCManager).GetAvailableWAL).Return(w, nil).Build()
	defer manager.UnPatch()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	startServer := func(listener net.Listener) *grpc.Server {
		server := grpc.NewServer()
		streamingpb.RegisterStreamingNodeReplicaServiceServer(server, service.NewReplicaService(&schemaRPCManager{}))
		go func() { _ = server.Serve(listener) }()
		return server
	}
	server := startServer(listener)
	defer func() { server.Stop() }()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	follower, ro, err := adaptor.NewReplicaFollowerForTest(ctx, recovery.ReadOnlyConfig{Channel: channel, Modules: vchannel.PChannelManagerConfig{Logger: mlog.With()}}, snapshot, func(ctx context.Context) (message.ImmutableMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-scanner.Chan():
			if !ok {
				return nil, scanner.Error()
			}
			return msg, nil
		}
	}, streamingpb.NewStreamingNodeReplicaServiceClient(conn))
	require.NoError(t, err)
	defer follower.Close()
	baselineSchema, err := ro.VChannelManager().Module("replica-rpc_2v0").GetSchemaVersion(2, 1)
	require.NoError(t, err)
	require.Equal(t, "baseline", baselineSchema.Schema.Name)
	scanner.Close()
	_, err = w.Append(ctx, message.NewCreateCollectionMessageBuilderV1().WithVChannel("replica-rpc_1v0").
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: "replicated", Version: 1}}).MustBuildMutable())
	require.NoError(t, err)
	resource.Resource().TimeTickInspector().TriggerSync(channel, false)
	require.Eventually(t, func() bool {
		module := ro.VChannelManager().Module("replica-rpc_1v0")
		if module == nil {
			return false
		}
		schema, err := module.GetSchemaVersion(1, 1)
		return err == nil && schema.Schema.Name == "replicated"
	}, 5*time.Second, time.Millisecond)
	module := ro.VChannelManager().Module("replica-rpc_1v0")
	before := ro.Applied()
	server.Stop() // Close the actual HTTP/2 connection without replacing the WAL.
	_, err = w.Append(ctx, message.NewCreateCollectionMessageBuilderV1().WithVChannel("replica-rpc_3v0").
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 3}).
		WithBody(&msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: "during-disconnect", Version: 1}}).MustBuildMutable())
	require.NoError(t, err)
	resource.Resource().TimeTickInspector().TriggerSync(channel, false)
	listener, err = net.Listen("tcp", address)
	require.NoError(t, err)
	server = startServer(listener)
	require.Eventually(t, func() bool {
		module := ro.VChannelManager().Module("replica-rpc_3v0")
		if module == nil {
			return false
		}
		schema, err := module.GetSchemaVersion(3, 1)
		return err == nil && schema.Schema.Name == "during-disconnect"
	}, 5*time.Second, time.Millisecond)
	require.Greater(t, ro.Applied().Seq, before.Seq)
	require.Equal(t, before.Term, ro.Applied().Term)
	require.True(t, follower.IsAvailable())
	require.False(t, follower.(wal.ReplicaRefreshProvider).ReplicaRefreshRequired())
	closePrimary()
	require.Same(t, module, ro.VChannelManager().Module("replica-rpc_1v0"))
	schema, err := module.GetSchemaVersion(1, 1)
	require.NoError(t, err)
	require.Equal(t, "replicated", schema.Schema.Name)
}
