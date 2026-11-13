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
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walevent"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
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
	var primary recovery.RecoveryStorage
	var recoverStorage func(context.Context, recovery.RecoveryStreamBuilder, *utility.WALCheckpoint, message.ImmutableMessage, ...recovery.RecoveryStorageOption) (recovery.RecoveryStorage, *recovery.RecoverySnapshot, error)
	capture := mockey.Mock(recovery.RecoverRecoveryStorage).Origin(&recoverStorage).To(func(ctx context.Context, builder recovery.RecoveryStreamBuilder, cp *utility.WALCheckpoint, barrier message.ImmutableMessage, opts ...recovery.RecoveryStorageOption) (recovery.RecoveryStorage, *recovery.RecoverySnapshot, error) {
		storage, snapshot, err := recoverStorage(ctx, builder, cp, barrier, opts...)
		primary = storage
		return storage, snapshot, err
	}).Build()
	defer capture.UnPatch()
	w, err := opener.Open(ctx, &wal.OpenOption{Channel: channel, DisableFlusher: true})
	require.NoError(t, err)
	closePrimary := sync.OnceFunc(w.Close)
	defer closePrimary()
	// This fixture starts with an empty catalog and WAL. Capturing that empty
	// baseline exercises the real source without pretending to test bootstrap.
	var baseline walevent.Cursor
	var checkpoint *recovery.WALCheckpoint
	reader, err := primary.Events().Cut(ctx, func(cursor walevent.Cursor) error {
		baseline = cursor
		checkpoint = primary.GetCheckpoint(ctx)
		return nil
	})
	require.NoError(t, err)
	reader.Close()
	roChannel := channel
	roChannel.AccessMode = types.AccessModeRO
	ro, err := recovery.NewReadOnlyRecoveryStorage(recovery.ReadOnlyConfig{Channel: roChannel, Applied: baseline, Checkpoint: checkpoint, Modules: vchannel.PChannelManagerConfig{Logger: mlog.With()}})
	require.NoError(t, err)
	defer ro.Close()
	manager := mockey.Mock((*schemaRPCManager).GetAvailableWAL).Return(w, nil).Build()
	defer manager.UnPatch()
	listener := bufconn.Listen(65536)
	server := grpc.NewServer()
	streamingpb.RegisterStreamingNodeReplicaServiceServer(server, service.NewReplicaService(&schemaRPCManager{}))
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()
	conn, err := grpc.NewClient("passthrough:///real-wal-replica", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	defer conn.Close()
	finished := make(chan error, 1)
	go func() {
		finished <- ro.SubscribeAndApply(ctx, streamingpb.NewStreamingNodeReplicaServiceClient(conn), channel)
	}()
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
	closePrimary()
	select {
	case err := <-finished:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("primary shutdown did not terminate the subscription")
	}
	require.Same(t, module, ro.VChannelManager().Module("replica-rpc_1v0"))
	schema, err := module.GetSchemaVersion(1, 1)
	require.NoError(t, err)
	require.Equal(t, "replicated", schema.Schema.Name)
}
