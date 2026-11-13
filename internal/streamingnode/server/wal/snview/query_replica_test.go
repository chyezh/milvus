package snview

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type replicaTaskManager struct {
	pchannelWALProvider
	wal.QueryReplicaManager
}

func (*replicaTaskManager) GetAvailableQueryReplica(*viewpb.StreamingWorkNode) (wal.WAL, error) {
	panic("mockey")
}

func TestQueryReplicaTaskLookupRejectsReplacement(t *testing.T) {
	mockey.PatchConvey("phase two validates the exact runtime before acquiring tasks", t, func() {
		instance := &fakeViewQueryWAL{}
		target := &viewpb.StreamingWorkNode{Pchannel: "p", NodeId: 2, WalReplicaId: 3, WalReplicaRevision: 4, RuntimeId: 5, WalTerm: 7, ReadOnly: true}
		outgoing, err := worknodehandler.EncodeQueryReplicaToOutgoingContext(t.Context(), target)
		require.NoError(t, err)
		md, _ := metadata.FromOutgoingContext(outgoing)
		ctx := metadata.NewIncomingContext(t.Context(), md)
		var lookupErr error
		lookup := mockey.Mock((*replicaTaskManager).GetAvailableQueryReplica).To(func(_ *replicaTaskManager, actual *viewpb.StreamingWorkNode) (wal.WAL, error) {
			require.EqualValues(t, 5, actual.RuntimeId)
			return instance, lookupErr
		}).Build()
		server := NewPChannelViewQueryServer(&replicaTaskManager{}, nil)
		provider, err := server.taskProviderForVChannel(ctx, "p_100v0")
		require.NoError(t, err)
		require.Same(t, instance, provider)
		lookupErr = context.Canceled
		_, err = server.taskProviderForVChannel(ctx, "p_100v0")
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		server.walManager = &fakeViewQueryWALManager{wal: instance}
		_, err = server.taskProviderForVChannel(ctx, "p_100v0")
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		target.RuntimeId = 0
		outgoing, err = worknodehandler.EncodeQueryReplicaToOutgoingContext(t.Context(), target)
		require.NoError(t, err)
		md, _ = metadata.FromOutgoingContext(outgoing)
		_, err = server.taskProviderForVChannel(metadata.NewIncomingContext(t.Context(), md), "p_100v0")
		require.Error(t, err)
		require.Equal(t, 2, lookup.Times())
	})
}
