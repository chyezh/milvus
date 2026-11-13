package queryplan

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/streamingnode/server/queryplan/provider"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	worknodehandler "github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type replicaMVCCWAL struct {
	wal.WAL
	provider.QueryPlanProvider
}

func (*replicaMVCCWAL) ResolveQueryPlanMVCC(context.Context, *viewpb.GetQueryPlanRequest) (*viewpb.QueryPlanMVCC, error) {
	panic("mockey")
}

type replicaMVCCManager struct{ walManager }

func (*replicaMVCCManager) GetAvailableWAL(types.PChannelInfo) (wal.WAL, error) { panic("mockey") }

func TestServerResolvesReplicaMVCCWithoutLocalPlanning(t *testing.T) {
	mockey.PatchConvey("the separate endpoint only invokes writer consistency resolution", t, func() {
		req := &viewpb.GetQueryPlanRequest{ShardId: &viewpb.ShardID{Vchannel: "by-dev-rootcoord-dml_0_100v0"}}
		frontier := &viewpb.QueryPlanMVCC{GrowingTimetick: 10, TransformingTimetick: 9}
		var local wal.WAL = &replicaMVCCWAL{}
		var resolveErr error
		mockey.Mock((*replicaMVCCManager).GetAvailableWAL).To(func(*replicaMVCCManager, types.PChannelInfo) (wal.WAL, error) { return local, nil }).Build()
		resolve := mockey.Mock((*replicaMVCCWAL).ResolveQueryPlanMVCC).To(func(_ *replicaMVCCWAL, _ context.Context, actual *viewpb.GetQueryPlanRequest) (*viewpb.QueryPlanMVCC, error) {
			require.Same(t, req, actual)
			return frontier, resolveErr
		}).Build()
		s := NewServer(&replicaMVCCManager{})
		outgoing := worknodehandler.EncodeQueryViewPChannelToOutgoingContext(t.Context(), types.PChannelInfo{Name: "by-dev-rootcoord-dml_0", Term: 7, AccessMode: types.AccessModeRW})
		md, _ := metadata.FromOutgoingContext(outgoing)
		ctx := metadata.NewIncomingContext(t.Context(), md)
		got, err := s.ResolveQueryPlanMVCC(ctx, req)
		require.NoError(t, err)
		require.Same(t, frontier, got)
		resolveErr = context.DeadlineExceeded
		_, err = s.ResolveQueryPlanMVCC(ctx, req)
		require.ErrorIs(t, err, resolveErr)
		local = &struct {
			wal.WAL
			provider.QueryPlanProvider
		}{}
		_, err = s.ResolveQueryPlanMVCC(ctx, req)
		require.True(t, viewerror.AsViewError(err).IsNotPrimary())
		_, err = s.ResolveQueryPlanMVCC(t.Context(), req)
		require.Error(t, err, "missing channel metadata must not reach the writer")
		require.Equal(t, 2, resolve.Times())
	})
}
