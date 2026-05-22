//go:build test && dynamic

package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/internal/streamingnode/server/viewresource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type recordingViewResourceRegistry struct {
	ready chan struct{}
	seen  chan viewresource.ViewResourceDescriptor
}

func newRecordingViewResourceRegistry() *recordingViewResourceRegistry {
	return &recordingViewResourceRegistry{
		ready: make(chan struct{}),
		seen:  make(chan viewresource.ViewResourceDescriptor, 1),
	}
}

func (r *recordingViewResourceRegistry) OnAlterLoadConfig(walview.VChannelWALView) walview.VChannelLiveObserver {
	return nil
}

func (r *recordingViewResourceRegistry) OnDropLoadConfig(walview.DropLoadConfigEvent) {}

func (r *recordingViewResourceRegistry) GetViewRuntime(desc viewresource.ViewResourceDescriptor) (*viewresource.ViewRuntime, bool, error) {
	select {
	case r.seen <- desc:
	default:
	}
	return &viewresource.ViewRuntime{
		CollectionID: desc.CollectionID,
		VChannel:     desc.VChannel,
		DataVersion:  desc.Version.DataVersion,
	}, true, nil
}

func (r *recordingViewResourceRegistry) EvictBefore(int64, string, qviews.DataVersion) {}

func (r *recordingViewResourceRegistry) ReleaseLoad(int64, string) {}

func (r *recordingViewResourceRegistry) NotifyReady() <-chan struct{} {
	return r.ready
}

type testStreamingNodeQueryViewCatalog struct {
	views []*viewpb.QueryViewOfShard
}

func (c *testStreamingNodeQueryViewCatalog) ListQueryViews(context.Context) ([]*viewpb.QueryViewOfShard, error) {
	return c.views, nil
}

func (c *testStreamingNodeQueryViewCatalog) SaveQueryView(*viewpb.QueryViewOfShard) error {
	return nil
}

func TestRegisterGRPCServiceRegistersViewSyncAndRecoversPersistedViews(t *testing.T) {
	registry := newRecordingViewResourceRegistry()
	view := &viewpb.QueryViewOfShard{
		Meta: &viewpb.QueryViewMeta{
			CollectionId: 10,
			ReplicaId:    20,
			Vchannel:     "by-dev-rootcoord-dml_0_10v0",
			Version: &viewpb.QueryViewVersion{
				DataVersion:  &viewpb.DataVersion{StreamingVersion: 30, CompactVersion: 40},
				QueryVersion: 50,
			},
			State: viewpb.QueryViewState_QueryViewStateUp,
			Settings: &viewpb.QueryViewSettings{
				RequiredFields: []int64{100},
			},
			DeleteApplyStartAfterTimetick: 60,
		},
		StreamingNode: &viewpb.QueryViewOfStreamingNode{},
	}
	resource.InitForTest(t,
		resource.OptViewResourceRegistry(registry),
		resource.OptStreamingNodeQueryViewCatalog(&testStreamingNodeQueryViewCatalog{views: []*viewpb.QueryViewOfShard{view}}),
	)

	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()
	s := &Server{
		handlerService: service.NewHandlerService(nil),
		managerService: service.NewManagerService(nil),
	}
	s.registerGRPCService(grpcServer)

	_, ok := grpcServer.GetServiceInfo()[viewpb.ViewSyncService_ServiceDesc.ServiceName]
	require.True(t, ok)

	select {
	case desc := <-registry.seen:
		require.Equal(t, int64(10), desc.CollectionID)
		require.Equal(t, int64(20), desc.ReplicaID)
		require.Equal(t, "by-dev-rootcoord-dml_0_10v0", desc.VChannel)
		require.Equal(t, qviews.DataVersion{StreamingVersion: 30, CompactVersion: 40}, desc.Version.DataVersion)
		require.Equal(t, int64(50), desc.Version.QueryVersion)
		require.Equal(t, uint64(60), desc.DeleteApplyStartAfterTimeTick)
	case <-time.After(3 * time.Second):
		t.Fatal("persisted query view was not recovered through view resource registry")
	}
}
