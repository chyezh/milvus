//go:build test
// +build test

package resource

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	tinspector "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type noopStreamingNodeQueryViewCatalog struct{}

var _ snview.StreamingNodeCatalog = noopStreamingNodeQueryViewCatalog{}

func (noopStreamingNodeQueryViewCatalog) ListQueryViews(context.Context) ([]*viewpb.QueryViewOfShard, error) {
	return nil, nil
}

func (noopStreamingNodeQueryViewCatalog) SaveQueryView(*viewpb.QueryViewOfShard) error {
	return nil
}

type testMixCoordClient struct {
	*mocks.MockMixCoordClient
}

func (c testMixCoordClient) GetDataView(context.Context, *datapb.GetDataViewRequest, ...grpc.CallOption) (*datapb.GetDataViewResponse, error) {
	return nil, errors.New("unexpected GetDataView call")
}

// OptWriteBufferManager provides a write buffer manager to the resource (test only).
func OptWriteBufferManager(wbMgr writebuffer.BufferManager) optResourceInit {
	return func(r *resourceImpl) {
		r.wbMgr = wbMgr
	}
}

// InitForTest initializes the singleton of resources for test.
func InitForTest(t *testing.T, opts ...optResourceInit) {
	r = &resourceImpl{
		logger: log.With(),
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.wbMgr == nil && r.chunkManager != nil {
		r.syncMgr = syncmgr.NewSyncManager(r.chunkManager)
		r.wbMgr = writebuffer.NewManager(r.syncMgr)
	}
	if r.mixCoordClient != nil {
		r.timestampAllocator = idalloc.NewTSOAllocator(r.mixCoordClient)
		r.idAllocator = idalloc.NewIDAllocator(r.mixCoordClient)
	} else {
		f := syncutil.NewFuture[types.MixCoordClient]()
		f.Set(testMixCoordClient{MockMixCoordClient: idalloc.NewMockRootCoordClient(t)})
		r.mixCoordClient = f
		r.timestampAllocator = idalloc.NewTSOAllocator(r.mixCoordClient)
		r.idAllocator = idalloc.NewIDAllocator(r.mixCoordClient)
	}
	r.segmentStatsManager = stats.NewStatsManager()
	r.timeTickInspector = tinspector.NewTimeTickSyncInspector()
	if r.streamingNodeQueryViewCatalog == nil {
		r.streamingNodeQueryViewCatalog = noopStreamingNodeQueryViewCatalog{}
	}
	if r.queryViewRouter == nil {
		r.queryViewRouter = snview.NewPChannelQueryViewRouter()
	}
}
