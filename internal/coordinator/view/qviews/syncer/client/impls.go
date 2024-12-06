package client

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews"
	"github.com/milvus-io/milvus/internal/proto/viewpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
)

type queryViewServiceClientImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]

	qnResolver resolver.Builder
	qnService  lazygrpc.Service[viewpb.QueryViewSyncServiceClient]

	snResolver resolver.Builder
	snService  lazygrpc.Service[viewpb.QueryViewSyncServiceClient]
}

func (c *queryViewServiceClientImpl) Sync(req SyncOption) (QueryViewServiceSyncer, error) {
	if c.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, ErrClosed
	}
	defer c.lifetime.Done()

	service := c.getService(req.WorkNode)
	newSyncer := newGRPCSyncer(service, &req)
	return newSyncer, nil
}

func (c *queryViewServiceClientImpl) getService(workNode qviews.WorkNode) lazygrpc.Service[viewpb.QueryViewSyncServiceClient] {
	switch workNode.(type) {
	case qviews.QueryNode:
		return c.qnService
	case qviews.StreamingNode:
		return c.snService
	default:
		panic("unknown work node type")
	}
}
