package queryclient

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// QueryPlanServiceResolver resolves the QueryPlanServiceClient for a given vchannel.
// Uses WAL binding to find the owning StreamingNode.
type QueryPlanServiceResolver interface {
	ResolveQueryPlanService(ctx context.Context, vchannel string) (viewpb.QueryPlanServiceClient, error)
}

// ViewQueryServiceResolver resolves the ViewQueryServiceClient for a given work node.
type ViewQueryServiceResolver interface {
	ResolveViewQueryService(ctx context.Context, node qviews.WorkNode) (viewpb.ViewQueryServiceClient, error)
}

// ShardResolver resolves the shards of a collection.
type ShardResolver interface {
	ResolveShards(ctx context.Context, collectionID int64) ([]qviews.ShardID, error)
}
