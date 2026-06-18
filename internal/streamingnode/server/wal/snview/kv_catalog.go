package snview

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type queryViewCatalog interface {
	ListQueryViews(ctx context.Context) ([]*viewpb.QueryViewOfShard, error)
	SaveQueryViews(ctx context.Context, views []*viewpb.QueryViewOfShard) error
}

type streamingNodeCatalog struct {
	catalog queryViewCatalog
}

// NewKVStreamingNodeCatalog creates a node-local SN query-view recovery catalog.
func NewKVStreamingNodeCatalog(metaKV kv.MetaKv, nodeID int64) StreamingNodeCatalog {
	return NewStreamingNodeCatalog(queryview.NewQueryViewCatalog(metaKV, fmt.Sprintf("streamingnode/%d", nodeID)))
}

// NewStreamingNodeCatalog adapts a generic query-view catalog to SN semantics.
func NewStreamingNodeCatalog(catalog queryViewCatalog) StreamingNodeCatalog {
	return &streamingNodeCatalog{catalog: catalog}
}

func (c *streamingNodeCatalog) ListQueryViews(ctx context.Context) ([]*viewpb.QueryViewOfShard, error) {
	views, err := c.catalog.ListQueryViews(ctx)
	if err != nil {
		return nil, err
	}
	upViews := make([]*viewpb.QueryViewOfShard, 0, len(views))
	for _, view := range views {
		if qviews.QueryViewState(view.GetMeta().GetState()) == qviews.QueryViewStateUp {
			upViews = append(upViews, view)
		}
	}
	return upViews, nil
}

func (c *streamingNodeCatalog) SaveQueryView(view *viewpb.QueryViewOfShard) error {
	if qviews.QueryViewState(view.GetMeta().GetState()) == qviews.QueryViewStateUp {
		return c.catalog.SaveQueryViews(context.Background(), []*viewpb.QueryViewOfShard{view})
	}
	dropped := proto.Clone(view).(*viewpb.QueryViewOfShard)
	dropped.Meta.State = viewpb.QueryViewState_QueryViewStateDropped
	return c.catalog.SaveQueryViews(context.Background(), []*viewpb.QueryViewOfShard{dropped})
}
