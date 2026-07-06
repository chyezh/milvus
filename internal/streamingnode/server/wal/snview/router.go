package snview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

var _ handler.QueryViewHandler = (*PChannelQueryViewRouter)(nil)
var _ viewquery.TaskProvider = (*PChannelQueryViewRouter)(nil)

type PChannelQueryViewRouter struct {
	mu       sync.RWMutex
	handlers map[string]*SNQueryViewHandler
}

func NewPChannelQueryViewRouter() *PChannelQueryViewRouter {
	return &PChannelQueryViewRouter{
		handlers: make(map[string]*SNQueryViewHandler),
	}
}

func (r *PChannelQueryViewRouter) Register(pchannel string, h *SNQueryViewHandler) func() {
	r.mu.Lock()
	r.handlers[pchannel] = h
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		if r.handlers[pchannel] == h {
			delete(r.handlers, pchannel)
		}
		r.mu.Unlock()
	}
}

func (r *PChannelQueryViewRouter) ApplyViews(views []handler.ApplyView) {
	grouped := make(map[string][]handler.ApplyView)
	for i := range views {
		pchannel := funcutil.ToPhysicalChannel(views[i].View.ShardID().VChannel)
		grouped[pchannel] = append(grouped[pchannel], views[i])
	}
	for pchannel, pchannelViews := range grouped {
		h := r.handler(pchannel)
		if h == nil {
			reportUnrecoverable(pchannelViews)
			continue
		}
		h.ApplyViews(pchannelViews)
	}
}

func (r *PChannelQueryViewRouter) handler(pchannel string) *SNQueryViewHandler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.handlers[pchannel]
}

func (r *PChannelQueryViewRouter) AcquireSearchSegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.SearchRequest,
) (viewquery.SearchSegmentTasks, error) {
	handler := r.handler(funcutil.ToPhysicalChannel(shardID.VChannel))
	if handler == nil {
		return nil, viewerror.NewViewNotFound("streamingnode query view handler for shard %s is not found", shardID.String())
	}
	return handler.AcquireSearchSegmentTasks(ctx, shardID, version, mvcc, req)
}

func (r *PChannelQueryViewRouter) AcquireQuerySegmentTasks(
	ctx context.Context,
	shardID qviews.ShardID,
	version qviews.QueryViewVersion,
	mvcc *viewpb.QueryPlanMVCC,
	req *internalpb.RetrieveRequest,
) (viewquery.QuerySegmentTasks, error) {
	handler := r.handler(funcutil.ToPhysicalChannel(shardID.VChannel))
	if handler == nil {
		return nil, viewerror.NewViewNotFound("streamingnode query view handler for shard %s is not found", shardID.String())
	}
	return handler.AcquireQuerySegmentTasks(ctx, shardID, version, mvcc, req)
}

func reportUnrecoverable(views []handler.ApplyView) {
	for _, view := range views {
		if view.OnReport == nil {
			continue
		}
		pb := view.View.IntoProto()
		pb.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateUnrecoverable)
		view.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(pb))
	}
}
