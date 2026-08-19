package snview

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func (h *SNQueryViewHandler) AcquireUpView(ctx context.Context, shardID qviews.ShardID, walReplicaID int64, version qviews.QueryViewVersion) (*QueryViewLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	h.mu.Lock()
	shard := h.shards[shardID]
	h.mu.Unlock()
	if shard == nil {
		return nil, viewerror.NewViewNotFound("query view %s is not found", shardID.String())
	}
	return shard.acquireUpView(ctx, qviews.QueryViewKey{ShardID: shardID, WALReplicaID: walReplicaID, QueryViewVersion: version})
}

func (s *snShardView) acquireUpView(ctx context.Context, key qviews.QueryViewKey) (*QueryViewLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[key]
	if !exists {
		return nil, viewerror.NewViewNotFound("query view %s is not found", key.String())
	}
	if entry.sm.State() != qviews.QueryViewStateUp {
		return nil, viewerror.NewViewInvalidated("query view %s is not up, current state is %s", key.String(), entry.sm.State().String())
	}
	entry.queryRefs++
	view := proto.Clone(entry.View.IntoProto()).(*viewpb.QueryViewOfShard)
	var once sync.Once
	return &QueryViewLease{
		Version: key.QueryViewVersion,
		Meta:    proto.Clone(view.GetMeta()).(*viewpb.QueryViewMeta),
		View:    view,
		Release: func() { once.Do(func() { s.releaseQueryViewLease(key) }) },
	}, nil
}
