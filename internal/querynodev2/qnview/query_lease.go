package qnview

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type QueryViewLease struct {
	Version qviews.QueryViewVersion
	Meta    *viewpb.QueryViewMeta
	View    *viewpb.QueryViewOfQueryNode
	Release func()
}

func (h *QNQueryViewHandler) AcquireReadyView(ctx context.Context, shardID qviews.ShardID, walReplicaID int64, version qviews.QueryViewVersion) (*QueryViewLease, error) {
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
	return shard.acquireReadyView(ctx, walReplicaID, version)
}

func (s *qnShardView) acquireReadyView(ctx context.Context, walReplicaID int64, version qviews.QueryViewVersion) (*QueryViewLease, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	key, entry, exists := s.findReadyViewEntryLocked(walReplicaID, version)
	if !exists {
		return nil, viewerror.NewViewNotFound("query view %s is not found", version.String())
	}
	if entry.sm.State() != qviews.QueryViewStateReady {
		return nil, viewerror.NewViewInvalidated("query view %s is not ready, current state is %s", version.String(), entry.sm.State().String())
	}
	entry.queryRefs++
	view := proto.Clone(entry.View.IntoProto()).(*viewpb.QueryViewOfShard)
	var once sync.Once
	return &QueryViewLease{
		Version: version,
		Meta:    proto.Clone(view.GetMeta()).(*viewpb.QueryViewMeta),
		View:    proto.Clone(view.GetQueryNode()[0]).(*viewpb.QueryViewOfQueryNode),
		Release: func() { once.Do(func() { s.releaseQueryViewLease(key) }) },
	}, nil
}

func (s *qnShardView) findReadyViewEntryLocked(walReplicaID int64, version qviews.QueryViewVersion) (qviews.QueryViewKey, *qnViewEntry, bool) {
	for key, entry := range s.views {
		if key.WALReplicaID != walReplicaID || key.QueryViewVersion != version {
			continue
		}
		return key, entry, true
	}
	return qviews.QueryViewKey{}, nil, false
}

func (s *qnShardView) releaseQueryViewLease(key qviews.QueryViewKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.views[key]
	if !exists || entry.queryRefs == 0 {
		return
	}
	entry.queryRefs--
	if entry.queryRefs == 0 && entry.releasePending {
		s.releaseQueryResourceLocked(key, entry)
	}
}
