package qnview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/nodeview/handler"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

var _ handler.QueryViewHandler = (*QNQueryViewHandler)(nil)

// QNQueryViewHandler implements QueryViewHandler for QueryNode.
//
// It manages query view state machines across multiple shards using a
// two-level locking scheme:
//   - Outer sync.Mutex: protects the shard map
//   - Per-shard sync.Mutex: serializes SM operations within a shard
//
// QN is stateless: no persistence, no recovery. On restart, Coord
// re-pushes all Preparing views.
//
// Segment loading is delegated to the SegmentManager. When a new Preparing
// view arrives, the handler acquires segments via SegmentManager. The
// SegmentManager drives SM progress by invoking OnReady/OnUnrecoverable
// callbacks asynchronously.
type QNQueryViewHandler struct {
	mu     sync.Mutex
	shards map[qviews.ShardID]*qnShardView
	segMgr SegmentManager
}

// NewQNQueryViewHandler creates a new QNQueryViewHandler.
func NewQNQueryViewHandler(segMgr SegmentManager) *QNQueryViewHandler {
	return &QNQueryViewHandler{
		shards: make(map[qviews.ShardID]*qnShardView),
		segMgr: segMgr,
	}
}

// ApplyViews applies a batch of coord-pushed views.
// Views are grouped by ShardID and applied atomically per shard.
// All state reports are delivered through the OnReport callback.
func (h *QNQueryViewHandler) ApplyViews(views []handler.ApplyView) {
	// Group views by ShardID.
	grouped := make(map[qviews.ShardID][]handler.ApplyView)
	for i := range views {
		shardID := views[i].View.QueryViewKey().ShardID
		grouped[shardID] = append(grouped[shardID], views[i])
	}

	// Apply each group atomically under the shard lock.
	for shardID, shardViews := range grouped {
		shard := h.getOrCreateShard(shardID)
		shard.ApplyViews(shardViews)
	}
}

// Close releases all resources.
func (h *QNQueryViewHandler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.shards = make(map[qviews.ShardID]*qnShardView)
}

func (h *QNQueryViewHandler) getOrCreateShard(shardID qviews.ShardID) *qnShardView {
	h.mu.Lock()
	defer h.mu.Unlock()
	shard, ok := h.shards[shardID]
	if !ok {
		shard = &qnShardView{
			views:  make(map[qviews.QueryViewVersion]*qnViewEntry),
			segMgr: h.segMgr,
		}
		h.shards[shardID] = shard
	}
	return shard
}
