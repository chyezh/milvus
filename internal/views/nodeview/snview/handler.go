package snview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/nodeview/handler"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

var _ handler.QueryViewHandler = (*SNQueryViewHandler)(nil)

// StreamingNodeCatalog defines the persistence interface for SN query views,
// implemented by the streaming node's catalog layer.
type StreamingNodeCatalog interface {
	// SaveQueryView persists a query view for crash recovery.
	SaveQueryView(key string, view *viewpb.QueryViewOfShard) error

	// DeleteQueryView removes a persisted query view.
	DeleteQueryView(key string) error

	// ListQueryViews returns all persisted query views.
	ListQueryViews() ([]*viewpb.QueryViewOfShard, error)
}

// SNQueryViewHandler implements QueryViewHandler for StreamingNode.
//
// It manages query view state machines across multiple shards using a
// two-level locking scheme:
//   - Outer sync.RWMutex: protects the shard map
//   - Per-shard sync.Mutex: serializes SM operations within a shard
//
// SN supports crash recovery via persistence. Recovered views start in
// UpRecovering state and transition to Up once WAL catch-up completes.
type SNQueryViewHandler struct {
	mu      sync.Mutex
	shards  map[qviews.ShardID]*snShardView
	catalog StreamingNodeCatalog
}

// NewSNQueryViewHandler creates a new SNQueryViewHandler.
func NewSNQueryViewHandler(catalog StreamingNodeCatalog) *SNQueryViewHandler {
	return &SNQueryViewHandler{
		shards:  make(map[qviews.ShardID]*snShardView),
		catalog: catalog,
	}
}

// Recover reconstructs state machines from persisted views during SN startup.
// Must be called once before any ApplyViews calls.
func (h *SNQueryViewHandler) Recover(views []*viewpb.QueryViewOfShard) {
	for _, view := range views {
		meta := view.Meta
		snView := view.StreamingNode
		shardID := qviews.NewShardIDFromQVMeta(meta)
		version := qviews.FromProtoQueryViewVersion(meta.Version)

		shard := h.getOrCreateShard(shardID)
		sm := RecoverSNQueryViewStateMachine(meta, snView)
		shard.Recover(version, sm)
	}
}

// ApplyViews applies a batch of coord-pushed views.
// Views are grouped by ShardID and applied atomically per shard.
// All state reports are delivered through the OnReport callback.
func (h *SNQueryViewHandler) ApplyViews(views []handler.ApplyView) {
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

// NotifyReady is called when async resource preparation completes.
// Transitions SM from Preparing → Ready.
func (h *SNQueryViewHandler) NotifyReady(key qviews.QueryViewKey) {
	shard := h.getShard(key.ShardID)
	if shard == nil {
		return
	}
	shard.NotifyReady(key.QueryViewVersion)
}

// NotifyRecoveringDone is called after WAL catch-up during crash recovery.
// Transitions SM from UpRecovering → Up.
func (h *SNQueryViewHandler) NotifyRecoveringDone(key qviews.QueryViewKey) {
	shard := h.getShard(key.ShardID)
	if shard == nil {
		return
	}
	shard.NotifyRecoveringDone(key.QueryViewVersion)
}

// NotifyUnrecoverable is called when a fatal error occurs.
func (h *SNQueryViewHandler) NotifyUnrecoverable(key qviews.QueryViewKey) {
	shard := h.getShard(key.ShardID)
	if shard == nil {
		return
	}
	shard.NotifyUnrecoverable(key.QueryViewVersion)
}

// Close releases all resources.
func (h *SNQueryViewHandler) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.shards = make(map[qviews.ShardID]*snShardView)
}

func (h *SNQueryViewHandler) getOrCreateShard(shardID qviews.ShardID) *snShardView {
	h.mu.Lock()
	defer h.mu.Unlock()
	shard, ok := h.shards[shardID]
	if !ok {
		shard = &snShardView{
			views:   make(map[qviews.QueryViewVersion]*snViewEntry),
			catalog: h.catalog,
		}
		h.shards[shardID] = shard
	}
	return shard
}

func (h *SNQueryViewHandler) getShard(shardID qviews.ShardID) *snShardView {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.shards[shardID]
}
