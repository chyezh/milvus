package snview

import (
	"slices"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

var _ handler.QueryViewHandler = (*SNQueryViewHandler)(nil)

// SNQueryViewHandler implements QueryViewHandler for StreamingNode.
//
// It manages query view state machines across multiple shards using a
// two-level locking scheme:
//   - Outer sync.Mutex: protects the shard map
//   - Per-shard sync.Mutex: serializes SM operations within a shard
//
// SN supports crash recovery via persistence. Recovered views start in
// UpRecovering state and transition to Up once WAL catch-up completes.
//
// Resource management is delegated to the StreamingNodeResourceManager.
// When a new Preparing view arrives, the handler acquires resources via
// ResourceManager. The ResourceManager drives SM progress by invoking
// OnReady/OnUnrecoverable callbacks asynchronously.
//
// # Response Guarantee
//
// Every view pushed via ApplyViews is guaranteed to eventually produce a
// response (via OnReport callback), provided the StreamingNodeResourceManager
// fulfills its liveness contracts (see StreamingNodeResourceManager doc).
// The response paths are:
//
// View does not exist in handler:
//
//   - Preparing: creates SM + calls Acquire. No immediate response.
//     Response depends on ResourceManager calling OnReady or OnUnrecoverable.
//   - Dropped: responds immediately with the Dropped view (SN restart case).
//   - Other states: responds immediately with Unrecoverable (state lost after restart).
//
// View already exists in handler:
//
//   - Preparing, SM in Preparing/UpRecovering/Dropping: no immediate response.
//     Response depends on ResourceManager callbacks.
//   - Preparing, SM past Preparing/UpRecovering/Dropping: responds immediately with
//     current state (Ready/Up/Down/Unrecoverable/Dropped) for Coord fast-forward.
//   - Dropped, SM in Preparing/Ready/Up/Down/Unrecoverable: transitions to Dropping,
//     calls Release. No immediate response.
//     Response depends on ResourceManager calling OnDropped.
//   - Dropped, SM in Dropping: ignored (Release already in progress).
//     Response depends on prior Release's OnDropped callback.
//   - Dropped, SM in Dropped: responds immediately with Dropped re-report.
//     (In practice unreachable — entry is deleted upon reaching Dropped.)
//   - Other states: SM handles coord push and responds accordingly.
type SNQueryViewHandler struct {
	mu      sync.Mutex
	shards  map[qviews.ShardID]*snShardView
	catalog StreamingNodeCatalog
	resMgr  StreamingNodeResourceManager
}

// RecoverSNQueryViewHandler reconstructs the handler from persisted views
// during SN startup. Pass nil or empty views for a fresh handler.
func RecoverSNQueryViewHandler(
	catalog StreamingNodeCatalog,
	resMgr StreamingNodeResourceManager,
	views []*viewpb.QueryViewOfShard,
) *SNQueryViewHandler {
	h := &SNQueryViewHandler{
		shards:  make(map[qviews.ShardID]*snShardView),
		catalog: catalog,
		resMgr:  resMgr,
	}

	// Build SMs grouped by shard.
	type shardRecovery struct {
		shardID qviews.ShardID
		views   map[qviews.QueryViewVersion]*SNQueryViewStateMachine
	}
	grouped := make(map[qviews.ShardID]*shardRecovery)

	for _, view := range views {
		meta := view.Meta
		snView := view.StreamingNode
		shardID := qviews.NewShardIDFromQVMeta(meta)
		version := qviews.FromProtoQueryViewVersion(meta.Version)

		sr, ok := grouped[shardID]
		if !ok {
			sr = &shardRecovery{shardID: shardID, views: make(map[qviews.QueryViewVersion]*SNQueryViewStateMachine)}
			grouped[shardID] = sr
		}
		sr.views[version] = RecoverSNQueryViewStateMachine(meta, snView)
	}

	// Create shard views and start recovery via ResourceManager.
	for shardID, sr := range grouped {
		shard := recoverSnShardView(shardID, sr.views, catalog, resMgr)
		shard.onEmpty = h.makeOnEmpty(shardID)
		h.shards[shardID] = shard
	}

	return h
}

func RecoverPChannelSNQueryViewHandler(
	pchannel string,
	catalog StreamingNodeCatalog,
	resMgr StreamingNodeResourceManager,
	views []*viewpb.QueryViewOfShard,
) *SNQueryViewHandler {
	return RecoverSNQueryViewHandler(catalog, resMgr, FilterQueryViewsByPChannel(pchannel, views))
}

func FilterQueryViewsByPChannel(pchannel string, views []*viewpb.QueryViewOfShard) []*viewpb.QueryViewOfShard {
	filtered := make([]*viewpb.QueryViewOfShard, 0, len(views))
	for _, view := range views {
		if funcutil.ToPhysicalChannel(view.GetMeta().GetVchannel()) == pchannel {
			filtered = append(filtered, view)
		}
	}
	return filtered
}

func OldestUpDataVersions(views []*viewpb.QueryViewOfShard) map[string]qviews.DataVersion {
	result := make(map[string]qviews.DataVersion)
	for _, view := range views {
		meta := view.GetMeta()
		if qviews.QueryViewState(meta.GetState()) != qviews.QueryViewStateUp || meta.GetVersion() == nil {
			continue
		}
		version := qviews.FromProtoQueryViewVersion(meta.GetVersion())
		current, ok := result[meta.GetVchannel()]
		if !ok || current.GT(version.DataVersion) {
			result[meta.GetVchannel()] = version.DataVersion
		}
	}
	return result
}

func RecoveredLoadConfigs(views []*viewpb.QueryViewOfShard) map[string]*streamingpb.VChannelLoadConfig {
	selected := make(map[string]*viewpb.QueryViewMeta)
	selectedVersion := make(map[string]qviews.QueryViewVersion)
	for _, view := range views {
		meta := view.GetMeta()
		if qviews.QueryViewState(meta.GetState()) != qviews.QueryViewStateUp || meta.GetVersion() == nil {
			continue
		}
		version := qviews.FromProtoQueryViewVersion(meta.GetVersion())
		current, ok := selectedVersion[meta.GetVchannel()]
		if !ok || current.GT(version) {
			selected[meta.GetVchannel()] = meta
			selectedVersion[meta.GetVchannel()] = version
		}
	}
	result := make(map[string]*streamingpb.VChannelLoadConfig, len(selected))
	for vchannel, meta := range selected {
		settings := meta.GetSettings()
		fields := make([]*messagespb.LoadFieldConfig, 0, len(settings.GetRequiredFields()))
		for _, fieldID := range settings.GetRequiredFields() {
			fields = append(fields, &messagespb.LoadFieldConfig{FieldId: fieldID})
		}
		result[vchannel] = &streamingpb.VChannelLoadConfig{
			Header: &messagespb.AlterLoadConfigMessageHeader{
				CollectionId: meta.GetCollectionId(),
				PartitionIds: append([]int64{}, settings.GetRequiredPartitions()...),
				LoadFields:   fields,
			},
		}
	}
	return result
}

func SortQueryViewsByVersion(views []*viewpb.QueryViewOfShard) {
	slices.SortFunc(views, func(left, right *viewpb.QueryViewOfShard) int {
		lv := qviews.FromProtoQueryViewVersion(left.GetMeta().GetVersion())
		rv := qviews.FromProtoQueryViewVersion(right.GetMeta().GetVersion())
		if lv.EQ(rv) {
			return 0
		}
		if rv.GT(lv) {
			return -1
		}
		return 1
	})
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

func (h *SNQueryViewHandler) CloseForHandoff() {
	h.mu.Lock()
	shards := make([]*snShardView, 0, len(h.shards))
	for _, shard := range h.shards {
		shards = append(shards, shard)
	}
	h.shards = make(map[qviews.ShardID]*snShardView)
	h.mu.Unlock()

	for _, shard := range shards {
		shard.CloseForHandoff()
	}
}

func (h *SNQueryViewHandler) getOrCreateShard(shardID qviews.ShardID) *snShardView {
	h.mu.Lock()
	defer h.mu.Unlock()
	shard, ok := h.shards[shardID]
	if !ok {
		shard = &snShardView{
			shardID: shardID,
			views:   make(map[qviews.QueryViewVersion]*snViewEntry),
			catalog: h.catalog,
			resMgr:  h.resMgr,
			onEmpty: h.makeOnEmpty(shardID),
		}
		h.shards[shardID] = shard
	}
	return shard
}

func (h *SNQueryViewHandler) makeOnEmpty(shardID qviews.ShardID) func() {
	return func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.shards, shardID)
	}
}
