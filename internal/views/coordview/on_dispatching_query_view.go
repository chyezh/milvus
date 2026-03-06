package coordview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// onDispatchingQueryView tracks all query views that have been dispatched to
// work nodes but are still waiting for responses.
// Thread-safe.
//
// Views are indexed by WorkNodeKey → QueryViewKey for fast per-node lookups.
// A view's target node (WorkNode) is immutable for a given QueryViewKey.
type onDispatchingQueryView struct {
	mu     sync.Mutex
	byNode map[qviews.WorkNodeKey]map[qviews.QueryViewKey]SyncView
}

func newOnDispatchingQueryView() *onDispatchingQueryView {
	return &onDispatchingQueryView{
		byNode: make(map[qviews.WorkNodeKey]map[qviews.QueryViewKey]SyncView),
	}
}

// Upsert inserts or replaces a dispatching entry for the given view.
// Returns the proto to send to the node.
func (d *onDispatchingQueryView) Upsert(sv SyncView) *viewpb.QueryViewOfShard {
	key := sv.View.QueryViewKey()
	nodeKey := sv.View.WorkNode().Key()

	d.mu.Lock()
	nodeEntries := d.byNode[nodeKey]
	if nodeEntries == nil {
		nodeEntries = make(map[qviews.QueryViewKey]SyncView)
		d.byNode[nodeKey] = nodeEntries
	}
	nodeEntries[key] = sv
	d.mu.Unlock()

	return sv.View.IntoProto()
}

// MatchResponse matches a received response proto to dispatching entries
// and invokes the callback. If callback returns true, the entry is removed.
//
// The callback is invoked without holding the lock (it is concurrent-safe).
func (d *onDispatchingQueryView) MatchResponse(pb *viewpb.QueryViewOfShard) {
	view := qviews.NewQueryViewAtWorkNodeFromProto(pb)
	key := view.QueryViewKey()
	nodeKey := view.WorkNode().Key()

	d.mu.Lock()
	nodeEntries := d.byNode[nodeKey]
	entry, ok := nodeEntries[key]
	d.mu.Unlock()
	if !ok {
		return
	}

	if entry.Callback(view) {
		d.mu.Lock()
		if nodeEntries := d.byNode[nodeKey]; nodeEntries != nil {
			delete(nodeEntries, key)
			if len(nodeEntries) == 0 {
				delete(d.byNode, nodeKey)
			}
		}
		d.mu.Unlock()
	}
}

// DrainByNode removes all dispatching entries targeting the given node,
// invokes OnNodeLost() for each, and delivers the result via Callback.
// This is called when service discovery reports a node removal.
func (d *onDispatchingQueryView) DrainByNode(node qviews.WorkNode) {
	nodeKey := node.Key()

	d.mu.Lock()
	nodeEntries := d.byNode[nodeKey]
	drained := make([]SyncView, 0, len(nodeEntries))
	for _, sv := range nodeEntries {
		drained = append(drained, sv)
	}
	delete(d.byNode, nodeKey)
	d.mu.Unlock()

	for _, entry := range drained {
		resp := entry.OnNodeLost()
		entry.Callback(resp)
	}
}

// CollectProtosForNode returns the protos of all dispatching entries targeting
// the given node. Used by ResumableSyncer to re-push on stream reconnection.
func (d *onDispatchingQueryView) CollectProtosForNode(node qviews.WorkNode) []*viewpb.QueryViewOfShard {
	nodeKey := node.Key()

	d.mu.Lock()
	defer d.mu.Unlock()

	nodeEntries := d.byNode[nodeKey]
	if len(nodeEntries) == 0 {
		return nil
	}

	protos := make([]*viewpb.QueryViewOfShard, 0, len(nodeEntries))
	for _, sv := range nodeEntries {
		protos = append(protos, sv.View.IntoProto())
	}
	return protos
}
