package coordview

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
)

// outstandingEntry tracks a single outstanding sync with its callback and node-loss builder.
type outstandingEntry struct {
	view       qviews.QueryViewAtWorkNode
	callback   SyncCallback
	onNodeLost NodeLostResponseBuilder
}

// outstanding tracks all pending view requests that are waiting for responses.
// Thread-safe.
type outstanding struct {
	mu      sync.Mutex
	entries map[viewKey]outstandingEntry
}

func newOutstanding() *outstanding {
	return &outstanding{
		entries: make(map[viewKey]outstandingEntry),
	}
}

// Upsert inserts or replaces an outstanding entry for the given view.
// Returns the proto to send to the node.
func (o *outstanding) Upsert(sv SyncView) *viewpb.QueryViewOfShard {
	key := viewKeyFromView(sv.View)
	o.mu.Lock()
	o.entries[key] = outstandingEntry{
		view:       sv.View,
		callback:   sv.Callback,
		onNodeLost: sv.OnNodeLost,
	}
	o.mu.Unlock()
	return sv.View.IntoProto()
}

// MatchResponse matches a received response proto to outstanding entries
// and invokes the callback. If callback returns true, the entry is removed.
func (o *outstanding) MatchResponse(pb *viewpb.QueryViewOfShard) {
	view := qviews.NewQueryViewAtWorkNodeFromProto(pb)
	key := viewKeyFromView(view)

	o.mu.Lock()
	entry, ok := o.entries[key]
	if !ok {
		o.mu.Unlock()
		return
	}
	if entry.callback(view) {
		delete(o.entries, key)
	}
	o.mu.Unlock()
}

// DrainByNode removes all outstanding entries targeting the given node,
// invokes onNodeLost() for each, and delivers the result via callback.
// This is called when service discovery reports a node removal.
func (o *outstanding) DrainByNode(node qviews.WorkNode) {
	nodeKey := node.String()

	o.mu.Lock()
	var drained []outstandingEntry
	for key, entry := range o.entries {
		if entry.view.WorkNode().String() == nodeKey {
			drained = append(drained, entry)
			delete(o.entries, key)
		}
	}
	o.mu.Unlock()

	for _, entry := range drained {
		resp := entry.onNodeLost()
		entry.callback(resp)
	}
}

// CollectProtosForNode returns the protos of all outstanding entries targeting
// the given node. Used by ResumableSyncer to re-push on stream reconnection.
func (o *outstanding) CollectProtosForNode(node qviews.WorkNode) []*viewpb.QueryViewOfShard {
	nodeKey := node.String()

	o.mu.Lock()
	defer o.mu.Unlock()

	var protos []*viewpb.QueryViewOfShard
	for _, entry := range o.entries {
		if entry.view.WorkNode().String() == nodeKey {
			protos = append(protos, entry.view.IntoProto())
		}
	}
	return protos
}
