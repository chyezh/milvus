package coordview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// SyncCallback is invoked when the ReliableSyncer has a response for a synced view.
//
// The response may be:
//
//	(a) A real response from the work node (normal operation).
//	(b) A synthetic response built by NodeLostResponseBuilder on node loss.
//
// Return value:
//
//	true  — the caller no longer needs to monitor this view; the ReliableSyncer
//	        removes it from the outstanding set and will not invoke the callback again.
//	false — the view remains in the outstanding set; the ReliableSyncer continues
//	        tracking it and may invoke the callback again on future responses.
//
// Called from a ReliableSyncer internal goroutine (per-node recv goroutine).
// Must not block for long.
type SyncCallback func(resp qviews.QueryViewAtWorkNode) bool

// NodeLostResponseBuilder builds a synthetic QueryViewAtWorkNode to be delivered
// via SyncCallback when the target node is declared lost.
//
// The ReliableSyncer does not interpret view states. The caller defines what
// response to generate on node loss by providing this builder.
type NodeLostResponseBuilder func() qviews.QueryViewAtWorkNode

// SyncView pairs a query view with its event callback and node-loss behavior.
type SyncView struct {
	// View is the query view state to push to the target work node.
	// The target node is determined by View.WorkNode().
	View qviews.QueryViewAtWorkNode

	// Callback is invoked when the ReliableSyncer receives a response (real or
	// synthetic) for this view. See SyncCallback for semantics.
	Callback SyncCallback

	// OnNodeLost builds the synthetic response to deliver via Callback when
	// the target node is declared lost. The ReliableSyncer calls OnNodeLost()
	// to obtain the response, then invokes Callback with it.
	OnNodeLost NodeLostResponseBuilder
}

// SyncGroup represents a batch of views to sync, grouped as a single unit.
// Using a struct instead of a bare slice allows future extension with
// group-level parameters (e.g., priority, deadline, metadata) without
// breaking the SyncViews signature.
type SyncGroup struct {
	// Views is the list of views to sync in this group.
	// Each view targets a specific work node (extracted from View.WorkNode()).
	// Views targeting different nodes are routed to their respective streams.
	Views []SyncView
}

// ReliableSyncer manages reliable delivery of QueryView syncs from Coord to work nodes.
//
// Delivery guarantee (the core contract):
//
//	For every outstanding sync whose callback has not yet returned true,
//	the ReliableSyncer guarantees that eventually the callback will be invoked
//	with either:
//	  (a) The node's real response, OR
//	  (b) A response built by SyncView.OnNodeLost if the node is declared lost.
//
// The guarantee is achieved through two mechanisms:
//  1. Re-push on reconnection: when a stream breaks and is re-established,
//     all outstanding syncs are re-pushed automatically.
//  2. Node loss handling: when reconnection fails beyond a configurable timeout,
//     the node is declared lost. For each outstanding entry, OnNodeLost() is
//     called to build a response, which is then delivered via Callback.
//
// The ReliableSyncer is stateless with respect to state machine semantics.
// It does not interpret view states or transitions. It simply:
//   - Tracks outstanding syncs keyed by (node, viewKey).
//   - Delivers views to nodes and routes responses back via callbacks.
//   - Removes an outstanding entry when its callback returns true.
//   - Re-pushes outstanding entries on reconnection.
//   - On node loss, calls OnNodeLost() to build a response and delivers it via Callback.
//
// Thread-safety: All methods are thread-safe.
type ReliableSyncer interface {
	// SyncViews delivers a group of query views to work nodes with delivery guarantee.
	//
	// Each SyncView in the group contains a view and its callback. The target
	// node for each view is extracted from view.WorkNode(). Views targeting
	// different nodes are routed to their respective per-node streams.
	//
	// The views are tracked internally as "outstanding syncs" keyed by
	// (node, viewKey) where viewKey = (replicaID, vchannel, version).
	//
	// When SyncViews is called for a viewKey that already has an outstanding
	// entry, the old entry (including its callback) is replaced.
	//
	// Outstanding entry lifecycle:
	//   - Persists until its callback is invoked and returns true.
	//   - Re-pushed to the node on stream reconnection.
	//   - On node loss, OnNodeLost() builds a response, then Callback is invoked.
	//
	// Non-blocking: returns after enqueuing. Returns error only if the
	// ReliableSyncer is closed or ctx is canceled.
	SyncViews(ctx context.Context, group SyncGroup) error

	// Close gracefully closes all streams and releases resources.
	Close() error
}
