package coordview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v2/proto/viewpb"
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
// Thread-safety: the callback must be safe for concurrent invocation from
// multiple ReliableSyncer internal goroutines (per-node recv goroutines).
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

// SyncGroup represents a batch of views to sync, pre-grouped by target work node.
// Using a struct instead of a bare map allows future extension with
// group-level parameters (e.g., priority, deadline, metadata) without
// breaking the SyncViews signature.
type SyncGroup struct {
	// ViewsByNode maps each work node to the views targeting it.
	ViewsByNode map[qviews.WorkNodeKey][]SyncView
}

// ReliableSyncer manages reliable delivery of QueryView syncs from Coord to work nodes.
//
// Delivery guarantee (the core contract):
//
//	For every outstanding sync whose callback has not yet returned true,
//	the ReliableSyncer guarantees that eventually the callback will be invoked
//	with either:
//	  (a) The node's real response, OR
//	  (b) A response built by SyncView.OnNodeLost if the node is declared lost
//	      (detected via service discovery).
//
// The guarantee is achieved through two mechanisms:
//  1. Re-push on reconnection: when a stream breaks and is re-established,
//     all outstanding syncs are re-pushed automatically via ResumableSyncer.
//  2. Node loss handling: when service discovery reports a node removal,
//     the node's ResumableSyncer is closed. For each outstanding entry
//     targeting that node, OnNodeLost() builds a response, which is then
//     delivered via Callback.
//
// The ReliableSyncer is stateless with respect to state machine semantics.
// It does not interpret view states or transitions. It simply:
//   - Tracks outstanding syncs keyed by viewKey in a global Outstanding structure.
//   - Delivers views to nodes via per-node ResumableSyncers and routes responses
//     back via callbacks.
//   - Removes an outstanding entry when its callback returns true.
//   - On node loss (service discovery), calls OnNodeLost() and delivers via Callback.
//
// Thread-safety: All methods are thread-safe.
type ReliableSyncer interface {
	// SyncViews delivers a group of query views to work nodes with delivery guarantee.
	//
	// Each SyncView in the group contains a view and its callback. Views are
	// pre-grouped by target work node in SyncGroup.ViewsByNode and routed to
	// their respective per-node ResumableSyncers.
	//
	// The views are tracked internally as "outstanding syncs" keyed by
	// viewKey = (replicaID, vchannel, version).
	//
	// When SyncViews is called for a viewKey that already has an outstanding
	// entry, the old entry (including its callback) is replaced.
	//
	// Outstanding entry lifecycle:
	//   - Persists until its callback is invoked and returns true.
	//   - Re-pushed to the node on stream reconnection.
	//   - On node loss (service discovery), OnNodeLost() builds a response,
	//     then Callback is invoked.
	//
	// Non-blocking: returns after enqueuing. Returns error only if the
	// ReliableSyncer is closed or ctx is canceled.
	SyncViews(ctx context.Context, group SyncGroup) error

	// Close gracefully closes all ResumableSyncers and releases resources.
	Close() error
}

// ViewSyncClient provides service discovery and gRPC stream creation for a specific node type.
// Implemented separately for StreamingNode (via HandlerClient) and QueryNode (via etcd session).
type ViewSyncClient interface {
	// WatchNodeChanged returns a channel that signals node membership changes.
	// The channel receives a value whenever the set of known nodes changes.
	WatchNodeChanged(ctx context.Context) (<-chan struct{}, error)

	// GetAllNodes returns all currently known nodes as a map from WorkNodeKey
	// to the node identity.
	GetAllNodes(ctx context.Context) (map[qviews.WorkNodeKey]qviews.WorkNode, error)

	// OpenSyncStream opens a SyncQueryView bidirectional stream to the given node.
	OpenSyncStream(ctx context.Context, node qviews.WorkNode) (viewpb.ViewSyncService_SyncQueryViewClient, error)

	// Close closes the client and releases resources.
	Close()
}
