package qview

import "github.com/milvus-io/milvus/internal/proto/viewpb"

type (
	QueryViewState viewpb.QueryViewState

	NodeSyncState int // NodeSyncState marks the on-syncing node state.
)

// Preparing -> Ready: If all worknodes are ready.
// Preparing -> Unrecoverable: If any worknode are unrecoverable, or the query view is deprecated when preparing by balancer.
// Ready -> Up: If the streamingnode is up.
// Up -> Down: By Down interface.
// Down -> Dropping: If the streamingnode is down.
// Unrecoverable -> Dropping: By DropView interface.
// Dropping -> Dropped: If all worknodes are dropped.
const (
	QueryViewStatePreparing     = QueryViewState(viewpb.QueryViewState_QueryViewStatePreparing)
	QueryViewStateReady         = QueryViewState(viewpb.QueryViewState_QueryViewStateReady)
	QueryViewStateUp            = QueryViewState(viewpb.QueryViewState_QueryViewStateUp)
	QueryViewStateDown          = QueryViewState(viewpb.QueryViewState_QueryViewStateDown)
	QueryViewStateUnrecoverable = QueryViewState(viewpb.QueryViewState_QueryViewStateUnrecoverable)
	QueryViewStateDropping      = QueryViewState(viewpb.QueryViewState_QueryViewStateDropping)
	QueryViewStateDropped       = QueryViewState(viewpb.QueryViewState_QueryViewStateDropped)
)

// StateTransition is the transition of the query view state.
type StateTransition struct {
	From QueryViewState
	To   QueryViewState
}
