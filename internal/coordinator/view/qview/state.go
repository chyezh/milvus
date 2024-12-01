package qview

import "github.com/milvus-io/milvus/internal/proto/viewpb"

type (
	QueryViewState viewpb.QueryViewState

	NodeSyncState int // NodeSyncState marks the on-syncing node state.
)

const (
	QueryViewStatePreparing = QueryViewState(viewpb.QueryViewState_QueryViewStatePreparing)
	QueryViewStateReady     = QueryViewState(viewpb.QueryViewState_QueryViewStateReady)
	QueryViewStateUp        = QueryViewState(viewpb.QueryViewState_QueryViewStateUp)
	QueryViewStateDown      = QueryViewState(viewpb.QueryViewState_QueryViewStateDown)
	QueryViewStateDropping  = QueryViewState(viewpb.QueryViewState_QueryViewStateDropping)
	QueryViewStateDropped   = QueryViewState(viewpb.QueryViewState_QueryViewStateDropped)

	NodeSyncStateNot      NodeSyncState = iota + 1 // NodeSync signal haven't been sent.
	NodeSyncStateInFlight                          // NodeState signal has been sent, on flight and wait for acknowledge.
	NodeSyncStateAcked                             // NodeState signal has been sent to node and acknowledged, but still in preparing state not ready.
	NodeSyncStateReady                             // NodeState signal has been acknowledged and ready.
)
